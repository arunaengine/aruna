//! Queues one webhook or email delivery job per held notice, deduplicated by the notice id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::events::Event;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::email::{EMAIL_CATEGORIES, EmailDeliverySpec};
use aruna_core::structs::execution::job::{JobId, JobPayload, WorkspaceMode, user_dedup_key};
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::webhook::WebhookDeliverySpec;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::submit::{SubmitJobError, SubmitJobOperation, SubmitJobSpec, mint_configured_job};

#[derive(Debug, Error, PartialEq)]
pub enum EnqueueError {
    #[error("{channel} delivery queue: {source}")]
    Submit {
        channel: &'static str,
        source: SubmitJobError,
    },
    #[error("the delivery queue did not finish")]
    NotFinished,
}

/// Queues a webhook job per share upload and an email job per email-category notice unless the
/// realm has no email profile; the job reads the setting when it runs. A queued job is kept,
/// and a failure is returned so the sender delivers again.
#[derive(Debug, PartialEq)]
pub struct EnqueueDeliveriesOperation {
    waiting: VecDeque<(&'static str, SubmitJobOperation)>,
    current: Option<(&'static str, SubmitJobOperation)>,
    /// Events still owed to the closing effects of the last finished submit.
    owed: usize,
    result: Option<Result<(), EnqueueError>>,
}

impl EnqueueDeliveriesOperation {
    /// `config` is the realm config of the records' realm; `None` when it could not be read,
    /// which fails any delivery so the sender delivers again.
    pub fn new(
        owner_node: NodeId,
        config: Option<&RealmConfigDocument>,
        records: &[NotificationRecord],
        now_ms: u64,
    ) -> Self {
        let mut deliveries = Vec::new();
        for record in records {
            if matches!(record.kind, NotificationKind::ShareUpload { .. }) {
                let payload = JobPayload::WebhookDelivery(WebhookDeliverySpec {
                    subscription_id: Ulid::nil(),
                    notification: record.clone(),
                });
                deliveries.push(("webhook", record, payload));
            }
        }
        if config.is_none_or(|config| config.email.is_some()) {
            for record in records {
                if EMAIL_CATEGORIES.contains(&record.kind.category()) {
                    let payload = JobPayload::EmailDelivery(EmailDeliverySpec {
                        preference_id: Ulid::nil(),
                        notification: record.clone(),
                    });
                    deliveries.push(("email", record, payload));
                }
            }
        }
        let mut operation = Self {
            waiting: VecDeque::with_capacity(deliveries.len()),
            current: None,
            owed: 0,
            result: None,
        };
        for (channel, record, payload) in deliveries {
            let name = format!("{channel}/{}", record.notification_id);
            let dedup_key = user_dedup_key(record.recipient, &name);
            let job_id = match mint(config, owner_node, &dedup_key) {
                Ok(job_id) => job_id,
                Err(source) => {
                    operation.result = Some(Err(EnqueueError::Submit { channel, source }));
                    break;
                }
            };
            let spec = SubmitJobSpec {
                payload,
                created_by: record.recipient,
                owner_node_id: owner_node,
                dedup_key: Some(dedup_key),
                now_ms,
                retention_ms: JOB_RETENTION_MS,
                workspace_mode: WorkspaceMode::default(),
                workspace_bucket: None,
                active_cap: None,
            };
            (operation.waiting).push_back((channel, SubmitJobOperation::new(spec, job_id)));
        }
        operation
    }

    /// Starts the next submit, and ends once none is left or one failed.
    fn next(&mut self) -> Effects {
        while self.result.is_none() {
            let Some((channel, mut submit)) = self.waiting.pop_front() else {
                self.result = Some(Ok(()));
                break;
            };
            let effects = submit.start();
            if !submit.is_complete() {
                self.current = Some((channel, submit));
                return effects;
            }
            self.settle(channel, submit);
            if !effects.is_empty() {
                return self.close(effects);
            }
        }
        smallvec![]
    }

    /// Runs the closing effects of a finished submit before the next one starts.
    fn close(&mut self, effects: Effects) -> Effects {
        self.owed = effects.len();
        effects
    }

    fn settle(&mut self, channel: &'static str, submit: SubmitJobOperation) {
        if let Err(source) = submit.finalize() {
            self.result = Some(Err(EnqueueError::Submit { channel, source }));
        }
    }
}

fn mint(
    config: Option<&RealmConfigDocument>,
    owner_node: NodeId,
    dedup_key: &[u8],
) -> Result<JobId, SubmitJobError> {
    let unavailable = || SubmitJobError::PlacementUnavailable("realm config unavailable".into());
    mint_configured_job(config.ok_or_else(unavailable)?, owner_node, dedup_key)
}

impl EffectOperation for EnqueueDeliveriesOperation {
    type Output = ();
    type Error = EnqueueError;

    fn start(&mut self) -> Effects {
        self.next()
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.owed > 0 {
            self.owed -= 1;
            return match self.owed {
                0 => self.next(),
                _ => smallvec![],
            };
        }
        let Some((channel, mut submit)) = self.current.take() else {
            let got = format!("{event:?}");
            let source = SubmitJobError::UnexpectedEvent(got);
            self.result = Some(Err(EnqueueError::Submit {
                channel: "delivery",
                source,
            }));
            return smallvec![];
        };
        let effects = submit.step(event);
        if !submit.is_complete() {
            self.current = Some((channel, submit));
            return effects;
        }
        self.settle(channel, submit);
        match effects.is_empty() {
            true => self.next(),
            false => self.close(effects),
        }
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(EnqueueError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.current.as_mut() {
            Some((_, submit)) => submit.abort(),
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::operation_check::rejects_finalize;
    use aruna_core::structs::execution::notification::NotificationClass;

    fn upload() -> NotificationRecord {
        let mut notice = record(user(1, 2), NotificationClass::Direct, 1_000);
        notice.kind = NotificationKind::ShareUpload {
            share_id: Ulid::from_parts(4, 4),
            node_id: node_id(3),
            group_id: Ulid::from_parts(5, 5),
            bucket: "inbox".to_string(),
            key: "a.csv".to_string(),
            size_bytes: 9,
        };
        notice
    }

    fn placed() -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
        config.seed_default_placement();
        config.seed_job_control(node_id(1), 0);
        config
    }

    #[test]
    fn nothing_to_queue() {
        let notice = record(user(1, 2), NotificationClass::Direct, 1_000);
        let config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
        let mut enqueue =
            EnqueueDeliveriesOperation::new(node_id(1), Some(&config), &[notice], 1_000);
        assert!(enqueue.start().is_empty());
        assert_eq!(enqueue.finalize(), Ok(()));
    }

    #[test]
    fn unplaced_job_fails() {
        let mut enqueue = EnqueueDeliveriesOperation::new(node_id(1), None, &[upload()], 1_000);
        assert!(enqueue.start().is_empty());
        assert!(matches!(
            enqueue.finalize(),
            Err(EnqueueError::Submit {
                channel: "webhook",
                ..
            })
        ));
    }

    #[test]
    fn upload_submits_job() {
        let config = placed();
        let mut enqueue =
            EnqueueDeliveriesOperation::new(node_id(1), Some(&config), &[upload()], 1_000);
        assert!(matches!(
            enqueue.start().as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        enqueue.abort();
        rejects_finalize(enqueue);
    }

    #[test]
    fn stray_event_fails() {
        let mut enqueue = EnqueueDeliveriesOperation::new(node_id(1), None, &[], 1_000);
        enqueue.start();
        let mut done = EnqueueDeliveriesOperation::new(node_id(1), None, &[], 1_000);
        assert!(done.step(Event::Search()).is_empty());
        assert!(done.finalize().is_err());
        assert_eq!(enqueue.finalize(), Ok(()));
    }
}
