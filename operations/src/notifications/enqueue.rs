//! Queues one webhook or email delivery job per held notice, deduplicated by the notice id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::structs::execution::email::{EMAIL_CATEGORIES, EmailDeliverySpec};
use aruna_core::structs::execution::job::{JobPayload, WorkspaceMode, user_dedup_key};
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::webhook::WebhookDeliverySpec;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use thiserror::Error;
use ulid::Ulid;

use crate::jobs::JOB_RETENTION_MS;
use crate::jobs::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, job_placement, mint_job_id,
};

#[derive(Debug, Error, PartialEq)]
pub enum EnqueueError {
    #[error("{channel} delivery queue: {reason}")]
    Submit {
        channel: &'static str,
        reason: String,
    },
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Queues a webhook job per share upload and an email job per email-category notice unless the
/// realm has no email profile; the job reads the setting when it runs. A queued job is kept,
/// and a failure is returned so the sender delivers again.
#[derive(Debug, PartialEq)]
pub struct EnqueueDeliveriesOperation {
    waiting: VecDeque<(&'static str, SubmitJobSpec, (PlacementHandle, BucketId))>,
    /// The channel of the submit that runs.
    current: &'static str,
    /// A job id that could not be placed, returned before any submit runs.
    failed: Option<EnqueueError>,
    ids: IdMint,
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
            current: "delivery",
            failed: None,
            ids: IdMint::default(),
        };
        for (channel, record, payload) in deliveries {
            let name = format!("{channel}/{}", record.notification_id);
            let dedup_key = user_dedup_key(record.recipient, &name);
            let placement = match place(config, owner_node, &dedup_key) {
                Ok(placement) => placement,
                Err(source) => {
                    let reason = source.to_string();
                    operation.failed = Some(EnqueueError::Submit { channel, reason });
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
            (operation.waiting).push_back((channel, spec, placement));
        }
        operation
    }

    /// Runs the next submit as a child that a lost conflict reruns, or ends once none is left.
    fn submit(&mut self) -> Result<Next<()>, EnqueueError> {
        let Some((channel, spec, (handle, bucket))) = self.waiting.pop_front() else {
            return Ok(Next::Done(()));
        };
        self.current = channel;
        let job_id = mint_job_id(handle, bucket, &mut self.ids, spec.now_ms).map_err(|error| {
            let reason = SubmitJobError::from(error).to_string();
            EnqueueError::Submit { channel, reason }
        })?;
        let build = move || SubmitJobOperation::new(spec.clone(), job_id);
        let submit = boxed_retry(build, &job_id.to_bytes(), |result| {
            Event::Child(ChildOutcome::new(result))
        });
        Ok(Next::Call(IoCall::Child(submit)))
    }
}

fn place(
    config: Option<&RealmConfigDocument>,
    owner_node: NodeId,
    dedup_key: &[u8],
) -> Result<(PlacementHandle, BucketId), SubmitJobError> {
    let unavailable = || SubmitJobError::PlacementUnavailable("realm config unavailable".into());
    job_placement(config.ok_or_else(unavailable)?, owner_node, dedup_key)
}

impl Operation for EnqueueDeliveriesOperation {
    type Output = ();
    type Error = EnqueueError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        match self.failed.take() {
            Some(error) => Err(error),
            None => self.submit(),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let submitted = result.child::<SubmitJobResult, SubmitJobError>()?;
        submitted.map_err(|error| EnqueueError::Submit {
            channel: self.current,
            reason: error.to_string(),
        })?;
        self.submit()
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::structs::execution::job::JobId;
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

    fn queued(result: Result<SubmitJobResult, SubmitJobError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    #[test]
    fn nothing_to_queue() {
        let notice = record(user(1, 2), NotificationClass::Direct, 1_000);
        let config = RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3);
        let mut enqueue =
            EnqueueDeliveriesOperation::new(node_id(1), Some(&config), &[notice], 1_000);
        assert_eq!(enqueue.first(), Ok(Next::Done(())));
    }

    #[test]
    fn unplaced_job_fails() {
        let mut enqueue = EnqueueDeliveriesOperation::new(node_id(1), None, &[upload()], 1_000);
        assert!(matches!(
            enqueue.first(),
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
        assert!(matches!(enqueue.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(
            enqueue.next(queued(Ok(SubmitJobResult {
                job_id: JobId::from_bytes([1; 16]),
                created: true,
            }))),
            Ok(Next::Done(()))
        );
        let mut failed =
            EnqueueDeliveriesOperation::new(node_id(1), Some(&config), &[upload()], 1_000);
        failed.first().unwrap();
        assert!(matches!(
            failed.next(queued(Err(SubmitJobError::PlacementUnavailable(
                "full".to_string()
            )))),
            Err(EnqueueError::Submit {
                channel: "webhook",
                ..
            })
        ));
        assert!(failed.next(IoResult::Done).is_err());
    }
}
