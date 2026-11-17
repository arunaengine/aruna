//! Answers a peer's delivery of notification records to the inbox this node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification_transport::NotificationTransportMessage;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::notifications::checks::{
    batch_realm, validate_inbound_batch, verify_batch_holder, verify_node_origin,
};
use crate::notifications::enqueue::EnqueueDeliveriesOperation;
use crate::notifications::inbox::UpsertInboxOperation;

#[derive(Debug, Error, PartialEq)]
pub enum DeliverError {
    #[error("unexpected event while storing delivered notifications: {0}")]
    UnexpectedEvent(String),
    #[error("the notification delivery did not finish")]
    NotFinished,
}

/// Who delivers and where the records land.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliverInput {
    pub peer: NodeId,
    pub local: NodeId,
    pub local_realm: RealmId,
    pub now_ms: u64,
}

#[derive(Debug, Default, PartialEq)]
enum DeliverState {
    #[default]
    Init,
    Config {
        realm_id: RealmId,
    },
    Upsert(Box<UpsertInboxOperation>, Box<RealmConfigDocument>),
    Wake {
        config: Box<RealmConfigDocument>,
        held: Vec<NotificationRecord>,
        written: usize,
    },
    Enqueue(Box<EnqueueDeliveriesOperation>, usize),
    /// Waits for the cleanup of a failed child before it answers.
    Cleanup {
        left: usize,
        answer: NotificationTransportMessage,
    },
    Done(NotificationTransportMessage),
    Failed(DeliverError),
}

/// Stores a peer's batch in this holder's inbox and queues its deliveries. Only a sync-eligible
/// node of this realm may deliver, every record must be valid and held here, and a bucket
/// notice must come from the node that holds the bucket. Every attempt queues the held records,
/// so a delivery a failed queue left behind is queued when the sender delivers again. The
/// output is the answer frame.
#[derive(Debug, PartialEq)]
pub struct DeliverBatchOperation {
    input: DeliverInput,
    records: Vec<NotificationRecord>,
    state: DeliverState,
}

impl DeliverBatchOperation {
    pub fn new(input: DeliverInput, records: Vec<NotificationRecord>) -> Self {
        Self {
            input,
            records,
            state: DeliverState::Init,
        }
    }

    fn answer(&mut self, answer: NotificationTransportMessage) -> Effects {
        self.state = DeliverState::Done(answer);
        smallvec![]
    }

    fn reject(&mut self, reason: String) -> Effects {
        self.answer(NotificationTransportMessage::Reject(reason))
    }

    /// The realm config with the peer checked against it.
    fn authorized(&self, realm_id: RealmId, value: Option<&[u8]>) -> Result<RealmConfigDocument, String> {
        let peer = self.input.peer;
        let bytes = value.ok_or_else(|| format!("realm `{realm_id}` config unavailable"))?;
        let config = RealmConfigDocument::from_bytes(bytes).map_err(|error| error.to_string())?;
        let eligible = config.sync_eligible_nodes().map_err(|error| error.to_string())?;
        if !eligible.contains(&peer) {
            return Err(format!(
                "notification peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
            ));
        }
        Ok(config)
    }

    fn checked(&mut self, realm_id: RealmId, value: Option<&[u8]>) -> Effects {
        let config = match self.authorized(realm_id, value) {
            Ok(config) => config,
            Err(reason) => return self.reject(reason),
        };
        let records = std::mem::take(&mut self.records);
        let checks = validate_inbound_batch(&records, self.input.now_ms)
            .and_then(|()| verify_node_origin(&records, self.input.peer))
            .and_then(|()| verify_batch_holder(&records, &config, self.input.local));
        if let Err(reason) = checks {
            return self.reject(reason);
        }
        let mut upsert = UpsertInboxOperation::new(records);
        let effects = upsert.start();
        if upsert.is_complete() {
            return self.upserted(upsert, Box::new(config), effects);
        }
        self.state = DeliverState::Upsert(Box::new(upsert), Box::new(config));
        effects
    }

    fn upserted(
        &mut self,
        upsert: UpsertInboxOperation,
        config: Box<RealmConfigDocument>,
        cleanup: Effects,
    ) -> Effects {
        let outcome = match upsert.finalize() {
            Ok(outcome) => outcome,
            Err(error) => {
                return self.after_cleanup(cleanup, NotificationTransportMessage::Reject(error.to_string()));
            }
        };
        if outcome.recipients.is_empty() {
            return self.run_enqueue(&config, outcome.held, outcome.written);
        }
        self.state = DeliverState::Wake {
            config,
            held: outcome.held,
            written: outcome.written,
        };
        smallvec![Effect::Net(NetEffect::WakeInbox(outcome.recipients))]
    }

    fn run_enqueue(&mut self, config: &RealmConfigDocument, held: Vec<NotificationRecord>, written: usize) -> Effects {
        let input = self.input;
        let mut enqueue = EnqueueDeliveriesOperation::new(input.local, Some(config), &held, input.now_ms);
        let effects = enqueue.start();
        if enqueue.is_complete() {
            return self.enqueued(enqueue, written, effects);
        }
        self.state = DeliverState::Enqueue(Box::new(enqueue), written);
        effects
    }

    fn enqueued(&mut self, enqueue: EnqueueDeliveriesOperation, written: usize, cleanup: Effects) -> Effects {
        let answer = match enqueue.finalize() {
            Ok(()) => NotificationTransportMessage::DeliverAck {
                written: written as u32,
            },
            Err(error) => NotificationTransportMessage::Reject(error.to_string()),
        };
        self.after_cleanup(cleanup, answer)
    }

    fn after_cleanup(&mut self, cleanup: Effects, answer: NotificationTransportMessage) -> Effects {
        if cleanup.is_empty() {
            return self.answer(answer);
        }
        self.state = DeliverState::Cleanup {
            left: cleanup.len(),
            answer,
        };
        cleanup
    }
}

impl Operation for DeliverBatchOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn start(&mut self) -> Effects {
        let realm_id = match batch_realm(&self.records) {
            Ok(realm_id) => realm_id,
            Err(reason) => return self.reject(reason),
        };
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return self.reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        self.state = DeliverState::Config { realm_id };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: Key::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (DeliverState::Config { realm_id }, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.checked(realm_id, value.as_deref())
            }
            (DeliverState::Config { .. }, Event::Storage(StorageEvent::Error { error })) => {
                self.reject(error.to_string())
            }
            (DeliverState::Upsert(mut upsert, config), event) => {
                let effects = upsert.step(event);
                if upsert.is_complete() {
                    return self.upserted(*upsert, config, effects);
                }
                self.state = DeliverState::Upsert(upsert, config);
                effects
            }
            (DeliverState::Wake { config, held, written }, Event::Net(NetEvent::InboxWoken)) => {
                self.run_enqueue(&config, held, written)
            }
            (DeliverState::Enqueue(mut enqueue, written), event) => {
                let effects = enqueue.step(event);
                if enqueue.is_complete() {
                    return self.enqueued(*enqueue, written, effects);
                }
                self.state = DeliverState::Enqueue(enqueue, written);
                effects
            }
            (DeliverState::Cleanup { left, answer }, _) if left > 1 => {
                self.state = DeliverState::Cleanup { left: left - 1, answer };
                smallvec![]
            }
            (DeliverState::Cleanup { answer, .. }, _) => self.answer(answer),
            (_, event) => {
                self.state = DeliverState::Failed(DeliverError::UnexpectedEvent(format!("{event:?}")));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeliverState::Done(_) | DeliverState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            DeliverState::Done(answer) => Ok(answer),
            DeliverState::Failed(error) => Err(error),
            _ => Err(DeliverError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            DeliverState::Upsert(upsert, _) => upsert.abort(),
            DeliverState::Enqueue(enqueue, _) => enqueue.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::notification::NotificationClass;

    fn input() -> DeliverInput {
        DeliverInput {
            peer: node_id(2),
            local: node_id(1),
            local_realm: user(1, 2).realm_id,
            now_ms: 1_000,
        }
    }

    fn batch() -> Vec<NotificationRecord> {
        vec![record(user(1, 2), NotificationClass::Direct, 1_000)]
    }

    fn rejected(operation: DeliverBatchOperation) -> String {
        match operation.finalize() {
            Ok(NotificationTransportMessage::Reject(reason)) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn reads_realm_config() {
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        let key = Key::from(input().local_realm.as_bytes().to_vec());
        check_trace(
            &mut deliver,
            &[Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: key.clone(),
                txn_id: None,
            })],
            vec![(
                Event::Storage(StorageEvent::ReadResult { key, value: None }),
                vec![],
            )],
        );
        assert!(rejected(deliver).contains("config unavailable"));
    }

    #[test]
    fn empty_and_foreign_rejected() {
        let mut empty = DeliverBatchOperation::new(input(), Vec::new());
        assert!(empty.start().is_empty());
        assert_eq!(rejected(empty), "empty batch");
        let foreign = DeliverInput {
            local_realm: user(9, 2).realm_id,
            ..input()
        };
        let mut deliver = DeliverBatchOperation::new(foreign, batch());
        assert!(deliver.start().is_empty());
        assert!(rejected(deliver).contains("foreign realm"));
    }

    #[test]
    fn storage_error_rejected() {
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        deliver.start();
        deliver.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert!(!rejected(deliver).is_empty());
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut deliver = DeliverBatchOperation::new(input(), batch());
            deliver.start();
            deliver
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        let refusals = rejects_events(make, sample_events, expected);
        assert!(!refusals.is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        deliver.start();
        check_abort(&mut deliver, &[]);
        rejects_finalize(deliver);
    }
}
