//! Answers a peer's delivery of notification records to the inbox this node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, SubOperationEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use thiserror::Error;

use crate::notifications::checks::{
    batch_realm, peer_config, validate_inbound_batch, verify_batch_holder, verify_node_origin,
};
use crate::notifications::enqueue::{EnqueueDeliveriesOperation, EnqueueError};
use crate::notifications::inbox::UpsertInboxOperation;
use crate::notifications::unexpected;

#[derive(Debug, Error, PartialEq)]
pub enum DeliverError {
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Who delivers and where the records land.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliverInput {
    pub peer: NodeId,
    pub local: NodeId,
    pub local_realm: RealmId,
    pub now_ms: u64,
}

/// The next call of a peer request, or its answer frame.
pub(crate) type AnswerTurn = Result<Next<NotificationTransportMessage>, DeliverError>;

/// Answers the peer with a rejection.
pub(crate) fn reject(reason: String) -> AnswerTurn {
    Ok(Next::Done(NotificationTransportMessage::Reject(reason)))
}

#[derive(Debug, Default, PartialEq)]
enum DeliverState {
    #[default]
    Config,
    Upsert(Box<RealmConfigDocument>),
    Wake {
        config: Box<RealmConfigDocument>,
        held: Vec<NotificationRecord>,
        written: usize,
    },
    Enqueue(Box<EnqueueDeliveriesOperation>, usize),
}

/// Stores a sync-eligible peer's valid batch held here and queues its deliveries on every
/// attempt, so a delivery a failed queue left behind is queued on the next one. A bucket notice
/// must come from the bucket's node. The output is the answer frame.
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
            state: DeliverState::Config,
        }
    }

    /// Checks the peer and the batch against the realm config, then upserts the batch.
    fn checked(&mut self, realm_id: RealmId, value: Option<&[u8]>) -> AnswerTurn {
        let config = match peer_config(self.input.peer, realm_id, value) {
            Ok(config) => config,
            Err(reason) => return reject(reason),
        };
        let records = std::mem::take(&mut self.records);
        let checks = validate_inbound_batch(&records, self.input.now_ms)
            .and_then(|()| verify_node_origin(&records, self.input.peer))
            .and_then(|()| verify_batch_holder(&records, &config, self.input.local));
        if let Err(reason) = checks {
            return reject(reason);
        }
        self.state = DeliverState::Upsert(Box::new(config));
        let upsert = boxed_suboperation(UpsertInboxOperation::new(records), |result| {
            let result = result.map_err(|error| error.to_string());
            Event::SubOperation(SubOperationEvent::InboxUpserted { result })
        });
        Ok(Next::Call(IoCall::Child(upsert)))
    }

    fn enqueue(
        &mut self,
        config: &RealmConfigDocument,
        held: &[NotificationRecord],
        written: usize,
    ) -> AnswerTurn {
        let input = self.input;
        let mut enqueue =
            EnqueueDeliveriesOperation::new(input.local, Some(config), held, input.now_ms);
        let turn = enqueue.first();
        self.state = DeliverState::Enqueue(Box::new(enqueue), written);
        enqueued(turn, written)
    }
}

/// Follows the delivery queue; its failure is a rejection, so the sender delivers again.
fn enqueued(turn: Result<Next<()>, EnqueueError>, written: usize) -> AnswerTurn {
    match turn {
        Ok(Next::Call(call)) => Ok(Next::Call(call)),
        Ok(Next::Wait) => Ok(Next::Wait),
        Ok(Next::Done(())) => Ok(Next::Done(NotificationTransportMessage::DeliverAck {
            written: written as u32,
        })),
        Err(error) => reject(error.to_string()),
    }
}

impl Operation for DeliverBatchOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn first(&mut self) -> AnswerTurn {
        let realm_id = match batch_realm(&self.records) {
            Ok(realm_id) => realm_id,
            Err(reason) => return reject(reason),
        };
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        Ok(Next::Call(read_row(
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> AnswerTurn {
        match (std::mem::take(&mut self.state), result) {
            (DeliverState::Config, result) => {
                let [value] = values::<1>(result)?;
                self.checked(self.input.local_realm, value.as_deref())
            }
            (
                DeliverState::Upsert(config),
                IoResult::Event(Event::SubOperation(SubOperationEvent::InboxUpserted { result })),
            ) => {
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(reason) => return reject(reason),
                };
                if outcome.recipients.is_empty() {
                    return self.enqueue(&config, &outcome.held, outcome.written);
                }
                self.state = DeliverState::Wake {
                    config,
                    held: outcome.held,
                    written: outcome.written,
                };
                let wake = NetEffect::WakeInbox(outcome.recipients);
                Ok(Next::Call(IoCall::Effect(Effect::Net(wake))))
            }
            (
                DeliverState::Wake {
                    config,
                    held,
                    written,
                },
                IoResult::Event(Event::Net(NetEvent::InboxWoken)),
            ) => self.enqueue(&config, &held, written),
            (DeliverState::Enqueue(mut enqueue, written), result) => {
                let turn = enqueue.next(result);
                self.state = DeliverState::Enqueue(enqueue, written);
                enqueued(turn, written)
            }
            (_, other) => Err(unexpected("an inbox upsert or a wake", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::{record, user};
    use aruna_core::structs::execution::notification::{InboxWriteOutcome, NotificationClass};

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

    fn rejected(turn: AnswerTurn) -> String {
        match turn {
            Ok(Next::Done(NotificationTransportMessage::Reject(reason))) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn reads_realm_config() {
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        let key = input().local_realm.as_bytes().to_vec();
        assert_eq!(
            deliver.first(),
            Ok(Next::Call(read_row(REALM_CONFIG_KEYSPACE, key)))
        );
        let missing = deliver.next(IoResult::Values(vec![None]));
        assert!(rejected(missing).contains("config unavailable"));
    }

    #[test]
    fn stray_batches_rejected() {
        let mut empty = DeliverBatchOperation::new(input(), Vec::new());
        assert_eq!(rejected(empty.first()), "empty batch");
        let foreign = DeliverInput {
            local_realm: user(9, 2).realm_id,
            ..input()
        };
        let mut deliver = DeliverBatchOperation::new(foreign, batch());
        assert!(rejected(deliver.first()).contains("foreign realm"));
    }

    /// A failed upsert is a rejection; new recipients are woken before the queue runs.
    #[test]
    fn upsert_outcomes_answered() {
        let config = Box::new(RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3));
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        deliver.state = DeliverState::Upsert(config.clone());
        let failed = SubOperationEvent::InboxUpserted {
            result: Err("conflict".to_string()),
        };
        let failed = deliver.next(IoResult::Event(Event::SubOperation(failed)));
        assert_eq!(rejected(failed), "conflict");

        let mut deliver = DeliverBatchOperation::new(input(), batch());
        deliver.state = DeliverState::Upsert(config);
        let outcome = InboxWriteOutcome {
            written: 1,
            recipients: vec![user(1, 2)],
            ..InboxWriteOutcome::default()
        };
        let stored = SubOperationEvent::InboxUpserted {
            result: Ok(outcome),
        };
        assert_eq!(
            deliver.next(IoResult::Event(Event::SubOperation(stored))),
            Ok(Next::Call(IoCall::Effect(Effect::Net(
                NetEffect::WakeInbox(vec![user(1, 2)])
            ))))
        );
        let woken = deliver.next(IoResult::Event(Event::Net(NetEvent::InboxWoken)));
        assert_eq!(
            woken,
            Ok(Next::Done(NotificationTransportMessage::DeliverAck {
                written: 1
            }))
        );
    }

    #[test]
    fn unexpected_results_fail() {
        let mut deliver = DeliverBatchOperation::new(input(), batch());
        deliver.first().unwrap();
        assert!(matches!(
            deliver.next(IoResult::Done),
            Err(DeliverError::Io(IoError::Unexpected { .. }))
        ));
    }
}
