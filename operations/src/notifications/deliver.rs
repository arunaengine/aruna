//! Answers a peer's delivery of notification records to the inbox this node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, values};
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification::{InboxWriteOutcome, NotificationRecord};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use thiserror::Error;

use crate::notifications::checks::{
    batch_realm, peer_config, validate_inbound_batch, verify_batch_holder, verify_node_origin,
};
use crate::notifications::enqueue::{EnqueueDeliveriesOperation, EnqueueError};
use crate::notifications::inbox::{InboxError, UpsertInboxOperation};
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

#[derive(Clone, Debug, Default, PartialEq)]
enum DeliverState {
    #[default]
    Config,
    Upsert(Box<RealmConfigDocument>),
    Wake {
        config: Box<RealmConfigDocument>,
        held: Vec<NotificationRecord>,
        written: usize,
    },
    Enqueue(usize),
}

/// Stores a sync-eligible peer's valid batch held here and queues its deliveries on every
/// attempt, so a delivery a failed queue left behind is queued on the next one. A bucket notice
/// must come from the bucket's node. The output is the answer frame.
#[derive(Clone, Debug, PartialEq)]
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
        Ok(Next::Call(IoCall::child(UpsertInboxOperation::new(
            records,
        ))))
    }

    fn enqueue(
        &mut self,
        config: &RealmConfigDocument,
        held: &[NotificationRecord],
        written: usize,
    ) -> AnswerTurn {
        let input = self.input;
        let enqueue =
            EnqueueDeliveriesOperation::new(input.local, Some(config), held, input.now_ms);
        self.state = DeliverState::Enqueue(written);
        Ok(Next::Call(IoCall::child(enqueue)))
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
            (DeliverState::Upsert(config), result) => {
                let outcome = match result.child::<InboxWriteOutcome, InboxError>()? {
                    Ok(outcome) => outcome,
                    Err(error) => return reject(error.to_string()),
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
                Ok(Next::Call(IoCall::from(Effect::Net(wake))))
            }
            (
                DeliverState::Wake {
                    config,
                    held,
                    written,
                },
                IoResult::Event(event),
            ) if matches!(*event, Event::Net(NetEvent::InboxWoken)) => {
                self.enqueue(&config, &held, written)
            }
            // A failed queue is a rejection, so the sender delivers again.
            (DeliverState::Enqueue(written), result) => {
                match result.child::<(), EnqueueError>()? {
                    Ok(()) => Ok(Next::Done(NotificationTransportMessage::DeliverAck {
                        written: written as u32,
                    })),
                    Err(error) => reject(error.to_string()),
                }
            }
            (_, other) => Err(unexpected("an inbox upsert or a wake", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::notifications::{peer_input, record, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::notification::NotificationClass;

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
        let mut deliver = DeliverBatchOperation::new(peer_input(), batch());
        let key = peer_input().local_realm.as_bytes().to_vec();
        assert_eq!(
            deliver.first(),
            Ok(Next::Call(read_row(REALM_CONFIG_KEYSPACE, key)))
        );
        let missing = deliver.next(IoResult::Values(vec![None]));
        assert!(rejected(missing).contains("config unavailable"));
    }

    #[test]
    fn stray_batches_rejected() {
        let mut empty = DeliverBatchOperation::new(peer_input(), Vec::new());
        assert_eq!(rejected(empty.first()), "empty batch");
        let foreign = DeliverInput {
            local_realm: user(9, 2).realm_id,
            ..peer_input()
        };
        let mut deliver = DeliverBatchOperation::new(foreign, batch());
        assert!(rejected(deliver.first()).contains("foreign realm"));
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    /// A failed upsert is a rejection; new recipients are woken before the queue runs.
    #[test]
    fn upsert_outcomes_answered() {
        let config = Box::new(RealmConfigDocument::new(user(1, 2).realm_id, Vec::new(), 3));
        let mut deliver = DeliverBatchOperation::new(peer_input(), batch());
        deliver.state = DeliverState::Upsert(config.clone());
        let failed = child::<InboxWriteOutcome, _>(Err(InboxError::Conflict));
        assert_eq!(
            rejected(deliver.next(failed)),
            InboxError::Conflict.to_string()
        );

        let mut deliver = DeliverBatchOperation::new(peer_input(), batch());
        deliver.state = DeliverState::Upsert(config);
        let outcome = InboxWriteOutcome {
            written: 1,
            recipients: vec![user(1, 2)],
            ..InboxWriteOutcome::default()
        };
        assert_eq!(
            deliver.next(child::<_, InboxError>(Ok(outcome))),
            Ok(Next::Call(IoCall::from(Effect::Net(NetEffect::WakeInbox(
                vec![user(1, 2)]
            )))))
        );
        let woken = deliver.next(IoResult::Event(Box::new(Event::Net(NetEvent::InboxWoken))));
        assert!(matches!(woken, Ok(Next::Call(IoCall::Child(_)))));
        let queued = deliver.next(child::<_, EnqueueError>(Ok(())));
        assert_eq!(
            queued,
            Ok(Next::Done(NotificationTransportMessage::DeliverAck {
                written: 1
            }))
        );
    }

    #[test]
    fn unexpected_results_fail() {
        let mut deliver = DeliverBatchOperation::new(peer_input(), batch());
        deliver.first().unwrap();
        assert!(matches!(
            deliver.next(IoResult::Done),
            Err(DeliverError::Io(IoError::Unexpected { .. }))
        ));
    }
}
