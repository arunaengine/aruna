//! Answers a peer's request to delete one watch of a user whose inbox this node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::types::Key;
use ulid::Ulid;

use crate::notifications::checks::{peer_config, verify_recipient_holder};
use crate::notifications::deliver::{AnswerTurn, DeliverError, DeliverInput, reject};
use crate::notifications::unexpected;
use crate::notifications::watch::digest::InterestOperation;
use crate::notifications::watch::records::{WatchSubscriptionError, watch_delete_replication};
use crate::notifications::watch::store::StoreWatchOperation;

#[derive(Debug, Default, PartialEq)]
enum RemoveState {
    #[default]
    Config,
    Store(Box<StoreWatchOperation>),
    Publish(Box<InterestOperation>),
}

/// Deletes the watch for a sync-eligible peer when this node holds the owner's inbox, writes
/// its replication rows and requests the interest publish. A missing watch is still deleted.
/// The output is the answer frame.
#[derive(Debug, PartialEq)]
pub struct RemoveWatchOperation {
    input: DeliverInput,
    owner: UserId,
    watch_id: Ulid,
    state: RemoveState,
}

impl RemoveWatchOperation {
    pub fn new(input: DeliverInput, owner: UserId, watch_id: Ulid) -> Self {
        Self {
            input,
            owner,
            watch_id,
            state: RemoveState::Config,
        }
    }

    fn checked(&mut self, value: Option<&[u8]>) -> AnswerTurn {
        let input = self.input;
        let checked = peer_config(input.peer, self.owner.realm_id, value)
            .and_then(|config| verify_recipient_holder(&self.owner, &config, input.local));
        if let Err(reason) = checked {
            return reject(reason);
        }
        let replication =
            watch_delete_replication(input.local, self.owner, self.watch_id, input.now_ms);
        let mut store = StoreWatchOperation::delete(self.owner, self.watch_id, Some(replication));
        let turn = store.first();
        self.stored(store, turn)
    }

    /// Follows the store; a failed store is a rejection.
    fn stored(
        &mut self,
        store: StoreWatchOperation,
        turn: Result<Next<bool>, WatchSubscriptionError>,
    ) -> AnswerTurn {
        match turn {
            Ok(Next::Call(call)) => {
                self.state = RemoveState::Store(Box::new(store));
                Ok(Next::Call(call))
            }
            Ok(Next::Wait) => {
                self.state = RemoveState::Store(Box::new(store));
                Ok(Next::Wait)
            }
            Ok(Next::Done(_)) => {
                let mut publish = InterestOperation::publish();
                let turn = publish.first();
                self.published(publish, turn.map_err(|_| ()))
            }
            Err(error) => reject(error.to_string()),
        }
    }

    /// Follows the publish request; the watch is deleted whatever it answers.
    fn published(
        &mut self,
        publish: InterestOperation,
        turn: Result<Next<bool>, ()>,
    ) -> AnswerTurn {
        match turn {
            Ok(Next::Call(call)) => {
                self.state = RemoveState::Publish(Box::new(publish));
                Ok(Next::Call(call))
            }
            _ => Ok(Next::Done(NotificationTransportMessage::WatchDeleted)),
        }
    }
}

impl Operation for RemoveWatchOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn first(&mut self) -> AnswerTurn {
        let realm_id = self.owner.realm_id;
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        let key = Key::from(realm_id.as_bytes().to_vec());
        Ok(Next::Call(IoCall::Read(vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> AnswerTurn {
        match (std::mem::take(&mut self.state), result) {
            (RemoveState::Config, IoResult::Values(values)) => {
                let value = values.into_iter().flatten().next();
                self.checked(value.as_deref())
            }
            (RemoveState::Store(mut store), result) => {
                let turn = store.next(result);
                self.stored(*store, turn)
            }
            (RemoveState::Publish(mut publish), result) => {
                let turn = publish.next(result).map_err(|_| ());
                self.published(*publish, turn)
            }
            (RemoveState::Config, other) => Err(unexpected("IoResult::Values", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;
    use aruna_core::events::{Event, SubOperationEvent};
    use aruna_core::task::{TaskEvent, TaskKey};

    fn input() -> DeliverInput {
        DeliverInput {
            peer: node_id(2),
            local: node_id(1),
            local_realm: user(1, 2).realm_id,
            now_ms: 1_000,
        }
    }

    fn remove() -> RemoveWatchOperation {
        RemoveWatchOperation::new(input(), user(1, 2), Ulid::from_parts(5, 5))
    }

    fn rejected(turn: AnswerTurn) -> String {
        match turn {
            Ok(Next::Done(NotificationTransportMessage::Reject(reason))) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn missing_config_rejected() {
        let key = Key::from(user(1, 2).realm_id.as_bytes().to_vec());
        let mut operation = remove();
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(vec![(
                REALM_CONFIG_KEYSPACE.to_string(),
                key
            )])))
        );
        let turn = operation.next(IoResult::Values(vec![None]));
        assert!(rejected(turn).contains("config unavailable"));
    }

    #[test]
    fn foreign_realm_rejected() {
        let mut operation = RemoveWatchOperation::new(input(), user(9, 2), Ulid::from_parts(5, 5));
        assert!(rejected(operation.first()).contains("foreign realm"));
        let mut failed = remove();
        failed.first().unwrap();
        assert!(failed.next(IoResult::Done).is_err());
    }

    #[test]
    fn stored_then_published() {
        let mut operation = remove();
        let store = StoreWatchOperation::delete(user(1, 2), Ulid::from_parts(5, 5), None);
        operation.state = RemoveState::Store(Box::new(store));
        let stored = Event::SubOperation(SubOperationEvent::WatchStored { result: Ok(false) });
        assert!(matches!(
            operation.next(IoResult::Event(stored)),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let scheduled = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishWatchInterest,
            after: std::time::Duration::from_secs(2),
        });
        assert_eq!(
            operation.next(IoResult::Event(scheduled)),
            Ok(Next::Done(NotificationTransportMessage::WatchDeleted))
        );
    }
}
