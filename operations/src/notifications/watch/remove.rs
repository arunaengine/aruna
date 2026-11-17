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
    Store,
    Publish,
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
        let store = StoreWatchOperation::delete(self.owner, self.watch_id, Some(replication));
        self.state = RemoveState::Store;
        Ok(Next::Call(IoCall::child(store)))
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
            // A failed store is a rejection.
            (RemoveState::Store, result) => {
                if let Err(error) = result.child::<bool, WatchSubscriptionError>()? {
                    return reject(error.to_string());
                }
                self.state = RemoveState::Publish;
                Ok(Next::Call(IoCall::child(InterestOperation::publish())))
            }
            // The watch is deleted whatever the publish request answers.
            (RemoveState::Publish, _) => Ok(Next::Done(NotificationTransportMessage::WatchDeleted)),
            (RemoveState::Config, other) => Err(unexpected("IoResult::Values", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::notifications::watch::digest::InterestError;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;
    use aruna_core::calls::ChildOutcome;

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
        operation.state = RemoveState::Store;
        let stored = IoResult::Child(ChildOutcome::new(Ok::<_, WatchSubscriptionError>(false)));
        assert!(matches!(
            operation.next(stored),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let published = IoResult::Child(ChildOutcome::new(Ok::<_, InterestError>(true)));
        assert_eq!(
            operation.next(published),
            Ok(Next::Done(NotificationTransportMessage::WatchDeleted))
        );
        let mut failed = remove();
        failed.state = RemoveState::Store;
        let refused = Err::<bool, _>(WatchSubscriptionError::CapExceeded);
        let refused = failed.next(IoResult::Child(ChildOutcome::new(refused)));
        assert_eq!(
            rejected(refused),
            WatchSubscriptionError::CapExceeded.to_string()
        );
    }
}
