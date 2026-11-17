//! Answers a peer's request to delete one watch of a user whose inbox this node holds.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use ulid::Ulid;

use crate::notifications::checks::{peer_config, verify_recipient_holder};
use crate::notifications::deliver::{DeliverError, DeliverInput};
use crate::notifications::watch::digest::InterestOperation;
use crate::notifications::watch::records::watch_delete_replication;
use crate::notifications::watch::store::StoreWatchOperation;

#[derive(Debug, Default, PartialEq)]
enum RemoveState {
    #[default]
    Init,
    Config,
    Store(Box<StoreWatchOperation>),
    Publish(Box<InterestOperation>),
    Done(NotificationTransportMessage),
    Failed(DeliverError),
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
            state: RemoveState::Init,
        }
    }

    fn answer(&mut self, answer: NotificationTransportMessage) -> Effects {
        self.state = RemoveState::Done(answer);
        smallvec![]
    }

    fn reject(&mut self, reason: String) -> Effects {
        self.answer(NotificationTransportMessage::Reject(reason))
    }

    fn checked(&mut self, realm_id: RealmId, value: Option<&[u8]>) -> Effects {
        let input = self.input;
        let checked = peer_config(input.peer, realm_id, value)
            .and_then(|config| verify_recipient_holder(&self.owner, &config, input.local));
        if let Err(reason) = checked {
            return self.reject(reason);
        }
        let replication =
            watch_delete_replication(input.local, self.owner, self.watch_id, input.now_ms);
        let mut store = StoreWatchOperation::delete(self.owner, self.watch_id, Some(replication));
        let effects = store.start();
        self.state = RemoveState::Store(Box::new(store));
        effects
    }

    fn stored(&mut self, store: StoreWatchOperation) -> Effects {
        if let Err(error) = store.finalize() {
            return self.reject(error.to_string());
        }
        let mut publish = InterestOperation::publish();
        let effects = publish.start();
        self.state = RemoveState::Publish(Box::new(publish));
        effects
    }
}

impl EffectOperation for RemoveWatchOperation {
    type Output = NotificationTransportMessage;
    type Error = DeliverError;

    fn start(&mut self) -> Effects {
        let realm_id = self.owner.realm_id;
        if realm_id != self.input.local_realm {
            let peer = self.input.peer;
            return self.reject(format!(
                "notification peer `{peer}` addressed foreign realm `{realm_id}`"
            ));
        }
        self.state = RemoveState::Config;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: Key::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (RemoveState::Config, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.checked(self.owner.realm_id, value.as_deref())
            }
            (RemoveState::Config, Event::Storage(StorageEvent::Error { error })) => {
                self.reject(error.to_string())
            }
            (RemoveState::Store(mut store), event) => {
                let effects = store.step(event);
                if store.is_complete() {
                    return self.stored(*store);
                }
                self.state = RemoveState::Store(store);
                effects
            }
            (RemoveState::Publish(mut publish), event) => {
                let effects = publish.step(event);
                if publish.is_complete() {
                    return self.answer(NotificationTransportMessage::WatchDeleted);
                }
                self.state = RemoveState::Publish(publish);
                effects
            }
            (_, event) => {
                let got = format!("{event:?}");
                self.state = RemoveState::Failed(DeliverError::UnexpectedEvent(got));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoveState::Done(_) | RemoveState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            RemoveState::Done(answer) => Ok(answer),
            RemoveState::Failed(error) => Err(error),
            _ => Err(DeliverError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match &mut self.state {
            RemoveState::Store(store) => store.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use crate::tests::notifications::user;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

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

    fn rejected(operation: RemoveWatchOperation) -> String {
        match operation.finalize() {
            Ok(NotificationTransportMessage::Reject(reason)) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn missing_config_rejected() {
        let key = Key::from(user(1, 2).realm_id.as_bytes().to_vec());
        let mut operation = remove();
        check_trace(
            &mut operation,
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
        assert!(rejected(operation).contains("config unavailable"));
    }

    #[test]
    fn foreign_realm_rejected() {
        let mut operation = RemoveWatchOperation::new(input(), user(9, 2), Ulid::from_parts(5, 5));
        assert!(operation.start().is_empty());
        assert!(rejected(operation).contains("foreign realm"));
        let mut failed = remove();
        failed.start();
        failed.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert!(!rejected(failed).is_empty());
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut operation = remove();
            operation.start();
            operation
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = remove();
        operation.start();
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
