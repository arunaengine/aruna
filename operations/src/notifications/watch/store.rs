//! Stores or deletes one watch subscription with its interest marker and replication rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{sync_revision_entry, watch_delete_entry, watch_write_entry};
use aruna_core::structs::execution::notification_watch::{
    WATCH_USER_CAP, WatchSubscription, watch_subscription_prefix,
};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

use crate::notifications::watch::digest::dirty_marker_write;
use crate::notifications::watch::records::{
    WatchReplication, WatchSubscriptionError, watch_subscription_target,
};
use crate::sync::outbox_record::{outbox_write_entry, schedule_drain_effect};

/// What to store.
#[derive(Debug, PartialEq)]
enum Change {
    Create(Box<WatchSubscription>),
    Delete { owner: UserId, watch_id: Ulid },
}

impl Change {
    fn owner(&self) -> UserId {
        match self {
            Change::Create(subscription) => subscription.owner,
            Change::Delete { owner, .. } => *owner,
        }
    }

    fn watch_id(&self) -> Ulid {
        match self {
            Change::Create(subscription) => subscription.watch_id,
            Change::Delete { watch_id, .. } => *watch_id,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Change::Create(_) => "create",
            Change::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Default, PartialEq)]
enum StoreState {
    #[default]
    Init,
    Start,
    /// Create: counts the owner's watches against the per-user cap.
    Count(TxnId),
    /// Delete: reads the row so a missing watch changes nothing.
    Read(TxnId),
    Remove(TxnId),
    Write(TxnId),
    Commit,
    /// Waits for the abort before it ends with `then`, or runs once more.
    Abort(Option<Result<bool, WatchSubscriptionError>>),
    Schedule,
    Done(bool),
    Failed(WatchSubscriptionError),
}

/// Creates or deletes one watch in one transaction with the realm's interest marker and, on the
/// holder, its replication rows, once more after a conflict. The output is whether the store
/// changed: a create under the cap always, a delete only for an existing watch.
#[derive(Debug, PartialEq)]
pub struct StoreWatchOperation {
    change: Change,
    replication: Option<WatchReplication>,
    retried: bool,
    state: StoreState,
}

impl StoreWatchOperation {
    pub fn create(subscription: WatchSubscription, replication: Option<WatchReplication>) -> Self {
        Self::build(Change::Create(Box::new(subscription)), replication)
    }

    pub fn delete(owner: UserId, watch_id: Ulid, replication: Option<WatchReplication>) -> Self {
        Self::build(Change::Delete { owner, watch_id }, replication)
    }

    fn build(change: Change, replication: Option<WatchReplication>) -> Self {
        Self {
            change,
            replication,
            retried: false,
            state: StoreState::Init,
        }
    }

    fn begin(&mut self) -> Effects {
        self.state = StoreState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn txn(&self) -> Option<TxnId> {
        match self.state {
            StoreState::Count(txn_id)
            | StoreState::Read(txn_id)
            | StoreState::Remove(txn_id)
            | StoreState::Write(txn_id) => Some(txn_id),
            _ => None,
        }
    }

    /// Aborts the open transaction, then ends with `then` or runs once more on `None`.
    fn abort_then(
        &mut self,
        txn_id: TxnId,
        then: Option<Result<bool, WatchSubscriptionError>>,
    ) -> Effects {
        self.state = StoreState::Abort(then);
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    /// A conflict runs once more; a second one or any other error ends the store.
    fn storage_failed(&mut self, txn_id: Option<TxnId>, error: StorageError) -> Effects {
        let outcome = match error {
            StorageError::TransactionConflict if !self.retried => {
                self.retried = true;
                None
            }
            StorageError::TransactionConflict => Some(Err(WatchSubscriptionError::Storage(
                format!("watch subscription {} conflicted twice", self.change.name()),
            ))),
            error => Some(Err(WatchSubscriptionError::Storage(error.to_string()))),
        };
        match (txn_id, outcome) {
            (Some(txn_id), outcome) => self.abort_then(txn_id, outcome),
            (None, None) => self.begin(),
            (None, Some(outcome)) => self.finish(outcome),
        }
    }

    fn finish(&mut self, outcome: Result<bool, WatchSubscriptionError>) -> Effects {
        match outcome {
            Ok(changed) if changed && self.replication.is_some() => {
                self.state = StoreState::Schedule;
                smallvec![schedule_drain_effect()]
            }
            Ok(changed) => {
                self.state = StoreState::Done(changed);
                smallvec![]
            }
            Err(error) => {
                self.state = StoreState::Failed(error);
                smallvec![]
            }
        }
    }

    fn fatal(&mut self, txn_id: TxnId, error: impl ToString) -> Effects {
        let error = WatchSubscriptionError::Storage(error.to_string());
        self.abort_then(txn_id, Some(Err(error)))
    }

    /// The rows written with the change: the subscription on create, the interest marker, and
    /// the replication revision and outbox rows on the holder.
    fn writes(&self) -> Result<Vec<(KeySpace, Key, Value)>, String> {
        let mut writes = Vec::with_capacity(4);
        if let Change::Create(subscription) = &self.change {
            writes.push(watch_write_entry(subscription).map_err(|error| error.to_string())?);
        }
        writes.push(dirty_marker_write(self.change.owner().realm_id));
        if let Some(replication) = &self.replication {
            let target = watch_subscription_target(self.change.owner(), self.change.watch_id());
            let revision = sync_revision_entry(&target, &replication.revision)
                .map_err(|error| error.to_string())?;
            let outbox =
                outbox_write_entry(&replication.outbox).map_err(|error| error.to_string())?;
            writes.push(revision);
            writes.push(outbox);
        }
        Ok(writes)
    }

    fn write(&mut self, txn_id: TxnId) -> Effects {
        match self.writes() {
            Ok(writes) => {
                self.state = StoreState::Write(txn_id);
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: Some(txn_id),
                })]
            }
            Err(error) => self.fatal(txn_id, error),
        }
    }

    fn started(&mut self, txn_id: TxnId) -> Effects {
        match &self.change {
            Change::Create(subscription) => {
                let prefix = watch_subscription_prefix(subscription.owner);
                self.state = StoreState::Count(txn_id);
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                    prefix: Some(prefix),
                    start: None,
                    limit: WATCH_USER_CAP.saturating_add(1),
                    txn_id: Some(txn_id),
                })]
            }
            Change::Delete { owner, watch_id } => {
                let (_, key) = watch_delete_entry(*owner, *watch_id);
                self.state = StoreState::Read(txn_id);
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                    key,
                    txn_id: Some(txn_id),
                })]
            }
        }
    }
}

impl Operation for StoreWatchOperation {
    type Output = bool;
    type Error = WatchSubscriptionError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        let txn_id = self.txn();
        match (std::mem::take(&mut self.state), event) {
            (StoreState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.started(txn_id)
            }
            (
                StoreState::Count(txn_id),
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => {
                if values.len() >= WATCH_USER_CAP {
                    return self.abort_then(txn_id, Some(Err(WatchSubscriptionError::CapExceeded)));
                }
                self.write(txn_id)
            }
            (StoreState::Read(txn_id), Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if value.is_none() {
                    return self.abort_then(txn_id, Some(Ok(false)));
                }
                let (key_space, key) =
                    watch_delete_entry(self.change.owner(), self.change.watch_id());
                self.state = StoreState::Remove(txn_id);
                smallvec![Effect::Storage(StorageEffect::Delete {
                    key_space,
                    key,
                    txn_id: Some(txn_id),
                })]
            }
            (StoreState::Remove(txn_id), Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.write(txn_id)
            }
            (StoreState::Write(txn_id), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = StoreState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (StoreState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.finish(Ok(true))
            }
            (StoreState::Abort(then), Event::Storage(_)) => match then {
                Some(outcome) => self.finish(outcome),
                None => self.begin(),
            },
            // The changes are stored; a failed schedule waits for the drain's own re-arm.
            (StoreState::Schedule, Event::Task(_)) => {
                self.state = StoreState::Done(true);
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.storage_failed(txn_id, error)
            }
            (_, other) => {
                let error = format!("unexpected storage event: {other:?}");
                match txn_id {
                    Some(txn_id) => self.fatal(txn_id, error),
                    None => self.finish(Err(WatchSubscriptionError::Storage(error))),
                }
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StoreState::Done(_) | StoreState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            StoreState::Done(changed) => Ok(changed),
            StoreState::Failed(error) => Err(error),
            _ => Err(WatchSubscriptionError::Storage(
                "the watch store did not finish".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.txn() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::notifications::user;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    const TXN: TxnId = TxnId::from_parts(4, 4);

    fn subscription() -> WatchSubscription {
        let mask = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
        WatchSubscription::new(user(1, 2), "meta/x".to_string(), mask, 1_000)
    }

    fn conflict() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    }

    #[test]
    fn missing_delete_trace() {
        let owner = user(1, 2);
        let watch_id = Ulid::from_parts(9, 9);
        let (_, key) = watch_delete_entry(owner, watch_id);
        let mut store = StoreWatchOperation::delete(owner, watch_id, None);
        check_trace(
            &mut store,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::Read {
                        key_space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                        key: key.clone(),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::ReadResult { key, value: None }),
                    vec![Effect::Storage(StorageEffect::AbortTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::TransactionAborted { txn_id: TXN }),
                    vec![],
                ),
            ],
        );
        assert_eq!(store.finalize(), Ok(false));
    }

    #[test]
    fn cap_reached_refused() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        store.start();
        store.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let rows = (0..WATCH_USER_CAP)
            .map(|at| (Key::from(vec![at as u8]), Value::from(vec![1])))
            .collect();
        let abort = store.step(Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: None,
        }));
        assert_eq!(
            abort.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN
            })]
        );
        store.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        assert_eq!(store.finalize(), Err(WatchSubscriptionError::CapExceeded));
    }

    #[test]
    fn conflict_runs_twice() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        store.start();
        store.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        store.step(conflict());
        let again = store.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        assert_eq!(
            again.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        store.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        store.step(conflict());
        store.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        assert!(matches!(
            store.finalize(),
            Err(WatchSubscriptionError::Storage(_))
        ));
    }

    #[test]
    fn create_writes_rows() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        store.start();
        store.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let write = store.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = write.as_slice()
        else {
            panic!("one batch write: {write:?}");
        };
        assert_eq!((writes.len(), *txn_id), (2, Some(TXN)));
        let commit = store.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            commit.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: TXN
            })]
        );
        store.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TXN,
        }));
        assert_eq!(store.finalize(), Ok(true));
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut store = StoreWatchOperation::create(subscription(), None);
            store.start();
            store
        };
        let expected = |event: &Event| {
            matches!(
                event,
                Event::Storage(
                    StorageEvent::TransactionStarted { .. } | StorageEvent::Error { .. }
                )
            )
        };
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_per_phase() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        store.start();
        check_abort(&mut store, &[]);
        store.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        check_abort(
            &mut store,
            &[Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN,
            })],
        );
        rejects_finalize(store);
    }
}
