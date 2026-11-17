//! Stores or deletes one watch subscription with its interest marker and replication rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WATCH_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{sync_revision_entry, watch_delete_entry, watch_write_entry};
use aruna_core::structs::execution::notification_watch::{
    WATCH_USER_CAP, WatchSubscription, watch_subscription_prefix,
};
use aruna_core::types::{Key, KeySpace, Value};
use ulid::Ulid;

use crate::notifications::unexpected;
use crate::notifications::watch::digest::dirty_marker_write;
use crate::notifications::watch::records::{
    WatchReplication, WatchSubscriptionError, watch_subscription_target,
};
use crate::sync::outbox_record::{outbox_write_entry, schedule_drain_effect};

/// What to store.
#[derive(Clone, Debug, PartialEq)]
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

    /// The rows written with the change: the subscription on create, the interest marker,
    /// and the replication revision and outbox rows on the holder.
    fn writes(
        &self,
        replication: Option<&WatchReplication>,
    ) -> Result<Vec<(KeySpace, Key, Value)>, String> {
        let mut writes = Vec::with_capacity(4);
        if let Change::Create(subscription) = self {
            writes.push(watch_write_entry(subscription).map_err(|error| error.to_string())?);
        }
        writes.push(dirty_marker_write(self.owner().realm_id));
        if let Some(replication) = replication {
            let target = watch_subscription_target(self.owner(), self.watch_id());
            let revision = sync_revision_entry(&target, &replication.revision)
                .map_err(|error| error.to_string())?;
            let outbox =
                outbox_write_entry(&replication.outbox).map_err(|error| error.to_string())?;
            writes.push(revision);
            writes.push(outbox);
        }
        Ok(writes)
    }
}

#[derive(Debug, Default, PartialEq)]
enum RowsState {
    /// Create: counts the owner's watches against the per-user cap.
    #[default]
    Count,
    /// Delete: reads the row so a missing watch changes nothing.
    Read,
    Remove,
    Write,
}

/// The rows of one change in one transaction; the output is whether anything changed.
#[derive(Debug, PartialEq)]
struct WatchRowsOperation {
    change: Change,
    replication: Option<WatchReplication>,
    state: RowsState,
}

impl WatchRowsOperation {
    fn write(&mut self) -> Result<Next<bool>, WatchSubscriptionError> {
        let writes = self
            .change
            .writes(self.replication.as_ref())
            .map_err(WatchSubscriptionError::Storage)?;
        self.state = RowsState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for WatchRowsOperation {
    type Output = bool;
    type Error = WatchSubscriptionError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let call = match &self.change {
            Change::Create(subscription) => {
                self.state = RowsState::Count;
                IoCall::Page {
                    space: WATCH_SUBSCRIPTIONS_KEYSPACE.to_string(),
                    prefix: watch_subscription_prefix(subscription.owner),
                    after: None,
                    limit: WATCH_USER_CAP.saturating_add(1),
                }
            }
            Change::Delete { owner, watch_id } => {
                let (key_space, key) = watch_delete_entry(*owner, *watch_id);
                self.state = RowsState::Read;
                IoCall::Read(vec![(key_space, key)])
            }
        };
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match (std::mem::take(&mut self.state), result) {
            (RowsState::Count, IoResult::Page { rows, .. }) => {
                if rows.len() >= WATCH_USER_CAP {
                    return Err(WatchSubscriptionError::CapExceeded);
                }
                self.write()
            }
            (RowsState::Read, IoResult::Values(values)) => {
                if values.into_iter().flatten().next().is_none() {
                    return Ok(Next::Done(false));
                }
                let row = watch_delete_entry(self.change.owner(), self.change.watch_id());
                self.state = RowsState::Remove;
                Ok(Next::Call(IoCall::Delete(vec![row])))
            }
            (RowsState::Remove, IoResult::Done) => self.write(),
            (RowsState::Write, IoResult::Done) => Ok(Next::Done(true)),
            (_, other) => Err(unexpected("a watch row result", &other).into()),
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// Creates or deletes one watch in one transaction with the realm's interest marker and, on the
/// holder, its replication rows, once more after a conflict. The output is whether the store
/// changed: a create under the cap always, a delete only for an existing watch.
#[derive(Debug, PartialEq)]
pub struct StoreWatchOperation {
    change: Change,
    replication: Option<WatchReplication>,
    retried: bool,
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
        }
    }

    /// The rows run as a child, so the drain is scheduled only after their commit.
    fn rows(&self) -> Next<bool> {
        let rows = WatchRowsOperation {
            change: self.change.clone(),
            replication: self.replication.clone(),
            state: RowsState::Count,
        };
        Next::Call(IoCall::child(rows))
    }

    /// A first conflict runs the rows once more.
    fn failed(
        &mut self,
        error: WatchSubscriptionError,
    ) -> Result<Next<bool>, WatchSubscriptionError> {
        if error != WatchSubscriptionError::from(IoError::Conflict) {
            return Err(error);
        }
        if !self.retried {
            self.retried = true;
            return Ok(self.rows());
        }
        let name = self.change.name();
        let error = format!("watch subscription {name} conflicted twice");
        Err(WatchSubscriptionError::Storage(error))
    }
}

impl Operation for StoreWatchOperation {
    type Output = bool;
    type Error = WatchSubscriptionError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(self.rows())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match result.child::<bool, WatchSubscriptionError>()? {
            // The changes are stored; a failed schedule waits for the drain's own re-arm.
            Ok(true) if self.replication.is_some() => Ok(Next::Then {
                output: true,
                calls: vec![IoCall::from(schedule_drain_effect())],
            }),
            Ok(changed) => Ok(Next::Done(changed)),
            Err(error) => self.failed(error),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::notifications::user;
    use aruna_core::structs::execution::notification_watch::{WatchEventKind, WatchEventMask};

    fn subscription() -> WatchSubscription {
        let mask = WatchEventMask::from_kinds([WatchEventKind::MetadataCreated]);
        WatchSubscription::new(user(1, 2), "meta/x".to_string(), mask, 1_000)
    }

    fn rows(change: Change) -> WatchRowsOperation {
        WatchRowsOperation {
            change,
            replication: None,
            state: RowsState::Count,
        }
    }

    fn stored(result: Result<bool, WatchSubscriptionError>) -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    #[test]
    fn missing_delete_calls() {
        let owner = user(1, 2);
        let watch_id = Ulid::from_parts(9, 9);
        let mut rows = rows(Change::Delete { owner, watch_id });
        assert!(rows.transaction());
        let (space, key) = watch_delete_entry(owner, watch_id);
        assert_eq!(
            rows.first(),
            Ok(Next::Call(IoCall::Read(vec![(space, key)])))
        );
        assert_eq!(
            rows.next(IoResult::Values(vec![None])),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn cap_reached_refused() {
        let mut rows = rows(Change::Create(Box::new(subscription())));
        rows.first().unwrap();
        let full = (0..WATCH_USER_CAP)
            .map(|at| (Key::from(vec![at as u8]), Value::from(vec![1])))
            .collect();
        let page = IoResult::Page {
            rows: full,
            next: None,
        };
        assert_eq!(rows.next(page), Err(WatchSubscriptionError::CapExceeded));
    }

    #[test]
    fn create_writes_rows() {
        let mut rows = rows(Change::Create(Box::new(subscription())));
        rows.first().unwrap();
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let Ok(Next::Call(IoCall::Write(writes))) = rows.next(empty) else {
            panic!("one write");
        };
        assert_eq!(writes.len(), 2);
        assert_eq!(rows.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn conflict_runs_twice() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        assert!(matches!(store.first(), Ok(Next::Call(IoCall::Child(_)))));
        let conflict = || stored(Err(IoError::Conflict.into()));
        assert!(matches!(
            store.next(conflict()),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            store.next(conflict()),
            Err(WatchSubscriptionError::Storage(_))
        ));
        let mut capped = StoreWatchOperation::create(subscription(), None);
        capped.first().unwrap();
        let full = stored(Err(WatchSubscriptionError::CapExceeded));
        assert_eq!(capped.next(full), Err(WatchSubscriptionError::CapExceeded));
    }

    #[test]
    fn holder_schedules_drain() {
        let mut store = StoreWatchOperation::create(subscription(), None);
        store.first().unwrap();
        assert_eq!(store.next(stored(Ok(true))), Ok(Next::Done(true)));
        let mut holder = StoreWatchOperation::create(subscription(), None);
        holder.replication = Some(
            crate::notifications::watch::records::watch_delete_replication(
                crate::tests::locations::node_id(1),
                user(1, 2),
                Ulid::from_parts(9, 9),
                1_000,
            ),
        );
        holder.first().unwrap();
        assert_eq!(
            holder.next(stored(Ok(true))),
            Ok(Next::Then {
                output: true,
                calls: vec![IoCall::from(schedule_drain_effect())],
            })
        );
    }
}
