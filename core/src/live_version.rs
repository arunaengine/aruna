//! Queues continuous replication of one committed version: stores its live obligation once,
//! then wakes the replication drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use tracing::warn;
use ulid::Ulid;

use crate::NodeId;
use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::errors::StorageError;
use crate::events::Event;
use crate::keyspaces::REPLICATION_OBLIGATION_KEYSPACE;
use crate::operation::{Operation, TransactionMode};
use crate::rows::read_row;
use crate::structs::identity::auth::AuthContext;
use crate::structs::storage::replication_queue::{
    LiveObligationRecord, LiveReplicationContinuation, ReferenceAdvance, live_obligation_entry,
    schedule_blob_drain,
};
use crate::task::TaskEvent;
use crate::types::{Key, Value};

/// Whether the drain timer was set; the queued work is durable either way.
pub fn drain_scheduled(result: IoResult) -> bool {
    match result.event() {
        Ok(Event::Task(TaskEvent::TimerScheduled { .. })) => true,
        Ok(Event::Task(TaskEvent::Error { .. })) => false,
        other => {
            warn!(event = ?other, "Queued replication work persisted but drain scheduling returned an unexpected event");
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVersionInput {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
}

/// Stores a live obligation in one transaction, unless a readable one is already there.
#[derive(Debug, PartialEq)]
struct ObligationWrite {
    key: Key,
    value: Value,
    writing: bool,
}

impl Operation for ObligationWrite {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let read = read_row(REPLICATION_OBLIGATION_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let stored = result.value()?;
        if stored.is_some_and(|value| LiveObligationRecord::from_bytes(&value).is_ok()) {
            return Ok(Next::Done(()));
        }
        self.writing = true;
        Ok(Next::Call(IoCall::Write(vec![(
            REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            self.key.clone(),
            self.value.clone(),
        )])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LiveVersionState {
    WriteObligation,
    ScheduleDrain,
}

/// Queues one committed version or delete marker for continuous replication. Answers whether
/// the drain timer was set.
#[derive(Debug, PartialEq)]
pub struct LiveVersionOperation {
    input: LiveVersionInput,
    reference_advance: Option<ReferenceAdvance>,
    state: LiveVersionState,
}

type Turn = Result<Next<bool>, IoError>;

impl LiveVersionOperation {
    pub fn new(input: LiveVersionInput) -> Self {
        Self {
            input,
            reference_advance: None,
            state: LiveVersionState::WriteObligation,
        }
    }

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    fn seed_record(&self) -> LiveObligationRecord {
        let mut record = LiveObligationRecord::new(
            self.input.local_node_id,
            self.input.auth_context.clone(),
            self.input.bucket.clone(),
            self.input.key.clone(),
            self.input.version_id,
            self.input.delete_marker,
        );
        if let Some(advance) = self.reference_advance {
            record = record.with_reference_advance(advance);
        }
        record.continuation = Some(LiveReplicationContinuation::default());
        record
    }

    /// A lost conflict means another writer stored the obligation, so the drain still runs.
    fn obligation_written(&mut self, written: Result<(), IoError>) -> Turn {
        match written {
            Err(error) if !Self::is_conflict(&error) => return Err(error),
            _ => {}
        }
        self.state = LiveVersionState::ScheduleDrain;
        Ok(Next::Call(schedule_blob_drain().into()))
    }
}

impl Operation for LiveVersionOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        let (_, key, value) = live_obligation_entry(&self.seed_record())
            .map_err(|error| IoError::Codec(error.to_string()))?;
        let write = ObligationWrite {
            key,
            value,
            writing: false,
        };
        Ok(Next::Call(IoCall::child(write)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            LiveVersionState::WriteObligation => {
                self.obligation_written(result.child::<(), IoError>()?)
            }
            LiveVersionState::ScheduleDrain => Ok(Next::Done(drain_scheduled(result))),
        }
    }

    fn is_conflict(error: &IoError) -> bool {
        matches!(
            error,
            IoError::Conflict | IoError::Storage(StorageError::TransactionConflict)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::UserId;
    use crate::calls::ChildOutcome;
    use crate::structs::identity::realm::RealmId;
    use crate::task::TaskKey;

    fn live_operation() -> LiveVersionOperation {
        let realm = RealmId::from_bytes([9u8; 32]);
        let user = UserId::local(Ulid::from_parts(1, 9), realm);
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
            auth_context: AuthContext::new(user, realm),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::from_parts(3, 3),
            delete_marker: false,
        })
    }

    fn live_written(result: Result<(), IoError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn timer_set() -> IoResult {
        let key = TaskKey::DrainReplicationQueue;
        let after = Duration::ZERO;
        IoResult::Event(Box::new(Event::Task(TaskEvent::TimerScheduled { key, after })))
    }

    #[test]
    fn obligation_write_once() {
        // The obligation is written in one transaction, only when no readable one exists.
        let record = live_operation().seed_record();
        let (space, key, value) = live_obligation_entry(&record).unwrap();
        let write = || ObligationWrite {
            key: key.clone(),
            value: value.clone(),
            writing: false,
        };
        let mut missing = write();
        assert_eq!(missing.transaction(), TransactionMode::Write);
        assert_eq!(
            missing.first(),
            Ok(Next::Call(read_row(&space, key.clone())))
        );
        assert_eq!(
            missing.next(IoResult::Values(vec![None])),
            Ok(Next::Call(IoCall::Write(vec![(
                space.clone(),
                key.clone(),
                value.clone()
            )])))
        );
        assert_eq!(missing.next(IoResult::Done), Ok(Next::Done(())));

        let mut stored = write();
        stored.first().unwrap();
        let row = IoResult::Values(vec![Some(value.clone())]);
        assert_eq!(stored.next(row), Ok(Next::Done(())));

        let mut unreadable = write();
        unreadable.first().unwrap();
        let row = IoResult::Values(vec![Some(vec![0xff].into())]);
        assert!(matches!(
            unreadable.next(row),
            Ok(Next::Call(IoCall::Write(_)))
        ));
    }

    #[test]
    fn live_conflict_drains() {
        // A conflict means another writer stored the obligation; other failures end the run.
        let mut operation = live_operation();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let drain = Ok(Next::Call(IoCall::from(schedule_blob_drain())));
        assert_eq!(operation.next(live_written(Err(IoError::Conflict))), drain);
        assert_eq!(operation.next(timer_set()), Ok(Next::Done(true)));

        let mut failing = live_operation();
        failing.first().unwrap();
        let error = IoError::Storage(StorageError::Timeout);
        let failed = failing.next(live_written(Err(error.clone())));
        assert_eq!(failed, Err(error));
    }
}
