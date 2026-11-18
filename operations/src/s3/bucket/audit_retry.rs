//! Keeps bucket key audit records whose write failed until they are stored: a retry timer per
//! record, persisted when storage allows and stored first at startup. Records are non-secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::key_audit::BucketAuditRecord;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use tracing::warn;

/// Writes of an audit record inside its operation before the record moves to a retry timer.
pub(crate) const AUDIT_ATTEMPTS: u32 = 3;
/// How long a record that could not be stored waits before its next write.
pub const AUDIT_RETRY: Duration = Duration::from_secs(30);

/// The timer that keeps `record` until it is stored.
pub fn retry_timer(record: &BucketAuditRecord) -> TaskKey {
    TaskKey::RecordAudit {
        record: Box::new(record.clone()),
    }
}

/// Arms the retry timer of `record`.
pub fn retry_effect(record: &BucketAuditRecord) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: retry_timer(record),
        after: AUDIT_RETRY,
    })
}

/// Timer rows one resume page reads.
const TIMER_PAGE: usize = 256;

/// Writes audit records in one batch and, for records that must survive a crash, syncs them to
/// disk. A child, so its parent sees a failed write and decides what follows.
#[derive(Debug, PartialEq)]
pub(crate) struct AuditWrite {
    rows: Vec<(String, Key, Value)>,
    sync: bool,
    syncing: bool,
}

impl AuditWrite {
    pub(crate) fn new(record: &BucketAuditRecord, sync: bool) -> Result<Self, ConversionError> {
        Self::batch(std::slice::from_ref(record), sync)
    }

    pub(crate) fn batch(
        records: &[BucketAuditRecord],
        sync: bool,
    ) -> Result<Self, ConversionError> {
        let row = |record: &BucketAuditRecord| {
            let value = record.to_bytes()?.into();
            Ok((
                BUCKET_AUDIT_KEYSPACE.to_string(),
                record.key().into(),
                value,
            ))
        };
        Ok(Self {
            rows: records.iter().map(row).collect::<Result<_, _>>()?,
            sync,
            syncing: false,
        })
    }
}

impl Operation for AuditWrite {
    type Output = ();
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<()>, StorageError> {
        Ok(Next::Call(IoCall::Write(self.rows.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StorageError> {
        if self.syncing {
            return match result.event()? {
                Event::Storage(StorageEvent::SyncAllFinished) => Ok(Next::Done(())),
                other => Err(StorageError::ReadError(format!(
                    "unexpected answer to a sync: {}",
                    other.label()
                ))),
            };
        }
        result.done()?;
        if !self.sync {
            return Ok(Next::Done(()));
        }
        self.syncing = true;
        Ok(Next::Call(Effect::Storage(StorageEffect::SyncAll).into()))
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum AuditError {
    #[error("not an audit record timer")]
    NotAuditTimer,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl From<IoError> for AuditError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Stores the record a `RecordAudit` timer keeps and deletes the timer's persisted row in the
/// same transaction, so a crash at any point keeps either the row or the record. The event id
/// makes a repeated write idempotent.
#[derive(Debug, PartialEq)]
pub struct StoreAuditOperation {
    key: TaskKey,
    timer: Option<Key>,
}

impl StoreAuditOperation {
    pub fn new(key: TaskKey) -> Self {
        Self { key, timer: None }
    }
}

impl Operation for StoreAuditOperation {
    type Output = ();
    type Error = AuditError;

    fn first(&mut self) -> Result<Next<()>, AuditError> {
        let TaskKey::RecordAudit { record } = &self.key else {
            return Err(AuditError::NotAuditTimer);
        };
        let row = (
            BUCKET_AUDIT_KEYSPACE.to_string(),
            record.key().into(),
            record.to_bytes()?.into(),
        );
        self.timer = Some(self.key.storage_key()?);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AuditError> {
        result.done()?;
        Ok(match self.timer.take() {
            Some(timer) => Next::Call(IoCall::Delete(vec![(
                TASK_TIMER_KEYSPACE.to_string(),
                timer,
            )])),
            None => Next::Done(()),
        })
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum ResumeState {
    Scan,
    Store,
}

/// Stores every audit record a persisted timer still keeps, before startup rebuilds the unlock
/// state from the audit trail. Answers the records stored; a record that fails keeps its timer.
#[derive(Debug, PartialEq)]
pub struct ResumeAuditOperation {
    state: ResumeState,
    kept: VecDeque<TaskKey>,
    next: Option<Key>,
    stored: usize,
}

impl Default for ResumeAuditOperation {
    fn default() -> Self {
        Self {
            state: ResumeState::Scan,
            kept: VecDeque::new(),
            next: None,
            stored: 0,
        }
    }
}

type ResumeTurn = Result<Next<usize>, AuditError>;

impl ResumeAuditOperation {
    fn scan(&mut self, after: Option<Key>) -> ResumeTurn {
        self.state = ResumeState::Scan;
        Ok(Next::Call(IoCall::Page {
            space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: TIMER_PAGE,
        }))
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> ResumeTurn {
        for (_, value) in values {
            let Ok(timer) = postcard::from_bytes::<PersistedTaskTimer>(value.as_ref()) else {
                continue;
            };
            if matches!(timer.key, TaskKey::RecordAudit { .. }) {
                self.kept.push_back(timer.key);
            }
        }
        self.next = next;
        self.store_next()
    }

    fn store_next(&mut self) -> ResumeTurn {
        let Some(key) = self.kept.pop_front() else {
            return match self.next.take() {
                Some(next) => self.scan(Some(next)),
                None => Ok(Next::Done(self.stored)),
            };
        };
        self.state = ResumeState::Store;
        Ok(Next::Call(IoCall::child(StoreAuditOperation::new(key))))
    }
}

impl Operation for ResumeAuditOperation {
    type Output = usize;
    type Error = AuditError;

    fn first(&mut self) -> ResumeTurn {
        self.scan(None)
    }

    fn next(&mut self, result: IoResult) -> ResumeTurn {
        match self.state {
            ResumeState::Scan => {
                let (rows, next) = result.page()?;
                self.page_read(rows, next)
            }
            ResumeState::Store => {
                match result.child::<(), AuditError>()? {
                    Ok(()) => self.stored += 1,
                    // The timer stays persisted and retries once the task runtime restores it.
                    Err(error) => warn!(error = %error, "Kept audit record still not stored"),
                }
                self.store_next()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, next_event_id};
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn kept_records_resume() {
        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let record = BucketAuditRecord {
            event_id: next_event_id(5),
            bucket_id: Ulid::from_bytes([4; 16]),
            at_ms: 5,
            action: AuditAction::Lock,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(1),
            session_id: Some(Ulid::from_bytes([7; 16])),
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        };
        // A crash left only the persisted retry timer of a lock record.
        let arm = TaskEffect::ResetTimer {
            key: retry_timer(&record),
            after: AUDIT_RETRY,
        };
        let persist = crate::tasks::task_persistence::persist_task_effect(&storage_handle, &arm);
        persist.await.unwrap();
        assert_eq!(
            drive(ResumeAuditOperation::default(), &context).await,
            Ok(1)
        );
        let read = StorageEffect::Read {
            key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
            key: record.key().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            storage_handle.send_storage_effect(read).await
        else {
            panic!("missing read result");
        };
        let stored = BucketAuditRecord::from_bytes(&value.unwrap()).unwrap();
        assert_eq!(stored, record);
        // The timer is gone, so a second start stores nothing again.
        assert_eq!(
            drive(ResumeAuditOperation::default(), &context).await,
            Ok(0)
        );
    }

    /// Whether `key` holds a row in `key_space`.
    async fn stored(storage: &aruna_storage::StorageHandle, key_space: &str, key: Vec<u8>) -> bool {
        let read = StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        };
        match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected read answer: {other:?}"),
        }
    }

    #[tokio::test]
    async fn retry_survives_crash() {
        use crate::jobs::runtime::JobsRuntime;
        use crate::tasks::incoming::OperationsTaskHandler;
        use aruna_tasks::InboundTaskHandler;
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let handler = Arc::new(OperationsTaskHandler::new(context, JobsRuntime::new()));
        // The handler is stopped after a growing number of steps, as a crash would stop it,
        // and the last run completes.
        for steps in 0..=16 {
            let record = BucketAuditRecord {
                event_id: next_event_id(5),
                bucket_id: Ulid::from_bytes([4; 16]),
                at_ms: 5,
                action: AuditAction::TimedLock,
                actor: None,
                node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
                generation: Some(1),
                session_id: Some(Ulid::from_bytes([7; 16])),
                intent_id: None,
                sequence: None,
                deadline_ms: None,
                reason: None,
                outcome: AuditOutcome::Applied,
            };
            let key = retry_timer(&record);
            let arm = TaskEffect::ResetTimer {
                key: key.clone(),
                after: AUDIT_RETRY,
            };
            let persist =
                crate::tasks::task_persistence::persist_task_effect(&storage_handle, &arm);
            persist.await.unwrap();
            let (fired, timer_key) = (Arc::clone(&handler), key.clone());
            let run = tokio::spawn(async move { fired.handle_timer(timer_key).await });
            if steps < 16 {
                for _ in 0..steps {
                    tokio::task::yield_now().await;
                }
                run.abort();
            }
            let finished = run.await.is_ok();
            let row = key.storage_key().unwrap().to_vec();
            let kept = stored(&storage_handle, TASK_TIMER_KEYSPACE, row).await;
            let written = stored(&storage_handle, BUCKET_AUDIT_KEYSPACE, record.key()).await;
            // Never both and never neither: the row goes only with the stored record.
            assert_ne!(kept, written, "after {steps} steps");
            if finished {
                assert!(written, "a finished run stores the record");
            }
        }
    }

    fn record() -> BucketAuditRecord {
        BucketAuditRecord {
            event_id: Ulid::from_parts(5, 1),
            bucket_id: Ulid::from_bytes([4; 16]),
            at_ms: 5,
            action: AuditAction::Lock,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(1),
            session_id: Some(Ulid::from_bytes([7; 16])),
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        }
    }

    #[test]
    fn stores_in_transaction() {
        // Each `record()` gets a new event id, so the timer and its rows share one record.
        let record = record();
        let key = retry_timer(&record);
        let mut operation = StoreAuditOperation::new(key.clone());
        assert_eq!(operation.transaction(), TransactionMode::Write);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Write(vec![(
                BUCKET_AUDIT_KEYSPACE.to_string(),
                record.key().into(),
                record.to_bytes().unwrap().into(),
            )])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Delete(vec![(
                TASK_TIMER_KEYSPACE.to_string(),
                key.storage_key().unwrap(),
            )])))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn other_timers_refused() {
        let mut operation = StoreAuditOperation::new(TaskKey::DrainCleanupQueue);
        assert_eq!(operation.first(), Err(AuditError::NotAuditTimer));
    }

    #[test]
    fn audit_write_syncs() {
        // A record that must survive a crash is synced before the write counts.
        let mut write = AuditWrite::new(&record(), true).unwrap();
        assert!(matches!(write.first(), Ok(Next::Call(IoCall::Write(_)))));
        assert_eq!(
            write.next(IoResult::Done),
            Ok(Next::Call(Effect::Storage(StorageEffect::SyncAll).into()))
        );
        let synced = IoResult::Event(Box::new(Event::Storage(StorageEvent::SyncAllFinished)));
        assert_eq!(write.next(synced), Ok(Next::Done(())));
        let mut plain = AuditWrite::new(&record(), false).unwrap();
        plain.first().unwrap();
        assert_eq!(plain.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn resume_keeps_failures() {
        // A record that cannot be stored keeps its timer and does not stop the others.
        let record = record();
        let timer = PersistedTaskTimer {
            key: retry_timer(&record),
            due_unix_millis: 0,
        };
        let value = Value::from(postcard::to_allocvec(&timer).unwrap());
        let mut resume = ResumeAuditOperation::default();
        assert!(matches!(
            resume.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let page = IoResult::Page {
            rows: vec![
                (Key::from(vec![1]), value.clone()),
                (Key::from(vec![2]), value),
            ],
            next: None,
        };
        assert!(matches!(
            resume.next(page),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let failed = Err::<(), _>(AuditError::Storage(StorageError::Timeout));
        let turn = resume.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let stored = Ok::<(), AuditError>(());
        assert_eq!(
            resume.next(IoResult::Child(ChildOutcome::new(stored))),
            Ok(Next::Done(1))
        );
    }
}
