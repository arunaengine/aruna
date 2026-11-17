//! Keeps bucket key audit records whose write failed until they are stored: a retry timer per
//! record, persisted when storage allows and stored first at startup. Records are non-secret.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, TASK_TIMER_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::key_audit::BucketAuditRecord;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
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

#[derive(Debug, Error, PartialEq)]
pub enum AuditError {
    #[error("not an audit record timer")]
    NotAuditTimer,
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected event while storing an audit record: {0}")]
    UnexpectedEvent(String),
    #[error("the audit record store did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
enum StoreState {
    Init,
    StartTransaction,
    WriteRecord,
    DeleteTimer,
    Commit,
    Finish,
    Error,
}

/// Stores the record a `RecordAudit` timer keeps and deletes the timer's persisted row in the
/// same transaction, so a crash at any point keeps either the row or the record. The event id
/// makes a repeated write idempotent.
#[derive(Debug, PartialEq)]
pub struct StoreAuditOperation {
    key: TaskKey,
    rows: Option<(Key, Vec<u8>, Key)>,
    txn_id: Option<TxnId>,
    state: StoreState,
    error: Option<AuditError>,
}

impl StoreAuditOperation {
    pub fn new(key: TaskKey) -> Self {
        Self {
            key,
            rows: None,
            txn_id: None,
            state: StoreState::Init,
            error: None,
        }
    }

    fn rows(key: &TaskKey) -> Result<(Key, Vec<u8>, Key), AuditError> {
        let TaskKey::RecordAudit { record } = key else {
            return Err(AuditError::NotAuditTimer);
        };
        let timer = key.storage_key()?;
        Ok((record.key().into(), record.to_bytes()?, timer))
    }

    fn fail(&mut self, error: AuditError) -> Effects {
        self.state = StoreState::Error;
        self.error = Some(error);
        self.abort()
    }

    fn write(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        let Some((key, value, _)) = &self.rows else {
            return self.fail(AuditError::NotFinished);
        };
        self.state = StoreState::WriteRecord;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
            key: key.clone(),
            value: value.clone().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn delete_timer(&mut self) -> Effects {
        let (Some((_, _, timer)), Some(txn_id)) = (&self.rows, self.txn_id) else {
            return self.fail(AuditError::NotFinished);
        };
        self.state = StoreState::DeleteTimer;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: timer.clone(),
            txn_id: Some(txn_id),
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(AuditError::NotFinished);
        };
        self.state = StoreState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for StoreAuditOperation {
    type Output = ();
    type Error = AuditError;

    fn start(&mut self) -> Effects {
        match Self::rows(&self.key) {
            Ok(rows) => self.rows = Some(rows),
            Err(error) => return self.fail(error),
        }
        self.state = StoreState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            // A failed commit closed the transaction, so nothing is left to abort.
            (StoreState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.txn_id = None;
                self.fail(error.into())
            }
            (
                StoreState::StartTransaction | StoreState::WriteRecord | StoreState::DeleteTimer,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error.into()),
            (
                StoreState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.write(txn_id),
            (StoreState::WriteRecord, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.delete_timer()
            }
            (StoreState::DeleteTimer, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.commit()
            }
            (StoreState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = StoreState::Finish;
                smallvec![]
            }
            (_, event) => self.fail(AuditError::UnexpectedEvent(format!("{event:?}"))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StoreState::Finish | StoreState::Error)
    }

    fn finalize(self) -> Result<(), AuditError> {
        match (self.state, self.error) {
            (StoreState::Finish, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(AuditError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        aruna_core::effects::abort_effects(&mut self.txn_id)
    }
}

#[derive(Debug, PartialEq)]
enum ResumeState {
    Init,
    Scan,
    Store,
    Finish,
    Error,
}

/// Stores every audit record a persisted timer still keeps, before startup rebuilds the unlock
/// state from the audit trail. Answers the records stored; a record that fails keeps its timer.
#[derive(Debug, PartialEq)]
pub struct ResumeAuditOperation {
    state: ResumeState,
    kept: VecDeque<TaskKey>,
    next: Option<Key>,
    stored: usize,
    error: Option<AuditError>,
}

impl Default for ResumeAuditOperation {
    fn default() -> Self {
        Self {
            state: ResumeState::Init,
            kept: VecDeque::new(),
            next: None,
            stored: 0,
            error: None,
        }
    }
}

impl ResumeAuditOperation {
    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.state = ResumeState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: TIMER_PAGE,
            txn_id: None,
        })]
    }

    fn page_read(&mut self, values: Vec<(Key, Key)>, next: Option<Key>) -> Effects {
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

    fn store_next(&mut self) -> Effects {
        let Some(key) = self.kept.pop_front() else {
            return match self.next.take() {
                Some(next) => self.scan(Some(next)),
                None => {
                    self.state = ResumeState::Finish;
                    smallvec![]
                }
            };
        };
        self.state = ResumeState::Store;
        let store = StoreAuditOperation::new(key);
        smallvec![Effect::SubOperation(boxed_suboperation(store, |result| {
            Event::SubOperation(SubOperationEvent::AuditStored {
                result: result.map_err(|error| error.to_string()),
            })
        }))]
    }

    fn fail(&mut self, error: AuditError) -> Effects {
        self.state = ResumeState::Error;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for ResumeAuditOperation {
    type Output = usize;
    type Error = AuditError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (ResumeState::Scan, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(error.into())
            }
            (
                ResumeState::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after),
            (
                ResumeState::Store,
                Event::SubOperation(SubOperationEvent::AuditStored { result }),
            ) => {
                match result {
                    Ok(()) => self.stored += 1,
                    // The timer stays persisted and retries once the task runtime restores it.
                    Err(error) => warn!(error = %error, "Kept audit record still not stored"),
                }
                self.store_next()
            }
            (_, event) => self.fail(AuditError::UnexpectedEvent(format!("{event:?}"))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ResumeState::Finish | ResumeState::Error)
    }

    fn finalize(self) -> Result<usize, AuditError> {
        match (self.state, self.error) {
            (ResumeState::Finish, _) => Ok(self.stored),
            (_, Some(error)) => Err(error),
            _ => Err(AuditError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
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
            task_handle: None,
            compute_handle: None,
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
            task_handle: None,
            compute_handle: None,
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
        }
    }

    fn txn() -> TxnId {
        Ulid::from_parts(9, 9)
    }

    fn storage_error() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    }

    /// A store operation advanced through `events` after start.
    fn store_after(events: Vec<Event>) -> StoreAuditOperation {
        let mut operation = StoreAuditOperation::new(retry_timer(&record()));
        operation.start();
        for event in events {
            operation.step(event);
        }
        operation
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        })
    }

    fn deleted() -> Event {
        Event::Storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        })
    }

    #[test]
    fn stores_in_transaction() {
        let key = retry_timer(&record());
        let mut operation = StoreAuditOperation::new(key.clone());
        let write = Effect::Storage(StorageEffect::Write {
            key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
            key: record().key().into(),
            value: record().to_bytes().unwrap().into(),
            txn_id: Some(txn()),
        });
        let delete = Effect::Storage(StorageEffect::Delete {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: key.storage_key().unwrap(),
            txn_id: Some(txn()),
        });
        let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id: txn() });
        let committed = Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() });
        let start = [Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })];
        let steps = vec![
            (started(), vec![write]),
            (written(), vec![delete]),
            (deleted(), vec![commit]),
            (committed, vec![]),
        ];
        check_trace(&mut operation, &start, steps);
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn other_timers_refused() {
        let mut operation = StoreAuditOperation::new(TaskKey::DrainCleanupQueue);
        assert!(operation.start().is_empty());
        assert_eq!(operation.finalize(), Err(AuditError::NotAuditTimer));
    }

    #[test]
    fn store_errors_abort() {
        // Before the commit an open transaction is aborted; a failed commit has none left.
        let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
        let mut starting = store_after(Vec::new());
        assert!(starting.step(storage_error()).is_empty());
        let mut writing = store_after(vec![started()]);
        assert_eq!(writing.step(storage_error()).as_slice(), [abort.clone()]);
        let mut deleting = store_after(vec![started(), written()]);
        assert_eq!(deleting.step(storage_error()).as_slice(), [abort]);
        let mut committing = store_after(vec![started(), written(), deleted()]);
        assert!(committing.step(storage_error()).is_empty());
        for operation in [starting, writing, deleting, committing] {
            assert_eq!(
                operation.finalize(),
                Err(AuditError::Storage(StorageError::Timeout))
            );
        }
    }

    #[test]
    fn store_rejects_events() {
        let phases: Vec<Vec<Event>> = vec![
            Vec::new(),
            vec![started()],
            vec![started(), written()],
            vec![started(), written(), deleted()],
        ];
        for (index, phase) in phases.into_iter().enumerate() {
            let make = || store_after(phase.clone());
            let refused = rejects_events(make, sample_events, |event| {
                let expected = [
                    matches!(
                        event,
                        Event::Storage(StorageEvent::TransactionStarted { .. })
                    ),
                    matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
                    matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })),
                    matches!(
                        event,
                        Event::Storage(StorageEvent::TransactionCommitted { .. })
                    ),
                ];
                expected[index] || matches!(event, Event::Storage(StorageEvent::Error { .. }))
            });
            assert!(!refused.is_empty());
        }
    }

    #[test]
    fn store_abort_finalize() {
        let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
        let mut starting = store_after(Vec::new());
        check_abort(&mut starting, &[]);
        rejects_finalize(starting);
        let mut writing = store_after(vec![started()]);
        check_abort(&mut writing, &[abort]);
        rejects_finalize(writing);
    }

    fn timer_row(key: TaskKey) -> (Key, Key) {
        let timer = PersistedTaskTimer {
            key,
            due_unix_millis: 1,
        };
        let row = postcard::to_allocvec(&timer).unwrap();
        (Vec::new().into(), row.into())
    }

    fn scan(start: Option<&[u8]>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(|key| IterStart::After(key.to_vec().into())),
            limit: TIMER_PAGE,
            txn_id: None,
        })
    }

    fn stored_event(result: Result<(), String>) -> Event {
        Event::SubOperation(SubOperationEvent::AuditStored { result })
    }

    #[test]
    fn resumes_kept_records() {
        // Other timers and undecodable rows are skipped; a failed store keeps its timer.
        let mut operation = ResumeAuditOperation::default();
        assert_eq!(operation.start().as_slice(), [scan(None)]);
        let page = Event::Storage(StorageEvent::IterResult {
            values: vec![
                timer_row(retry_timer(&record())),
                timer_row(TaskKey::DrainCleanupQueue),
                (Vec::new().into(), vec![0xff].into()),
                timer_row(retry_timer(&record())),
            ],
            next_start_after: Some(b"last".to_vec().into()),
        });
        let effects = operation.step(page);
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = operation.step(stored_event(Ok(())));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = operation.step(stored_event(Err("busy".to_string())));
        assert_eq!(effects.as_slice(), [scan(Some(b"last"))]);
        let last = Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        });
        assert!(operation.step(last).is_empty());
        assert_eq!(operation.finalize(), Ok(1));
    }

    #[test]
    fn resume_contract() {
        let mut scanning = ResumeAuditOperation::default();
        scanning.start();
        assert!(scanning.step(storage_error()).is_empty());
        assert_eq!(
            scanning.finalize(),
            Err(AuditError::Storage(StorageError::Timeout))
        );
        let scanning = || {
            let mut operation = ResumeAuditOperation::default();
            operation.start();
            operation
        };
        let refused = rejects_events(scanning, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refused.is_empty());
        let storing = || {
            let mut operation = scanning();
            operation.step(Event::Storage(StorageEvent::IterResult {
                values: vec![timer_row(retry_timer(&record()))],
                next_start_after: None,
            }));
            operation
        };
        let refused = rejects_events(storing, sample_events, |_| false);
        assert_eq!(refused.len(), sample_events().len());
        let mut fresh = ResumeAuditOperation::default();
        check_abort(&mut fresh, &[]);
        rejects_finalize(fresh);
        let mut open = storing();
        check_abort(&mut open, &[]);
        rejects_finalize(open);
    }
}
