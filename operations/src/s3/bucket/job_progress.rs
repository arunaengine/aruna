//! Stores what one page of a bucket job did on the job it ran for, in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::LINKED_JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// What one job page did; a waiting page keeps its cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobStep {
    pub cursor: Option<String>,
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    pub finished: bool,
    pub waiting: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum StoreProgressError {
    #[error("could not start a job transaction: {0}")]
    Begin(&'static str),
    #[error("could not read the job: {0}")]
    Read(&'static str),
    #[error("{0}")]
    Encoding(String),
    #[error("could not write the job")]
    Write,
    #[error("could not store the job: {0}")]
    Commit(&'static str),
    /// A pause or an abort committed first; a fresh run puts the progress onto it.
    #[error("a change of the job committed first")]
    Conflict,
    #[error("the job progress was not stored")]
    Unfinished,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ProgressState {
    Begin,
    Read,
    Write,
    Commit,
    Done,
    Failed,
}

/// Stores a page's progress on the job it ran for; a pause or an abort meanwhile stays, and a
/// job that another one replaced is left as it is.
#[derive(Debug, PartialEq)]
pub struct StoreProgressOperation {
    key: Key,
    job_id: Ulid,
    step: JobStep,
    ended: Option<LinkedState>,
    now_ms: u64,
    txn_id: Option<TxnId>,
    state: ProgressState,
    error: Option<StoreProgressError>,
}

impl StoreProgressOperation {
    pub fn new(
        bucket: &str,
        job_id: Ulid,
        step: JobStep,
        ended: Option<LinkedState>,
        now_ms: u64,
    ) -> Self {
        Self {
            key: bucket.as_bytes().to_vec().into(),
            job_id,
            step,
            ended,
            now_ms,
            txn_id: None,
            state: ProgressState::Begin,
            error: None,
        }
    }

    fn fail(&mut self, error: StoreProgressError) -> Effects {
        self.state = ProgressState::Failed;
        self.error = Some(error);
        abort_effects(&mut self.txn_id)
    }

    fn stored(&mut self, value: Option<Value>) -> Effects {
        let stored = value.as_deref().map(LinkedJob::from_bytes).transpose();
        let current = match stored {
            Ok(Some(current)) if current.id == self.job_id => current,
            Ok(_) => {
                self.state = ProgressState::Done;
                return abort_effects(&mut self.txn_id);
            }
            Err(error) => return self.fail(StoreProgressError::Encoding(error.to_string())),
        };
        let value = match self.applied(current).to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(StoreProgressError::Encoding(error.to_string())),
        };
        self.state = ProgressState::Write;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: LINKED_JOB_KEYSPACE.to_string(),
            key: self.key.clone(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn applied(&mut self, mut current: LinkedJob) -> LinkedJob {
        let step = std::mem::take(&mut self.step);
        current.cursor = step.cursor;
        current.done += step.done;
        current.skipped += step.skipped;
        current.failed += step.failed;
        current.waiting = step.waiting;
        current.updated_at_ms = self.now_ms;
        let next = self
            .ended
            .or((step.finished).then_some(LinkedState::Finished));
        if let (LinkedState::Running, Some(next)) = (current.state, next) {
            current.state = next;
            current.finished_at_ms = Some(self.now_ms);
        }
        current
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(StoreProgressError::Write);
        };
        self.state = ProgressState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn committed(&mut self, event: Event) -> Effects {
        // The commit closes the transaction whatever it answers.
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = ProgressState::Done;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => self.fail(StoreProgressError::Conflict),
            other => self.fail(StoreProgressError::Commit(other.label())),
        }
    }
}

impl Operation for StoreProgressOperation {
    type Output = ();
    type Error = StoreProgressError;

    fn start(&mut self) -> Effects {
        self.state = ProgressState::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (ProgressState::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = ProgressState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: LINKED_JOB_KEYSPACE.to_string(),
                    key: self.key.clone(),
                    txn_id: Some(txn_id),
                })]
            }
            (ProgressState::Begin, other) => self.fail(StoreProgressError::Begin(other.label())),
            (ProgressState::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.stored(value)
            }
            (ProgressState::Read, other) => self.fail(StoreProgressError::Read(other.label())),
            (ProgressState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.commit()
            }
            (ProgressState::Write, _) => self.fail(StoreProgressError::Write),
            (ProgressState::Commit, event) => self.committed(event),
            (ProgressState::Done | ProgressState::Failed, _) => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ProgressState::Done | ProgressState::Failed)
    }

    fn finalize(self) -> Result<(), StoreProgressError> {
        match (self.state, self.error) {
            (ProgressState::Done, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(StoreProgressError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }

    fn is_conflict(error: &StoreProgressError) -> bool {
        *error == StoreProgressError::Conflict
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::storage::backend_import::TokenGrant;
    use aruna_core::structs::storage::linked_job::LinkedTask;

    use super::*;
    use crate::s3::bucket::rename::tests::{group, realm};
    use crate::tests::records::node;

    fn job(id: u8, state: LinkedState) -> LinkedJob {
        LinkedJob {
            id: Ulid::from_bytes([id; 16]),
            task: LinkedTask::Unify {
                prefix: String::new(),
            },
            group_id: group(),
            auth: AuthContext::anonymous(realm()),
            node_id: node(4),
            grant: TokenGrant {
                token_hash: "token".to_string(),
                issuer: realm(),
                issued_at_secs: 1,
                expires_at_secs: u64::MAX,
                via: None,
            },
            state,
            cursor: None,
            total: 4,
            counted: true,
            done: 1,
            skipped: 0,
            failed: 0,
            waiting: Some("an import runs".to_string()),
            started_at_ms: 1,
            updated_at_ms: 2,
            finished_at_ms: None,
        }
    }

    fn page(finished: bool) -> JobStep {
        JobStep {
            cursor: Some("b".to_string()),
            done: 2,
            skipped: 1,
            finished,
            ..JobStep::default()
        }
    }

    fn operation(finished: bool, ended: Option<LinkedState>) -> StoreProgressOperation {
        StoreProgressOperation::new(
            "bucket",
            Ulid::from_bytes([1; 16]),
            page(finished),
            ended,
            9,
        )
    }

    fn txn() -> TxnId {
        Ulid::from_parts(7, 7)
    }

    fn started() -> Event {
        Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
    }

    fn read(row: Option<LinkedJob>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: row.map(|row| row.to_bytes().unwrap().into()),
        })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: b"bucket".to_vec().into(),
        })
    }

    fn failed(error: StorageError) -> Event {
        Event::Storage(StorageEvent::Error { error })
    }

    fn write(row: LinkedJob) -> Effect {
        Effect::Storage(StorageEffect::Write {
            key_space: LINKED_JOB_KEYSPACE.to_string(),
            key: b"bucket".to_vec().into(),
            value: row.to_bytes().unwrap().into(),
            txn_id: Some(txn()),
        })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() })
    }

    fn after(mut operation: StoreProgressOperation, events: Vec<Event>) -> StoreProgressOperation {
        operation.start();
        for event in events {
            operation.step(event);
        }
        operation
    }

    #[test]
    fn stores_page_progress() {
        let mut expected = job(1, LinkedState::Running);
        expected.cursor = Some("b".to_string());
        expected.done = 3;
        expected.skipped = 1;
        expected.waiting = None;
        expected.updated_at_ms = 9;
        let begin = Effect::Storage(StorageEffect::StartTransaction { read: false });
        let steps = vec![
            (
                started(),
                vec![Effect::Storage(StorageEffect::Read {
                    key_space: LINKED_JOB_KEYSPACE.to_string(),
                    key: b"bucket".to_vec().into(),
                    txn_id: Some(txn()),
                })],
            ),
            (
                read(Some(job(1, LinkedState::Running))),
                vec![write(expected)],
            ),
            (
                written(),
                vec![Effect::Storage(StorageEffect::CommitTransaction {
                    txn_id: txn(),
                })],
            ),
            (
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() }),
                vec![],
            ),
        ];
        let mut operation = operation(false, None);
        check_trace(&mut operation, &[begin], steps);
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn ends_running_only() {
        // A finished page ends a running job; a paused job keeps its state.
        let cases = [
            (LinkedState::Running, None, LinkedState::Finished, Some(9)),
            (
                LinkedState::Running,
                Some(LinkedState::Aborted),
                LinkedState::Aborted,
                Some(9),
            ),
            (LinkedState::Paused, None, LinkedState::Paused, None),
        ];
        for (stored, ended, state, finished_at_ms) in cases {
            let mut expected = job(1, stored);
            expected.cursor = Some("b".to_string());
            expected.done = 3;
            expected.skipped = 1;
            expected.waiting = None;
            expected.updated_at_ms = 9;
            expected.state = state;
            expected.finished_at_ms = finished_at_ms;
            let mut operation = after(operation(true, ended), vec![started()]);
            assert_eq!(
                operation.step(read(Some(job(1, stored)))).as_slice(),
                [write(expected)]
            );
        }
    }

    #[test]
    fn replaced_job_stays() {
        for row in [None, Some(job(2, LinkedState::Running))] {
            let mut operation = after(operation(false, None), vec![started()]);
            assert_eq!(operation.step(read(row)).as_slice(), [abort()]);
            assert_eq!(operation.finalize(), Ok(()));
        }
        let mut corrupt = after(operation(false, None), vec![started()]);
        let row = Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(vec![0xff].into()),
        });
        assert_eq!(corrupt.step(row).as_slice(), [abort()]);
        assert!(matches!(
            corrupt.finalize(),
            Err(StoreProgressError::Encoding(_))
        ));
    }

    #[test]
    fn conflict_retried_outside() {
        let events = vec![
            started(),
            read(Some(job(1, LinkedState::Running))),
            written(),
        ];
        let mut operation = after(operation(false, None), events);
        assert!(
            operation
                .step(failed(StorageError::TransactionConflict))
                .is_empty()
        );
        let error = operation.finalize().unwrap_err();
        assert!(StoreProgressOperation::is_conflict(&error));
        assert!(!StoreProgressOperation::is_conflict(
            &StoreProgressError::Write
        ));
    }

    #[test]
    fn errors_abort_open() {
        let mut starting = after(operation(false, None), Vec::new());
        assert!(starting.step(failed(StorageError::Timeout)).is_empty());
        let mut reading = after(operation(false, None), vec![started()]);
        assert_eq!(
            reading.step(failed(StorageError::Timeout)).as_slice(),
            [abort()]
        );
        let events = vec![started(), read(Some(job(1, LinkedState::Running)))];
        let mut writing = after(operation(false, None), events);
        assert_eq!(
            writing.step(failed(StorageError::Timeout)).as_slice(),
            [abort()]
        );
        assert!(matches!(
            starting.finalize(),
            Err(StoreProgressError::Begin(_))
        ));
        assert!(matches!(
            reading.finalize(),
            Err(StoreProgressError::Read(_))
        ));
        assert_eq!(writing.finalize(), Err(StoreProgressError::Write));
    }

    #[test]
    fn progress_contract() {
        let phases: Vec<Vec<Event>> = vec![
            Vec::new(),
            vec![started()],
            vec![started(), read(Some(job(1, LinkedState::Running)))],
            vec![
                started(),
                read(Some(job(1, LinkedState::Running))),
                written(),
            ],
        ];
        for (index, phase) in phases.into_iter().enumerate() {
            let make = || after(operation(false, None), phase.clone());
            let accepted = |event: &Event| match event {
                Event::Storage(StorageEvent::TransactionStarted { .. }) => index == 0,
                Event::Storage(StorageEvent::ReadResult { .. }) => index == 1,
                Event::Storage(StorageEvent::WriteResult { .. }) => index == 2,
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => index == 3,
                _ => false,
            };
            let refused = rejects_events(make, sample_events, accepted);
            assert_eq!(refused.len(), sample_events().len() - 1);
        }
        let mut fresh = operation(false, None);
        check_abort(&mut fresh, &[]);
        rejects_finalize(fresh);
        let mut open = after(operation(false, None), vec![started()]);
        check_abort(&mut open, &[abort()]);
        rejects_finalize(open);
    }
}
