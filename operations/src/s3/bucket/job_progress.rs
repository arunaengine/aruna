//! Stores what one page of a bucket job did on the job it ran for, in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::LINKED_JOB_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState};
use aruna_core::types::{Key, Value};
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
    #[error("{0}")]
    Encoding(String),
    /// A pause or an abort committed first; a fresh run puts the progress onto it.
    #[error("a change of the job committed first")]
    Conflict,
    #[error("could not store the job: {0}")]
    Io(IoError),
}

impl From<IoError> for StoreProgressError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Conflict,
            other => Self::Io(other),
        }
    }
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
    writing: bool,
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
            writing: false,
        }
    }

    fn stored(&mut self, value: Option<Value>) -> Result<Next<()>, StoreProgressError> {
        let stored = value.as_deref().map(LinkedJob::from_bytes).transpose();
        let current = match stored {
            Ok(Some(current)) if current.id == self.job_id => current,
            Ok(_) => return Ok(Next::Done(())),
            Err(error) => return Err(StoreProgressError::Encoding(error.to_string())),
        };
        let value = (self.applied(current).to_bytes())
            .map_err(|error| StoreProgressError::Encoding(error.to_string()))?;
        self.writing = true;
        let row = (
            LINKED_JOB_KEYSPACE.to_string(),
            self.key.clone(),
            value.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
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
}

impl Operation for StoreProgressOperation {
    type Output = ();
    type Error = StoreProgressError;

    fn first(&mut self) -> Result<Next<()>, StoreProgressError> {
        let read = (LINKED_JOB_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StoreProgressError> {
        if self.writing {
            result.done()?;
            return Ok(Next::Done(()));
        }
        self.stored(result.value()?)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
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

    fn stored(row: Option<LinkedJob>) -> IoResult {
        IoResult::Values(vec![row.map(|row| row.to_bytes().unwrap().into())])
    }

    fn write(row: LinkedJob) -> Next<()> {
        Next::Call(IoCall::Write(vec![(
            LINKED_JOB_KEYSPACE.to_string(),
            b"bucket".to_vec().into(),
            row.to_bytes().unwrap().into(),
        )]))
    }

    fn progressed(stored: LinkedState) -> LinkedJob {
        let mut expected = job(1, stored);
        expected.cursor = Some("b".to_string());
        expected.done = 3;
        expected.skipped = 1;
        expected.waiting = None;
        expected.updated_at_ms = 9;
        expected
    }

    #[test]
    fn stores_page_progress() {
        let mut operation = operation(false, None);
        let read = vec![(LINKED_JOB_KEYSPACE.to_string(), b"bucket".to_vec().into())];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(read))));
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let answer = operation.next(stored(Some(job(1, LinkedState::Running))));
        assert_eq!(answer, Ok(write(progressed(LinkedState::Running))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
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
        for (stored_state, ended, state, finished_at_ms) in cases {
            let mut expected = progressed(stored_state);
            expected.state = state;
            expected.finished_at_ms = finished_at_ms;
            let mut operation = operation(true, ended);
            let answer = operation.next(stored(Some(job(1, stored_state))));
            assert_eq!(answer, Ok(write(expected)));
        }
    }

    #[test]
    fn replaced_job_stays() {
        for row in [None, Some(job(2, LinkedState::Running))] {
            assert_eq!(operation(false, None).next(stored(row)), Ok(Next::Done(())));
        }
        let corrupt = IoResult::Values(vec![Some(vec![0xff].into())]);
        assert!(matches!(
            operation(false, None).next(corrupt),
            Err(StoreProgressError::Encoding(_))
        ));
    }

    #[test]
    fn conflict_retried_outside() {
        let error = StoreProgressError::from(IoError::Conflict);
        assert!(StoreProgressOperation::is_conflict(&error));
        let wrong = StoreProgressError::from(IoError::Aborted);
        assert!(!StoreProgressOperation::is_conflict(&wrong));
    }
}
