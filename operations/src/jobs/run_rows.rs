//! Writes a running job's own rows, again after each conflict the write loses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::JOB_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::{JobDeletes, JobId, JobWrites};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use super::MUTATE_MAX_ATTEMPTS;
use super::store::JobMutationError;
use super::store::job_rows::{PutEntryOperation, PutRowsOperation};

/// Why a job's stored state could not be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub(crate) struct StateReadError(pub(crate) String);

impl From<IoError> for StateReadError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Reads one state row of a job, such as its checkpoint; a missing row stays distinct from a
/// malformed one.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadStateOperation<T> {
    key: Key,
    state: PhantomData<fn() -> T>,
}

impl<T> ReadStateOperation<T> {
    /// Reads the row the job keeps under its id.
    pub(crate) fn new(job_id: JobId) -> Self {
        Self::at(job_state_key(job_id))
    }

    /// Reads the row under `key`.
    pub(crate) fn at(key: Key) -> Self {
        Self {
            key,
            state: PhantomData,
        }
    }
}

impl<T> Operation for ReadStateOperation<T>
where
    T: DeserializeOwned + Debug + PartialEq + Send,
{
    type Output = Option<T>;
    type Error = StateReadError;

    fn first(&mut self) -> Result<Next<Option<T>>, StateReadError> {
        let row = (JOB_STATE_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<T>>, StateReadError> {
        let [row] = values::<1>(result)?;
        let state = row
            .map(|value| postcard::from_bytes(&value))
            .transpose()
            .map_err(|error| StateReadError(error.to_string()))?;
        Ok(Next::Done(state))
    }
}

/// One encoded row of `T` under `key` in the job state key space.
pub(crate) fn state_row<T: serde::Serialize>(
    key: Key,
    row: &T,
) -> Result<(String, Key, Value), String> {
    let value = postcard::to_allocvec(row).map_err(|error| error.to_string())?;
    Ok((JOB_STATE_KEYSPACE.to_string(), key, ByteView::from(value)))
}

/// The state row a job keeps under its id, as [`ReadStateOperation`] reads it.
pub(crate) fn job_state_key(job_id: JobId) -> Key {
    ByteView::from(job_id.to_bytes().to_vec())
}

/// Writes one report entry of a running job, again after each conflict the write loses.
#[derive(Debug, PartialEq)]
pub(crate) struct SettledEntryOperation {
    job_id: JobId,
    token: Ulid,
    entry_key: Vec<u8>,
    value: Value,
    attempt: u32,
    waiting: bool,
}

impl SettledEntryOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, entry_key: &[u8], value: Value) -> Self {
        Self {
            job_id,
            token,
            entry_key: entry_key.to_vec(),
            value,
            attempt: 0,
            waiting: false,
        }
    }

    fn write(&self) -> Next<()> {
        let value = self.value.clone();
        let entry = PutEntryOperation::new(self.job_id, self.token, &self.entry_key, value);
        Next::Call(IoCall::child(entry))
    }
}

impl Operation for SettledEntryOperation {
    type Output = ();
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        Ok(self.write())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        if self.waiting {
            let IoResult::Woke = result else {
                let got = result.label();
                let expected = "IoResult::Woke";
                return Err(IoError::Unexpected { expected, got }.into());
            };
            self.waiting = false;
            return Ok(self.write());
        }
        match result.child::<(), JobMutationError>()? {
            Ok(()) => Ok(Next::Done(())),
            Err(JobMutationError::Conflict) if self.attempt + 1 < MUTATE_MAX_ATTEMPTS => {
                let pause = Duration::from_millis(1 << self.attempt.min(MAX_PAUSE_SHIFT));
                self.attempt += 1;
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(pause)))
            }
            Err(JobMutationError::Conflict) => Err(JobMutationError::Storage(
                "job entry write exhausted conflict retries".to_string(),
            )),
            Err(error) => Err(error),
        }
    }
}

/// The longest pause exponent between two attempts: 64 ms.
const MAX_PAUSE_SHIFT: u32 = 6;

/// Runs [`PutRowsOperation`] until it stops losing conflicts, pausing a little longer each time.
#[derive(Debug, PartialEq)]
pub(crate) struct SettledRowsOperation {
    job_id: JobId,
    token: Ulid,
    writes: JobWrites,
    deletes: JobDeletes,
    attempt: u32,
    waiting: bool,
}

impl SettledRowsOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, writes: JobWrites, deletes: JobDeletes) -> Self {
        Self {
            job_id,
            token,
            writes,
            deletes,
            attempt: 0,
            waiting: false,
        }
    }

    fn write(&self) -> Next<()> {
        let (writes, deletes) = (self.writes.clone(), self.deletes.clone());
        let rows = PutRowsOperation::new(self.job_id, self.token, writes, deletes);
        Next::Call(IoCall::child(rows))
    }
}

impl Operation for SettledRowsOperation {
    type Output = ();
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        Ok(self.write())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        if self.waiting {
            let IoResult::Woke = result else {
                let got = result.label();
                let expected = "IoResult::Woke";
                return Err(IoError::Unexpected { expected, got }.into());
            };
            self.waiting = false;
            return Ok(self.write());
        }
        match result.child::<(), JobMutationError>()? {
            Ok(()) => Ok(Next::Done(())),
            Err(JobMutationError::Conflict) if self.attempt + 1 < MUTATE_MAX_ATTEMPTS => {
                let pause = Duration::from_millis(1 << self.attempt.min(MAX_PAUSE_SHIFT));
                self.attempt += 1;
                self.waiting = true;
                Ok(Next::Call(IoCall::Delay(pause)))
            }
            Err(JobMutationError::Conflict) => Err(JobMutationError::Storage(
                "checkpoint write exhausted conflict retries".to_string(),
            )),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn operation() -> SettledRowsOperation {
        SettledRowsOperation::new(
            JobId::from_bytes([1; 16]),
            Ulid::from_bytes([2; 16]),
            Vec::new(),
            Vec::new(),
        )
    }

    fn answer(error: Option<JobMutationError>) -> IoResult {
        let result = error.map_or(Ok(()), Err);
        IoResult::Child(ChildOutcome::new(result))
    }

    // A lost conflict pauses and writes again; the write then ends the operation.
    #[test]
    fn conflict_writes_again() {
        let mut rows = operation();
        assert!(matches!(rows.first(), Ok(Next::Call(IoCall::Child(_)))));
        let pause = rows.next(answer(Some(JobMutationError::Conflict)));
        assert_eq!(
            pause,
            Ok(Next::Call(IoCall::Delay(Duration::from_millis(1))))
        );
        assert!(matches!(
            rows.next(IoResult::Woke),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(rows.next(answer(None)), Ok(Next::Done(())));
    }

    // The last attempt turns the conflict into a storage failure the job retries.
    #[test]
    fn conflicts_run_out() {
        let mut rows = operation();
        rows.attempt = MUTATE_MAX_ATTEMPTS - 1;
        let ended = rows.next(answer(Some(JobMutationError::Conflict)));
        assert!(matches!(ended, Err(JobMutationError::Storage(_))));
    }

    // A stored state row decodes; a missing row is none and a malformed one an error.
    #[test]
    fn state_read_decodes() {
        let job_id = JobId::from_bytes([1; 16]);
        let read = || ReadStateOperation::<u32>::new(job_id);
        let key = ByteView::from(job_id.to_bytes().to_vec());
        let row = (JOB_STATE_KEYSPACE.to_string(), key);
        assert_eq!(read().first(), Ok(Next::Call(IoCall::Read(vec![row]))));
        let value = ByteView::from(postcard::to_allocvec(&7u32).unwrap());
        let found = IoResult::Values(vec![Some(value)]);
        assert_eq!(read().next(found), Ok(Next::Done(Some(7))));
        assert_eq!(
            read().next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
        let broken = IoResult::Values(vec![Some(ByteView::from(Vec::new()))]);
        assert!(read().next(broken).is_err());
    }

    // A lost claim is no conflict and ends the write at once.
    #[test]
    fn lost_claim_ends() {
        let ended = operation().next(answer(Some(JobMutationError::TokenMismatch)));
        assert_eq!(ended, Err(JobMutationError::TokenMismatch));
    }
}
