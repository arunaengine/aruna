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
use byteview::ByteView;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use super::MUTATE_MAX_ATTEMPTS;
use super::store::JobMutationError;
use super::store::job_rows::PutRowsOperation;

/// Why a job's stored state could not be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub(crate) struct StateReadError(pub(crate) String);

impl From<IoError> for StateReadError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// Reads the state row a job keeps under its id, such as its checkpoint; a missing row stays
/// distinct from a malformed one.
#[derive(Debug, PartialEq)]
pub(crate) struct ReadStateOperation<T> {
    job_id: JobId,
    state: PhantomData<fn() -> T>,
}

impl<T> ReadStateOperation<T> {
    pub(crate) fn new(job_id: JobId) -> Self {
        Self {
            job_id,
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
        let key = ByteView::from(self.job_id.to_bytes().to_vec());
        let row = (JOB_STATE_KEYSPACE.to_string(), key);
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
