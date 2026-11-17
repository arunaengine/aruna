//! Pauses and resumes a running purge between its batches, and reads whether it is paused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PURGE_FILES_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::Key;
use ulid::Ulid;

fn pause_key(job_id: Ulid) -> Key {
    [&job_id.to_bytes()[..], b"p"].concat().into()
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PauseError {
    #[error("could not change the purge pause: {0}")]
    Storage(#[from] aruna_core::errors::StorageError),
    #[error("unexpected answer for the purge pause: {0}")]
    Io(IoError),
}

impl From<IoError> for PauseError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

/// Pauses or resumes the purge `job_id` between its batches.
#[derive(Debug, PartialEq)]
pub struct PausePurgeOperation {
    job_id: Ulid,
    paused: bool,
}

impl PausePurgeOperation {
    pub fn new(job_id: Ulid, paused: bool) -> Self {
        Self { job_id, paused }
    }
}

impl Operation for PausePurgeOperation {
    type Output = ();
    type Error = PauseError;

    fn first(&mut self) -> Result<Next<()>, PauseError> {
        let key = pause_key(self.job_id);
        let space = PURGE_FILES_KEYSPACE.to_string();
        Ok(Next::Call(match self.paused {
            true => IoCall::Write(vec![(space, key, Vec::new().into())]),
            false => IoCall::Delete(vec![(space, key)]),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PauseError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

/// Whether the purge `job_id` is paused.
#[derive(Debug, PartialEq)]
pub struct PausedOperation {
    job_id: Ulid,
}

impl PausedOperation {
    pub fn new(job_id: Ulid) -> Self {
        Self { job_id }
    }
}

impl Operation for PausedOperation {
    type Output = bool;
    type Error = PauseError;

    fn first(&mut self) -> Result<Next<bool>, PauseError> {
        let read = (PURGE_FILES_KEYSPACE.to_string(), pause_key(self.job_id));
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, PauseError> {
        Ok(Next::Done(result.value()?.is_some()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_marks_reads() {
        let job_id = Ulid::from_parts(5, 5);
        let space = || PURGE_FILES_KEYSPACE.to_string();
        let mut pause = PausePurgeOperation::new(job_id, true);
        let write = IoCall::Write(vec![(space(), pause_key(job_id), Vec::new().into())]);
        assert_eq!(pause.first(), Ok(Next::Call(write)));
        assert_eq!(pause.next(IoResult::Done), Ok(Next::Done(())));
        let mut resume = PausePurgeOperation::new(job_id, false);
        let delete = IoCall::Delete(vec![(space(), pause_key(job_id))]);
        assert_eq!(resume.first(), Ok(Next::Call(delete)));
        let mut read = PausedOperation::new(job_id);
        let call = IoCall::Read(vec![(space(), pause_key(job_id))]);
        assert_eq!(read.first(), Ok(Next::Call(call)));
        let marked = IoResult::Values(vec![Some(Vec::new().into())]);
        assert_eq!(read.next(marked), Ok(Next::Done(true)));
        assert_eq!(
            read.next(IoResult::Values(vec![None])),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn pause_answer_errors() {
        let mut pause = PausePurgeOperation::new(Ulid::from_parts(5, 5), true);
        let wrong = pause.next(IoResult::Woke);
        assert!(matches!(
            wrong,
            Err(PauseError::Io(IoError::Unexpected { .. }))
        ));
        let storage = PauseError::from(IoError::Storage(aruna_core::errors::StorageError::Timeout));
        assert!(matches!(storage, PauseError::Storage(_)));
    }
}
