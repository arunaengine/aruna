//! Pauses and resumes a running purge between its batches, and reads whether it is paused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PURGE_FILES_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use ulid::Ulid;

fn pause_key(job_id: Ulid) -> Key {
    [&job_id.to_bytes()[..], b"p"].concat().into()
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PauseError {
    #[error("could not change the purge pause: {0}")]
    Storage(#[from] aruna_core::errors::StorageError),
    #[error("unexpected event for the purge pause: {0}")]
    UnexpectedEvent(String),
    #[error("the purge pause did not finish")]
    NotFinished,
}

/// Pauses or resumes the purge `job_id` between its batches.
#[derive(Debug, PartialEq)]
pub struct PausePurgeOperation {
    job_id: Ulid,
    paused: bool,
    output: Option<Result<(), PauseError>>,
}

impl PausePurgeOperation {
    pub fn new(job_id: Ulid, paused: bool) -> Self {
        Self {
            job_id,
            paused,
            output: None,
        }
    }
}

impl Operation for PausePurgeOperation {
    type Output = ();
    type Error = PauseError;

    fn start(&mut self) -> Effects {
        let key = pause_key(self.job_id);
        let effect = match self.paused {
            true => StorageEffect::Write {
                key_space: PURGE_FILES_KEYSPACE.to_string(),
                key,
                value: Vec::new().into(),
                txn_id: None,
            },
            false => StorageEffect::Delete {
                key_space: PURGE_FILES_KEYSPACE.to_string(),
                key,
                txn_id: None,
            },
        };
        smallvec![Effect::Storage(effect)]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match (&self.output, event) {
            (
                None,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => Ok(()),
            (None, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(PauseError::UnexpectedEvent(format!("{event:?}"))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), PauseError> {
        self.output.unwrap_or(Err(PauseError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Whether the purge `job_id` is paused.
#[derive(Debug, PartialEq)]
pub struct PausedOperation {
    job_id: Ulid,
    output: Option<Result<bool, PauseError>>,
}

impl PausedOperation {
    pub fn new(job_id: Ulid) -> Self {
        Self {
            job_id,
            output: None,
        }
    }
}

impl Operation for PausedOperation {
    type Output = bool;
    type Error = PauseError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: PURGE_FILES_KEYSPACE.to_string(),
            key: pause_key(self.job_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match (&self.output, event) {
            (None, Event::Storage(StorageEvent::ReadResult { value, .. })) => Ok(value.is_some()),
            (None, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(PauseError::UnexpectedEvent(format!("{event:?}"))),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<bool, PauseError> {
        self.output.unwrap_or(Err(PauseError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pause_marks_reads() {
        use aruna_core::operation_check::{
            check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
        };
        let job_id = Ulid::from_parts(5, 5);
        let mut pause = PausePurgeOperation::new(job_id, true);
        let write = Effect::Storage(StorageEffect::Write {
            key_space: PURGE_FILES_KEYSPACE.to_string(),
            key: pause_key(job_id),
            value: Vec::new().into(),
            txn_id: None,
        });
        let written = Event::Storage(StorageEvent::WriteResult {
            key: pause_key(job_id),
        });
        check_trace(&mut pause, &[write], vec![(written, vec![])]);
        assert_eq!(pause.finalize(), Ok(()));
        let mut read = PausedOperation::new(job_id);
        read.start();
        read.step(Event::Storage(StorageEvent::ReadResult {
            key: pause_key(job_id),
            value: Some(Vec::new().into()),
        }));
        assert_eq!(read.finalize(), Ok(true));
        let failed = Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::Timeout,
        });
        let mut resume = PausePurgeOperation::new(job_id, false);
        resume.start();
        resume.step(failed);
        assert!(matches!(resume.finalize(), Err(PauseError::Storage(_))));
        let reading = || {
            let mut operation = PausedOperation::new(job_id);
            operation.start();
            operation
        };
        let refused = rejects_events(reading, sample_events, |event| {
            matches!(event, Event::Storage(StorageEvent::ReadResult { .. }))
        });
        assert_eq!(refused.len(), sample_events().len() - 1);
        let mut open = reading();
        check_abort(&mut open, &[]);
        rejects_finalize(open);
    }
}
