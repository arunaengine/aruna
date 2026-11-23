//! Stores the config record of one compute backend and yields its config generation, which
//! advances only when the backend's home or config digest changed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::identity::BackendConfigRecord;
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::COMPUTE_BACKEND_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::types::{Key, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum BackendRecordError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

/// Reads the record of backend `backend` and writes it only when the digests differ.
#[derive(Debug, PartialEq)]
pub struct BackendRecordOperation {
    backend: String,
    home_digest: [u8; 32],
    config_digest: [u8; 32],
    written: Option<BackendConfigRecord>,
}

type RecordNext = Result<Next<BackendConfigRecord>, BackendRecordError>;

impl BackendRecordOperation {
    pub fn new(backend: &str, (home_digest, config_digest): ([u8; 32], [u8; 32])) -> Self {
        Self {
            backend: backend.to_string(),
            home_digest,
            config_digest,
            written: None,
        }
    }

    fn key(&self) -> Key {
        self.backend.as_bytes().to_vec().into()
    }

    /// The stored record advanced to this process's digests, or the stored one unchanged.
    fn decide(&mut self, stored: Option<Value>) -> RecordNext {
        let (home, config) = (self.home_digest, self.config_digest);
        let next = match stored {
            None => BackendConfigRecord::seed(home, config),
            Some(stored) => {
                let stored = BackendConfigRecord::from_bytes(&stored)?;
                match stored.advance(home, config)? {
                    Some(next) => next,
                    None => return Ok(Next::Done(stored)),
                }
            }
        };
        let row = (
            COMPUTE_BACKEND_KEYSPACE.to_string(),
            self.key(),
            next.to_bytes()?.into(),
        );
        self.written = Some(next);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for BackendRecordOperation {
    type Output = BackendConfigRecord;
    type Error = BackendRecordError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> RecordNext {
        Ok(Next::Call(read_row(COMPUTE_BACKEND_KEYSPACE, self.key())))
    }

    fn next(&mut self, result: IoResult) -> RecordNext {
        match self.written.take() {
            Some(written) => {
                result.done()?;
                Ok(Next::Done(written))
            }
            None => self.decide(result.value()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(config_digest: [u8; 32]) -> BackendRecordOperation {
        let mut record = BackendRecordOperation::new("lab", ([1; 32], config_digest));
        assert!(matches!(record.first(), Ok(Next::Call(IoCall::Read(_)))));
        record
    }

    fn stored(record: &BackendConfigRecord) -> IoResult {
        IoResult::Values(vec![Some(record.to_bytes().unwrap().into())])
    }

    fn written(next: RecordNext) -> BackendConfigRecord {
        let Ok(Next::Call(IoCall::Write(rows))) = next else {
            panic!("the record is written");
        };
        assert_eq!(rows[0].0, COMPUTE_BACKEND_KEYSPACE);
        BackendConfigRecord::from_bytes(rows[0].2.as_ref()).unwrap()
    }

    // The first record of a name starts at generation one.
    #[test]
    fn seeds_first_record() {
        let mut record = operation([2; 32]);
        let seeded = written(record.next(IoResult::Values(vec![None])));
        assert_eq!(seeded, BackendConfigRecord::seed([1; 32], [2; 32]));
        assert_eq!(record.next(IoResult::Done), Ok(Next::Done(seeded)));
    }

    // The same digests keep the stored record; a changed config advances its generation.
    #[test]
    fn advances_on_change() {
        let seed = BackendConfigRecord::seed([1; 32], [2; 32]);
        let mut same = operation([2; 32]);
        assert_eq!(same.next(stored(&seed)), Ok(Next::Done(seed.clone())));
        let mut changed = operation([3; 32]);
        let next = written(changed.next(stored(&seed)));
        assert_eq!(next.config_generation, 2);
    }
}
