//! Reads a staging job's progress with its open prefix lanes counted in.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{read_row, value};
use aruna_core::structs::execution::job::{JobId, StagingJobCheckpoint};
use aruna_core::types::Key;
use thiserror::Error;

use super::checkpoint_key;
use super::lanes::{LANE_ROWS, lanes_of, merged};

/// Why a checkpoint could not be read.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{0}")]
pub struct CheckpointError(String);

impl From<IoError> for CheckpointError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// The job's progress with its open prefix lanes counted in; a lane's prefix reads as not
/// started yet, as a job handed over goes on from there.
#[derive(Debug, PartialEq)]
pub struct ReadCheckpointOperation {
    job_id: JobId,
    main: Option<StagingJobCheckpoint>,
}

impl ReadCheckpointOperation {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id, main: None }
    }
}

impl Operation for ReadCheckpointOperation {
    type Output = Option<StagingJobCheckpoint>;
    type Error = CheckpointError;

    fn first(&mut self) -> Result<Next<Self::Output>, CheckpointError> {
        let key = checkpoint_key(self.job_id, None).to_vec();
        Ok(Next::Call(read_row(STAGING_STATE_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CheckpointError> {
        let Some(main) = self.main.take() else {
            let Some(row) = value(result)? else {
                return Ok(Next::Done(None));
            };
            let main =
                postcard::from_bytes(&row).map_err(|error| CheckpointError(error.to_string()))?;
            self.main = Some(main);
            return Ok(Next::Call(IoCall::Page {
                space: STAGING_STATE_KEYSPACE.to_string(),
                prefix: Key::from(self.job_id.to_bytes().to_vec()),
                after: None,
                limit: LANE_ROWS,
            }));
        };
        let (rows, _) = result.page()?;
        let lanes = lanes_of(rows).map_err(CheckpointError)?;
        Ok(Next::Done(Some(merged(main, lanes))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::Value;

    fn job() -> JobId {
        JobId::from_bytes([1; 16])
    }

    fn checkpoint(current: u64) -> StagingJobCheckpoint {
        StagingJobCheckpoint {
            items_current: current,
            ..StagingJobCheckpoint::default()
        }
    }

    fn row(checkpoint: &StagingJobCheckpoint) -> Value {
        Value::from(postcard::to_allocvec(checkpoint).unwrap())
    }

    // A job without a checkpoint has no progress to report.
    #[test]
    fn missing_row_none() {
        let mut read = ReadCheckpointOperation::new(job());
        let key = checkpoint_key(job(), None).to_vec();
        assert_eq!(
            read.first(),
            Ok(Next::Call(read_row(STAGING_STATE_KEYSPACE, key)))
        );
        let absent = read.next(IoResult::Values(vec![None]));
        assert_eq!(absent, Ok(Next::Done(None)));
    }

    // Open lanes count in the job's progress.
    #[test]
    fn counts_open_lanes() {
        let mut read = ReadCheckpointOperation::new(job());
        read.first().unwrap();
        let main = IoResult::Values(vec![Some(row(&checkpoint(10)))]);
        assert!(matches!(
            read.next(main),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let lane = (
            Key::from(checkpoint_key(job(), Some(0))),
            row(&checkpoint(5)),
        );
        let page = IoResult::Page {
            rows: vec![lane],
            next: None,
        };
        let Ok(Next::Done(Some(done))) = read.next(page) else {
            panic!("expected the checkpoint");
        };
        assert_eq!(done.items_current, 15);
    }

    // A row that does not decode is an error, never missing progress.
    #[test]
    fn bad_row_fails() {
        let mut read = ReadCheckpointOperation::new(job());
        read.first().unwrap();
        let bad = IoResult::Values(vec![Some(Value::from(vec![0xFF]))]);
        assert!(read.next(bad).is_err());
    }
}
