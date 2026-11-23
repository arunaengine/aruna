//! Checks at startup whether the usage counters need a rebuild, and if so writes the marker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The marker makes the counters count as incomplete at once. The rebuild itself runs later in
// the background, so startup never waits for it.

use std::mem;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::usage::{
    USAGE_COMPLETE_KEY, USAGE_REBUILD_KEY, global_shard_keys, usage_bucket_key,
};
use aruna_core::types::Key;
use thiserror::Error;

/// Outputs `true` when the counters need a rebuild: shards or bucket rows are missing, or an
/// earlier rebuild was interrupted.
#[derive(Debug, PartialEq)]
pub struct CounterCheckOperation {
    state: State,
}

#[derive(Debug, PartialEq)]
enum State {
    ReadProbe,
    FirstBucket,
    BucketRow,
    ForgetComplete,
    WriteMarker,
}

#[derive(Debug, Error, PartialEq)]
pub enum CounterCheckError {
    #[error("usage counter probe failed: {0}")]
    Io(#[from] IoError),
    #[error("a bucket name is not UTF-8")]
    BucketName,
}

/// The shard rows, then the rebuild marker.
fn probe_keys() -> Vec<Vec<u8>> {
    let mut keys = global_shard_keys();
    keys.push(USAGE_REBUILD_KEY.to_vec());
    keys
}

impl CounterCheckOperation {
    pub fn new() -> Self {
        Self {
            state: State::ReadProbe,
        }
    }

    fn mark(&mut self, forget_complete: bool) -> Next<bool> {
        // Missing shards leave no complete counters for quota decisions.
        if forget_complete {
            self.state = State::ForgetComplete;
            let complete = (
                USAGE_STATS_KEYSPACE.to_string(),
                USAGE_COMPLETE_KEY.to_vec().into(),
            );
            return Next::Call(IoCall::Delete(vec![complete]));
        }
        self.state = State::WriteMarker;
        let key = USAGE_REBUILD_KEY.to_vec().into();
        let marker = (USAGE_STATS_KEYSPACE.to_string(), key, Vec::new().into());
        Next::Call(IoCall::Write(vec![marker]))
    }
}

impl Default for CounterCheckOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for CounterCheckOperation {
    type Output = bool;
    type Error = CounterCheckError;

    fn first(&mut self) -> Result<Next<bool>, CounterCheckError> {
        let reads = (probe_keys().into_iter())
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, CounterCheckError> {
        match (mem::replace(&mut self.state, State::ReadProbe), result) {
            (State::ReadProbe, IoResult::Values(mut values)) => {
                let marker = values.pop().flatten();
                let shards_missing = values.iter().any(Option::is_none);
                if marker.is_some() || shards_missing {
                    return Ok(self.mark(shards_missing));
                }
                self.state = State::FirstBucket;
                Ok(Next::Call(IoCall::Page {
                    space: S3_BUCKET_KEYSPACE.to_string(),
                    prefix: Key::from(Vec::new()),
                    after: None,
                    limit: 1,
                }))
            }
            // A first bucket without a counter row means bucket rows were never built.
            (State::FirstBucket, IoResult::Page { rows, .. }) => {
                let Some((bucket, _)) = rows.first() else {
                    return Ok(Next::Done(false));
                };
                let bucket =
                    std::str::from_utf8(bucket).map_err(|_| CounterCheckError::BucketName)?;
                self.state = State::BucketRow;
                let row = (
                    USAGE_STATS_KEYSPACE.to_string(),
                    usage_bucket_key(bucket).into(),
                );
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            (State::BucketRow, IoResult::Values(values)) => match values.first() {
                Some(Some(_)) => Ok(Next::Done(false)),
                _ => Ok(self.mark(false)),
            },
            (State::ForgetComplete, IoResult::Done) => Ok(self.mark(false)),
            (State::WriteMarker, IoResult::Done) => Ok(Next::Done(true)),
            _ => Err(IoError::Unexpected {
                expected: "the answer to the counter check call",
                got: "another result",
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::types::Value;

    fn probe(marker: bool, shards: bool) -> IoResult {
        let mut values: Vec<Option<Value>> = (global_shard_keys().iter())
            .map(|_| shards.then(|| Vec::new().into()))
            .collect();
        values.push(marker.then(|| Vec::new().into()));
        IoResult::Values(values)
    }

    fn marker() -> Next<bool> {
        let key = USAGE_REBUILD_KEY.to_vec().into();
        let write = (USAGE_STATS_KEYSPACE.to_string(), key, Vec::new().into());
        Next::Call(IoCall::Write(vec![write]))
    }

    fn probed(marker: bool, shards: bool) -> (CounterCheckOperation, Next<bool>) {
        let mut operation = CounterCheckOperation::new();
        let reads = (probe_keys().into_iter())
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
            .collect();
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads))));
        let next = operation.next(probe(marker, shards)).unwrap();
        (operation, next)
    }

    #[test]
    fn marks_missing_shards() {
        let (mut operation, next) = probed(false, false);
        let complete = (
            USAGE_STATS_KEYSPACE.to_string(),
            USAGE_COMPLETE_KEY.to_vec().into(),
        );
        assert_eq!(next, Next::Call(IoCall::Delete(vec![complete])));
        assert_eq!(operation.next(IoResult::Done), Ok(marker()));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn marks_interrupted_rebuild() {
        let (mut operation, next) = probed(true, true);
        assert_eq!(next, marker());
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn marks_missing_row() {
        let (mut operation, next) = probed(false, true);
        let page = IoCall::Page {
            space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: 1,
        };
        assert_eq!(next, Next::Call(page));
        let rows = vec![(b"older".to_vec().into(), Vec::new().into())];
        let answer = operation.next(IoResult::Page { rows, next: None });
        let row = (
            USAGE_STATS_KEYSPACE.to_string(),
            usage_bucket_key("older").into(),
        );
        assert_eq!(answer, Ok(Next::Call(IoCall::Read(vec![row]))));
        assert_eq!(operation.next(IoResult::Values(vec![None])), Ok(marker()));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn intact_counters_finish() {
        let (mut empty, _) = probed(false, true);
        let answer = empty.next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        });
        assert_eq!(answer, Ok(Next::Done(false)));
        let (mut counted, _) = probed(false, true);
        let rows = vec![(b"older".to_vec().into(), Vec::new().into())];
        counted.next(IoResult::Page { rows, next: None }).unwrap();
        let answer = counted.next(IoResult::Values(vec![Some(Vec::new().into())]));
        assert_eq!(answer, Ok(Next::Done(false)));
    }

    #[test]
    fn rejects_bad_answers() {
        let (mut operation, _) = probed(false, true);
        let rows = vec![(vec![0xff, 0xfe].into(), Vec::new().into())];
        let answer = operation.next(IoResult::Page { rows, next: None });
        assert_eq!(answer, Err(CounterCheckError::BucketName));
        let mut early = CounterCheckOperation::new();
        assert!(matches!(
            early.next(IoResult::Done),
            Err(CounterCheckError::Io(_))
        ));
        let message = CounterCheckError::from(IoError::Conflict).to_string();
        assert!(message.contains("usage counter probe failed"), "{message}");
    }
}
