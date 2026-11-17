//! Checks at startup whether the usage counters need a rebuild, and if so writes the marker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The marker makes the counters count as incomplete at once. The rebuild itself runs later in
// the background, so startup never waits for it.

use std::mem;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::usage::{
    USAGE_COMPLETE_KEY, USAGE_REBUILD_KEY, global_shard_keys, usage_bucket_key,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

/// Outputs `true` when the counters need a rebuild: shards or bucket rows are missing, or an
/// earlier rebuild was interrupted.
#[derive(Debug, PartialEq)]
pub struct CounterCheckOperation {
    state: State,
}

#[derive(Debug, PartialEq)]
enum State {
    Init,
    ReadProbe { rows: usize },
    FirstBucket,
    BucketRow,
    ForgetComplete,
    WriteMarker,
    Done(bool),
    Failed(CounterCheckError),
}

#[derive(Debug, Error, PartialEq)]
pub enum CounterCheckError {
    #[error("usage counter probe failed: {0}")]
    Storage(StorageError),
    #[error("usage counter probe returned {got} values for {expected} rows")]
    Count { got: usize, expected: usize },
    #[error("a bucket name is not UTF-8")]
    BucketName,
    #[error("usage counter probe received unexpected event: {0}")]
    Unexpected(String),
    #[error("usage counter check did not finish")]
    Unfinished,
}

impl CounterCheckOperation {
    pub fn new() -> Self {
        Self { state: State::Init }
    }

    fn mark(&mut self, forget_complete: bool) -> Effects {
        // Missing shards leave no complete counters for quota decisions.
        if forget_complete {
            self.state = State::ForgetComplete;
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: USAGE_COMPLETE_KEY.to_vec().into(),
                txn_id: None,
            })];
        }
        self.state = State::WriteMarker;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: USAGE_REBUILD_KEY.to_vec().into(),
            value: Vec::new().into(),
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: CounterCheckError) -> Effects {
        self.state = State::Failed(error);
        smallvec![]
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

    fn start(&mut self) -> Effects {
        let mut keys = global_shard_keys();
        keys.push(USAGE_REBUILD_KEY.to_vec());
        self.state = State::ReadProbe { rows: keys.len() };
        let reads = (keys.into_iter())
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = mem::replace(&mut self.state, State::Init);
        let Event::Storage(event) = event else {
            return self.fail(CounterCheckError::Unexpected(format!("{event:?}")));
        };
        match (state, event) {
            (_, StorageEvent::Error { error }) => self.fail(CounterCheckError::Storage(error)),
            (State::ReadProbe { rows }, StorageEvent::BatchReadResult { values }) => {
                if values.len() != rows {
                    let got = values.len();
                    return self.fail(CounterCheckError::Count {
                        got,
                        expected: rows,
                    });
                }
                let Some(((_, marker), shards)) = values.split_last() else {
                    return self.fail(CounterCheckError::Count {
                        got: 0,
                        expected: rows,
                    });
                };
                let shards_missing = shards.iter().any(|(_, value)| value.is_none());
                if marker.is_some() || shards_missing {
                    return self.mark(shards_missing);
                }
                self.state = State::FirstBucket;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    prefix: None,
                    start: None,
                    limit: 1,
                    txn_id: None,
                })]
            }
            // A first bucket without a counter row means bucket rows were never built.
            (State::FirstBucket, StorageEvent::IterResult { values, .. }) => {
                let Some((bucket, _)) = values.first() else {
                    self.state = State::Done(false);
                    return smallvec![];
                };
                let Ok(bucket) = std::str::from_utf8(bucket) else {
                    return self.fail(CounterCheckError::BucketName);
                };
                self.state = State::BucketRow;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: USAGE_STATS_KEYSPACE.to_string(),
                    key: usage_bucket_key(bucket).into(),
                    txn_id: None,
                })]
            }
            (State::BucketRow, StorageEvent::ReadResult { value: None, .. }) => self.mark(false),
            (State::BucketRow, StorageEvent::ReadResult { value: Some(_), .. }) => {
                self.state = State::Done(false);
                smallvec![]
            }
            (State::ForgetComplete, StorageEvent::DeleteResult { .. }) => self.mark(false),
            (State::WriteMarker, StorageEvent::WriteResult { .. }) => {
                self.state = State::Done(true);
                smallvec![]
            }
            (_, event) => self.fail(CounterCheckError::Unexpected(format!("{event:?}"))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Done(_) | State::Failed(_))
    }

    fn finalize(self) -> Result<bool, CounterCheckError> {
        match self.state {
            State::Done(rebuild) => Ok(rebuild),
            State::Failed(error) => Err(error),
            _ => Err(CounterCheckError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{rejects_events, sample_events};
    use aruna_core::types::Key;

    fn probe_values(marker: bool, shards: bool) -> Vec<(Key, Option<aruna_core::types::Value>)> {
        let mut values: Vec<_> = (global_shard_keys().into_iter())
            .map(|key| (key.into(), shards.then(|| Vec::new().into())))
            .collect();
        values.push((
            USAGE_REBUILD_KEY.to_vec().into(),
            marker.then(|| Vec::new().into()),
        ));
        values
    }

    fn probed(marker: bool, shards: bool) -> (CounterCheckOperation, Effects) {
        let mut operation = CounterCheckOperation::new();
        operation.start();
        let values = probe_values(marker, shards);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        (operation, effects)
    }

    fn written(mut operation: CounterCheckOperation) -> bool {
        let key = USAGE_REBUILD_KEY.to_vec().into();
        operation.step(Event::Storage(StorageEvent::WriteResult { key }));
        assert!(operation.is_complete());
        operation.finalize().expect("check finishes")
    }

    #[test]
    fn marks_missing_shards() {
        let (mut operation, effects) = probed(false, false);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key, .. })] if key.as_ref() == USAGE_COMPLETE_KEY
        ));
        let key = USAGE_COMPLETE_KEY.to_vec().into();
        let effects = operation.step(Event::Storage(StorageEvent::DeleteResult { key }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key, .. })] if key.as_ref() == USAGE_REBUILD_KEY
        ));
        assert!(written(operation));
    }

    #[test]
    fn marks_interrupted_rebuild() {
        let (operation, effects) = probed(true, true);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));
        assert!(written(operation));
    }

    #[test]
    fn marks_missing_row() {
        let (mut operation, effects) = probed(false, true);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { limit: 1, .. })]
        ));
        let values = vec![(b"older".to_vec().into(), Vec::new().into())];
        let next_start_after = None;
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }));
        let row: Key = usage_bucket_key("older").into();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key, .. })] if *key == row
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: row,
            value: None,
        }));
        assert!(written(operation));
    }

    #[test]
    fn intact_counters_finish() {
        let (mut operation, _) = probed(false, true);
        let values = Vec::new();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(false));
    }

    #[test]
    fn reports_storage_error() {
        let mut operation = CounterCheckOperation::new();
        operation.start();
        let error = StorageError::ReadError("boom".to_string());
        operation.step(Event::Storage(StorageEvent::Error { error }));
        let message = operation.finalize().unwrap_err().to_string();
        assert!(message.contains("usage counter probe failed"));
        assert!(message.contains("boom"), "storage cause missing: {message}");
    }

    #[test]
    fn rejects_short_probe() {
        let mut operation = CounterCheckOperation::new();
        operation.start();
        let values = Vec::new();
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            operation.finalize(),
            Err(CounterCheckError::Count { got: 0, .. })
        ));
    }

    #[test]
    fn rejects_other_events() {
        let make = || {
            let mut operation = CounterCheckOperation::new();
            operation.start();
            operation
        };
        let accepted = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::BatchReadResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, effects, error) in rejects_events(make, sample_events, accepted) {
            assert!(effects.is_empty(), "{label}");
            assert!(matches!(error, CounterCheckError::Unexpected(_)), "{label}");
        }
    }

    #[test]
    fn rejects_early_finalize() {
        let mut operation = CounterCheckOperation::new();
        operation.start();
        assert_eq!(operation.finalize(), Err(CounterCheckError::Unfinished));
    }
}
