//! Reads the current version of every object under a key prefix, up to a limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use thiserror::Error;

use super::prefix_scan::PrefixScanOperation;

/// The current versions found, and whether every head under the prefix was read.
pub type HeadVersions = (Vec<(VersionKey, BlobVersion)>, bool);

#[derive(Debug, Error, PartialEq)]
pub enum HeadVersionsError {
    #[error("object version rows are unavailable")]
    Unavailable,
    #[error("{0}")]
    Invalid(String),
}

#[derive(Debug, PartialEq)]
enum HeadsState {
    Init,
    Heads,
    Versions,
    Done,
}

/// Scans at most `limit` object heads under `prefix`, then reads all their current
/// version rows in one batch. A head whose version row is missing is unavailable.
#[derive(Debug, PartialEq)]
pub struct HeadVersionsOperation {
    scan: Option<PrefixScanOperation>,
    version_keys: Vec<VersionKey>,
    complete: bool,
    state: HeadsState,
    output: Option<Result<HeadVersions, HeadVersionsError>>,
}

impl HeadVersionsOperation {
    pub(super) fn new(prefix: Key, limit: usize) -> Self {
        Self {
            scan: Some(PrefixScanOperation::new(
                BLOB_HEAD_KEYSPACE,
                (prefix, None),
                limit,
            )),
            version_keys: Vec::new(),
            complete: false,
            state: HeadsState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<HeadVersions, HeadVersionsError>) -> Effects {
        self.state = HeadsState::Done;
        self.output = Some(output);
        smallvec![]
    }

    /// Passes the head scan's effects on until it completes, then reads the versions.
    fn heads_progress(&mut self, effects: Effects) -> Effects {
        if !self.scan.as_ref().is_some_and(|scan| scan.is_complete()) {
            return effects;
        }
        let Some(scan) = self.scan.take() else {
            return self.finish(Err(HeadVersionsError::Unavailable));
        };
        let (heads, more) = match scan.finalize() {
            Ok(scanned) => scanned,
            Err(_) => return self.finish(Err(HeadVersionsError::Unavailable)),
        };
        self.complete = !more;
        match heads.iter().map(version_key).collect::<Result<Vec<_>, _>>() {
            Ok(keys) => self.read_versions(keys),
            Err(error) => self.finish(Err(error)),
        }
    }

    fn read_versions(&mut self, keys: Vec<(VersionKey, Vec<u8>)>) -> Effects {
        if keys.is_empty() {
            return self.finish(Ok((Vec::new(), self.complete)));
        }
        let (version_keys, reads): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|(key, row)| (key, (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(row))))
            .unzip();
        self.version_keys = version_keys;
        self.state = HeadsState::Versions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn versions_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let keys = std::mem::take(&mut self.version_keys);
        if values.len() != keys.len() {
            return self.finish(Err(HeadVersionsError::Unavailable));
        }
        let mut versions = Vec::with_capacity(keys.len());
        for (version_key, (_, value)) in keys.into_iter().zip(values) {
            let Some(value) = value else {
                return self.finish(Err(HeadVersionsError::Unavailable));
            };
            match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => versions.push((version_key, version)),
                Err(error) => {
                    return self.finish(Err(HeadVersionsError::Invalid(error.to_string())));
                }
            }
        }
        let complete = self.complete;
        self.finish(Ok((versions, complete)))
    }
}

/// The version key a head row points at, with its storage row key.
fn version_key((key, value): &(Key, Value)) -> Result<(VersionKey, Vec<u8>), HeadVersionsError> {
    let invalid =
        |error: aruna_core::errors::ConversionError| HeadVersionsError::Invalid(error.to_string());
    let head = BlobHeadKey::from_bytes(key.as_ref()).map_err(invalid)?;
    let pointer = CurrentVersionPointer::from_bytes(value.as_ref()).map_err(invalid)?;
    let version_key = VersionKey::new(head.bucket, head.key, pointer.version_id);
    let row = version_key.to_bytes().map_err(invalid)?;
    Ok((version_key, row))
}

impl Operation for HeadVersionsOperation {
    type Output = HeadVersions;
    type Error = HeadVersionsError;

    fn start(&mut self) -> Effects {
        self.state = HeadsState::Heads;
        let effects = match self.scan.as_mut() {
            Some(scan) => scan.start(),
            None => smallvec![],
        };
        self.heads_progress(effects)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (HeadsState::Heads, event) => {
                let effects = match self.scan.as_mut() {
                    Some(scan) => scan.step(event),
                    None => smallvec![],
                };
                self.heads_progress(effects)
            }
            (HeadsState::Versions, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.versions_read(values)
            }
            _ => self.finish(Err(HeadVersionsError::Unavailable)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == HeadsState::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(HeadVersionsError::Unavailable))
    }

    // Untransacted reads leave nothing to clean up.
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use aruna_core::effects::IterStart;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use std::time::SystemTime;
    use ulid::Ulid;

    const BUCKET: &str = "bucket";

    fn prefix() -> Key {
        BlobHeadKey::bucket_prefix(BUCKET).unwrap().into()
    }

    fn version_id(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    fn head(name: &str, seed: u8) -> (Key, Value) {
        let key = BlobHeadKey::new(BUCKET, name).to_bytes().unwrap();
        let pointer = CurrentVersionPointer::new(version_id(seed))
            .to_bytes()
            .unwrap();
        (key.into(), pointer.into())
    }

    fn version_key(name: &str, seed: u8) -> VersionKey {
        VersionKey::new(BUCKET, name, version_id(seed))
    }

    fn version() -> BlobVersion {
        BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default())
    }

    fn scan_effect(limit: usize) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix()),
            start: None::<IterStart>,
            limit,
            txn_id: None,
        })
    }

    fn reads_effect(keys: &[VersionKey]) -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: keys
                .iter()
                .map(|key| {
                    let row = key.to_bytes().unwrap();
                    (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(row))
                })
                .collect(),
            txn_id: None,
        })
    }

    fn heads(rows: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: rows,
            next_start_after: next,
        })
    }

    fn versions(values: Vec<Option<Value>>) -> Event {
        let values = values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value))
            .collect();
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    fn stored() -> Option<Value> {
        Some(version().to_bytes().unwrap().into())
    }

    fn after_heads(rows: Vec<(Key, Value)>) -> HeadVersionsOperation {
        let mut operation = HeadVersionsOperation::new(prefix(), 2);
        operation.start();
        operation.step(heads(rows, None));
        operation
    }

    #[test]
    fn reads_versions_batch() {
        let mut operation = HeadVersionsOperation::new(prefix(), 2);
        check_trace(
            &mut operation,
            &[scan_effect(3)],
            vec![
                (
                    heads(vec![head("a", 1), head("b", 2)], None),
                    vec![reads_effect(&[version_key("a", 1), version_key("b", 2)])],
                ),
                (versions(vec![stored(), stored()]), vec![]),
            ],
        );
        let expected = vec![
            (version_key("a", 1), version()),
            (version_key("b", 2), version()),
        ];
        assert_eq!(operation.finalize(), Ok((expected, true)));
    }

    #[test]
    fn cut_scan_incomplete() {
        // More heads than the limit read the first ones and report the scope as cut.
        let mut operation = after_heads(vec![head("a", 1), head("b", 2), head("c", 3)]);
        operation.step(versions(vec![stored(), stored()]));
        let (found, complete) = operation.finalize().unwrap();
        assert_eq!(found.len(), 2);
        assert!(!complete);
    }

    #[test]
    fn empty_prefix_complete() {
        let mut operation = after_heads(Vec::new());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok((Vec::new(), true)));
    }

    #[test]
    fn missing_rows_unavailable() {
        // A storage error, a missing version row or a short batch never answers partly.
        let mut failed_scan = HeadVersionsOperation::new(prefix(), 2);
        failed_scan.start();
        failed_scan.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(failed_scan.finalize(), Err(HeadVersionsError::Unavailable));

        for answer in [
            versions(vec![None]),
            versions(Vec::new()),
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            }),
        ] {
            let mut operation = after_heads(vec![head("a", 1)]);
            assert!(operation.step(answer).is_empty());
            assert_eq!(operation.finalize(), Err(HeadVersionsError::Unavailable));
        }
    }

    #[test]
    fn broken_rows_invalid() {
        let bad_head = (head("a", 1).0, Value::from(vec![0xff]));
        let operation = after_heads(vec![bad_head]);
        assert!(matches!(
            operation.finalize(),
            Err(HeadVersionsError::Invalid(_))
        ));

        let mut bad_version = after_heads(vec![head("a", 1)]);
        bad_version.step(versions(vec![Some(vec![0xff].into())]));
        assert!(matches!(
            bad_version.finalize(),
            Err(HeadVersionsError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_other_events() {
        let reading = || after_heads(vec![head("a", 1)]);
        let accepted =
            |event: &Event| matches!(event, Event::Storage(StorageEvent::BatchReadResult { .. }));
        for (_, effects, error) in rejects_events(reading, sample_events, accepted) {
            assert!(effects.is_empty());
            assert_eq!(error, HeadVersionsError::Unavailable);
        }
    }

    #[test]
    fn abort_cleans_nothing() {
        let mut operation = HeadVersionsOperation::new(prefix(), 2);
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
    }

    #[test]
    fn finalize_needs_result() {
        rejects_finalize(HeadVersionsOperation::new(prefix(), 2));
        rejects_finalize(after_heads(vec![head("a", 1)]));
    }
}
