//! Reads the current version of every object under a key prefix, up to a limit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;

use super::prefix_scan::PrefixScanOperation;
use crate::metadata::call_results::read_values;

/// The current versions found, and whether every head under the prefix was read.
pub type HeadVersions = (Vec<(VersionKey, BlobVersion)>, bool);

#[derive(Debug, Error, PartialEq)]
pub enum HeadVersionsError {
    #[error("object version rows are unavailable")]
    Unavailable,
    #[error("{0}")]
    Invalid(String),
}

// Storage, adapter and runner failures all leave the version rows unread.
impl From<IoError> for HeadVersionsError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

type HeadsTurn = Result<Next<HeadVersions>, HeadVersionsError>;

/// Scans at most `limit` object heads under `prefix`, then reads all their current
/// version rows in one batch. A head whose version row is missing is unavailable.
#[derive(Debug, PartialEq)]
pub struct HeadVersionsOperation {
    /// The head scan until it finishes; `None` while the version rows are read.
    scan: Option<PrefixScanOperation>,
    version_keys: Vec<VersionKey>,
    complete: bool,
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
        }
    }

    /// Passes the head scan's calls on until it finishes, then reads the versions.
    fn heads_turn(&mut self, turn: Next<(Vec<(Key, Value)>, bool)>) -> HeadsTurn {
        let (heads, more) = match turn {
            Next::Call(call) => return Ok(Next::Call(call)),
            Next::Wait => return Ok(Next::Wait),
            // The scan is a plain read, so it has no calls after a commit.
            Next::Done(scanned)
            | Next::Then {
                output: scanned, ..
            } => scanned,
        };
        self.scan = None;
        self.complete = !more;
        let keys = heads
            .iter()
            .map(version_key)
            .collect::<Result<Vec<_>, _>>()?;
        if keys.is_empty() {
            return Ok(Next::Done((Vec::new(), self.complete)));
        }
        let (version_keys, reads): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|(key, row)| (key, (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(row))))
            .unzip();
        self.version_keys = version_keys;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn versions_read(&mut self, values: Vec<Option<Value>>) -> HeadsTurn {
        let keys = std::mem::take(&mut self.version_keys);
        if values.len() != keys.len() {
            return Err(HeadVersionsError::Unavailable);
        }
        let mut versions = Vec::with_capacity(keys.len());
        for (version_key, value) in keys.into_iter().zip(values) {
            let value = value.ok_or(HeadVersionsError::Unavailable)?;
            let version = BlobVersion::from_bytes(value.as_ref()).map_err(invalid)?;
            versions.push((version_key, version));
        }
        Ok(Next::Done((versions, self.complete)))
    }
}

fn invalid(error: ConversionError) -> HeadVersionsError {
    HeadVersionsError::Invalid(error.to_string())
}

/// The version key a head row points at, with its storage row key.
fn version_key((key, value): &(Key, Value)) -> Result<(VersionKey, Vec<u8>), HeadVersionsError> {
    let head = BlobHeadKey::from_bytes(key.as_ref()).map_err(invalid)?;
    let pointer = CurrentVersionPointer::from_bytes(value.as_ref()).map_err(invalid)?;
    let version_key = VersionKey::new(head.bucket, head.key, pointer.version_id);
    let row = version_key.to_bytes().map_err(invalid)?;
    Ok((version_key, row))
}

impl Operation for HeadVersionsOperation {
    type Output = HeadVersions;
    type Error = HeadVersionsError;

    fn first(&mut self) -> HeadsTurn {
        let turn = match self.scan.as_mut() {
            Some(scan) => scan.first().map_err(|_| HeadVersionsError::Unavailable)?,
            None => return Err(HeadVersionsError::Unavailable),
        };
        self.heads_turn(turn)
    }

    fn next(&mut self, result: IoResult) -> HeadsTurn {
        match self.scan.as_mut() {
            Some(scan) => {
                let turn = scan
                    .next(result)
                    .map_err(|_| HeadVersionsError::Unavailable)?;
                self.heads_turn(turn)
            }
            None => self.versions_read(read_values(result)?),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
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

    fn heads_call(limit: usize) -> HeadsTurn {
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: prefix(),
            after: None,
            limit,
        }))
    }

    fn reads_call(keys: &[VersionKey]) -> HeadsTurn {
        let reads = keys
            .iter()
            .map(|key| {
                let row = key.to_bytes().unwrap();
                (BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(row))
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn heads(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn stored() -> Option<Value> {
        Some(version().to_bytes().unwrap().into())
    }

    fn after_heads(rows: Vec<(Key, Value)>) -> (HeadVersionsOperation, HeadsTurn) {
        let mut operation = HeadVersionsOperation::new(prefix(), 2);
        operation.first().unwrap();
        let turn = operation.next(heads(rows));
        (operation, turn)
    }

    #[test]
    fn reads_versions_batch() {
        let mut operation = HeadVersionsOperation::new(prefix(), 2);
        assert_eq!(operation.first(), heads_call(3));
        assert_eq!(
            operation.next(heads(vec![head("a", 1), head("b", 2)])),
            reads_call(&[version_key("a", 1), version_key("b", 2)])
        );
        let expected = vec![
            (version_key("a", 1), version()),
            (version_key("b", 2), version()),
        ];
        assert_eq!(
            operation.next(IoResult::Values(vec![stored(), stored()])),
            Ok(Next::Done((expected, true)))
        );
    }

    #[test]
    fn cut_scan_incomplete() {
        // More heads than the limit read the first ones and report the scope as cut.
        let (mut operation, _) = after_heads(vec![head("a", 1), head("b", 2), head("c", 3)]);
        let Ok(Next::Done((found, complete))) =
            operation.next(IoResult::Values(vec![stored(), stored()]))
        else {
            panic!("expected the versions");
        };
        assert_eq!(found.len(), 2);
        assert!(!complete);
    }

    #[test]
    fn empty_prefix_complete() {
        let (_, turn) = after_heads(Vec::new());
        assert_eq!(turn, Ok(Next::Done((Vec::new(), true))));
    }

    #[test]
    fn missing_rows_unavailable() {
        // A failed scan, a missing version row or a short batch never answers partly.
        let mut failed_scan = HeadVersionsOperation::new(prefix(), 2);
        failed_scan.first().unwrap();
        assert_eq!(
            failed_scan.next(IoResult::Done),
            Err(HeadVersionsError::Unavailable)
        );
        for answer in [IoResult::Values(vec![None]), IoResult::Values(Vec::new())] {
            let (mut operation, _) = after_heads(vec![head("a", 1)]);
            assert_eq!(operation.next(answer), Err(HeadVersionsError::Unavailable));
        }
        assert_eq!(
            HeadVersionsError::from(IoError::Aborted),
            HeadVersionsError::Unavailable
        );
    }

    #[test]
    fn broken_rows_invalid() {
        let bad_head = (head("a", 1).0, Value::from(vec![0xff]));
        let (_, turn) = after_heads(vec![bad_head]);
        assert!(matches!(turn, Err(HeadVersionsError::Invalid(_))));

        let (mut bad_version, _) = after_heads(vec![head("a", 1)]);
        assert!(matches!(
            bad_version.next(IoResult::Values(vec![Some(vec![0xff].into())])),
            Err(HeadVersionsError::Invalid(_))
        ));
    }
}
