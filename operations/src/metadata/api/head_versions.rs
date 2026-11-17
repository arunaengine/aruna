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
