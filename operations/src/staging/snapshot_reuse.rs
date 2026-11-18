//! Finds the snapshot a retried staging already wrote: the head version of the key, when it
//! holds the same source binding and a stored copy of the source's length.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, VersionKey,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SnapshotReuseError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for SnapshotReuseError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// The stored copy and version id a retried staging may answer with.
pub type Reusable = Option<(BackendLocation, Ulid)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReuseStep {
    Head,
    Version,
    Location,
}

/// Outputs the stored copy and version id the retried staging may answer with, if any.
#[derive(Debug, PartialEq)]
pub struct SnapshotReuseOperation {
    bucket: String,
    key: String,
    source: VersionSourceBinding,
    content_length: u64,
    step: ReuseStep,
    version_id: Option<Ulid>,
}

impl SnapshotReuseOperation {
    pub fn new(
        bucket: String,
        key: String,
        source: VersionSourceBinding,
        content_length: u64,
    ) -> Self {
        Self {
            bucket,
            key,
            source,
            content_length,
            step: ReuseStep::Head,
            version_id: None,
        }
    }

    fn read(&mut self, step: ReuseStep, key_space: &str, key: Key) -> Next<Reusable> {
        self.step = step;
        Next::Call(IoCall::Read(vec![(key_space.to_string(), key)]))
    }

    /// The next read, or the answer once a row is missing or does not match.
    fn found(&mut self, value: Value) -> Result<Next<Reusable>, SnapshotReuseError> {
        match self.step {
            ReuseStep::Head => {
                let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
                self.version_id = Some(pointer.version_id);
                let row =
                    VersionKey::new(&self.bucket, &self.key, pointer.version_id).to_bytes()?;
                Ok(self.read(ReuseStep::Version, BLOB_VERSIONS_KEYSPACE, row.into()))
            }
            ReuseStep::Version => {
                let version = BlobVersion::from_bytes(value.as_ref())?;
                let BlobVersionState::Materialized {
                    blob_hash,
                    backend,
                    encoding,
                    source: Some(source),
                } = version.state
                else {
                    return Ok(Next::Done(None));
                };
                if source != self.source {
                    return Ok(Next::Done(None));
                }
                let key = BlobLocationKey::new(blob_hash, encoding, backend).to_bytes();
                Ok(self.read(ReuseStep::Location, BLOB_LOCATIONS_KEYSPACE, key.into()))
            }
            ReuseStep::Location => {
                let location = BackendLocation::from_bytes(value.as_ref())?;
                let version_id = self.version_id.ok_or(StorageError::InvalidEffect)?;
                let kept =
                    (location.blob_size == self.content_length).then_some((location, version_id));
                Ok(Next::Done(kept))
            }
        }
    }
}

impl Operation for SnapshotReuseOperation {
    type Output = Reusable;
    type Error = SnapshotReuseError;

    fn first(&mut self) -> Result<Next<Reusable>, SnapshotReuseError> {
        let head = BlobHeadKey::new(&self.bucket, &self.key).to_bytes()?;
        Ok(self.read(ReuseStep::Head, BLOB_HEAD_KEYSPACE, head.into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Reusable>, SnapshotReuseError> {
        match result.values()?.into_iter().next().flatten() {
            Some(value) => self.found(value),
            None => Ok(Next::Done(None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::staging::{sample_connector, sample_metadata};
    use aruna_core::structs::execution::staging::StagingStrategy;

    fn operation() -> SnapshotReuseOperation {
        let source = crate::connectors::descriptor::build_source_binding(
            StagingStrategy::Snapshot,
            &sample_connector(),
            &sample_metadata(),
            "folder/file.txt".to_string(),
            None,
            None,
        );
        SnapshotReuseOperation::new("bucket".into(), "key".into(), source, 42)
    }

    // A key without a head has no snapshot to reuse.
    #[test]
    fn missing_head_ignored() {
        let mut reuse = operation();
        let head = BlobHeadKey::new("bucket", "key").to_bytes().unwrap();
        let read = IoCall::Read(vec![(BLOB_HEAD_KEYSPACE.to_string(), head.into())]);
        assert_eq!(reuse.first(), Ok(Next::Call(read)));
        assert_eq!(
            reuse.next(IoResult::Values(vec![None])),
            Ok(Next::Done(None))
        );
    }

    // An undecodable head fails the lookup, and so does a result that answers no read.
    #[test]
    fn failures_fail() {
        let mut reuse = operation();
        reuse.first().unwrap();
        let garbage = IoResult::Values(vec![Some(vec![0xFF].into())]);
        assert!(matches!(
            reuse.next(garbage),
            Err(SnapshotReuseError::Conversion(_))
        ));
        let mut reuse = operation();
        reuse.first().unwrap();
        assert!(reuse.next(IoResult::Done).is_err());
    }
}
