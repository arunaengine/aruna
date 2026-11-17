//! Reads the version an object key names: the given one, or the current head.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::call_results::read_value;

/// The version an object resolved to, and its record when one is stored.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectVersion {
    pub version_id: Option<Ulid>,
    pub version: Option<BlobVersion>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ObjectVersionError {
    #[error("the object key does not encode: {0}")]
    Key(ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Decode(#[from] ConversionError),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for ObjectVersionError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Io(other),
        }
    }
}

/// Resolves `version_id`, or the head of `bucket/key` when none is given, and
/// reads that version's record. An object without a head resolves to nothing.
#[derive(Debug, PartialEq)]
pub struct ObjectVersionOperation {
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    /// Set once the head is resolved, so the next result is the version row.
    reading_version: bool,
}

impl ObjectVersionOperation {
    pub fn new(bucket: &str, key: &str, version_id: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            reading_version: false,
        }
    }

    fn read_version(
        &mut self,
        version_id: Ulid,
    ) -> Result<Next<ObjectVersion>, ObjectVersionError> {
        self.version_id = Some(version_id);
        self.reading_version = true;
        let row = VersionKey::new(&self.bucket, &self.key, version_id)
            .to_bytes()
            .map_err(ObjectVersionError::Key)?;
        let read = vec![(BLOB_VERSIONS_KEYSPACE.to_string(), row.into())];
        Ok(Next::Call(IoCall::Read(read)))
    }
}

impl Operation for ObjectVersionOperation {
    type Output = ObjectVersion;
    type Error = ObjectVersionError;

    fn first(&mut self) -> Result<Next<ObjectVersion>, ObjectVersionError> {
        if let Some(version_id) = self.version_id {
            return self.read_version(version_id);
        }
        let head = BlobHeadKey::new(&self.bucket, &self.key)
            .to_bytes()
            .map_err(ObjectVersionError::Key)?;
        let read = vec![(BLOB_HEAD_KEYSPACE.to_string(), head.into())];
        Ok(Next::Call(IoCall::Read(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectVersion>, ObjectVersionError> {
        let value = read_value(result)?;
        if self.reading_version {
            let version = value
                .map(|value| BlobVersion::from_bytes(&value))
                .transpose()?;
            return Ok(Next::Done(ObjectVersion {
                version_id: self.version_id,
                version,
            }));
        }
        match value {
            Some(value) => self.read_version(CurrentVersionPointer::from_bytes(&value)?.version_id),
            None => Ok(Next::Done(ObjectVersion {
                version_id: None,
                version: None,
            })),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use aruna_core::types::Value;
    use std::time::SystemTime;

    const BUCKET: &str = "bucket";
    const KEY: &str = "a.txt";

    fn version_id() -> Ulid {
        Ulid::from_bytes([7; 16])
    }

    fn version() -> BlobVersion {
        BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default())
    }

    fn head_call() -> Next<ObjectVersion> {
        let head = BlobHeadKey::new(BUCKET, KEY).to_bytes().unwrap();
        Next::Call(IoCall::Read(vec![(
            BLOB_HEAD_KEYSPACE.to_string(),
            head.into(),
        )]))
    }

    fn version_call() -> Next<ObjectVersion> {
        let row = VersionKey::new(BUCKET, KEY, version_id())
            .to_bytes()
            .unwrap();
        Next::Call(IoCall::Read(vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            row.into(),
        )]))
    }

    fn value(bytes: Vec<u8>) -> IoResult {
        IoResult::Values(vec![Some(Value::from(bytes))])
    }

    fn head_bytes() -> Vec<u8> {
        CurrentVersionPointer::new(version_id()).to_bytes().unwrap()
    }

    fn by_head() -> ObjectVersionOperation {
        ObjectVersionOperation::new(BUCKET, KEY, None)
    }

    #[test]
    fn reads_head_version() {
        let mut operation = by_head();
        assert_eq!(operation.first(), Ok(head_call()));
        assert_eq!(operation.next(value(head_bytes())), Ok(version_call()));
        assert_eq!(
            operation.next(value(version().to_bytes().unwrap())),
            Ok(Next::Done(ObjectVersion {
                version_id: Some(version_id()),
                version: Some(version()),
            }))
        );
    }

    #[test]
    fn skips_head_read() {
        // A named version that is not stored resolves to its id without a record.
        let mut operation = ObjectVersionOperation::new(BUCKET, KEY, Some(version_id()));
        assert_eq!(operation.first(), Ok(version_call()));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(ObjectVersion {
                version_id: Some(version_id()),
                version: None,
            }))
        );
    }

    #[test]
    fn missing_head_empty() {
        let mut operation = by_head();
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Ok(Next::Done(ObjectVersion {
                version_id: None,
                version: None,
            }))
        );
    }

    #[test]
    fn broken_rows_fail() {
        let mut bad_head = by_head();
        bad_head.first().unwrap();
        assert!(matches!(
            bad_head.next(value(vec![0xff])),
            Err(ObjectVersionError::Decode(_))
        ));

        let mut bad_version = by_head();
        bad_version.first().unwrap();
        bad_version.next(value(head_bytes())).unwrap();
        assert!(matches!(
            bad_version.next(value(vec![0xff])),
            Err(ObjectVersionError::Decode(_))
        ));
    }

    #[test]
    fn wrong_result_fails() {
        let mut operation = by_head();
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ObjectVersionError::Io(IoError::Unexpected { .. }))
        ));
    }

    #[test]
    fn runner_errors_map() {
        assert_eq!(
            ObjectVersionError::from(IoError::Storage(StorageError::Timeout)),
            ObjectVersionError::Storage(StorageError::Timeout)
        );
        assert_eq!(
            ObjectVersionError::from(IoError::Conflict),
            ObjectVersionError::Storage(StorageError::TransactionConflict)
        );
        assert_eq!(
            ObjectVersionError::from(IoError::Aborted),
            ObjectVersionError::Io(IoError::Aborted)
        );
    }
}
