//! Refuses a write addressed to an offered bucket or to the bucket of a folder being unbound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The write path calls it, so the read-only rule lives with the registration, not in a handler.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{OFFERED_DIRECTORY_KEYSPACE, SYNCED_FOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::offered_directory::OfferedDirectory;
use aruna_core::structs::{FolderState, SyncedFolder};
use aruna_core::types::Value;
use thiserror::Error;

use crate::device::sync::repository::bucket_folder;

#[derive(Debug, Error, PartialEq)]
pub enum BucketGuardError {
    #[error("bucket `{0}` is an offered directory and is read-only")]
    ReadOnly(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
}

impl From<IoError> for BucketGuardError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Checks that `bucket` takes writes: neither an offered directory nor the bucket of a synced
/// folder that is being unbound.
#[derive(Debug, PartialEq)]
pub struct BucketGuardOperation {
    bucket: String,
    /// Whether the folder row is read, after the offered directory row.
    folder: bool,
}

impl BucketGuardOperation {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            folder: false,
        }
    }

    fn read(key_space: &str, key: Vec<u8>) -> Next<()> {
        Next::Call(IoCall::Read(vec![(key_space.to_string(), key.into())]))
    }

    fn read_only(&self) -> BucketGuardError {
        BucketGuardError::ReadOnly(self.bucket.clone())
    }

    fn offered(&mut self, value: Option<Value>) -> Result<Next<()>, BucketGuardError> {
        if value
            .map(|value| OfferedDirectory::from_bytes(&value))
            .transpose()?
            .is_some()
        {
            return Err(self.read_only());
        }
        let Some(folder_id) = bucket_folder(&self.bucket) else {
            return Ok(Next::Done(()));
        };
        self.folder = true;
        Ok(Self::read(
            SYNCED_FOLDER_KEYSPACE,
            folder_id.to_bytes().to_vec(),
        ))
    }

    fn folder(&self, value: Option<Value>) -> Result<Next<()>, BucketGuardError> {
        let folder = value
            .map(|value| SyncedFolder::from_bytes(&value))
            .transpose()?;
        match folder {
            Some(folder) if folder.state == FolderState::Deleting => Err(self.read_only()),
            _ => Ok(Next::Done(())),
        }
    }
}

impl Operation for BucketGuardOperation {
    type Output = ();
    type Error = BucketGuardError;

    fn first(&mut self) -> Result<Next<()>, BucketGuardError> {
        Ok(Self::read(
            OFFERED_DIRECTORY_KEYSPACE,
            self.bucket.as_bytes().to_vec(),
        ))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, BucketGuardError> {
        let value = result.values()?.into_iter().next().flatten();
        match self.folder {
            false => self.offered(value),
            true => self.folder(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    fn values(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    // An ordinary bucket that is neither offered nor a folder bucket takes writes.
    #[test]
    fn plain_bucket_writes() {
        let mut operation = BucketGuardOperation::new("plain");
        let offered = BucketGuardOperation::read(OFFERED_DIRECTORY_KEYSPACE, b"plain".to_vec());
        assert_eq!(operation.first(), Ok(offered));
        assert_eq!(operation.next(values(None)), Ok(Next::Done(())));
    }

    // A folder bucket is checked against its folder; a missing folder row takes writes.
    #[test]
    fn folder_bucket_checked() {
        let folder_id = Ulid::from_bytes([3; 16]);
        let bucket = crate::device::sync::repository::folder_bucket(folder_id);
        let mut operation = BucketGuardOperation::new(bucket);
        operation.first().unwrap();
        let folder =
            BucketGuardOperation::read(SYNCED_FOLDER_KEYSPACE, folder_id.to_bytes().to_vec());
        assert_eq!(operation.next(values(None)), Ok(folder));
        assert_eq!(operation.next(values(None)), Ok(Next::Done(())));
    }

    // An undecodable row refuses the write, and so does a result that answers no read.
    #[test]
    fn failures_refuse() {
        let mut operation = BucketGuardOperation::new("plain");
        operation.first().unwrap();
        assert!(matches!(
            operation.next(values(Some(vec![0xFF]))),
            Err(BucketGuardError::Conversion(_))
        ));
        let conflict = BucketGuardError::from(IoError::Conflict);
        assert_eq!(conflict, StorageError::TransactionConflict.into());
        let mut operation = BucketGuardOperation::new("plain");
        operation.first().unwrap();
        assert!(operation.next(IoResult::Done).is_err());
    }
}
