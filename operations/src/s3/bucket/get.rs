//! Reads one bucket record by name from the bucket keyspace.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::BucketInfo;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetBucketState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received}]")]
    InvalidStateEvent {
        state: GetBucketState,
        expected: &'static str,
        received: &'static str,
    },
    #[error("GetBucketInfo has no result yet")]
    Incomplete,
}

impl From<IoError> for GetBucketError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            IoError::Unexpected { expected, got } => Self::InvalidStateEvent {
                state: GetBucketState::ReadBucket,
                expected,
                received: got,
            },
            _ => Self::Incomplete,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetBucketOperation {
    bucket: String,
}

impl GetBucketOperation {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

impl Operation for GetBucketOperation {
    type Output = BucketInfo;
    type Error = GetBucketError;

    fn first(&mut self) -> Result<Next<BucketInfo>, GetBucketError> {
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketInfo>, GetBucketError> {
        let bytes = result.value()?.ok_or(GetBucketError::NotFound)?;
        Ok(Next::Done(BucketInfo::from_bytes(&bytes)?))
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, GetBucketError::NotFound)
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::{GetBucketError, GetBucketOperation};
    use aruna_core::UserId;
    use aruna_core::calls::{IoError, IoResult, Next};
    use aruna_core::errors::StorageError;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::rows::read_row;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    fn fixed_bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([7u8; 16]),
            created_at: std::time::UNIX_EPOCH,
            created_by: UserId::new(Ulid::from_bytes([8u8; 16]), RealmId([9u8; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn read_result(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    #[test]
    fn bucket_not_found() {
        let mut operation = GetBucketOperation::new("missing".to_owned());
        assert_eq!(
            operation.first(),
            Ok(Next::Call(read_row(
                S3_BUCKET_KEYSPACE,
                b"missing".as_slice()
            )))
        );
        let missing = operation.next(read_result(None));
        assert_eq!(missing, Err(GetBucketError::NotFound));
    }

    #[test]
    fn found_bucket_returns() {
        let info = fixed_bucket_info();
        let bytes = info.to_bytes().expect("fixture encodes");
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        assert_eq!(
            operation.next(read_result(Some(bytes))),
            Ok(Next::Done(info))
        );
    }

    #[test]
    fn malformed_record_rejected() {
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        let error = (operation.next(read_result(Some(vec![0xff, 0x00, 0x01]))))
            .expect_err("malformed record must fail");
        assert!(
            matches!(error, GetBucketError::ConversionError(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn io_errors_map() {
        let storage = StorageError::ReadError("boom".to_string());
        let error = GetBucketError::from(IoError::Storage(storage.clone()));
        assert_eq!(error, GetBucketError::StorageError(storage));
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        let error = operation
            .next(IoResult::Done)
            .expect_err("wrong result must fail");
        assert!(matches!(error, GetBucketError::InvalidStateEvent { .. }));
        let aborted = GetBucketError::from(IoError::Aborted);
        assert_eq!(aborted, GetBucketError::Incomplete);
    }

    #[test]
    fn absence_is_expected() {
        assert!(GetBucketOperation::expected_error(
            &GetBucketError::NotFound
        ));
        assert!(!GetBucketOperation::expected_error(
            &GetBucketError::Incomplete
        ));
    }
}
