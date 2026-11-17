//! Reads the stored S3 credential record that belongs to one access key id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::UserAccess;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum GetAccessError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No user access found")]
    NotFound,
    #[error("could not read the user access: {0}")]
    Io(IoError),
}

impl From<IoError> for GetAccessError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::StorageError(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetAccessOperation {
    access_key_id: String,
}

impl GetAccessOperation {
    pub fn new(access_key_id: String) -> Self {
        Self { access_key_id }
    }
}

impl Operation for GetAccessOperation {
    type Output = UserAccess;
    type Error = GetAccessError;

    fn first(&mut self) -> Result<Next<UserAccess>, GetAccessError> {
        let key = self.access_key_id.as_bytes();
        Ok(Next::Call(read_row(USER_ACCESS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UserAccess>, GetAccessError> {
        let value = result.value()?.ok_or(GetAccessError::NotFound)?;
        Ok(Next::Done(UserAccess::from_bytes(&value)?))
    }
}

#[cfg(test)]
mod test {
    use crate::driver::DriverContext;
    use crate::driver::drive;
    use crate::s3::access::get::{GetAccessError, GetAccessOperation, UserAccess};
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_storage::storage;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn missing_access_errors() {
        let mut operation = GetAccessOperation::new("key".to_string());
        let read = aruna_core::rows::read_row(USER_ACCESS_KEYSPACE, "key".as_bytes());
        assert_eq!(operation.first(), Ok(Next::Call(read)));
        let missing = IoResult::Values(vec![None]);
        assert_eq!(operation.next(missing), Err(GetAccessError::NotFound));
    }

    #[tokio::test]
    pub async fn gets_user_access() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();

        let user_identity = Default::default();
        let access_key_id = UserAccess::build_access_key(&Ulid::generate().to_string()).unwrap();
        let user_access = UserAccess {
            access_key: access_key_id.clone(),
            user_identity,
            group_id: Ulid::generate(),
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access_key_id.as_bytes().into(),
                value: user_access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let operation = GetAccessOperation::new(access_key_id.to_string());
        let result = drive(operation, &driver_ctx).await.unwrap();

        assert_eq!(result, user_access);
    }
}
