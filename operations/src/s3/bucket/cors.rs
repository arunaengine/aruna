//! Puts, reads and deletes the CORS configuration stored on a bucket record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{BucketCorsConfiguration, BucketInfo};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum PutCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("PutBucketCors failed: {0}")]
    Io(IoError),
}

#[derive(Debug, Error, PartialEq)]
pub enum GetCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    BucketNotFound,
    #[error("Bucket CORS configuration not found")]
    CorsNotFound,
    #[error("GetBucketCors failed: {0}")]
    Io(IoError),
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("DeleteBucketCors failed: {0}")]
    Io(IoError),
}

macro_rules! from_io {
    ($error:ident) => {
        impl From<IoError> for $error {
            fn from(error: IoError) -> Self {
                match error {
                    IoError::Storage(error) => Self::StorageError(error),
                    other => Self::Io(other),
                }
            }
        }
    };
}

from_io!(PutCorsError);
from_io!(GetCorsError);
from_io!(DeleteCorsError);

/// The bucket record row with `config` as its CORS configuration.
fn cors_row(
    bucket: &str,
    bytes: &[u8],
    config: Option<BucketCorsConfiguration>,
) -> Result<IoCall, ConversionError> {
    let mut bucket_info = BucketInfo::from_bytes(bytes)?;
    bucket_info.cors_configuration = config;
    let row = (
        S3_BUCKET_KEYSPACE.to_string(),
        bucket.as_bytes().to_vec().into(),
        bucket_info.to_bytes()?.into(),
    );
    Ok(IoCall::Write(vec![row]))
}

#[derive(Debug, PartialEq)]
pub struct PutCorsOperation {
    bucket: String,
    config: BucketCorsConfiguration,
    written: bool,
}

impl PutCorsOperation {
    pub fn new(bucket: String, config: BucketCorsConfiguration) -> Self {
        Self {
            bucket,
            config,
            written: false,
        }
    }
}

impl Operation for PutCorsOperation {
    type Output = BucketCorsConfiguration;
    type Error = PutCorsError;

    fn first(&mut self) -> Result<Next<Self::Output>, PutCorsError> {
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, PutCorsError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(self.config.clone()));
        }
        let bytes = result.value()?.ok_or(PutCorsError::NotFound)?;
        self.written = true;
        let config = Some(self.config.clone());
        Ok(Next::Call(cors_row(&self.bucket, &bytes, config)?))
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[derive(Debug, PartialEq)]
pub struct GetCorsOperation {
    bucket: String,
}

impl GetCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

impl Operation for GetCorsOperation {
    type Output = BucketCorsConfiguration;
    type Error = GetCorsError;

    fn first(&mut self) -> Result<Next<Self::Output>, GetCorsError> {
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GetCorsError> {
        let bytes = result.value()?.ok_or(GetCorsError::BucketNotFound)?;
        let info = BucketInfo::from_bytes(&bytes)?;
        let config = info.cors_configuration.ok_or(GetCorsError::CorsNotFound)?;
        Ok(Next::Done(config))
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteCorsOperation {
    bucket: String,
    written: bool,
}

impl DeleteCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            written: false,
        }
    }
}

impl Operation for DeleteCorsOperation {
    type Output = ();
    type Error = DeleteCorsError;

    fn first(&mut self) -> Result<Next<()>, DeleteCorsError> {
        Ok(Next::Call(read_row(
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, DeleteCorsError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let bytes = result.value()?.ok_or(DeleteCorsError::NotFound)?;
        self.written = true;
        Ok(Next::Call(cors_row(&self.bucket, &bytes, None)?))
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeleteCorsError, DeleteCorsOperation, GetCorsError, GetCorsOperation, PutCorsError,
        PutCorsOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::storage::blob::{BucketCorsConfiguration, BucketCorsRule, BucketInfo};
    use aruna_core::structs::storage::format::Compression;
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn make_context() -> DriverContext {
        let temp_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        std::mem::forget(temp_dir);
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    fn bucket_info(cors_configuration: Option<BucketCorsConfiguration>) -> BucketInfo {
        BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn sample_cors() -> BucketCorsConfiguration {
        BucketCorsConfiguration {
            rules: vec![BucketCorsRule {
                id: Some("rule-1".into()),
                allowed_origins: vec!["https://example.org".into()],
                allowed_methods: vec!["GET".into(), "PUT".into()],
                allowed_headers: vec!["content-type".into()],
                expose_headers: vec!["etag".into()],
                max_age_seconds: Some(300),
            }],
        }
    }

    async fn write_bucket(context: &DriverContext, bucket: &str, info: &BucketInfo) {
        let txn = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await;
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = txn else {
            panic!("unexpected event");
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: info.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::TransactionCommitted { .. })
        ));
    }

    async fn read_bucket(context: &DriverContext, bucket: &str) -> BucketInfo {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            panic!("unexpected event");
        };
        BucketInfo::from_bytes(&value.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn configuration_roundtrips() {
        let context = make_context();
        let bucket = "my-bucket";
        let original = bucket_info(None);
        write_bucket(&context, bucket, &original).await;

        let config = sample_cors();
        let stored = drive(
            PutCorsOperation::new(bucket.to_string(), config.clone()),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(stored, config);

        let persisted = read_bucket(&context, bucket).await;
        assert_eq!(persisted.group_id, original.group_id);
        assert_eq!(persisted.created_at, original.created_at);
        assert_eq!(persisted.created_by, original.created_by);
        assert_eq!(persisted.cors_configuration, Some(config.clone()));

        let fetched = drive(GetCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap();
        assert_eq!(fetched, config);

        drive(DeleteCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap();

        let cleared = read_bucket(&context, bucket).await;
        assert_eq!(cleared.group_id, original.group_id);
        assert_eq!(cleared.created_at, original.created_at);
        assert_eq!(cleared.created_by, original.created_by);
        assert_eq!(cleared.cors_configuration, None);

        let missing_cors = drive(GetCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap_err();
        assert_eq!(missing_cors, GetCorsError::CorsNotFound);

        drive(DeleteCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn missing_config_rejected() {
        let bucket = "missing-bucket".to_string();

        let context = make_context();
        let put_missing = drive(
            PutCorsOperation::new(bucket.clone(), sample_cors()),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(put_missing, PutCorsError::NotFound);

        let get_missing = drive(GetCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap_err();
        assert_eq!(get_missing, GetCorsError::BucketNotFound);

        let delete_missing = drive(DeleteCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap_err();
        assert_eq!(delete_missing, DeleteCorsError::NotFound);

        let context = make_context();
        write_bucket(&context, &bucket, &bucket_info(None)).await;

        let get_no_config = drive(GetCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap_err();
        assert_eq!(get_no_config, GetCorsError::CorsNotFound);
    }
}
