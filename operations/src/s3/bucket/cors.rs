//! Puts, reads and deletes the CORS configuration stored on a bucket record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BucketCorsConfiguration, BucketInfo};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutCorsState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("PutBucketCors did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct PutCorsOperation {
    bucket: String,
    config: BucketCorsConfiguration,
    state: PutCorsState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BucketCorsConfiguration, PutCorsError>>,
}

impl PutCorsOperation {
    pub fn new(bucket: String, config: BucketCorsConfiguration) -> Self {
        Self {
            bucket,
            config,
            state: PutCorsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutCorsError) -> Effects {
        self.state = PutCorsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutCorsState::Init => "Init",
            PutCorsState::StartTransaction => "StartTransaction",
            PutCorsState::ReadBucket => "ReadBucket",
            PutCorsState::WriteBucket => "WriteBucket",
            PutCorsState::CommitTransaction => "CommitTransaction",
            PutCorsState::Finish => "Finish",
            PutCorsState::Error => "Error",
        }
    }
}

impl Operation for PutCorsOperation {
    type Output = BucketCorsConfiguration;
    type Error = PutCorsError;

    fn start(&mut self) -> Effects {
        self.state = PutCorsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            PutCorsState::Init => self.start(),
            PutCorsState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutCorsState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutCorsError::NoTransactionFound);
                };
                let Some(bytes) = value else {
                    return self.fail(PutCorsError::NotFound);
                };
                let mut bucket_info = match BucketInfo::from_bytes(&bytes) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                bucket_info.cors_configuration = Some(self.config.clone());
                let value = match bucket_info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = PutCorsState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            PutCorsState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutCorsError::NoTransactionFound);
                };
                self.state = PutCorsState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutCorsState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutCorsState::Finish;
                self.output = Some(Ok(self.config.clone()));
                smallvec![]
            }
            PutCorsState::Finish => smallvec![],
            PutCorsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutCorsState::Finish | PutCorsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PutCorsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetCorsState {
    Init,
    ReadBucket,
    Finish,
    Error,
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
    #[error("GetBucketCors did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetCorsOperation {
    bucket: String,
    state: GetCorsState,
    output: Option<Result<BucketCorsConfiguration, GetCorsError>>,
}

impl GetCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetCorsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GetCorsError) -> Effects {
        self.state = GetCorsState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetCorsState::Init => "Init",
            GetCorsState::ReadBucket => "ReadBucket",
            GetCorsState::Finish => "Finish",
            GetCorsState::Error => "Error",
        }
    }
}

impl Operation for GetCorsOperation {
    type Output = BucketCorsConfiguration;
    type Error = GetCorsError;

    fn start(&mut self) -> Effects {
        self.state = GetCorsState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            GetCorsState::Init => self.start(),
            GetCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                self.state = GetCorsState::Finish;
                self.output = Some(match value {
                    Some(bytes) => match BucketInfo::from_bytes(&bytes) {
                        Ok(info) => info.cors_configuration.ok_or(GetCorsError::CorsNotFound),
                        Err(err) => Err(GetCorsError::ConversionError(err)),
                    },
                    None => Err(GetCorsError::BucketNotFound),
                });
                smallvec![]
            }
            GetCorsState::Finish => smallvec![],
            GetCorsState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetCorsState::Finish | GetCorsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetCorsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteCorsState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("DeleteBucketCors did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct DeleteCorsOperation {
    bucket: String,
    state: DeleteCorsState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<(), DeleteCorsError>>,
}

impl DeleteCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteCorsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: DeleteCorsError) -> Effects {
        self.state = DeleteCorsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            DeleteCorsState::Init => "Init",
            DeleteCorsState::StartTransaction => "StartTransaction",
            DeleteCorsState::ReadBucket => "ReadBucket",
            DeleteCorsState::WriteBucket => "WriteBucket",
            DeleteCorsState::CommitTransaction => "CommitTransaction",
            DeleteCorsState::Finish => "Finish",
            DeleteCorsState::Error => "Error",
        }
    }
}

impl Operation for DeleteCorsOperation {
    type Output = ();
    type Error = DeleteCorsError;

    fn start(&mut self) -> Effects {
        self.state = DeleteCorsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            DeleteCorsState::Init => self.start(),
            DeleteCorsState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(DeleteCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = DeleteCorsState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(DeleteCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteCorsError::NoTransactionFound);
                };
                let Some(bytes) = value else {
                    return self.fail(DeleteCorsError::NotFound);
                };
                let mut bucket_info = match BucketInfo::from_bytes(&bytes) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                bucket_info.cors_configuration = None;
                let value = match bucket_info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = DeleteCorsState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteCorsState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(DeleteCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteCorsError::NoTransactionFound);
                };
                self.state = DeleteCorsState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteCorsState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(DeleteCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = DeleteCorsState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            DeleteCorsState::Finish => smallvec![],
            DeleteCorsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeleteCorsState::Finish | DeleteCorsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(DeleteCorsError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
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
            task_handle: None,
            compute_handle: None,
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
