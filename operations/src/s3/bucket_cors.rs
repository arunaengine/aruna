use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketCorsConfiguration, BucketInfo};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutBucketCorsState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct PutBucketCorsOperation {
    bucket: String,
    config: BucketCorsConfiguration,
    state: PutBucketCorsState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BucketCorsConfiguration, PutBucketCorsError>>,
}

impl PutBucketCorsOperation {
    pub fn new(bucket: String, config: BucketCorsConfiguration) -> Self {
        Self {
            bucket,
            config,
            state: PutBucketCorsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutBucketCorsError) -> Effects {
        self.state = PutBucketCorsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutBucketCorsState::Init => "Init",
            PutBucketCorsState::StartTransaction => "StartTransaction",
            PutBucketCorsState::ReadBucket => "ReadBucket",
            PutBucketCorsState::WriteBucket => "WriteBucket",
            PutBucketCorsState::CommitTransaction => "CommitTransaction",
            PutBucketCorsState::Finish => "Finish",
            PutBucketCorsState::Error => "Error",
        }
    }
}

impl Operation for PutBucketCorsOperation {
    type Output = Option<Result<BucketCorsConfiguration, PutBucketCorsError>>;
    type Error = PutBucketCorsError;

    fn start(&mut self) -> Effects {
        self.state = PutBucketCorsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutBucketCorsState::Init => self.start(),
            PutBucketCorsState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutBucketCorsState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketCorsError::NoTransactionFound);
                };
                let Some(bytes) = value else {
                    return self.fail(PutBucketCorsError::NotFound);
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
                self.state = PutBucketCorsState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketCorsState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketCorsError::NoTransactionFound);
                };
                self.state = PutBucketCorsState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutBucketCorsState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutBucketCorsState::Finish;
                self.output = Some(Ok(self.config.clone()));
                smallvec![]
            }
            PutBucketCorsState::Finish => smallvec![],
            PutBucketCorsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutBucketCorsState::Finish | PutBucketCorsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == PutBucketCorsState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetBucketCorsState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    BucketNotFound,
    #[error("Bucket CORS configuration not found")]
    CorsNotFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetBucketCorsOperation {
    bucket: String,
    state: GetBucketCorsState,
    output: Option<Result<BucketCorsConfiguration, GetBucketCorsError>>,
}

impl GetBucketCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketCorsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GetBucketCorsError) -> Effects {
        self.state = GetBucketCorsState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetBucketCorsState::Init => "Init",
            GetBucketCorsState::ReadBucket => "ReadBucket",
            GetBucketCorsState::Finish => "Finish",
            GetBucketCorsState::Error => "Error",
        }
    }
}

impl Operation for GetBucketCorsOperation {
    type Output = Option<Result<BucketCorsConfiguration, GetBucketCorsError>>;
    type Error = GetBucketCorsError;

    fn start(&mut self) -> Effects {
        self.state = GetBucketCorsState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketCorsState::Init => self.start(),
            GetBucketCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                self.state = GetBucketCorsState::Finish;
                self.output = Some(match value {
                    Some(bytes) => match BucketInfo::from_bytes(&bytes) {
                        Ok(info) => info
                            .cors_configuration
                            .ok_or(GetBucketCorsError::CorsNotFound),
                        Err(err) => Err(GetBucketCorsError::ConversionError(err)),
                    },
                    None => Err(GetBucketCorsError::BucketNotFound),
                });
                smallvec![]
            }
            GetBucketCorsState::Finish => smallvec![],
            GetBucketCorsState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetBucketCorsState::Finish | GetBucketCorsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetBucketCorsState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteBucketCorsState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteBucketCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct DeleteBucketCorsOperation {
    bucket: String,
    state: DeleteBucketCorsState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<(), DeleteBucketCorsError>>,
}

impl DeleteBucketCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteBucketCorsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: DeleteBucketCorsError) -> Effects {
        self.state = DeleteBucketCorsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            DeleteBucketCorsState::Init => "Init",
            DeleteBucketCorsState::StartTransaction => "StartTransaction",
            DeleteBucketCorsState::ReadBucket => "ReadBucket",
            DeleteBucketCorsState::WriteBucket => "WriteBucket",
            DeleteBucketCorsState::CommitTransaction => "CommitTransaction",
            DeleteBucketCorsState::Finish => "Finish",
            DeleteBucketCorsState::Error => "Error",
        }
    }
}

impl Operation for DeleteBucketCorsOperation {
    type Output = Option<Result<(), DeleteBucketCorsError>>;
    type Error = DeleteBucketCorsError;

    fn start(&mut self) -> Effects {
        self.state = DeleteBucketCorsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteBucketCorsState::Init => self.start(),
            DeleteBucketCorsState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(DeleteBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = DeleteBucketCorsState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteBucketCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(DeleteBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteBucketCorsError::NoTransactionFound);
                };
                let Some(bytes) = value else {
                    return self.fail(DeleteBucketCorsError::NotFound);
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
                self.state = DeleteBucketCorsState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteBucketCorsState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(DeleteBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteBucketCorsError::NoTransactionFound);
                };
                self.state = DeleteBucketCorsState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteBucketCorsState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(DeleteBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = DeleteBucketCorsState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            DeleteBucketCorsState::Finish => smallvec![],
            DeleteBucketCorsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteBucketCorsState::Finish | DeleteBucketCorsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteBucketCorsState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeleteBucketCorsError, DeleteBucketCorsOperation, GetBucketCorsError,
        GetBucketCorsOperation, PutBucketCorsError, PutBucketCorsOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::structs::{BucketCorsConfiguration, BucketCorsRule, BucketInfo};
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
        }
    }

    fn bucket_info(cors_configuration: Option<BucketCorsConfiguration>) -> BucketInfo {
        BucketInfo {
            group_id: Ulid::r#gen(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration,
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
    async fn roundtrips_bucket_cors_configuration() {
        let context = make_context();
        let bucket = "my-bucket";
        let original = bucket_info(None);
        write_bucket(&context, bucket, &original).await;

        let config = sample_cors();
        let stored = drive(
            PutBucketCorsOperation::new(bucket.to_string(), config.clone()),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(stored, config);

        let persisted = read_bucket(&context, bucket).await;
        assert_eq!(persisted.group_id, original.group_id);
        assert_eq!(persisted.created_at, original.created_at);
        assert_eq!(persisted.created_by, original.created_by);
        assert_eq!(persisted.cors_configuration, Some(config.clone()));

        let fetched = drive(GetBucketCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetched, config);

        let deleted = drive(DeleteBucketCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deleted, Ok(()));

        let cleared = read_bucket(&context, bucket).await;
        assert_eq!(cleared.group_id, original.group_id);
        assert_eq!(cleared.created_at, original.created_at);
        assert_eq!(cleared.created_by, original.created_by);
        assert_eq!(cleared.cors_configuration, None);

        let missing_cors = drive(GetBucketCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(missing_cors, GetBucketCorsError::CorsNotFound);

        let deleted_again = drive(DeleteBucketCorsOperation::new(bucket.to_string()), &context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(deleted_again, Ok(()));
    }

    #[tokio::test]
    async fn rejects_missing_bucket_and_missing_config() {
        let bucket = "missing-bucket".to_string();

        let context = make_context();
        let put_missing = drive(
            PutBucketCorsOperation::new(bucket.clone(), sample_cors()),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(put_missing, PutBucketCorsError::NotFound);

        let get_missing = drive(GetBucketCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(get_missing, GetBucketCorsError::BucketNotFound);

        let delete_missing = drive(DeleteBucketCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap_err();
        assert_eq!(delete_missing, DeleteBucketCorsError::NotFound);

        let context = make_context();
        write_bucket(&context, &bucket, &bucket_info(None)).await;

        let get_no_config = drive(GetBucketCorsOperation::new(bucket.clone()), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(get_no_config, GetBucketCorsError::CorsNotFound);
    }
}
