use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_REPLICATION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketReplicationConfig, BucketReplicationTarget};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutBucketReplicationState {
    Init,
    StartTransaction,
    WriteConfig,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketReplicationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
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
pub struct PutBucketReplicationOperation {
    bucket: String,
    config: BucketReplicationConfig,
    state: PutBucketReplicationState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BucketReplicationConfig, PutBucketReplicationError>>,
}

impl PutBucketReplicationOperation {
    pub fn new(bucket: String, targets: Vec<BucketReplicationTarget>) -> Self {
        Self {
            bucket,
            config: BucketReplicationConfig { targets },
            state: PutBucketReplicationState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutBucketReplicationError) -> Effects {
        self.state = PutBucketReplicationState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutBucketReplicationState::Init => "Init",
            PutBucketReplicationState::StartTransaction => "StartTransaction",
            PutBucketReplicationState::WriteConfig => "WriteConfig",
            PutBucketReplicationState::CommitTransaction => "CommitTransaction",
            PutBucketReplicationState::Finish => "Finish",
            PutBucketReplicationState::Error => "Error",
        }
    }
}

impl Operation for PutBucketReplicationOperation {
    type Output = Option<Result<BucketReplicationConfig, PutBucketReplicationError>>;
    type Error = PutBucketReplicationError;

    fn start(&mut self) -> Effects {
        self.state = PutBucketReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutBucketReplicationState::Init => self.start(),
            PutBucketReplicationState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutBucketReplicationState::WriteConfig;
                let value = match self.config.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketReplicationState::WriteConfig => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketReplicationError::NoTransactionFound);
                };
                self.state = PutBucketReplicationState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutBucketReplicationState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutBucketReplicationState::Finish;
                self.output = Some(Ok(self.config.clone()));
                smallvec![]
            }
            PutBucketReplicationState::Finish => smallvec![],
            PutBucketReplicationState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutBucketReplicationState::Finish | PutBucketReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == PutBucketReplicationState::Error
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
enum GetBucketReplicationState {
    Init,
    ReadConfig,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketReplicationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Replication config not found")]
    NotFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetBucketReplicationOperation {
    bucket: String,
    state: GetBucketReplicationState,
    output: Option<Result<BucketReplicationConfig, GetBucketReplicationError>>,
}

impl GetBucketReplicationOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketReplicationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GetBucketReplicationError) -> Effects {
        self.state = GetBucketReplicationState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetBucketReplicationState::Init => "Init",
            GetBucketReplicationState::ReadConfig => "ReadConfig",
            GetBucketReplicationState::Finish => "Finish",
            GetBucketReplicationState::Error => "Error",
        }
    }
}

impl Operation for GetBucketReplicationOperation {
    type Output = Option<Result<BucketReplicationConfig, GetBucketReplicationError>>;
    type Error = GetBucketReplicationError;

    fn start(&mut self) -> Effects {
        self.state = GetBucketReplicationState::ReadConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketReplicationState::Init => self.start(),
            GetBucketReplicationState::ReadConfig => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                self.state = GetBucketReplicationState::Finish;
                self.output = Some(match value {
                    Some(value) => BucketReplicationConfig::from_bytes(value.as_ref())
                        .map_err(GetBucketReplicationError::ConversionError),
                    None => Err(GetBucketReplicationError::NotFound),
                });
                smallvec![]
            }
            GetBucketReplicationState::Finish => smallvec![],
            GetBucketReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetBucketReplicationState::Finish | GetBucketReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetBucketReplicationState::Error
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
enum DeleteBucketReplicationState {
    Init,
    StartTransaction,
    DeleteConfig,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteBucketReplicationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
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
pub struct DeleteBucketReplicationOperation {
    bucket: String,
    state: DeleteBucketReplicationState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<(), DeleteBucketReplicationError>>,
}

impl DeleteBucketReplicationOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteBucketReplicationState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: DeleteBucketReplicationError) -> Effects {
        self.state = DeleteBucketReplicationState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            DeleteBucketReplicationState::Init => "Init",
            DeleteBucketReplicationState::StartTransaction => "StartTransaction",
            DeleteBucketReplicationState::DeleteConfig => "DeleteConfig",
            DeleteBucketReplicationState::CommitTransaction => "CommitTransaction",
            DeleteBucketReplicationState::Finish => "Finish",
            DeleteBucketReplicationState::Error => "Error",
        }
    }
}

impl Operation for DeleteBucketReplicationOperation {
    type Output = Option<Result<(), DeleteBucketReplicationError>>;
    type Error = DeleteBucketReplicationError;

    fn start(&mut self) -> Effects {
        self.state = DeleteBucketReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteBucketReplicationState::Init => self.start(),
            DeleteBucketReplicationState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(DeleteBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = DeleteBucketReplicationState::DeleteConfig;
                smallvec![Effect::Storage(StorageEffect::Delete {
                    key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                    key: self.bucket.as_bytes().to_vec().into(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteBucketReplicationState::DeleteConfig => {
                let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
                    return self.fail(DeleteBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::DeleteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteBucketReplicationError::NoTransactionFound);
                };
                self.state = DeleteBucketReplicationState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            DeleteBucketReplicationState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(DeleteBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = DeleteBucketReplicationState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            DeleteBucketReplicationState::Finish => smallvec![],
            DeleteBucketReplicationState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteBucketReplicationState::Finish | DeleteBucketReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteBucketReplicationState::Error
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
        DeleteBucketReplicationOperation, GetBucketReplicationOperation,
        PutBucketReplicationOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_BUCKET_REPLICATION_KEYSPACE;
    use aruna_core::structs::{
        ArunaArn, BucketReplicationConfig, BucketReplicationTarget, RealmId,
    };
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn make_target(bucket: &str) -> BucketReplicationTarget {
        let mut rng = rand::rng();
        let node_id = iroh::SecretKey::generate(&mut rng).public();
        BucketReplicationTarget {
            node_id,
            realm_id: RealmId::from_bytes([9u8; 32]),
            bucket: bucket.to_string(),
            arn: ArunaArn::s3_bucket(RealmId::from_bytes([9u8; 32]), node_id, bucket)
                .unwrap()
                .to_string(),
            replicate_delete_markers: true,
        }
    }

    #[tokio::test]
    async fn roundtrips_bucket_replication_config() {
        let temp_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "my-bucket".to_string();
        let targets = vec![make_target(&bucket)];
        let stored = drive(
            PutBucketReplicationOperation::new(bucket.clone(), targets.clone()),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(stored.targets, targets);

        let fetched = drive(GetBucketReplicationOperation::new(bucket.clone()), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fetched, BucketReplicationConfig { targets });

        let deleted = drive(
            DeleteBucketReplicationOperation::new(bucket.clone()),
            &context,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(deleted, Ok(()));

        let missing = drive(GetBucketReplicationOperation::new(bucket), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(missing.to_string(), "Replication config not found");

        let event = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: b"my-bucket".to_vec().into(),
                txn_id: None,
            })
            .await;
        if let Event::Storage(StorageEvent::ReadResult { value, .. }) = event {
            assert!(value.is_none());
        } else {
            panic!("unexpected event");
        }
    }
}
