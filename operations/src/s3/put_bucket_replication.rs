use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketInfo, BucketReplicationConfig, BucketReplicationTarget};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutBucketReplicationState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
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
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
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
            PutBucketReplicationState::ReadBucket => "ReadBucket",
            PutBucketReplicationState::WriteBucket => "WriteBucket",
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
                self.state = PutBucketReplicationState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketReplicationState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(PutBucketReplicationError::NoSuchBucket);
                };
                let mut info = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                info.replication = Some(self.config.clone());
                let value = match info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = PutBucketReplicationState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: self.txn_id,
                })]
            }
            PutBucketReplicationState::WriteBucket => {
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
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetBucketReplicationState {
    Init,
    ReadBucket,
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

    fn state_name(&self) -> &'static str {
        match self.state {
            GetBucketReplicationState::Init => "Init",
            GetBucketReplicationState::ReadBucket => "ReadBucket",
            GetBucketReplicationState::Finish => "Finish",
            GetBucketReplicationState::Error => "Error",
        }
    }

    fn fail(&mut self, err: GetBucketReplicationError) -> Effects {
        self.state = GetBucketReplicationState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for GetBucketReplicationOperation {
    type Output = Option<Result<BucketReplicationConfig, GetBucketReplicationError>>;
    type Error = GetBucketReplicationError;

    fn start(&mut self) -> Effects {
        self.state = GetBucketReplicationState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketReplicationState::Init => self.start(),
            GetBucketReplicationState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                self.state = GetBucketReplicationState::Finish;
                self.output = Some(match value {
                    Some(value) => BucketInfo::from_bytes(value.as_ref())
                        .map_err(GetBucketReplicationError::ConversionError)
                        .and_then(|info| {
                            info.replication.ok_or(GetBucketReplicationError::NotFound)
                        }),
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
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteBucketReplicationError {
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
            DeleteBucketReplicationState::ReadBucket => "ReadBucket",
            DeleteBucketReplicationState::WriteBucket => "WriteBucket",
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
                self.state = DeleteBucketReplicationState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.bucket.as_bytes().to_vec().into(),
                    txn_id: Some(txn_id),
                })]
            }
            DeleteBucketReplicationState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(DeleteBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(DeleteBucketReplicationError::NoTransactionFound);
                };
                // A missing bucket or config leaves nothing to clear; commit as a no-op.
                let cleared = match value {
                    Some(value) => match BucketInfo::from_bytes(value.as_ref()) {
                        Ok(info) if info.replication.is_some() => Some(BucketInfo {
                            replication: None,
                            ..info
                        }),
                        Ok(_) => None,
                        Err(err) => return self.fail(err.into()),
                    },
                    None => None,
                };
                match cleared {
                    Some(info) => {
                        let value = match info.to_bytes() {
                            Ok(value) => value,
                            Err(err) => return self.fail(err.into()),
                        };
                        self.state = DeleteBucketReplicationState::WriteBucket;
                        smallvec![Effect::Storage(StorageEffect::Write {
                            key_space: S3_BUCKET_KEYSPACE.to_string(),
                            key: self.bucket.as_bytes().to_vec().into(),
                            value: value.into(),
                            txn_id: Some(txn_id),
                        })]
                    }
                    None => {
                        self.state = DeleteBucketReplicationState::CommitTransaction;
                        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                    }
                }
            }
            DeleteBucketReplicationState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(DeleteBucketReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
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
        DeleteBucketReplicationOperation, GetBucketReplicationOperation, PutBucketReplicationError,
        PutBucketReplicationOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        ArunaArn, BucketInfo, BucketReplicationConfig, BucketReplicationTarget, RealmId,
        RoutingTarget, StorageRoutingRule,
    };
    use aruna_core::types::UserId;
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn make_target(bucket: &str) -> BucketReplicationTarget {
        let node_id = iroh::SecretKey::generate().public();
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

    async fn write_bucket(storage_handle: &storage::StorageHandle, bucket: &str) {
        let info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::now(),
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([9u8; 32])),
            cors_configuration: None,
            replication: None,
            storage_routing: vec![StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTarget::Class("cold".to_string()),
            }],
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: info.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_bucket(storage_handle: &storage::StorageHandle, bucket: &str) -> BucketInfo {
        let event = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            panic!("bucket record missing");
        };
        BucketInfo::from_bytes(value.as_ref()).unwrap()
    }

    fn make_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    #[tokio::test]
    async fn roundtrips_replication_config() {
        // The config lives on the bucket record, not in a separate keyspace.
        let temp_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let context = make_context(storage_handle.clone());

        let bucket = "my-bucket".to_string();
        write_bucket(&storage_handle, &bucket).await;
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
        assert!(
            read_bucket(&storage_handle, &bucket)
                .await
                .replication
                .is_some()
        );

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

        let info = read_bucket(&storage_handle, &bucket).await;
        assert!(info.replication.is_none());

        let missing = drive(GetBucketReplicationOperation::new(bucket), &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(missing.to_string(), "Replication config not found");
    }

    #[tokio::test]
    async fn delete_keeps_routing() {
        // Clearing replication must leave the bucket's routing rules intact.
        let temp_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let context = make_context(storage_handle.clone());

        let bucket = "routed-bucket".to_string();
        write_bucket(&storage_handle, &bucket).await;
        drive(
            PutBucketReplicationOperation::new(bucket.clone(), vec![make_target(&bucket)]),
            &context,
        )
        .await
        .unwrap();

        drive(
            DeleteBucketReplicationOperation::new(bucket.clone()),
            &context,
        )
        .await
        .unwrap();

        let info = read_bucket(&storage_handle, &bucket).await;
        assert!(info.replication.is_none());
        assert_eq!(info.storage_routing.len(), 1);
    }

    #[tokio::test]
    async fn rejects_missing_bucket() {
        // Attaching replication to a bucket that does not exist must fail.
        let temp_dir = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let context = make_context(storage_handle);

        let error = drive(
            PutBucketReplicationOperation::new("missing".to_string(), vec![make_target("missing")]),
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(error, PutBucketReplicationError::NoSuchBucket);
    }

    #[test]
    fn aborts_once() {
        // A repeated abort must not re-enqueue AbortTransaction forever.
        let mut op = PutBucketReplicationOperation::new("missing".to_string(), Vec::new());
        op.start();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        let aborting = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"missing".to_vec().into(),
            value: None,
        }));
        assert!(matches!(
            aborting.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(op.abort().is_empty());
    }
}
