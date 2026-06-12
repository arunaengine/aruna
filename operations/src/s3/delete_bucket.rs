use crate::usage_stats::{UsageCounterUpdate, UsageUpdateError};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
    S3_MULTIPART_UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobHeadKey, BucketInfo, MultipartUpload, UsageDelta, VersionKey};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteBucketState {
    Init,
    StartTransaction,
    ReadBucket,
    CheckCurrentObjects,
    CheckVersions,
    CheckMultipartUploads,
    DeleteBucket,
    UpdateUsage,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("Bucket is not empty")]
    NotEmpty,
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: DeleteBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("DeleteBucket failed")]
    DeleteBucketFailed,
}

#[derive(Debug, PartialEq)]
pub struct DeleteBucketOperation {
    bucket: String,
    state: DeleteBucketState,
    txn_id: Option<TxnId>,
    group_id: Option<GroupId>,
    usage_update: Option<UsageCounterUpdate>,
    output: Option<Result<(), DeleteBucketError>>,
}

impl DeleteBucketOperation {
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteBucketState::Init,
            txn_id: None,
            group_id: None,
            usage_update: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteBucketError) -> Effects {
        self.state = DeleteBucketState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteBucketState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = DeleteBucketState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(DeleteBucketError::NotFound);
        };
        match BucketInfo::from_bytes(value.as_ref()) {
            Ok(info) => self.group_id = Some(info.group_id),
            Err(err) => return self.emit_error(err.into()),
        }

        let prefix = match BlobHeadKey::bucket_prefix(&self.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteBucketState::CheckCurrentObjects;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_current_objects_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        if !values.is_empty() {
            return self.emit_error(DeleteBucketError::NotEmpty);
        }

        let prefix = match VersionKey::bucket_prefix(&self.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteBucketState::CheckVersions;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: Self::SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_versions_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        if !values.is_empty() {
            return self.emit_error(DeleteBucketError::NotEmpty);
        }

        self.state = DeleteBucketState::CheckMultipartUploads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_uploads_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        for (_, value) in values {
            let Ok(upload) = MultipartUpload::from_bytes(value.as_ref()) else {
                continue;
            };
            if upload.bucket == self.bucket {
                return self.emit_error(DeleteBucketError::NotEmpty);
            }
        }

        self.state = DeleteBucketState::DeleteBucket;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    self.bucket.as_bytes().to_vec().into(),
                ),
                (
                    S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                    self.bucket.as_bytes().to_vec().into(),
                ),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_bucket_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteBucketError::TransactionMissing);
        };
        let Some(group_id) = self.group_id else {
            return self.emit_error(DeleteBucketError::DeleteBucketFailed);
        };

        let mut update = UsageCounterUpdate::for_group(
            group_id,
            UsageDelta {
                buckets: -1,
                ..Default::default()
            },
        );
        self.state = DeleteBucketState::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteBucketError::TransactionMissing);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(DeleteBucketError::DeleteBucketFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.state = DeleteBucketState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.txn_id = None;
        self.state = DeleteBucketState::Finish;
        self.output = Some(Ok(()));
        smallvec![]
    }
}

impl Operation for DeleteBucketOperation {
    type Output = Option<Result<(), DeleteBucketError>>;
    type Error = DeleteBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteBucketState::Init => self.handle_init(),
            DeleteBucketState::StartTransaction => self.handle_transaction_started(event),
            DeleteBucketState::ReadBucket => self.handle_bucket_read(event),
            DeleteBucketState::CheckCurrentObjects => self.handle_current_objects_checked(event),
            DeleteBucketState::CheckVersions => self.handle_versions_checked(event),
            DeleteBucketState::CheckMultipartUploads => {
                self.handle_multipart_uploads_checked(event)
            }
            DeleteBucketState::DeleteBucket => self.handle_bucket_deleted(event),
            DeleteBucketState::UpdateUsage => self.handle_usage_update(event),
            DeleteBucketState::CommitTransaction => self.handle_transaction_committed(event),
            DeleteBucketState::Finish => smallvec![],
            DeleteBucketState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteBucketState::Finish | DeleteBucketState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteBucketState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteBucketError::DeleteBucketFailed);
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
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
    };
    use aruna_core::structs::{
        BlobVersion, BucketReplicationConfig, BucketReplicationTarget, CurrentVersionPointer,
        RealmId,
    };
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn make_replication_target(bucket: &str) -> BucketReplicationTarget {
        let node_id = iroh::SecretKey::generate().public();
        BucketReplicationTarget {
            node_id,
            realm_id: RealmId::from_bytes([9u8; 32]),
            bucket: bucket.to_string(),
            arn: format!(
                "arn:aruna:{}:{}:s3/{bucket}",
                RealmId::from_bytes([9u8; 32]),
                node_id
            ),
            replicate_delete_markers: true,
        }
    }

    #[tokio::test]
    async fn test_delete_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::new(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket.clone()), &driver_ctx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, Ok(()));

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing read result");
        };
        assert!(value.is_none());

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing replication config read result");
        };
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_delete_bucket_removes_replication_config() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::new(),
                    created_at: SystemTime::now(),
                    created_by: aruna_core::UserId::nil(RealmId::from_bytes([0u8; 32])),
                    cors_configuration: None,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketReplicationConfig {
                    targets: vec![make_replication_target(&bucket)],
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket.clone()), &driver_ctx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result, Ok(()));

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing bucket read result");
        };
        assert!(value.is_none());

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing replication config read result");
        };
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::new(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;

        let version_id = Ulid::new();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, "key").to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, "key", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(SystemTime::now(), Default::default())
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket), &driver_ctx).await;
        assert_eq!(result.unwrap_err(), DeleteBucketError::NotEmpty);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty_with_versions_only() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "bucket-a".to_string();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.clone().into(),
                value: BucketInfo {
                    group_id: Ulid::new(),
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, "deleted-key", Ulid::new())
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::deleted(SystemTime::now(), Default::default())
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(DeleteBucketOperation::new(bucket), &driver_ctx).await;
        assert_eq!(result.unwrap_err(), DeleteBucketError::NotEmpty);
    }
}
