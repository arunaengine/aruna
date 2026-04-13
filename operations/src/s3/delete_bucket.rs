use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE, S3_VERSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketInfo, LookupKey, MultipartUpload, VersionKey};
use aruna_core::types::{Effects, TxnId};
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
    #[error("DeleteBucket failed")]
    DeleteBucketFailed,
}

#[derive(Debug, PartialEq)]
pub struct DeleteBucketOperation {
    bucket: String,
    state: DeleteBucketState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), DeleteBucketError>>,
}

impl DeleteBucketOperation {
    const SCAN_LIMIT: usize = 10_000;

    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: DeleteBucketState::Init,
            txn_id: None,
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
        if let Err(err) = BucketInfo::from_bytes(value.as_ref()) {
            return self.emit_error(err.into());
        }

        self.state = DeleteBucketState::CheckCurrentObjects;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
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

        for (key, _) in values {
            let Ok(lookup_key) = LookupKey::from_bytes(key.as_ref()) else {
                continue;
            };
            if matches!(lookup_key, LookupKey::Object { bucket, .. } if bucket == self.bucket) {
                return self.emit_error(DeleteBucketError::NotEmpty);
            }
        }

        self.state = DeleteBucketState::CheckVersions;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
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

        for (key, _) in values {
            let Ok(version_key) = VersionKey::from_bytes(key.as_ref()) else {
                continue;
            };
            if version_key.bucket == self.bucket {
                return self.emit_error(DeleteBucketError::NotEmpty);
            }
        }

        self.state = DeleteBucketState::CheckMultipartUploads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
            limit: Self::SCAN_LIMIT,
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
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_bucket_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::DeleteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteBucketError::TransactionMissing);
        };

        self.state = DeleteBucketState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
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
    use aruna_core::structs::{BackendLocation, Location, VersionMetadata};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_delete_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
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
                    created_by: Ulid::new(),
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
            automerge_handle: None,
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
                    created_by: Ulid::new(),
                }
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await;

        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: format!("obj/key_{}", Ulid::new()),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: Ulid::new(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::from([("blake3".to_string(), vec![0u8; 32])]),
        };
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key: LookupKey::object(&bucket, "key").to_bytes().unwrap().into(),
                value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, "key", Ulid::new())
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: VersionMetadata {
                    version_id: Ulid::new(),
                    location: Location::Real(location),
                    created_at: SystemTime::now(),
                    created_by: Ulid::new(),
                }
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
