use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{Location, LookupKey, VersionKey, VersionMetadata};
use aruna_core::types::{Effects, UserId};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteObjectState {
    Init,
    StartTransaction,
    WriteTombstone,
    WriteVersion,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("DeleteObject failed")]
    DeleteObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectInput {
    pub bucket: String,
    pub key: String,
    pub deleted_by: UserId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteObjectResult {
    pub version_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectOperation {
    input: DeleteObjectInput,
    state: DeleteObjectState,
    txn_id: Option<Ulid>,
    version_id: Option<Ulid>,
    output: Option<Result<DeleteObjectResult, DeleteObjectError>>,
}

impl DeleteObjectOperation {
    pub fn new(input: DeleteObjectInput) -> Self {
        Self {
            input,
            state: DeleteObjectState::Init,
            txn_id: None,
            version_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteObjectError) -> Effects {
        self.state = DeleteObjectState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteObjectState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.txn_id = Some(txn_id);
        self.write_tombstone()
    }

    fn write_tombstone(&mut self) -> Effects {
        self.state = DeleteObjectState::WriteTombstone;
        let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };
        let value = match Location::Deleted.to_bytes() {
            Ok(value) => value.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })]
    }

    fn handle_tombstone_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let version_id = Ulid::new();
        self.version_id = Some(version_id);
        self.state = DeleteObjectState::WriteVersion;

        let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };
        let value = match (VersionMetadata {
            version_id,
            location: Location::Deleted,
            created_at: SystemTime::now(),
            created_by: self.input.deleted_by,
        })
        .to_bytes()
        {
            Ok(value) => value.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key,
            value,
            txn_id: self.txn_id,
        })]
    }

    fn handle_version_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteObjectError::NoTransactionFound);
        };
        self.state = DeleteObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = DeleteObjectState::Finish;
        self.output = Some(Ok(DeleteObjectResult { version_id }));
        smallvec![]
    }
}

impl Operation for DeleteObjectOperation {
    type Output = Option<Result<DeleteObjectResult, DeleteObjectError>>;
    type Error = DeleteObjectError;

    fn start(&mut self) -> Effects {
        if self.state != DeleteObjectState::Init {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        }
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteObjectState::Init => self.handle_init(),
            DeleteObjectState::StartTransaction => self.handle_transaction_started(event),
            DeleteObjectState::WriteTombstone => self.handle_tombstone_written(event),
            DeleteObjectState::WriteVersion => self.handle_version_written(event),
            DeleteObjectState::CommitTransaction => self.handle_transaction_committed(event),
            DeleteObjectState::Finish => smallvec![],
            DeleteObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteObjectState::Finish | DeleteObjectState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteObjectState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteObjectError::DeleteObjectFailed);
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
    use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE};
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Backend, BackendConfig, Location, LookupKey, RealmId, UserIdentity, VersionKey,
        VersionMetadata,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::fs::exists;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_delete_object_writes_tombstone_and_preserves_blob() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();

        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                root: blob_root,
                service_config: HashMap::new(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            task_handle: None,
        };

        let user_id = Ulid::new();
        let put_result = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id: Ulid::new(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "to-delete.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                exists: false,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let delete_result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "to-delete.txt".to_string(),
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(exists(put_result.get_full_path().unwrap()).unwrap());

        let object_lookup_key = LookupKey::object("mybucket", "to-delete.txt")
            .to_bytes()
            .unwrap();
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(object_lookup_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key: object_lookup_key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing tombstone lookup entry");
        };
        assert_eq!(
            Location::from_bytes(object_lookup_value.as_ref()).unwrap(),
            Location::Deleted
        );

        let version_prefix = VersionKey::object_prefix("mybucket", "to-delete.txt").unwrap();
        let Event::Storage(StorageEvent::IterResult { values, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                prefix: Some(version_prefix.into()),
                start_after: None,
                limit: 10,
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata");
        };
        assert_eq!(values.len(), 2);
        let tombstone = values
            .iter()
            .map(|(_, value)| VersionMetadata::from_bytes(value.as_ref()).unwrap())
            .find(|metadata| metadata.location == Location::Deleted)
            .expect("missing tombstone version metadata");
        assert_eq!(tombstone.version_id, delete_result.version_id);
        assert_eq!(tombstone.location, Location::Deleted);
        assert_eq!(tombstone.created_by, user_id);

        let get_result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "to-delete.txt".to_string(),
                range: None,
                group_id: Ulid::new(),
                user_identity: UserIdentity {
                    user_id,
                    realm_key: RealmId([0u8; 32]),
                },
            }),
            &context,
        )
        .await;
        assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));
    }
}
