use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    S3_LOOKUP_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_VERSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    Location, LookupKey, MultipartObjectMetadataKey, VersionKey, VersionMetadata,
};
use aruna_core::types::{Effects, Key, UserId};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteObjectState {
    Init,
    StartTransaction,
    ReadTargetVersion,
    ReadAllVersions,
    WriteCurrentLookup,
    DeleteCurrentLookup,
    DeleteTargetVersion,
    DeleteMultipartSummary,
    ReadMultipartParts,
    DeleteMultipartPart,
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
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("DeleteObject failed")]
    DeleteObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub deleted_by: UserId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteObjectResult {
    pub version_id: Ulid,
    pub delete_marker: bool,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectOperation {
    input: DeleteObjectInput,
    state: DeleteObjectState,
    txn_id: Option<Ulid>,
    version_id: Option<Ulid>,
    target_version: Option<VersionMetadata>,
    latest_remaining: Option<VersionMetadata>,
    multipart_part_keys: Vec<Key>,
    multipart_delete_index: usize,
    output: Option<Result<DeleteObjectResult, DeleteObjectError>>,
}

impl DeleteObjectOperation {
    pub fn new(input: DeleteObjectInput) -> Self {
        Self {
            input,
            state: DeleteObjectState::Init,
            txn_id: None,
            version_id: None,
            target_version: None,
            latest_remaining: None,
            multipart_part_keys: Vec::new(),
            multipart_delete_index: 0,
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
        if let Some(version_id) = self.input.version_id {
            self.read_target_version(version_id)
        } else {
            self.write_tombstone()
        }
    }

    fn read_target_version(&mut self, version_id: Ulid) -> Effects {
        self.state = DeleteObjectState::ReadTargetVersion;
        let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_target_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(value) = value else {
            return self.emit_error(DeleteObjectError::NoSuchVersion);
        };
        let metadata = match VersionMetadata::from_bytes(value.as_ref()) {
            Ok(metadata) => metadata,
            Err(err) => return self.emit_error(err.into()),
        };
        self.target_version = Some(metadata);

        self.state = DeleteObjectState::ReadAllVersions;
        let prefix = match VersionKey::object_prefix(&self.input.bucket, &self.input.key) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start_after: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn handle_all_versions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(target_version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.latest_remaining = values
            .into_iter()
            .filter_map(|(key, value)| {
                let version_key = VersionKey::from_bytes(key.as_ref()).ok()?;
                if version_key.version_id == target_version_id {
                    return None;
                }
                let metadata = VersionMetadata::from_bytes(value.as_ref()).ok()?;
                Some((version_key.version_id, metadata))
            })
            .max_by_key(|(version_id, _)| *version_id)
            .map(|(_, metadata)| metadata);

        match self.latest_remaining.clone() {
            Some(metadata) => self.write_current_lookup(metadata.location),
            None => self.delete_current_lookup(),
        }
    }

    fn write_current_lookup(&mut self, location: Location) -> Effects {
        self.state = DeleteObjectState::WriteCurrentLookup;
        let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };
        let value = match location.to_bytes() {
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

    fn delete_current_lookup(&mut self) -> Effects {
        self.state = DeleteObjectState::DeleteCurrentLookup;
        let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_current_lookup_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.delete_target_version()
    }

    fn handle_current_lookup_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.delete_target_version()
    }

    fn delete_target_version(&mut self) -> Effects {
        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.state = DeleteObjectState::DeleteTargetVersion;
        let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_target_version_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.state = DeleteObjectState::DeleteMultipartSummary;
        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_summary_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let prefix = match MultipartObjectMetadataKey::part_prefix(version_id) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteObjectState::ReadMultipartParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start_after: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_part_keys = values.into_iter().map(|(key, _)| key).collect();
        self.multipart_delete_index = 0;
        self.delete_next_multipart_part()
    }

    fn delete_next_multipart_part(&mut self) -> Effects {
        let Some(key) = self
            .multipart_part_keys
            .get(self.multipart_delete_index)
            .cloned()
        else {
            let Some(txn_id) = self.txn_id else {
                return self.emit_error(DeleteObjectError::NoTransactionFound);
            };
            self.state = DeleteObjectState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        };

        self.state = DeleteObjectState::DeleteMultipartPart;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_part_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_delete_index += 1;
        self.delete_next_multipart_part()
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
            if let Some(version_id) = self.input.version_id {
                let delete_marker = self
                    .target_version
                    .as_ref()
                    .is_some_and(|metadata| metadata.location == Location::Deleted);
                self.txn_id = None;
                self.state = DeleteObjectState::Finish;
                self.output = Some(Ok(DeleteObjectResult {
                    version_id,
                    delete_marker,
                }));
                return smallvec![];
            }
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = DeleteObjectState::Finish;
        self.output = Some(Ok(DeleteObjectResult {
            version_id,
            delete_marker: true,
        }));
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
            DeleteObjectState::ReadTargetVersion => self.handle_target_version_read(event),
            DeleteObjectState::ReadAllVersions => self.handle_all_versions_read(event),
            DeleteObjectState::WriteCurrentLookup => self.handle_current_lookup_written(event),
            DeleteObjectState::DeleteCurrentLookup => self.handle_current_lookup_deleted(event),
            DeleteObjectState::DeleteTargetVersion => self.handle_target_version_deleted(event),
            DeleteObjectState::DeleteMultipartSummary => {
                self.handle_multipart_summary_deleted(event)
            }
            DeleteObjectState::ReadMultipartParts => self.handle_multipart_parts_read(event),
            DeleteObjectState::DeleteMultipartPart => self.handle_multipart_part_deleted(event),
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
        Backend, BackendConfig, Location, LookupKey, RealmId, VersionKey, VersionMetadata,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::fs::exists;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_delete_object_tombstone() {
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
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
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
            metadata_handle: None,
            task_handle: None,
        };

        let user_id = aruna_core::UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32]));
        let put_result = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id: Ulid::new(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "to-delete.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
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
                version_id: None,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(exists(put_result.location.get_full_path().unwrap()).unwrap());

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
                version_id: None,
                range: None,
                group_id: Ulid::new(),
                user_identity: user_id,
            }),
            &context,
        )
        .await;
        assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));
    }

    #[tokio::test]
    async fn test_delete_object_version() {
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
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
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
            metadata_handle: None,
            task_handle: None,
        };

        let user_id = aruna_core::UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32]));
        let put_result = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id: Ulid::new(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "versioned.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let tombstone = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(tombstone.delete_marker);

        let delete_marker_result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: Some(tombstone.version_id),
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(delete_marker_result.delete_marker);

        let mut restored_blob = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::new(),
                user_identity: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .blob;
        let restored = restored_blob.next().await.unwrap().unwrap();
        assert_eq!(restored.as_ref(), b"hello");

        let removed_object_version = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: Some(put_result.version_id),
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(!removed_object_version.delete_marker);

        let get_result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::new(),
                user_identity: user_id,
            }),
            &context,
        )
        .await;
        assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_LOOKUP_KEYSPACE.to_string(),
                key: LookupKey::object("mybucket", "versioned.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing object lookup read result");
        };
        assert!(value.is_none());
    }
}
