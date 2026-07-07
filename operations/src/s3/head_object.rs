use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectSummary, SourceMetadata,
    VersionKey,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadObjectState {
    Init,
    StartTransaction,
    GetVersion,
    GetBlobLocation,
    GetCurrentVersion,
    ReadMultipartSummary,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: HeadObjectState,
        expected: HeadObjectState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: HeadObjectState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error("HeadObject failed")]
    HeadObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct HeadObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadObjectResult {
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub composite_hashes: HashMap<String, Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct HeadObjectOperation {
    input: HeadObjectInput,
    state: HeadObjectState,
    txn_id: Option<Ulid>,
    location: Option<BackendLocation>,
    source_metadata: Option<SourceMetadata>,
    last_refresh: Option<SystemTime>,
    resolved_version_id: Option<Ulid>,
    checksum_type: MultipartChecksumType,
    composite_hashes: HashMap<String, Vec<u8>>,
    output: Option<Result<HeadObjectResult, HeadObjectError>>,
}

impl HeadObjectOperation {
    pub fn new(input: HeadObjectInput) -> Self {
        Self {
            input,
            state: HeadObjectState::Init,
            txn_id: None,
            location: None,
            source_metadata: None,
            last_refresh: None,
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            composite_hashes: HashMap::new(),
            output: None,
        }
    }

    fn emit_error(&mut self, error: HeadObjectError) -> Effects {
        self.state = HeadObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.state != HeadObjectState::Init {
            self.emit_error(HeadObjectError::InvalidState {
                current: self.state.clone(),
                expected: HeadObjectState::Init,
            })
        } else {
            self.state = HeadObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            if let Some(version_id) = self.input.version_id {
                self.state = HeadObjectState::GetVersion;
                let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id)
                    .to_bytes()
                {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            } else {
                self.state = HeadObjectState::GetCurrentVersion;
                let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            }
        } else {
            self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            })
        }
    }

    fn handle_received_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(if self.input.version_id.is_some() {
                HeadObjectError::NoSuchVersion
            } else {
                HeadObjectError::NoSuchKey
            });
        };

        let version = match BlobVersion::from_bytes(val.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        let Some(version_id) = self.resolved_version_id.or(self.input.version_id) else {
            return self.emit_error(HeadObjectError::HeadObjectFailed);
        };

        self.read_version(version_id, version, self.input.version_id.is_some())
    }

    fn handle_received_current_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(HeadObjectError::NoSuchKey);
        };

        let pointer = match CurrentVersionPointer::from_bytes(val.as_ref()) {
            Ok(pointer) => pointer,
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.resolved_version_id = Some(pointer.version_id);
        self.state = HeadObjectState::GetVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn read_version(
        &mut self,
        version_id: Ulid,
        version: BlobVersion,
        explicit_version_request: bool,
    ) -> Effects {
        self.resolved_version_id = Some(version_id);

        match version.state {
            BlobVersionState::Materialized { blob_hash, .. } => {
                self.source_metadata = None;
                self.last_refresh = None;
                self.read_blob_location(blob_hash)
            }
            BlobVersionState::Deleted => self.emit_error(if explicit_version_request {
                HeadObjectError::DeleteMarker
            } else {
                HeadObjectError::NoSuchKey
            }),
            BlobVersionState::Reference {
                cached_metadata,
                last_refresh,
                ..
            } => {
                self.location = None;
                self.source_metadata = Some(cached_metadata);
                self.last_refresh = Some(last_refresh);
                self.finish_lookup()
            }
        }
    }

    fn read_blob_location(&mut self, blob_hash: [u8; 32]) -> Effects {
        self.state = HeadObjectState::GetBlobLocation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: blob_hash.to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_blob_location_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(HeadObjectError::HeadObjectFailed);
        };

        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.read_multipart_summary(location, self.resolved_version_id)
    }

    fn read_multipart_summary(
        &mut self,
        location: BackendLocation,
        resolved_version_id: Option<Ulid>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };

        self.location = Some(location);
        self.resolved_version_id = resolved_version_id;

        let Some(version_id) = resolved_version_id else {
            return self.finish_lookup();
        };

        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(HeadObjectError::ConversionError(err)),
        };

        self.state = HeadObjectState::ReadMultipartSummary;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_multipart_summary_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        if let Some(summary) =
            value.and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok())
        {
            self.checksum_type = summary.checksum_type;
            self.composite_hashes = summary.composite_hashes;
        }

        self.finish_lookup()
    }

    fn finish_lookup(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(HeadObjectError::NoTransactionFound);
        };
        if self.location.is_none() && self.source_metadata.is_none() {
            return self.emit_error(HeadObjectError::HeadObjectFailed);
        }

        self.state = HeadObjectState::CommitTransaction;
        self.output = Some(Ok(HeadObjectResult {
            location: self.location.clone(),
            source_metadata: self.source_metadata.clone(),
            last_refresh: self.last_refresh,
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            checksum_type: self.checksum_type,
            composite_hashes: self.composite_hashes.clone(),
        }));

        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.state = HeadObjectState::Finish;
            smallvec![]
        } else {
            self.emit_error(HeadObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            })
        }
    }
}

impl Operation for HeadObjectOperation {
    type Output = Option<Result<HeadObjectResult, HeadObjectError>>;
    type Error = HeadObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(HeadObjectError::StorageError(error));
        }

        match self.state {
            HeadObjectState::Init => self.handle_init(),
            HeadObjectState::StartTransaction => self.handle_transaction_started(event),
            HeadObjectState::GetVersion => self.handle_received_version(event),
            HeadObjectState::GetBlobLocation => self.handle_blob_location_read(event),
            HeadObjectState::GetCurrentVersion => self.handle_received_current_version(event),
            HeadObjectState::ReadMultipartSummary => self.handle_multipart_summary_read(event),
            HeadObjectState::CommitTransaction => self.handle_transaction_committed(event),
            HeadObjectState::Finish | HeadObjectState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, HeadObjectState::Finish | HeadObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if HeadObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(HeadObjectError::HeadObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::BackendConfig;
    use aruna_core::structs::BackendLocation;
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::{
        Backend, BlobHeadKey, BlobVersion, CurrentVersionPointer, PortableSourceDescriptor,
        SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
    };
    use aruna_net::{NetConfig, NetHandle};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn location_with_hash() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), vec![2; 32]);
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::r#gen(),
            compressed: false,
            encrypted: false,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 5,
            hashes,
        }
    }

    #[tokio::test]
    async fn head_object_reads_current_version_pointer() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::r#gen();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = BlobHeadKey::new("mybucket", "hello.txt")
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: key.into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "hello.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(
                    location.get_blake3().unwrap().try_into().unwrap(),
                    location.created_at,
                    location.created_by,
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: location.get_blake3().unwrap().to_vec().into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[tokio::test]
    async fn head_object_reads_specific_version() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::r#gen();
        let metadata = BlobVersion::materialized(
            location.get_blake3().unwrap().try_into().unwrap(),
            SystemTime::now(),
            Default::default(),
            None,
        );

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = VersionKey::new("mybucket", "hello.txt", version_id)
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: key.into(),
                value: metadata.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: location.get_blake3().unwrap().to_vec().into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[tokio::test]
    async fn head_object_returns_cached_reference_metadata() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let version_id = Ulid::r#gen();
        let cached_metadata = SourceMetadata {
            content_length: 11,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-123".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        };
        let last_refresh = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(17);
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::r#gen()),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        let key = BlobHeadKey::new("mybucket", "hello.txt")
            .to_bytes()
            .unwrap();
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: key.into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "hello.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::reference(
                    source,
                    cached_metadata.clone(),
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    last_refresh,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            HeadObjectOperation::new(HeadObjectInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.location.is_none());
        assert_eq!(result.source_metadata, Some(cached_metadata));
        assert_eq!(result.last_refresh, Some(last_refresh));
        assert_eq!(result.version_id, Some(version_id));
    }
}
