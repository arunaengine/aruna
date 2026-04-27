use crate::connectors::{
    ResolveVersionSourceBindingInput, resolve_version_source_binding_suboperation,
};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{
    ConversionError, SourceConnectorResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    S3_CURRENT_VERSION_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_VERSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    BackendLocation, CurrentVersionPointer, LookupKey, MultipartChecksumType,
    MultipartObjectMetadataKey, MultipartObjectSummary, ResolvedSourceAccess, SourceMetadata,
    VersionKey, VersionMetadata, VersionState,
};
use aruna_core::types::Effects;
use bytes::Bytes;
use smallvec::{SmallVec, smallvec};
use std::ops::Range;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetObjectState {
    Init,
    StartTransaction,
    GetVersion,
    GetCurrentVersion,
    ResolveReferenceAccess,
    ReadMultipartSummary,
    CommitTransaction,
    GetBlob,
    ReadReferenceSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: GetObjectState,
        expected: GetObjectState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetObjectState,
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
    #[error(transparent)]
    ResolveReferenceError(#[from] SourceConnectorResolutionError),
    #[error(transparent)]
    StagingSourceError(#[from] StagingSourceError),
    #[error("GetObject failed (miserably)")]
    GetObjectFailed,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub range: Option<Range<u64>>,
    pub group_id: Ulid,
    pub user_identity: UserId,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectResult {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectOperation {
    input: GetObjectInput,
    state: GetObjectState,
    txn_id: Option<Ulid>,
    location: Option<BackendLocation>,
    reference_access: Option<ResolvedSourceAccess>,
    reference_stream: Option<BackendStream<Result<Bytes, StreamError>>>,
    source_metadata: Option<SourceMetadata>,
    last_refresh: Option<SystemTime>,
    resolved_version_id: Option<Ulid>,
    checksum_type: MultipartChecksumType,
    output: Option<Result<GetObjectResult, GetObjectError>>,
}

impl GetObjectOperation {
    pub fn new(input: GetObjectInput) -> Self {
        GetObjectOperation {
            input,
            state: GetObjectState::Init,
            txn_id: None,
            location: None,
            reference_access: None,
            reference_stream: None,
            source_metadata: None,
            last_refresh: None,
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            output: None,
        }
    }

    pub fn emit_error(&mut self, error: GetObjectError) -> Effects {
        self.state = GetObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    pub fn handle_init(&mut self) -> Effects {
        if self.state != GetObjectState::Init {
            self.emit_error(GetObjectError::InvalidState {
                current: self.state.clone(),
                expected: GetObjectState::Init,
            })
        } else {
            self.state = GetObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        }
    }

    pub fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            if let Some(version_id) = self.input.version_id {
                self.state = GetObjectState::GetVersion;
                let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id)
                    .to_bytes()
                {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_VERSION_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            } else {
                self.state = GetObjectState::GetCurrentVersion;

                let key = match LookupKey::object(&self.input.bucket, &self.input.key).to_bytes() {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            }
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            })
        }
    }

    pub fn handle_received_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(if self.input.version_id.is_some() {
                GetObjectError::NoSuchVersion
            } else {
                GetObjectError::NoSuchKey
            });
        };

        let metadata = match VersionMetadata::from_bytes(val.as_ref()) {
            Ok(metadata) => metadata,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.read_version(metadata, self.input.version_id.is_some())
    }

    fn handle_received_current_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(GetObjectError::NoSuchKey);
        };

        let pointer = match CurrentVersionPointer::from_bytes(val.as_ref()) {
            Ok(pointer) => pointer,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.resolved_version_id = Some(pointer.version_id);
        self.state = GetObjectState::GetVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn read_version(
        &mut self,
        metadata: VersionMetadata,
        explicit_version_request: bool,
    ) -> Effects {
        let version_id = metadata.version_id;

        let location = match metadata.state {
            VersionState::Materialized { location, .. } => location,
            VersionState::Deleted => {
                return self.emit_error(if explicit_version_request {
                    GetObjectError::DeleteMarker
                } else {
                    GetObjectError::NoSuchKey
                });
            }
            VersionState::Reference { source, .. } => {
                self.location = None;
                self.reference_access = None;
                self.reference_stream = None;
                self.source_metadata = None;
                self.last_refresh = None;
                self.resolved_version_id = Some(version_id);
                self.state = GetObjectState::ResolveReferenceAccess;
                return smallvec![resolve_version_source_binding_suboperation(
                    ResolveVersionSourceBindingInput { source },
                )];
            }
        };

        self.read_multipart_summary(location, Some(version_id))
    }

    fn handle_resolved_reference_access(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(access),
            }) => {
                self.reference_access = Some(access);
                self.commit_and_read_reference()
            }
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Err(error),
            }) => self.emit_error(error.into()),
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                received: other,
            }),
        }
    }

    fn read_multipart_summary(
        &mut self,
        location: BackendLocation,
        resolved_version_id: Option<Ulid>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };

        self.location = Some(location);
        self.resolved_version_id = resolved_version_id;

        let Some(version_id) = resolved_version_id else {
            return self.commit_and_read_blob();
        };

        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.state = GetObjectState::ReadMultipartSummary;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    pub fn handle_multipart_summary_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        self.checksum_type = value
            .and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok())
            .map(|summary| summary.checksum_type)
            .unwrap_or(MultipartChecksumType::FullObject);

        self.commit_and_read_blob()
    }

    fn commit_and_read_blob(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        let Some(location) = self.location.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        let read_effect = match self.input.range.clone() {
            Some(range) => BlobEffect::ReadRange { location, range },
            None => BlobEffect::Read { location },
        };

        self.state = GetObjectState::CommitTransaction;
        smallvec![
            Effect::Storage(StorageEffect::CommitTransaction { txn_id }),
            Effect::Blob(read_effect)
        ]
    }

    fn commit_and_read_reference(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        let Some(access) = self.reference_access.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        self.state = GetObjectState::CommitTransaction;
        smallvec![
            Effect::Storage(StorageEffect::CommitTransaction { txn_id }),
            Effect::StagingSource(StagingSourceEffect::Read {
                access,
                range: self.input.range.clone(),
            })
        ]
    }

    pub fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.txn_id = None;
            self.state = if self.reference_access.is_some() {
                GetObjectState::ReadReferenceSource
            } else {
                GetObjectState::GetBlob
            };
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            })
        }
    }

    pub fn handle_received_blob(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = event {
            let Some(location) = self.location.clone() else {
                return self.emit_error(GetObjectError::GetObjectFailed);
            };
            self.state = GetObjectState::Finish;
            self.output = Some(Ok(GetObjectResult {
                blob,
                location: Some(location),
                source_metadata: None,
                last_refresh: None,
                version_id: self.resolved_version_id.or(self.input.version_id),
                resolved_version_id: self.resolved_version_id,
                checksum_type: self.checksum_type,
            }));
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Blob(BlobEvent::ReadFinished)",
                received: event,
            })
        }
    }

    pub fn handle_received_reference_source(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                self.last_refresh = Some(SystemTime::now());
                self.source_metadata = Some(metadata);
                self.reference_stream = Some(stream);
                self.finish_reference_output()
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::ReadResult)",
                received: other,
            }),
        }
    }

    fn finish_reference_output(&mut self) -> Effects {
        let Some(blob) = self.reference_stream.take() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        let Some(source_metadata) = self.source_metadata.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        self.state = GetObjectState::Finish;
        self.output = Some(Ok(GetObjectResult {
            blob,
            location: None,
            source_metadata: Some(source_metadata),
            last_refresh: self.last_refresh,
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            checksum_type: self.checksum_type,
        }));
        smallvec![]
    }
}

impl Operation for GetObjectOperation {
    type Output = Option<Result<GetObjectResult, GetObjectError>>;
    type Error = GetObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            GetObjectState::Init => self.handle_init(),
            GetObjectState::StartTransaction => self.handle_transaction_started(event),
            GetObjectState::GetVersion => self.handle_received_version(event),
            GetObjectState::GetCurrentVersion => self.handle_received_current_version(event),
            GetObjectState::ResolveReferenceAccess => self.handle_resolved_reference_access(event),
            GetObjectState::ReadMultipartSummary => self.handle_multipart_summary_read(event),
            GetObjectState::CommitTransaction => self.handle_transaction_committed(event),
            GetObjectState::GetBlob => self.handle_received_blob(event),
            GetObjectState::ReadReferenceSource => self.handle_received_reference_source(event),
            GetObjectState::Finish => smallvec![],
            GetObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetObjectState::Finish | GetObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if GetObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetObjectError::GetObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::s3::get_object::{GetObjectInput, GetObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_blob::hash::Hasher;
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        S3_CURRENT_VERSION_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE,
    };
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, CurrentVersionPointer, Location, LookupKey,
        MultipartChecksumType, PortableSourceDescriptor, RealmId, SourceConnectorKind,
        SourceMetadata, StagingStrategy, VersionKey, VersionMetadata, VersionSourceBinding,
        VersionState,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use ulid::Ulid;

    async fn spawn_reference_server(body: &'static [u8]) -> String {
        let app =
            Router::new().route(
                "/folder/file.txt",
                get(move || async move {
                    ([("content-type", "text/plain"), ("etag", "etag-123")], body)
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}", addr)
    }

    #[test]
    fn materialized_range_read_emits_blob_read_range() {
        let mut operation = GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "range.txt".to_string(),
            version_id: None,
            range: Some(2..5),
            group_id: Ulid::new(),
            user_identity: UserId::local(Ulid::new(), RealmId([0u8; 32])),
        });
        let txn_id = Ulid::new();
        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "aruna_test".to_string(),
            backend_path: "s3test/range.txt".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        };
        operation.txn_id = Some(txn_id);
        operation.location = Some(location.clone());

        let effects = operation.commit_and_read_blob();

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed_txn_id }),
                Effect::Blob(BlobEffect::ReadRange { location: emitted_location, range })
            ] if *committed_txn_id == txn_id && emitted_location == &location && range == &(2..5)
        ));
    }

    #[tokio::test]
    pub async fn test_get_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
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
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let content = "Hello, World!";
        let hasher = Hasher::new_with_bytes(content.as_bytes());
        let hashes = hasher.finalize();

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let blob_ulid = Ulid::new();
        let location = BackendLocation {
            root: temp_root.to_string(),
            storage_bucket: format!("aruna_{}", Ulid::new()),
            backend_path: format!("{bucket}/{key}_{blob_ulid}"),
            ulid: blob_ulid,
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: content.len() as u64,
            hashes: hasher.to_map(),
        };

        // Write file + db entries
        std::fs::create_dir_all(
            Path::new(&location.get_full_path().unwrap())
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(location.get_full_path().unwrap(), content).unwrap();

        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            let hash_lookup_key = LookupKey::from_blake3_hash(hashes.blake3.as_slice())
                .unwrap()
                .to_bytes()
                .unwrap();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
                    key: hash_lookup_key.into(),
                    value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;

            let object_lookup_key = LookupKey::object(&bucket, &key).to_bytes().unwrap();
            let version_id = Ulid::new();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                    key: object_lookup_key.into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_VERSION_KEYSPACE.to_string(),
                    key: VersionKey::new(&bucket, &key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: VersionMetadata::materialized(
                        version_id,
                        location.clone(),
                        location.created_at,
                        location.created_by,
                        None,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await;
        } else {
            panic!("Failed to start transaction");
        }

        // Read file with operation
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let operation = GetObjectOperation::new(GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: None,
            group_id: Ulid::new(),
            user_identity: Default::default(),
        });

        let blob_result = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            blob_result.location.as_ref().unwrap().hashes,
            location.hashes
        );
        assert!(blob_result.source_metadata.is_none());
        assert!(blob_result.last_refresh.is_none());
        assert_eq!(blob_result.checksum_type, MultipartChecksumType::FullObject);
        let mut blob_stream = blob_result.blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = blob_stream.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, content.as_bytes());
    }

    #[tokio::test]
    pub async fn test_get_object_hash_mismatch() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
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
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        let content = "Hello, World!";
        let tampered = "Hallo, World!";
        let hasher = Hasher::new_with_bytes(content.as_bytes());
        let hashes = hasher.finalize();

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let blob_ulid = Ulid::new();
        let location = BackendLocation {
            root: temp_root.to_string(),
            storage_bucket: format!("aruna_{}", Ulid::new()),
            backend_path: format!("{bucket}/{key}_{blob_ulid}"),
            ulid: blob_ulid,
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: content.len() as u64,
            hashes: hasher.to_map(),
        };

        std::fs::create_dir_all(
            Path::new(&location.get_full_path().unwrap())
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(location.get_full_path().unwrap(), tampered).unwrap();

        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            let hash_lookup_key = LookupKey::from_blake3_hash(hashes.blake3.as_slice())
                .unwrap()
                .to_bytes()
                .unwrap();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
                    key: hash_lookup_key.into(),
                    value: Location::Real(location.clone()).to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;

            let object_lookup_key = LookupKey::object(&bucket, &key).to_bytes().unwrap();
            let version_id = Ulid::new();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                    key: object_lookup_key.into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_VERSION_KEYSPACE.to_string(),
                    key: VersionKey::new(&bucket, &key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: VersionMetadata::materialized(
                        version_id,
                        location.clone(),
                        location.created_at,
                        location.created_by,
                        None,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await;
        } else {
            panic!("Failed to start transaction");
        }

        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let operation = GetObjectOperation::new(GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: None,
            group_id: Ulid::new(),
            user_identity: Default::default(),
        });

        let mut blob_stream = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .blob;
        let mut read_buffer = Vec::new();
        let mut read_error = None;
        while let Some(result) = blob_stream.next().await {
            match result {
                Ok(bytes) => read_buffer.extend_from_slice(&bytes),
                Err(err) => {
                    read_error = Some(err.to_string());
                    break;
                }
            }
        }

        assert_eq!(read_buffer, tampered.as_bytes());
        assert!(read_error.is_some());
        assert!(read_error.unwrap().contains("Integrity check failed"));
    }

    #[tokio::test]
    async fn test_get_reference_object_uses_exact_bound_connector() {
        let endpoint = spawn_reference_server(b"hello reference").await;
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let version_id = Ulid::new();
        let connector_id = Ulid::new();
        let cached_metadata = SourceMetadata {
            content_length: 15,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-123".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
        };
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(connector_id),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("Failed to start transaction");
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: LookupKey::object(&bucket, &key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, &key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: VersionMetadata::reference(
                    version_id,
                    source,
                    cached_metadata,
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    SystemTime::UNIX_EPOCH,
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
            GetObjectOperation::new(GetObjectInput {
                bucket,
                key,
                version_id: None,
                range: None,
                group_id: Ulid::new(),
                user_identity: UserId::local(Ulid::new(), RealmId([0u8; 32])),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.location.is_none());
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .and_then(|m| m.content_type.clone()),
            Some("text/plain".to_string())
        );
        assert!(result.last_refresh.is_some());
        let mut stream = result.blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = stream.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, b"hello reference");

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "test.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata");
        };
        let metadata = VersionMetadata::from_bytes(value.unwrap().as_ref()).unwrap();
        let VersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = metadata.state
        else {
            panic!("expected reference metadata");
        };
        assert_eq!(cached_metadata.content_type.as_deref(), Some("text/plain"));
        assert_eq!(last_refresh, SystemTime::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn test_get_reference_object_returns_fresh_metadata_without_persisting() {
        let endpoint = spawn_reference_server(b"hello reference").await;
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let version_id = Ulid::new();
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::new()),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("Failed to start transaction");
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_CURRENT_VERSION_KEYSPACE.to_string(),
                key: LookupKey::object("s3test", "refresh.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "refresh.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: VersionMetadata::reference(
                    version_id,
                    source,
                    SourceMetadata {
                        content_length: 1,
                        content_type: Some("application/octet-stream".to_string()),
                        etag: Some("stale-etag".to_string()),
                        last_modified: None,
                    },
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    SystemTime::UNIX_EPOCH,
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
            GetObjectOperation::new(GetObjectInput {
                bucket: "s3test".to_string(),
                key: "refresh.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::new(),
                user_identity: UserId::local(Ulid::new(), RealmId([0u8; 32])),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let mut stream = result.blob;
        assert!(result.last_refresh.is_some());
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length),
            Some(15)
        );
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.content_type.as_deref()),
            Some("text/plain")
        );
        while let Some(Ok(_)) = stream.next().await {}

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: S3_VERSION_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "refresh.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata");
        };
        let metadata = VersionMetadata::from_bytes(value.unwrap().as_ref()).unwrap();
        let VersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = metadata.state
        else {
            panic!("expected reference metadata");
        };
        assert_eq!(cached_metadata.content_length, 1);
        assert_eq!(
            cached_metadata.content_type.as_deref(),
            Some("application/octet-stream")
        );
        assert_eq!(cached_metadata.etag.as_deref(), Some("stale-etag"));
        assert_eq!(last_refresh, SystemTime::UNIX_EPOCH);
    }
}
