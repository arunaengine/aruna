//! Runs S3 HeadObject: resolves object metadata without reading data.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::{ResolveBindingInput, resolve_binding_effect};
use crate::s3::object::lookup::{FoundVersion, VersionLookup, VersionLookupError};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::errors::{
    ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::managed_copy::ManagedCopyError;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadObjectState {
    Lookup,
    ResolveReferenceAccess,
    HeadReferenceSource,
}

#[derive(Debug, Error, PartialEq)]
pub enum HeadObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received}]")]
    InvalidStateEvent {
        state: HeadObjectState,
        expected: &'static str,
        received: &'static str,
    },
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error(transparent)]
    ResolveReferenceError(#[from] SourceResolutionError),
    #[error(transparent)]
    StagingSourceError(#[from] StagingSourceError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error("HeadObject failed")]
    HeadObjectFailed,
}

impl From<IoError> for HeadObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<VersionLookupError> for HeadObjectError {
    fn from(error: VersionLookupError) -> Self {
        match error {
            VersionLookupError::Storage(error) => Self::StorageError(error),
            VersionLookupError::Conversion(error) => Self::ConversionError(error),
            VersionLookupError::Managed(error) => Self::ManagedCopyError(error),
            VersionLookupError::NoSuchKey => Self::NoSuchKey,
            VersionLookupError::NoSuchVersion => Self::NoSuchVersion,
            VersionLookupError::DeleteMarker => Self::DeleteMarker,
            VersionLookupError::Failed => Self::HeadObjectFailed,
        }
    }
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
    pub metadata: HashMap<String, String>,
    pub source_metadata: Option<SourceMetadata>,
    pub last_refresh: Option<SystemTime>,
    pub version_created_at: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub composite_hashes: HashMap<String, Vec<u8>>,
    pub part_count: Option<usize>,
    /// Refs stored on the version that was described. A derived write unions
    /// them with its destination default and never drops one.
    pub source_policies: Vec<PlacementPolicyRef>,
    /// Where the version's bytes come from, for a reference and a snapshot alike.
    pub source_binding: Option<VersionSourceBinding>,
}

type HeadTurn = Result<Next<HeadObjectResult>, HeadObjectError>;

/// Reads the version in one snapshot in a child; a reference to another Aruna node then asks
/// that source for its current metadata, outside the snapshot.
#[derive(Debug, PartialEq)]
pub struct HeadObjectOperation {
    input: HeadObjectInput,
    state: HeadObjectState,
    found: Option<FoundVersion>,
}

impl HeadObjectOperation {
    pub fn new(input: HeadObjectInput) -> Self {
        Self {
            input,
            state: HeadObjectState::Lookup,
            found: None,
        }
    }

    fn result(&self, found: FoundVersion) -> HeadObjectResult {
        let summary = found.summary;
        HeadObjectResult {
            location: found.location,
            metadata: found.metadata,
            source_metadata: found.source_metadata,
            last_refresh: found.last_refresh,
            version_created_at: found.created_at,
            version_id: found.version_id.or(self.input.version_id),
            resolved_version_id: found.version_id,
            checksum_type: (summary.as_ref())
                .map_or(MultipartChecksumType::FullObject, |it| it.checksum_type),
            part_count: summary.as_ref().map(|summary| summary.part_count),
            composite_hashes: summary.map(|it| it.composite_hashes).unwrap_or_default(),
            source_policies: found.source_policies,
            source_binding: found.source_binding,
        }
    }

    fn invalid(&self, expected: &'static str, event: Event) -> HeadTurn {
        Err(HeadObjectError::InvalidStateEvent {
            state: self.state.clone(),
            expected,
            received: event.label(),
        })
    }

    fn looked_up(&mut self, outcome: Result<FoundVersion, VersionLookupError>) -> HeadTurn {
        let mut found = outcome?;
        let Some(source) = found.native_reference.take() else {
            return Ok(Next::Done(self.result(found)));
        };
        self.found = Some(found);
        self.state = HeadObjectState::ResolveReferenceAccess;
        let resolve = resolve_binding_effect(ResolveBindingInput {
            source,
            group_id: None,
        });
        Ok(Next::Call(resolve.into()))
    }

    fn access_resolved(&mut self, event: Event) -> HeadTurn {
        match event {
            Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                result: Ok(access),
            }) => {
                self.state = HeadObjectState::HeadReferenceSource;
                let head = Effect::StagingSource(StagingSourceEffect::Head { access });
                Ok(Next::Call(head.into()))
            }
            Event::SubOperation(SubOperationEvent::VersionAccessResolved {
                result: Err(error),
            }) => Err(error.into()),
            other => self.invalid(
                "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                other,
            ),
        }
    }

    fn source_answered(&mut self, event: Event) -> HeadTurn {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let mut found = self.found.take().ok_or(IoError::Unfinished)?;
                found.source_metadata = Some(metadata);
                found.last_refresh = Some(SystemTime::now());
                found.location = None;
                found.created_at = None;
                Ok(Next::Done(self.result(found)))
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => Err(error.into()),
            other => self.invalid(
                "Event::StagingSource(StagingSourceEvent::HeadResult)",
                other,
            ),
        }
    }
}

impl Operation for HeadObjectOperation {
    type Output = HeadObjectResult;
    type Error = HeadObjectError;

    fn first(&mut self) -> HeadTurn {
        Ok(Next::Call(IoCall::child(VersionLookup::new(
            self.input.bucket.clone(),
            self.input.key.clone(),
            self.input.version_id,
            false,
        ))))
    }

    fn next(&mut self, result: IoResult) -> HeadTurn {
        match self.state {
            HeadObjectState::Lookup => {
                self.looked_up(result.child::<FoundVersion, VersionLookupError>()?)
            }
            HeadObjectState::ResolveReferenceAccess => self.access_resolved(result.event()?),
            HeadObjectState::HeadReferenceSource => self.source_answered(result.event()?),
        }
    }

    // A missing key or version is a normal answer, for example when an import checks a key first.
    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::BackendConfig;
    use aruna_core::structs::storage::blob::BackendLocation;
    use aruna_core::structs::storage::blob::{
        Backend, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, CurrentVersionPointer,
        VersionKey,
    };
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::MultipartObjectSummary;
    use aruna_net::NetHandle;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn location_with_hash() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), vec![2; 32]);
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 5,
            hashes,
        }
    }

    fn found(version_id: Ulid) -> FoundVersion {
        FoundVersion {
            location: Some(location_with_hash()),
            version_id: Some(version_id),
            summary: Some(MultipartObjectSummary {
                checksum_type: MultipartChecksumType::Composite,
                part_count: 3,
                composite_hashes: HashMap::from([("md5".to_string(), vec![1])]),
            }),
            ..FoundVersion::default()
        }
    }

    fn looked_up(outcome: Result<FoundVersion, VersionLookupError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn head_operation(version_id: Option<Ulid>) -> HeadObjectOperation {
        HeadObjectOperation::new(HeadObjectInput {
            bucket: "mybucket".to_string(),
            key: "hello.txt".to_string(),
            version_id,
        })
    }

    #[test]
    fn lookup_answers_head() {
        // The snapshot runs in a child; its finding is the answer.
        let version_id = Ulid::from_parts(4, 4);
        let mut operation = head_operation(None);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let version = found(version_id);
        let location = version.location.clone();
        let Ok(Next::Done(result)) = operation.next(looked_up(Ok(version))) else {
            panic!("a plain version needs nothing after its lookup");
        };
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.resolved_version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::Composite);
        assert_eq!(result.part_count, Some(3));
        assert_eq!(result.composite_hashes.get("md5"), Some(&vec![1]));
        assert_eq!(result.location, location);
    }

    #[test]
    fn lookup_errors_map() {
        for (from, to) in [
            (VersionLookupError::NoSuchKey, HeadObjectError::NoSuchKey),
            (
                VersionLookupError::NoSuchVersion,
                HeadObjectError::NoSuchVersion,
            ),
            (
                VersionLookupError::DeleteMarker,
                HeadObjectError::DeleteMarker,
            ),
            (
                VersionLookupError::Failed,
                HeadObjectError::HeadObjectFailed,
            ),
        ] {
            let mut operation = head_operation(None);
            operation.first().unwrap();
            assert_eq!(operation.next(looked_up(Err(from))), Err(to));
        }
    }

    #[tokio::test]
    async fn current_version_read() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::generate();
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
                    BackendRef::node_default(),
                    EncodingClass::Raw,
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
                key: BlobLocationKey::from_blake3(
                    location.get_blake3().unwrap(),
                    EncodingClass::Raw,
                    location.backend.clone(),
                )
                .unwrap()
                .to_bytes()
                .into(),
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
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[tokio::test]
    async fn specific_version_read() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let location = location_with_hash();
        let version_id = Ulid::generate();
        let metadata = BlobVersion::materialized(
            location.get_blake3().unwrap().try_into().unwrap(),
            BackendRef::node_default(),
            EncodingClass::Raw,
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
                key: BlobLocationKey::from_blake3(
                    location.get_blake3().unwrap(),
                    EncodingClass::Raw,
                    location.backend.clone(),
                )
                .unwrap()
                .to_bytes()
                .into(),
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
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert!(result.source_metadata.is_none());
        assert!(result.last_refresh.is_none());
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
    }

    #[test]
    fn head_resolves_native() {
        let version_id = Ulid::generate();
        let relationship_id = Ulid::generate();
        let origin = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let cached_metadata = SourceMetadata {
            content_length: 11,
            content_type: Some("text/plain".to_string()),
            etag: Some("cached".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        };
        let refreshed_metadata = SourceMetadata {
            content_length: 17,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some("current".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: Some(version_id.to_string()),
            digests: Default::default(),
        };
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::from([(
                    "relationship_id".to_string(),
                    relationship_id.to_string(),
                )]),
                source_path: "source-bucket/folder/file.txt".to_string(),
                version_selector: Some(format!("version:{version_id}")),
                capabilities: Vec::new(),
                origin_node_id: Some(origin),
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let mut operation = HeadObjectOperation::new(HeadObjectInput {
            bucket: "target-bucket".to_string(),
            key: "folder/file.txt".to_string(),
            version_id: Some(version_id),
        });
        operation.first().unwrap();
        let reference = FoundVersion {
            source_metadata: Some(cached_metadata),
            last_refresh: Some(SystemTime::UNIX_EPOCH),
            version_id: Some(version_id),
            source_binding: Some(source.clone()),
            native_reference: Some(source),
            ..FoundVersion::default()
        };
        assert!(matches!(
            operation.next(looked_up(Ok(reference))),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::ArunaNative,
            config: HashMap::new(),
            path: "source-bucket/folder/file.txt".to_string(),
            version: Some(version_id.to_string()),
        };
        let resolved = SubOperationEvent::VersionAccessResolved {
            result: Ok(access.clone()),
        };
        let turn = operation.next(IoResult::Event(Box::new(Event::SubOperation(resolved))));
        let head = Effect::StagingSource(StagingSourceEffect::Head { access });
        assert_eq!(turn, Ok(Next::Call(head.into())));

        let answer = Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: refreshed_metadata.clone(),
        });
        let Ok(Next::Done(result)) = operation.next(IoResult::Event(Box::new(answer))) else {
            panic!("the source's answer ends the head");
        };
        assert_eq!(result.source_metadata, Some(refreshed_metadata));
        assert!(result.last_refresh.is_some());
        assert_eq!(result.version_id, Some(version_id));
    }

    #[tokio::test]
    async fn cached_metadata_returned() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = aruna_storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let version_id = Ulid::generate();
        let cached_metadata = SourceMetadata {
            content_length: 11,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-123".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
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
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
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
        .unwrap();

        assert!(result.location.is_none());
        assert_eq!(result.source_metadata, Some(cached_metadata));
        assert_eq!(result.last_refresh, Some(last_refresh));
        assert_eq!(result.version_id, Some(version_id));
    }
}
