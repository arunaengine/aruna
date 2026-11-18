//! Describes one object version: its location, checksums, part summary and part list.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::object::lookup::{FoundVersion, VersionLookup, VersionLookupError};
use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::managed_copy::ManagedCopyError;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartObjectPart, MultipartObjectSummary,
};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum GetAttributesError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error("GetObjectAttributes failed")]
    GetAttributesFailed,
}

impl From<IoError> for GetAttributesError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<VersionLookupError> for GetAttributesError {
    fn from(error: VersionLookupError) -> Self {
        match error {
            VersionLookupError::Storage(error) => Self::StorageError(error),
            VersionLookupError::Conversion(error) => Self::ConversionError(error),
            VersionLookupError::Managed(error) => Self::ManagedCopyError(error),
            VersionLookupError::NoSuchKey => Self::NoSuchKey,
            VersionLookupError::NoSuchVersion => Self::NoSuchVersion,
            VersionLookupError::DeleteMarker => Self::DeleteMarker,
            VersionLookupError::Failed => Self::GetAttributesFailed,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetAttributesInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub include_parts: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetAttributesResult {
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub version_created_at: Option<std::time::SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub summary: Option<MultipartObjectSummary>,
    pub parts: Vec<MultipartObjectPart>,
}

type AttributesTurn = Result<Next<GetAttributesResult>, GetAttributesError>;

/// The attributes of one version, from one read snapshot.
#[derive(Debug, PartialEq)]
pub struct GetAttributesOperation {
    requested: Option<Ulid>,
    lookup: VersionLookup,
}

impl GetAttributesOperation {
    pub fn new(input: GetAttributesInput) -> Self {
        Self {
            requested: input.version_id,
            lookup: VersionLookup::new(
                input.bucket,
                input.key,
                input.version_id,
                input.include_parts,
            ),
        }
    }

    fn result(&self, found: FoundVersion) -> GetAttributesResult {
        let checksum_type = (found.summary.as_ref())
            .map(|summary| summary.checksum_type)
            .unwrap_or(MultipartChecksumType::FullObject);
        GetAttributesResult {
            location: found.location,
            source_metadata: found.source_metadata,
            version_created_at: found.created_at,
            version_id: found.version_id.or(self.requested),
            resolved_version_id: found.version_id,
            checksum_type,
            summary: found.summary,
            parts: found.parts,
        }
    }

    fn advance(&self, turn: Result<Next<FoundVersion>, VersionLookupError>) -> AttributesTurn {
        match turn? {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Done(found) => Ok(Next::Done(self.result(found))),
            Next::Wait | Next::Then { .. } => Err(IoError::Unfinished.into()),
        }
    }
}

impl Operation for GetAttributesOperation {
    type Output = GetAttributesResult;
    type Error = GetAttributesError;

    fn first(&mut self) -> AttributesTurn {
        let turn = self.lookup.first();
        self.advance(turn)
    }

    fn next(&mut self, result: IoResult) -> AttributesTurn {
        let turn = self.lookup.next(result);
        self.advance(turn)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        OBJECT_METADATA_KEYSPACE,
    };
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5, HASH_SHA256};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{
        BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::MultipartObjectKey;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    #[test]
    fn answers_from_snapshot() {
        // A reference version answers from its cached metadata, with no copy to describe.
        let source = aruna_core::structs::execution::staging::VersionSourceBinding {
            strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
                kind: aruna_core::structs::execution::source_connector::SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        };
        let metadata = SourceMetadata {
            content_length: 3,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let version = BlobVersion::reference(
            source,
            metadata.clone(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        );
        let version_id = Ulid::from_parts(4, 4);
        let mut operation = GetAttributesOperation::new(GetAttributesInput {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Some(version_id),
            include_parts: true,
        });
        assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(aruna_core::calls::IoCall::Read(_)))
        ));
        let row = IoResult::Values(vec![Some(version.to_bytes().unwrap().into())]);
        let Ok(Next::Done(result)) = operation.next(row) else {
            panic!("a reference needs no further reads");
        };
        assert_eq!(result.source_metadata, Some(metadata));
        assert_eq!(result.version_id, Some(version_id));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
        assert_eq!(result.location, None);
    }

    #[test]
    fn lookup_errors_map() {
        for (from, to) in [
            (VersionLookupError::NoSuchKey, GetAttributesError::NoSuchKey),
            (
                VersionLookupError::NoSuchVersion,
                GetAttributesError::NoSuchVersion,
            ),
            (
                VersionLookupError::DeleteMarker,
                GetAttributesError::DeleteMarker,
            ),
            (
                VersionLookupError::Failed,
                GetAttributesError::GetAttributesFailed,
            ),
        ] {
            assert_eq!(GetAttributesError::from(from), to);
        }
    }

    fn location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), vec![7; 32]);
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        hashes.insert(HASH_SHA256.to_string(), vec![2; 32]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            staging: false,
            partial: false,
            blob_size: 10,
            hashes,
        }
    }

    async fn write(
        storage_handle: &storage::StorageHandle,
        key_space: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
    }

    async fn seed_current_version(
        storage_handle: &storage::StorageHandle,
        location: &BackendLocation,
        version_id: Ulid,
    ) {
        write(
            storage_handle,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "hello.txt")
                .to_bytes()
                .unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        )
        .await;
        write(
            storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "hello.txt", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::materialized(
                location.get_blake3().unwrap().try_into().unwrap(),
                BackendRef::node_default(),
                EncodingClass::Raw,
                location.created_at,
                location.created_by,
                None,
            )
            .to_bytes()
            .unwrap(),
        )
        .await;
        write(
            storage_handle,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::from_blake3(
                location.get_blake3().unwrap(),
                EncodingClass::Raw,
                location.backend.clone(),
            )
            .unwrap()
            .to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;
    }

    async fn seed_multipart_metadata(
        storage_handle: &storage::StorageHandle,
        version_id: Ulid,
        checksum_type: MultipartChecksumType,
        part_numbers: &[u16],
    ) {
        write(
            storage_handle,
            OBJECT_METADATA_KEYSPACE,
            MultipartObjectKey::summary(version_id).to_bytes().unwrap(),
            MultipartObjectSummary {
                checksum_type,
                part_count: part_numbers.len(),
                composite_hashes: HashMap::from([(HASH_SHA256.to_string(), vec![9u8; 32])]),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        for part_number in part_numbers {
            let mut hashes = HashMap::new();
            hashes.insert(HASH_MD5.to_string(), vec![*part_number as u8; 16]);
            hashes.insert(HASH_SHA256.to_string(), vec![*part_number as u8; 32]);
            write(
                storage_handle,
                OBJECT_METADATA_KEYSPACE,
                MultipartObjectKey::part(version_id, *part_number)
                    .to_bytes()
                    .unwrap(),
                MultipartObjectPart {
                    part_number: *part_number,
                    size: 5,
                    hashes,
                }
                .to_bytes()
                .unwrap(),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn multipart_returns_parts() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let location = location();
        let version_id = Ulid::generate();
        seed_current_version(&storage_handle, &location, version_id).await;
        seed_multipart_metadata(
            &storage_handle,
            version_id,
            MultipartChecksumType::Composite,
            &[2, 1],
        )
        .await;

        let result = drive(
            GetAttributesOperation::new(GetAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
                include_parts: true,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert_eq!(result.checksum_type, MultipartChecksumType::Composite);
        let summary = result.summary.expect("multipart summary");
        assert_eq!(summary.part_count, 2);
        assert_eq!(
            summary.composite_hashes.get(HASH_SHA256),
            Some(&vec![9u8; 32])
        );
        let numbers: Vec<u16> = result.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, vec![1, 2]);
        assert_eq!(result.version_id, Some(version_id));
    }

    #[tokio::test]
    async fn simple_omits_parts() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let location = location();
        let version_id = Ulid::generate();
        seed_current_version(&storage_handle, &location, version_id).await;

        let result = drive(
            GetAttributesOperation::new(GetAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
                include_parts: true,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert_eq!(result.checksum_type, MultipartChecksumType::FullObject);
        assert!(result.summary.is_none());
        assert!(result.parts.is_empty());
    }

    #[tokio::test]
    async fn targets_specific_version() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let location = location();
        let version_id = Ulid::generate();
        write(
            &storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "hello.txt", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::materialized(
                location.get_blake3().unwrap().try_into().unwrap(),
                BackendRef::node_default(),
                EncodingClass::Raw,
                location.created_at,
                location.created_by,
                None,
            )
            .to_bytes()
            .unwrap(),
        )
        .await;
        write(
            &storage_handle,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::from_blake3(
                location.get_blake3().unwrap(),
                EncodingClass::Raw,
                location.backend.clone(),
            )
            .unwrap()
            .to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;

        let result = drive(
            GetAttributesOperation::new(GetAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
                include_parts: false,
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert_eq!(result.resolved_version_id, Some(version_id));
        assert_eq!(result.version_id, Some(version_id));
    }

    #[tokio::test]
    async fn delete_marker_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let version_id = Ulid::generate();
        write(
            &storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "hello.txt", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::deleted(SystemTime::UNIX_EPOCH, Default::default())
                .to_bytes()
                .unwrap(),
        )
        .await;

        let result = drive(
            GetAttributesOperation::new(GetAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
                include_parts: false,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(result, Err(GetAttributesError::DeleteMarker)));
    }
}
