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
    MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary,
    SourceMetadata, VersionKey,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const PART_SCAN_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetObjectAttributesState {
    Init,
    StartTransaction,
    GetVersion,
    GetCurrentVersion,
    GetBlobLocation,
    ReadMultipartSummary,
    ReadMultipartParts,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetObjectAttributesError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetObjectAttributesState,
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
    #[error("GetObjectAttributes failed")]
    GetObjectAttributesFailed,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectAttributesInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub include_parts: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GetObjectAttributesResult {
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub summary: Option<MultipartObjectSummary>,
    pub parts: Vec<MultipartObjectPart>,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectAttributesOperation {
    input: GetObjectAttributesInput,
    state: GetObjectAttributesState,
    txn_id: Option<Ulid>,
    location: Option<BackendLocation>,
    source_metadata: Option<SourceMetadata>,
    resolved_version_id: Option<Ulid>,
    summary: Option<MultipartObjectSummary>,
    parts: Vec<MultipartObjectPart>,
    output: Option<Result<GetObjectAttributesResult, GetObjectAttributesError>>,
}

impl GetObjectAttributesOperation {
    pub fn new(input: GetObjectAttributesInput) -> Self {
        Self {
            input,
            state: GetObjectAttributesState::Init,
            txn_id: None,
            location: None,
            source_metadata: None,
            resolved_version_id: None,
            summary: None,
            parts: Vec::new(),
            output: None,
        }
    }

    fn emit_error(&mut self, error: GetObjectAttributesError) -> Effects {
        self.state = GetObjectAttributesState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = GetObjectAttributesState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        if let Some(version_id) = self.input.version_id {
            self.state = GetObjectAttributesState::GetVersion;
            let key =
                match VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes() {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(err.into()),
                };
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            })]
        } else {
            self.state = GetObjectAttributesState::GetCurrentVersion;
            let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
                Ok(key) => key.into(),
                Err(err) => return self.emit_error(err.into()),
            };
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            })]
        }
    }

    fn handle_received_current_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(GetObjectAttributesError::NoSuchKey);
        };

        let pointer = match CurrentVersionPointer::from_bytes(val.as_ref()) {
            Ok(pointer) => pointer,
            Err(err) => return self.emit_error(err.into()),
        };

        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.resolved_version_id = Some(pointer.version_id);
        self.state = GetObjectAttributesState::GetVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_received_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(if self.input.version_id.is_some() {
                GetObjectAttributesError::NoSuchVersion
            } else {
                GetObjectAttributesError::NoSuchKey
            });
        };

        let version = match BlobVersion::from_bytes(val.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.emit_error(err.into()),
        };

        let Some(version_id) = self.resolved_version_id.or(self.input.version_id) else {
            return self.emit_error(GetObjectAttributesError::GetObjectAttributesFailed);
        };

        self.read_version(version_id, version, self.input.version_id.is_some())
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
                self.read_blob_location(blob_hash)
            }
            BlobVersionState::Deleted => self.emit_error(if explicit_version_request {
                GetObjectAttributesError::DeleteMarker
            } else {
                GetObjectAttributesError::NoSuchKey
            }),
            BlobVersionState::Reference {
                cached_metadata, ..
            } => {
                self.location = None;
                self.source_metadata = Some(cached_metadata);
                self.finish_lookup()
            }
        }
    }

    fn read_blob_location(&mut self, blob_hash: [u8; 32]) -> Effects {
        self.state = GetObjectAttributesState::GetBlobLocation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: blob_hash.to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_blob_location_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(GetObjectAttributesError::GetObjectAttributesFailed);
        };

        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(err) => return self.emit_error(err.into()),
        };
        self.location = Some(location);

        self.read_multipart_summary()
    }

    fn read_multipart_summary(&mut self) -> Effects {
        let Some(version_id) = self.resolved_version_id else {
            return self.finish_lookup();
        };

        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = GetObjectAttributesState::ReadMultipartSummary;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_summary_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        self.summary = match value {
            Some(value) => match MultipartObjectSummary::from_bytes(value.as_ref()) {
                Ok(summary) => Some(summary),
                Err(err) => return self.emit_error(err.into()),
            },
            None => None,
        };

        let Some(version_id) = self.resolved_version_id else {
            return self.finish_lookup();
        };
        if !self.input.include_parts || self.summary.is_none() {
            return self.finish_lookup();
        }

        let prefix = match MultipartObjectMetadataKey::part_prefix(version_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = GetObjectAttributesState::ReadMultipartParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: None,
            limit: PART_SCAN_LIMIT,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let mut parts = Vec::with_capacity(values.len());
        for (_key, value) in values {
            let part = match MultipartObjectPart::from_bytes(value.as_ref()) {
                Ok(part) => part,
                Err(err) => return self.emit_error(err.into()),
            };
            parts.push(part);
        }
        // Part keys are varint-encoded, so iteration is not numeric order past
        // the 127/255 boundaries; sort in memory to restore ascending order.
        parts.sort_by_key(|part| part.part_number);
        self.parts = parts;

        self.finish_lookup()
    }

    fn finish_lookup(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectAttributesError::NoTransactionFound);
        };
        if self.location.is_none() && self.source_metadata.is_none() {
            return self.emit_error(GetObjectAttributesError::GetObjectAttributesFailed);
        }

        let checksum_type = self
            .summary
            .as_ref()
            .map(|summary| summary.checksum_type)
            .unwrap_or(MultipartChecksumType::FullObject);

        self.state = GetObjectAttributesState::CommitTransaction;
        self.output = Some(Ok(GetObjectAttributesResult {
            location: self.location.clone(),
            source_metadata: self.source_metadata.clone(),
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            checksum_type,
            summary: self.summary.clone(),
            parts: std::mem::take(&mut self.parts),
        }));

        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(GetObjectAttributesError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = GetObjectAttributesState::Finish;
        smallvec![]
    }
}

impl Operation for GetObjectAttributesOperation {
    type Output = Option<Result<GetObjectAttributesResult, GetObjectAttributesError>>;
    type Error = GetObjectAttributesError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(GetObjectAttributesError::StorageError(error));
        }

        match self.state {
            GetObjectAttributesState::Init => self.handle_init(),
            GetObjectAttributesState::StartTransaction => self.handle_transaction_started(event),
            GetObjectAttributesState::GetVersion => self.handle_received_version(event),
            GetObjectAttributesState::GetCurrentVersion => {
                self.handle_received_current_version(event)
            }
            GetObjectAttributesState::GetBlobLocation => self.handle_blob_location_read(event),
            GetObjectAttributesState::ReadMultipartSummary => {
                self.handle_multipart_summary_read(event)
            }
            GetObjectAttributesState::ReadMultipartParts => self.handle_multipart_parts_read(event),
            GetObjectAttributesState::CommitTransaction => self.handle_transaction_committed(event),
            GetObjectAttributesState::Finish | GetObjectAttributesState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetObjectAttributesState::Finish | GetObjectAttributesState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetObjectAttributesState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetObjectAttributesError::GetObjectAttributesFailed);
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
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5, HASH_SHA256};
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
            task_handle: None,
        }
    }

    fn location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), vec![7; 32]);
        hashes.insert(HASH_MD5.to_string(), vec![1; 16]);
        hashes.insert(HASH_SHA256.to_string(), vec![2; 32]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "mybucket".to_string(),
            backend_path: "hello.txt".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32])),
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
            location.get_blake3().unwrap().to_vec(),
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
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
            MultipartObjectMetadataKey::summary(version_id)
                .to_bytes()
                .unwrap(),
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
                S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
                MultipartObjectMetadataKey::part(version_id, *part_number)
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
    async fn multipart_object_returns_parts_and_composite_type() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let location = location();
        let version_id = Ulid::new();
        seed_current_version(&storage_handle, &location, version_id).await;
        seed_multipart_metadata(
            &storage_handle,
            version_id,
            MultipartChecksumType::Composite,
            &[2, 1],
        )
        .await;

        let result = drive(
            GetObjectAttributesOperation::new(GetObjectAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
                include_parts: true,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
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
    async fn simple_object_omits_parts() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let location = location();
        let version_id = Ulid::new();
        seed_current_version(&storage_handle, &location, version_id).await;

        let result = drive(
            GetObjectAttributesOperation::new(GetObjectAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: None,
                include_parts: true,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
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
        let version_id = Ulid::new();
        write(
            &storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "hello.txt", version_id)
                .to_bytes()
                .unwrap(),
            BlobVersion::materialized(
                location.get_blake3().unwrap().try_into().unwrap(),
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
            location.get_blake3().unwrap().to_vec(),
            location.to_bytes().unwrap(),
        )
        .await;

        let result = drive(
            GetObjectAttributesOperation::new(GetObjectAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
                include_parts: false,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.location, Some(location));
        assert_eq!(result.resolved_version_id, Some(version_id));
        assert_eq!(result.version_id, Some(version_id));
    }

    #[tokio::test]
    async fn delete_marker_version_returns_error() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let version_id = Ulid::new();
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
            GetObjectAttributesOperation::new(GetObjectAttributesInput {
                bucket: "mybucket".to_string(),
                key: "hello.txt".to_string(),
                version_id: Some(version_id),
                include_parts: false,
            }),
            &driver_ctx,
        )
        .await;

        assert!(matches!(
            result,
            Err(GetObjectAttributesError::DeleteMarker)
        ));
    }
}
