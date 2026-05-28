use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, delete_hash_path_index_effect,
    write_blob_head_effect, write_blob_location_effect, write_blob_version_effect,
};
use aruna_blob::hash::Hasher;
use aruna_core::effects::{BlobEffect, DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum, HASH_MD5};
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer, MultipartChecksumType,
    MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary, MultipartUpload,
    MultipartUploadPart, MultipartUploadPartKey, MultipartUploadStatus, RealmId, VersionKey,
};
use aruna_core::types::{Effects, NodeId, TxnId, UserId};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum CompleteMultipartUploadState {
    Init,
    StartMarkTransaction,
    ReadUploadForMark,
    WriteUploadCompleting,
    CommitMarkTransaction,
    ReadUploadParts,
    ComposeBlob,
    StartFinalizeTransaction,
    CheckHashLookup,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadPreviousVersion,
    WriteBlobHead,
    DeletePreviousHashPathIndex,
    WriteHashPathIndex,
    WriteBlobVersionRecord,
    WriteObjectMetadata,
    DeleteUploadRecords,
    CommitFinalizeTransaction,
    RegisterBlobInDht,
    CleanupDuplicate,
    CleanupPartBlobs,
    ResetUploadTransaction,
    ReadUploadForReset,
    WriteUploadReset,
    CommitResetTransaction,
    CleanupFailedCompose,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CompleteMultipartUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("The requested multipart upload contains no parts.")]
    MissingParts,
    #[error("The specified multipart upload has missing part data.")]
    InvalidPart,
    #[error("The list of parts was not in ascending order.")]
    InvalidPartOrder,
    #[error("The provided multipart object size did not match the uploaded parts.")]
    InvalidObjectSize,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error("missing MD5 hash for part etag validation")]
    MissingPartEtag,
    #[error("part etag mismatch")]
    PartEtagMismatch,
    #[error("CompleteMultipartUpload failed")]
    CompleteMultipartUploadFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteMultipartPart {
    pub part_number: u16,
    pub etag: Option<String>,
    pub expected_checksums: Vec<ExpectedChecksum>,
}

#[derive(Debug, PartialEq)]
pub struct CompleteMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub completed_parts: Vec<CompleteMultipartPart>,
    pub expected_checksums: Vec<ExpectedChecksum>,
    pub checksum_type: MultipartChecksumType,
    pub object_size: Option<u64>,
    pub created_by: UserId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteMultipartUploadResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
    pub checksum_type: MultipartChecksumType,
    pub response_hashes: HashMap<String, Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct CompleteMultipartUploadOperation {
    state: CompleteMultipartUploadState,
    input: CompleteMultipartUploadInput,
    txn_id: Option<TxnId>,
    upload_record: Option<MultipartUpload>,
    resolved_parts: Vec<MultipartUploadPart>,
    composed_location: Option<BackendLocation>,
    final_location: Option<BackendLocation>,
    composite_hashes: HashMap<String, Vec<u8>>,
    version_id: Option<Ulid>,
    version_created_at: Option<SystemTime>,
    existing_pointer: Option<CurrentVersionPointer>,
    previous_current_hash: Option<[u8; 32]>,
    cleanup_part_index: usize,
    pending_error: Option<CompleteMultipartUploadError>,
    output: Option<Result<CompleteMultipartUploadResult, CompleteMultipartUploadError>>,
}

impl CompleteMultipartUploadOperation {
    pub fn new(input: CompleteMultipartUploadInput) -> Self {
        Self {
            state: CompleteMultipartUploadState::Init,
            input,
            txn_id: None,
            upload_record: None,
            resolved_parts: Vec::new(),
            composed_location: None,
            final_location: None,
            composite_hashes: HashMap::new(),
            version_id: None,
            version_created_at: None,
            existing_pointer: None,
            previous_current_hash: None,
            cleanup_part_index: 0,
            pending_error: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CompleteMultipartUploadError) -> Effects {
        self.state = CompleteMultipartUploadState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn schedule_error(&mut self, error: CompleteMultipartUploadError) -> Effects {
        self.pending_error = Some(error);
        if self.upload_record.is_some() {
            self.state = CompleteMultipartUploadState::ResetUploadTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })]
        } else if self.composed_location.is_some() {
            self.state = CompleteMultipartUploadState::CleanupFailedCompose;
            smallvec![Effect::Blob(BlobEffect::Delete {
                location: self
                    .composed_location
                    .clone()
                    .expect("composed_location checked above"),
            })]
        } else {
            self.emit_pending_error()
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.pending_error.take() else {
            return self.emit_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        self.emit_error(error)
    }

    fn validate_upload_target(
        &self,
        record: &MultipartUpload,
    ) -> Result<(), CompleteMultipartUploadError> {
        if record.bucket != self.input.bucket || record.key != self.input.key {
            return Err(CompleteMultipartUploadError::UploadTargetMismatch);
        }
        if record.status != MultipartUploadStatus::Open {
            return Err(CompleteMultipartUploadError::UploadNotOpen);
        }
        Ok(())
    }

    fn alias_context(&self) -> Result<HeadAliasContext, CompleteMultipartUploadError> {
        let Some(upload_record) = self.upload_record.as_ref() else {
            return Err(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };

        Ok(HeadAliasContext::new(
            self.input.realm_id,
            upload_record.group_id,
            self.input.node_id,
            self.input.bucket.clone(),
            self.input.key.clone(),
        ))
    }

    fn handle_init(&mut self) -> Effects {
        self.state = CompleteMultipartUploadState::StartMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_mark_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = CompleteMultipartUploadState::ReadUploadForMark;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_upload_read_for_mark(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.emit_error(CompleteMultipartUploadError::NoSuchUpload);
        };
        let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        if let Err(err) = self.validate_upload_target(&record) {
            return self.emit_error(err);
        }

        record.status = MultipartUploadStatus::Completing;
        let bytes = match record.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };
        self.upload_record = Some(record);
        self.state = CompleteMultipartUploadState::WriteUploadCompleting;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value: bytes.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_marked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CompleteMultipartUploadError::NoTransactionFound);
        };

        self.state = CompleteMultipartUploadState::CommitMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_mark_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = CompleteMultipartUploadState::ReadUploadParts;
        let prefix = match MultipartUploadPartKey::prefix(self.input.upload_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.schedule_error(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start_after: None,
            limit: 10_000,
            txn_id: None,
        })]
    }

    fn extract_requested_parts(
        &self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Result<Vec<MultipartUploadPart>, CompleteMultipartUploadError> {
        if self.input.completed_parts.is_empty() {
            return Err(CompleteMultipartUploadError::MissingParts);
        }

        let mut all_parts = HashMap::new();
        for (key, value) in values {
            let part_key = MultipartUploadPartKey::from_bytes(key.as_ref())?;
            let part_record = MultipartUploadPart::from_bytes(value.as_ref())?;
            all_parts.insert(part_key.part_number, part_record);
        }

        let mut previous = None;
        let mut resolved = Vec::with_capacity(self.input.completed_parts.len());
        for requested in &self.input.completed_parts {
            if previous.is_some_and(|prev| requested.part_number <= prev) {
                return Err(CompleteMultipartUploadError::InvalidPartOrder);
            }
            previous = Some(requested.part_number);

            let Some(record) = all_parts.get(&requested.part_number).cloned() else {
                return Err(CompleteMultipartUploadError::InvalidPart);
            };
            validate_requested_part(requested, &record)?;
            resolved.push(record);
        }

        if self.input.object_size.is_some_and(|size| {
            size != resolved
                .iter()
                .map(|part| part.location.blob_size)
                .sum::<u64>()
        }) {
            return Err(CompleteMultipartUploadError::InvalidObjectSize);
        }

        Ok(resolved)
    }

    fn handle_upload_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let resolved = match self.extract_requested_parts(values) {
            Ok(resolved) => resolved,
            Err(err) => return self.schedule_error(err),
        };
        self.composite_hashes = match compute_composite_hashes(&resolved) {
            Ok(hashes) => hashes,
            Err(err) => return self.schedule_error(err),
        };
        self.resolved_parts = resolved.clone();

        self.state = CompleteMultipartUploadState::ComposeBlob;
        smallvec![Effect::Blob(BlobEffect::Compose {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            created_by: self.input.created_by,
            parts: resolved.into_iter().map(|part| part.location).collect(),
        })]
    }

    fn handle_blob_composed(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::WriteFinished { location }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.composed_location = Some(location.clone());
        self.final_location = None;

        let hashes = match self.input.checksum_type {
            MultipartChecksumType::FullObject => &location.hashes,
            MultipartChecksumType::Composite => &self.composite_hashes,
        };

        for expected in &self.input.expected_checksums {
            let Some(actual) = hashes.get(expected.algorithm.hash_key()) else {
                return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                    expected.algorithm.s3_name(),
                ));
            };
            if actual != &expected.digest {
                return self.schedule_error(CompleteMultipartUploadError::ChecksumMismatch(
                    expected.algorithm.s3_name(),
                ));
            }
        }

        self.state = CompleteMultipartUploadState::StartFinalizeTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_finalize_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);

        let Some(location) = self.composed_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                "blake3",
            ));
        };
        self.state = CompleteMultipartUploadState::CheckHashLookup;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: blake3_hash.to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_hash_lookup_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let Some(composed_location) = self.composed_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };

        self.final_location = match value {
            Some(value) => match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) => Some(location),
                Err(err) => {
                    return self.schedule_error(CompleteMultipartUploadError::ConversionError(err));
                }
            },
            None => Some(composed_location),
        };

        self.write_blob_location()
    }

    fn write_blob_location(&mut self) -> Effects {
        let Some(location) = self.final_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                "blake3",
            ));
        };
        let effect = match write_blob_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self
                        .schedule_error(CompleteMultipartUploadError::ConversionError(err.into()));
                }
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteMultipartUploadState::WriteBlobLocation;
        smallvec![effect]
    }

    fn handle_blob_location_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteMultipartUploadState::ReadObjectLookup;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_object_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let existing = match value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(existing) => existing,
            Err(err) => return self.schedule_error(err.into()),
        };
        self.existing_pointer = existing;
        self.previous_current_hash = None;

        if let Some(existing_pointer) = self.existing_pointer.as_ref() {
            let version_key = match VersionKey::new(
                &self.input.bucket,
                &self.input.key,
                existing_pointer.version_id,
            )
            .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.schedule_error(err.into()),
            };

            self.state = CompleteMultipartUploadState::ReadPreviousVersion;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                txn_id: self.txn_id,
            })];
        }

        self.write_current_lookup(None)
    }

    fn handle_previous_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        self.previous_current_hash = value
            .as_ref()
            .and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok())
            .and_then(|version| version.blob_hash().copied());

        let existing_pointer = self.existing_pointer.clone();
        self.write_current_lookup(existing_pointer.as_ref())
    }

    fn write_current_lookup(&mut self, existing: Option<&CurrentVersionPointer>) -> Effects {
        let version_id = *self.version_id.get_or_insert_with(Ulid::new);
        let pointer = CurrentVersionPointer::next_for(existing, version_id);
        let alias_context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.schedule_error(err),
        };
        let effect = match write_blob_head_effect(&alias_context, pointer, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteMultipartUploadState::WriteBlobHead;
        smallvec![effect]
    }

    fn handle_blob_head_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        if let Some(previous_hash) = self.previous_current_hash {
            let alias_context = match self.alias_context() {
                Ok(context) => context,
                Err(err) => return self.schedule_error(err),
            };
            let effect =
                match delete_hash_path_index_effect(&alias_context, previous_hash, self.txn_id) {
                    Ok(effect) => effect,
                    Err(err) => return self.schedule_error(err.into()),
                };
            self.state = CompleteMultipartUploadState::DeletePreviousHashPathIndex;
            return smallvec![effect];
        }

        self.write_hash_path_index()
    }

    fn handle_previous_hash_path_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        self.write_hash_path_index()
    }

    fn write_hash_path_index(&mut self) -> Effects {
        let Some(location) = self.final_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                "blake3",
            ));
        };
        let alias_context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.schedule_error(err),
        };
        let effect = match add_hash_path_index_effect(
            &alias_context,
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self
                        .schedule_error(CompleteMultipartUploadError::ConversionError(err.into()));
                }
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteMultipartUploadState::WriteHashPathIndex;
        smallvec![effect]
    }

    fn handle_hash_path_index_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let Some(location) = self.final_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(version_id) = self.version_id else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                "blake3",
            ));
        };
        let created_at = self
            .version_created_at
            .get_or_insert_with(SystemTime::now)
            .to_owned();
        let version = BlobVersion::materialized(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self
                        .schedule_error(CompleteMultipartUploadError::ConversionError(err.into()));
                }
            },
            created_at,
            self.input.created_by,
            None,
        );
        let version_key = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteMultipartUploadState::WriteBlobVersionRecord;
        smallvec![effect]
    }

    fn handle_blob_version_record_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let Some(version_id) = self.version_id else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let mut writes = Vec::with_capacity(self.resolved_parts.len() + 1);

        let summary = MultipartObjectSummary {
            checksum_type: self.input.checksum_type,
            part_count: self.resolved_parts.len(),
        };
        let summary_key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.schedule_error(err.into()),
        };
        let summary_value = match summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.schedule_error(err.into()),
        };
        writes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for record in &self.resolved_parts {
            let object_part = MultipartObjectPart {
                part_number: record.part_number,
                size: record.location.blob_size,
                hashes: record.location.hashes.clone(),
            };
            let key =
                match MultipartObjectMetadataKey::part(version_id, record.part_number).to_bytes() {
                    Ok(key) => key,
                    Err(err) => return self.schedule_error(err.into()),
                };
            let value = match object_part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.schedule_error(err.into()),
            };
            writes.push((
                S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        self.state = CompleteMultipartUploadState::WriteObjectMetadata;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_object_metadata_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.delete_upload_records()
    }

    fn delete_upload_records(&mut self) -> Effects {
        let mut deletes = Vec::with_capacity(self.resolved_parts.len() + 1);
        for record in &self.resolved_parts {
            let key = match MultipartUploadPartKey::new(self.input.upload_id, record.part_number)
                .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.schedule_error(err.into()),
            };
            deletes.push((S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(), key.into()));
        }
        deletes.push((
            S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            self.input.upload_id.to_bytes().to_vec().into(),
        ));

        self.state = CompleteMultipartUploadState::DeleteUploadRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_records_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteMultipartUploadError::NoTransactionFound);
        };
        self.state = CompleteMultipartUploadState::CommitFinalizeTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_finalize_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.register_blob_in_dht_or_continue()
    }

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Some(location) = self.final_location.as_ref() else {
            return self.begin_cleanup_part_blobs();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.begin_cleanup_part_blobs();
        };
        let key: [u8; 32] = match blake3_hash.try_into() {
            Ok(key) => key,
            Err(_) => return self.begin_cleanup_part_blobs(),
        };

        self.state = CompleteMultipartUploadState::RegisterBlobInDht;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key,
            realm_id: self.input.realm_id,
            value: self.input.node_id.as_bytes().to_vec(),
            ttl: Default::default(),
        }))]
    }

    fn handle_blob_registered_in_dht(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.cleanup_duplicate_or_continue(),
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn cleanup_duplicate_or_continue(&mut self) -> Effects {
        let Some(composed_location) = self.composed_location.clone() else {
            return self.begin_cleanup_part_blobs();
        };
        let Some(final_location) = self.final_location.as_ref() else {
            return self.begin_cleanup_part_blobs();
        };

        if &composed_location != final_location {
            self.state = CompleteMultipartUploadState::CleanupDuplicate;
            return smallvec![Effect::Blob(BlobEffect::Delete {
                location: composed_location,
            })];
        }

        self.begin_cleanup_part_blobs()
    }

    fn handle_duplicate_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.begin_cleanup_part_blobs()
            }
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn begin_cleanup_part_blobs(&mut self) -> Effects {
        self.cleanup_part_index = 0;
        self.upload_record = None;
        self.state = CompleteMultipartUploadState::CleanupPartBlobs;
        self.cleanup_next_part_blob()
    }

    fn cleanup_next_part_blob(&mut self) -> Effects {
        let Some(record) = self.resolved_parts.get(self.cleanup_part_index) else {
            self.state = CompleteMultipartUploadState::Finish;
            self.composed_location = None;
            let Some(location) = self.final_location.clone() else {
                return self
                    .emit_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
            };
            let Some(version_id) = self.version_id else {
                return self
                    .emit_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
            };
            let response_hashes = match self.input.checksum_type {
                MultipartChecksumType::FullObject => location.hashes.clone(),
                MultipartChecksumType::Composite => self.composite_hashes.clone(),
            };
            self.output = Some(Ok(CompleteMultipartUploadResult {
                location,
                version_id,
                checksum_type: self.input.checksum_type,
                response_hashes,
            }));
            return smallvec![];
        };

        self.state = CompleteMultipartUploadState::CleanupPartBlobs;
        smallvec![Effect::Blob(BlobEffect::Delete {
            location: record.location.clone(),
        })]
    }

    fn handle_cleanup_part_blob(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.cleanup_part_index += 1;
                self.cleanup_next_part_blob()
            }
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn handle_reset_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = CompleteMultipartUploadState::ReadUploadForReset;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_upload_read_for_reset(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        if let Some(value) = value {
            let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(err) => return self.emit_error(err.into()),
            };
            record.status = MultipartUploadStatus::Open;
            self.upload_record = Some(record.clone());
            let bytes = match record.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.emit_error(err.into()),
            };
            self.state = CompleteMultipartUploadState::WriteUploadReset;
            return smallvec![Effect::Storage(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                key: self.input.upload_id.to_bytes().to_vec().into(),
                value: bytes.into(),
                txn_id: self.txn_id,
            })];
        }

        if let Some(txn_id) = self.txn_id.take() {
            self.state = CompleteMultipartUploadState::CleanupFailedCompose;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = CompleteMultipartUploadState::CleanupFailedCompose;
        if let Some(location) = self.composed_location.clone() {
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.emit_pending_error()
        }
    }

    fn handle_upload_reset_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CompleteMultipartUploadError::NoTransactionFound);
        };
        self.state = CompleteMultipartUploadState::CommitResetTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_reset_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = CompleteMultipartUploadState::CleanupFailedCompose;
        if let Some(location) = self.composed_location.clone() {
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.emit_pending_error()
        }
    }

    fn handle_failed_compose_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished)
            | Event::Blob(BlobEvent::Error(_))
            | Event::Storage(StorageEvent::TransactionAborted { .. }) => self.emit_pending_error(),
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }
}

impl Operation for CompleteMultipartUploadOperation {
    type Output = Option<Result<CompleteMultipartUploadResult, CompleteMultipartUploadError>>;
    type Error = CompleteMultipartUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CompleteMultipartUploadState::Init => self.handle_init(),
            CompleteMultipartUploadState::StartMarkTransaction => {
                self.handle_mark_transaction_started(event)
            }
            CompleteMultipartUploadState::ReadUploadForMark => {
                self.handle_upload_read_for_mark(event)
            }
            CompleteMultipartUploadState::WriteUploadCompleting => self.handle_upload_marked(event),
            CompleteMultipartUploadState::CommitMarkTransaction => {
                self.handle_mark_committed(event)
            }
            CompleteMultipartUploadState::ReadUploadParts => self.handle_upload_parts_read(event),
            CompleteMultipartUploadState::ComposeBlob => self.handle_blob_composed(event),
            CompleteMultipartUploadState::StartFinalizeTransaction => {
                self.handle_finalize_transaction_started(event)
            }
            CompleteMultipartUploadState::CheckHashLookup => self.handle_hash_lookup_checked(event),
            CompleteMultipartUploadState::WriteBlobLocation => {
                self.handle_blob_location_written(event)
            }
            CompleteMultipartUploadState::ReadObjectLookup => self.handle_object_lookup_read(event),
            CompleteMultipartUploadState::ReadPreviousVersion => {
                self.handle_previous_version_read(event)
            }
            CompleteMultipartUploadState::WriteBlobHead => self.handle_blob_head_written(event),
            CompleteMultipartUploadState::DeletePreviousHashPathIndex => {
                self.handle_previous_hash_path_deleted(event)
            }
            CompleteMultipartUploadState::WriteHashPathIndex => {
                self.handle_hash_path_index_written(event)
            }
            CompleteMultipartUploadState::WriteBlobVersionRecord => {
                self.handle_blob_version_record_written(event)
            }
            CompleteMultipartUploadState::WriteObjectMetadata => {
                self.handle_object_metadata_written(event)
            }
            CompleteMultipartUploadState::DeleteUploadRecords => {
                self.handle_upload_records_deleted(event)
            }
            CompleteMultipartUploadState::CommitFinalizeTransaction => {
                self.handle_finalize_committed(event)
            }
            CompleteMultipartUploadState::RegisterBlobInDht => {
                self.handle_blob_registered_in_dht(event)
            }
            CompleteMultipartUploadState::CleanupDuplicate => self.handle_duplicate_cleanup(event),
            CompleteMultipartUploadState::CleanupPartBlobs => self.handle_cleanup_part_blob(event),
            CompleteMultipartUploadState::ResetUploadTransaction => {
                self.handle_reset_transaction_started(event)
            }
            CompleteMultipartUploadState::ReadUploadForReset => {
                self.handle_upload_read_for_reset(event)
            }
            CompleteMultipartUploadState::WriteUploadReset => {
                self.handle_upload_reset_written(event)
            }
            CompleteMultipartUploadState::CommitResetTransaction => {
                self.handle_reset_committed(event)
            }
            CompleteMultipartUploadState::CleanupFailedCompose => {
                self.handle_failed_compose_cleanup(event)
            }
            CompleteMultipartUploadState::Finish => smallvec![],
            CompleteMultipartUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CompleteMultipartUploadState::Finish | CompleteMultipartUploadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == CompleteMultipartUploadState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

fn validate_requested_part(
    requested: &CompleteMultipartPart,
    record: &MultipartUploadPart,
) -> Result<(), CompleteMultipartUploadError> {
    if let Some(etag) = &requested.etag {
        let Some(md5) = record.location.hashes.get(HASH_MD5) else {
            return Err(CompleteMultipartUploadError::MissingPartEtag);
        };
        if STANDARD.encode(md5) != *etag {
            return Err(CompleteMultipartUploadError::PartEtagMismatch);
        }
    }

    for expected in &requested.expected_checksums {
        let Some(actual) = record.location.hashes.get(expected.algorithm.hash_key()) else {
            return Err(CompleteMultipartUploadError::MissingExpectedChecksum(
                expected.algorithm.s3_name(),
            ));
        };
        if actual != &expected.digest {
            return Err(CompleteMultipartUploadError::ChecksumMismatch(
                expected.algorithm.s3_name(),
            ));
        }
    }

    Ok(())
}

fn compute_composite_hashes(
    parts: &[MultipartUploadPart],
) -> Result<HashMap<String, Vec<u8>>, CompleteMultipartUploadError> {
    let mut hashes = HashMap::new();
    for algorithm in [
        ChecksumAlgorithm::Md5,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
        ChecksumAlgorithm::Crc32,
        ChecksumAlgorithm::Crc32c,
        ChecksumAlgorithm::Crc64Nvme,
    ] {
        let mut combined = Vec::new();
        for part in parts {
            let Some(digest) = part.location.hashes.get(algorithm.hash_key()) else {
                return Err(CompleteMultipartUploadError::MissingExpectedChecksum(
                    algorithm.s3_name(),
                ));
            };
            combined.extend_from_slice(digest);
        }

        let digest = composite_digest_for_algorithm(algorithm, &combined);
        hashes.insert(algorithm.hash_key().to_string(), digest);
    }
    Ok(hashes)
}

fn composite_digest_for_algorithm(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Vec<u8> {
    let hashes = Hasher::new_with_bytes(bytes).finalize();
    match algorithm {
        ChecksumAlgorithm::Md5 => hashes.md5.to_vec(),
        ChecksumAlgorithm::Sha1 => hashes.sha1.to_vec(),
        ChecksumAlgorithm::Sha256 => hashes.sha256.to_vec(),
        ChecksumAlgorithm::Crc32 => hashes.crc32.to_vec(),
        ChecksumAlgorithm::Crc32c => hashes.crc32c.to_vec(),
        ChecksumAlgorithm::Crc64Nvme => hashes.crc64nvme.to_vec(),
    }
}
