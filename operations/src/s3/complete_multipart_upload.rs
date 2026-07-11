use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, write_blob_head_effect,
    write_blob_location_effect, write_blob_version_effect,
};
use crate::replication::queue::write_live_replication_obligation_effect;
use crate::usage_stats::{
    QuotaGate, QuotaGateError, UsageCounterUpdate, UsageUpdateError,
    schedule_usage_snapshot_publish_effect,
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
    AuthContext, BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer,
    MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary,
    MultipartUpload, MultipartUploadPart, MultipartUploadPartKey, MultipartUploadStatus, RealmId,
    UsageDelta, VersionKey,
};
use aruna_core::types::{Effects, NodeId, TxnId, UserId};
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
    ReadLivenessVersion,
    WriteBlobHead,
    WriteHashPathIndex,
    WriteBlobVersionRecord,
    WriteObjectMetadata,
    DeleteUploadRecords,
    WriteLiveReplicationObligation,
    EnforceQuota,
    UpdateUsage,
    CommitFinalizeTransaction,
    RegisterBlobInDht,
    CleanupDuplicate,
    CleanupPartBlobs,
    AbortFinalizeTransaction,
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
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error(transparent)]
    QuotaGateError(#[from] QuotaGateError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("write conflicted after {MAX_FINALIZE_TXN_ATTEMPTS} attempts; retry the request")]
    RetryableConflict,
    #[error("CompleteMultipartUpload failed")]
    CompleteMultipartUploadFailed,
}

/// Bounded re-runs of the finalize transaction on commit conflict. The composed
/// blob is already durable, so a conflict re-runs only the finalize writes and
/// the gate in a fresh transaction without recomposing.
const MAX_FINALIZE_TXN_ATTEMPTS: u32 = 3;

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
    /// Hard ceiling (bytes) the group's realm-wide `logical_bytes` may reach,
    /// resolved from the realm quota config at the request surface. `None` =
    /// unlimited, so no gate is enforced.
    pub quota_ceiling: Option<u64>,
    /// Nodes whose per-group snapshots the quota gate trusts, resolved from the
    /// realm config at the request surface. `None` counts every snapshot.
    pub active_node_ids: Option<std::collections::HashSet<NodeId>>,
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
    upload_parts: Vec<MultipartUploadPart>,
    resolved_parts: Vec<MultipartUploadPart>,
    composed_location: Option<BackendLocation>,
    final_location: Option<BackendLocation>,
    composite_hashes: HashMap<String, Vec<u8>>,
    version_id: Option<Ulid>,
    version_created_at: Option<SystemTime>,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    was_live: bool,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    finalize_attempts: u32,
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
            upload_parts: Vec::new(),
            resolved_parts: Vec::new(),
            composed_location: None,
            final_location: None,
            composite_hashes: HashMap::new(),
            version_id: None,
            version_created_at: None,
            existing_pointer: None,
            new_blob: false,
            was_live: false,
            usage_update: None,
            quota_gate: None,
            finalize_attempts: 1,
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
        // Any open finalize transaction must be aborted before we start the reset
        // transaction, otherwise the old txn is orphaned in the storage actor and
        // pins an LSM snapshot forever. The original error is preserved in
        // `pending_error` and surfaced once cleanup completes.
        if let Some(txn_id) = self.txn_id.take() {
            self.state = CompleteMultipartUploadState::AbortFinalizeTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.continue_error_cleanup()
    }

    fn continue_error_cleanup(&mut self) -> Effects {
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

    fn handle_abort_finalize_transaction(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => self.continue_error_cleanup(),
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
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
            start: None,
            limit: 10_000,
            txn_id: None,
        })]
    }

    fn extract_requested_parts(
        &self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Result<(Vec<MultipartUploadPart>, Vec<MultipartUploadPart>), CompleteMultipartUploadError>
    {
        if self.input.completed_parts.is_empty() {
            return Err(CompleteMultipartUploadError::MissingParts);
        }

        let mut all_parts = HashMap::new();
        let mut upload_parts = Vec::new();
        for (key, value) in values {
            let part_key = MultipartUploadPartKey::from_bytes(key.as_ref())?;
            let part_record = MultipartUploadPart::from_bytes(value.as_ref())?;
            upload_parts.push(part_record.clone());
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

        Ok((resolved, upload_parts))
    }

    fn handle_upload_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        let (resolved, upload_parts) = match self.extract_requested_parts(values) {
            Ok(parts) => parts,
            Err(err) => return self.schedule_error(err),
        };
        self.composite_hashes = match compute_composite_hashes(&resolved) {
            Ok(hashes) => hashes,
            Err(err) => return self.schedule_error(err),
        };
        self.upload_parts = upload_parts;
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
            None => {
                self.new_blob = true;
                Some(composed_location)
            }
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
        let existing_pointer = self.existing_pointer.clone();
        if let Some(pointer) = existing_pointer.as_ref() {
            let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
                .to_bytes()
            {
                Ok(key) => key.into(),
                Err(err) => return self.schedule_error(err.into()),
            };
            self.state = CompleteMultipartUploadState::ReadLivenessVersion;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            })];
        }
        self.write_current_lookup(existing_pointer.as_ref())
    }

    fn handle_liveness_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };

        self.was_live = value
            .and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok())
            .is_some_and(|version| !version.is_deleted());

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
            match self.version_id {
                Some(version_id) => version_id,
                None => {
                    return self.schedule_error(
                        CompleteMultipartUploadError::CompleteMultipartUploadFailed,
                    );
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
        let mut deletes = Vec::with_capacity(self.upload_parts.len() + 1);
        for record in &self.upload_parts {
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
        self.write_live_replication_obligation()
    }

    fn write_live_replication_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let effect = match write_live_replication_obligation_effect(
            self.input.node_id,
            AuthContext {
                user_id: self.input.created_by,
                realm_id: self.input.realm_id,
                path_restrictions: None,
            },
            self.input.bucket.clone(),
            self.input.key.clone(),
            version_id,
            false,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };
        self.state = CompleteMultipartUploadState::WriteLiveReplicationObligation;
        smallvec![effect]
    }

    fn handle_live_replication_obligation_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteMultipartUploadError::NoTransactionFound);
        };
        let Some(group_id) = self.upload_record.as_ref().map(|record| record.group_id) else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(size) = self
            .final_location
            .as_ref()
            .map(|location| i128::from(location.blob_size))
        else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };

        let group_delta = UsageDelta {
            objects: if self.was_live { 0 } else { 1 },
            logical_bytes: size,
            ..Default::default()
        };
        let global_delta = UsageDelta {
            stored_blobs: if self.new_blob { 1 } else { 0 },
            stored_bytes: if self.new_blob { size } else { 0 },
            ..group_delta
        };
        self.usage_update = Some(UsageCounterUpdate::with_global(
            group_id,
            group_delta,
            global_delta,
        ));

        // Enforce the hard group quota before the counters commit. Only a positive
        // logical-bytes delta can push a group over its ceiling.
        let object_size = self
            .final_location
            .as_ref()
            .map(|location| location.blob_size)
            .unwrap_or(0);
        if let Some(ceiling) = self.input.quota_ceiling
            && object_size > 0
        {
            let mut gate = QuotaGate::new(
                ceiling,
                object_size,
                group_id,
                self.input.node_id,
                self.input.active_node_ids.clone(),
            );
            self.state = CompleteMultipartUploadState::EnforceQuota;
            let effects = gate.start(txn_id);
            self.quota_gate = Some(gate);
            effects
        } else {
            self.start_usage_update(txn_id)
        }
    }

    fn start_usage_update(&mut self, txn_id: TxnId) -> Effects {
        self.state = CompleteMultipartUploadState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => {
                self.schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed)
            }
        }
    }

    fn handle_enforce_quota(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteMultipartUploadError::NoTransactionFound);
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        match gate.step(event) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                if gate.is_exceeded() {
                    let limit = gate.ceiling();
                    let usage = gate.projected_usage();
                    // schedule_error resets the upload back to Open and cleans up
                    // the composed blob, mirroring every other finalize-phase error.
                    self.schedule_error(CompleteMultipartUploadError::QuotaExceeded {
                        limit,
                        usage,
                    })
                } else {
                    self.start_usage_update(txn_id)
                }
            }
            Err(err) => self.schedule_error(err.into()),
        }
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteMultipartUploadError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.state = CompleteMultipartUploadState::CommitFinalizeTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.schedule_error(err.into()),
        }
    }

    fn handle_finalize_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.register_blob_in_dht_or_continue()
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => {
                self.txn_id = None;
                if self.finalize_attempts < MAX_FINALIZE_TXN_ATTEMPTS {
                    self.finalize_attempts += 1;
                    self.retry_finalize_txn()
                } else {
                    self.schedule_error(CompleteMultipartUploadError::RetryableConflict)
                }
            }
            _ => self.schedule_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    /// Re-runs the finalize transaction against current state, keeping the durable
    /// composed blob. Per-attempt finalize state is reset.
    fn retry_finalize_txn(&mut self) -> Effects {
        self.final_location = None;
        self.version_id = None;
        self.version_created_at = None;
        self.existing_pointer = None;
        self.new_blob = false;
        self.was_live = false;
        self.usage_update = None;
        self.quota_gate = None;
        self.state = CompleteMultipartUploadState::StartFinalizeTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Some(location) = self.final_location.as_ref() else {
            return self.begin_cleanup_part_blobs();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.begin_cleanup_part_blobs();
        };
        let key = match blake3_hash.try_into() {
            Ok(key) => aruna_core::id::DhtKeyId::from_bytes(key),
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
        let Some(record) = self.upload_parts.get(self.cleanup_part_index) else {
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
            return smallvec![schedule_usage_snapshot_publish_effect()];
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
            CompleteMultipartUploadState::ReadLivenessVersion => {
                self.handle_liveness_version_read(event)
            }
            CompleteMultipartUploadState::WriteBlobHead => self.handle_blob_head_written(event),
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
            CompleteMultipartUploadState::WriteLiveReplicationObligation => {
                self.handle_live_replication_obligation_written(event)
            }
            CompleteMultipartUploadState::EnforceQuota => self.handle_enforce_quota(event),
            CompleteMultipartUploadState::UpdateUsage => self.handle_usage_update(event),
            CompleteMultipartUploadState::CommitFinalizeTransaction => {
                self.handle_finalize_committed(event)
            }
            CompleteMultipartUploadState::RegisterBlobInDht => {
                self.handle_blob_registered_in_dht(event)
            }
            CompleteMultipartUploadState::CleanupDuplicate => self.handle_duplicate_cleanup(event),
            CompleteMultipartUploadState::CleanupPartBlobs => self.handle_cleanup_part_blob(event),
            CompleteMultipartUploadState::AbortFinalizeTransaction => {
                self.handle_abort_finalize_transaction(event)
            }
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
        if hex::encode(md5) != *etag {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn finalize_input() -> CompleteMultipartUploadInput {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        CompleteMultipartUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::new(),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            completed_parts: vec![],
            expected_checksums: vec![],
            checksum_type: MultipartChecksumType::FullObject,
            object_size: Some(10),
            created_by: UserId::local(Ulid::new(), realm_id),
            quota_ceiling: Some(30),
            active_node_ids: None,
        }
    }

    fn open_upload_record(input: &CompleteMultipartUploadInput) -> MultipartUpload {
        MultipartUpload {
            upload_id: input.upload_id,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            group_id: Ulid::new(),
            created_by: input.created_by,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Completing,
            checksum_hint: None,
        }
    }

    fn part_record(part_number: u16, blob_size: u64) -> MultipartUploadPart {
        MultipartUploadPart {
            part_number,
            location: BackendLocation {
                root: "/tmp".to_string(),
                storage_bucket: "multipart".to_string(),
                backend_path: format!("part-{part_number}"),
                ulid: Ulid::new(),
                compressed: false,
                encrypted: false,
                created_by: UserId::local(Ulid::new(), RealmId::from_bytes([4u8; 32])),
                created_at: SystemTime::now(),
                staging: false,
                partial: true,
                blob_size,
                hashes: HashMap::new(),
            },
            created_at: SystemTime::now(),
        }
    }

    #[test]
    fn finalize_conflict_exhausts() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let composed = part_record(1, 10).location;
        op.composed_location = Some(composed.clone());

        for _ in 0..(MAX_FINALIZE_TXN_ATTEMPTS - 1) {
            op.state = CompleteMultipartUploadState::CommitFinalizeTransaction;
            op.txn_id = Some(Ulid::new());
            let effects = op.handle_finalize_committed(Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::StartTransaction { .. })]
            ));
            assert_eq!(
                op.state,
                CompleteMultipartUploadState::StartFinalizeTransaction
            );
            assert_eq!(op.txn_id, None);
        }

        // No upload_record, so exhaustion deletes the composed blob directly.
        op.state = CompleteMultipartUploadState::CommitFinalizeTransaction;
        op.txn_id = Some(Ulid::new());
        let effects = op.handle_finalize_committed(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));
        let [Effect::Blob(BlobEffect::Delete { location })] = effects.as_slice() else {
            panic!("expected composed blob cleanup on retry exhaustion")
        };
        assert_eq!(location, &composed);
        assert_eq!(op.state, CompleteMultipartUploadState::CleanupFailedCompose);

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert_eq!(
            op.finalize(),
            Err(CompleteMultipartUploadError::RetryableConflict)
        );
    }

    #[test]
    fn delete_upload_records_includes_omitted_parts() {
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        op.upload_parts = vec![part_record(1, 10), part_record(2, 20)];
        op.resolved_parts = vec![op.upload_parts[0].clone()];

        let effects = op.delete_upload_records();

        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice()
        else {
            panic!("expected batch delete effect");
        };
        assert_eq!(deletes.len(), 3);
        let omitted_key = MultipartUploadPartKey::new(op.input.upload_id, 2)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(_, key)| key.as_ref() == omitted_key));
    }

    #[test]
    fn cleanup_part_blobs_includes_omitted_parts() {
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let requested = part_record(1, 10);
        let omitted = part_record(2, 20);
        op.final_location = Some(BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::new(), RealmId::from_bytes([4u8; 32])),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        });
        op.version_id = Some(Ulid::new());
        op.upload_parts = vec![requested.clone(), omitted.clone()];
        op.resolved_parts = vec![requested.clone()];

        let effects = op.begin_cleanup_part_blobs();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { location })] if location == &requested.location
        ));

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { location })] if location == &omitted.location
        ));
    }

    // A quota rejection at EnforceQuota leaves the finalize transaction open. It
    // must be aborted before the reset transaction starts, otherwise the storage
    // actor orphans the txn and pins an LSM snapshot forever.
    #[test]
    fn quota_rejection_aborts_finalize_txn_before_reset() {
        let input = finalize_input();
        let record = open_upload_record(&input);
        let mut op = CompleteMultipartUploadOperation::new(input);
        let finalize_txn = TxnId::new();
        op.txn_id = Some(finalize_txn);
        op.upload_record = Some(record);
        op.state = CompleteMultipartUploadState::EnforceQuota;

        let effects = op.schedule_error(CompleteMultipartUploadError::QuotaExceeded {
            limit: 30,
            usage: 35,
        });

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id }) if txn_id == finalize_txn
        ));
        assert_eq!(
            op.state,
            CompleteMultipartUploadState::AbortFinalizeTransaction
        );
        // Cleared so the reset StartTransaction can never overwrite (orphan) it.
        assert_eq!(op.txn_id, None);

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: finalize_txn,
        }));

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        ));
        assert_eq!(
            op.state,
            CompleteMultipartUploadState::ResetUploadTransaction
        );
        assert_eq!(
            op.pending_error,
            Some(CompleteMultipartUploadError::QuotaExceeded {
                limit: 30,
                usage: 35
            })
        );
    }

    // If the finalize-txn abort itself errors, the original quota error must still
    // be surfaced rather than masked by the abort failure.
    #[test]
    fn quota_rejection_abort_failure_preserves_original_error() {
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let finalize_txn = TxnId::new();
        op.txn_id = Some(finalize_txn);
        op.state = CompleteMultipartUploadState::EnforceQuota;

        let effects = op.schedule_error(CompleteMultipartUploadError::QuotaExceeded {
            limit: 30,
            usage: 35,
        });
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id }) if txn_id == finalize_txn
        ));

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert_eq!(
            op.finalize(),
            Err(CompleteMultipartUploadError::QuotaExceeded {
                limit: 30,
                usage: 35
            })
        );
    }
}
