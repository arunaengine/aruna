use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, blob_location_read, write_blob_head_effect,
    write_blob_location_effect, write_blob_version_effect,
};
use crate::blob::cleanup::{PendingCleanup, schedule_blob_cleanup_effect};
use crate::blob::managed_copy::{ManagedCopyError, register_effect};
use crate::group_backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement_policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::replication::queue::write_live_replication_obligation_effect;
use crate::usage_stats::{
    QuotaGate, QuotaGateError, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_usage_snapshot_publish_effect,
};
use aruna_blob::hash::Hasher;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum, HASH_MD5};
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, MultipartChecksumType, MultipartObjectMetadataKey,
    MultipartObjectPart, MultipartObjectSummary, MultipartUpload, MultipartUploadPart,
    MultipartUploadPartKey, MultipartUploadStatus, PathRestriction, PlacementPolicyError,
    PlacementPolicyRef, RealmId, ResolvedBackend, RoCrateLimits, UsageDelta, VersionKey,
    WriteOwner,
};
use aruna_core::types::{Effects, NodeId, TxnId, UserId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum CompleteMultipartUploadState {
    Init,
    StartMarkTransaction,
    ReadUploadForMark,
    WriteUploadCompleting,
    CommitMarkTransaction,
    ReadUploadParts,
    ReadGateBucket,
    PolicyGate,
    ComposeBlob,
    StartFinalizeTransaction,
    ReadBucketDefault,
    FenceBackend,
    CheckHashLookup,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WriteHashPathIndex,
    WriteBlobVersionRecord,
    RegisterManagedCopy,
    WriteObjectMetadata,
    DeleteUploadRecords,
    WriteCleanupRecords,
    WriteLiveReplicationObligation,
    EnforceQuota,
    UpdateUsage,
    CommitFinalizeTransaction,
    AbortFinalizeTransaction,
    ResetUploadTransaction,
    ReadUploadForReset,
    WriteUploadReset,
    CommitResetTransaction,
    CleanupFailedCompose,
    QueueCleanupRow,
    ReleaseReservation,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CompleteMultipartUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("stored part does not live on the upload's pinned backend")]
    BackendMismatch,
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
    #[error("Your proposed upload is smaller than the minimum allowed object size.")]
    EntityTooSmall,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error("multipart completion checksum contract does not match the initiation request")]
    ChecksumContractMismatch,
    #[error("missing MD5 hash for part etag validation")]
    MissingPartEtag,
    #[error("part etag mismatch")]
    PartEtagMismatch,
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error(transparent)]
    QuotaGateError(#[from] QuotaGateError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error(transparent)]
    PolicyGate(#[from] PolicyGateError),
    #[error(transparent)]
    PolicyError(#[from] PlacementPolicyError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
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
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: MultipartChecksumType,
    pub checksum_type_explicit: bool,
    pub object_size: Option<u64>,
    pub created_by: UserId,
    /// Hard ceiling (bytes) the group's realm-wide `logical_bytes` may reach,
    /// resolved from the realm quota config at the request surface. `None` =
    /// unlimited, so no gate is enforced.
    pub quota_ceiling: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteMultipartUploadResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
    pub checksum_type: MultipartChecksumType,
    pub response_hashes: HashMap<String, Vec<u8>>,
    pub part_count: usize,
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
    /// The composed object after a commit whose outcome is unknown. Held apart
    /// from `composed_location` so `abort` cannot delete bytes a commit owns.
    reconcile_location: Option<BackendLocation>,
    /// A pre-finalize write has no committed owner, even when its hash is absent.
    delete_location: Option<BackendLocation>,
    rollback_location: Option<BackendLocation>,
    release_id: Option<Ulid>,
    pending_cleanup: PendingCleanup,
    cleanup_closed: bool,
    final_location: Option<BackendLocation>,
    composite_hashes: HashMap<String, Vec<u8>>,
    version_id: Option<Ulid>,
    version_created_at: Option<SystemTime>,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    was_live: bool,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    pending_error: Option<CompleteMultipartUploadError>,
    output: Option<Result<CompleteMultipartUploadResult, CompleteMultipartUploadError>>,
    rocrate_limits: RoCrateLimits,
    restrictions: Option<Vec<PathRestriction>>,
    /// Refs sealed on the version record, reused verbatim by its registration.
    sealed_policies: Vec<PlacementPolicyRef>,
    /// Destination default, read inside the finalize transaction.
    bucket_policies: Vec<PlacementPolicyRef>,
    /// Destination facts of this node. Absent means no governed object may be
    /// composed or registered here.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// What the gate decided on, re-read inside the finalize transaction.
    gated_bucket: Option<GatedBucket>,
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
            reconcile_location: None,
            delete_location: None,
            rollback_location: None,
            release_id: None,
            pending_cleanup: PendingCleanup::default(),
            cleanup_closed: false,
            final_location: None,
            composite_hashes: HashMap::new(),
            version_id: None,
            version_created_at: None,
            existing_pointer: None,
            new_blob: false,
            was_live: false,
            usage_update: None,
            quota_gate: None,
            pending_error: None,
            output: None,
            rocrate_limits: RoCrateLimits::default(),
            restrictions: None,
            sealed_policies: Vec::new(),
            bucket_policies: Vec::new(),
            gate_context: None,
            gate: None,
            gated_bucket: None,
        }
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// The uploader's credential restrictions. They are persisted on the durable
    /// replication obligation, so a scoped upload cannot escalate to unscoped
    /// when the obligation repair path enqueues replication instead.
    /// The destination this completion is evaluated against. Omitting it leaves
    /// the ungoverned path untouched and fails every governed one closed.
    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    /// Subject generation the gate admitted this completion under; zero for an
    /// ungoverned object, which no subject ever evaluated.
    fn sealed_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    /// The terminal state is complete, so the driver never calls `abort` for us;
    /// releasing the transaction here is what keeps it from outliving the
    /// operation. `abort` takes the id, so it cannot run twice.
    fn emit_error(&mut self, error: CompleteMultipartUploadError) -> Effects {
        self.state = CompleteMultipartUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
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
        } else {
            self.rollback_composed_blob()
        }
    }

    /// Takes the location: once its delete is queued the rollback in `abort`
    /// must not queue a second one. A copy stays behind so a delete that fails
    /// can still be handed to the durable cleanup queue.
    fn rollback_composed_blob(&mut self) -> Effects {
        if let Some(location) = self.reconcile_location.take() {
            return self.queue_reconcile_write(location);
        }
        if let Some(location) = self.delete_location.take() {
            return self.queue_cleanup_work(BlobCleanupWork::DeleteBlob { location });
        }
        if let Some(location) = self.rollback_location.take() {
            return self.queue_cleanup_work(BlobCleanupWork::DeleteBlob { location });
        }
        self.state = CompleteMultipartUploadState::CleanupFailedCompose;
        match self.composed_location.take() {
            Some(location) => {
                self.rollback_location = Some(location.clone());
                smallvec![Effect::Blob(BlobEffect::Delete { location })]
            }
            None => self.emit_pending_error(),
        }
    }

    fn queue_reconcile_write(&mut self, location: BackendLocation) -> Effects {
        let Some(blake3) = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok())
        else {
            return self.queue_cleanup_work(BlobCleanupWork::ReconcileReservation { location });
        };
        self.queue_cleanup_work(BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Blob {
                blake3,
                realm_id: self.input.realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
            },
        })
    }

    fn queue_rollback_delete(&mut self) -> Effects {
        let Some(location) = self.rollback_location.take() else {
            return self.emit_pending_error();
        };
        self.queue_cleanup_work(BlobCleanupWork::DeleteBlob { location })
    }

    /// Hands one row to the durable cleanup queue outside any transaction. The
    /// row keeps the location until storage accepts it, so a refused write can
    /// still be retried rather than losing the only record of the bytes.
    fn queue_cleanup_work(&mut self, work: BlobCleanupWork) -> Effects {
        if self.cleanup_closed {
            return self.fail_node();
        }
        let Some(effect) = self.pending_cleanup.queue(work) else {
            return self.release_or_error();
        };
        self.state = CompleteMultipartUploadState::QueueCleanupRow;
        smallvec![effect]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                self.pending_cleanup.accepted();
                self.release_or_error()
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::ChannelClosed,
            }) => {
                self.cleanup_closed = true;
                self.fail_node()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                match self.pending_cleanup.retry(&error) {
                    Some(effect) => smallvec![effect],
                    None => self.release_or_error(),
                }
            }
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn fail_node(&mut self) -> Effects {
        self.state = CompleteMultipartUploadState::Error;
        if !matches!(self.output.as_ref(), Some(Err(_))) {
            self.output = Some(Err(self
                .pending_error
                .take()
                .unwrap_or(CompleteMultipartUploadError::CompleteMultipartUploadFailed)));
        }
        smallvec![]
    }

    fn release_or_error(&mut self) -> Effects {
        if self.has_cleanup() {
            return self.rollback_composed_blob();
        }
        let Some(id) = self.release_id else {
            return self.emit_pending_error();
        };
        self.state = CompleteMultipartUploadState::ReleaseReservation;
        smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
    }

    fn handle_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        if self.release_id != Some(id) {
            return self.emit_error(CompleteMultipartUploadError::InvalidOperationState);
        }
        self.release_id = None;
        if self.pending_error.is_some() {
            self.emit_pending_error()
        } else {
            self.finish_commit()
        }
    }

    fn finish_commit(&mut self) -> Effects {
        self.state = CompleteMultipartUploadState::Finish;
        smallvec![
            schedule_usage_snapshot_publish_effect(),
            schedule_blob_cleanup_effect()
        ]
    }

    fn handle_abort_finalize_transaction(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                self.continue_error_cleanup()
            }
            Event::Storage(StorageEvent::Error { error }) => self.abort_uncertain(&error),
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn abort_uncertain(&mut self, error: &StorageError) -> Effects {
        self.txn_id = None;
        self.preserve_blob();
        if matches!(error, StorageError::ChannelClosed) {
            self.cleanup_closed = true;
            return self.fail_node();
        }
        self.queue_reconcile()
    }

    fn queue_reconcile(&mut self) -> Effects {
        let Some(location) = self.reconcile_location.take() else {
            return self.fail_node();
        };
        self.queue_reconcile_write(location)
    }

    fn preserve_blob(&mut self) {
        if self.reconcile_location.is_none() {
            let location = self
                .composed_location
                .take()
                .or_else(|| self.delete_location.take())
                .or_else(|| self.rollback_location.take());
            self.reconcile_location = location;
        }
        self.release_id = None;
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

    fn validate_checksum_contract(
        &mut self,
        record: &MultipartUpload,
    ) -> Result<(), CompleteMultipartUploadError> {
        let Some(hint) = record.checksum_hint.as_ref() else {
            return Ok(());
        };

        if !self.input.checksum_type_explicit {
            self.input.checksum_type = hint.checksum_type;
        }
        if self.input.checksum_type_explicit && hint.checksum_type != self.input.checksum_type {
            return Err(CompleteMultipartUploadError::ChecksumContractMismatch);
        }

        if let Some(algorithm) = hint.algorithm
            && (self.input.checksum_algorithm.is_some()
                || !self.input.expected_checksums.is_empty())
        {
            let matching_expected = self
                .input
                .expected_checksums
                .iter()
                .any(|checksum| checksum.algorithm == algorithm);
            if self.input.checksum_algorithm != Some(algorithm) || !matching_expected {
                return Err(CompleteMultipartUploadError::ChecksumContractMismatch);
            }
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
            return self.schedule_error(err);
        }
        if let Err(err) = self.validate_checksum_contract(&record) {
            return self.schedule_error(err);
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
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
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
            Event::Storage(StorageEvent::Error { error }) if error.proves_no_commit() => {
                self.emit_error(error.into())
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.schedule_error(error.into())
            }
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
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
        let required_checksum_algorithm = self
            .upload_record
            .as_ref()
            .and_then(|upload| upload.checksum_hint.as_ref())
            .filter(|hint| hint.checksum_type == MultipartChecksumType::Composite)
            .and_then(|hint| hint.algorithm);
        for requested in &self.input.completed_parts {
            if previous.is_some_and(|prev| requested.part_number <= prev) {
                return Err(CompleteMultipartUploadError::InvalidPartOrder);
            }
            previous = Some(requested.part_number);

            let Some(record) = all_parts.get(&requested.part_number).cloned() else {
                return Err(CompleteMultipartUploadError::InvalidPart);
            };
            // Compose stays same-backend: a part elsewhere means a routing bug.
            if let Some(upload) = self.upload_record.as_ref()
                && record.location.backend != upload.backend
            {
                return Err(CompleteMultipartUploadError::BackendMismatch);
            }
            validate_requested_part(requested, &record, required_checksum_algorithm)?;
            resolved.push(record);
        }

        if resolved
            .iter()
            .take(resolved.len().saturating_sub(1))
            .any(|part| part.location.blob_size < 5 * 1024 * 1024)
        {
            return Err(CompleteMultipartUploadError::EntityTooSmall);
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
        self.resolved_parts = resolved;

        // The destination default is read before the compose, so the gate that
        // admits the object sees the refs the version would actually carry.
        self.state = CompleteMultipartUploadState::ReadGateBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let bucket = match value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(bucket) => bucket,
            Err(error) => return self.schedule_error(error.into()),
        };
        let inherited = self
            .upload_record
            .as_ref()
            .map(|upload| upload.placement_policies.clone())
            .unwrap_or_default();
        let refs = match union_refs(&GatedBucket::observe(bucket.as_ref()).policies, &inherited) {
            Ok(refs) => refs,
            Err(error) => return self.schedule_error(error.into()),
        };
        self.gated_bucket = Some(
            GatedBucket::observe(bucket.as_ref())
                .sealed_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        match write_gate(self.gate_context.as_ref(), &refs) {
            Ok(None) => self.compose_blob(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = CompleteMultipartUploadState::PolicyGate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.schedule_error(error.into()),
        }
    }

    fn handle_policy_gate(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.schedule_error(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome.decision) {
            Ok(()) => self.compose_blob(),
            Err(error) => self.schedule_error(error.into()),
        }
    }

    fn compose_blob(&mut self) -> Effects {
        let Some(upload) = self.upload_record.as_ref() else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let pinned = ResolvedBackend::new(upload.backend.clone(), upload.storage_class.clone());
        let parts = self
            .resolved_parts
            .iter()
            .map(|part| part.location.clone())
            .collect();
        self.state = CompleteMultipartUploadState::ComposeBlob;
        smallvec![Effect::Blob(BlobEffect::Compose {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved: pinned,
            created_by: self.input.created_by,
            parts,
        })]
    }

    fn handle_blob_composed(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.release_id = Some(location.ulid);
                self.delete_location = Some(location);
                return self
                    .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
            }
            _ => return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState),
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
        // The version snapshots the default this transaction observes, not one
        // read while the parts were still being uploaded.
        self.state = CompleteMultipartUploadState::ReadBucketDefault;
        smallvec![drift_reads(&self.input.bucket, self.txn_id)]
    }

    fn handle_default_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        let (bucket, subject) = match split_drift_reads(values) {
            Ok(split) => split,
            Err(error) => return self.schedule_error(error.into()),
        };
        // The refs the version commits must be the refs the gate admitted: a
        // default or subject changed during the compose was never evaluated.
        let observed = GatedBucket::observe(bucket.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return self.schedule_error(PolicyGateError::Drift.into());
            }
            if let Err(error) = gated.check_subject(subject.as_ref()) {
                return self.schedule_error(error.into());
            }
        }
        self.bucket_policies = observed.policies;

        let Some(location) = self.composed_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        // The compose already ran on the pinned backend, so the finalize must
        // prove it is still enabled or roll the composed object back.
        match fence_backend(&location.backend, self.txn_id) {
            Some(effect) => {
                self.state = CompleteMultipartUploadState::FenceBackend;
                smallvec![effect]
            }
            None => self.check_hash_lookup(),
        }
    }

    fn handle_backend_fenced(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.check_hash_lookup(),
            Err(error) => self.schedule_error(error.into()),
        }
    }

    fn check_hash_lookup(&mut self) -> Effects {
        let Some(location) = self.composed_location.clone() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteMultipartUploadError::MissingExpectedChecksum(
                "blake3",
            ));
        };
        // Only the copy on the upload's pinned backend may be deduplicated.
        let key = match BlobLocationKey::from_blake3(blake3_hash, location.backend.clone()) {
            Ok(key) => key,
            Err(error) => return self.schedule_error(error.into()),
        };
        self.state = CompleteMultipartUploadState::CheckHashLookup;
        smallvec![blob_location_read(&key, self.txn_id)]
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
        let version_id = *self.version_id.get_or_insert_with(Ulid::generate);
        let pointer = match CurrentVersionPointer::next_for(existing, version_id) {
            Ok(pointer) => pointer,
            Err(err) => return self.schedule_error(err.into()),
        };
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
        let Some(upload_record) = self.upload_record.as_ref() else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let version = BlobVersion::materialized(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self
                        .schedule_error(CompleteMultipartUploadError::ConversionError(err.into()));
                }
            },
            location.backend.clone(),
            created_at,
            self.input.created_by,
            None,
        )
        .with_metadata(upload_record.metadata.clone());
        // Union with what part copies inherited: a part-wise copy of a governed
        // source can only add refs to the composed object.
        let mut policies = self.bucket_policies.clone();
        policies.extend(upload_record.placement_policies.iter().copied());
        let version = match version.with_policies(policies) {
            Ok(version) => version,
            Err(err) => return self.schedule_error(err.into()),
        };
        let version_key = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        self.sealed_policies = version.placement_policies.clone();
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

        self.register_managed_copy()
    }

    /// Joins the finalize transaction, so the composed copy becomes serveable
    /// exactly when the logical version does and never before.
    fn register_managed_copy(&mut self) -> Effects {
        let (Some(version_id), Some(location)) = (self.version_id, self.final_location.clone())
        else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let effect = match register_effect(
            VersionKey::new(&self.input.bucket, &self.input.key, version_id),
            self.input.node_id,
            &location,
            &self.sealed_policies,
            self.sealed_subject(),
            version_id.timestamp_ms(),
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };
        self.state = CompleteMultipartUploadState::RegisterManagedCopy;
        smallvec![effect]
    }

    fn handle_copy_registered(&mut self, event: Event) -> Effects {
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
            composite_hashes: self.composite_hashes.clone(),
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
        self.write_cleanup_records()
    }

    // Deferred housekeeping commits atomically with the completed upload, so a
    // crash after commit can never leak part blobs.
    fn write_cleanup_records(&mut self) -> Effects {
        let mut works: Vec<BlobCleanupWork> = self
            .upload_parts
            .iter()
            .map(|record| BlobCleanupWork::DeleteBlob {
                location: record.location.clone(),
            })
            .collect();
        if let (Some(composed), Some(chosen)) = (
            self.composed_location.as_ref(),
            self.final_location.as_ref(),
        ) {
            if composed != chosen {
                works.push(BlobCleanupWork::DeleteBlob {
                    location: composed.clone(),
                });
            } else if let Some(blake3) = composed.get_blake3()
                && let Ok(blake3) = blake3.try_into()
            {
                works.push(BlobCleanupWork::ReconcileWrite {
                    location: composed.clone(),
                    owner: WriteOwner::Blob {
                        blake3,
                        realm_id: self.input.realm_id,
                        ttl_ms: self.rocrate_limits.holder_ttl_ms,
                    },
                });
            }
        }
        if let Some(blake3) = self
            .final_location
            .as_ref()
            .and_then(|location| location.get_blake3())
            && self.composed_location.as_ref() != self.final_location.as_ref()
            && let Ok(blake3) = blake3.try_into()
        {
            works.push(BlobCleanupWork::RegisterDht {
                blake3,
                realm_id: self.input.realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
            });
        }

        let mut writes = Vec::with_capacity(works.len());
        for work in works {
            let key = match &work {
                BlobCleanupWork::ReconcileWrite { location, .. } => {
                    location.ulid.to_bytes().to_vec().into()
                }
                BlobCleanupWork::DeleteBlob { .. } | BlobCleanupWork::RegisterDht { .. } => {
                    Ulid::generate().to_bytes().to_vec().into()
                }
                BlobCleanupWork::ReconcileReservation { .. } => {
                    Ulid::generate().to_bytes().to_vec().into()
                }
            };
            let value = match work.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.schedule_error(err.into()),
            };
            writes.push((BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into()));
        }

        self.state = CompleteMultipartUploadState::WriteCleanupRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_cleanup_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
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
                path_restrictions: self.restrictions.clone(),
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
        let stored = self
            .final_location
            .as_ref()
            .and_then(|location| StoredDelta::for_location(location, self.new_blob));
        let Some(stored) = stored else {
            return self
                .schedule_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        self.usage_update = Some(UsageCounterUpdate::with_stored(
            group_id,
            group_delta,
            stored,
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
            let mut gate = QuotaGate::new_for_realm(
                ceiling,
                object_size,
                group_id,
                self.input.node_id,
                self.input.realm_id,
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
        match gate.step(event, txn_id) {
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

    /// A commit whose outcome is unknown may already own the composed object, so
    /// only a proven refusal rolls it back. The rest moves to the reconciliation
    /// queue, out of reach of `abort`, and the committed blob location row
    /// decides its fate.
    fn handle_finalize_failure(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::Error { error }) = event else {
            return self.schedule_error(CompleteMultipartUploadError::InvalidOperationState);
        };
        if matches!(error, StorageError::TransactionConflict) {
            self.txn_id = None;
        }
        if !error.proves_no_commit() {
            self.txn_id = None;
            if let Some(location) = self.composed_location.take() {
                warn!(
                    event = "complete_multipart_upload.commit_outcome_unknown",
                    backend = %location.backend,
                    blob_size = location.blob_size,
                    error = %error,
                    "Queuing the composed object for reconciliation"
                );
                self.reconcile_location = Some(location);
            }
        }
        self.schedule_error(error.into())
    }

    fn handle_finalize_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.handle_finalize_failure(event);
        };
        self.txn_id = None;
        let release_id = self.composed_location.take().map(|location| location.ulid);
        let Some(location) = self.final_location.clone() else {
            return self.emit_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        };
        let Some(version_id) = self.version_id else {
            return self.emit_error(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
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
            part_count: self.resolved_parts.len(),
        }));
        if let Some(id) = release_id {
            self.release_id = Some(id);
            self.state = CompleteMultipartUploadState::ReleaseReservation;
            smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
        } else {
            self.finish_commit()
        }
    }

    fn handle_reset_transaction_started(&mut self, event: Event) -> Effects {
        let txn_id = match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self
                    .reset_failed(Some(CompleteMultipartUploadError::InvalidOperationState));
            }
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
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self
                    .reset_failed(Some(CompleteMultipartUploadError::InvalidOperationState));
            }
        };
        if let Some(value) = value {
            let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(err) => return self.reset_failed(Some(err.into())),
            };
            record.status = MultipartUploadStatus::Open;
            self.upload_record = Some(record.clone());
            let bytes = match record.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.reset_failed(Some(err.into())),
            };
            self.state = CompleteMultipartUploadState::WriteUploadReset;
            return smallvec![Effect::Storage(StorageEffect::Write {
                key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                key: self.input.upload_id.to_bytes().to_vec().into(),
                value: bytes.into(),
                txn_id: self.txn_id,
            })];
        }

        self.reset_failed(None)
    }

    fn handle_upload_reset_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self
                    .reset_failed(Some(CompleteMultipartUploadError::InvalidOperationState));
            }
        };
        let Some(txn_id) = self.txn_id else {
            return self.reset_failed(Some(CompleteMultipartUploadError::NoTransactionFound));
        };
        self.state = CompleteMultipartUploadState::CommitResetTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_reset_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                if matches!(error, StorageError::TransactionConflict) {
                    self.txn_id = None;
                    return self.rollback_composed_blob();
                }
                if !error.proves_no_commit() {
                    self.txn_id = None;
                    self.preserve_blob();
                    return self.queue_reconcile();
                }
                return self.reset_failed(None);
            }
            _ => {
                return self
                    .reset_failed(Some(CompleteMultipartUploadError::InvalidOperationState));
            }
        };
        self.txn_id = None;
        self.rollback_composed_blob()
    }

    fn reset_failed(&mut self, error: Option<CompleteMultipartUploadError>) -> Effects {
        if let Some(error) = error {
            self.pending_error = Some(error);
        }
        self.state = CompleteMultipartUploadState::CleanupFailedCompose;
        if let Some(txn_id) = self.txn_id.take() {
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.rollback_composed_blob()
    }

    fn handle_failed_compose_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                self.rollback_composed_blob()
            }
            Event::Storage(StorageEvent::Error { error }) => self.abort_uncertain(&error),
            Event::Blob(BlobEvent::DeleteFinished) => {
                self.rollback_location = None;
                if self.has_cleanup() {
                    self.rollback_composed_blob()
                } else {
                    self.emit_pending_error()
                }
            }
            // The composed object is still on the backend and this operation is
            // over; only a queued delete can still reach it.
            Event::Blob(BlobEvent::Error(_)) => self.queue_rollback_delete(),
            _ => self.emit_error(CompleteMultipartUploadError::InvalidOperationState),
        }
    }

    fn has_cleanup(&self) -> bool {
        self.reconcile_location.is_some()
            || self.delete_location.is_some()
            || self.rollback_location.is_some()
            || self.composed_location.is_some()
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
            CompleteMultipartUploadState::ReadGateBucket => self.handle_gate_bucket(event),
            CompleteMultipartUploadState::PolicyGate => self.handle_policy_gate(event),
            CompleteMultipartUploadState::ComposeBlob => self.handle_blob_composed(event),
            CompleteMultipartUploadState::StartFinalizeTransaction => {
                self.handle_finalize_transaction_started(event)
            }
            CompleteMultipartUploadState::ReadBucketDefault => self.handle_default_read(event),
            CompleteMultipartUploadState::FenceBackend => self.handle_backend_fenced(event),
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
            CompleteMultipartUploadState::RegisterManagedCopy => self.handle_copy_registered(event),
            CompleteMultipartUploadState::WriteObjectMetadata => {
                self.handle_object_metadata_written(event)
            }
            CompleteMultipartUploadState::DeleteUploadRecords => {
                self.handle_upload_records_deleted(event)
            }
            CompleteMultipartUploadState::WriteCleanupRecords => self.handle_cleanup_written(event),
            CompleteMultipartUploadState::WriteLiveReplicationObligation => {
                self.handle_live_replication_obligation_written(event)
            }
            CompleteMultipartUploadState::EnforceQuota => self.handle_enforce_quota(event),
            CompleteMultipartUploadState::UpdateUsage => self.handle_usage_update(event),
            CompleteMultipartUploadState::CommitFinalizeTransaction => {
                self.handle_finalize_committed(event)
            }
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
            CompleteMultipartUploadState::QueueCleanupRow => self.handle_cleanup_queued(event),
            CompleteMultipartUploadState::ReleaseReservation => self.handle_release(event),
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
        if self.state != CompleteMultipartUploadState::Finish {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        if self.cleanup_closed {
            return smallvec![];
        }
        if let Some(txn_id) = self.txn_id.take() {
            if matches!(
                self.state,
                CompleteMultipartUploadState::CommitFinalizeTransaction
                    | CompleteMultipartUploadState::CommitResetTransaction
            ) {
                self.preserve_blob();
            }
            if self.state != CompleteMultipartUploadState::Error
                || self.has_cleanup()
                || self.release_id.is_some()
            {
                self.state = CompleteMultipartUploadState::CleanupFailedCompose;
            }
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        if let Some(effect) = self.pending_cleanup.retry(&StorageError::Timeout) {
            self.state = CompleteMultipartUploadState::QueueCleanupRow;
            return smallvec![effect];
        }
        if self.has_cleanup() {
            return self.rollback_composed_blob();
        }
        if let Some(id) = self.release_id {
            self.state = CompleteMultipartUploadState::ReleaseReservation;
            return smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })];
        }
        self.state = CompleteMultipartUploadState::Error;
        if !matches!(self.output.as_ref(), Some(Err(_))) {
            self.output = Some(Err(
                CompleteMultipartUploadError::CompleteMultipartUploadFailed,
            ));
        }
        smallvec![]
    }
}

fn validate_requested_part(
    requested: &CompleteMultipartPart,
    record: &MultipartUploadPart,
    required_checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Result<(), CompleteMultipartUploadError> {
    if let Some(etag) = &requested.etag {
        let Some(md5) = record.location.hashes.get(HASH_MD5) else {
            return Err(CompleteMultipartUploadError::MissingPartEtag);
        };
        if hex::encode(md5) != *etag {
            return Err(CompleteMultipartUploadError::PartEtagMismatch);
        }
    }

    if let Some(algorithm) = required_checksum_algorithm
        && !requested
            .expected_checksums
            .iter()
            .any(|expected| expected.algorithm == algorithm)
    {
        return Err(CompleteMultipartUploadError::ChecksumContractMismatch);
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
    use aruna_core::structs::{BackendRef, MultipartUploadChecksumHint};
    use aruna_core::task::{TaskEffect, TaskKey};

    fn finalize_input() -> CompleteMultipartUploadInput {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        CompleteMultipartUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::generate(),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            completed_parts: vec![],
            expected_checksums: vec![],
            checksum_algorithm: None,
            checksum_type: MultipartChecksumType::FullObject,
            checksum_type_explicit: false,
            object_size: Some(10),
            created_by: UserId::local(Ulid::generate(), realm_id),
            quota_ceiling: Some(30),
        }
    }

    #[test]
    fn obligation_keeps_restrictions() {
        // The durable repair record is what a lost enqueue replays, so a scoped
        // credential must stay scoped on it.
        let restrictions = vec![PathRestriction {
            pattern: "/realm/g/group/data/node/bucket/scoped/**".to_string(),
            permission: aruna_core::structs::Permission::WRITE,
        }];
        let mut operation = CompleteMultipartUploadOperation::new(finalize_input())
            .with_restrictions(Some(restrictions.clone()));
        operation.version_id = Some(Ulid::generate());

        let effects = operation.write_live_replication_obligation();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one obligation write, got {effects:?}")
        };
        let record =
            crate::replication::queue::LiveReplicationObligationRecord::from_bytes(value.as_ref())
                .expect("obligation decodes");
        assert_eq!(record.auth_context.path_restrictions, Some(restrictions));
    }

    fn open_upload_record(input: &CompleteMultipartUploadInput) -> MultipartUpload {
        MultipartUpload {
            backend: BackendRef::node_default(),
            storage_class: None,
            upload_id: input.upload_id,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            group_id: Ulid::generate(),
            created_by: input.created_by,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Completing,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
        }
    }

    fn part_record(part_number: u16, blob_size: u64) -> MultipartUploadPart {
        MultipartUploadPart {
            part_number,
            location: BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/tmp".to_string(),
                storage_bucket: "multipart".to_string(),
                backend_path: format!("part-{part_number}"),
                ulid: Ulid::generate(),
                compressed: false,
                encrypted: false,
                created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([4u8; 32])),
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
    fn rejects_foreign_part() {
        // A part stored elsewhere means routing was re-run; compose must fail.
        let mut input = finalize_input();
        input.completed_parts = vec![CompleteMultipartPart {
            part_number: 1,
            etag: None,
            expected_checksums: Vec::new(),
        }];
        let mut operation = CompleteMultipartUploadOperation::new(input);
        let record = open_upload_record(&operation.input);
        let upload_id = record.upload_id;
        operation.upload_record = Some(record);
        let mut part = part_record(1, 10);
        part.location.backend = BackendRef::Node("elsewhere".to_string());

        let result = operation.extract_requested_parts(part_values(upload_id, vec![part]));

        assert!(matches!(
            result,
            Err(CompleteMultipartUploadError::BackendMismatch)
        ));
    }

    fn part_values(
        upload_id: Ulid,
        parts: Vec<MultipartUploadPart>,
    ) -> Vec<(aruna_core::types::Key, aruna_core::types::Value)> {
        parts
            .into_iter()
            .map(|part| {
                (
                    MultipartUploadPartKey::new(upload_id, part.part_number)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    part.to_bytes().unwrap().into(),
                )
            })
            .collect()
    }

    #[test]
    fn refuses_disabled_backend() {
        // Compose already ran on the pinned backend, so the finalize fence has
        // to abort the transaction and roll the composed object back.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.upload_record = Some(open_upload_record(&op.input));
        op.composed_location = Some(composed_location(backend_id));
        op.state = CompleteMultipartUploadState::StartFinalizeTransaction;
        let txn_id = TxnId::generate();

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, CompleteMultipartUploadState::ReadBucketDefault);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (b"bucket".to_vec().into(), None),
                (b"subject".to_vec().into(), None),
            ],
        }));
        assert_eq!(op.state, CompleteMultipartUploadState::FenceBackend);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled_record(backend_id).into()),
        }));

        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                    if *aborted == txn_id
            ),
            "expected the finalize transaction to abort, got {effects:?}"
        );
        assert_eq!(
            op.pending_error,
            Some(BackendFenceError::Unavailable.into())
        );
    }

    #[test]
    fn fence_rejects_stray() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
        op.state = CompleteMultipartUploadState::FenceBackend;

        op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));

        assert!(matches!(
            op.pending_error,
            Some(CompleteMultipartUploadError::BackendFenceError(
                BackendFenceError::Read(_)
            ))
        ));
    }

    #[test]
    fn rollback_queues_cleanup() {
        // A backend that refuses the rollback delete would otherwise leave a
        // composed object no location or cleanup row can find.
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
        op.state = CompleteMultipartUploadState::FenceBackend;

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled_record(Ulid::from_bytes([5u8; 16])).into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));

        let effects = op.step(Event::Blob(BlobEvent::Error(
            aruna_core::errors::BlobError::DeleteError("unreachable".to_string()),
        )));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_CLEANUP_KEYSPACE
        ));
        assert!(
            op.step(Event::Storage(StorageEvent::WriteResult {
                key: b"x".to_vec().into(),
            }))
            .is_empty()
        );
        assert!(op.is_complete());
    }

    #[test]
    fn commit_keeps_composed() {
        // A finalize commit that may have landed already owns the composed
        // object, so nothing here may delete it; it goes to reconciliation with
        // the blob location row that decides whether the commit landed.
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let mut location = composed_location(Ulid::from_bytes([5u8; 16]));
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        op.composed_location = Some(location.clone());
        op.state = CompleteMultipartUploadState::CommitFinalizeTransaction;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected reconciliation to be queued, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Blob {
                    blake3: [7u8; 32],
                    realm_id: op.input.realm_id,
                    ttl_ms: RoCrateLimits::default().holder_ttl_ms,
                },
            }
        );
        assert!(op.composed_location.is_none());

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"k".to_vec().into(),
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, CompleteMultipartUploadState::Error);
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(CompleteMultipartUploadError::StorageError(
                StorageError::CommitFailed
            ))
        ));
    }

    #[test]
    fn release_on_failure() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let mut location = composed_location(Ulid::from_bytes([5u8; 16]));
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        let id = location.ulid;
        op.pending_error = Some(CompleteMultipartUploadError::StorageError(
            StorageError::CommitFailed,
        ));
        op.release_id = Some(id);
        op.state = CompleteMultipartUploadState::QueueCleanupRow;
        assert!(
            op.pending_cleanup
                .queue(BlobCleanupWork::ReconcileWrite {
                    location,
                    owner: WriteOwner::Blob {
                        blake3: [7u8; 32],
                        realm_id: op.input.realm_id,
                        ttl_ms: op.rocrate_limits.holder_ttl_ms,
                    },
                })
                .is_some()
        );

        // The row is retried until storage accepts it; only then is the
        // reservation released.
        for _ in 0..2 {
            assert!(matches!(
                op.step(Event::Storage(StorageEvent::Error {
                    error: StorageError::Timeout,
                }))
                .as_slice(),
                [Effect::Storage(StorageEffect::Write { .. })]
            ));
        }
        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"k".to_vec().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation { id: observed })]
                if *observed == id
        ));
        assert!(
            op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
                .is_empty()
        );
        assert!(op.is_complete());
    }

    #[test]
    fn cleanup_keeps_location() {
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let location = composed_location(Ulid::from_bytes([5u8; 16]));
        let release_id = location.ulid;
        let record = open_upload_record(&op.input);
        op.upload_record = Some(record.clone());
        op.state = CompleteMultipartUploadState::ComposeBlob;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
            location: location.clone(),
            message: "reservation finalization failed".to_string(),
        })));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        assert_eq!(op.delete_location, Some(location.clone()));
        assert_eq!(op.reconcile_location, None);
        assert_eq!(op.release_id, Some(release_id));

        let reset_txn = TxnId::generate();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: reset_txn,
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(record.to_bytes().unwrap().into()),
        }));
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: reset_txn,
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected durable cleanup row, got {effects:?}");
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(key.as_ref().len(), 16);
        assert_eq!(*txn_id, None);
        assert!(matches!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location: observed } if observed == location
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: release_id
            })]
        );
        assert!(
            op.step(Event::Blob(BlobEvent::ReservationReleased {
                id: release_id
            }))
            .is_empty()
        );
        assert_eq!(
            op.finalize(),
            Err(CompleteMultipartUploadError::CompleteMultipartUploadFailed)
        );
    }

    #[test]
    fn cleanup_reset_error() {
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let location = composed_location(Ulid::from_bytes([5u8; 16]));
        let release_id = location.ulid;
        op.upload_record = Some(open_upload_record(&op.input));
        op.state = CompleteMultipartUploadState::ComposeBlob;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
            location: location.clone(),
            message: "reservation finalization failed".to_string(),
        })));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected durable cleanup row, got {effects:?}");
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: release_id
            })]
        );
    }

    #[test]
    fn conflict_deletes_composed() {
        // A refused finalize proves no version names the composed object.
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
        op.state = CompleteMultipartUploadState::CommitFinalizeTransaction;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));
    }

    #[test]
    fn abort_deletes_composed() {
        // The reset transaction never commits, so nothing else can reach the
        // composed object once this operation ends.
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.composed_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
        op.state = CompleteMultipartUploadState::CommitResetTransaction;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));
        assert!(op.composed_location.is_none());
    }

    #[test]
    fn unknown_reset_reconciles() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let location = composed_location(Ulid::from_bytes([5u8; 16]));
        op.composed_location = Some(location.clone());
        op.txn_id = Some(TxnId::from_bytes([3u8; 16]));
        op.pending_error = Some(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        op.state = CompleteMultipartUploadState::CommitResetTransaction;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected reconciliation row, got {effects:?}");
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert!(matches!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::ReconcileReservation { location: observed }
                if observed == location
        ));
        assert_eq!(op.txn_id, None);
        assert_eq!(op.state, CompleteMultipartUploadState::QueueCleanupRow);
    }

    #[test]
    fn abort_keeps_blob() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let location = composed_location(Ulid::from_bytes([5u8; 16]));
        op.composed_location = Some(location.clone());
        op.pending_error = Some(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        op.state = CompleteMultipartUploadState::AbortFinalizeTransaction;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected reconciliation row, got {effects:?}");
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert!(matches!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::ReconcileReservation { location: observed }
                if observed == location
        ));
        assert_eq!(op.release_id, None);
    }

    #[test]
    fn cleanup_close_stops() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        let location = composed_location(Ulid::from_bytes([5u8; 16]));
        op.pending_error = Some(CompleteMultipartUploadError::CompleteMultipartUploadFailed);
        op.state = CompleteMultipartUploadState::QueueCleanupRow;
        assert!(
            op.pending_cleanup
                .queue(BlobCleanupWork::ReconcileReservation { location })
                .is_some()
        );

        assert!(
            op.step(Event::Storage(StorageEvent::Error {
                error: StorageError::ChannelClosed,
            }))
            .is_empty()
        );
        assert_eq!(op.state, CompleteMultipartUploadState::Error);
        assert!(op.abort().is_empty());
    }

    #[test]
    fn abort_queues_cleanup() {
        let mut op = CompleteMultipartUploadOperation::new(finalize_input());
        op.delete_location = Some(composed_location(Ulid::from_bytes([5u8; 16])));
        op.state = CompleteMultipartUploadState::ComposeBlob;

        let effects = op.abort();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_CLEANUP_KEYSPACE
        ));
        assert_eq!(op.state, CompleteMultipartUploadState::QueueCleanupRow);
        assert!(op.delete_location.is_none());
        assert!(matches!(
            op.finalize(),
            Err(CompleteMultipartUploadError::CompleteMultipartUploadFailed)
        ));
    }

    fn composed_location(backend_id: Ulid) -> BackendLocation {
        let mut location = part_record(1, 10).location;
        location.backend = BackendRef::Group(backend_id);
        location.partial = false;
        location
    }

    fn disabled_record(backend_id: Ulid) -> Vec<u8> {
        aruna_core::structs::GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: true,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        }
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn checksum_contract_failure_aborts_mark_transaction() {
        let mut input = finalize_input();
        input.checksum_type = MultipartChecksumType::FullObject;
        input.checksum_type_explicit = true;
        let mut record = open_upload_record(&input);
        record.status = MultipartUploadStatus::Open;
        record.checksum_hint = Some(MultipartUploadChecksumHint {
            algorithm: Some(ChecksumAlgorithm::Sha256),
            checksum_type: MultipartChecksumType::Composite,
        });
        let mut op = CompleteMultipartUploadOperation::new(input);
        let txn_id = TxnId::generate();
        op.txn_id = Some(txn_id);
        op.state = CompleteMultipartUploadState::ReadUploadForMark;

        let effects = op.handle_upload_read_for_mark(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(record.to_bytes().unwrap().into()),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == txn_id
        ));
        assert_eq!(op.txn_id, None);
    }

    #[test]
    fn composite_checksum_contract_allows_only_per_part_checksums() {
        let digest = vec![7; ChecksumAlgorithm::Sha256.digest_len()];
        let mut input = finalize_input();
        input.completed_parts = vec![CompleteMultipartPart {
            part_number: 1,
            etag: None,
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Sha256,
                digest: digest.clone(),
            }],
        }];
        input.object_size = Some(1);
        let mut upload = open_upload_record(&input);
        upload.checksum_hint = Some(MultipartUploadChecksumHint {
            algorithm: Some(ChecksumAlgorithm::Sha256),
            checksum_type: MultipartChecksumType::Composite,
        });
        let mut part = part_record(1, 1);
        part.location
            .hashes
            .insert(ChecksumAlgorithm::Sha256.hash_key().to_string(), digest);
        let values = part_values(input.upload_id, vec![part]);
        let mut op = CompleteMultipartUploadOperation::new(input);

        assert_eq!(op.validate_checksum_contract(&upload), Ok(()));
        assert_eq!(op.input.checksum_type, MultipartChecksumType::Composite);
        op.upload_record = Some(upload);
        assert!(op.extract_requested_parts(values).is_ok());
    }

    #[test]
    fn requires_part_checksum() {
        let mut input = finalize_input();
        input.completed_parts = vec![CompleteMultipartPart {
            part_number: 1,
            etag: None,
            expected_checksums: vec![],
        }];
        input.object_size = Some(1);
        let mut upload = open_upload_record(&input);
        upload.checksum_hint = Some(MultipartUploadChecksumHint {
            algorithm: Some(ChecksumAlgorithm::Sha256),
            checksum_type: MultipartChecksumType::Composite,
        });
        let values = part_values(input.upload_id, vec![part_record(1, 1)]);
        let mut op = CompleteMultipartUploadOperation::new(input);
        op.upload_record = Some(upload);

        assert_eq!(
            op.extract_requested_parts(values),
            Err(CompleteMultipartUploadError::ChecksumContractMismatch)
        );
    }

    #[test]
    fn rejects_undersized_non_final_part() {
        let mut input = finalize_input();
        input.completed_parts = vec![
            CompleteMultipartPart {
                part_number: 1,
                etag: None,
                expected_checksums: vec![],
            },
            CompleteMultipartPart {
                part_number: 2,
                etag: None,
                expected_checksums: vec![],
            },
        ];
        input.object_size = None;
        let values = part_values(
            input.upload_id,
            vec![part_record(1, 5 * 1024 * 1024 - 1), part_record(2, 1)],
        );
        let op = CompleteMultipartUploadOperation::new(input);

        assert_eq!(
            op.extract_requested_parts(values),
            Err(CompleteMultipartUploadError::EntityTooSmall)
        );
    }

    #[test]
    fn allows_undersized_final_part() {
        let mut input = finalize_input();
        input.completed_parts = vec![
            CompleteMultipartPart {
                part_number: 1,
                etag: None,
                expected_checksums: vec![],
            },
            CompleteMultipartPart {
                part_number: 2,
                etag: None,
                expected_checksums: vec![],
            },
        ];
        input.object_size = None;
        let values = part_values(
            input.upload_id,
            vec![part_record(1, 5 * 1024 * 1024), part_record(2, 1)],
        );
        let op = CompleteMultipartUploadOperation::new(input);

        assert!(op.extract_requested_parts(values).is_ok());
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
    fn cleanup_covers_omitted() {
        // Deferred delete records must cover requested AND omitted parts.
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let requested = part_record(1, 10);
        let omitted = part_record(2, 20);
        let mut final_location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([4u8; 32])),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        };
        final_location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        op.final_location = Some(final_location.clone());
        op.composed_location = Some(final_location.clone());
        let txn_id = Ulid::generate();
        op.txn_id = Some(txn_id);
        op.version_id = Some(Ulid::generate());
        op.upload_parts = vec![requested.clone(), omitted.clone()];
        op.resolved_parts = vec![requested.clone()];

        let effects = op.write_cleanup_records();
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: observed,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected cleanup batch write");
        };
        assert_eq!(*observed, Some(txn_id));
        assert!(writes.iter().any(|(key_space, key, value)| {
            key_space == BLOB_CLEANUP_KEYSPACE
                && key.as_ref() == final_location.ulid.to_bytes()
                && matches!(
                    BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
                    BlobCleanupWork::ReconcileWrite { .. }
                )
        }));
        let works: Vec<BlobCleanupWork> = writes
            .iter()
            .map(|(key_space, _, value)| {
                assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
                BlobCleanupWork::from_bytes(value.as_ref()).unwrap()
            })
            .collect();
        for record in [&requested, &omitted] {
            assert!(works.iter().any(|work| matches!(
                work,
                BlobCleanupWork::DeleteBlob { location } if location == &record.location
            )));
        }
        assert!(works.iter().any(|work| matches!(
            work,
            BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::Blob {
                    blake3,
                    realm_id,
                    ttl_ms,
                },
            } if *blake3 == [7u8; 32]
                && location.get_blake3() == Some(&[7u8; 32][..])
                && *realm_id == op.input.realm_id
                && *ttl_ms == op.rocrate_limits.holder_ttl_ms
        )));
    }

    #[test]
    fn finish_after_commit() {
        // The response must be ready at finalize commit; housekeeping is deferred.
        let input = finalize_input();
        let mut op = CompleteMultipartUploadOperation::new(input);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([4u8; 32])),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        };
        op.final_location = Some(location.clone());
        op.composed_location = Some(location.clone());
        op.version_id = Some(Ulid::generate());
        op.state = CompleteMultipartUploadState::CommitFinalizeTransaction;
        op.txn_id = Some(TxnId::generate());

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::generate(),
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: location.ulid
            })]
        );
        assert_eq!(op.state, CompleteMultipartUploadState::ReleaseReservation);
        let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
            id: location.ulid,
        }));
        assert!(op.is_complete());
        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::PublishUsageSnapshots,
                    ..
                }),
                Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::DrainBlobCleanupQueue,
                    ..
                }),
            ]
        ));
        let result = op.finalize().unwrap().unwrap().unwrap();
        assert_eq!(result.location, location);
    }

    // A quota rejection at EnforceQuota leaves the finalize transaction open. It
    // must be aborted before the reset transaction starts, otherwise the storage
    // actor orphans the txn and pins an LSM snapshot forever.
    #[test]
    fn quota_rejection_aborts_finalize_txn_before_reset() {
        let input = finalize_input();
        let record = open_upload_record(&input);
        let mut op = CompleteMultipartUploadOperation::new(input);
        let finalize_txn = TxnId::generate();
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
        let finalize_txn = TxnId::generate();
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

    #[test]
    fn unknown_mark_resets() {
        let input = finalize_input();
        let mut operation = CompleteMultipartUploadOperation::new(input);
        operation.upload_record = Some(open_upload_record(&operation.input));
        operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
        operation.state = CompleteMultipartUploadState::CommitMarkTransaction;

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        assert_eq!(
            operation.state,
            CompleteMultipartUploadState::ResetUploadTransaction
        );
        assert_eq!(operation.txn_id, None);
        assert_eq!(
            operation.pending_error,
            Some(CompleteMultipartUploadError::StorageError(
                StorageError::CommitFailed
            ))
        );
    }

    #[test]
    fn conflict_mark_aborts() {
        let input = finalize_input();
        let mut operation = CompleteMultipartUploadOperation::new(input);
        operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
        operation.state = CompleteMultipartUploadState::CommitMarkTransaction;
        let txn_id = operation.txn_id.unwrap();

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert_eq!(operation.state, CompleteMultipartUploadState::Error);
        assert_eq!(operation.txn_id, None);
    }

    #[test]
    fn committed_mark_continues() {
        let input = finalize_input();
        let mut operation = CompleteMultipartUploadOperation::new(input);
        operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
        operation.state = CompleteMultipartUploadState::CommitMarkTransaction;

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from_bytes([3u8; 16]),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == S3_MULTIPART_UPLOAD_PART_KEYSPACE
        ));
        assert_eq!(
            operation.state,
            CompleteMultipartUploadState::ReadUploadParts
        );
        assert_eq!(operation.txn_id, None);
    }
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use crate::placement_policy::PolicyCacheEntry;
    use aruna_core::structs::{
        BackendRef, MultipartUploadChecksumHint, PlacementPolicy, PlacementSelector,
        PlacementSubject, VerifiedPolicy,
    };
    use aruna_core::types::Value;
    use std::collections::BTreeMap;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn node() -> aruna_core::types::NodeId {
        iroh::SecretKey::from_bytes(&[9u8; 32]).public()
    }

    fn policy(location: &str) -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([1u8; 16]),
            "residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some(location.to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn gate(location: &str) -> GateContext {
        GateContext {
            realm_id: realm(),
            subject: PlacementSubject {
                node_id: node(),
                generation: 1,
                location: location.to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: true,
            },
            now_ms: 1_000,
        }
    }

    fn input() -> CompleteMultipartUploadInput {
        let realm_id = realm();
        CompleteMultipartUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::generate(),
            realm_id,
            node_id: node(),
            completed_parts: vec![],
            expected_checksums: vec![],
            checksum_algorithm: None,
            checksum_type: MultipartChecksumType::FullObject,
            checksum_type_explicit: false,
            object_size: Some(10),
            created_by: UserId::local(Ulid::generate(), realm_id),
            quota_ceiling: Some(30),
        }
    }

    fn upload(input: &CompleteMultipartUploadInput) -> MultipartUpload {
        MultipartUpload {
            upload_id: input.upload_id,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            group_id: Ulid::generate(),
            created_by: input.created_by,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None::<MultipartUploadChecksumHint>,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
        }
    }

    fn bucket(refs: Vec<PlacementPolicyRef>, generation: u64) -> Value {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: refs,
            placement_policy_generation: generation,
        };
        info.to_bytes().expect("bucket encodes").into()
    }

    fn read(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value,
        })
    }

    /// `location` of `None` leaves the node without a subject, which fails
    /// every governed completion closed.
    fn at_gate(location: Option<&str>) -> CompleteMultipartUploadOperation {
        let input = input();
        let record = upload(&input);
        let mut operation = CompleteMultipartUploadOperation::new(input);
        if let Some(location) = location {
            operation = operation.with_gate(gate(location));
        }
        operation.upload_record = Some(record);
        operation.state = CompleteMultipartUploadState::ReadGateBucket;
        operation
    }

    fn composes(effects: &Effects) -> bool {
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::Blob(BlobEffect::Compose { .. })))
    }

    #[test]
    fn denies_before_compose() {
        // The rule admits another location, so no part may be composed at all.
        let rule = policy("us-east");
        let mut operation = at_gate(Some("eu-west"));
        let effects = operation.step(read(Some(bucket(vec![rule.policy_ref()], 1))));
        assert!(!composes(&effects));

        let document = crate::placement_policy::fixtures::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(cached.into())));
        let effects = operation.step(crate::placement_policy::fixtures::authority(realm()));

        assert!(!composes(&effects));
        assert_eq!(
            operation.pending_error,
            Some(CompleteMultipartUploadError::PolicyGate(
                PolicyGateError::Denied {
                    policy_ids: vec![rule.policy().policy_id]
                }
            ))
        );
    }

    #[test]
    fn no_policy_blocks_compose() {
        // A rule that cannot be obtained blocks; it is never read as a grant.
        let rule = policy("eu-west");
        let mut operation = at_gate(Some("eu-west"));
        operation.step(read(Some(bucket(vec![rule.policy_ref()], 1))));
        let hint = PolicyCacheEntry::unavailable(1_000)
            .to_bytes()
            .expect("entry encodes");
        let effects = operation.step(read(Some(hint.into())));

        assert!(!composes(&effects));
        assert!(matches!(
            operation.pending_error,
            Some(CompleteMultipartUploadError::PolicyGate(
                PolicyGateError::Unavailable { .. }
            ))
        ));
    }

    #[test]
    fn missing_subject_blocks_compose() {
        let mut operation = at_gate(None);
        let effects = operation.step(read(Some(bucket(
            vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([1u8; 16]),
                digest: [4u8; 32],
            }],
            1,
        ))));

        assert!(!composes(&effects));
        assert_eq!(
            operation.pending_error,
            Some(CompleteMultipartUploadError::PolicyGate(
                PolicyGateError::NoSubject
            ))
        );
    }

    #[test]
    fn ungoverned_composes() {
        // An object with no refs reaches the compose with no policy round trip.
        let mut operation = at_gate(Some("eu-west"));
        let effects = operation.step(read(Some(bucket(Vec::new(), 0))));
        assert!(composes(&effects));
    }

    #[test]
    fn drift_aborts_finalize() {
        // The default changed while the parts composed, so the object must not
        // commit refs nothing evaluated.
        let mut operation = at_gate(Some("eu-west"));
        operation.step(read(Some(bucket(Vec::new(), 0))));
        operation.composed_location = Some(composed());
        operation.txn_id = Some(Ulid::from_bytes([7u8; 16]));
        operation.state = CompleteMultipartUploadState::ReadBucketDefault;

        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    Vec::new().into(),
                    Some(bucket(vec![policy("us-east").policy_ref()], 1)),
                ),
                (Vec::new().into(), None),
            ],
        }));

        assert_eq!(
            operation.pending_error,
            Some(CompleteMultipartUploadError::PolicyGate(
                PolicyGateError::Drift
            ))
        );
    }

    fn composed() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: UserId::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::from([("blake3".to_string(), vec![6u8; 32])]),
        }
    }
}
