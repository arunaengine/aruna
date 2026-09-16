use crate::blob::cleanup::schedule_cleanup_effect;
use crate::blob::managed_copy::{CopyRegistration, ManagedCopyError, register_effect};
use crate::blob::records::{
    HeadAliasContext, add_index_effect, blob_location_read, write_head_effect,
    write_location_effect, write_version_effect,
};
use crate::groups::backends::{BackendFenceError, check_fence, fence_backend};
use crate::node::usage_stats::{
    QuotaGate, QuotaGateError, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish,
};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::replication::queue::build_live_obligation;
use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use crate::s3::write_cleanup::{CleanupStep, WriteCleanup, delete_records_effect};
use aruna_blob::hash::Hasher;
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, OBJECT_METADATA_KEYSPACE,
    S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum, HASH_MD5};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CopyOrigin, CurrentVersionPointer, ResolvedBackend, VersionKey, WriteOwner,
};
use aruna_core::structs::storage::multipart::{
    MultipartChecksumType, MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary,
    MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum CompleteUploadState {
    Init,
    StartMarkTransaction,
    CheckPurgeMark,
    ReadUploadMark,
    WriteUploadCompleting,
    CommitMarkTransaction,
    ReadUploadParts,
    ReadGateBucket,
    PolicyGate,
    ComposeBlob,
    StartFinalizeTransaction,
    CheckPurgeFinalize,
    ReadBucketDefault,
    FenceBackend,
    CheckHashLookup,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WritePathIndex,
    WriteVersionRecord,
    RegisterManagedCopy,
    WriteObjectMetadata,
    DeleteUploadRecords,
    WriteCleanupRecords,
    WriteReplicationObligation,
    EnforceQuota,
    UpdateUsage,
    CommitFinalizeTransaction,
    AbortFinalizeTransaction,
    ResetUploadTransaction,
    ReadUploadReset,
    WriteUploadReset,
    CommitResetTransaction,
    CleanupFailedCompose,
    QueueCleanupRow,
    ReleaseReservation,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CompleteUploadError {
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
    #[error("The upload is being completed, retry shortly.")]
    CompletionInProgress,
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
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("CompleteMultipartUpload failed")]
    CompleteUploadFailed,
    #[error("operation did not finish")]
    NotFinished,
}

impl From<UploadTargetError> for CompleteUploadError {
    fn from(error: UploadTargetError) -> Self {
        match error {
            UploadTargetError::TargetMismatch => Self::UploadTargetMismatch,
            UploadTargetError::NotOpen => Self::UploadNotOpen,
            UploadTargetError::CompletionInProgress => Self::CompletionInProgress,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteMultipartPart {
    pub part_number: u16,
    pub etag: Option<String>,
    pub expected_checksums: Vec<ExpectedChecksum>,
}

#[derive(Debug, PartialEq)]
pub struct CompleteUploadInput {
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
    /// Wall clock (epoch ms) the completion lease is stamped and judged against.
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteUploadResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
    pub checksum_type: MultipartChecksumType,
    pub response_hashes: HashMap<String, Vec<u8>>,
    pub part_count: usize,
}

#[derive(Debug, PartialEq)]
pub struct CompleteUploadOperation {
    state: CompleteUploadState,
    input: CompleteUploadInput,
    txn_id: Option<TxnId>,
    upload_record: Option<MultipartUpload>,
    upload_parts: Vec<MultipartPart>,
    resolved_parts: Vec<MultipartPart>,
    composed_location: Option<BackendLocation>,
    /// The composed object after a commit whose outcome is unknown. Held apart
    /// from `composed_location` so `abort` cannot delete bytes a commit owns.
    reconcile_location: Option<BackendLocation>,
    /// A pre-finalize write has no committed owner, even when its hash is absent.
    delete_location: Option<BackendLocation>,
    rollback_location: Option<BackendLocation>,
    cleanup: WriteCleanup<CompleteUploadError>,
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
    output: Option<Result<CompleteUploadResult, CompleteUploadError>>,
    rocrate_limits: RoCrateLimits,
    restrictions: Option<Vec<PathRestriction>>,
    /// Refs stored on the version record, reused verbatim by its registration.
    stored_policies: Vec<PlacementPolicyRef>,
    /// Destination default, read inside the finalize transaction.
    bucket_policies: Vec<PlacementPolicyRef>,
    /// Destination details of this node. Absent means no governed object may be
    /// composed or registered here.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// What the gate decided on, re-read inside the finalize transaction.
    gated_bucket: Option<GatedBucket>,
    /// The reset that returns the record to `Open` has already been taken, so
    /// no later cleanup step may take it a second time.
    reset_done: bool,
}

impl CompleteUploadOperation {
    pub fn new(input: CompleteUploadInput) -> Self {
        Self {
            state: CompleteUploadState::Init,
            input,
            txn_id: None,
            upload_record: None,
            upload_parts: Vec::new(),
            resolved_parts: Vec::new(),
            composed_location: None,
            reconcile_location: None,
            delete_location: None,
            rollback_location: None,
            cleanup: WriteCleanup::default(),
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
            output: None,
            rocrate_limits: RoCrateLimits::default(),
            restrictions: None,
            stored_policies: Vec::new(),
            bucket_policies: Vec::new(),
            gate_context: None,
            gate: None,
            gated_bucket: None,
            reset_done: false,
        }
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// The uploader's credential restrictions, persisted on the durable
    /// replication obligation so a scoped upload cannot escalate. The gate
    /// destination: omitting it fails every governed completion closed.
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
    fn stored_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    /// The terminal state is complete, so the driver never calls `abort` for us;
    /// releasing the transaction here is what keeps it from outliving the
    /// operation. `abort` takes the id, so it cannot run twice.
    fn emit_error(&mut self, error: CompleteUploadError) -> Effects {
        self.state = CompleteUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn schedule_error(&mut self, error: CompleteUploadError) -> Effects {
        self.cleanup.set_error(error);
        // Abort the open finalize transaction before the reset one or the
        // orphaned txn pins an LSM snapshot; the error stays pending.
        if let Some(txn_id) = self.txn_id.take() {
            self.state = CompleteUploadState::AbortFinalizeTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.continue_error_cleanup()
    }

    /// Whether the record may still be `Completing` in storage.
    fn needs_reset(&self) -> bool {
        self.upload_record.is_some() && !self.reset_done
    }

    fn continue_error_cleanup(&mut self) -> Effects {
        if self.needs_reset() {
            self.reset_done = true;
            self.state = CompleteUploadState::ResetUploadTransaction;
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
        self.state = CompleteUploadState::CleanupFailedCompose;
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
        let Some(effect) = self.cleanup.queue(work) else {
            return self.release_or_error();
        };
        self.state = CompleteUploadState::QueueCleanupRow;
        smallvec![effect]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        match self.cleanup.handle_queued(event) {
            CleanupStep::Retry(effect) => smallvec![effect],
            CleanupStep::Accepted | CleanupStep::Exhausted => self.release_or_error(),
            CleanupStep::Closed => {
                self.cleanup_closed = true;
                self.fail_node()
            }
            CleanupStep::Invalid => self.emit_error(CompleteUploadError::InvalidOperationState),
        }
    }

    fn fail_node(&mut self) -> Effects {
        self.state = CompleteUploadState::Error;
        if !matches!(self.output.as_ref(), Some(Err(_))) {
            self.output = Some(Err(self
                .cleanup
                .take_error()
                .unwrap_or(CompleteUploadError::CompleteUploadFailed)));
        }
        smallvec![]
    }

    fn release_or_error(&mut self) -> Effects {
        if self.has_cleanup() {
            return self.rollback_composed_blob();
        }
        let Some(effect) = self.cleanup.release_effect() else {
            return self.emit_pending_error();
        };
        self.state = CompleteUploadState::ReleaseReservation;
        smallvec![effect]
    }

    fn handle_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            return self.emit_error(CompleteUploadError::InvalidOperationState);
        };
        if self.cleanup.release_id() != Some(id) {
            return self.emit_error(CompleteUploadError::InvalidOperationState);
        }
        self.cleanup.clear_release();
        if self.cleanup.error_pending() {
            self.emit_pending_error()
        } else {
            self.finish_commit()
        }
    }

    fn finish_commit(&mut self) -> Effects {
        self.state = CompleteUploadState::Finish;
        smallvec![schedule_snapshot_publish(), schedule_cleanup_effect()]
    }

    fn abort_finalize(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                self.continue_error_cleanup()
            }
            Event::Storage(StorageEvent::Error { error }) => self.abort_uncertain(&error),
            _ => self.emit_error(CompleteUploadError::InvalidOperationState),
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
        // An uncertain finalize commit may already have deleted the record, so
        // resetting it to Open could resurrect an upload the object replaced.
        self.reset_done = true;
        if self.reconcile_location.is_none() {
            let location = self
                .composed_location
                .take()
                .or_else(|| self.delete_location.take())
                .or_else(|| self.rollback_location.take());
            self.reconcile_location = location;
        }
        self.cleanup.clear_release();
    }

    fn emit_pending_error(&mut self) -> Effects {
        let error = match (self.cleanup.take_error(), self.output.take()) {
            (Some(error), _) | (None, Some(Err(error))) => error,
            _ => CompleteUploadError::CompleteUploadFailed,
        };
        self.emit_error(error)
    }

    fn validate_checksum_contract(
        &mut self,
        record: &MultipartUpload,
    ) -> Result<(), CompleteUploadError> {
        let Some(hint) = record.checksum_hint.as_ref() else {
            return Ok(());
        };

        if !self.input.checksum_type_explicit {
            self.input.checksum_type = hint.checksum_type;
        }
        if self.input.checksum_type_explicit && hint.checksum_type != self.input.checksum_type {
            return Err(CompleteUploadError::ChecksumContractMismatch);
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
                return Err(CompleteUploadError::ChecksumContractMismatch);
            }
        }

        Ok(())
    }

    fn alias_context(&self) -> Result<HeadAliasContext, CompleteUploadError> {
        let Some(upload_record) = self.upload_record.as_ref() else {
            return Err(CompleteUploadError::CompleteUploadFailed);
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
        self.state = CompleteUploadState::StartMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn mark_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CompleteUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = CompleteUploadState::CheckPurgeMark;
        smallvec![write_fence_read(&self.input.bucket, self.txn_id)]
    }

    fn mark_fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.input.bucket, &self.input.key) {
            return self.emit_error(error.into());
        }
        self.state = CompleteUploadState::ReadUploadMark;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn mark_upload_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CompleteUploadError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.emit_error(CompleteUploadError::NoSuchUpload);
        };
        let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        if let Err(err) = validate_upload(
            &record,
            &self.input.bucket,
            &self.input.key,
            StatusCheck::Takeover {
                now_ms: self.input.now_ms,
            },
        ) {
            return self.schedule_error(err.into());
        }
        if let Err(err) = self.validate_checksum_contract(&record) {
            return self.schedule_error(err);
        }

        if record.status == MultipartUploadStatus::Completing {
            warn!(
                upload_id = %self.input.upload_id,
                "Taking over a multipart completion whose lease expired"
            );
        }
        record.status = MultipartUploadStatus::Completing;
        record.completing_since_ms = Some(self.input.now_ms);
        let bytes = match record.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };
        self.upload_record = Some(record);
        self.state = CompleteUploadState::WriteUploadCompleting;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value: bytes.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_marked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CompleteUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CompleteUploadError::NoTransactionFound);
        };

        self.state = CompleteUploadState::CommitMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_mark_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = CompleteUploadState::ReadUploadParts;
                let prefix = match MultipartPartKey::prefix(self.input.upload_id) {
                    Ok(prefix) => prefix,
                    Err(err) => return self.schedule_error(err.into()),
                };
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: UPLOAD_PART_KEYSPACE.to_string(),
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
            _ => self.emit_error(CompleteUploadError::InvalidOperationState),
        }
    }

    fn extract_requested_parts(
        &self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Result<(Vec<MultipartPart>, Vec<MultipartPart>), CompleteUploadError> {
        if self.input.completed_parts.is_empty() {
            return Err(CompleteUploadError::MissingParts);
        }

        let mut all_parts = HashMap::new();
        let mut upload_parts = Vec::new();
        for (key, value) in values {
            let part_key = MultipartPartKey::from_bytes(key.as_ref())?;
            let part_record = MultipartPart::from_bytes(value.as_ref())?;
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
                return Err(CompleteUploadError::InvalidPartOrder);
            }
            previous = Some(requested.part_number);

            let Some(record) = all_parts.get(&requested.part_number).cloned() else {
                return Err(CompleteUploadError::InvalidPart);
            };
            // Compose stays same-backend: a part elsewhere means a routing bug.
            if let Some(upload) = self.upload_record.as_ref()
                && record.location.backend != upload.backend
            {
                return Err(CompleteUploadError::BackendMismatch);
            }
            validate_requested_part(requested, &record, required_checksum_algorithm)?;
            resolved.push(record);
        }

        if resolved
            .iter()
            .take(resolved.len().saturating_sub(1))
            .any(|part| part.location.blob_size < 5 * 1024 * 1024)
        {
            return Err(CompleteUploadError::EntityTooSmall);
        }

        if self.input.object_size.is_some_and(|size| {
            size != resolved
                .iter()
                .map(|part| part.location.blob_size)
                .sum::<u64>()
        }) {
            return Err(CompleteUploadError::InvalidObjectSize);
        }

        Ok((resolved, upload_parts))
    }

    fn upload_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
        self.state = CompleteUploadState::ReadGateBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
                .stored_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        let group_id = bucket
            .as_ref()
            .map(|bucket| bucket.group_id)
            .or_else(|| self.upload_record.as_ref().map(|upload| upload.group_id));
        match write_gate(self.gate_context.as_ref(), &refs, group_id) {
            Ok(None) => self.compose_blob(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = CompleteUploadState::PolicyGate;
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
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.schedule_error(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => self.compose_blob(),
            Err(error) => self.schedule_error(error.into()),
        }
    }

    fn compose_blob(&mut self) -> Effects {
        let Some(upload) = self.upload_record.as_ref() else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let pinned = ResolvedBackend::new(upload.backend.clone(), upload.storage_class.clone());
        let parts = self
            .resolved_parts
            .iter()
            .map(|part| part.location.clone())
            .collect();
        self.state = CompleteUploadState::ComposeBlob;
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
                self.cleanup.set_release(location.ulid);
                self.delete_location = Some(location);
                return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
            }
            _ => return self.schedule_error(CompleteUploadError::InvalidOperationState),
        };
        self.composed_location = Some(location.clone());
        self.final_location = None;

        let hashes = match self.input.checksum_type {
            MultipartChecksumType::FullObject => &location.hashes,
            MultipartChecksumType::Composite => &self.composite_hashes,
        };

        for expected in &self.input.expected_checksums {
            let Some(actual) = hashes.get(expected.algorithm.hash_key()) else {
                return self.schedule_error(CompleteUploadError::MissingExpectedChecksum(
                    expected.algorithm.s3_name(),
                ));
            };
            if actual != &expected.digest {
                return self.schedule_error(CompleteUploadError::ChecksumMismatch(
                    expected.algorithm.s3_name(),
                ));
            }
        }

        self.state = CompleteUploadState::StartFinalizeTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn finalize_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = CompleteUploadState::CheckPurgeFinalize;
        smallvec![write_fence_read(&self.input.bucket, self.txn_id)]
    }

    fn finalize_fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.input.bucket, &self.input.key) {
            return self.schedule_error(error.into());
        }
        // The version snapshots the default this transaction observes, not one
        // read while the parts were still being uploaded.
        self.state = CompleteUploadState::ReadBucketDefault;
        smallvec![drift_reads(&self.input.bucket, self.txn_id)]
    }

    fn handle_default_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        // The compose already ran on the pinned backend, so the finalize must
        // prove it is still enabled or roll the composed object back.
        match fence_backend(&location.backend, self.txn_id) {
            Some(effect) => {
                self.state = CompleteUploadState::FenceBackend;
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
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteUploadError::MissingExpectedChecksum("blake3"));
        };
        // Only the copy on the upload's pinned backend may be deduplicated.
        let key = match BlobLocationKey::from_blake3(blake3_hash, location.backend.clone()) {
            Ok(key) => key,
            Err(error) => return self.schedule_error(error.into()),
        };
        self.state = CompleteUploadState::CheckHashLookup;
        smallvec![blob_location_read(&key, self.txn_id)]
    }

    fn hash_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        let Some(composed_location) = self.composed_location.clone() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };

        self.final_location = match value {
            Some(value) => match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) => Some(location),
                Err(err) => {
                    return self.schedule_error(CompleteUploadError::ConversionError(err));
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
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteUploadError::MissingExpectedChecksum("blake3"));
        };
        let effect = match write_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self.schedule_error(CompleteUploadError::ConversionError(err.into()));
                }
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteUploadState::WriteBlobLocation;
        smallvec![effect]
    }

    fn location_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteUploadState::ReadObjectLookup;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn object_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
            self.state = CompleteUploadState::ReadLivenessVersion;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key,
                txn_id: self.txn_id,
            })];
        }
        self.write_current_lookup(existing_pointer.as_ref())
    }

    fn liveness_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
        let effect = match write_head_effect(&alias_context, pointer, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteUploadState::WriteBlobHead;
        smallvec![effect]
    }

    fn head_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        self.write_path_index()
    }

    fn write_path_index(&mut self) -> Effects {
        let Some(location) = self.final_location.clone() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteUploadError::MissingExpectedChecksum("blake3"));
        };
        let alias_context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.schedule_error(err),
        };
        let effect = match add_index_effect(
            &alias_context,
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self.schedule_error(CompleteUploadError::ConversionError(err.into()));
                }
            },
            match self.version_id {
                Some(version_id) => version_id,
                None => {
                    return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
                }
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteUploadState::WritePathIndex;
        smallvec![effect]
    }

    fn path_index_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        let Some(location) = self.final_location.clone() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(version_id) = self.version_id else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.schedule_error(CompleteUploadError::MissingExpectedChecksum("blake3"));
        };
        let created_at = self
            .version_created_at
            .get_or_insert_with(SystemTime::now)
            .to_owned();
        let Some(upload_record) = self.upload_record.as_ref() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let version = BlobVersion::materialized(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => {
                    return self.schedule_error(CompleteUploadError::ConversionError(err.into()));
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
        self.stored_policies = version.placement_policies.clone();
        let effect = match write_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };

        self.state = CompleteUploadState::WriteVersionRecord;
        smallvec![effect]
    }

    fn version_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        self.register_managed_copy()
    }

    /// Joins the finalize transaction, so the composed copy becomes serveable
    /// exactly when the logical version does and never before.
    fn register_managed_copy(&mut self) -> Effects {
        let (Some(version_id), Some(location)) = (self.version_id, self.final_location.clone())
        else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let effect = match register_effect(
            CopyRegistration {
                version: VersionKey::new(&self.input.bucket, &self.input.key, version_id),
                node_id: self.input.node_id,
                location: &location,
                policies: &self.stored_policies,
                origin: CopyOrigin::Write,
                subject_generation: self.stored_subject(),
                registered_at_ms: version_id.timestamp_ms(),
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.schedule_error(err.into()),
        };
        self.state = CompleteUploadState::RegisterManagedCopy;
        smallvec![effect]
    }

    fn handle_copy_registered(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };

        let Some(version_id) = self.version_id else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let mut writes = Vec::with_capacity(self.resolved_parts.len() + 1);

        let summary = MultipartObjectSummary {
            checksum_type: self.input.checksum_type,
            part_count: self.resolved_parts.len(),
            composite_hashes: self.composite_hashes.clone(),
        };
        let summary_key = match MultipartObjectKey::summary(version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.schedule_error(err.into()),
        };
        let summary_value = match summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.schedule_error(err.into()),
        };
        writes.push((
            OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for record in &self.resolved_parts {
            let object_part = MultipartObjectPart {
                part_number: record.part_number,
                size: record.location.blob_size,
                hashes: record.location.hashes.clone(),
            };
            let key = match MultipartObjectKey::part(version_id, record.part_number).to_bytes() {
                Ok(key) => key,
                Err(err) => return self.schedule_error(err.into()),
            };
            let value = match object_part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.schedule_error(err.into()),
            };
            writes.push((
                OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        self.state = CompleteUploadState::WriteObjectMetadata;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn metadata_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        self.delete_upload_records()
    }

    fn delete_upload_records(&mut self) -> Effects {
        let effect =
            match delete_records_effect(self.input.upload_id, &self.upload_parts, self.txn_id) {
                Ok(effect) => effect,
                Err(err) => return self.schedule_error(err.into()),
            };
        self.state = CompleteUploadState::DeleteUploadRecords;
        smallvec![effect]
    }

    fn records_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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

        self.state = CompleteUploadState::WriteCleanupRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_cleanup_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        self.write_obligation()
    }

    fn write_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let effect = match build_live_obligation(
            self.input.node_id,
            AuthContext {
                user_id: self.input.created_by,
                realm_id: self.input.realm_id,
                path_restrictions: self.restrictions.clone(),
                session: None,
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
        self.state = CompleteUploadState::WriteReplicationObligation;
        smallvec![effect]
    }

    fn obligation_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteUploadError::NoTransactionFound);
        };
        let Some(group_id) = self.upload_record.as_ref().map(|record| record.group_id) else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(size) = self
            .final_location
            .as_ref()
            .map(|location| i128::from(location.blob_size))
        else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
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
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
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
            self.state = CompleteUploadState::EnforceQuota;
            let effects = gate.start(txn_id);
            self.quota_gate = Some(gate);
            effects
        } else {
            self.start_usage_update(txn_id)
        }
    }

    fn start_usage_update(&mut self, txn_id: TxnId) -> Effects {
        self.state = CompleteUploadState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => self.schedule_error(CompleteUploadError::CompleteUploadFailed),
        }
    }

    fn handle_enforce_quota(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteUploadError::NoTransactionFound);
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                if gate.is_exceeded() {
                    let limit = gate.ceiling();
                    let usage = gate.projected_usage();
                    // schedule_error resets the upload back to Open and cleans up
                    // the composed blob, mirroring every other finalize-phase error.
                    self.schedule_error(CompleteUploadError::QuotaExceeded { limit, usage })
                } else {
                    self.start_usage_update(txn_id)
                }
            }
            Err(err) => self.schedule_error(err.into()),
        }
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(CompleteUploadError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.schedule_error(CompleteUploadError::CompleteUploadFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.state = CompleteUploadState::CommitFinalizeTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.schedule_error(err.into()),
        }
    }

    /// A commit with unknown outcome may already own the composed object, so only
    /// a proven refusal rolls it back; the rest moves to the reconciliation queue,
    /// out of reach of `abort`, where the committed location row decides its fate.
    fn handle_finalize_failure(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::Error { error }) = event else {
            return self.schedule_error(CompleteUploadError::InvalidOperationState);
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
        // The finalize transaction deleted the record, so nothing is left to reset.
        self.reset_done = true;
        let release_id = self.composed_location.take().map(|location| location.ulid);
        let Some(location) = self.final_location.clone() else {
            return self.emit_error(CompleteUploadError::CompleteUploadFailed);
        };
        let Some(version_id) = self.version_id else {
            return self.emit_error(CompleteUploadError::CompleteUploadFailed);
        };
        let response_hashes = match self.input.checksum_type {
            MultipartChecksumType::FullObject => location.hashes.clone(),
            MultipartChecksumType::Composite => self.composite_hashes.clone(),
        };
        self.output = Some(Ok(CompleteUploadResult {
            location,
            version_id,
            checksum_type: self.input.checksum_type,
            response_hashes,
            part_count: self.resolved_parts.len(),
        }));
        if let Some(id) = release_id {
            self.cleanup.set_release(id);
            self.state = CompleteUploadState::ReleaseReservation;
            smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
        } else {
            self.finish_commit()
        }
    }

    fn reset_started(&mut self, event: Event) -> Effects {
        let txn_id = match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self.reset_failed(Some(CompleteUploadError::InvalidOperationState));
            }
        };
        self.txn_id = Some(txn_id);
        self.state = CompleteUploadState::ReadUploadReset;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn reset_upload_read(&mut self, event: Event) -> Effects {
        let value = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self.reset_failed(Some(CompleteUploadError::InvalidOperationState));
            }
        };
        if let Some(value) = value {
            let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(err) => return self.reset_failed(Some(err.into())),
            };
            // Another attempt already took the lease over; its completion owns
            // the record now and must not be reopened underneath it.
            if record.completing_since_ms != Some(self.input.now_ms) {
                return self.reset_failed(None);
            }
            record.status = MultipartUploadStatus::Open;
            record.completing_since_ms = None;
            self.upload_record = Some(record.clone());
            let bytes = match record.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.reset_failed(Some(err.into())),
            };
            self.state = CompleteUploadState::WriteUploadReset;
            return smallvec![Effect::Storage(StorageEffect::Write {
                key_space: UPLOAD_KEYSPACE.to_string(),
                key: self.input.upload_id.to_bytes().to_vec().into(),
                value: bytes.into(),
                txn_id: self.txn_id,
            })];
        }

        self.reset_failed(None)
    }

    fn upload_reset(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { .. }) => return self.reset_failed(None),
            _ => {
                return self.reset_failed(Some(CompleteUploadError::InvalidOperationState));
            }
        };
        let Some(txn_id) = self.txn_id else {
            return self.reset_failed(Some(CompleteUploadError::NoTransactionFound));
        };
        self.state = CompleteUploadState::CommitResetTransaction;
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
                return self.reset_failed(Some(CompleteUploadError::InvalidOperationState));
            }
        };
        self.txn_id = None;
        self.rollback_composed_blob()
    }

    fn reset_failed(&mut self, error: Option<CompleteUploadError>) -> Effects {
        if let Some(error) = error {
            self.cleanup.set_error(error);
        }
        self.state = CompleteUploadState::CleanupFailedCompose;
        if let Some(txn_id) = self.txn_id.take() {
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.rollback_composed_blob()
    }

    fn compose_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                self.continue_error_cleanup()
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
            _ => self.emit_error(CompleteUploadError::InvalidOperationState),
        }
    }

    fn has_cleanup(&self) -> bool {
        self.reconcile_location.is_some()
            || self.delete_location.is_some()
            || self.rollback_location.is_some()
            || self.composed_location.is_some()
    }
}

impl Operation for CompleteUploadOperation {
    type Output = CompleteUploadResult;
    type Error = CompleteUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CompleteUploadState::Init => self.handle_init(),
            CompleteUploadState::StartMarkTransaction => self.mark_started(event),
            CompleteUploadState::CheckPurgeMark => self.mark_fence_checked(event),
            CompleteUploadState::ReadUploadMark => self.mark_upload_read(event),
            CompleteUploadState::WriteUploadCompleting => self.handle_upload_marked(event),
            CompleteUploadState::CommitMarkTransaction => self.handle_mark_committed(event),
            CompleteUploadState::ReadUploadParts => self.upload_parts_read(event),
            CompleteUploadState::ReadGateBucket => self.handle_gate_bucket(event),
            CompleteUploadState::PolicyGate => self.handle_policy_gate(event),
            CompleteUploadState::ComposeBlob => self.handle_blob_composed(event),
            CompleteUploadState::StartFinalizeTransaction => self.finalize_started(event),
            CompleteUploadState::CheckPurgeFinalize => self.finalize_fence_checked(event),
            CompleteUploadState::ReadBucketDefault => self.handle_default_read(event),
            CompleteUploadState::FenceBackend => self.handle_backend_fenced(event),
            CompleteUploadState::CheckHashLookup => self.hash_checked(event),
            CompleteUploadState::WriteBlobLocation => self.location_written(event),
            CompleteUploadState::ReadObjectLookup => self.object_lookup_read(event),
            CompleteUploadState::ReadLivenessVersion => self.liveness_read(event),
            CompleteUploadState::WriteBlobHead => self.head_written(event),
            CompleteUploadState::WritePathIndex => self.path_index_written(event),
            CompleteUploadState::WriteVersionRecord => self.version_written(event),
            CompleteUploadState::RegisterManagedCopy => self.handle_copy_registered(event),
            CompleteUploadState::WriteObjectMetadata => self.metadata_written(event),
            CompleteUploadState::DeleteUploadRecords => self.records_deleted(event),
            CompleteUploadState::WriteCleanupRecords => self.handle_cleanup_written(event),
            CompleteUploadState::WriteReplicationObligation => self.obligation_written(event),
            CompleteUploadState::EnforceQuota => self.handle_enforce_quota(event),
            CompleteUploadState::UpdateUsage => self.handle_usage_update(event),
            CompleteUploadState::CommitFinalizeTransaction => self.handle_finalize_committed(event),
            CompleteUploadState::AbortFinalizeTransaction => self.abort_finalize(event),
            CompleteUploadState::ResetUploadTransaction => self.reset_started(event),
            CompleteUploadState::ReadUploadReset => self.reset_upload_read(event),
            CompleteUploadState::WriteUploadReset => self.upload_reset(event),
            CompleteUploadState::CommitResetTransaction => self.handle_reset_committed(event),
            CompleteUploadState::CleanupFailedCompose => self.compose_cleanup(event),
            CompleteUploadState::QueueCleanupRow => self.handle_cleanup_queued(event),
            CompleteUploadState::ReleaseReservation => self.handle_release(event),
            CompleteUploadState::Finish => smallvec![],
            CompleteUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CompleteUploadState::Finish | CompleteUploadState::Error
        )
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            CompleteUploadError::NoSuchUpload
                | CompleteUploadError::UploadTargetMismatch
                | CompleteUploadError::UploadNotOpen
                | CompleteUploadError::CompletionInProgress
        )
    }

    /// The mark transaction commits long before the finalize one, so a deadline
    /// must still reopen the record it left `Completing`.
    fn abort_after_commit(&self) -> bool {
        self.needs_reset()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(CompleteUploadError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        if self.cleanup_closed {
            return smallvec![];
        }
        if let Some(txn_id) = self.txn_id.take() {
            if matches!(
                self.state,
                CompleteUploadState::CommitFinalizeTransaction
                    | CompleteUploadState::CommitResetTransaction
            ) {
                self.preserve_blob();
            }
            if self.state != CompleteUploadState::Error
                || self.has_cleanup()
                || self.cleanup.release_id().is_some()
            {
                self.state = CompleteUploadState::CleanupFailedCompose;
            }
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        if let Some(effect) = self.cleanup.retry(&StorageError::Timeout) {
            self.state = CompleteUploadState::QueueCleanupRow;
            return smallvec![effect];
        }
        // A deadline that fires between the mark and the finalize leaves the
        // record `Completing`; reopening it keeps the upload retryable.
        if self.needs_reset() || self.has_cleanup() {
            return self.continue_error_cleanup();
        }
        if let Some(effect) = self.cleanup.release_effect() {
            self.state = CompleteUploadState::ReleaseReservation;
            return smallvec![effect];
        }
        self.state = CompleteUploadState::Error;
        if !matches!(self.output.as_ref(), Some(Err(_))) {
            self.output = Some(Err(CompleteUploadError::CompleteUploadFailed));
        }
        smallvec![]
    }
}

fn validate_requested_part(
    requested: &CompleteMultipartPart,
    record: &MultipartPart,
    required_checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Result<(), CompleteUploadError> {
    if let Some(etag) = &requested.etag {
        let Some(md5) = record.location.hashes.get(HASH_MD5) else {
            return Err(CompleteUploadError::MissingPartEtag);
        };
        if hex::encode(md5) != *etag {
            return Err(CompleteUploadError::PartEtagMismatch);
        }
    }

    if let Some(algorithm) = required_checksum_algorithm
        && !requested
            .expected_checksums
            .iter()
            .any(|expected| expected.algorithm == algorithm)
    {
        return Err(CompleteUploadError::ChecksumContractMismatch);
    }

    for expected in &requested.expected_checksums {
        let Some(actual) = record.location.hashes.get(expected.algorithm.hash_key()) else {
            return Err(CompleteUploadError::MissingExpectedChecksum(
                expected.algorithm.s3_name(),
            ));
        };
        if actual != &expected.digest {
            return Err(CompleteUploadError::ChecksumMismatch(
                expected.algorithm.s3_name(),
            ));
        }
    }

    Ok(())
}

fn compute_composite_hashes(
    parts: &[MultipartPart],
) -> Result<HashMap<String, Vec<u8>>, CompleteUploadError> {
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
                return Err(CompleteUploadError::MissingExpectedChecksum(
                    algorithm.s3_name(),
                ));
            };
            combined.extend_from_slice(digest);
        }

        let digest = composite_digest(algorithm, &combined);
        hashes.insert(algorithm.hash_key().to_string(), digest);
    }
    Ok(hashes)
}

fn composite_digest(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> Vec<u8> {
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
#[path = "complete_tests.rs"]
mod pure_tests;

#[cfg(test)]
mod decision_tests {
    use super::pure_tests::TEST_NOW_MS;
    use super::*;
    use crate::placement::policy::PolicyCacheEntry;
    use aruna_core::structs::placement::policy::{
        PlacementPolicy, PlacementSelector, PlacementSubject, VerifiedPolicy,
    };
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::multipart::MultipartChecksumHint;
    use aruna_core::types::Value;
    use std::collections::BTreeMap;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    fn node() -> aruna_core::id::NodeId {
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
            admitting: true,
        }
    }

    fn input() -> CompleteUploadInput {
        let realm_id = realm();
        CompleteUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::from_parts(1, 1),
            realm_id,
            node_id: node(),
            completed_parts: vec![],
            expected_checksums: vec![],
            checksum_algorithm: None,
            checksum_type: MultipartChecksumType::FullObject,
            checksum_type_explicit: false,
            object_size: Some(10),
            created_by: UserId::local(Ulid::from_parts(2, 2), realm_id),
            quota_ceiling: Some(30),
            now_ms: TEST_NOW_MS,
        }
    }

    fn upload(input: &CompleteUploadInput) -> MultipartUpload {
        MultipartUpload {
            upload_id: input.upload_id,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            group_id: Ulid::from_parts(3, 3),
            created_by: input.created_by,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None::<MultipartChecksumHint>,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
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
    fn at_gate(location: Option<&str>) -> CompleteUploadOperation {
        let input = input();
        let record = upload(&input);
        let mut operation = CompleteUploadOperation::new(input);
        if let Some(location) = location {
            operation = operation.with_gate(gate(location));
        }
        operation.upload_record = Some(record);
        operation.state = CompleteUploadState::ReadGateBucket;
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

        let document = crate::tests::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(cached.into())));
        let effects = operation.step(crate::tests::policy::authority(realm()));

        assert!(!composes(&effects));
        assert_eq!(
            operation.cleanup.take_error(),
            Some(CompleteUploadError::PolicyGate(PolicyGateError::Denied {
                policy_ids: vec![rule.policy().policy_id]
            }))
        );
    }

    #[test]
    fn missing_policy_blocks() {
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
            operation.cleanup.take_error(),
            Some(CompleteUploadError::PolicyGate(
                PolicyGateError::Unavailable { .. }
            ))
        ));
    }

    #[test]
    fn missing_subject_blocks() {
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
            operation.cleanup.take_error(),
            Some(CompleteUploadError::PolicyGate(PolicyGateError::NoSubject))
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
        operation.state = CompleteUploadState::ReadBucketDefault;

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
            operation.cleanup.take_error(),
            Some(CompleteUploadError::PolicyGate(PolicyGateError::Drift))
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
