//! Completes a multipart upload by composing its parts into one blob version and committing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup_rows::schedule_cleanup_effect;
use crate::blob::records::HeadAliasContext;
use crate::distribution::guard::{GuardError, WriteAuthority, authority_reads, record_completion};
use crate::distribution::inventory::{
    AddedVersion, InventoryChange, InventoryUpdate, stamp_head, stamp_version, version_written,
};
use crate::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, ReadPolicyError, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::s3::multipart::create::storage_current;
use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::object::put::abe::{abe_reads, keyed, located_rows, parse_abe};
use crate::s3::purge_fence::{
    PurgeFenceError, check_fence_row, check_published, fence_key, publication_rows,
};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish, upload_count,
};
use crate::s3::write_cleanup::{RollbackWrite, WriteCleanup, record_rows};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ABE_PENDING_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, COPY_OWNER_KEYSPACE, NODE_SUBJECT_KEYSPACE,
    OBJECT_METADATA_KEYSPACE, PATHS_INDEX_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE,
    UPLOAD_PART_KEYSPACE,
};
use aruna_core::managed_copy::{CopyRegistration, ManagedCopyError, register_entry};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum, HASH_MD5};
use aruna_core::structs::checksum::{Hasher, combine_crcs};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::table::searchable;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::abe::{AbeError, ObjectEnvelope};
use aruna_core::structs::storage::backend_import::PlainPath;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobCleanupWork, BlobHeadKey, BlobLocationKey,
    BlobVersion, BucketInfo, CopyOrigin, CopyOwner, CurrentVersionPointer, ResolvedBackend,
    VersionKey, WriteOwner,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};
use aruna_core::structs::storage::distribution::inventory::VersionEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, SealPlan};
use aruna_core::structs::storage::format::{Compression, EncodingClass};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::multipart::{
    MAX_PART_SIZE, MultipartChecksumType, MultipartObjectKey, MultipartObjectPart,
    MultipartObjectSummary, MultipartPart, MultipartPartKey, MultipartUpload,
    MultipartUploadStatus, WorkingShare,
};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::replication_queue::{
    LiveObligationRecord, live_obligation_entry,
};
use aruna_core::structs::storage::routing::{PlainTarget, RoutingSnapshot};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Part rows read per page.
const PART_PAGE: usize = 256;

#[derive(Debug, Eq, PartialEq)]
pub enum CompleteUploadState {
    Init,
    /// Marks the upload completing in its own transaction.
    MarkUpload,
    /// Reads which key the rule recorded at the path of a same-name object.
    ReadPlainOwner,
    /// Looks at the path of a same-name object before it is composed.
    StatPlain,
    ReserveCompose,
    ReadUploadParts,
    ReadGateBucket,
    /// Reads a distributed bucket's record: only the key's coordinator completes, at its epoch.
    ReadRecord,
    PolicyGate,
    ComposeBlob,
    /// Records what a plain object will be before its bytes reach the path.
    WriteIntent,
    /// Syncs that record to disk before the compose.
    SyncIntent,
    /// Deletes the record of a compose the provider refused before any byte moved.
    DropIntent,
    /// Syncs that delete to disk.
    SyncDroppedIntent,
    /// Records the hash of the composed bytes in the write intent.
    WriteEvidence,
    /// Publishes the composed object in the finalize transaction, run as a child.
    Finalize,
    CheckPurgeFinalize,
    ReadBucketDefault,
    CheckSealSettings,
    FenceAbe,
    FenceBackend,
    CheckHashLookup,
    /// Reads the current version and its file, which a re-upload of the same bytes reads too.
    ReadDedupHead,
    ReadDedupVersion,
    ReadDedupLocation,
    ReadDedupProvider,
    ReadDedupClaim,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WritePathIndex,
    WriteVersionRecord,
    WriteEnvelope,
    WriteCopyOwner,
    RegisterManagedCopy,
    WriteObjectMetadata,
    /// Reads the record in the finalize transaction, so only the delete that finds it counts.
    ReadUploadRecord,
    CountUpload,
    DeleteUploadRecords,
    /// Reads the import rule a plain object's import row belongs to.
    ReadImportRule,
    /// Reads which rules read the composed path, inside the finalize transaction.
    ReadReaders,
    /// Deletes the intent record in the version transaction.
    RetireIntent,
    /// Reads the version an import may have registered already.
    ReadRegistered,
    /// Reads the delete trail of an upload no import registered, before its head moves.
    ReadTrail,
    /// Drops the location the upload staged for bytes an import registered already.
    DropStagedLocation,
    WriteCleanupRecords,
    WriteReplicationObligation,
    EnforceQuota,
    UpdateUsage,
    /// Closes the upload in the inventory of a distributed bucket before a registered commit.
    CloseInventory,
    /// The finalize failed; only the cleanup in `abort` remains.
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CompleteUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("{}", aruna_core::structs::storage::routing::UNMAPPED_KEY)]
    Unmapped,
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error(transparent)]
    BlobError(#[from] BlobError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("stored part does not live on the upload's pinned backend")]
    BackendMismatch,
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
    #[error("the version already exists with other content")]
    VersionExists,
    #[error("the version was deleted on purpose")]
    HistoryDeleted,
    #[error("the import rule of the folder was removed")]
    ImportRemoved,
    #[error(transparent)]
    BucketKey(#[from] BucketKeyError),
    #[error("usage counters are being rebuilt; retry later")]
    UsageRebuilding,
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

/// A refused call fails as its storage error; a lost conflict as the transaction conflict.
impl From<IoError> for CompleteUploadError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<CompleteUploadResult>, CompleteUploadError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteMultipartPart {
    pub part_number: u16,
    pub etag: Option<String>,
    pub expected_checksums: Vec<ExpectedChecksum>,
}

#[derive(Clone, Debug, PartialEq)]
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

/// Runs outside any transaction: the mark, the finalize and the reopen of the record each
/// commit as their own child. The finalize runs as a second instance of this operation with
/// `finalizing` set, in one write transaction.
#[derive(Debug, PartialEq)]
pub struct CompleteUploadOperation {
    state: CompleteUploadState,
    input: CompleteUploadInput,
    /// This instance is the finalize child.
    finalizing: bool,
    upload_record: Option<MultipartUpload>,
    /// The mark may have committed, so a failure reopens the record.
    marked: bool,
    upload_parts: Vec<MultipartPart>,
    resolved_parts: Vec<MultipartPart>,
    composed_location: Option<BackendLocation>,
    /// The composed object after a commit whose outcome is unknown. Held apart
    /// from `composed_location` so `abort` cannot delete bytes a commit owns.
    reconcile_location: Option<BackendLocation>,
    /// A pre-finalize write has no committed owner, even when its hash is absent.
    delete_location: Option<BackendLocation>,
    /// The composed object a refused finalize deletes.
    rollback: Option<BackendLocation>,
    cleanup: WriteCleanup<CompleteUploadError>,
    final_location: Option<BackendLocation>,
    composite_hashes: HashMap<String, Vec<u8>>,
    version_id: Option<Ulid>,
    version_created_at: Option<SystemTime>,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    was_live: bool,
    /// Offered directory bytes the replaced head stops charging.
    released_bytes: u64,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    rocrate_limits: RoCrateLimits,
    restrictions: Option<Vec<PathRestriction>>,
    /// Refs stored on the version record, reused verbatim by its registration.
    stored_policies: Vec<PlacementPolicyRef>,
    /// Destination default, read inside the finalize transaction.
    bucket_policies: Vec<PlacementPolicyRef>,
    /// Destination details of this node. Absent means no governed object may be
    /// composed or registered here.
    gate_context: Option<GateContext>,
    /// What the gate decided on, re-read inside the finalize transaction.
    gated_bucket: Option<GatedBucket>,
    /// The bucket's compression when the gate read it; the composed object uses it.
    compression: Compression,
    /// The set hint of a distributed bucket; ordinary buckets mint plain ULIDs.
    set_hint: Option<SetHint>,
    /// The bucket's distribution binding, whose inventory the finalize transaction maintains.
    binding: Option<DistributionBinding>,
    /// The head the finalize installs and the epoch of its version, for the inventory.
    head_pointer: Option<CurrentVersionPointer>,
    version_epoch: OriginalEpoch,
    /// The coordinator authority the completion was routed under; finalize rechecks it.
    authority: Option<WriteAuthority>,
    /// The close-only inventory update of a completion that adopts a registered version.
    close: Option<InventoryUpdate>,
    /// The reopen of the record is already planned or done.
    reset_done: bool,
    /// Working set of a sealed composition, reserved before the piece records load. It stays
    /// until the operation drops the selected piece records.
    compose_share: Option<WorkingShare>,
    /// Selected parts with their piece records, gathered while the part rows are paged.
    selected_parts: HashMap<u16, MultipartPart>,
    selected_bytes: u64,
    /// The pending envelope that passed the fence, published with the version.
    envelope: Option<ObjectEnvelope>,
    envelope_bytes: u64,
    /// A stale encrypted upload loses its pending envelope when its record is reopened.
    reclaim_pending: bool,
    /// A refused publication rechecks its pending envelope in the reopen.
    recheck_pending: bool,
    routing: Option<RoutingSnapshot>,
    plain: Option<PlainTarget>,
    /// The path a same-name object looks at, and whether the file there may be replaced.
    plain_stat_at: Option<(BackendRef, String)>,
    plain_owned: bool,
    /// The path composed last and the backend version the compose reported.
    plain_path: Option<PlainPath>,
    plain_version: Option<String>,
    plain_etag: Option<String>,
    /// The compose that waits for its intent record.
    pending_compose: Option<IoCall>,
    /// An import registered this version with the same bytes before the finalize.
    registered: bool,
    /// The assembled linked file the version gives up for the current version's identical one.
    dedup: Option<BackendLocation>,
    dedup_checked: bool,
    /// The delete trail read for a listed upload's version before its head moves.
    trail_scan: Option<crate::s3::object::put::TrailScan>,
    /// The intent record of the compose in flight.
    written_intent: Vec<Key>,
    /// The rules that read the composed path, as the finalize transaction reads them.
    readers: crate::s3::object::put::ReaderScan,
    version_origin: Option<VersionOrigin>,
}

impl CompleteUploadOperation {
    pub fn new(input: CompleteUploadInput) -> Self {
        Self {
            state: CompleteUploadState::Init,
            input,
            finalizing: false,
            upload_record: None,
            marked: false,
            upload_parts: Vec::new(),
            resolved_parts: Vec::new(),
            composed_location: None,
            reconcile_location: None,
            delete_location: None,
            rollback: None,
            cleanup: WriteCleanup::default(),
            final_location: None,
            composite_hashes: HashMap::new(),
            version_id: None,
            version_created_at: None,
            existing_pointer: None,
            new_blob: false,
            was_live: false,
            released_bytes: 0,
            usage_update: None,
            quota_gate: None,
            rocrate_limits: RoCrateLimits::default(),
            restrictions: None,
            stored_policies: Vec::new(),
            bucket_policies: Vec::new(),
            gate_context: None,
            gated_bucket: None,
            compression: Compression::Off,
            set_hint: None,
            binding: None,
            head_pointer: None,
            version_epoch: OriginalEpoch::default(),
            authority: None,
            close: None,
            reset_done: false,
            compose_share: None,
            selected_parts: HashMap::new(),
            selected_bytes: 0,
            envelope: None,
            envelope_bytes: 0,
            reclaim_pending: false,
            recheck_pending: false,
            routing: None,
            plain: None,
            plain_stat_at: None,
            plain_owned: false,
            plain_path: None,
            plain_version: None,
            plain_etag: None,
            pending_compose: None,
            registered: false,
            dedup: None,
            dedup_checked: false,
            trail_scan: None,
            written_intent: Vec::new(),
            readers: Default::default(),
            version_origin: None,
        }
    }

    /// The origin the completed version records; an upload without a client by default.
    pub fn with_version_origin(mut self, origin: VersionOrigin) -> Self {
        self.version_origin = Some(origin);
        self
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

    fn call(&mut self, state: CompleteUploadState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn effect(&mut self, state: CompleteUploadState, effect: Effect) -> Turn {
        self.call(state, effect.into())
    }

    /// Subject generation the gate admitted this completion under; zero for an
    /// ungoverned object, which no subject ever evaluated.
    fn stored_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    fn sealed(&self) -> bool {
        (self.upload_record.as_ref()).is_some_and(|upload| upload.encryption.is_some())
    }

    fn plan(&self) -> Option<SealPlan> {
        let upload = self.upload_record.as_ref()?;
        upload.encryption.map(|encryption| encryption.plan)
    }

    fn in_place_target(&self, location: &BackendLocation) -> bool {
        self.upload_record
            .as_ref()
            .and_then(|upload| upload.backend_upload.as_ref())
            .is_some_and(|upload| upload.location.same_object(location))
    }

    /// The cleanup row that reconciles a composed object whose commit outcome is unknown.
    fn reconcile_work(&self, location: BackendLocation) -> BlobCleanupWork {
        let Some(blake3) = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok())
        else {
            return BlobCleanupWork::ReconcileReservation { location };
        };
        let (realm_id, ttl_ms) = (self.input.realm_id, self.rocrate_limits.holder_ttl_ms);
        // Distributed buckets are found through their content indexes, not the DHT.
        let announce = self.binding.is_none();
        // An in-place object is the only copy of its parts: a commit that did not land must
        // leave it to the upload, one that did to its version.
        let owner = match self.in_place_target(&location) {
            true => WriteOwner::CompletedUpload {
                upload_id: self.input.upload_id,
                blake3,
                realm_id,
                ttl_ms,
                announce,
            },
            false => WriteOwner::Blob {
                blake3,
                realm_id,
                ttl_ms,
                announce,
            },
        };
        BlobCleanupWork::ReconcileWrite { location, owner }
    }

    /// The rows that roll the composed object back. An in-place object is the only copy of its
    /// parts, so a retry completes from it; any other is deleted by a rollback child.
    fn rollback_works(&mut self) -> Vec<BlobCleanupWork> {
        let mut works = Vec::new();
        if let Some(location) = self.reconcile_location.take() {
            works.push(self.reconcile_work(location));
        }
        if let Some(location) = self.delete_location.take() {
            works.push(BlobCleanupWork::DeleteBlob { location });
        }
        match self.composed_location.take() {
            Some(location) if self.in_place_target(&location) => {
                let upload_id = self.input.upload_id;
                works.push(BlobCleanupWork::ReconcileWrite {
                    location,
                    owner: WriteOwner::Upload { upload_id },
                });
            }
            Some(location) => self.rollback = Some(location),
            None => {}
        }
        works
    }

    /// Keeps the composed object for reconciliation after a finalize whose commit may have
    /// applied: its record may already be gone, so it is not reopened either.
    fn preserve_blob(&mut self) {
        self.reset_done = true;
        if self.reconcile_location.is_none() {
            let location = self
                .composed_location
                .take()
                .or_else(|| self.delete_location.take());
            self.reconcile_location = location;
        }
        self.cleanup.clear_release();
    }

    fn validate_checksum_contract(
        input: &mut CompleteUploadInput,
        record: &MultipartUpload,
    ) -> Result<(), CompleteUploadError> {
        let Some(hint) = record.checksum_hint.as_ref() else {
            return Ok(());
        };

        if !input.checksum_type_explicit {
            input.checksum_type = hint.checksum_type;
        }
        if input.checksum_type_explicit && hint.checksum_type != input.checksum_type {
            return Err(CompleteUploadError::ChecksumContractMismatch);
        }

        if let Some(algorithm) = hint.algorithm
            && (input.checksum_algorithm.is_some() || !input.expected_checksums.is_empty())
        {
            let matching_expected = input
                .expected_checksums
                .iter()
                .any(|checksum| checksum.algorithm == algorithm);
            if input.checksum_algorithm != Some(algorithm) || !matching_expected {
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

    fn mark_upload(&mut self) -> Turn {
        let mark = MarkCompleting {
            input: self.input.clone(),
            record: None,
        };
        self.call(CompleteUploadState::MarkUpload, IoCall::child(mark))
    }

    fn upload_marked(&mut self, result: IoResult) -> Turn {
        let (record, checksum_type) = match result.child::<Marked, CompleteUploadError>()? {
            Ok(marked) => marked,
            // A commit that may have applied leaves the record completing: reopen it.
            Err(CompleteUploadError::StorageError(error)) if !error.proves_no_commit() => {
                self.marked = true;
                return Err(error.into());
            }
            Err(error) => return Err(error),
        };
        self.marked = true;
        self.input.checksum_type = checksum_type;
        let sealed = record.encryption.is_some();
        self.upload_record = Some(record);
        if !sealed {
            return self.read_parts(None);
        }
        // Piece records are large, so the composition's share comes before they load.
        let parts = self.input.completed_parts.len() as u64;
        let reserve = BlobEffect::ReserveCompose {
            content: parts.saturating_mul(MAX_PART_SIZE),
        };
        self.effect(CompleteUploadState::ReserveCompose, Effect::Blob(reserve))
    }

    fn compose_reserved(&mut self, event: Event) -> Turn {
        match event {
            Event::Blob(BlobEvent::ComposeReserved { share }) => {
                self.compose_share = Some(share);
                self.read_parts(None)
            }
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            _ => Err(CompleteUploadError::InvalidOperationState),
        }
    }

    /// Reads one page of part rows; only the selected parts keep their piece records.
    fn read_parts(&mut self, after: Option<Key>) -> Turn {
        let prefix = MultipartPartKey::prefix(self.input.upload_id)?;
        let page = IoCall::Page {
            space: UPLOAD_PART_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after,
            limit: PART_PAGE,
        };
        self.call(CompleteUploadState::ReadUploadParts, page)
    }

    /// Keeps every part as cleanup metadata without its piece record, and the selected parts
    /// in full.
    fn collect_parts(&mut self, values: Vec<(Key, Value)>) -> Result<(), CompleteUploadError> {
        let requested: std::collections::HashSet<u16> = (self.input.completed_parts.iter())
            .map(|part| part.part_number)
            .collect();
        for (key, value) in values {
            let part_key = MultipartPartKey::from_bytes(key.as_ref())?;
            let mut part_record = MultipartPart::from_bytes(value.as_ref())?;
            if requested.contains(&part_key.part_number) {
                if self.compose_share.is_some() {
                    let limit = aruna_blob::blob::pithos::MAX_SIZE;
                    self.selected_bytes = self
                        .selected_bytes
                        .checked_add(part_record.location.blob_size)
                        .filter(|size| *size <= limit)
                        .ok_or(BlobError::SizeLimitExceeded { limit })?;
                }
                self.selected_parts
                    .insert(part_key.part_number, part_record.clone());
            }
            part_record.piece = None;
            self.upload_parts.push(part_record);
        }
        Ok(())
    }

    fn extract_requested_parts(
        &mut self,
        values: Vec<(Key, Value)>,
    ) -> Result<(Vec<MultipartPart>, Vec<MultipartPart>), CompleteUploadError> {
        if self.input.completed_parts.is_empty() {
            return Err(CompleteUploadError::MissingParts);
        }
        self.collect_parts(values)?;
        let mut all_parts = std::mem::take(&mut self.selected_parts);
        let upload_parts = std::mem::take(&mut self.upload_parts);

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

            let Some(record) = all_parts.remove(&requested.part_number) else {
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

    fn upload_parts_read(&mut self, result: IoResult) -> Turn {
        let (values, next) = result.page()?;
        if let Some(next) = next.filter(|_| values.len() == PART_PAGE) {
            self.collect_parts(values)?;
            return self.read_parts(Some(next));
        }

        let (resolved, upload_parts) = self.extract_requested_parts(values)?;
        self.composite_hashes = compute_composite_hashes(&resolved)?;
        self.upload_parts = upload_parts;
        self.resolved_parts = resolved;

        // The destination default is read before the compose, so the gate that
        // admits the object sees the refs the version would actually carry.
        let bucket = read_row(S3_BUCKET_KEYSPACE, self.input.bucket.as_bytes().to_vec());
        self.call(CompleteUploadState::ReadGateBucket, bucket)
    }

    fn handle_gate_bucket(&mut self, value: Option<Value>) -> Turn {
        let bucket = (value.as_ref())
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        self.compression = (bucket.as_ref())
            .map(|bucket| bucket.compression)
            .unwrap_or_default();
        self.binding = (bucket.as_ref()).and_then(|bucket| bucket.distribution.clone());
        self.set_hint = self.binding.as_ref().map(DistributionBinding::hint);
        let inherited = (self.upload_record.as_ref())
            .map(|upload| upload.placement_policies.clone())
            .unwrap_or_default();
        let refs = union_refs(&GatedBucket::observe(bucket.as_ref()).policies, &inherited)?;
        self.gated_bucket = Some(
            GatedBucket::observe(bucket.as_ref())
                .stored_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        let group_id = (bucket.as_ref())
            .map(|bucket| bucket.group_id)
            .or_else(|| self.upload_record.as_ref().map(|upload| upload.group_id));
        match write_gate(self.gate_context.as_ref(), &refs, group_id)? {
            None => self.compose_blob(),
            Some(gate) => self.call(CompleteUploadState::PolicyGate, IoCall::child(gate)),
        }
    }

    fn gate_passed(&mut self, result: IoResult) -> Turn {
        let outcome = result.child::<PolicyGateOutcome, ReadPolicyError>()?;
        let outcome = outcome.map_err(PolicyGateError::from)?;
        gate_decision(outcome)?;
        self.compose_blob()
    }

    fn compose_blob(&mut self) -> Turn {
        match self.binding.as_ref() {
            Some(binding) => {
                let reads = IoCall::Read(authority_reads(binding.set_id));
                self.call(CompleteUploadState::ReadRecord, reads)
            }
            None => self.compose_upload(),
        }
    }

    fn record_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let binding = self.binding.as_ref();
        let binding = binding.ok_or(CompleteUploadError::InvalidOperationState)?;
        // An upload opened before its range was fenced may still complete during the grace.
        let answer = Event::Storage(StorageEvent::BatchReadResult {
            values: keyed(&values),
        });
        let authority = record_completion(binding, &self.input.key, answer)?;
        self.version_epoch = authority.epoch;
        self.authority = Some(authority);
        self.compose_upload()
    }

    fn compose_upload(&mut self) -> Turn {
        let upload = self.upload_record.as_ref();
        let upload = upload.ok_or(CompleteUploadError::InvalidOperationState)?;
        if let Some(encryption) = upload.encryption {
            // Sealed pieces compose without a key; the adapter checks every record.
            if self.resolved_parts.iter().any(|part| part.piece.is_none()) {
                return Err(CompleteUploadError::InvalidPart);
            }
            let resolved =
                ResolvedBackend::new(upload.backend.clone(), upload.storage_class.clone())
                    .with_compression(encryption.compression)
                    .with_encryption(Some(encryption.plan));
            // The operation keeps its clone while it holds the piece records.
            let share = self.compose_share.clone();
            let share = share.ok_or(CompleteUploadError::InvalidOperationState)?;
            let compose = BlobEffect::ComposePieces {
                bucket: self.input.bucket.clone(),
                key: self.input.key.clone(),
                resolved,
                created_by: self.input.created_by,
                parts: self.resolved_parts.clone(),
                share,
            };
            return self.compose(Effect::Blob(compose).into());
        }
        if let Some(turn) = self.plain_compose() {
            return turn;
        }
        let bucket = &self.input.bucket;
        if (self.routing.as_ref()).is_some_and(|routing| routing.mapped(bucket)) {
            return Err(CompleteUploadError::Unmapped);
        }
        if let Some(call) = self.complete_provider(false) {
            return self.compose(call);
        }
        let parts = (self.resolved_parts.iter())
            .map(|part| part.location.clone())
            .collect();
        let call = self.compose_parts(parts)?;
        self.compose(call)
    }

    fn compose(&mut self, call: IoCall) -> Turn {
        self.call(CompleteUploadState::ComposeBlob, call)
    }

    /// Completes the provider upload the parts streamed into, where the record has one.
    fn complete_provider(&self, if_not_exists: bool) -> Option<IoCall> {
        let backend_upload = self.upload_record.as_ref()?.backend_upload.clone()?;
        let streamed = (self.upload_parts.iter()).filter(|part| part.location.partial);
        let complete = BlobEffect::CompleteUpload {
            backend_upload: Box::new(backend_upload),
            parts: self.resolved_parts.clone(),
            streamed: streamed.map(|part| part.part_number).collect(),
            compression: self.compression,
            if_not_exists,
        };
        Some(Effect::Blob(complete).into())
    }

    fn compose_parts(&self, parts: Vec<BackendLocation>) -> Result<IoCall, CompleteUploadError> {
        let upload = self.upload_record.as_ref();
        let upload = upload.ok_or(CompleteUploadError::InvalidOperationState)?;
        let pinned = ResolvedBackend::new(upload.backend.clone(), upload.storage_class.clone())
            .with_compression(self.compression);
        let compose = BlobEffect::Compose {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved: pinned,
            created_by: self.input.created_by,
            parts,
            plain: None,
        };
        Ok(Effect::Blob(compose).into())
    }

    fn handle_blob_composed(&mut self, event: Event) -> Turn {
        let mut location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            Event::Blob(BlobEvent::PlainWritten {
                location,
                version,
                etag,
            }) => {
                self.plain_version = version;
                self.plain_etag = etag;
                location
            }
            Event::Blob(BlobEvent::Error(BlobError::PathTaken)) => return self.plain_taken(),
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.cleanup.set_release(location.ulid);
                self.delete_location = Some(location);
                return Err(CompleteUploadError::CompleteUploadFailed);
            }
            Event::Blob(BlobEvent::Error(error)) => return Err(error.into()),
            _ => return Err(CompleteUploadError::InvalidOperationState),
        };
        // A compression changed since the upload opened composes the bytes again, except at a
        // tenant path. The target row keeps the provider object until the upload is gone.
        if EncodingClass::from(self.compression) != location.format.encoding().setting()
            && self.in_place_target(&location)
            && !location.is_external()
        {
            let call = self.compose_parts(vec![location])?;
            return self.compose(call);
        }
        // A sealed object has no full-object digest; its CRCs combine from the parts.
        if location.format.bucket_key().is_some() {
            let parts = self.resolved_parts.iter();
            let parts = parts.map(|part| (&part.location.hashes, part.location.blob_size));
            location
                .hashes
                .extend(combine_crcs(parts).unwrap_or_default());
        }
        self.composed_location = Some(location.clone());
        self.final_location = None;

        let hashes = match self.input.checksum_type {
            MultipartChecksumType::FullObject => &location.hashes,
            MultipartChecksumType::Composite => &self.composite_hashes,
        };

        for expected in &self.input.expected_checksums {
            let Some(actual) = hashes.get(expected.algorithm.hash_key()) else {
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
        if let Some(turn) = self.write_evidence() {
            return turn;
        }
        self.start_finalize()
    }

    /// Hands the state the finalize needs to its child; the parent keeps what its cleanup needs.
    fn finalize_child(&mut self) -> Self {
        let mut child = Self::new(self.input.clone());
        child.finalizing = true;
        child.state = CompleteUploadState::CheckPurgeFinalize;
        child.upload_record = self.upload_record.clone();
        child.upload_parts = std::mem::take(&mut self.upload_parts);
        child.resolved_parts = std::mem::take(&mut self.resolved_parts);
        child.composed_location = self.composed_location.clone();
        child.composite_hashes = std::mem::take(&mut self.composite_hashes);
        child.version_id = self.version_id;
        child.rocrate_limits = self.rocrate_limits.clone();
        child.restrictions = self.restrictions.clone();
        child.gated_bucket = self.gated_bucket.take();
        child.compression = self.compression;
        child.set_hint = self.set_hint;
        child.binding = self.binding.clone();
        child.version_epoch = self.version_epoch;
        child.authority = self.authority.clone();
        child.plain = self.plain.clone();
        child.plain_path = self.plain_path.take();
        child.plain_version = self.plain_version.take();
        child.plain_etag = self.plain_etag.take();
        child.written_intent = std::mem::take(&mut self.written_intent);
        child.version_origin = self.version_origin.take();
        child
    }

    fn start_finalize(&mut self) -> Turn {
        let child = self.finalize_child();
        self.call(CompleteUploadState::Finalize, IoCall::child(child))
    }

    /// A commit with unknown outcome may already own the composed object, so only a proven
    /// refusal rolls it back; the rest moves to the reconciliation queue.
    fn finalized(&mut self, result: IoResult) -> Turn {
        let error = match result.child::<CompleteUploadResult, CompleteUploadError>()? {
            Ok(result) => return Ok(self.finished(result)),
            Err(error) => error,
        };
        self.state = CompleteUploadState::Error;
        let sealed = self.sealed();
        match &error {
            CompleteUploadError::StorageError(storage) => {
                self.recheck_pending = sealed && *storage == StorageError::TransactionConflict;
                if !storage.proves_no_commit()
                    && let Some(location) = self.composed_location.take()
                {
                    warn!(
                        event = "complete_multipart_upload.commit_outcome_unknown",
                        backend = %location.backend,
                        blob_size = location.blob_size,
                        error = %storage,
                        "Queuing the composed object for reconciliation"
                    );
                    self.reconcile_location = Some(location);
                }
            }
            CompleteUploadError::BucketKey(_) => self.reclaim_pending = sealed,
            CompleteUploadError::BlobError(BlobError::Abe(
                AbeError::Parameters | AbeError::Epoch,
            )) => self.reclaim_pending = true,
            _ => {}
        }
        Err(error)
    }

    /// The reservation of the composed object is released and the follow-up work woken once
    /// the finalize committed.
    fn finished(&mut self, result: CompleteUploadResult) -> Next<CompleteUploadResult> {
        // The finalize deleted the record, so nothing is left to reopen.
        self.reset_done = true;
        let mut calls = Vec::new();
        if let Some(location) = self.composed_location.take() {
            let release = BlobEffect::ReleaseReservation { id: location.ulid };
            calls.push(Effect::Blob(release).into());
        }
        calls.push(schedule_snapshot_publish().into());
        calls.push(schedule_cleanup_effect().into());
        // A pending archive is hashed at once when its key is unlocked, also in this session.
        let pending = Some(&result.location).filter(|location| location.get_blake3().is_none());
        if let Some(key) = pending.and_then(|location| location.format.bucket_key()) {
            let wake = TaskEffect::ShortenTimer {
                key: TaskKey::PromotePending {
                    bucket_id: key.bucket_id,
                    generation: key.generation,
                },
                after: std::time::Duration::ZERO,
            };
            calls.push(Effect::Task(wake).into());
        }
        Next::Then {
            output: result,
            calls,
        }
    }
}

impl Operation for CompleteUploadOperation {
    type Output = CompleteUploadResult;
    type Error = CompleteUploadError;

    fn first(&mut self) -> Turn {
        match self.finalizing {
            true => self.read_publication(),
            false => self.mark_upload(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            CompleteUploadState::Init => self.first(),
            CompleteUploadState::MarkUpload => self.upload_marked(result),
            CompleteUploadState::ReadPlainOwner => self.plain_owner(result.value()?),
            CompleteUploadState::StatPlain => self.plain_stat(result.event()?),
            CompleteUploadState::ReserveCompose => self.compose_reserved(result.event()?),
            CompleteUploadState::ReadUploadParts => self.upload_parts_read(result),
            CompleteUploadState::ReadGateBucket => self.handle_gate_bucket(result.value()?),
            CompleteUploadState::ReadRecord => self.record_checked(result.values()?),
            CompleteUploadState::PolicyGate => self.gate_passed(result),
            CompleteUploadState::ComposeBlob => self.handle_blob_composed(result.event()?),
            CompleteUploadState::WriteIntent => self.intent_written(result),
            CompleteUploadState::SyncIntent => self.intent_synced(result),
            CompleteUploadState::DropIntent => self.intent_dropped(result),
            CompleteUploadState::SyncDroppedIntent => self.dropped_synced(result),
            CompleteUploadState::WriteEvidence => self.evidence_written(result),
            CompleteUploadState::Finalize => self.finalized(result),
            CompleteUploadState::CheckPurgeFinalize => self.publication_checked(result.values()?),
            CompleteUploadState::ReadBucketDefault => self.handle_default_read(result.values()?),
            CompleteUploadState::CheckSealSettings => self.seal_settings_read(result.value()?),
            CompleteUploadState::FenceAbe => self.abe_fenced(result.values()?),
            CompleteUploadState::FenceBackend => self.backend_fenced(result.value()?),
            CompleteUploadState::CheckHashLookup => self.hash_checked(result.value()?),
            CompleteUploadState::ReadDedupHead
            | CompleteUploadState::ReadDedupVersion
            | CompleteUploadState::ReadDedupLocation
            | CompleteUploadState::ReadDedupProvider
            | CompleteUploadState::ReadDedupClaim => self.dedup_read(result.value()?),
            CompleteUploadState::WriteBlobLocation => {
                result.done()?;
                self.read_object_lookup()
            }
            CompleteUploadState::ReadObjectLookup => self.object_lookup_read(result.value()?),
            CompleteUploadState::ReadLivenessVersion => self.liveness_read(result.value()?),
            CompleteUploadState::WriteBlobHead => {
                result.done()?;
                self.write_path_index()
            }
            CompleteUploadState::ReadRegistered => self.registered_read(result.value()?),
            CompleteUploadState::DropStagedLocation => {
                result.done()?;
                self.delete_upload_records()
            }
            CompleteUploadState::ReadTrail => self.trail_read(result),
            CompleteUploadState::WritePathIndex => {
                result.done()?;
                self.write_version()
            }
            CompleteUploadState::WriteVersionRecord => {
                result.done()?;
                self.version_written()
            }
            CompleteUploadState::WriteEnvelope => {
                result.done()?;
                self.write_owner()
            }
            CompleteUploadState::WriteCopyOwner => {
                result.done()?;
                self.register_managed_copy()
            }
            CompleteUploadState::RegisterManagedCopy => {
                result.done()?;
                self.write_object_metadata()
            }
            CompleteUploadState::WriteObjectMetadata => {
                result.done()?;
                self.delete_upload_records()
            }
            CompleteUploadState::ReadUploadRecord => self.upload_record_read(result.value()?),
            CompleteUploadState::CountUpload => {
                result.done()?;
                self.delete_records()
            }
            CompleteUploadState::DeleteUploadRecords => {
                result.done()?;
                self.records_deleted()
            }
            CompleteUploadState::ReadImportRule => self.import_rule_read(result.values()?),
            CompleteUploadState::ReadReaders => self.readers_read(result),
            CompleteUploadState::RetireIntent => {
                result.done()?;
                self.write_obligation()
            }
            CompleteUploadState::WriteCleanupRecords => {
                result.done()?;
                self.cleanup_written()
            }
            CompleteUploadState::WriteReplicationObligation => {
                result.done()?;
                self.obligation_written()
            }
            CompleteUploadState::EnforceQuota => self.handle_enforce_quota(result),
            CompleteUploadState::UpdateUsage => self.handle_usage_update(result),
            CompleteUploadState::CloseInventory => self.handle_inventory_closed(result),
            CompleteUploadState::Error => Err(CompleteUploadError::InvalidOperationState),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.finalizing {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
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

    /// Reopens a record the mark may have left completing, rolls the composed object back and
    /// releases its reservation; the runner makes these calls in order before the run ends.
    /// The finalize child leaves all of it to its parent.
    fn abort(&mut self) -> Vec<IoCall> {
        if self.finalizing {
            return Vec::new();
        }
        // The finalize was cut off: its commit may have applied.
        if self.state == CompleteUploadState::Finalize {
            self.preserve_blob();
        }
        let mut calls = Vec::new();
        if self.marked && !self.reset_done {
            self.reset_done = true;
            let reopen = ReopenUpload {
                input: self.input.clone(),
                plan: self.plan(),
                reclaim_pending: self.reclaim_pending,
                recheck_pending: self.recheck_pending,
                step: ReopenStep::ReadRecord,
            };
            calls.push(IoCall::child(reopen));
        }
        let works = self.rollback_works();
        if let Some(location) = self.rollback.take() {
            calls.push(IoCall::child(RollbackWrite::new(location)));
        }
        for work in works {
            match work.to_bytes() {
                Ok(value) => {
                    let key = cleanup_key(&work);
                    let row = (BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into());
                    calls.push(IoCall::Write(vec![row]));
                }
                Err(error) => warn!(%error, "A multipart cleanup row could not be encoded"),
            }
        }
        if let Some(id) = self.cleanup.take_release() {
            calls.push(Effect::Blob(BlobEffect::ReleaseReservation { id }).into());
        }
        calls
    }
}

/// The key of a cleanup row: a reconciled object by its id, so a second one replaces it.
fn cleanup_key(work: &BlobCleanupWork) -> Key {
    match work {
        BlobCleanupWork::ReconcileWrite { location, .. } => {
            location.ulid.to_bytes().to_vec().into()
        }
        _ => Ulid::generate().to_bytes().to_vec().into(),
    }
}

/// The marked record and the checksum type the completion uses.
type Marked = (MultipartUpload, MultipartChecksumType);

/// Marks the upload completing in one transaction, after the purge fence of its key.
#[derive(Debug, PartialEq)]
struct MarkCompleting {
    input: CompleteUploadInput,
    record: Option<MultipartUpload>,
}

impl MarkCompleting {
    fn upload_key(&self) -> Key {
        self.input.upload_id.to_bytes().to_vec().into()
    }

    fn marked(&mut self, values: Vec<Option<Value>>) -> Result<Next<Marked>, CompleteUploadError> {
        let [fence, upload] =
            <[_; 2]>::try_from(values).map_err(|_| CompleteUploadError::InvalidOperationState)?;
        check_fence_row(fence.as_deref(), &self.input.bucket, &self.input.key)?;
        let value = upload.ok_or(CompleteUploadError::NoSuchUpload)?;
        let mut record = MultipartUpload::from_bytes(value.as_ref())?;
        let check = StatusCheck::Takeover {
            now_ms: self.input.now_ms,
        };
        validate_upload(&record, &self.input.bucket, &self.input.key, check)?;
        CompleteUploadOperation::validate_checksum_contract(&mut self.input, &record)?;
        if record.status == MultipartUploadStatus::Completing {
            warn!(
                upload_id = %self.input.upload_id,
                "Taking over a multipart completion whose lease expired"
            );
        }
        record.status = MultipartUploadStatus::Completing;
        record.completing_since_ms = Some(self.input.now_ms);
        let row = (
            UPLOAD_KEYSPACE.to_string(),
            self.upload_key(),
            record.to_bytes()?.into(),
        );
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for MarkCompleting {
    type Output = Marked;
    type Error = CompleteUploadError;

    fn first(&mut self) -> Result<Next<Marked>, CompleteUploadError> {
        let reads = vec![
            (
                PURGE_FENCE_KEYSPACE.to_string(),
                fence_key(&self.input.bucket),
            ),
            (UPLOAD_KEYSPACE.to_string(), self.upload_key()),
        ];
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Marked>, CompleteUploadError> {
        match self.record.take() {
            Some(record) => {
                result.done()?;
                Ok(Next::Done((record, self.input.checksum_type)))
            }
            None => self.marked(result.values()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum ReopenStep {
    ReadRecord,
    WriteRecord,
    RecheckPending,
    DeletePending,
}

/// Reopens a record this completion left completing, in one transaction. Another attempt
/// that took the lease over owns the record, so it is left alone.
#[derive(Debug, PartialEq)]
struct ReopenUpload {
    input: CompleteUploadInput,
    plan: Option<SealPlan>,
    reclaim_pending: bool,
    recheck_pending: bool,
    step: ReopenStep,
}

type Reopened = Result<Next<()>, CompleteUploadError>;

impl ReopenUpload {
    fn upload_key(&self) -> Key {
        self.input.upload_id.to_bytes().to_vec().into()
    }

    fn record_read(&mut self, value: Option<Value>) -> Reopened {
        let Some(value) = value else {
            return Ok(Next::Done(()));
        };
        let mut record = MultipartUpload::from_bytes(value.as_ref())?;
        if record.completing_since_ms != Some(self.input.now_ms) {
            return Ok(Next::Done(()));
        }
        record.status = MultipartUploadStatus::Open;
        record.completing_since_ms = None;
        let row = (
            UPLOAD_KEYSPACE.to_string(),
            self.upload_key(),
            record.to_bytes()?.into(),
        );
        self.step = ReopenStep::WriteRecord;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn record_written(&mut self) -> Reopened {
        if self.reclaim_pending {
            return self.delete_pending();
        }
        let Some(plan) = self.plan.filter(|_| self.recheck_pending) else {
            return Ok(Next::Done(()));
        };
        let mut reads = vec![
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                self.input.bucket.as_bytes().to_vec().into(),
            ),
            (ABE_PENDING_KEYSPACE.to_string(), self.upload_key()),
        ];
        reads.extend(abe_reads(plan.key));
        self.step = ReopenStep::RecheckPending;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Only a moved generation, parameters or epoch drop the envelope; other conflicts retry.
    fn pending_rechecked(&mut self, values: Vec<Option<Value>>) -> Reopened {
        let Some((settings, pending)) = values.split_first() else {
            return Err(CompleteUploadError::InvalidOperationState);
        };
        let moved = BucketEncryption::from_row(settings.as_deref())
            .is_ok_and(|settings| storage_current(self.plan.as_ref(), &settings).is_err());
        let fence = pending_envelope(self.plan, &keyed(pending), &self.input);
        if moved || matches!(fence, Err(AbeError::Parameters | AbeError::Epoch)) {
            return self.delete_pending();
        }
        Ok(Next::Done(()))
    }

    fn delete_pending(&mut self) -> Reopened {
        self.step = ReopenStep::DeletePending;
        let row = (ABE_PENDING_KEYSPACE.to_string(), self.upload_key());
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for ReopenUpload {
    type Output = ();
    type Error = CompleteUploadError;

    fn first(&mut self) -> Reopened {
        self.step = ReopenStep::ReadRecord;
        Ok(Next::Call(read_row(UPLOAD_KEYSPACE, self.upload_key())))
    }

    fn next(&mut self, result: IoResult) -> Reopened {
        match self.step {
            ReopenStep::ReadRecord => self.record_read(result.value()?),
            ReopenStep::WriteRecord => {
                result.done()?;
                self.record_written()
            }
            ReopenStep::RecheckPending => self.pending_rechecked(result.values()?),
            ReopenStep::DeletePending => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Checks the pending envelope, then the parameters and epoch, as read in that order.
/// A generation without admitted parameters and envelope publishes bucket-only.
fn pending_envelope(
    plan: Option<SealPlan>,
    values: &[(Key, Option<Value>)],
    input: &CompleteUploadInput,
) -> Result<Option<ObjectEnvelope>, AbeError> {
    let plan = plan.ok_or(AbeError::Context)?;
    let [(_, pending), current @ ..] = values else {
        return Err(AbeError::Context);
    };
    if pending.is_none() && current.first().is_some_and(|(_, value)| value.is_none()) {
        return Ok(None);
    }
    let envelope = ObjectEnvelope::from_bytes(pending.as_deref().ok_or(AbeError::Stale)?)?;
    let (parameters, epoch) = parse_abe(current, plan.key)?;
    if parameters.realm_id != input.realm_id
        || parameters.node_id != input.node_id
        || envelope.context.object_key != input.key
    {
        return Err(AbeError::Context);
    }
    envelope.anchored(&parameters, epoch)?;
    Ok(Some(envelope))
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

#[path = "dedup.rs"]
mod dedup;
#[path = "finalize.rs"]
mod finalize;
#[path = "plain.rs"]
mod plain;

#[cfg(test)]
#[path = "complete_tests.rs"]
mod pure_tests;
