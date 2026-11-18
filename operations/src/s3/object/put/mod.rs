//! Runs the S3 PutObject operation: quota and policy gates, blob write, head and version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup_rows::schedule_cleanup_effect;
use crate::blob::holders::dht_registration_effect;
use crate::blob::records::{HeadAliasContext, release_rows};
use crate::distribution::guard::{GuardError, WriteAuthority, authority_reads, record_authority};
use crate::distribution::inventory::{AddedVersion, stamp_head, stamp_version, version_written};
use crate::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, ReadPolicyError, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::s3::purge_fence::{
    PurgeFenceError, check_fence_row, check_published, fence_key, publication_rows,
};
use crate::s3::row_fence::{FenceError, RowFence};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish,
};
use crate::s3::write_cleanup::{ReleaseWrite, RollbackWrite, WriteCleanup};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, COPY_OWNER_KEYSPACE, MANAGED_COPY_KEYSPACE,
    NODE_SUBJECT_KEYSPACE, PATHS_INDEX_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::managed_copy::{
    CopyRegistration, CopyRequest, ManagedCopyError, register_entry, split_serve_reads,
    validate_registration,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::rows::read_row;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::execution::table::searchable;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CopyOrigin, CopyOwner, CurrentVersionPointer, ManagedCopyKey, VersionKey,
    WriteOwner,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};
use aruna_core::structs::storage::distribution::inventory::VersionEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan,
};
use aruna_core::structs::storage::format::EncodingClass;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::replication_queue::{
    LiveObligationRecord, live_obligation_entry,
};
use aruna_core::structs::storage::routing::{RoutingError, RoutingSnapshot, resolve_backend};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, Value};
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::time::unix_timestamp_millis;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

fn seal_error(error: BucketKeyError) -> PutObjectError {
    PutObjectError::ConversionError(ConversionError::BucketKey(error))
}

/// Bounded retries for an SSI conflict on the metadata commit. Concurrent
/// writes in one group contend on the shared usage counters, which is a
/// refused transaction, not a failed request.
const CONFLICT_RETRIES: u8 = 4;

#[derive(Debug, Eq, PartialEq)]
pub enum PutObjectState {
    Init,
    ReadPreassignedVersion,
    ReadPreassignedLocation,
    ReadPreassignedCopy,
    ReadGateBucket,
    PolicyGate,
    /// Reads a distributed bucket's record: only the key's coordinator writes, at its epoch.
    ReadRecord,
    CheckPurgeWrite,
    /// Reads the bucket's encryption settings before any byte moves.
    ReadSealSettings,
    ReadSealKey,
    WriteBlob,
    /// Publishes the written bytes in the version transaction, run as a child.
    Publish,
    /// Writes the reconcile row of the written blob in the version transaction.
    WriteCleanupRow,
    CheckPurgeFence,
    CheckBucket,
    /// Reads the bucket's encryption settings in the version transaction.
    CheckSealFence,
    FenceBackend,
    CheckHashLookup,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WritePathIndex,
    CreateVersionRecord,
    WriteCopyOwner,
    RegisterManagedCopy,
    WriteReplicationObligation,
    EnforceQuota,
    UpdateUsage,
    /// Checks the caller's guard rows and writes its receipts in the version transaction.
    RowFence,
    /// The write failed; only the cleanup in `abort` remains.
    Error,
    ReadAbe,
    FenceAbe,
    WriteEnvelope,
    /// Reads which key the rule recorded at the path of a same-name write.
    ReadPlainOwner,
    /// Looks at the path of a same-name write before the bytes go there.
    StatPlain,
    WritePlainRecords,
    /// Reads the import rule in the version transaction.
    ReadImportRule,
    /// Records what a plain write will be before its bytes reach the path.
    WriteIntent,
    /// Syncs that record to disk before the write.
    SyncIntent,
    /// Reads the version an import may have registered already.
    ReadHistory,
    /// Deletes the intent records the committed version made obsolete.
    RetireIntents,
    /// Deletes the record of a write the provider refused before any byte moved.
    DropIntent,
    /// Syncs that delete to disk.
    SyncDroppedIntent,
    /// Records the hash of the written bytes in the write intent.
    WriteEvidence,
    /// Reads the delete trail of the key in the version transaction.
    ReadTrail,
    /// Reads the group's import rules in the version transaction.
    ReadRules,
    /// Reads which rules read the written path, inside the version transaction.
    ReadReaders,
    /// Reads the current version and its file, which a re-upload of the same bytes reads too.
    ReadDedupHead,
    ReadDedupVersion,
    ReadDedupLocation,
    ReadDedupProvider,
    ReadDedupClaim,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("{}", aruna_core::structs::storage::routing::UNMAPPED_KEY)]
    Unmapped,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("output is missing")]
    MissingOutput,
    #[error("hash missing: {0}")]
    MissingHash(String),
    #[error("request body missing")]
    MissingBody,
    #[error("body size did not match Content-Length header")]
    IncompleteBody,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error("blob write failed: {0}")]
    WriteFailed(String),
    #[error("blob backend write failed: {0}")]
    BlobWriteFailed(BlobError),
    #[error("preassigned version exists without a materialized blob")]
    InvalidPreassignedVersion,
    #[error("an object already exists at this key")]
    ObjectExists,
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error(transparent)]
    QuotaGateError(#[from] QuotaGateError),
    #[error(transparent)]
    RoutingFailed(#[from] RoutingError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error(transparent)]
    PolicyError(#[from] PlacementPolicyError),
    #[error(transparent)]
    PolicyGate(#[from] PolicyGateError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("Something went wrong ...")]
    PutObjectFailed,
    #[error("the object key changed since it was read")]
    HeadChanged,
    #[error("the import rule was removed")]
    ImportRemoved,
    #[error("the version already exists")]
    VersionExists,
    #[error("the version or the current object of its key was deleted and is not restored")]
    HistoryDeleted,
    #[error("a guarded row changed before the version committed")]
    FenceTaken,
    #[error("usage counters are being rebuilt; retry later")]
    UsageRebuilding,
}

/// A refused call fails as its storage error; a lost conflict as the transaction conflict.
impl From<IoError> for PutObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<PutObjectResult>, PutObjectError>;

#[derive(Debug, PartialEq)]
pub struct PutObjectInput {
    pub bucket: String,
    pub key: String,
    pub content_length: Option<u64>,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
}

#[derive(Debug, PartialEq)]
pub struct PutObjectConfig {
    pub user_id: UserId,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub request: PutObjectInput,
    pub expected_checksums: Vec<ExpectedChecksum>,
    pub checksum_type: Option<String>,
    pub exists: bool, //Note: For version shenanigans which will be implemented later
    pub version_source: Option<VersionSourceBinding>,
    /// Retry fence. Must be a freshly minted, time-meaningful ULID because its
    /// timestamp defines `created_at` and ULID ordering defines version order.
    pub preassigned_version_id: Option<Ulid>,
    /// Hard ceiling (bytes) the group's realm-wide `logical_bytes` may reach,
    /// resolved from the realm quota config at the request surface. `None` =
    /// unlimited, so no gate is enforced.
    pub quota_ceiling: Option<u64>,
    /// Routing inputs assembled by the caller, so resolution stays a pure
    /// synchronous step inside the operation.
    pub routing: RoutingSnapshot,
}

impl PutObjectConfig {
    /// The settings without the body, which only the write that streams it needs.
    fn without_body(&self) -> Self {
        let request = &self.request;
        Self {
            user_id: self.user_id,
            group_id: self.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: PutObjectInput {
                bucket: request.bucket.clone(),
                key: request.key.clone(),
                content_length: request.content_length,
                body: None,
            },
            expected_checksums: self.expected_checksums.clone(),
            checksum_type: self.checksum_type.clone(),
            exists: self.exists,
            version_source: self.version_source.clone(),
            preassigned_version_id: self.preassigned_version_id,
            quota_ceiling: self.quota_ceiling,
            routing: self.routing.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutObjectResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
    /// The preassigned version already existed, so this put committed nothing new.
    pub replayed: bool,
}

/// Runs outside any transaction: the bytes are written first, then the version transaction
/// runs as a second instance of this operation with `publishing` set, rebuilt after a lost
/// conflict.
#[derive(Debug, PartialEq)]
pub struct PutObjectOperation {
    state: PutObjectState,
    config: PutObjectConfig,
    /// This instance is the version transaction.
    publishing: bool,
    conflicts: u8,
    version_id: Option<Ulid>,
    written_location: Option<BackendLocation>,
    cleanup_location: Option<BackendLocation>,
    /// The written blob after a publish whose commit outcome is unknown.
    reconcile_location: Option<BackendLocation>,
    cleanup: WriteCleanup<PutObjectError>,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    /// A blob already on the resolved backend, taken instead of a stream.
    adopt: Option<BackendLocation>,
    /// The written location belongs to another version; never delete it.
    adopted: bool,
    was_live: bool,
    /// Offered directory bytes the replaced head stops charging.
    released_bytes: u64,
    /// Refuses the write when the key has a live head; a replayed preassigned version passes.
    create_only: bool,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    /// The location the version publishes.
    output: Option<BackendLocation>,
    expected_bucket: Option<BucketInfo>,
    metadata: HashMap<String, String>,
    rocrate_limits: RoCrateLimits,
    restrictions: Option<Vec<PathRestriction>>,
    /// Refs stored on the version record, reused verbatim by its registration.
    stored_policies: Vec<PlacementPolicyRef>,
    /// Destination default, read inside the version transaction so an edit
    /// observed after streaming cannot commit a stale ref set.
    bucket_policies: Vec<PlacementPolicyRef>,
    inherited_policies: Vec<PlacementPolicyRef>,
    /// The origin a caller names, such as a copy's source; an upload by default.
    version_origin: Option<VersionOrigin>,
    /// Destination details of this node. Absent means no governed byte may be
    /// materialized here at all.
    gate_context: Option<GateContext>,
    /// What the gate decided on, re-read inside the version transaction.
    gated_bucket: Option<GatedBucket>,
    /// Refs of an existing preassigned version, checked against its registration
    /// before the replay may hand back its location.
    replay_policies: Vec<PlacementPolicyRef>,
    replay_location: Option<BackendLocation>,
    /// Recorded on the registration so a reader learns why the copy is here.
    origin: CopyOrigin,
    /// Settings read before the write, kept until the active key record is read.
    seal_settings: Option<BucketEncryption>,
    /// How the bytes are sealed, captured once before they stream; `None` writes plain bytes.
    seal_plan: Option<SealPlan>,
    envelope_enabled: bool,
    envelope_plan: Option<aruna_core::structs::storage::abe::EnvelopePlan>,
    envelope: Option<aruna_core::structs::storage::abe::ObjectEnvelope>,
    envelope_bytes: u64,
    plain: Option<plain::PlainWrite>,
    /// The head the caller saw, `None` for no head; another head refuses the write.
    expected_head: Option<Option<Ulid>>,
    import_rows: Option<plain::ImportRows>,
    /// Whether the import rule exists, read in the current version transaction.
    rule_exists: Option<bool>,
    /// Registers the version as history: the head and its generation stay as they are.
    keep_head: bool,
    /// The plain write that waits for its intent record.
    pending_write: Option<IoCall>,
    /// The intent record of the plain write in flight.
    written_intent: Vec<Key>,
    /// The delete trail the version transaction reads before an import registers a version.
    trail_scan: Option<trail::TrailScan>,
    rule_scan: Option<sharing::RuleScan>,
    readers: sharing::ReaderScan,
    /// The written linked file the version gives up for the current version's identical one.
    dedup: Option<BackendLocation>,
    dedup_checked: bool,
    /// Set when the preassigned version already existed and the put only handed it back.
    replayed: bool,
    /// An import registered the version with the same bytes, so nothing is written.
    registered: bool,
    row_fence: Option<RowFence>,
    /// The set hint of a distributed bucket; ordinary buckets mint plain ULIDs.
    pub(super) set_hint: Option<SetHint>,
    /// The bucket's distribution binding, whose inventory the version transaction maintains.
    binding: Option<DistributionBinding>,
    /// The coordinator authority the write was routed under; the version transaction rechecks it.
    authority: Option<WriteAuthority>,
    /// The head this write installs and the epoch of its version, for the inventory.
    head_pointer: Option<CurrentVersionPointer>,
    version_epoch: OriginalEpoch,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        let version_id = config.preassigned_version_id;
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            publishing: false,
            conflicts: 0,
            version_id,
            written_location: None,
            cleanup_location: None,
            reconcile_location: None,
            cleanup: WriteCleanup::default(),
            existing_pointer: None,
            new_blob: false,
            adopt: None,
            adopted: false,
            was_live: false,
            released_bytes: 0,
            create_only: false,
            usage_update: None,
            quota_gate: None,
            output: None,
            expected_bucket: None,
            metadata: HashMap::new(),
            rocrate_limits: RoCrateLimits::default(),
            restrictions: None,
            stored_policies: Vec::new(),
            bucket_policies: Vec::new(),
            inherited_policies: Vec::new(),
            version_origin: None,
            gate_context: None,
            gated_bucket: None,
            replay_policies: Vec::new(),
            replay_location: None,
            origin: CopyOrigin::Write,
            seal_settings: None,
            seal_plan: None,
            envelope_enabled: true,
            envelope_plan: None,
            envelope: None,
            envelope_bytes: 0,
            plain: None,
            expected_head: None,
            import_rows: None,
            rule_exists: None,
            keep_head: false,
            pending_write: None,
            written_intent: Vec::new(),
            trail_scan: None,
            rule_scan: None,
            readers: sharing::ReaderScan::default(),
            dedup: None,
            dedup_checked: false,
            replayed: false,
            registered: false,
            row_fence: None,
            set_hint: None,
            binding: None,
            authority: None,
            head_pointer: None,
            version_epoch: OriginalEpoch::default(),
        }
    }

    /// Mints no object key or envelope for this copied version.
    pub fn without_envelope(mut self) -> Self {
        self.envelope_enabled = false;
        self
    }

    /// Why this write places a copy here. A plain client write records itself;
    /// compute input staging names itself instead.
    pub fn with_origin(mut self, origin: CopyOrigin) -> Self {
        self.origin = origin;
        self
    }

    /// The destination this write is evaluated against. Omitting it leaves the
    /// ungoverned path untouched and fails every governed write closed.
    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    pub fn with_bucket_guard(mut self, bucket: BucketInfo) -> Self {
        self.expected_bucket = Some(bucket);
        self
    }

    pub fn create_only(mut self) -> Self {
        self.create_only = true;
        self
    }

    /// Refuses the write with `HeadChanged` unless the key's head is still `head`.
    pub fn with_expected_head(mut self, head: Option<Ulid>) -> Self {
        self.expected_head = Some(head);
        self
    }

    /// Registers an older version below the current head, with no replication obligation.
    /// The head, its generation and the live object count stay as they are.
    pub fn keep_head(mut self) -> Self {
        self.keep_head = true;
        self
    }

    /// Refs a copy or derived write carries over from its source. They are
    /// unioned with the destination default, so a copy can only be at least as
    /// constrained as what it was copied from.
    pub fn with_inherited_policies(mut self, policies: Vec<PlacementPolicyRef>) -> Self {
        self.inherited_policies = policies;
        self
    }

    /// The origin the new version records instead of an upload.
    pub fn with_version_origin(mut self, origin: Option<VersionOrigin>) -> Self {
        self.version_origin = origin;
        self
    }

    /// A blob the destination's backend already holds. The version is minted
    /// on it without streaming; the request body must be `None`.
    pub fn with_adopted(mut self, location: BackendLocation) -> Self {
        self.adopt = Some(location);
        self
    }

    /// Subject generation the gate admitted this write under; zero for an
    /// ungoverned write, which no subject ever evaluated.
    fn stored_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    /// Union of the destination default read in this transaction and whatever
    /// the write inherited. Both empty leaves the version ungoverned.
    fn effective_policies(&self) -> Vec<PlacementPolicyRef> {
        let mut policies = self.bucket_policies.clone();
        policies.extend(self.inherited_policies.iter().copied());
        policies
    }

    /// The writer's credential restrictions. They are persisted on the durable
    /// replication obligation, so a scoped write cannot escalate to unscoped
    /// when the obligation repair path enqueues replication instead.
    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Refuses the write with `FenceTaken` unless the version transaction still reads each
    /// guard as given; each receipt key then records the version id in that transaction.
    pub fn with_fence(
        mut self,
        guards: Vec<(String, Key, Option<Value>)>,
        receipts: Vec<(String, Key)>,
    ) -> Self {
        self.row_fence = Some(RowFence::new(guards, receipts));
        self
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    fn call(&mut self, state: PutObjectState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn effect(&mut self, state: PutObjectState, effect: Effect) -> Turn {
        self.call(state, effect.into())
    }

    fn version_key(&self, version_id: Ulid) -> VersionKey {
        VersionKey::new(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
            version_id,
        )
    }

    fn begin(&mut self) -> Turn {
        let Some(version_id) = self.config.preassigned_version_id else {
            return self.handle_init();
        };
        let key = self.version_key(version_id).to_bytes()?;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
        self.call(PutObjectState::ReadPreassignedVersion, read)
    }

    fn handle_preassigned_version(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.handle_init();
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        let location_key = version.location_key();
        let location_key = location_key.ok_or(PutObjectError::InvalidPreassignedVersion)?;
        self.replay_policies = version.placement_policies.clone();
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, location_key.to_bytes());
        self.call(PutObjectState::ReadPreassignedLocation, read)
    }

    fn handle_preassigned_location(&mut self, value: Option<Value>) -> Turn {
        let value = value.ok_or(PutObjectError::InvalidPreassignedVersion)?;
        let location = BackendLocation::from_bytes(value.as_ref())?;
        if self.replay_policies.is_empty() {
            return self.finish_replay(location);
        }
        // A governed replay may only hand back a copy this node registered.
        let version_id = self.version_id;
        let version_id = version_id.ok_or(PutObjectError::InvalidPreassignedVersion)?;
        let key = ManagedCopyKey::new(self.version_key(version_id), location.backend.clone());
        // The replay hands back bytes, so it answers the same question a serve
        // does: is this copy registered *and* may this node serve at all.
        let reads = vec![
            (
                MANAGED_COPY_KEYSPACE.to_string(),
                key.to_bytes().map_err(ManagedCopyError::from)?.into(),
            ),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ];
        self.replay_location = Some(location);
        self.call(PutObjectState::ReadPreassignedCopy, IoCall::Read(reads))
    }

    fn handle_preassigned_copy(&mut self, values: Vec<Option<Value>>) -> Turn {
        let (value, subject) = split_serve_reads(abe::keyed(&values))?;
        let (Some(version_id), Some(location)) = (self.version_id, self.replay_location.take())
        else {
            return Err(PutObjectError::InvalidPreassignedVersion);
        };
        let key = ManagedCopyKey::new(self.version_key(version_id), location.backend.clone());
        validate_registration(
            value.as_deref(),
            &CopyRequest {
                key: &key,
                node_id: Some(self.config.node_id),
                blake3: None,
                refs: &self.replay_policies,
                subject_generation: Some(subject.subject.generation),
            },
        )?;
        self.finish_replay(location)
    }

    fn finish_replay(&mut self, location: BackendLocation) -> Turn {
        self.replayed = true;
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        Ok(Next::Done(PutObjectResult {
            location,
            version_id,
            replayed: true,
        }))
    }

    /// The destination default is read before any byte moves, so the gate that
    /// admits this write sees the refs the version would actually carry.
    fn handle_init(&mut self) -> Turn {
        let read = read_row(
            S3_BUCKET_KEYSPACE,
            self.config.request.bucket.as_bytes().to_vec(),
        );
        self.call(PutObjectState::ReadGateBucket, read)
    }

    fn handle_gate_bucket(&mut self, value: Option<Value>) -> Turn {
        let bucket = (value.as_ref())
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let refs = union_refs(
            &GatedBucket::observe(bucket.as_ref()).policies,
            &self.inherited_policies,
        )?;
        self.gated_bucket = Some(
            GatedBucket::observe(bucket.as_ref())
                .stored_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        self.binding = (bucket.as_ref()).and_then(|bucket| bucket.distribution.clone());
        self.set_hint = self.binding.as_ref().map(DistributionBinding::hint);
        // A planned id is minted before the write; inside a set it must already carry the hint.
        if let (Some(hint), Some(id)) = (self.set_hint, self.config.preassigned_version_id)
            && SetHint::of_version(id) != hint
        {
            return Err(PutObjectError::InvalidPreassignedVersion);
        }
        let group_id = (bucket.as_ref()).map_or(self.config.group_id, |bucket| bucket.group_id);
        match write_gate(self.gate_context.as_ref(), &refs, Some(group_id))? {
            None => self.check_write_fence(),
            Some(gate) => self.call(PutObjectState::PolicyGate, IoCall::child(gate)),
        }
    }

    fn gate_passed(&mut self, result: IoResult) -> Turn {
        let outcome = result.child::<PolicyGateOutcome, ReadPolicyError>()?;
        let outcome = outcome.map_err(PolicyGateError::from)?;
        gate_decision(outcome)?;
        self.check_write_fence()
    }

    fn check_write_fence(&mut self) -> Turn {
        if let Some(binding) = self.binding.as_ref() {
            let reads = IoCall::Read(authority_reads(binding.set_id));
            return self.call(PutObjectState::ReadRecord, reads);
        }
        self.read_write_fence()
    }

    fn record_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let binding = self.binding.as_ref();
        let binding = binding.ok_or(PutObjectError::InvalidOperationState)?;
        let answer = Event::Storage(StorageEvent::BatchReadResult {
            values: abe::keyed(&values),
        });
        let authority = record_authority(binding, &self.config.request.key, answer)?;
        self.version_epoch = authority.epoch;
        self.authority = Some(authority);
        self.read_write_fence()
    }

    fn read_write_fence(&mut self) -> Turn {
        let read = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.config.request.bucket));
        self.call(PutObjectState::CheckPurgeWrite, read)
    }

    fn write_fence_checked(&mut self, value: Option<Value>) -> Turn {
        let (bucket, key) = (&self.config.request.bucket, &self.config.request.key);
        check_fence_row(value.as_deref(), bucket, key)?;
        self.read_seal_settings()
    }

    fn read_seal_settings(&mut self) -> Turn {
        let read = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.config.request.bucket.as_bytes().to_vec(),
        );
        self.call(PutObjectState::ReadSealSettings, read)
    }

    /// A plain bucket writes at once; an encrypting one first reads its active key record.
    fn seal_settings_read(&mut self, value: Option<Value>) -> Turn {
        let settings = BucketEncryption::from_row(value.as_deref())?;
        let Some(key) = settings.active_key() else {
            return self.write_blob();
        };
        self.seal_settings = Some(settings);
        let read = read_row(BUCKET_KEY_KEYSPACE, key.key());
        self.call(PutObjectState::ReadSealKey, read)
    }

    fn seal_key_read(&mut self, value: Option<Value>) -> Turn {
        let settings = self.seal_settings.take();
        let settings = settings.ok_or(PutObjectError::InvalidOperationState)?;
        let record = match value.map(|value| BucketKeyRecord::from_bytes(value.as_ref())) {
            Some(record) => record?,
            None => return Err(seal_error(BucketKeyError::Unsupported)),
        };
        self.seal_plan = SealPlan::capture(&settings, &record).map_err(seal_error)?;
        if self.seal_plan.is_none()
            || self.adopt.is_some()
            || !self.envelope_enabled
            || self.origin != CopyOrigin::Write
            || self.config.preassigned_version_id.is_some()
        {
            self.write_blob()
        } else {
            self.read_abe(false)
        }
    }

    fn write_blob(&mut self) -> Turn {
        // Resolution runs before any bytes move; a failure is terminal.
        let resolved = resolve_backend(
            &self.config.routing,
            &self.config.request.bucket,
            &self.config.request.key,
        )?;
        let resolved = resolved.with_encryption(self.seal_plan);
        if let Some(location) = self.adopt.take() {
            // Separate encrypted writes never share an archive.
            if self.seal_plan.is_some() {
                return Err(PutObjectError::WriteFailed(
                    "an encrypting bucket does not adopt another blob".to_string(),
                ));
            }
            // Imported bytes stay on the backend they were found on.
            if location.backend != resolved.backend && !location.is_external() {
                return Err(PutObjectError::WriteFailed(
                    "the adopted blob sits on another backend".to_string(),
                ));
            }
            if location.format.encoding().setting() != EncodingClass::from(resolved.compression) {
                return Err(PutObjectError::WriteFailed(
                    "the adopted blob uses another encoding".to_string(),
                ));
            }
            self.adopted = true;
            self.written_location = Some(location);
            return self.start_publish();
        }
        if let Some(target) = self.plain_target(&resolved) {
            return self.start_plain(target, resolved);
        }
        // A mapped bucket stores every key 1:1 or not at all.
        if self.config.routing.mapped(&self.config.request.bucket) {
            return Err(PutObjectError::Unmapped);
        }
        let blob = self.config.request.body.take();
        let blob = blob.ok_or(PutObjectError::MissingBody)?;
        let write = match self.envelope_plan.clone() {
            Some(plan) => BlobEffect::Abe(Box::new(
                aruna_core::structs::storage::abe::AbeEffect::Write {
                    plan,
                    resolved,
                    bucket: self.config.request.bucket.clone(),
                    key: self.config.request.key.clone(),
                    created_by: self.config.user_id,
                    blob,
                    size: self.config.request.content_length,
                },
            )),
            None => BlobEffect::Write {
                bucket: self.config.request.bucket.clone(),
                key: self.config.request.key.clone(),
                resolved,
                created_by: self.config.user_id,
                blob,
                size: self.config.request.content_length,
            },
        };
        self.effect(PutObjectState::WriteBlob, Effect::Blob(write))
    }

    fn handle_write_finished(&mut self, event: Event) -> Turn {
        let location = match event {
            Event::Blob(BlobEvent::Abe(event)) => {
                let aruna_core::structs::storage::abe::AbeEvent::Written { location, envelope } =
                    *event
                else {
                    return Err(PutObjectError::InvalidOperationState);
                };
                self.envelope = Some(envelope);
                location
            }
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            Event::Blob(BlobEvent::PlainWritten {
                location,
                version,
                etag,
            }) => {
                if let Some(plain) = self.plain.as_mut() {
                    plain.version = version;
                    plain.etag = etag;
                }
                location
            }
            // Only a client-sourced stream fault may become a client error; a
            // server-side write fault must stay retryable, never a bad digest.
            Event::Blob(BlobEvent::Error(BlobError::StreamFailed(message))) => {
                return Err(PutObjectError::WriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(error @ BlobError::WriteCleanup { .. })) => {
                if let BlobError::WriteCleanup { location, .. } = &error {
                    self.written_location = Some(location.clone());
                }
                return Err(PutObjectError::BlobWriteFailed(error));
            }
            Event::Blob(BlobEvent::Error(BlobError::PathTaken)) => return self.drop_intent(),
            Event::Blob(BlobEvent::Error(error)) => {
                return Err(PutObjectError::BlobWriteFailed(error));
            }
            _ => return Err(PutObjectError::InvalidOperationState),
        };
        self.written_location = Some(location.clone());

        // Check if the body was fully written
        if (self.config.request.content_length)
            .is_some_and(|expected| location.blob_size != expected)
        {
            return Err(PutObjectError::IncompleteBody);
        }

        for expected in &self.config.expected_checksums {
            let Some(actual) = location.hashes.get(expected.algorithm.hash_key()) else {
                return Err(PutObjectError::MissingExpectedChecksum(
                    expected.algorithm.s3_name(),
                ));
            };
            if actual != &expected.digest {
                return Err(PutObjectError::ChecksumMismatch(
                    expected.algorithm.s3_name(),
                ));
            }
        }
        if let Some(turn) = self.write_evidence() {
            return turn;
        }
        self.start_publish()
    }

    /// Hands the state the version transaction needs to a fresh child; the parent keeps its
    /// own copy, so a lost conflict runs the transaction again from the same state.
    fn publish_child(&mut self) -> Self {
        let set_hint = self.set_hint;
        let version_id = *self
            .version_id
            .get_or_insert_with(|| mint_version(set_hint));
        let mut child = Self::new(self.config.without_body());
        child.publishing = true;
        child.state = PutObjectState::CheckPurgeFence;
        child.version_id = Some(version_id);
        child.written_location = self.written_location.clone();
        child.adopted = self.adopted;
        child.create_only = self.create_only;
        child.expected_bucket = self.expected_bucket.clone();
        child.metadata = self.metadata.clone();
        child.rocrate_limits = self.rocrate_limits.clone();
        child.restrictions = self.restrictions.clone();
        child.inherited_policies = self.inherited_policies.clone();
        child.version_origin = self.version_origin.clone();
        child.gated_bucket = self.gated_bucket.clone();
        child.origin = self.origin;
        child.seal_plan = self.seal_plan;
        child.envelope = self.envelope.clone();
        child.plain = self.plain.clone();
        child.expected_head = self.expected_head;
        child.import_rows = self.import_rows.clone();
        child.keep_head = self.keep_head;
        child.written_intent = self.written_intent.clone();
        child.trail_scan = self.trail_scan.clone();
        child.rule_scan = self.rule_scan.clone();
        child.row_fence = self.row_fence.clone();
        child.set_hint = self.set_hint;
        child.binding = self.binding.clone();
        child.authority = self.authority.clone();
        child.version_epoch = self.version_epoch;
        child
    }

    fn start_publish(&mut self) -> Turn {
        let child = self.publish_child();
        self.call(PutObjectState::Publish, IoCall::child(child))
    }

    /// A lost conflict publishes again with the bytes already written; a refused publish
    /// rolls them back; one whose commit outcome is unknown leaves them to reconciliation.
    fn published(&mut self, result: IoResult) -> Turn {
        let error = match result.child::<PutObjectResult, PutObjectError>()? {
            Ok(result) => return Ok(Next::Done(result)),
            Err(error) => error,
        };
        self.state = PutObjectState::Error;
        let PutObjectError::StorageError(storage) = &error else {
            return Err(error);
        };
        if *storage == StorageError::TransactionConflict && self.conflicts < CONFLICT_RETRIES {
            self.conflicts += 1;
            return self.start_publish();
        }
        if !storage.proves_no_commit() {
            self.keep_written_blob(storage);
        }
        Err(error)
    }

    /// A commit whose outcome is unknown may already own these bytes, so they go to the
    /// reconciliation queue rather than being deleted: the committed blob location row is
    /// what decides their fate.
    fn keep_written_blob(&mut self, error: &StorageError) {
        let Some(location) = self.written_location.take() else {
            return;
        };
        if self.adopted {
            return;
        }
        warn!(
            event = "put_object.commit_outcome_unknown",
            backend = %location.backend,
            blob_size = location.blob_size,
            error = %error,
            "Queuing the written blob for reconciliation"
        );
        self.cleanup.set_release(location.ulid);
        self.reconcile_location = Some(location);
    }

    fn reconcile_work(&self, location: BackendLocation) -> Result<BlobCleanupWork, PutObjectError> {
        let Some(blake3) = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok())
        else {
            return Err(PutObjectError::MissingHash("blake3".to_string()));
        };
        Ok(BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::Blob {
                blake3,
                realm_id: self.config.realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
                announce: self.binding.is_none(),
            },
        })
    }

    /// A cleanup row written outside any transaction.
    fn cleanup_row(work: &BlobCleanupWork) -> Option<IoCall> {
        let key = match work {
            BlobCleanupWork::ReconcileWrite { location, .. } => {
                location.ulid.to_bytes().to_vec().into()
            }
            _ => Ulid::generate().to_bytes().to_vec().into(),
        };
        match work.to_bytes() {
            Ok(value) => Some(IoCall::Write(vec![(
                BLOB_CLEANUP_KEYSPACE.to_string(),
                key,
                value.into(),
            )])),
            Err(error) => {
                warn!(%error, "A put cleanup row could not be encoded");
                None
            }
        }
    }

    fn get_output(&self) -> Option<&BackendLocation> {
        self.output.as_ref()
    }

    fn get_written_location(&self) -> Option<&BackendLocation> {
        self.written_location.as_ref()
    }
}

impl Operation for PutObjectOperation {
    type Output = PutObjectResult;
    type Error = PutObjectError;

    fn first(&mut self) -> Turn {
        match self.publishing {
            true => self.read_publication(),
            false => self.begin(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match &self.state {
            PutObjectState::Init => self.first(),
            PutObjectState::ReadPreassignedVersion => {
                self.handle_preassigned_version(result.value()?)
            }
            PutObjectState::ReadPreassignedLocation => {
                self.handle_preassigned_location(result.value()?)
            }
            PutObjectState::ReadPreassignedCopy => self.handle_preassigned_copy(result.values()?),
            PutObjectState::ReadGateBucket => self.handle_gate_bucket(result.value()?),
            PutObjectState::PolicyGate => self.gate_passed(result),
            PutObjectState::ReadRecord => self.record_checked(result.values()?),
            PutObjectState::CheckPurgeWrite => self.write_fence_checked(result.value()?),
            PutObjectState::ReadSealSettings => self.seal_settings_read(result.value()?),
            PutObjectState::ReadSealKey => self.seal_key_read(result.value()?),
            PutObjectState::ReadAbe => self.abe_read(result.values()?, false),
            PutObjectState::FenceAbe => self.abe_read(result.values()?, true),
            PutObjectState::WriteEnvelope => {
                result.done()?;
                self.write_copy_owner()
            }
            PutObjectState::WriteBlob => self.handle_write_finished(result.event()?),
            PutObjectState::Publish => self.published(result),
            PutObjectState::ReadPlainOwner => self.plain_owner(result.value()?),
            PutObjectState::StatPlain => self.plain_stat(result.event()?),
            PutObjectState::WritePlainRecords => {
                result.done()?;
                self.plain_written()
            }
            PutObjectState::RetireIntents => {
                result.done()?;
                self.finish_publish()
            }
            PutObjectState::ReadImportRule => self.import_rule_read(result.values()?),
            PutObjectState::WriteIntent => self.intent_written(result),
            PutObjectState::SyncIntent => self.intent_synced(result),
            PutObjectState::DropIntent => {
                result.done()?;
                self.call(PutObjectState::SyncDroppedIntent, IoCall::Sync)
            }
            PutObjectState::SyncDroppedIntent => {
                result.done()?;
                self.written_intent.clear();
                Err(PutObjectError::BlobWriteFailed(BlobError::PathTaken))
            }
            PutObjectState::ReadTrail => self.trail_read(result),
            PutObjectState::ReadRules => self.rules_read(result.page()?),
            PutObjectState::ReadReaders => self.readers_read(result.page()?),
            PutObjectState::WriteEvidence => self.evidence_written(result),
            PutObjectState::ReadHistory => self.history_read(result.value()?),
            PutObjectState::WriteCleanupRow => {
                result.done()?;
                self.finish_publish()
            }
            PutObjectState::CheckPurgeFence => self.publication_checked(result.values()?),
            PutObjectState::CheckBucket => self.handle_bucket_checked(result.values()?),
            PutObjectState::CheckSealFence => self.seal_fence_checked(result.value()?),
            PutObjectState::FenceBackend => self.handle_backend_fenced(result.value()?),
            PutObjectState::CheckHashLookup => self.hash_checked(result.value()?),
            PutObjectState::ReadDedupHead
            | PutObjectState::ReadDedupVersion
            | PutObjectState::ReadDedupLocation
            | PutObjectState::ReadDedupProvider
            | PutObjectState::ReadDedupClaim => self.dedup_read(result.value()?),
            PutObjectState::ReadObjectLookup => self.object_lookup_read(result.value()?),
            PutObjectState::ReadLivenessVersion => self.liveness_read(result.value()?),
            PutObjectState::WriteBlobHead => {
                result.done()?;
                self.write_path_index()
            }
            PutObjectState::WritePathIndex => {
                result.done()?;
                self.write_version()
            }
            PutObjectState::CreateVersionRecord => {
                result.done()?;
                self.store_envelope()
            }
            PutObjectState::WriteCopyOwner => {
                result.done()?;
                self.register_managed_copy()
            }
            PutObjectState::RegisterManagedCopy => {
                result.done()?;
                match self.keep_head {
                    true => self.charge_usage(),
                    false => self.write_obligation(),
                }
            }
            PutObjectState::WriteReplicationObligation => {
                result.done()?;
                self.charge_usage()
            }
            PutObjectState::EnforceQuota => self.handle_enforce_quota(result),
            PutObjectState::UpdateUsage => self.handle_usage_update(result),
            PutObjectState::RowFence => self.fence_answered(result),
            PutObjectState::Error => Err(PutObjectError::InvalidOperationState),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.publishing {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }

    /// Rolls the written bytes back, or leaves them to reconciliation after a publish whose
    /// commit outcome is unknown, and releases their reservation; the runner makes these
    /// calls in order before the run ends. The version transaction leaves all of it to its
    /// parent.
    fn abort(&mut self) -> Vec<IoCall> {
        if self.publishing {
            return Vec::new();
        }
        // The publish was cut off: its commit may have applied.
        if self.state == PutObjectState::Publish
            && !self.adopted
            && let Some(location) = self.written_location.take()
        {
            self.reconcile_location = Some(location);
        }
        let mut calls = Vec::new();
        if let Some(location) = self.reconcile_location.take() {
            let work = self.reconcile_work(location);
            calls.extend(work.ok().as_ref().and_then(Self::cleanup_row));
        }
        if let Some(location) = self.written_location.take()
            && !self.adopted
        {
            calls.push(IoCall::child(RollbackWrite::new(location)));
        }
        if let Some(id) = self.cleanup.take_release() {
            calls.push(Effect::Blob(BlobEffect::ReleaseReservation { id }).into());
        }
        calls
    }
}

mod dedup;
pub(crate) mod fenced;
mod plain;
#[path = "publish.rs"]
mod publish;
mod sharing;
mod trail;
pub(crate) use sharing::{ReaderScan, RuleScan};
pub(crate) use trail::{Trail, TrailScan, trail_ranges};

pub(crate) use plain::{
    PlainRecord, intent_deletes, intent_keys, intent_rows, owner_call, owner_read, owns_path,
    path_owned, same_name_path, version_row,
};

#[path = "abe.rs"]
pub(crate) mod abe;

#[cfg(test)]
#[path = "tests.rs"]
mod test;
