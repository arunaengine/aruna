use crate::blob::managed_copy::{
    CopyRegistration, CopyRequest, ManagedCopyError, register_effect, serve_reads,
    split_serve_reads, validate_registration,
};
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
use crate::replication::dht_registration::dht_registration_effect;
use crate::replication::queue::build_live_obligation;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use crate::s3::write_cleanup::{CleanupStep, WriteCleanup};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
    CopyOrigin, CurrentVersionPointer, ManagedCopyKey, VersionKey, WriteOwner,
};
use aruna_core::structs::placement::placement_policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::routing::{RoutingError, RoutingSnapshot, resolve_backend};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::types::{Effects, GroupId};
use bytes::Bytes;
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

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
    CheckPurgeWrite,
    WriteBlob,
    CleanupFailedWrite,
    QueueCleanupRow,
    WriteCleanupRow,
    StartTransaction,
    CheckPurgeFence,
    CheckBucket,
    FenceBackend,
    CheckHashLookup,
    CreateBlobLocation,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WritePathIndex,
    CreateVersionRecord,
    RegisterManagedCopy,
    WriteReplicationObligation,
    EnforceQuota,
    QuotaRejectAbort,
    UpdateUsage,
    CommitTransaction,
    ReleaseReservation,
    RegisterBlobDht,
    CleanupDuplicate,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
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
    PurgeFence(#[from] PurgeFenceError),
    #[error("group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes")]
    QuotaExceeded { limit: u64, usage: u64 },
    #[error("Something went wrong ...")]
    PutObjectFailed,
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct PutObjectResult {
    pub location: BackendLocation,
    pub version_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct PutObjectOperation {
    state: PutObjectState,
    config: PutObjectConfig,
    txn_id: Option<Ulid>,
    conflicts: u8,
    version_id: Option<Ulid>,
    written_location: Option<BackendLocation>,
    cleanup_location: Option<BackendLocation>,
    rollback_location: Option<BackendLocation>,
    cleanup: WriteCleanup<PutObjectError>,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    /// A blob already on the resolved backend, taken instead of a stream.
    adopt: Option<BackendLocation>,
    /// The written location belongs to another version; never delete it.
    adopted: bool,
    was_live: bool,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    output: Option<Result<BackendLocation, PutObjectError>>,
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
    /// Destination details of this node. Absent means no governed byte may be
    /// materialized here at all.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// What the gate decided on, re-read inside the version transaction.
    gated_bucket: Option<GatedBucket>,
    /// Refs of an existing preassigned version, checked against its registration
    /// before the replay may hand back its location.
    replay_policies: Vec<PlacementPolicyRef>,
    replay_location: Option<BackendLocation>,
    /// Recorded on the registration so a reader learns why the copy is here.
    origin: CopyOrigin,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        let version_id = config.preassigned_version_id;
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            txn_id: None,
            conflicts: 0,
            version_id,
            written_location: None,
            cleanup_location: None,
            rollback_location: None,
            cleanup: WriteCleanup::default(),
            existing_pointer: None,
            new_blob: false,
            adopt: None,
            adopted: false,
            was_live: false,
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
            gate_context: None,
            gate: None,
            gated_bucket: None,
            replay_policies: Vec::new(),
            replay_location: None,
            origin: CopyOrigin::Write,
        }
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

    /// Refs a copy or derived write carries over from its source. They are
    /// unioned with the destination default, so a copy can only be at least as
    /// constrained as what it was copied from.
    pub fn with_inherited_policies(mut self, policies: Vec<PlacementPolicyRef>) -> Self {
        self.inherited_policies = policies;
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

    fn begin(&mut self) -> Effects {
        let Some(version_id) = self.config.preassigned_version_id else {
            return self.handle_init();
        };
        let key = match VersionKey::new(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
            version_id,
        )
        .to_bytes()
        {
            Ok(key) => key.into(),
            Err(error) => return self.emit_error(error.into()),
        };
        self.state = PutObjectState::ReadPreassignedVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn handle_preassigned_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.handle_init();
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return self.emit_error(error.into()),
        };
        let Some(location_key) = version.location_key() else {
            return self.emit_error(PutObjectError::InvalidPreassignedVersion);
        };
        self.replay_policies = version.placement_policies.clone();
        self.state = PutObjectState::ReadPreassignedLocation;
        smallvec![blob_location_read(&location_key, None)]
    }

    fn handle_preassigned_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            return self.emit_error(PutObjectError::InvalidPreassignedVersion);
        };
        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(error) => return self.emit_error(error.into()),
        };
        if self.replay_policies.is_empty() {
            return self.finish_replay(location);
        }
        // A governed replay may only hand back a copy this node registered.
        let Some(version_id) = self.version_id else {
            return self.emit_error(PutObjectError::InvalidPreassignedVersion);
        };
        let key = ManagedCopyKey::new(self.version_key(version_id), location.backend.clone());
        // The replay hands back bytes, so it answers the same question a serve
        // does: is this copy registered *and* may this node serve at all.
        let effect = match serve_reads(&key, None) {
            Ok(effect) => effect,
            Err(error) => return self.emit_error(error.into()),
        };
        self.replay_location = Some(location);
        self.state = PutObjectState::ReadPreassignedCopy;
        smallvec![effect]
    }

    fn handle_preassigned_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let (value, subject) = match split_serve_reads(values) {
            Ok(split) => split,
            Err(error) => return self.emit_error(error.into()),
        };
        let (Some(version_id), Some(location)) = (self.version_id, self.replay_location.take())
        else {
            return self.emit_error(PutObjectError::InvalidPreassignedVersion);
        };
        let key = ManagedCopyKey::new(self.version_key(version_id), location.backend.clone());
        match validate_registration(
            value.as_deref(),
            &CopyRequest {
                key: &key,
                node_id: Some(self.config.node_id),
                blake3: None,
                refs: &self.replay_policies,
                subject_generation: Some(subject.subject.generation),
            },
        ) {
            Ok(_) => self.finish_replay(location),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn finish_replay(&mut self, location: BackendLocation) -> Effects {
        self.output = Some(Ok(location));
        self.state = PutObjectState::Finish;
        smallvec![]
    }

    fn version_key(&self, version_id: Ulid) -> VersionKey {
        VersionKey::new(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
            version_id,
        )
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// The destination default is read before any byte moves, so the gate that
    /// admits this write sees the refs the version would actually carry.
    fn handle_init(&mut self) -> Effects {
        self.state = PutObjectState::ReadGateBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.config.request.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let bucket = match value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(bucket) => bucket,
            Err(error) => return self.emit_error(error.into()),
        };
        let refs = match union_refs(
            &GatedBucket::observe(bucket.as_ref()).policies,
            &self.inherited_policies,
        ) {
            Ok(refs) => refs,
            Err(error) => return self.emit_error(error.into()),
        };
        self.gated_bucket = Some(
            GatedBucket::observe(bucket.as_ref())
                .stored_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        let group_id = bucket
            .as_ref()
            .map_or(self.config.group_id, |bucket| bucket.group_id);
        match write_gate(self.gate_context.as_ref(), &refs, Some(group_id)) {
            Ok(None) => self.check_write_fence(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = PutObjectState::PolicyGate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_policy_gate(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.emit_error(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => self.check_write_fence(),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn check_write_fence(&mut self) -> Effects {
        self.state = PutObjectState::CheckPurgeWrite;
        smallvec![write_fence_read(&self.config.request.bucket, None)]
    }

    fn write_fence_checked(&mut self, event: Event) -> Effects {
        match check_write_fence(event, &self.config.request.bucket, &self.config.request.key) {
            Ok(()) => self.write_blob(),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn write_blob(&mut self) -> Effects {
        // Resolution runs before any bytes move; a failure is terminal.
        let resolved = match resolve_backend(
            &self.config.routing,
            &self.config.request.bucket,
            &self.config.request.key,
        ) {
            Ok(resolved) => resolved,
            Err(error) => return self.emit_error(error.into()),
        };
        if let Some(location) = self.config_adopt() {
            if location.backend != resolved.backend {
                return self.emit_error(PutObjectError::WriteFailed(
                    "the adopted blob sits on another backend".to_string(),
                ));
            }
            self.adopted = true;
            self.written_location = Some(location);
            self.state = PutObjectState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        }
        self.state = PutObjectState::WriteBlob;
        if let Some(blob) = self.config.request.body.take() {
            smallvec![Effect::Blob(BlobEffect::Write {
                bucket: self.config.request.bucket.clone(),
                key: self.config.request.key.clone(),
                resolved,
                created_by: self.config.user_id,
                blob
            })]
        } else {
            self.emit_error(PutObjectError::MissingBody)
        }
    }

    fn config_adopt(&mut self) -> Option<BackendLocation> {
        self.adopt.take()
    }

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            // Only a client-sourced stream fault may become a client error; a
            // server-side write fault must stay retryable, never a bad digest.
            Event::Blob(BlobEvent::Error(BlobError::StreamFailed(message))) => {
                return self.cleanup_failed_write(PutObjectError::WriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(error @ BlobError::WriteCleanup { .. })) => {
                if let BlobError::WriteCleanup { location, .. } = &error {
                    self.written_location = Some(location.clone());
                }
                return self.cleanup_failed_write(PutObjectError::BlobWriteFailed(error));
            }
            Event::Blob(BlobEvent::Error(error)) => {
                return self.cleanup_failed_write(PutObjectError::BlobWriteFailed(error));
            }
            _ => return self.emit_error(PutObjectError::InvalidOperationState),
        };
        self.written_location = Some(location.clone());

        // Check if the body was fully written
        if self
            .config
            .request
            .content_length
            .is_some_and(|expected| location.blob_size != expected)
        {
            return self.cleanup_failed_write(PutObjectError::IncompleteBody);
        }

        for expected in &self.config.expected_checksums {
            let Some(actual) = location.hashes.get(expected.algorithm.hash_key()) else {
                return self.cleanup_failed_write(PutObjectError::MissingExpectedChecksum(
                    expected.algorithm.s3_name(),
                ));
            };

            if actual != &expected.digest {
                return self.cleanup_failed_write(PutObjectError::ChecksumMismatch(
                    expected.algorithm.s3_name(),
                ));
            }
        }

        self.state = PutObjectState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            self.state = PutObjectState::CheckPurgeFence;
            smallvec![write_fence_read(&self.config.request.bucket, self.txn_id)]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) =
            check_write_fence(event, &self.config.request.bucket, &self.config.request.key)
        {
            return self.cleanup_failed_write(error.into());
        }
        // Read unconditionally: the version snapshots the default this
        // transaction observes, not one read before the bytes streamed.
        self.state = PutObjectState::CheckBucket;
        smallvec![drift_reads(&self.config.request.bucket, self.txn_id)]
    }

    fn start_fence(&mut self) -> Effects {
        let Some(location) = self.get_written_location() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        match fence_backend(&location.backend, self.txn_id) {
            Some(effect) => {
                self.state = PutObjectState::FenceBackend;
                smallvec![effect]
            }
            None => self.start_hash_lookup(),
        }
    }

    fn handle_backend_fenced(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.start_hash_lookup(),
            Err(error) => self.cleanup_failed_write(error.into()),
        }
    }

    fn handle_bucket_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        let (current, subject) = match split_drift_reads(values) {
            Ok(split) => split,
            Err(error) => return self.emit_error(error.into()),
        };
        if self.expected_bucket.is_some()
            && (current.as_ref().map(BucketInfo::identity)
                != self.expected_bucket.as_ref().map(BucketInfo::identity)
                || current
                    .as_ref()
                    .is_none_or(|bucket| bucket.group_id != self.config.group_id))
        {
            return self.emit_error(StorageError::TransactionConflict.into());
        }
        // The refs the version commits must be the refs the gate admitted: a
        // default changed while the bytes streamed was never evaluated.
        let observed = GatedBucket::observe(current.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return self.emit_error(PolicyGateError::Drift.into());
            }
            if let Err(error) = gated.check_subject(subject.as_ref()) {
                return self.emit_error(error.into());
            }
        }
        self.bucket_policies = observed.policies;
        self.start_fence()
    }

    /// Looks up only the copy on the backend this write resolved to, so
    /// identical content on another backend never overrides the placement.
    fn start_hash_lookup(&mut self) -> Effects {
        self.state = PutObjectState::CheckHashLookup;
        let Some(written_location) = self.get_written_location() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = written_location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };
        let key = match BlobLocationKey::from_blake3(blake3_hash, written_location.backend.clone())
        {
            Ok(key) => key,
            Err(error) => return self.emit_error(error.into()),
        };
        smallvec![blob_location_read(&key, self.txn_id)]
    }

    fn hash_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };

        let Some(written_location) = self.get_written_location().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };

        match value {
            Some(value) => {
                let existing_location = match BackendLocation::from_bytes(value.as_ref()) {
                    Ok(location) => location,
                    Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
                };

                if !self.adopted && existing_location != written_location {
                    self.cleanup_location = Some(written_location);
                }
                self.output = Some(Ok(existing_location));
                self.create_blob_location()
            }
            None => {
                self.new_blob = true;
                self.output = Some(Ok(written_location.clone()));
                self.create_blob_location()
            }
        }
    }

    fn create_blob_location(&mut self) -> Effects {
        self.state = PutObjectState::CreateBlobLocation;
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };

        let effect = match write_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err.into())),
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        smallvec![effect]
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.config.realm_id,
            self.config.group_id,
            self.config.node_id,
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
    }

    fn create_object_lookup(&mut self) -> Effects {
        let Some(_output) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };

        self.state = PutObjectState::ReadObjectLookup;
        let key = match BlobHeadKey::new(
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
        .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn object_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };

        let existing = match value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(existing) => existing,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        self.existing_pointer = existing;
        let existing_pointer = self.existing_pointer.clone();
        if let Some(pointer) = existing_pointer.as_ref() {
            let key = match VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                pointer.version_id,
            )
            .to_bytes()
            {
                Ok(key) => key.into(),
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };
            self.state = PutObjectState::ReadLivenessVersion;
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
            return self.emit_error(PutObjectError::InvalidOperationState);
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
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        let effect = match write_head_effect(&self.alias_context(), pointer, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        self.state = PutObjectState::WriteBlobHead;
        smallvec![effect]
    }

    fn location_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.create_object_lookup()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn head_written(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.write_path_index()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn write_path_index(&mut self) -> Effects {
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };
        let effect = match add_index_effect(
            &self.alias_context(),
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err.into())),
            },
            match self.version_id {
                Some(version_id) => version_id,
                None => return self.emit_error(PutObjectError::PutObjectFailed),
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };
        self.state = PutObjectState::WritePathIndex;
        smallvec![effect]
    }

    fn path_index_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            let Some(version_id) = self.version_id else {
                return self.emit_error(PutObjectError::PutObjectFailed);
            };
            let Some(output) = self.get_output().cloned() else {
                return self.emit_error(PutObjectError::MissingOutput);
            };
            let Some(blake3_hash) = output.get_blake3() else {
                return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
            };
            let version_created_at = UNIX_EPOCH + Duration::from_millis(version_id.timestamp_ms());
            let version = BlobVersion::materialized(
                match blake3_hash.try_into() {
                    Ok(hash) => hash,
                    Err(err) => {
                        return self.emit_error(PutObjectError::ConversionError(err.into()));
                    }
                },
                output.backend.clone(),
                version_created_at,
                output.created_by,
                self.config.version_source.clone(),
            )
            .with_metadata(self.metadata.clone());
            let version = match version.with_policies(self.effective_policies()) {
                Ok(version) => version,
                Err(err) => return self.emit_error(err.into()),
            };
            let version_key = VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                version_id,
            );
            self.stored_policies = version.placement_policies.clone();
            let effect = match write_version_effect(&version_key, &version, self.txn_id) {
                Ok(effect) => effect,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };
            self.state = PutObjectState::CreateVersionRecord;
            smallvec![effect]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn version_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.register_managed_copy()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    /// Joins the version transaction, so the copy becomes serveable exactly when
    /// the logical version does and never before.
    fn register_managed_copy(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let effect = match register_effect(
            CopyRegistration {
                version: VersionKey::new(
                    self.config.request.bucket.clone(),
                    self.config.request.key.clone(),
                    version_id,
                ),
                node_id: self.config.node_id,
                location: &location,
                policies: &self.stored_policies,
                origin: self.origin,
                subject_generation: self.stored_subject(),
                registered_at_ms: version_id.timestamp_ms(),
            },
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = PutObjectState::RegisterManagedCopy;
        smallvec![effect]
    }

    fn handle_copy_registered(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.write_obligation()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn write_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        let effect = match build_live_obligation(
            self.config.node_id,
            AuthContext {
                user_id: self.config.user_id,
                realm_id: self.config.realm_id,
                path_restrictions: self.restrictions.clone(),
                session: None,
            },
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
            version_id,
            false,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = PutObjectState::WriteReplicationObligation;
        smallvec![effect]
    }

    fn obligation_written(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            if let Some(txn_id) = self.txn_id {
                let Some(location) = self.get_output().cloned() else {
                    return self.emit_error(PutObjectError::MissingOutput);
                };
                let size = i128::from(location.blob_size);
                let group_delta = UsageDelta {
                    objects: if self.was_live { 0 } else { 1 },
                    logical_bytes: size,
                    ..Default::default()
                };
                let Some(stored) = StoredDelta::for_location(&location, self.new_blob) else {
                    return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
                };
                self.usage_update = Some(UsageCounterUpdate::with_stored(
                    self.config.group_id,
                    group_delta,
                    stored,
                ));

                // Enforce the hard quota before counters commit; only a
                // positive logical delta can breach it, so deletes pass.
                if let Some(ceiling) = self.config.quota_ceiling
                    && location.blob_size > 0
                {
                    let mut gate = QuotaGate::new_for_realm(
                        ceiling,
                        location.blob_size,
                        self.config.group_id,
                        self.config.node_id,
                        self.config.realm_id,
                    );
                    self.state = PutObjectState::EnforceQuota;
                    let effects = gate.start(txn_id);
                    self.quota_gate = Some(gate);
                    effects
                } else {
                    self.start_usage_update(txn_id)
                }
            } else {
                self.emit_error(PutObjectError::NoTransactionFound)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn start_usage_update(&mut self, txn_id: Ulid) -> Effects {
        self.state = PutObjectState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => self.emit_error(PutObjectError::PutObjectFailed),
        }
    }

    fn handle_enforce_quota(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                if gate.is_exceeded() {
                    self.cleanup.set_error(PutObjectError::QuotaExceeded {
                        limit: gate.ceiling(),
                        usage: gate.projected_usage(),
                    });
                    self.reject_over_quota()
                } else {
                    self.start_usage_update(txn_id)
                }
            }
            Err(err) => {
                self.cleanup.set_error(err.into());
                self.reject_over_quota()
            }
        }
    }

    /// Unwinds the pending write after quota/accounting failure: aborts the open
    /// transaction, then deletes the orphaned blob, before surfacing the error.
    fn reject_over_quota(&mut self) -> Effects {
        self.state = PutObjectState::QuotaRejectAbort;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => self.cleanup_orphan_blob(),
        }
    }

    fn abort_quota_reject(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => self.cleanup_orphan_blob(),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn cleanup_orphan_blob(&mut self) -> Effects {
        self.rollback_written_blob()
    }

    /// Takes the location: once its delete is queued the rollback in `abort`
    /// must not queue a second one. A copy stays behind so a delete that fails
    /// can still be handed to the durable cleanup queue.
    fn rollback_written_blob(&mut self) -> Effects {
        self.state = PutObjectState::CleanupFailedWrite;
        match self.written_location.take() {
            // An adopted blob belongs to another version and stays.
            Some(_) if self.adopted => self.emit_pending_error(),
            Some(location) => {
                self.rollback_location = Some(location.clone());
                smallvec![Effect::Blob(BlobEffect::Delete { location })]
            }
            None => self.emit_pending_error(),
        }
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(PutObjectError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.write_cleanup_row(txn_id),
            Err(err) => {
                self.cleanup.set_error(err.into());
                self.reject_over_quota()
            }
        }
    }

    fn write_cleanup_row(&mut self, txn_id: Ulid) -> Effects {
        if self.adopted {
            self.state = PutObjectState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        let Some(location) = self.written_location.clone() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let key = location.ulid.to_bytes().to_vec().into();
        let work = match self.reconcile_work(location) {
            Ok(work) => work,
            Err(error) => return self.emit_error(error),
        };
        let value = match work.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.emit_error(error.into()),
        };
        self.state = PutObjectState::WriteCleanupRow;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key,
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_cleanup_row(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.emit_error(PutObjectError::NoTransactionFound);
                };
                self.state = PutObjectState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                // The committed records own the blob now, so the rollback must
                // not still hold it.
                let release_id = self.written_location.take().map(|location| location.ulid);
                if let Some(id) = release_id {
                    self.cleanup.set_release(id);
                    self.state = PutObjectState::ReleaseReservation;
                    smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
                } else {
                    self.register_blob()
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                if matches!(error, StorageError::TransactionConflict)
                    && self.conflicts < CONFLICT_RETRIES
                {
                    return self.restart_after_conflict();
                }
                if error.proves_no_commit() {
                    return self.cleanup_failed_write(PutObjectError::StorageError(error));
                }
                self.keep_written_blob(error)
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    /// A refused commit discarded its writes, so only the metadata segment
    /// reopens: the blob is already on the backend and the version id is kept,
    /// so the retry stays the same write instead of re-reading the body.
    fn restart_after_conflict(&mut self) -> Effects {
        self.conflicts += 1;
        self.txn_id = None;
        self.existing_pointer = None;
        self.was_live = false;
        self.new_blob = false;
        self.cleanup_location = None;
        self.usage_update = None;
        self.quota_gate = None;
        self.output = None;
        self.state = PutObjectState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// A commit whose outcome is unknown may already own these bytes, so they go
    /// to the reconciliation queue rather than being deleted or dropped: the
    /// committed blob location row is what decides their fate.
    fn keep_written_blob(&mut self, error: StorageError) -> Effects {
        let Some(location) = self.written_location.take() else {
            return self.emit_error(error.into());
        };
        if self.adopted {
            return self.emit_error(error.into());
        }
        let release_id = location.ulid;
        warn!(
            event = "put_object.commit_outcome_unknown",
            backend = %location.backend,
            blob_size = location.blob_size,
            error = %error,
            "Queuing the written blob for reconciliation"
        );
        self.cleanup.set_error(error.into());
        self.cleanup.set_release(release_id);
        let work = match self.reconcile_work(location) {
            Ok(work) => work,
            Err(_) => return self.release_or_error(),
        };
        self.queue_cleanup_work(work)
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
            },
        })
    }

    fn register_blob(&mut self) -> Effects {
        let Some(location) = self.get_output().cloned() else {
            return self.continue_after_registration();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.continue_after_registration();
        };
        self.state = PutObjectState::RegisterBlobDht;
        match dht_registration_effect(blake3_hash, self.config.realm_id, &self.rocrate_limits) {
            Ok(effect) => smallvec![effect],
            Err(_) => self.continue_after_registration(),
        }
    }

    fn blob_registered(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.continue_after_registration(),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn continue_after_registration(&mut self) -> Effects {
        if let Some(location) = self.cleanup_location.take() {
            self.state = PutObjectState::CleanupDuplicate;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = PutObjectState::Finish;
            smallvec![schedule_snapshot_publish()]
        }
    }

    fn handle_duplicate_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = PutObjectState::Finish;
                smallvec![schedule_snapshot_publish()]
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn emit_finish(&mut self) -> Effects {
        self.state = PutObjectState::Finish;
        smallvec![]
    }

    fn cleanup_failed_write(&mut self, error: PutObjectError) -> Effects {
        self.cleanup.set_error(error);
        self.rollback_written_blob()
    }

    fn write_cleanup_failed(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) => {
                self.rollback_location = None;
                self.emit_pending_error()
            }
            // The bytes are still on the backend, and this operation is over;
            // only a queued delete can still reach them.
            Event::Blob(BlobEvent::Error(_)) => self.queue_rollback_delete(),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
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
        let Some(effect) = self.cleanup.queue(work) else {
            return self.release_or_error();
        };
        self.state = PutObjectState::QueueCleanupRow;
        smallvec![effect]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        match self.cleanup.handle_queued(event) {
            CleanupStep::Retry(effect) => smallvec![effect],
            CleanupStep::Accepted => self.release_or_error(),
            CleanupStep::Exhausted | CleanupStep::Closed => self.finish_or_error(),
            CleanupStep::Invalid => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn release_or_error(&mut self) -> Effects {
        let Some(effect) = self.cleanup.release_effect() else {
            return self.finish_or_error();
        };
        self.state = PutObjectState::ReleaseReservation;
        smallvec![effect]
    }

    /// Only a request that already carries an error fails here: a durable
    /// commit whose reservation release was deferred still succeeds.
    fn finish_or_error(&mut self) -> Effects {
        if self.cleanup.error_pending() {
            return self.emit_pending_error();
        }
        self.continue_after_registration()
    }

    /// The commit is durable, so a refused release must not fail the request.
    /// The reconciliation row clears the reservation and registers the blob;
    /// a duplicate copy is deleted by the cleanup the commit already planned.
    fn defer_release(&mut self) -> Effects {
        let Some(id) = self.cleanup.take_release() else {
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        warn!(
            event = "put_object.release_deferred",
            release_id = %id,
            "Queuing the blob reservation for reconciliation"
        );
        let Some(work) = self
            .get_output()
            .filter(|location| location.ulid == id)
            .cloned()
            .and_then(|location| self.reconcile_work(location).ok())
        else {
            return self.continue_after_registration();
        };
        self.queue_cleanup_work(work)
    }

    fn handle_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            if !self.cleanup.error_pending() {
                return self.defer_release();
            }
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        if self.cleanup.release_id() != Some(id) {
            return self.emit_error(PutObjectError::InvalidOperationState);
        }
        self.cleanup.clear_release();
        if self.cleanup.error_pending() {
            self.emit_pending_error()
        } else {
            self.register_blob()
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.cleanup.take_error() else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        self.emit_error(error)
    }

    /// The terminal state is complete, so the driver never calls `abort` for us;
    /// rolling back here is what keeps an open transaction from outliving the
    /// operation. `abort` takes what it releases, so it cannot run twice.
    fn emit_error(&mut self, error: PutObjectError) -> Effects {
        self.state = PutObjectState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn get_output(&self) -> Option<&BackendLocation> {
        self.output.as_ref()?.as_ref().ok()
    }

    fn get_written_location(&self) -> Option<&BackendLocation> {
        self.written_location.as_ref()
    }
}

impl Operation for PutObjectOperation {
    type Output = PutObjectResult;
    type Error = PutObjectError;

    fn start(&mut self) -> Effects {
        if self.state != PutObjectState::Init {
            self.emit_error(PutObjectError::InvalidOperationState)
        } else {
            self.begin()
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            PutObjectState::Init => self.begin(),
            PutObjectState::ReadPreassignedVersion => self.handle_preassigned_version(event),
            PutObjectState::ReadPreassignedLocation => self.handle_preassigned_location(event),
            PutObjectState::ReadPreassignedCopy => self.handle_preassigned_copy(event),
            PutObjectState::ReadGateBucket => self.handle_gate_bucket(event),
            PutObjectState::PolicyGate => self.handle_policy_gate(event),
            PutObjectState::CheckPurgeWrite => self.write_fence_checked(event),
            PutObjectState::WriteBlob => self.handle_write_finished(event),
            PutObjectState::CleanupFailedWrite => self.write_cleanup_failed(event),
            PutObjectState::QueueCleanupRow => self.handle_cleanup_queued(event),
            PutObjectState::WriteCleanupRow => self.handle_cleanup_row(event),
            PutObjectState::StartTransaction => self.handle_transaction_started(event),
            PutObjectState::CheckPurgeFence => self.fence_checked(event),
            PutObjectState::CheckBucket => self.handle_bucket_checked(event),
            PutObjectState::FenceBackend => self.handle_backend_fenced(event),
            PutObjectState::CheckHashLookup => self.hash_checked(event),
            PutObjectState::CreateBlobLocation => self.location_created(event),
            PutObjectState::ReadObjectLookup => self.object_lookup_read(event),
            PutObjectState::ReadLivenessVersion => self.liveness_read(event),
            PutObjectState::WriteBlobHead => self.head_written(event),
            PutObjectState::WritePathIndex => self.path_index_created(event),
            PutObjectState::CreateVersionRecord => self.version_created(event),
            PutObjectState::RegisterManagedCopy => self.handle_copy_registered(event),
            PutObjectState::WriteReplicationObligation => self.obligation_written(event),
            PutObjectState::EnforceQuota => self.handle_enforce_quota(event),
            PutObjectState::QuotaRejectAbort => self.abort_quota_reject(event),
            PutObjectState::UpdateUsage => self.handle_usage_update(event),
            PutObjectState::CommitTransaction => self.handle_transaction_committed(event),
            PutObjectState::ReleaseReservation => self.handle_release(event),
            PutObjectState::RegisterBlobDht => self.blob_registered(event),
            PutObjectState::CleanupDuplicate => self.handle_duplicate_cleanup(event),
            PutObjectState::Finish => self.emit_finish(),
            PutObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutObjectState::Finish | PutObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let location = match self.output {
            Some(Ok(location)) => location,
            Some(Err(error)) => return Err(error),
            None => return Err(PutObjectError::PutObjectFailed),
        };
        self.version_id
            .map(|version_id| PutObjectResult {
                location,
                version_id,
            })
            .ok_or(PutObjectError::PutObjectFailed)
    }

    fn abort(&mut self) -> Effects {
        let mut actions: Effects = smallvec![];
        if let Some(location) = self.written_location.take()
            && !self.adopted
        {
            actions.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            actions.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        actions
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
    use crate::groups::backends::BackendFenceError;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule,
    };
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::identity::auth::PathRestriction;
    use aruna_core::types::TxnId;
    use std::collections::{BTreeSet, HashMap};
    use ulid::Ulid;

    /// Answers the pre-write bucket read with an absent bucket, which is the
    /// ungoverned path every routing test exercises, then clears the fence.
    fn begin(operation: &mut PutObjectOperation) -> aruna_core::types::Effects {
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }));
        operation.step(fence_clear())
    }

    fn fence_clear() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: crate::s3::purge_fence::fence_key("bucket"),
            value: None,
        })
    }

    fn config(snapshot: RoutingSnapshot) -> PutObjectConfig {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 1), realm_id),
            group_id: snapshot.group_id,
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[65; 32]).public(),
            request: PutObjectInput {
                bucket: "bucket".to_string(),
                key: "archive/one".to_string(),
                content_length: Some(3),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                    &b"abc"[..],
                ))),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: snapshot,
        }
    }

    fn snapshot() -> RoutingSnapshot {
        RoutingSnapshot::new(
            Ulid::from_parts(2, 2),
            BackendCatalog::new("default")
                .with_backend("default", None)
                .with_backend("tape", Some("archive".to_string())),
        )
    }

    #[test]
    fn stamps_resolved_backend() {
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTarget::Class("archive".to_string()),
        }]);

        let effects = begin(&mut PutObjectOperation::new(config(snapshot)));

        let [Effect::Blob(BlobEffect::Write { resolved, .. })] = effects.as_slice() else {
            panic!("expected one blob write, got {effects:?}")
        };
        assert_eq!(resolved.backend, BackendRef::Node("tape".to_string()));
        assert_eq!(resolved.storage_class.as_deref(), Some("archive"));
    }

    #[test]
    fn obligation_keeps_restrictions() {
        // The durable repair record is what a lost enqueue replays, so a scoped
        // credential must stay scoped on it.
        let restrictions = vec![PathRestriction {
            pattern: "/realm/g/group/data/node/bucket/scoped/**".to_string(),
            permission: aruna_core::structs::identity::auth::Permission::WRITE,
        }];
        let mut operation = PutObjectOperation::new(config(snapshot()))
            .with_restrictions(Some(restrictions.clone()));
        operation.version_id = Some(Ulid::from_parts(3, 3));

        let effects = operation.write_obligation();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one obligation write, got {effects:?}")
        };
        let record = crate::replication::queue::LiveObligationRecord::from_bytes(value.as_ref())
            .expect("obligation decodes");
        assert_eq!(record.auth_context.path_restrictions, Some(restrictions));
    }

    #[test]
    fn missing_class_stamps() {
        // A class this node does not offer reroutes the write, never fails it.
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class("glacier".to_string()),
        }]);

        let effects = begin(&mut PutObjectOperation::new(config(snapshot)));

        let [Effect::Blob(BlobEffect::Write { resolved, .. })] = effects.as_slice() else {
            panic!("expected one blob write, got {effects:?}")
        };
        assert_eq!(resolved.backend, BackendRef::Node("default".to_string()));
        assert_eq!(resolved.storage_class, None);
    }

    #[test]
    fn unknown_backend_aborts() {
        // A named backend is binding: nothing may be written when it is gone.
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Node("ghost".to_string())),
        }]);

        let mut operation = PutObjectOperation::new(config(snapshot));
        let effects = begin(&mut operation);

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::RoutingFailed(RoutingError::UnknownBackend(
                _
            )))
        ));
    }

    #[test]
    fn refuses_disabled_backend() {
        // A disabled backend refuses writes, so this write must not commit a
        // location on it.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot()
            .with_group_inputs(GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            })
            .with_bucket_rules(Vec::new());
        let mut operation = PutObjectOperation::new(config(snapshot));
        begin(&mut operation);
        operation.step(Event::Blob(BlobEvent::WriteFinished {
            location: written(backend_id),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        operation.step(fence_clear());
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (b"bucket".to_vec().into(), None),
                (b"subject".to_vec().into(), None),
            ],
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled(backend_id).to_bytes().unwrap().into()),
        }));

        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::Delete { .. })]
            ),
            "expected the written blob to be rolled back, got {effects:?}"
        );

        let effects = operation.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ),
            "expected the transaction to abort, got {effects:?}"
        );
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::BackendFenceError(
                BackendFenceError::Unavailable
            ))
        ));
    }

    #[test]
    fn queues_failed_rollback() {
        // A rollback delete the backend refuses must become a durable cleanup
        // row, otherwise the written bytes are orphaned with nothing naming them.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot()
            .with_group_inputs(GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            })
            .with_bucket_rules(Vec::new());
        let mut operation = PutObjectOperation::new(config(snapshot));
        begin(&mut operation);
        operation.step(Event::Blob(BlobEvent::WriteFinished {
            location: written(backend_id),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        operation.step(fence_clear());
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (b"bucket".to_vec().into(), None),
                (b"subject".to_vec().into(), None),
            ],
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled(backend_id).to_bytes().unwrap().into()),
        }));

        let effects = operation.step(Event::Blob(BlobEvent::Error(
            aruna_core::errors::BlobError::UnknownBackend("gone".to_string()),
        )));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, txn_id, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected one cleanup row write, got {effects:?}")
        };
        assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
        // Outside the transaction: that transaction is about to be aborted.
        assert_eq!(*txn_id, None);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"k".to_vec().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
    }

    fn written(backend_id: Ulid) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::Group(backend_id),
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "bucket/object".to_string(),
            ulid: Ulid::from_bytes([6u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: aruna_core::UserId::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::new(),
        }
    }

    fn disabled(backend_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: true,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }
}

#[cfg(test)]
#[path = "put_tests.rs"]
mod test;

/// F1 acceptance: no byte-materialization effect and no registration may be
/// emitted before the destination passed the shared placement gate.
#[cfg(test)]
mod decision_tests {
    use super::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
    use crate::placement::policy::{GateContext, PolicyCacheEntry, PolicyGateError};
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::id::NodeId;
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::placement::placement_policy::{
        PlacementPolicy, PlacementPolicyRef, PlacementSelector, PlacementSubject, VerifiedPolicy,
    };
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::routing::RoutingSnapshot;
    use aruna_core::types::{Effects, Value};
    use byteview::ByteView;
    use std::collections::{BTreeMap, HashMap};
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    const BODY: &[u8] = b"payload";

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
    }

    /// A rule that admits exactly one location.
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

    /// The same rule under one group's ownership.
    fn owned_policy(location: &str, owner: Ulid) -> VerifiedPolicy {
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
        .expect("policy is valid")
        .owned_by(owner)
        .expect("owner is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    fn gate(location: &str) -> GateContext {
        GateContext {
            realm_id: realm(),
            subject: PlacementSubject {
                node_id: node(9),
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

    fn bucket(refs: Vec<PlacementPolicyRef>, generation: u64) -> Value {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: refs,
            placement_policy_generation: generation,
        };
        ByteView::from(info.to_bytes().expect("bucket encodes"))
    }

    fn read(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(Vec::new()),
            value,
        })
    }

    /// Answers a purge fence read with no fence held.
    fn fence_clear() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: crate::s3::purge_fence::fence_key("bucket"),
            value: None,
        })
    }

    fn operation(location: &str) -> PutObjectOperation {
        let group_id = Ulid::from_bytes([2u8; 16]);
        PutObjectOperation::new(PutObjectConfig {
            user_id: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            group_id,
            realm_id: realm(),
            node_id: node(9),
            request: PutObjectInput {
                bucket: "bucket".to_string(),
                key: "governed.txt".to_string(),
                content_length: Some(BODY.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(BODY))),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        })
        .with_gate(gate(location))
    }

    fn materializes(effects: &Effects) -> bool {
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::Blob(BlobEffect::Write { .. })))
    }

    #[test]
    fn denies_before_write() {
        // The rule admits another location, so nothing may be written at all.
        let rule = policy("us-east");
        let mut operation = operation("eu-west");
        assert!(!materializes(&operation.start()));
        let effects = operation.step(read(Some(bucket(vec![rule.policy_ref()], 1))));
        assert!(!materializes(&effects));
        let document = crate::tests::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(ByteView::from(cached))));
        let effects = operation.step(crate::tests::policy::authority(realm()));

        assert!(!materializes(&effects));
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Denied { .. }))
        ));
    }

    #[test]
    fn unresolved_blocks_write() {
        // A rule that cannot be obtained blocks; it is never read as a grant.
        let rule = policy("eu-west");
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(vec![rule.policy_ref()], 1))));
        let hint = PolicyCacheEntry::unavailable(1_000)
            .to_bytes()
            .expect("entry encodes");
        let effects = operation.step(read(Some(ByteView::from(hint))));

        assert!(!materializes(&effects));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(
                PolicyGateError::Unavailable { .. }
            ))
        ));
    }

    #[test]
    fn missing_subject_blocks() {
        // A node that advertises no subject may hold nothing governed.
        let mut config = operation("eu-west");
        config.gate_context = None;
        config.start();
        let effects = config.step(read(Some(bucket(
            vec![PlacementPolicyRef {
                policy_id: Ulid::from_bytes([1u8; 16]),
                digest: [4u8; 32],
            }],
            1,
        ))));

        assert!(!materializes(&effects));
        assert!(matches!(
            config.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::NoSubject))
        ));
    }

    #[test]
    fn ungoverned_skips_gate() {
        // An ungoverned write reaches the blob effect with no policy round trip.
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(Vec::new(), 0))));
        let effects = operation.step(fence_clear());
        assert!(materializes(&effects));
    }

    #[test]
    fn drift_aborts_commit() {
        // A default changed while the bytes streamed was never evaluated, so
        // the version must not commit the refs it would now inherit.
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(Vec::new(), 0))));
        operation.step(fence_clear());
        operation.step(Event::Blob(aruna_core::events::BlobEvent::WriteFinished {
            location: location(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([7u8; 16]),
        }));
        operation.step(fence_clear());
        let effects = operation.step(drift_read(
            Some(bucket(vec![policy("us-east").policy_ref()], 1)),
            None,
        ));

        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Storage(StorageEffect::AbortTransaction { .. })
        )));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Drift))
        ));
    }

    /// The exposing transaction re-reads the bucket and the local subject in
    /// one batch; both must still be what the gate decided on.
    fn drift_read(bucket: Option<Value>, subject: Option<Value>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (ByteView::from(Vec::new()), bucket),
                (ByteView::from(Vec::new()), subject),
            ],
        })
    }

    fn subject_row(generation: u64, blocked: bool) -> Value {
        let mut record = aruna_core::structs::placement::node_subject::NodeSubjectRecord::seed(
            crate::tests::policy::subject(node(9), "eu-west"),
        )
        .expect("subject is valid");
        record.subject.generation = generation;
        record.serving_blocked = blocked;
        record.policy_draining = blocked;
        ByteView::from(record.to_bytes().expect("record encodes"))
    }

    #[test]
    fn subject_advance_aborts() {
        // The subject that admitted the write moved on while the bytes
        // streamed, so the copy would commit refs nothing evaluated.
        let rule = policy("eu-west");
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(vec![rule.policy_ref()], 1))));
        let document = crate::tests::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(ByteView::from(cached))));
        operation.step(crate::tests::policy::authority(realm()));
        operation.step(fence_clear());
        operation.step(Event::Blob(aruna_core::events::BlobEvent::WriteFinished {
            location: location(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([7u8; 16]),
        }));
        operation.step(fence_clear());

        let effects = operation.step(drift_read(
            Some(bucket(vec![rule.policy_ref()], 1)),
            Some(subject_row(2, false)),
        ));

        assert!(effects.iter().any(|effect| matches!(
            effect,
            Effect::Storage(StorageEffect::AbortTransaction { .. })
        )));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Drift))
        ));
    }

    /// The realm view and the policy row a cache miss reads next.
    fn opened(policy_row: Option<Value>) -> Event {
        let mut config = aruna_core::structs::identity::realm::RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), aruna_core::structs::identity::realm::RealmNodeKind::Server);
        }
        let (config_value, auth_value) =
            crate::tests::policy::realm_view(&config, crate::tests::policy::admin_user(realm()));
        let key = ByteView::from(Vec::new());
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (key.clone(), policy_row),
                (key.clone(), Some(config_value)),
                (key, Some(auth_value)),
            ],
        })
    }

    #[test]
    fn digest_mismatch_blocks() {
        // The obtained document holds another definition under the same id, so
        // the write refuses instead of falling back to allow.
        let requested = policy("eu-west");
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(vec![requested.policy_ref()], 1))));
        operation.step(read(None));
        let substituted = crate::tests::policy::signed_document(realm(), &policy("us-east"), 9);
        let effects = operation.step(opened(Some(ByteView::from(
            substituted.to_bytes().expect("document encodes"),
        ))));

        assert!(!materializes(&effects));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Invalid))
        ));
    }

    #[test]
    fn inherited_ref_gates() {
        // Staging, imports and job outputs carry their source's refs into an
        // otherwise ungoverned destination; a sender may never drop one.
        let rule = policy("us-east");
        let mut operation = operation("eu-west").with_inherited_policies(vec![rule.policy_ref()]);
        operation.start();
        let effects = operation.step(read(Some(bucket(Vec::new(), 0))));
        assert!(!materializes(&effects));

        let document = crate::tests::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(ByteView::from(cached))));
        let effects = operation.step(crate::tests::policy::authority(realm()));

        assert!(!materializes(&effects));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Denied { .. }))
        ));
    }

    #[test]
    fn refuses_foreign_owner() {
        // A copy inherits a rule another group owns into this group's bucket;
        // the subject satisfies it, so only the ownership refuses the write.
        let rule = owned_policy("eu-west", Ulid::from_bytes([8u8; 16]));
        let mut operation = operation("eu-west").with_inherited_policies(vec![rule.policy_ref()]);
        operation.start();
        let effects = operation.step(read(Some(bucket(Vec::new(), 0))));
        assert!(!materializes(&effects));

        let document = crate::tests::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(ByteView::from(cached))));
        let effects = operation.step(crate::tests::policy::group_authority(
            realm(),
            Ulid::from_bytes([8u8; 16]),
        ));

        assert!(!materializes(&effects));
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(
                PolicyGateError::ForeignPolicy { .. }
            ))
        ));
    }

    fn location() -> aruna_core::structs::storage::blob::BackendLocation {
        aruna_core::structs::storage::blob::BackendLocation {
            backend: aruna_core::structs::storage::blob::BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "aruna".to_string(),
            backend_path: "objects/one".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: BODY.len() as u64,
            hashes: HashMap::from([("blake3".to_string(), vec![6u8; 32])]),
        }
    }
}
