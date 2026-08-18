use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, blob_location_read, write_blob_head_effect,
    write_blob_location_effect, write_blob_version_effect,
};
use crate::blob::cleanup::PendingCleanup;
use crate::blob::managed_copy::{
    CopyRequest, ManagedCopyError, register_effect, serve_reads, split_serve_reads,
    validate_registration,
};
use crate::group_backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement_policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::replication::queue::write_live_replication_obligation_effect;
use crate::replication::util::dht_registration_effect;
use crate::usage_stats::{
    QuotaGate, QuotaGateError, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_usage_snapshot_publish_effect,
};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion,
    BucketInfo, CurrentVersionPointer, ManagedCopyKey, PathRestriction, PlacementPolicyError,
    PlacementPolicyRef, RealmId, RoCrateLimits, RoutingError, RoutingSnapshot, UsageDelta,
    VersionKey, VersionSourceBinding, WriteOwner, resolve_backend,
};
use aruna_core::types::{Effects, GroupId, NodeId, UserId};
use bytes::Bytes;
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::{Duration, UNIX_EPOCH};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum PutObjectState {
    Init,
    ReadPreassignedVersion,
    ReadPreassignedLocation,
    ReadPreassignedCopy,
    ReadGateBucket,
    PolicyGate,
    WriteBlob,
    CleanupFailedWrite,
    QueueCleanupRow,
    WriteCleanupRow,
    StartTransaction,
    CheckBucket,
    FenceBackend,
    CheckHashLookup,
    CreateBlobLocation,
    ReadObjectLookup,
    ReadLivenessVersion,
    WriteBlobHead,
    WriteHashPathIndex,
    CreateBlobVersionRecord,
    RegisterManagedCopy,
    WriteLiveReplicationObligation,
    EnforceQuota,
    QuotaRejectAbort,
    UpdateUsage,
    CommitTransaction,
    ReleaseReservation,
    RegisterBlobInDht,
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
    BlobWriteFailed(String),
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
    version_id: Option<Ulid>,
    written_location: Option<BackendLocation>,
    cleanup_location: Option<BackendLocation>,
    rollback_location: Option<BackendLocation>,
    release_id: Option<Ulid>,
    pending_cleanup: PendingCleanup,
    existing_pointer: Option<CurrentVersionPointer>,
    new_blob: bool,
    was_live: bool,
    usage_update: Option<UsageCounterUpdate>,
    quota_gate: Option<QuotaGate>,
    pending_error: Option<PutObjectError>,
    output: Option<Result<BackendLocation, PutObjectError>>,
    expected_bucket: Option<BucketInfo>,
    metadata: HashMap<String, String>,
    rocrate_limits: RoCrateLimits,
    restrictions: Option<Vec<PathRestriction>>,
    /// Refs sealed on the version record, reused verbatim by its registration.
    sealed_policies: Vec<PlacementPolicyRef>,
    /// Destination default, read inside the version transaction so an edit
    /// observed after streaming cannot commit a stale ref set.
    bucket_policies: Vec<PlacementPolicyRef>,
    inherited_policies: Vec<PlacementPolicyRef>,
    /// Destination facts of this node. Absent means no governed byte may be
    /// materialized here at all.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// What the gate decided on, re-read inside the version transaction.
    gated_bucket: Option<GatedBucket>,
    /// Refs of an existing preassigned version, checked against its registration
    /// before the replay may hand back its location.
    replay_policies: Vec<PlacementPolicyRef>,
    replay_location: Option<BackendLocation>,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        let version_id = config.preassigned_version_id;
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            txn_id: None,
            version_id,
            written_location: None,
            cleanup_location: None,
            rollback_location: None,
            release_id: None,
            pending_cleanup: PendingCleanup::default(),
            existing_pointer: None,
            new_blob: false,
            was_live: false,
            usage_update: None,
            quota_gate: None,
            pending_error: None,
            output: None,
            expected_bucket: None,
            metadata: HashMap::new(),
            rocrate_limits: RoCrateLimits::default(),
            restrictions: None,
            sealed_policies: Vec::new(),
            bucket_policies: Vec::new(),
            inherited_policies: Vec::new(),
            gate_context: None,
            gate: None,
            gated_bucket: None,
            replay_policies: Vec::new(),
            replay_location: None,
        }
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

    /// Subject generation the gate admitted this write under; zero for an
    /// ungoverned write, which no subject ever evaluated.
    fn sealed_subject(&self) -> u64 {
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
                .sealed_under(self.gate_context.as_ref(), !refs.is_empty()),
        );
        match write_gate(self.gate_context.as_ref(), &refs) {
            Ok(None) => self.write_blob(),
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
        match gate_decision(outcome.decision) {
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

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            // Only a client-sourced stream fault may become a client error; a
            // server-side write fault must stay retryable, never a bad digest.
            Event::Blob(BlobEvent::Error(BlobError::StreamFailed(message))) => {
                return self.cleanup_failed_write(PutObjectError::WriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, message })) => {
                self.written_location = Some(location);
                return self.cleanup_failed_write(PutObjectError::BlobWriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(error)) => {
                return self
                    .cleanup_failed_write(PutObjectError::BlobWriteFailed(error.to_string()));
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
            // Read unconditionally: the version snapshots the default this
            // transaction observes, not one read before the bytes streamed.
            self.state = PutObjectState::CheckBucket;
            smallvec![drift_reads(&self.config.request.bucket, self.txn_id)]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
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

    fn handle_hash_lookup_checked(&mut self, event: Event) -> Effects {
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

                if existing_location != written_location {
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

        let effect = match write_blob_location_effect(
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

    fn handle_object_lookup_read(&mut self, event: Event) -> Effects {
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

    fn handle_liveness_version_read(&mut self, event: Event) -> Effects {
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
        let effect = match write_blob_head_effect(&self.alias_context(), pointer, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
        };

        self.state = PutObjectState::WriteBlobHead;
        smallvec![effect]
    }

    fn handle_blob_location_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.create_object_lookup()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_head_written(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.write_hash_path_index()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn write_hash_path_index(&mut self) -> Effects {
        let Some(location) = self.get_output().cloned() else {
            return self.emit_error(PutObjectError::MissingOutput);
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.emit_error(PutObjectError::MissingHash("blake3".to_string()));
        };
        let effect = match add_hash_path_index_effect(
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
        self.state = PutObjectState::WriteHashPathIndex;
        smallvec![effect]
    }

    fn handle_hash_path_index_created(&mut self, event: Event) -> Effects {
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
            self.sealed_policies = version.placement_policies.clone();
            let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
                Ok(effect) => effect,
                Err(err) => return self.emit_error(PutObjectError::ConversionError(err)),
            };
            self.state = PutObjectState::CreateBlobVersionRecord;
            smallvec![effect]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_version_record_created(&mut self, event: Event) -> Effects {
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
            VersionKey::new(
                self.config.request.bucket.clone(),
                self.config.request.key.clone(),
                version_id,
            ),
            self.config.node_id,
            &location,
            &self.sealed_policies,
            self.sealed_subject(),
            version_id.timestamp_ms(),
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
            self.write_live_replication_obligation()
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn write_live_replication_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(PutObjectError::PutObjectFailed);
        };
        let effect = match write_live_replication_obligation_effect(
            self.config.node_id,
            AuthContext {
                user_id: self.config.user_id,
                realm_id: self.config.realm_id,
                path_restrictions: self.restrictions.clone(),
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
        self.state = PutObjectState::WriteLiveReplicationObligation;
        smallvec![effect]
    }

    fn handle_live_replication_obligation_written(&mut self, event: Event) -> Effects {
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

                // Enforce the hard group quota before the counters commit. Only a
                // positive logical-bytes delta can push a group over its ceiling;
                // deletes and zero-length writes are never gated.
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
                    self.pending_error = Some(PutObjectError::QuotaExceeded {
                        limit: gate.ceiling(),
                        usage: gate.projected_usage(),
                    });
                    self.reject_over_quota()
                } else {
                    self.start_usage_update(txn_id)
                }
            }
            Err(err) => {
                self.pending_error = Some(err.into());
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

    fn handle_quota_reject_abort(&mut self, event: Event) -> Effects {
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
                self.pending_error = Some(err.into());
                self.reject_over_quota()
            }
        }
    }

    fn write_cleanup_row(&mut self, txn_id: Ulid) -> Effects {
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
                    self.release_id = Some(id);
                    self.state = PutObjectState::ReleaseReservation;
                    smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
                } else {
                    self.register_blob_in_dht_or_continue()
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                if error.proves_no_commit() {
                    return self.cleanup_failed_write(PutObjectError::StorageError(error));
                }
                self.keep_written_blob(error)
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    /// A commit whose outcome is unknown may already own these bytes, so they go
    /// to the reconciliation queue rather than being deleted or dropped: the
    /// committed blob location row is what decides their fate.
    fn keep_written_blob(&mut self, error: StorageError) -> Effects {
        let Some(location) = self.written_location.take() else {
            return self.emit_error(error.into());
        };
        let release_id = location.ulid;
        warn!(
            event = "put_object.commit_outcome_unknown",
            backend = %location.backend,
            blob_size = location.blob_size,
            error = %error,
            "Queuing the written blob for reconciliation"
        );
        self.pending_error = Some(error.into());
        self.release_id = Some(release_id);
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

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Some(location) = self.get_output().cloned() else {
            return self.continue_after_dht_registration();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.continue_after_dht_registration();
        };
        self.state = PutObjectState::RegisterBlobInDht;
        match dht_registration_effect(
            blake3_hash,
            self.config.realm_id,
            self.config.node_id,
            &self.rocrate_limits,
        ) {
            Ok(effect) => smallvec![effect],
            Err(_) => self.continue_after_dht_registration(),
        }
    }

    fn handle_blob_registered_in_dht(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.continue_after_dht_registration(),
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn continue_after_dht_registration(&mut self) -> Effects {
        if let Some(location) = self.cleanup_location.take() {
            self.state = PutObjectState::CleanupDuplicate;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = PutObjectState::Finish;
            smallvec![schedule_usage_snapshot_publish_effect()]
        }
    }

    fn handle_duplicate_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = PutObjectState::Finish;
                smallvec![schedule_usage_snapshot_publish_effect()]
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn emit_finish(&mut self) -> Effects {
        self.state = PutObjectState::Finish;
        smallvec![]
    }

    fn cleanup_failed_write(&mut self, error: PutObjectError) -> Effects {
        self.pending_error = Some(error);
        self.rollback_written_blob()
    }

    fn handle_failed_write_cleanup(&mut self, event: Event) -> Effects {
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
        let Some(effect) = self.pending_cleanup.queue(work) else {
            return self.release_or_error();
        };
        self.state = PutObjectState::QueueCleanupRow;
        smallvec![effect]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                self.pending_cleanup.accepted();
                self.release_or_error()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                match self.pending_cleanup.retry(&error) {
                    Some(effect) => smallvec![effect],
                    None => self.finish_or_error(),
                }
            }
            _ => self.emit_error(PutObjectError::InvalidOperationState),
        }
    }

    fn release_or_error(&mut self) -> Effects {
        let Some(id) = self.release_id else {
            return self.finish_or_error();
        };
        self.state = PutObjectState::ReleaseReservation;
        smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
    }

    /// Only a request that already carries an error fails here: a durable
    /// commit whose reservation release was deferred still succeeds.
    fn finish_or_error(&mut self) -> Effects {
        if self.pending_error.is_some() {
            return self.emit_pending_error();
        }
        self.continue_after_dht_registration()
    }

    /// The commit is durable, so a refused release must not fail the request.
    /// The reconciliation row clears the reservation and registers the blob;
    /// a duplicate copy is deleted by the cleanup the commit already planned.
    fn defer_release(&mut self) -> Effects {
        let Some(id) = self.release_id.take() else {
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
            return self.continue_after_dht_registration();
        };
        self.queue_cleanup_work(work)
    }

    fn handle_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            if self.pending_error.is_none() {
                return self.defer_release();
            }
            return self.emit_error(PutObjectError::InvalidOperationState);
        };
        if self.release_id != Some(id) {
            return self.emit_error(PutObjectError::InvalidOperationState);
        }
        self.release_id = None;
        if self.pending_error.is_some() {
            self.emit_pending_error()
        } else {
            self.register_blob_in_dht_or_continue()
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.pending_error.take() else {
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
    type Output = Option<Result<PutObjectResult, PutObjectError>>;
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
            PutObjectState::WriteBlob => self.handle_write_finished(event),
            PutObjectState::CleanupFailedWrite => self.handle_failed_write_cleanup(event),
            PutObjectState::QueueCleanupRow => self.handle_cleanup_queued(event),
            PutObjectState::WriteCleanupRow => self.handle_cleanup_row(event),
            PutObjectState::StartTransaction => self.handle_transaction_started(event),
            PutObjectState::CheckBucket => self.handle_bucket_checked(event),
            PutObjectState::FenceBackend => self.handle_backend_fenced(event),
            PutObjectState::CheckHashLookup => self.handle_hash_lookup_checked(event),
            PutObjectState::CreateBlobLocation => self.handle_blob_location_created(event),
            PutObjectState::ReadObjectLookup => self.handle_object_lookup_read(event),
            PutObjectState::ReadLivenessVersion => self.handle_liveness_version_read(event),
            PutObjectState::WriteBlobHead => self.handle_blob_head_written(event),
            PutObjectState::WriteHashPathIndex => self.handle_hash_path_index_created(event),
            PutObjectState::CreateBlobVersionRecord => {
                self.handle_blob_version_record_created(event)
            }
            PutObjectState::RegisterManagedCopy => self.handle_copy_registered(event),
            PutObjectState::WriteLiveReplicationObligation => {
                self.handle_live_replication_obligation_written(event)
            }
            PutObjectState::EnforceQuota => self.handle_enforce_quota(event),
            PutObjectState::QuotaRejectAbort => self.handle_quota_reject_abort(event),
            PutObjectState::UpdateUsage => self.handle_usage_update(event),
            PutObjectState::CommitTransaction => self.handle_transaction_committed(event),
            PutObjectState::ReleaseReservation => self.handle_release(event),
            PutObjectState::RegisterBlobInDht => self.handle_blob_registered_in_dht(event),
            PutObjectState::CleanupDuplicate => self.handle_duplicate_cleanup(event),
            PutObjectState::Finish => self.emit_finish(),
            PutObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutObjectState::Finish | PutObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if PutObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(PutObjectError::PutObjectFailed);
        }
        Ok(self.output.map(|result| {
            result.and_then(|location| {
                self.version_id
                    .map(|version_id| PutObjectResult {
                        location,
                        version_id,
                    })
                    .ok_or(PutObjectError::PutObjectFailed)
            })
        }))
    }

    fn abort(&mut self) -> Effects {
        let mut actions: Effects = smallvec![];
        if let Some(location) = self.written_location.take() {
            actions.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            actions.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        actions
    }
}

#[cfg(test)]
mod routing_test {
    use super::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
    use crate::group_backends::BackendFenceError;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::{
        BackendCatalog, BackendLocation, BackendRef, GroupBackendKind, GroupRoutingInputs,
        GroupStorageBackend, PathRestriction, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule,
    };
    use aruna_core::types::TxnId;
    use std::collections::{BTreeSet, HashMap};
    use ulid::Ulid;

    /// Answers the pre-write bucket read with an absent bucket, which is the
    /// ungoverned path every routing test exercises.
    fn begin(operation: &mut PutObjectOperation) -> aruna_core::types::Effects {
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: None,
        }))
    }

    fn config(snapshot: RoutingSnapshot) -> PutObjectConfig {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            group_id: snapshot.group_id,
            realm_id,
            node_id: iroh::SecretKey::generate().public(),
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
            Ulid::generate(),
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
            permission: aruna_core::structs::Permission::WRITE,
        }];
        let mut operation = PutObjectOperation::new(config(snapshot()))
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
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
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
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
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

    fn disabled(backend_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: true,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::s3::put_object::{
        PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectState,
    };

    use crate::usage_stats::{QuotaGate, UsageCounterUpdate};
    use aruna_blob::blob::BlobHandler;
    use aruna_blob::blob::{BackendRegistry, NodeBackend};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::egress::EgressPolicy;
    use aruna_core::errors::{BlobError, StorageError};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DHT_KEYSPACE,
        HASH_PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey,
        BlobVersion, BucketInfo, CurrentVersionPointer, HashPathIndexKey, RealmId, UsageDelta,
        VersionKey,
    };
    use aruna_core::structs::{BackendCatalog, NodeRoutingRule, RoutingSnapshot, RoutingTarget};
    use aruna_net::dht::storage::decode_entries;
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::fs::{exists, read_to_string};
    use std::path::Path;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn count_files(path: &Path) -> usize {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .map(|path| if path.is_dir() { count_files(&path) } else { 1 })
            .sum()
    }

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };

        value
    }

    fn test_location(created_by: aruna_core::UserId) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at: std::time::SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::new(),
        }
    }

    fn put_config(
        realm_id: RealmId,
        group_id: Ulid,
        node_id: aruna_core::NodeId,
    ) -> PutObjectConfig {
        PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: None,
                body: None,
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: Some(1),
            routing: RoutingSnapshot::single(group_id),
        }
    }

    #[test]
    fn guard_allows_edit() {
        // A routing or CORS edit is prospective policy, not a different bucket:
        // it must not discard a write that already landed.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = iroh::SecretKey::generate().public();
        let config = put_config(realm_id, group_id, node_id);
        let expected = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: config.user_id,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let edited = BucketInfo {
            storage_routing: vec![aruna_core::structs::StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target: aruna_core::structs::RoutingTarget::Class("cold".to_string()),
            }],
            ..expected.clone()
        };
        let mut op = PutObjectOperation::new(config).with_bucket_guard(expected);
        op.state = PutObjectState::StartTransaction;
        op.written_location = Some(test_location(op.config.user_id));
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"mybucket".to_vec().into(),
            value: Some(edited.to_bytes().unwrap().into()),
        }));

        assert_ne!(
            op.finalize(),
            Err(PutObjectError::StorageError(
                StorageError::TransactionConflict
            ))
        );
    }

    #[test]
    fn bucket_guard_rejects_recreate() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = iroh::SecretKey::generate().public();
        let config = put_config(realm_id, group_id, node_id);
        let expected = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: config.user_id,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let recreated = BucketInfo {
            created_at: std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..expected.clone()
        };
        let mut op = PutObjectOperation::new(config).with_bucket_guard(expected);
        op.state = PutObjectState::StartTransaction;
        op.written_location = Some(test_location(op.config.user_id));

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == S3_BUCKET_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"mybucket".to_vec().into(),
            value: Some(recreated.to_bytes().unwrap().into()),
        }));

        // The terminal state is complete, so this is the only chance to release
        // the transaction the guard read joined.
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::Blob(BlobEffect::Delete { .. }),
                    Effect::Storage(StorageEffect::AbortTransaction { .. }),
                ]
            ),
            "expected a rollback, got {effects:?}"
        );
        assert!(op.is_complete());
        assert!(op.step(Event::Blob(BlobEvent::DeleteFinished)).is_empty());
        assert_eq!(
            op.finalize(),
            Err(PutObjectError::StorageError(
                StorageError::TransactionConflict
            ))
        );
    }

    #[test]
    fn error_closes_transaction() {
        // Error is a complete state, so nothing else can release the transaction.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        let txn_id = Ulid::generate();
        op.state = PutObjectState::CheckHashLookup;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"unexpected".to_vec().into(),
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert!(op.txn_id.is_none());
        // Replaying the terminal state must not abort the same transaction twice.
        assert!(
            op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }))
                .is_empty()
        );
    }

    #[test]
    fn rejects_write_error() {
        // A rejected body stream (e.g. trailer checksum mismatch) must
        // surface WriteFailed instead of InvalidOperationState.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        op.state = PutObjectState::WriteBlob;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::StreamFailed(
            "checksum mismatch".to_string(),
        ))));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(op.finalize(), Err(PutObjectError::WriteFailed(_))));
    }

    #[test]
    fn rejects_server_write() {
        // A full or flapping disk must never be reported as a client bad digest.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        op.state = PutObjectState::WriteBlob;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
            "No space left on device".to_string(),
        ))));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(PutObjectError::BlobWriteFailed(_))
        ));
    }

    #[test]
    fn quota_gate_error_aborts_transaction_and_deletes_written_blob() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = iroh::SecretKey::generate().public();
        let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
        let txn_id = Ulid::generate();
        let location = test_location(op.config.user_id);

        op.state = PutObjectState::EnforceQuota;
        op.txn_id = Some(txn_id);
        op.written_location = Some(location.clone());
        op.quota_gate = Some(QuotaGate::new(1, 1, group_id, node_id));

        let effects = op.handle_enforce_quota(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id: observed }) if observed == txn_id
        ));
        assert_eq!(op.txn_id, None);

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));

        let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
            panic!("expected blob cleanup")
        };
        assert_eq!(deleted, &location);

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(crate::s3::put_object::PutObjectError::QuotaGateError(_))
        ));
    }

    #[test]
    fn usage_update_error_aborts_transaction_and_deletes_written_blob() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = iroh::SecretKey::generate().public();
        let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
        let txn_id = Ulid::generate();
        let location = test_location(op.config.user_id);

        op.state = PutObjectState::UpdateUsage;
        op.txn_id = Some(txn_id);
        op.written_location = Some(location.clone());
        op.usage_update = Some(UsageCounterUpdate::for_group(
            group_id,
            UsageDelta::default(),
        ));

        let effects = op.handle_usage_update(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id: observed }) if observed == txn_id
        ));
        assert_eq!(op.txn_id, None);

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));

        let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
            panic!("expected blob cleanup")
        };
        assert_eq!(deleted, &location);

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(crate::s3::put_object::PutObjectError::UsageUpdateError(_))
        ));
    }

    #[test]
    fn commit_transaction_conflict_deletes_written_blob_and_returns_conflict() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = iroh::SecretKey::generate().public();
        let mut op = PutObjectOperation::new(put_config(realm_id, group_id, node_id));
        let txn_id = Ulid::generate();
        let location = test_location(op.config.user_id);

        op.state = PutObjectState::CommitTransaction;
        op.txn_id = Some(txn_id);
        op.written_location = Some(location.clone());

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        let [Effect::Blob(BlobEffect::Delete { location: deleted })] = effects.as_slice() else {
            panic!("expected blob cleanup")
        };
        assert_eq!(deleted, &location);

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(crate::s3::put_object::PutObjectError::StorageError(
                StorageError::TransactionConflict
            ))
        ));
    }

    #[test]
    fn writes_before_commit() {
        // The reconciliation row must commit atomically with metadata ownership.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
        let txn_id = Ulid::generate();
        let mut location = test_location(op.config.user_id);
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        op.txn_id = Some(txn_id);
        op.written_location = Some(location.clone());

        let effects = op.write_cleanup_row(txn_id);
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                txn_id: observed,
                value,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a transactional reconciliation row, got {effects:?}")
        };
        assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
        assert_eq!(key.as_ref(), location.ulid.to_bytes());
        assert_eq!(*observed, Some(txn_id));
        assert!(matches!(
            super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            super::BlobCleanupWork::ReconcileWrite {
                location: observed,
                owner: super::WriteOwner::Blob { blake3, .. },
            } if blake3 == [7u8; 32] && observed == location
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: observed })]
                if *observed == txn_id
        ));
    }

    #[test]
    fn release_after_commit() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        let location = test_location(op.config.user_id);
        let id = location.ulid;
        op.state = PutObjectState::CommitTransaction;
        op.written_location = Some(location);

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::generate(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation { id })]
        );
        assert_eq!(op.state, PutObjectState::ReleaseReservation);

        let effects = op.step(Event::Blob(BlobEvent::ReservationReleased { id }));
        assert_eq!(op.state, PutObjectState::Finish);
        assert_eq!(effects.len(), 1);
        assert!(
            op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
                .is_empty()
        );
    }

    #[test]
    fn release_failure_succeeds() {
        // A refused release after a durable commit hands the reservation to the
        // cleanup queue; the client must not be told its committed write failed.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        let mut location = test_location(op.config.user_id);
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        let id = location.ulid;
        op.version_id = Some(Ulid::generate());
        op.state = PutObjectState::CommitTransaction;
        op.written_location = Some(location.clone());
        op.output = Some(Ok(location.clone()));

        op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::generate(),
        }));
        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
            "release refused".to_string(),
        ))));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a reconciliation row, got {effects:?}")
        };
        assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
        assert!(matches!(
            super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            super::BlobCleanupWork::ReconcileWrite { location: observed, .. }
                if observed.ulid == id
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"k".to_vec().into(),
        }));
        assert_eq!(effects.len(), 1);
        assert_eq!(op.state, PutObjectState::Finish);
        assert!(matches!(
            op.finalize(),
            Ok(Some(Ok(result))) if result.location == location
        ));
    }

    #[test]
    fn release_on_failure() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        let mut location = test_location(op.config.user_id);
        location.hashes.insert(
            aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
            vec![7u8; 32],
        );
        let id = location.ulid;
        op.pending_error = Some(PutObjectError::StorageError(StorageError::CommitFailed));
        op.release_id = Some(id);
        op.state = PutObjectState::QueueCleanupRow;
        assert!(
            op.pending_cleanup
                .queue(super::BlobCleanupWork::ReconcileWrite {
                    location,
                    owner: super::WriteOwner::Blob {
                        blake3: [7u8; 32],
                        realm_id,
                        ttl_ms: super::RoCrateLimits::default().holder_ttl_ms,
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
    fn closed_keeps_hold() {
        // A closed storage channel leaves the durable reservation for restart reconciliation.
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(put_config(
            realm_id,
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
        ));
        let location = test_location(op.config.user_id);
        let id = location.ulid;
        op.pending_error = Some(PutObjectError::StorageError(StorageError::ChannelClosed));
        op.release_id = Some(id);
        op.state = PutObjectState::QueueCleanupRow;
        assert!(
            op.pending_cleanup
                .queue(super::BlobCleanupWork::ReconcileReservation { location })
                .is_some()
        );

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::ChannelClosed,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.release_id, Some(id));
        assert!(op.pending_cleanup.retry(&StorageError::Timeout).is_none());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(PutObjectError::StorageError(StorageError::ChannelClosed))
        ));
    }

    #[test]
    fn unknown_keeps_blob() {
        // Only a proven refusal rolls the blob back; every other commit failure
        // may already have committed the version that names these bytes, so the
        // copy is handed to reconciliation instead of deleted or forgotten.
        for error in [
            StorageError::CommitFailed,
            StorageError::PersistError("journal".to_string()),
            StorageError::Timeout,
        ] {
            let realm_id = RealmId::from_bytes([1u8; 32]);
            let node_id = iroh::SecretKey::generate().public();
            let mut op = PutObjectOperation::new(put_config(realm_id, Ulid::generate(), node_id));
            op.state = PutObjectState::CommitTransaction;
            op.txn_id = Some(Ulid::generate());
            let mut location = test_location(op.config.user_id);
            location.hashes.insert(
                aruna_core::structs::checksum::HASH_BLAKE3.to_string(),
                vec![7u8; 32],
            );
            let release_id = location.ulid;
            op.written_location = Some(location.clone());

            let effects = op.step(Event::Storage(StorageEvent::Error {
                error: error.clone(),
            }));

            let [
                Effect::Storage(StorageEffect::Write {
                    key_space, value, ..
                }),
            ] = effects.as_slice()
            else {
                panic!("{error} must queue reconciliation, got {effects:?}")
            };
            assert_eq!(key_space, aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE);
            assert_eq!(
                super::BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
                super::BlobCleanupWork::ReconcileWrite {
                    location,
                    owner: super::WriteOwner::Blob {
                        blake3: [7u8; 32],
                        realm_id,
                        ttl_ms: super::RoCrateLimits::default().holder_ttl_ms,
                    },
                }
            );

            let effects = op.step(Event::Storage(StorageEvent::WriteResult {
                key: b"k".to_vec().into(),
            }));
            assert_eq!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::ReleaseReservation {
                    id: release_id
                })]
            );
            let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
                id: release_id,
            }));
            assert!(effects.is_empty());
            assert!(op.is_complete());
            assert!(matches!(
                op.finalize(),
                Err(PutObjectError::StorageError(observed)) if observed == error
            ));
        }
    }

    #[tokio::test]
    pub async fn test_put_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let data = b"hello, world!";
        let stream = tokio_util::io::ReaderStream::new(&data[..]);
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = net_handle.node_id();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let preassigned_version_id = Ulid::generate();
        let put_config = PutObjectConfig {
            user_id,
            group_id,
            realm_id,
            node_id,
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(stream)),
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(preassigned_version_id),
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        };
        let put_operation = PutObjectOperation::new(put_config);

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        // Jesus, Take the Wheel!
        let result = drive(put_operation, &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(exists(result.location.get_full_path().unwrap()).unwrap());
        assert_eq!(
            read_to_string(result.location.get_full_path().unwrap()).unwrap(),
            String::from_utf8_lossy(&data[..]).to_string()
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_location_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: BlobLocationKey::from_blake3(
                    result.location.get_blake3().unwrap(),
                    result.location.backend.clone(),
                )
                .unwrap()
                .to_bytes()
                .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob location entry");
        };
        assert_eq!(
            BackendLocation::from_bytes(blob_location_value.as_ref()).unwrap(),
            result.location.clone()
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_head_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("mybucket", "some-file.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob head entry");
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 1)
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_version_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("mybucket", "some-file.txt", result.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob version entry");
        };
        let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
        assert!(blob_version.is_materialized());
        assert_eq!(
            blob_version.blob_hash(),
            Some(&result.location.get_blake3().unwrap().try_into().unwrap())
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(hash_path_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
                key: HashPathIndexKey::new(
                    result.location.get_blake3().unwrap().try_into().unwrap(),
                    result.version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    "some-file.txt",
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing hash path index entry");
        };
        assert!(hash_path_value.is_empty());

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(dht_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: DHT_KEYSPACE.to_string(),
                key: result.location.get_blake3().unwrap().to_vec().into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing DHT blob registration");
        };
        let entries = decode_entries(dht_value.as_ref()).expect("decode DHT entries");
        assert!(entries.iter().any(|entry| {
            entry.realm_id == realm_id
                && entry.publisher == context.net_handle.as_ref().unwrap().node_id()
                && entry.value.is_empty()
        }));

        let retry_data = b"different content";
        let retry = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "some-file.txt".to_string(),
                    content_length: Some(retry_data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &retry_data[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: Some(preassigned_version_id),
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(retry, result);
        assert_eq!(
            read_to_string(retry.location.get_full_path().unwrap()).unwrap(),
            String::from_utf8_lossy(&data[..]).to_string()
        );

        let Event::Storage(StorageEvent::ReadResult {
            value: Some(blob_head_value),
            ..
        }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("mybucket", "some-file.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing blob head entry");
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 1)
        );
    }

    #[tokio::test]
    pub async fn test_put_object_dedup() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let data = b"hello, world!";
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let first = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "first.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let second = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "second.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(first.location, second.location);
        assert_eq!(count_files(Path::new(&blob_root)), 1);
        let blob_hash: [u8; 32] = first.location.get_blake3().unwrap().try_into().unwrap();

        let location_key =
            BlobLocationKey::new(blob_hash, first.location.backend.clone()).to_bytes();
        let blob_location_value = read_value(&context, BLOB_LOCATIONS_KEYSPACE, location_key)
            .await
            .expect("missing blob location entry");
        assert_eq!(
            BackendLocation::from_bytes(blob_location_value.as_ref()).unwrap(),
            first.location.clone()
        );

        for key in ["first.txt", "second.txt"] {
            let expected_version_id = if key == "first.txt" {
                first.version_id
            } else {
                second.version_id
            };

            let blob_head_value = read_value(
                &context,
                BLOB_HEAD_KEYSPACE,
                BlobHeadKey::new("mybucket", key).to_bytes().unwrap(),
            )
            .await
            .expect("missing blob head entry");
            assert_eq!(
                CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
                CurrentVersionPointer::new_with_generation(expected_version_id, 1)
            );

            let blob_version_value = read_value(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("mybucket", key, expected_version_id)
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .expect("missing blob version entry");
            let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
            assert!(blob_version.is_materialized());
            assert_eq!(blob_version.blob_hash(), Some(&blob_hash));

            let hash_path_value = read_value(
                &context,
                HASH_PATHS_INDEX_KEYSPACE,
                HashPathIndexKey::new(
                    blob_hash,
                    expected_version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    key,
                )
                .to_bytes()
                .unwrap(),
            )
            .await
            .expect("missing hash path index entry");
            assert!(hash_path_value.is_empty());
        }
    }

    /// Two filesystem node backends with distinct roots: enough to prove that a
    /// routed write never adopts a copy sitting on the other backend.
    async fn setup_two_backends(temp_root: &str) -> (DriverContext, String, String) {
        let hot_root = format!("{temp_root}/hot");
        let cold_root = format!("{temp_root}/cold");
        std::fs::create_dir_all(&hot_root).unwrap();
        std::fs::create_dir_all(&cold_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();

        let backend = |root: &str, prefix: &str, class: Option<String>| {
            std::sync::Arc::new(NodeBackend::new(
                BackendConfig {
                    backend_type: Backend::FileSystem,
                    bucket_prefix: Some(prefix.to_string()),
                    max_bucket_size: Some(100_000),
                    multipart_bucket: Some(format!("{prefix}parts")),
                    root: root.to_string(),
                    service_config: HashMap::new(),
                    timeouts: Default::default(),
                },
                class,
            ))
        };
        let mut backends = std::collections::BTreeMap::new();
        backends.insert("default".to_string(), backend(&hot_root, "hot-", None));
        backends.insert(
            "cold".to_string(),
            backend(&cold_root, "cold-", Some("cold".to_string())),
        );
        let registry = BackendRegistry::new(backends, "default".to_string()).unwrap();
        let blob_handle = BlobHandler::with_registry(
            registry,
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();

        (
            DriverContext {
                storage_handle,
                net_handle: Some(net_handle),
                blob_handle: Some(blob_handle),
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
            hot_root,
            cold_root,
        )
    }

    fn archive_routing(group_id: Ulid) -> RoutingSnapshot {
        let catalog = BackendCatalog::new("default")
            .with_backend("default", None)
            .with_backend("cold", Some("cold".to_string()));
        RoutingSnapshot::new(group_id, catalog).with_node_rules(vec![NodeRoutingRule {
            group: None,
            bucket: None,
            key_prefix: Some("archive/".to_string()),
            target: RoutingTarget::Class("cold".to_string()),
        }])
    }

    async fn put_routed(
        context: &DriverContext,
        group_id: Ulid,
        realm_id: RealmId,
        key: &str,
        data: &'static [u8],
    ) -> super::PutObjectResult {
        drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                group_id,
                realm_id,
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: key.to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: archive_routing(group_id),
            }),
            context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn dedup_per_backend() {
        // Identical bytes routed to two backends must keep one copy on each.
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let (context, hot_root, cold_root) = setup_two_backends(temp_root).await;

        let data = b"identical bytes";
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();

        let hot = put_routed(&context, group_id, realm_id, "hot.txt", data).await;
        let cold = put_routed(&context, group_id, realm_id, "archive/cold.txt", data).await;

        assert_eq!(hot.location.backend, BackendRef::node_default());
        assert_eq!(cold.location.backend, BackendRef::Node("cold".to_string()));
        assert_eq!(count_files(Path::new(&hot_root)), 1);
        assert_eq!(count_files(Path::new(&cold_root)), 1);

        let hash: [u8; 32] = hot.location.get_blake3().unwrap().try_into().unwrap();
        assert_eq!(cold.location.get_blake3().unwrap(), hash);

        for (key, result) in [("hot.txt", &hot), ("archive/cold.txt", &cold)] {
            let version_value = read_value(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("mybucket", key, result.version_id)
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .expect("missing blob version entry");
            let version = BlobVersion::from_bytes(version_value.as_ref()).unwrap();
            assert_eq!(version.blob_backend(), Some(&result.location.backend));

            let location_value = read_value(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                version.location_key().unwrap().to_bytes(),
            )
            .await
            .expect("missing blob location entry");
            assert_eq!(
                BackendLocation::from_bytes(location_value.as_ref()).unwrap(),
                result.location
            );
            assert!(exists(result.location.get_full_path().unwrap()).unwrap());
        }
    }

    #[tokio::test]
    async fn dedup_repeats_backend() {
        // A rewrite onto the same backend must still adopt the stored copy.
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let (context, _hot_root, cold_root) = setup_two_backends(temp_root).await;

        let data = b"identical bytes";
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();

        let first = put_routed(&context, group_id, realm_id, "archive/one.txt", data).await;
        let second = put_routed(&context, group_id, realm_id, "archive/two.txt", data).await;

        assert_eq!(first.location, second.location);
        assert_eq!(count_files(Path::new(&cold_root)), 1);
    }

    #[tokio::test]
    async fn delete_keeps_copy() {
        // Deleting one object must leave the twin copy on the other backend.
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let (context, _hot_root, cold_root) = setup_two_backends(temp_root).await;

        let data = b"identical bytes";
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();

        let hot = put_routed(&context, group_id, realm_id, "hot.txt", data).await;
        let cold = put_routed(&context, group_id, realm_id, "archive/cold.txt", data).await;

        let deleted = drive(
            crate::s3::delete_object::DeleteObjectOperation::new(
                crate::s3::delete_object::DeleteObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "hot.txt".to_string(),
                    version_id: Some(hot.version_id),
                    group_id,
                    realm_id,
                    node_id: context.net_handle.as_ref().unwrap().node_id(),
                    deleted_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
                },
            ),
            &context,
        )
        .await
        .unwrap();
        assert!(deleted.is_some_and(|result| result.is_ok()));

        let location_value = read_value(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(
                cold.location.get_blake3().unwrap().try_into().unwrap(),
                cold.location.backend.clone(),
            )
            .to_bytes(),
        )
        .await
        .expect("cold copy was removed with the hot object");
        assert_eq!(
            BackendLocation::from_bytes(location_value.as_ref()).unwrap(),
            cold.location
        );
        assert_eq!(count_files(Path::new(&cold_root)), 1);
    }

    #[test]
    fn put_object_current_pointer_generation_increments_from_existing_pointer() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let mut op = PutObjectOperation::new(PutObjectConfig {
            user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
            group_id: Ulid::generate(),
            realm_id,
            node_id: iroh::SecretKey::generate().public(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: None,
                body: None,
            },
            expected_checksums: vec![],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(Ulid::generate()),
        });
        let version_id = Ulid::generate();
        op.version_id = Some(version_id);
        op.output = Some(Ok(BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: op.config.user_id,
            created_at: std::time::SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::new(),
        }));
        op.txn_id = Some(Ulid::generate());
        let existing = CurrentVersionPointer::new_with_generation(Ulid::generate(), 4);

        let effects = op.handle_object_lookup_read(Event::Storage(StorageEvent::ReadResult {
            key: vec![0].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected liveness version read")
        };
        assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);

        let effects = op.handle_liveness_version_read(Event::Storage(StorageEvent::ReadResult {
            key: vec![0].into(),
            value: None,
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected current pointer write")
        };
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(version_id, 5)
        );
    }

    #[tokio::test]
    pub async fn test_put_object_overwrite_retains_historical_hash_path_index() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let realm_id = RealmId::from_bytes([1u8; 32]);
        let group_id = Ulid::generate();
        let node_id = context.net_handle.as_ref().unwrap().node_id();

        let first = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "same-key.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"first"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let second = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "same-key.txt".to_string(),
                    content_length: Some(6),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"second"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_ne!(first.location, second.location);
        assert_eq!(count_files(Path::new(&blob_root)), 2);

        let first_hash: [u8; 32] = first.location.get_blake3().unwrap().try_into().unwrap();
        let second_hash: [u8; 32] = second.location.get_blake3().unwrap().try_into().unwrap();

        let current_blob_head = read_value(
            &context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "same-key.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(current_blob_head.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(second.version_id, 2)
        );

        let historical_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                first_hash,
                first.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "same-key.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical hash path entry");
        assert!(historical_hash_path.is_empty());

        let new_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                second_hash,
                second.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "same-key.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing replacement hash path entry");
        assert!(new_hash_path.is_empty());

        let first_blob_version = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "same-key.txt", first.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing first blob version");
        assert_eq!(
            BlobVersion::from_bytes(first_blob_version.as_ref())
                .unwrap()
                .blob_hash(),
            Some(&first_hash)
        );

        let second_blob_version = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "same-key.txt", second.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing second blob version");
        assert_eq!(
            BlobVersion::from_bytes(second_blob_version.as_ref())
                .unwrap()
                .blob_hash(),
            Some(&second_hash)
        );
    }

    #[tokio::test]
    async fn test_put_object_checksum_mismatch_cleans_up_blob() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root.clone(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let data = b"hello, world!";
        let err = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id: aruna_core::UserId::local(
                    Ulid::generate(),
                    RealmId::from_bytes([1u8; 32]),
                ),
                group_id: Ulid::generate(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: context.net_handle.as_ref().unwrap().node_id(),
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "bad.txt".to_string(),
                    content_length: Some(data.len() as u64),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &data[..],
                    ))),
                },
                expected_checksums: vec![ExpectedChecksum {
                    algorithm: ChecksumAlgorithm::Sha256,
                    digest: vec![0; 32],
                }],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(Ulid::generate()),
            }),
            &context,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            crate::s3::put_object::PutObjectError::ChecksumMismatch("SHA256")
        ));
        assert_eq!(count_files(Path::new(&blob_root)), 0);
    }
}

/// F1 acceptance: no byte-materialization effect and no registration may be
/// emitted before the destination passed the shared placement gate.
#[cfg(test)]
mod gate_test {
    use super::{PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation};
    use crate::placement_policy::{GateContext, PolicyCacheEntry, PolicyGateError};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        BucketInfo, PlacementPolicy, PlacementPolicyRef, PlacementSelector, PlacementSubject,
        RealmId, RoutingSnapshot, VerifiedPolicy,
    };
    use aruna_core::types::{Effects, NodeId, UserId, Value};
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
        }
    }

    fn bucket(refs: Vec<PlacementPolicyRef>, generation: u64) -> Value {
        let info = BucketInfo {
            group_id: Ulid::from_bytes([2u8; 16]),
            created_at: UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([3u8; 16]), realm()),
            cors_configuration: None,
            replication: None,
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
        let document = crate::placement_policy::fixtures::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        let effects = operation.step(read(Some(ByteView::from(cached))));

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
        let effects = operation.step(read(Some(bucket(Vec::new(), 0))));
        assert!(materializes(&effects));
    }

    #[test]
    fn drift_aborts_commit() {
        // A default changed while the bytes streamed was never evaluated, so
        // the version must not commit the refs it would now inherit.
        let mut operation = operation("eu-west");
        operation.start();
        operation.step(read(Some(bucket(Vec::new(), 0))));
        operation.step(Event::Blob(aruna_core::events::BlobEvent::WriteFinished {
            location: location(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([7u8; 16]),
        }));
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
        let mut record = aruna_core::structs::NodeSubjectRecord::seed(
            crate::placement_policy::fixtures::subject(node(9), "eu-west"),
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
        let document = crate::placement_policy::fixtures::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(read(Some(ByteView::from(cached))));
        operation.step(Event::Blob(aruna_core::events::BlobEvent::WriteFinished {
            location: location(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([7u8; 16]),
        }));

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
        let mut config = aruna_core::structs::RealmConfigDocument::new(realm(), Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), aruna_core::structs::RealmNodeKind::Server);
        }
        let (config_value, auth_value) = crate::placement_policy::tests::realm_view(
            &config,
            crate::placement_policy::tests::admin_user(realm()),
        );
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
        let substituted =
            crate::placement_policy::fixtures::signed_document(realm(), &policy("us-east"), 9);
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

        let document = crate::placement_policy::fixtures::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        let effects = operation.step(read(Some(ByteView::from(cached))));

        assert!(!materializes(&effects));
        assert!(matches!(
            operation.finalize(),
            Err(PutObjectError::PolicyGate(PolicyGateError::Denied { .. }))
        ));
    }

    fn location() -> aruna_core::structs::BackendLocation {
        aruna_core::structs::BackendLocation {
            backend: aruna_core::structs::BackendRef::node_default(),
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
