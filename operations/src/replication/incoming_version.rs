use crate::blob::managed_copy::{CopyRegistration, ManagedCopyError, register_effect};
use crate::blob::records::{
    HeadAliasContext, add_index_effect, blob_location_read, build_transition_effects,
    write_location_effect, write_version_effect,
};
use crate::groups::backends::{BackendFenceError, check_fence, fence_backend};
use crate::groups::storage_routing::load_group_inputs;
use crate::node::usage_stats::{
    QuotaGate, QuotaGateError, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish,
};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, drift_reads, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::replication::dht_registration::dht_registration_effect;
use crate::replication::error::ReplicationError;
use crate::replication::protocol::{
    ReferenceAdvance, VersionReplicationManifest, VersionReplicationMessage,
};
use crate::replication::queue::{
    LiveObligationRecord, live_obligation_effect, schedule_blob_drain,
};
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    HASH_PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CopyOrigin, CurrentVersionPointer, ResolvedBackend, VersionKey, WriteOwner,
    bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingError, StorageRoutingRule, resolve_backend,
};
use aruna_core::structs::storage::multipart::MultipartObjectKey;
use aruna_core::structs::placement::placement_policy::PlacementPolicyRef;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::replication::{ReplicationItemKind, ReplicationNegotiationResult};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use std::collections::VecDeque;
use std::time::SystemTime;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

/// One state machine per inbound stream: every accepted event advances exactly
/// one state, `step` names its handler after the phase, and any failure after
/// the reply rejects, aborts the transaction, and deletes unowned bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionState {
    Init,
    ReadDestinationBucket,
    CreateDestinationBucket,
    LoadDestinationRouting,
    ReadExistingVersion,
    ReadReplacedBlob,
    ReadQuotaConfig,
    StartQuotaCheck,
    EnforceQuota,
    FinishQuotaCheck,
    ReadExistingBlob,
    PolicyGate,
    SendNegotiation,
    ReceiveBlob,
    StartTransaction,
    CheckPurgeFence,
    CheckDrift,
    VerifyReplaced,
    ReadReplacedMetadata,
    DeleteReplacedMetadata,
    WriteReclaimCandidate,
    FenceBackend,
    VerifyExistingBlob,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadCurrentVersion,
    ApplyHeadTransition,
    WriteBlobVersion,
    WriteMultipartMetadata,
    WriteLiveObligation,
    CheckCommitQuota,
    UpdateUsage,
    WriteCleanupRow,
    CommitTransaction,
    ReleaseReservation,
    ScheduleUsage,
    ScheduleLiveDrain,
    SendApplyRejected,
    AbortTransaction,
    CleanupReceivedBlob,
    RegisterBlobInDht,
    SendApplyComplete,
    CloseConnection,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingVersionError {
    #[error(transparent)]
    Policy(#[from] aruna_core::structs::placement::placement_policy::PlacementPolicyError),
    #[error(transparent)]
    PolicyGate(#[from] PolicyGateError),
    #[error(transparent)]
    ManagedCopy(#[from] ManagedCopyError),
    #[error("the placement gate finished without a pending negotiation")]
    GateNotPending,
    #[error(transparent)]
    RoutingFailed(#[from] RoutingError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error("Replication is only allowed within the same realm")]
    RealmMismatch,
    #[error("Destination bucket not found")]
    DestinationBucketNotFound,
    #[error("could not load the destination group's routing inputs: {0}")]
    RoutingInputsFailed(String),
    #[error("writer_access_denied")]
    WriterPermissionDenied,
    #[error("manifest_access_denied")]
    ManifestPermissionDenied,
    #[error("Replication hop limit exceeded")]
    HopLimitExceeded,
    #[error("Reference replication manifest is missing source metadata")]
    MissingReferenceMetadata,
    #[error("Reference replication manifest is missing source binding")]
    MissingReferenceSource,
    #[error("Reference replication manifest is missing its advance count")]
    MissingReferenceAdvanceCount,
    /// An offered directory resolves against the registration of the device that
    /// offers it, so a binding naming one is never valid on another node.
    #[error("Reference replication manifest names a device-local source")]
    LocalReferenceSource,
    #[error(transparent)]
    QuotaGateError(#[from] QuotaGateError),
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("quota")]
    QuotaExceeded,
    #[error("Current version manifest is missing current pointer generation")]
    MissingCurrentVersionGeneration,
    #[error("Destination current version not found")]
    CurrentVersionNotFound,
    #[error("Invalid reference advance")]
    InvalidReferenceAdvance,
    #[error("Materialized replication manifest is missing blob info")]
    MissingBlobInfo,
    #[error("Materialized replication manifest is missing local blob location")]
    MissingBlobLocation,
    #[error("Replicated blob hash does not match manifest")]
    BlobHashMismatch,
    #[error("Replicated blob size does not match manifest")]
    BlobSizeMismatch,
    #[error("Replicated blob storage flags do not match manifest")]
    BlobStorageFlagsMismatch,
    #[error("Existing blob copy changed before the version committed")]
    ExistingBlobChanged,
    #[error("Replaced multipart metadata exceeds the supported part limit")]
    MultipartMetadataOverflow,
    #[error("operation did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingVersionResult {
    pub applied: bool,
    pub group_id: Option<GroupId>,
}

/// The enqueue-phase clock. A function pointer compares by address with the
/// default lint noise, so equality is explicit; two operations that share the
/// production clock or the same test clock are equal.
#[derive(Clone, Copy, Debug)]
struct EnqueueClock(fn() -> SystemTime);

impl PartialEq for EnqueueClock {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::fn_addr_eq(self.0, other.0)
    }
}

impl Eq for EnqueueClock {}

impl EnqueueClock {
    fn now(self) -> SystemTime {
        (self.0)()
    }
}

/// The bytes accepted from the sender, with the reservation cleanup this node
/// still owes while the apply transaction is unresolved.
#[derive(Debug, PartialEq)]
struct ReceivedBlob {
    location: BackendLocation,
    /// Set until the apply commits; every abort before that must delete the
    /// copy, every failure after it must preserve the copy the commit owns.
    cleanup_on_abort: bool,
    /// The reconciliation row written inside the apply transaction for the
    /// case where the commit outcome is never learned, prepared once.
    reconciliation: Option<ReconciliationWork>,
}

impl ReceivedBlob {
    fn reserved(location: BackendLocation) -> Self {
        Self {
            location,
            cleanup_on_abort: true,
            reconciliation: None,
        }
    }

    /// The copy is already owned by this node, so no abort may delete it.
    #[cfg(test)]
    fn owned(location: BackendLocation) -> Self {
        Self {
            location,
            cleanup_on_abort: false,
            reconciliation: None,
        }
    }

    /// Takes the copy that must be deleted if this operation aborts now,
    /// clearing the outstanding cleanup.
    fn take_cleanup(&mut self) -> Option<BackendLocation> {
        if !self.cleanup_on_abort {
            return None;
        }
        self.cleanup_on_abort = false;
        Some(self.location.clone())
    }
}

/// The cleanup keyspace row that reconciles a received write whose commit
/// outcome was never learned.
#[derive(Debug, PartialEq)]
struct ReconciliationWork {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// The current-head transition the apply decided on. The hash rides with the
/// pointer because one transition consumes both, and only a pointer this
/// operation installs itself carries one.
#[derive(Debug, PartialEq)]
struct PendingHeadTransition {
    pointer: CurrentVersionPointer,
    current_hash: Option<[u8; 32]>,
}

#[derive(Debug, PartialEq)]
pub struct IncomingVersionOperation {
    state: IncomingVersionState,
    stream_id: Ulid,
    local_node_id: NodeId,
    /// The authenticated remote peer that pushed this stream, proven at Bao
    /// ingress. It is the accountable publisher, not the forgeable manifest.
    publisher_node_id: NodeId,
    local_realm_id: RealmId,
    manifest: VersionReplicationManifest,
    /// The clock the enqueue phase samples. A reclaim candidate is stamped when
    /// it is enqueued, so a long apply still gets its full grace window instead
    /// of inheriting an operation-start time that may already be stale.
    clock: EnqueueClock,
    txn_id: Option<Ulid>,
    destination_group_id: Option<GroupId>,
    /// The destination bucket's own rules, so this receiver routes its replica
    /// with the tenant's rules and its own class table.
    destination_rules: Vec<StorageRoutingRule>,
    destination_inputs: GroupRoutingInputs,
    create_attempted: bool,
    negotiation_result: Option<ReplicationNegotiationResult>,
    quota_ceiling: Option<u64>,
    quota_gate: Option<QuotaGate>,
    usage_update: Option<UsageCounterUpdate>,
    existing_blob_location: Option<BackendLocation>,
    replaced_version: Option<BlobVersion>,
    advance_version_exists: bool,
    received_blob: Option<ReceivedBlob>,
    existing_current_pointer: Option<CurrentVersionPointer>,
    object_delta: i128,
    replaced_logical_bytes: u64,
    replaced_reference_bytes: u64,
    pending_head: Option<PendingHeadTransition>,
    pending_head_transition_effects: VecDeque<Effect>,
    pending_version_effects: VecDeque<Effect>,
    release_id: Option<Ulid>,
    apply_committed: bool,
    output: Option<Result<IncomingVersionResult, IncomingVersionError>>,
    rocrate_limits: RoCrateLimits,
    routing: NodeRouting,
    /// Set when the destination backend is over its cap, which only refuses a
    /// negotiation that asks for the bytes.
    destination_full: Option<RoutingError>,
    manifest_policy: Option<String>,
    writer_policy: Option<String>,
    /// Destination details of this node. Absent means no governed replica may be
    /// materialized or registered here.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// Refs the gate admitted: the manifest's set unioned with what the local
    /// version already carried, so a sender cannot drop an inherited ref.
    gated_refs: Vec<PlacementPolicyRef>,
    /// Destination details the gate decided on, re-read inside the apply
    /// transaction so a default or subject change cannot expose a stale replica.
    gated_bucket: Option<GatedBucket>,
    pending_negotiation: Option<ReplicationNegotiationResult>,
}

impl IncomingVersionOperation {
    pub fn new(
        stream_id: Ulid,
        local_node_id: NodeId,
        local_realm_id: RealmId,
        manifest: VersionReplicationManifest,
    ) -> Self {
        Self {
            state: IncomingVersionState::Init,
            stream_id,
            local_node_id,
            // Defaults to the local node; the ingress handler overrides it with
            // the authenticated remote peer via `with_publisher_node`.
            publisher_node_id: local_node_id,
            local_realm_id,
            manifest,
            clock: EnqueueClock(SystemTime::now),
            txn_id: None,
            destination_group_id: None,
            destination_rules: Vec::new(),
            destination_inputs: GroupRoutingInputs::default(),
            create_attempted: false,
            negotiation_result: None,
            quota_ceiling: None,
            quota_gate: None,
            usage_update: None,
            existing_blob_location: None,
            replaced_version: None,
            advance_version_exists: false,
            received_blob: None,
            existing_current_pointer: None,
            object_delta: 0,
            replaced_logical_bytes: 0,
            replaced_reference_bytes: 0,
            pending_head: None,
            pending_head_transition_effects: VecDeque::new(),
            pending_version_effects: VecDeque::new(),
            release_id: None,
            apply_committed: false,
            output: None,
            rocrate_limits: RoCrateLimits::default(),
            routing: NodeRouting::default(),
            destination_full: None,
            manifest_policy: None,
            writer_policy: None,
            gate_context: None,
            gate: None,
            gated_refs: Vec::new(),
            gated_bucket: None,
            pending_negotiation: None,
        }
    }

    /// The destination this replica is evaluated against. Omitting it leaves
    /// ungoverned replication unchanged and fails every governed one closed.
    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    /// Node-local routing, so this receiver picks its own backend.
    pub fn with_routing(mut self, routing: NodeRouting) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// Binds the authenticated remote peer proven at Bao ingress as the
    /// accountable publisher of every version this stream writes.
    pub fn with_publisher_node(mut self, publisher_node_id: NodeId) -> Self {
        self.publisher_node_id = publisher_node_id;
        self
    }

    pub fn with_writer_policy(mut self, path: Option<String>) -> Self {
        self.writer_policy = path;
        self
    }

    pub fn with_manifest_policy(mut self, path: Option<String>) -> Self {
        self.manifest_policy = path;
        self
    }

    /// Replaces the clock the enqueue phase samples. Production keeps the
    /// default; tests fix it so a long apply can prove the candidate carries
    /// the enqueue time rather than the construction time.
    pub fn with_clock(mut self, clock: fn() -> SystemTime) -> Self {
        self.clock = EnqueueClock(clock);
        self
    }
}

impl Operation for IncomingVersionOperation {
    type Output = IncomingVersionResult;
    type Error = IncomingVersionError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.manifest.validate() {
            return self.reject_negotiation(error.into());
        }
        if self.manifest.reference_advance.is_some() && !self.valid_advance_manifest() {
            return self.reject_negotiation(IncomingVersionError::InvalidReferenceAdvance);
        }
        if self.is_reference_item()
            && let Err(error) = self.reference_version()
        {
            return self.reject_negotiation(error);
        }
        if self
            .manifest
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count > 4)
        {
            return self.reject_negotiation(IncomingVersionError::HopLimitExceeded);
        }
        if self.manifest.auth_context.realm_id != self.local_realm_id
            || self.manifest.auth_context.user_id.realm_id != self.local_realm_id
        {
            return self.reject_negotiation(IncomingVersionError::RealmMismatch);
        }
        if self.manifest.writer_auth_context.is_none() && self.manifest.reference_advance.is_none()
        {
            return self.reject_negotiation(IncomingVersionError::WriterPermissionDenied);
        }
        if self
            .manifest
            .writer_auth_context
            .as_ref()
            .is_some_and(|auth| {
                auth.realm_id != self.local_realm_id || auth.user_id.realm_id != self.local_realm_id
            })
        {
            return self.reject_negotiation(IncomingVersionError::RealmMismatch);
        }

        self.read_destination_bucket()
    }

    /// One accepted event per state, dispatched to the handler named
    /// after the accepted result. The phase order is documented on
    /// [`IncomingVersionState`].
    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            // Negotiation: decide and send the reply.
            IncomingVersionState::Init => self.start(),
            IncomingVersionState::ReadDestinationBucket => self.accept_destination_bucket(event),
            IncomingVersionState::CreateDestinationBucket => self.accept_bucket_created(event),
            IncomingVersionState::LoadDestinationRouting => self.accept_routing_loaded(event),
            IncomingVersionState::ReadExistingVersion => self.accept_existing_version(event),
            IncomingVersionState::ReadReplacedBlob => self.accept_replaced_blob(event),
            IncomingVersionState::ReadQuotaConfig => self.accept_quota_config(event),
            IncomingVersionState::StartQuotaCheck => self.accept_quota_transaction(event),
            IncomingVersionState::EnforceQuota => self.accept_quota_step(event),
            IncomingVersionState::FinishQuotaCheck => self.accept_quota_abort(event),
            IncomingVersionState::ReadExistingBlob => self.accept_existing_blob(event),
            IncomingVersionState::PolicyGate => self.accept_policy_gate(event),
            IncomingVersionState::SendNegotiation => self.accept_reply_sent(event),
            // Receiving: accept the bytes and open the apply.
            IncomingVersionState::ReceiveBlob => self.accept_blob_finish(event),
            IncomingVersionState::StartTransaction => self.accept_transaction_start(event),
            IncomingVersionState::CheckPurgeFence => self.accept_purge_fence(event),
            IncomingVersionState::CheckDrift => self.accept_drift_check(event),
            // Apply/commit: expose the version and settle ownership.
            IncomingVersionState::VerifyReplaced => self.accept_replaced_version(event),
            IncomingVersionState::ReadReplacedMetadata => self.accept_metadata_iterated(event),
            IncomingVersionState::DeleteReplacedMetadata => self.accept_metadata_deleted(event),
            IncomingVersionState::WriteReclaimCandidate => self.accept_reclaim_candidate(event),
            IncomingVersionState::FenceBackend => self.accept_backend_fence(event),
            IncomingVersionState::VerifyExistingBlob => self.accept_verified_blob(event),
            IncomingVersionState::WriteBlobLocation => self.accept_blob_location(event),
            IncomingVersionState::ReadObjectLookup => self.accept_object_lookup(event),
            IncomingVersionState::ReadCurrentVersion => self.accept_current_version(event),
            IncomingVersionState::ApplyHeadTransition => self.accept_head_progress(event),
            IncomingVersionState::WriteBlobVersion => self.accept_blob_version(event),
            IncomingVersionState::WriteMultipartMetadata => self.accept_multipart_write(event),
            IncomingVersionState::WriteLiveObligation => self.accept_live_obligation(event),
            IncomingVersionState::CheckCommitQuota => self.accept_commit_quota(event),
            IncomingVersionState::UpdateUsage => self.accept_usage_update(event),
            IncomingVersionState::WriteCleanupRow => self.accept_cleanup_row(event),
            IncomingVersionState::CommitTransaction => self.accept_transaction_commit(event),
            IncomingVersionState::ReleaseReservation => self.accept_reservation_release(event),
            IncomingVersionState::ScheduleUsage => self.accept_usage_schedule(event),
            IncomingVersionState::ScheduleLiveDrain => self.accept_live_drain(event),
            IncomingVersionState::RegisterBlobInDht => self.accept_blob_registration(event),
            IncomingVersionState::SendApplyComplete => self.accept_completion_sent(event),
            // Cleanup: reject, abort, delete and close.
            IncomingVersionState::SendApplyRejected => self.accept_apply_rejection(event),
            IncomingVersionState::AbortTransaction => self.accept_transaction_abort(event),
            IncomingVersionState::CleanupReceivedBlob => self.accept_blob_cleanup(event),
            IncomingVersionState::CloseConnection => self.accept_connection_close(event),
            IncomingVersionState::Finish => smallvec![],
            IncomingVersionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingVersionState::Finish | IncomingVersionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        // Only a terminal state carries an outcome; a premature finalize is an
        // explicit error rather than a successful default.
        if !matches!(
            self.state,
            IncomingVersionState::Finish | IncomingVersionState::Error
        ) {
            return Err(IncomingVersionError::NotFinished);
        }
        let default = self.result(self.apply_committed);
        match (self.state, self.output) {
            (_, Some(Ok(output))) => Ok(output),
            (_, Some(Err(error))) => Err(error),
            (IncomingVersionState::Finish, None) => Ok(default),
            (IncomingVersionState::Error, None) => Err(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
            _ => unreachable!("nonterminal states are rejected before the outcome is read"),
        }
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];

        let cleanup_location = match &self.state {
            // An unknown commit outcome must preserve the copy: the commit may
            // have landed, so the reservation is released instead of deleted.
            IncomingVersionState::CommitTransaction => {
                if let Some(received) = self.received_blob.as_mut() {
                    received.cleanup_on_abort = false;
                }
                None
            }
            _ => self
                .received_blob
                .as_mut()
                .and_then(ReceivedBlob::take_cleanup),
        };
        if let Some(location) = cleanup_location {
            effects.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects.push(Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        }));

        effects
    }
}

impl IncomingVersionOperation {
    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionState::Init => "Init",
            IncomingVersionState::ReadDestinationBucket => "ReadDestinationBucket",
            IncomingVersionState::CreateDestinationBucket => "CreateDestinationBucket",
            IncomingVersionState::LoadDestinationRouting => "LoadDestinationRouting",
            IncomingVersionState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionState::ReadReplacedBlob => "ReadReplacedBlob",
            IncomingVersionState::ReadQuotaConfig => "ReadQuotaConfig",
            IncomingVersionState::StartQuotaCheck => "StartQuotaCheck",
            IncomingVersionState::EnforceQuota => "EnforceQuota",
            IncomingVersionState::FinishQuotaCheck => "FinishQuotaCheck",
            IncomingVersionState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionState::PolicyGate => "PolicyGate",
            IncomingVersionState::SendNegotiation => "SendNegotiation",
            IncomingVersionState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionState::StartTransaction => "StartTransaction",
            IncomingVersionState::CheckPurgeFence => "CheckPurgeFence",
            IncomingVersionState::CheckDrift => "CheckDrift",
            IncomingVersionState::VerifyReplaced => "VerifyReplaced",
            IncomingVersionState::ReadReplacedMetadata => "ReadReplacedMetadata",
            IncomingVersionState::DeleteReplacedMetadata => "DeleteReplacedMetadata",
            IncomingVersionState::WriteReclaimCandidate => "WriteReclaimCandidate",
            IncomingVersionState::FenceBackend => "FenceBackend",
            IncomingVersionState::VerifyExistingBlob => "VerifyExistingBlob",
            IncomingVersionState::WriteBlobLocation => "WriteBlobLocation",
            IncomingVersionState::ReadObjectLookup => "ReadObjectLookup",
            IncomingVersionState::ReadCurrentVersion => "ReadCurrentVersion",
            IncomingVersionState::ApplyHeadTransition => "ApplyHeadTransition",
            IncomingVersionState::WriteBlobVersion => "WriteBlobVersion",
            IncomingVersionState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionState::WriteLiveObligation => "WriteLiveObligation",
            IncomingVersionState::CheckCommitQuota => "CheckCommitQuota",
            IncomingVersionState::UpdateUsage => "UpdateUsage",
            IncomingVersionState::WriteCleanupRow => "WriteCleanupRow",
            IncomingVersionState::CommitTransaction => "CommitTransaction",
            IncomingVersionState::ReleaseReservation => "ReleaseReservation",
            IncomingVersionState::ScheduleUsage => "ScheduleUsage",
            IncomingVersionState::ScheduleLiveDrain => "ScheduleLiveDrain",
            IncomingVersionState::SendApplyRejected => "SendApplyRejected",
            IncomingVersionState::AbortTransaction => "AbortTransaction",
            IncomingVersionState::CleanupReceivedBlob => "CleanupReceivedBlob",
            IncomingVersionState::RegisterBlobInDht => "RegisterBlobInDht",
            IncomingVersionState::SendApplyComplete => "SendApplyComplete",
            IncomingVersionState::CloseConnection => "CloseConnection",
            IncomingVersionState::Finish => "Finish",
            IncomingVersionState::Error => "Error",
        }
    }

    fn reject_negotiation(&mut self, err: IncomingVersionError) -> Effects {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            reason = %err,
            "Rejecting incoming version replication negotiation"
        );
        let reason = err.to_string();
        self.output = Some(Ok(self.result(false)));
        self.send_negotiation(ReplicationNegotiationResult::Rejected(reason))
    }

    fn result(&self, applied: bool) -> IncomingVersionResult {
        IncomingVersionResult {
            applied,
            group_id: self.destination_group_id,
        }
    }

    fn fail(&mut self, err: IncomingVersionError) -> Effects {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            state = %self.state_name(),
            error = %err,
            "Incoming version replication failed"
        );
        let should_reject = matches!(
            self.negotiation_result,
            Some(
                ReplicationNegotiationResult::NeedVersionOnly
                    | ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ) && !self.apply_committed
            && !matches!(
                self.state,
                IncomingVersionState::SendApplyRejected
                    | IncomingVersionState::AbortTransaction
                    | IncomingVersionState::CleanupReceivedBlob
                    | IncomingVersionState::CloseConnection
                    | IncomingVersionState::Error
            );
        self.output = Some(Err(err));
        if should_reject {
            self.send_apply_rejected()
        } else {
            self.state = IncomingVersionState::Error;
            self.abort()
        }
    }

    fn version_key_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        )
        .to_bytes()
    }

    fn target_authorization_path(&self, group_id: Ulid) -> String {
        if self.manifest.key.is_empty() {
            bucket_permission_path(
                self.local_realm_id,
                group_id,
                self.local_node_id,
                &self.manifest.bucket,
            )
        } else {
            object_permission_path(
                self.local_realm_id,
                group_id,
                self.local_node_id,
                &self.manifest.bucket,
                &self.manifest.key,
            )
        }
    }

    fn alias_context(&self) -> Result<HeadAliasContext, IncomingVersionError> {
        let Some(group_id) = self.destination_group_id else {
            return Err(IncomingVersionError::DestinationBucketNotFound);
        };

        Ok(HeadAliasContext::new(
            self.local_realm_id,
            group_id,
            self.local_node_id,
            self.manifest.bucket.clone(),
            self.manifest.key.clone(),
        ))
    }

    fn current_manifest_hash(&self) -> Option<[u8; 32]> {
        if !self.manifest.current_version
            || self.manifest.kind != ReplicationItemKind::Materialized
            || self.manifest.reference_intent
        {
            return None;
        }

        self.manifest.blob.as_ref().map(|blob| blob.hash)
    }

    fn is_reference_item(&self) -> bool {
        self.manifest.is_reference()
    }

    fn valid_advance_manifest(&self) -> bool {
        let Some(advance) = self.manifest.reference_advance.as_ref() else {
            return false;
        };
        advance.predecessor != self.manifest.version_id && self.manifest.validate().is_ok()
    }

    fn binding_continues(
        previous: &aruna_core::structs::execution::staging::VersionSourceBinding,
        incoming: &aruna_core::structs::execution::staging::VersionSourceBinding,
        advance: &ReferenceAdvance,
        version_id: Ulid,
    ) -> bool {
        if previous.descriptor.kind != aruna_core::structs::execution::source_connector::SourceConnectorKind::ArunaNative {
            return previous == incoming;
        }
        let previous_selector = format!("version:{}", advance.predecessor);
        let incoming_selector = format!("version:{version_id}");
        if previous.descriptor.version_selector.as_deref() != Some(previous_selector.as_str())
            || incoming.descriptor.version_selector.as_deref() != Some(incoming_selector.as_str())
        {
            return false;
        }

        let mut previous = previous.clone();
        let mut incoming = incoming.clone();
        previous.descriptor.version_selector = None;
        incoming.descriptor.version_selector = None;
        previous == incoming
    }

    fn validate_advance(&self, previous: &BlobVersion) -> Result<(), IncomingVersionError> {
        let Some(advance) = self.manifest.reference_advance.as_ref() else {
            return Ok(());
        };
        let incoming = self.reference_version()?;
        let (
            BlobVersionState::Reference {
                source: previous_source,
                advance_count: previous_count,
                ..
            },
            BlobVersionState::Reference {
                source: incoming_source,
                advance_count: incoming_count,
                ..
            },
        ) = (&previous.state, &incoming.state)
        else {
            return Err(IncomingVersionError::InvalidReferenceAdvance);
        };
        // Each advance mints exactly one successor, so the count must be
        // continuous: a gap or repeat would let the publisher reset the cap.
        if previous_count.checked_add(1) != Some(*incoming_count) {
            return Err(IncomingVersionError::InvalidReferenceAdvance);
        }
        if previous.published_by != Some(self.publisher_node_id)
            || previous.created_by != incoming.created_by
            || previous.metadata != incoming.metadata
            || !Self::binding_continues(
                previous_source,
                incoming_source,
                advance,
                self.manifest.version_id,
            )
        {
            return Err(IncomingVersionError::InvalidReferenceAdvance);
        }
        Ok(())
    }

    fn reference_version(&self) -> Result<BlobVersion, IncomingVersionError> {
        let source = self
            .manifest
            .source
            .clone()
            .ok_or(IncomingVersionError::MissingReferenceSource)?;
        if source.descriptor.kind == aruna_core::structs::execution::source_connector::SourceConnectorKind::LocalDirectory {
            return Err(IncomingVersionError::LocalReferenceSource);
        }
        let metadata = self
            .manifest
            .reference_metadata
            .clone()
            .ok_or(IncomingVersionError::MissingReferenceMetadata)?;
        let advance_count = self
            .manifest
            .reference_advance_count
            .ok_or(IncomingVersionError::MissingReferenceAdvanceCount)?;
        Ok(BlobVersion::reference(
            source,
            metadata,
            self.manifest.created_at,
            self.manifest.created_by,
            self.manifest.created_at,
        )
        .with_metadata(self.manifest.metadata.clone())
        .with_advance_count(advance_count)
        .with_publisher(self.publisher_node_id)
        .with_policies(self.manifest.placement_policies.clone())?)
    }

    fn incoming_logical_bytes(&self) -> Result<u64, IncomingVersionError> {
        if self.is_reference_item() {
            return self
                .manifest
                .reference_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
                .ok_or(IncomingVersionError::MissingReferenceMetadata);
        }

        self.manifest
            .blob
            .as_ref()
            .map(|blob| blob.size)
            .ok_or(IncomingVersionError::MissingBlobInfo)
    }

    fn prepare_head_transition(&mut self) -> Effects {
        let context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.fail(err),
        };
        let (pointer, current_hash) = match self.pending_head.take() {
            Some(pending) => (Some(pending.pointer), pending.current_hash),
            None => (None, None),
        };
        let effects = match build_transition_effects(&context, pointer, current_hash, self.txn_id) {
            Ok(effects) => effects,
            Err(err) => return self.fail(err.into()),
        };

        self.pending_head_transition_effects = effects.into_iter().collect();
        self.state = IncomingVersionState::ApplyHeadTransition;
        self.emit_head_transition()
    }

    fn emit_head_transition(&mut self) -> Effects {
        if let Some(effect) = self.pending_head_transition_effects.pop_front() {
            return smallvec![effect];
        }

        self.write_version()
    }

    fn read_destination_bucket(&mut self) -> Effects {
        self.state = IncomingVersionState::ReadDestinationBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.manifest.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn destination_bucket_info(&self) -> BucketInfo {
        BucketInfo {
            group_id: self.manifest.group_id,
            created_at: self.manifest.created_at,
            created_by: self.manifest.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    fn create_destination_bucket(&mut self) -> Effects {
        self.create_attempted = true;
        self.state = IncomingVersionState::CreateDestinationBucket;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CreateBucketOperation::new(
                self.manifest.bucket.clone(),
                self.destination_bucket_info()
            ),
            |result| Event::SubOperation(SubOperationEvent::BucketCreated {
                result: match result {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.to_string()),
                },
            }),
        ))]
    }

    /// The receiver's own group default and registered backend ids, loaded once
    /// the destination group is known so that the existing-copy probe and the
    /// transfer resolve the destination from identical inputs.
    fn load_destination_routing(&mut self) -> Effects {
        self.state = IncomingVersionState::LoadDestinationRouting;
        smallvec![load_group_inputs(
            self.destination_group_id.unwrap_or(self.manifest.group_id)
        )]
    }

    fn check_permissions(&mut self, group_id: Ulid) -> Effects {
        let path = self.target_authorization_path(group_id);
        if self.manifest_policy.as_deref() != Some(path.as_str()) {
            return self.reject_negotiation(IncomingVersionError::ManifestPermissionDenied);
        }
        if self.manifest.reference_advance.is_some() {
            if !self.valid_advance_manifest() {
                return self.reject_negotiation(IncomingVersionError::InvalidReferenceAdvance);
            }
            return self.read_existing_version();
        }
        match self.writer_policy.as_deref() {
            Some(allowed) if allowed == path.as_str() => self.read_existing_version(),
            _ => self.reject_negotiation(IncomingVersionError::WriterPermissionDenied),
        }
    }

    fn read_existing_version(&mut self) -> Effects {
        self.state = IncomingVersionState::ReadExistingVersion;
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_replaced_blob(&mut self, key: BlobLocationKey) -> Effects {
        self.state = IncomingVersionState::ReadReplacedBlob;
        smallvec![blob_location_read(&key, None)]
    }

    fn read_quota_config(&mut self) -> Effects {
        self.state = IncomingVersionState::ReadQuotaConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: DocumentTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_keyspace()
            .to_string(),
            key: DocumentTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_key(),
            txn_id: None,
        })]
    }

    fn start_quota_check(&mut self, ceiling: u64) -> Effects {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationBucketNotFound);
        };
        let logical_bytes = match self.incoming_logical_bytes() {
            Ok(logical_bytes) => logical_bytes,
            Err(error) => return self.fail(error),
        }
        .saturating_sub(self.replaced_logical_bytes);
        self.quota_ceiling = Some(ceiling);
        self.quota_gate = Some(QuotaGate::new_for_realm(
            ceiling,
            logical_bytes,
            group_id,
            self.local_node_id,
            self.local_realm_id,
        ));
        self.state = IncomingVersionState::StartQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true,
        })]
    }

    fn finish_quota_check(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionState::FinishQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    /// Asks only about the backend this node would route the blob to: a copy on
    /// any other backend cannot satisfy the destination placement.
    fn read_existing_blob(&mut self) -> Effects {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        let hash = blob.hash;
        // A full destination still probes, because a copy it already holds
        // costs it nothing; the cap only refuses the transfer itself.
        let backend = match self.resolve_destination() {
            Ok(resolved) => resolved.backend,
            Err(IncomingVersionError::RoutingFailed(RoutingError::BackendFull(backend))) => {
                self.destination_full = Some(RoutingError::BackendFull(backend.clone()));
                backend
            }
            // Still before the negotiation reply, so a node that cannot place
            // the blob owes the sender a reason rather than a dropped stream.
            Err(error) => return self.reject_negotiation(error),
        };
        self.state = IncomingVersionState::ReadExistingBlob;
        smallvec![blob_location_read(
            &BlobLocationKey::new(hash, backend),
            None
        )]
    }

    /// The only negotiation result that stores bytes, so the destination's cap
    /// is answered here rather than at the probe that keys the deduplication.
    fn request_blob_version(&mut self) -> Effects {
        match self.destination_full.take() {
            Some(error) => self.reject_negotiation(IncomingVersionError::RoutingFailed(error)),
            None => self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion),
        }
    }

    fn resolve_destination(&self) -> Result<ResolvedBackend, IncomingVersionError> {
        let snapshot = self
            .routing
            .snapshot(self.destination_group_id.unwrap_or(self.manifest.group_id))
            .with_group_inputs(self.destination_inputs.clone())
            .with_bucket_rules(self.destination_rules.clone());
        resolve_backend(&snapshot, &self.manifest.bucket, &self.manifest.key)
            .map_err(IncomingVersionError::RoutingFailed)
    }

    /// Nothing is admitted before the destination is evaluated: the reply that
    /// invites bytes is itself behind the gate.
    fn send_negotiation(&mut self, result: ReplicationNegotiationResult) -> Effects {
        // A rejection invites no bytes, so it is not gated. Gating it would
        // re-enter the gate from its own denial and never terminate.
        if matches!(
            result,
            ReplicationNegotiationResult::AlreadyReplicatedVersion
                | ReplicationNegotiationResult::Rejected(_)
        ) {
            return self.reply_negotiation(result);
        }
        let mut inherited = self
            .replaced_version
            .as_ref()
            .map(|version| version.placement_policies.clone())
            .unwrap_or_default();
        // The local default governs this copy too: a sender's manifest can only
        // add refs to what this destination already requires.
        if let Some(gated) = self.gated_bucket.as_ref() {
            inherited.extend(gated.policies.iter().copied());
        }
        let refs = match union_refs(&self.manifest.placement_policies, &inherited) {
            Ok(refs) => refs,
            Err(error) => return self.reject_negotiation(error.into()),
        };
        self.gated_refs = refs.clone();
        self.gated_bucket = self
            .gated_bucket
            .take()
            .map(|gated| gated.stored_under(self.gate_context.as_ref(), !refs.is_empty()));
        match write_gate(self.gate_context.as_ref(), &refs, self.destination_group_id) {
            Ok(None) => self.reply_negotiation(result),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.pending_negotiation = Some(result);
                self.state = IncomingVersionState::PolicyGate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn accept_policy_gate(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "an active placement gate",
                received: event,
            });
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let (Some(gate), Some(result)) = (self.gate.take(), self.pending_negotiation.take()) else {
            return self.fail(IncomingVersionError::GateNotPending);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.reject_negotiation(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => self.reply_negotiation(result),
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn reply_negotiation(&mut self, result: ReplicationNegotiationResult) -> Effects {
        self.negotiation_result = Some(result.clone());
        self.state = IncomingVersionState::SendNegotiation;
        let payload = match VersionReplicationMessage::VersionNegotiationResponse(result).to_bytes()
        {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn receive_blob(&mut self) -> Effects {
        // The receiver routes with its own snapshot; the sender's stamped
        // backend crossed the wire but is ignored.
        let resolved = match self.resolve_destination() {
            Ok(resolved) => resolved,
            Err(error) => return self.fail(error),
        };
        self.state = IncomingVersionState::ReceiveBlob;
        smallvec![Effect::Blob(BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            resolved,
            keep_alive: true,
        })]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = IncomingVersionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn check_purge_fence(&mut self) -> Effects {
        self.state = IncomingVersionState::CheckPurgeFence;
        smallvec![write_fence_read(&self.manifest.bucket, self.txn_id)]
    }

    fn read_replaced_metadata(&mut self) -> Effects {
        if self.replaced_version.is_none() {
            return self.write_hash_lookup();
        }
        let prefix = match MultipartObjectKey::part_prefix(self.manifest.version_id) {
            Ok(prefix) => prefix.into(),
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionState::ReadReplacedMetadata;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    /// Re-reads the details the negotiation gate decided on, inside the very
    /// transaction that exposes the replica.
    fn check_drift(&mut self) -> Effects {
        self.state = IncomingVersionState::CheckDrift;
        smallvec![drift_reads(&self.manifest.bucket, self.txn_id)]
    }

    /// Why this replica lands here, from what this node itself observed: an
    /// advance of a reference it serves, a standing relationship the sender
    /// names, or an explicitly requested copy.
    fn copy_origin(&self) -> CopyOrigin {
        if self.manifest.reference_advance.is_some() {
            return CopyOrigin::Reference;
        }
        match self.manifest.origin.as_ref() {
            Some(origin) => CopyOrigin::Sync {
                relationship_id: origin.relationship_id,
            },
            None => CopyOrigin::Replicate,
        }
    }

    /// Subject generation the gate admitted this replica under; zero when the
    /// version is ungoverned.
    fn stored_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    fn verify_replaced(&mut self) -> Effects {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionState::VerifyReplaced;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn delete_replaced_metadata(
        &mut self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Effects {
        let mut deletes = Vec::with_capacity(values.len() + 2);
        let summary_key = match MultipartObjectKey::summary(self.manifest.version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(error) => return self.fail(error.into()),
        };
        deletes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key,
        ));
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(), key)),
        );
        if let Some(hash) = self
            .replaced_version
            .as_ref()
            .and_then(BlobVersion::blob_hash)
        {
            let context = match self.alias_context() {
                Ok(context) => context,
                Err(error) => return self.fail(error),
            };
            let key = match context
                .path_index_key(*hash, self.manifest.version_id)
                .to_bytes()
            {
                Ok(key) => key.into(),
                Err(error) => return self.fail(error.into()),
            };
            deletes.push((HASH_PATHS_INDEX_KEYSPACE.to_string(), key));
        }
        self.state = IncomingVersionState::DeleteReplacedMetadata;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    /// The copy the replaced version named, once the replacement stops naming
    /// it. Nothing else drops that reference, so without this the bytes stay
    /// charged to the backend forever.
    fn replaced_reclaim_key(&self) -> Option<ReclaimCandidateKey> {
        let replaced = self.replaced_version.as_ref()?.location_key()?;
        let replacement = self.effective_materialized_location().ok().and_then(|it| {
            let hash: [u8; 32] = it.get_blake3()?.try_into().ok()?;
            Some(BlobLocationKey::new(hash, it.backend))
        });
        (replacement.as_ref() != Some(&replaced))
            .then(|| ReclaimCandidateKey::new(replaced.backend, replaced.blake3_hash))
    }

    fn write_replaced_candidate(&mut self, key: ReclaimCandidateKey) -> Effects {
        let candidate = ReclaimCandidate {
            enqueued_at: self.clock.now(),
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionState::WriteReclaimCandidate;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_RECLAIM_KEYSPACE.to_string(),
            key: key.to_bytes().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn effective_materialized_location(&self) -> Result<BackendLocation, IncomingVersionError> {
        self.received_blob
            .as_ref()
            .map(|received| received.location.clone())
            .or_else(|| self.existing_blob_location.clone())
            .ok_or(IncomingVersionError::MissingBlobLocation)
    }

    fn validate_materialized_location(
        &self,
        location: &BackendLocation,
    ) -> Result<(), IncomingVersionError> {
        let blob = self
            .manifest
            .blob
            .as_ref()
            .ok_or(IncomingVersionError::MissingBlobInfo)?;
        let blake3 = location
            .get_blake3()
            .ok_or(IncomingVersionError::MissingBlobLocation)?;

        if blake3 != blob.hash {
            return Err(IncomingVersionError::BlobHashMismatch);
        }
        if location.blob_size != blob.size {
            return Err(IncomingVersionError::BlobSizeMismatch);
        }
        if location.compressed != blob.compressed || location.encrypted != blob.encrypted {
            return Err(IncomingVersionError::BlobStorageFlagsMismatch);
        }

        Ok(())
    }

    fn write_hash_lookup(&mut self) -> Effects {
        if self.is_reference_item() {
            return self.write_object_lookup();
        }
        if let Some(received) = self.received_blob.as_ref()
            && let Err(err) = self.validate_materialized_location(&received.location)
        {
            return self.fail(err);
        }

        if self.received_blob.is_none() && self.existing_blob_location.is_none() {
            return self.write_object_lookup();
        }

        self.begin_blob_location()
    }

    fn begin_blob_location(&mut self) -> Effects {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        if let Some(effect) = fence_backend(&location.backend, self.txn_id) {
            self.state = IncomingVersionState::FenceBackend;
            return smallvec![effect];
        }
        self.verify_existing_blob()
    }

    /// A negotiation that adopted an existing copy read it outside the
    /// transaction. Re-reading it inside makes the commit fail rather than
    /// leave the version naming bytes another writer has since removed.
    fn verify_existing_blob(&mut self) -> Effects {
        if self.received_blob.is_some() {
            return self.write_blob_location();
        }
        let Some(location) = self.existing_blob_location.clone() else {
            return self.write_blob_location();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_blob_location();
        };
        let hash: [u8; 32] = match blake3_hash.try_into() {
            Ok(hash) => hash,
            Err(err) => return self.fail(ConversionError::from(err).into()),
        };
        self.state = IncomingVersionState::VerifyExistingBlob;
        smallvec![blob_location_read(
            &BlobLocationKey::new(hash, location.backend),
            self.txn_id
        )]
    }

    fn write_blob_location(&mut self) -> Effects {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_object_lookup();
        };

        self.state = IncomingVersionState::WriteBlobLocation;
        let effect = match write_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.fail(ConversionError::from(err).into()),
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![effect]
    }

    fn write_object_lookup(&mut self) -> Effects {
        if !self.manifest.current_version {
            return self.write_version();
        }
        if self.manifest.current_version_generation.is_none() {
            return self.fail(IncomingVersionError::MissingCurrentVersionGeneration);
        }

        self.state = IncomingVersionState::ReadObjectLookup;
        let key = match BlobHeadKey::new(&self.manifest.bucket, &self.manifest.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn read_current(&mut self, version_id: Ulid) -> Effects {
        self.state = IncomingVersionState::ReadCurrentVersion;
        let key = match VersionKey::new(&self.manifest.bucket, &self.manifest.key, version_id)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn apply_liveness(&mut self, previous_live: bool) -> Effects {
        let next_live = self.manifest.kind == ReplicationItemKind::Materialized;
        self.object_delta = i128::from(u8::from(next_live)) - i128::from(u8::from(previous_live));
        self.prepare_head_transition()
    }

    fn write_compared_object(&mut self, existing: Option<&[u8]>) -> Effects {
        let Some(incoming_generation) = self.manifest.current_version_generation else {
            return self.write_version();
        };

        let existing_pointer = match existing.map(CurrentVersionPointer::from_bytes).transpose() {
            Ok(pointer) => pointer,
            Err(err) => return self.fail(err.into()),
        };
        if let Some(advance) = self.manifest.reference_advance.as_ref() {
            let Some(previous_generation) = advance.generation.checked_sub(1) else {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            };
            let Some(pointer) = existing_pointer else {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            };
            if incoming_generation != advance.generation {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            }
            let advanced = CurrentVersionPointer::new_with_generation(
                self.manifest.version_id,
                advance.generation,
            );
            if self.advance_version_exists
                && (advanced == pointer || pointer.generation > advance.generation)
            {
                self.existing_current_pointer = Some(pointer);
                self.object_delta = 0;
                self.pending_head = None;
                return self.write_live_obligation();
            }
            // Lineage validation stands, and the accepted head order still has
            // the final word: an advance may never regress the observed head.
            if pointer.generation != previous_generation
                || pointer.version_id != advance.predecessor
                || !advanced.supersedes(Some(&pointer))
            {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            }

            self.existing_current_pointer = Some(pointer.clone());
            self.pending_head = Some(PendingHeadTransition {
                pointer: advanced,
                current_hash: None,
            });
            return self.read_current(pointer.version_id);
        }
        let candidate = CurrentVersionPointer::new_with_generation(
            self.manifest.version_id,
            incoming_generation,
        );
        let should_write = candidate.supersedes(existing_pointer.as_ref());

        self.existing_current_pointer = existing_pointer.clone();
        if should_write {
            self.pending_head = Some(PendingHeadTransition {
                pointer: candidate.clone(),
                current_hash: self.current_manifest_hash(),
            });
        } else {
            self.pending_head = None;
        }
        self.resume_head_transition()
    }

    /// Continues exactly where `write_compared_object` left off; the
    /// stored pointers carry the decision, so no continuation state is needed.
    fn resume_head_transition(&mut self) -> Effects {
        if self.pending_head.is_none() {
            return self.write_version();
        }
        match self.existing_current_pointer.clone() {
            Some(pointer) => self.read_current(pointer.version_id),
            None => self.apply_liveness(false),
        }
    }

    fn write_version(&mut self) -> Effects {
        self.write_blob_version()
    }

    fn write_blob_version(&mut self) -> Effects {
        self.state = IncomingVersionState::WriteBlobVersion;
        let version_key = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let (version, materialized_hash) = match self.manifest.kind {
            ReplicationItemKind::Materialized => {
                if self.is_reference_item() {
                    let version = match self
                        .reference_version()
                        .and_then(|version| Ok(version.with_policies(self.gated_refs.clone())?))
                    {
                        Ok(version) => version,
                        Err(error) => return self.fail(error),
                    };
                    (version, None)
                } else {
                    let Ok(location) = self.effective_materialized_location() else {
                        return self.fail(IncomingVersionError::MissingBlobLocation);
                    };
                    let Some(blake3_hash) = location.get_blake3() else {
                        return self.fail(IncomingVersionError::MissingBlobLocation);
                    };
                    let hash: [u8; 32] = match blake3_hash.try_into() {
                        Ok(hash) => hash,
                        Err(err) => return self.fail(ConversionError::from(err).into()),
                    };
                    let materialized = match BlobVersion::materialized(
                        hash,
                        location.backend.clone(),
                        self.manifest.created_at,
                        self.manifest.created_by,
                        self.manifest.source.clone(),
                    )
                    .with_metadata(self.manifest.metadata.clone())
                    .with_publisher(self.publisher_node_id)
                    .with_policies(self.gated_refs.clone())
                    {
                        Ok(materialized) => materialized,
                        Err(err) => return self.fail(err.into()),
                    };
                    (materialized, Some(hash))
                }
            }
            ReplicationItemKind::DeleteMarker => (
                BlobVersion::deleted(self.manifest.created_at, self.manifest.created_by)
                    .with_publisher(self.publisher_node_id),
                None,
            ),
        };

        let effect = match write_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        if let Some(hash) = materialized_hash {
            let context = match self.alias_context() {
                Ok(context) => context,
                Err(err) => return self.fail(err),
            };
            match add_index_effect(&context, hash, self.manifest.version_id, self.txn_id) {
                Ok(index_effect) => self.pending_version_effects.push_back(index_effect),
                Err(err) => return self.fail(err.into()),
            }
            // The replica becomes serveable exactly when its version commits.
            // A reference item materializes nothing and registers nothing.
            let location = match self.effective_materialized_location() {
                Ok(location) => location,
                Err(err) => return self.fail(err),
            };
            match register_effect(
                CopyRegistration {
                    version: version_key,
                    node_id: self.local_node_id,
                    location: &location,
                    policies: &self.gated_refs,
                    origin: self.copy_origin(),
                    subject_generation: self.stored_subject(),
                    registered_at_ms: self.manifest.version_id.timestamp_ms(),
                },
                self.txn_id,
            ) {
                Ok(register) => self.pending_version_effects.push_back(register),
                Err(err) => return self.fail(err.into()),
            }
        }
        smallvec![effect]
    }

    fn write_multipart_metadata(&mut self) -> Effects {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.write_live_obligation();
        };

        self.state = IncomingVersionState::WriteMultipartMetadata;
        let mut writes = Vec::with_capacity(multipart.parts.len() + 1);

        let summary_key = match MultipartObjectKey::summary(self.manifest.version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        let summary_value = match multipart.summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        writes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for part in &multipart.parts {
            let key = match MultipartObjectKey::part(self.manifest.version_id, part.part_number)
                .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.fail(err.into()),
            };
            let value = match part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.fail(err.into()),
            };
            writes.push((
                S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn write_live_obligation(&mut self) -> Effects {
        let auth_context = match self.manifest.reference_advance.as_ref() {
            Some(_) => self.manifest.auth_context.clone(),
            None => {
                let Some(auth_context) = self.manifest.writer_auth_context.clone() else {
                    return self.start_commit_quota();
                };
                auth_context
            }
        };
        self.state = IncomingVersionState::WriteLiveObligation;
        let mut record = LiveObligationRecord::new(
            self.local_node_id,
            auth_context,
            self.manifest.bucket.clone(),
            self.manifest.key.clone(),
            self.manifest.version_id,
            self.manifest.kind == ReplicationItemKind::DeleteMarker,
        )
        .with_origin(self.manifest.origin.clone())
        .with_sources(self.manifest.upstream_sources.clone());
        if let Some(advance) = self.manifest.reference_advance {
            record = record.with_reference_advance(advance);
        }
        match live_obligation_effect(record, self.txn_id) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error.into()),
        }
    }

    fn cleanup_key(location: &BackendLocation) -> Vec<u8> {
        location.ulid.to_bytes().to_vec()
    }

    fn prepare_cleanup(&mut self) -> Result<(), IncomingVersionError> {
        let Some(received) = self.received_blob.as_mut() else {
            return Ok(());
        };
        if received.reconciliation.is_some() {
            return Ok(());
        }
        let location = &received.location;
        let Some(blake3) = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok())
        else {
            return Err(IncomingVersionError::MissingBlobLocation);
        };
        let work = BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner: WriteOwner::Blob {
                blake3,
                realm_id: self.local_realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
            },
        };
        let key = Self::cleanup_key(location);
        let value = work.to_bytes()?;
        received.reconciliation = Some(ReconciliationWork { key, value });
        Ok(())
    }

    fn cleanup_effect(&self, txn_id: Ulid) -> Option<Effect> {
        let reconciliation = self.received_blob.as_ref()?.reconciliation.as_ref()?;
        Some(Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: reconciliation.key.clone().into(),
            value: reconciliation.value.clone().into(),
            txn_id: Some(txn_id),
        }))
    }

    fn commit_or_cleanup(&mut self) -> Effects {
        if self.manifest.kind != ReplicationItemKind::Materialized
            || self.is_reference_item()
            || self.received_blob.is_none()
        {
            return self.commit_transaction();
        }
        if let Err(error) = self.prepare_cleanup() {
            return self.fail(error);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(effect) = self.cleanup_effect(txn_id) else {
            return self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        self.state = IncomingVersionState::WriteCleanupRow;
        smallvec![effect]
    }

    fn usage_delta(&self) -> Result<UsageDelta, IncomingVersionError> {
        let bytes = match self.manifest.kind {
            ReplicationItemKind::Materialized => i128::from(self.incoming_logical_bytes()?),
            ReplicationItemKind::DeleteMarker => 0,
        };
        Ok(UsageDelta {
            objects: self.object_delta,
            logical_bytes: if self.is_reference_item() { 0 } else { bytes }
                - i128::from(self.replaced_logical_bytes),
            referenced_bytes: if self.is_reference_item() { bytes } else { 0 }
                - i128::from(self.replaced_reference_bytes),
            ..Default::default()
        })
    }

    fn start_commit_quota(&mut self) -> Effects {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationBucketNotFound);
        };
        let group_delta = match self.usage_delta() {
            Ok(delta) => delta,
            Err(error) => return self.fail(error),
        };
        if self.manifest.kind == ReplicationItemKind::DeleteMarker {
            let update = UsageCounterUpdate::for_group(group_id, group_delta);
            if update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        if self.is_reference_item() {
            let update = UsageCounterUpdate::for_group(group_id, group_delta);
            if self.manifest.reference_advance.is_some() && update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        self.usage_update = Some(match self.received_blob.as_ref() {
            None => UsageCounterUpdate::for_group(group_id, group_delta),
            Some(received) => match StoredDelta::for_location(&received.location, true) {
                Some(stored) => UsageCounterUpdate::with_stored(group_id, group_delta, stored),
                None => return self.fail(IncomingVersionError::MissingBlobInfo),
            },
        });
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let quota_bytes = blob.size.saturating_sub(self.replaced_logical_bytes);
        if let Some(ceiling) = self.quota_ceiling
            && quota_bytes > 0
        {
            let mut gate = QuotaGate::new_for_realm(
                ceiling,
                quota_bytes,
                group_id,
                self.local_node_id,
                self.local_realm_id,
            );
            self.state = IncomingVersionState::CheckCommitQuota;
            let effects = gate.start(txn_id);
            self.quota_gate = Some(gate);
            effects
        } else {
            self.start_usage_update()
        }
    }

    fn start_usage_update(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
    }

    fn commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit_failure(&mut self, error: StorageError) -> Effects {
        if error.proves_no_commit() {
            return self.fail(error.into());
        }

        self.txn_id = None;
        if let Some(received) = self.received_blob.as_mut() {
            received.cleanup_on_abort = false;
        }
        self.output = Some(Err(error.into()));
        self.release_or_reject()
    }

    fn release_or_reject(&mut self) -> Effects {
        let Some(id) = self
            .received_blob
            .as_ref()
            .map(|received| received.location.ulid)
        else {
            return self.send_apply_rejected();
        };
        self.release_id = Some(id);
        self.state = IncomingVersionState::ReleaseReservation;
        smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
    }

    fn accept_reservation_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::ReservationReleased)",
                received: event,
            });
        };
        if self.release_id != Some(id) {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "matching reservation id",
                received: Event::Blob(BlobEvent::ReservationReleased { id }),
            });
        }
        self.release_id = None;
        if self.apply_committed {
            self.state = IncomingVersionState::ScheduleUsage;
            smallvec![schedule_snapshot_publish()]
        } else {
            self.send_apply_rejected()
        }
    }

    fn accept_cleanup_row(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => self.commit_transaction(),
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::{WriteResult|Error})",
                received: other,
            }),
        }
    }

    fn register_blob_dht(&mut self) -> Effects {
        if self.is_reference_item() {
            return self.send_apply_complete();
        }
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        self.state = IncomingVersionState::RegisterBlobInDht;
        let effect =
            match dht_registration_effect(blake3_hash, self.local_realm_id, &self.rocrate_limits) {
                Ok(effect) => effect,
                Err(_) => return self.send_apply_complete(),
            };
        smallvec![effect]
    }

    fn finish_live_drain(&mut self) -> Effects {
        match self.manifest.kind {
            ReplicationItemKind::Materialized => self.register_blob_dht(),
            ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
        }
    }

    fn send_apply_complete(&mut self) -> Effects {
        self.state = IncomingVersionState::SendApplyComplete;
        let payload = match VersionReplicationMessage::VersionApplyComplete.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn send_apply_rejected(&mut self) -> Effects {
        self.state = IncomingVersionState::SendApplyRejected;
        let reason = self
            .output
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(ToString::to_string)
            .unwrap_or_else(|| "version replication apply failed".to_string());
        let payload = match VersionReplicationMessage::VersionApplyRejected(reason).to_bytes() {
            Ok(payload) => payload,
            Err(_) => {
                self.state = IncomingVersionState::Error;
                return self.abort();
            }
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn abort_or_close(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = IncomingVersionState::AbortTransaction;
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        } else {
            self.cleanup_or_close()
        }
    }

    fn cleanup_or_close(&mut self) -> Effects {
        if let Some(location) = self
            .received_blob
            .as_mut()
            .and_then(ReceivedBlob::take_cleanup)
        {
            self.state = IncomingVersionState::CleanupReceivedBlob;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = IncomingVersionState::Error;
            self.close_connection()
        }
    }

    fn close_connection(&mut self) -> Effects {
        self.state = IncomingVersionState::CloseConnection;
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }
}

// Phase: negotiation
// Destination resolution, the quota probe and the placement gate that
// together decide the reply sent to the sender.
impl IncomingVersionOperation {
    fn accept_destination_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            if self.create_attempted {
                return self.reject_negotiation(IncomingVersionError::DestinationBucketNotFound);
            }
            if self.manifest.reference_advance.is_some() {
                return self.reject_negotiation(IncomingVersionError::DestinationBucketNotFound);
            }
            if self.manifest_policy.is_none() {
                return self.reject_negotiation(IncomingVersionError::ManifestPermissionDenied);
            }
            return self.create_destination_bucket();
        };
        let bucket_info = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(bucket_info) => bucket_info,
            Err(err) => return self.fail(err.into()),
        };

        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            group_id = %bucket_info.group_id,
            kind = ?self.manifest.kind,
            current_version = self.manifest.current_version,
            current_version_generation = ?self.manifest.current_version_generation,
            "Loaded destination bucket for incoming replication"
        );

        self.destination_group_id = Some(bucket_info.group_id);
        self.gated_bucket = Some(GatedBucket::observe(Some(&bucket_info)));
        self.destination_rules = bucket_info.storage_routing;
        self.load_destination_routing()
    }

    fn accept_bucket_created(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::BucketCreated { result }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::SubOperation(SubOperationEvent::BucketCreated)",
                received: event,
            });
        };
        if let Err(reason) = result {
            debug!(
                bucket = %self.manifest.bucket,
                stream_id = %self.stream_id,
                reason = %reason,
                "Destination bucket auto-create did not create; re-reading"
            );
        }
        self.read_destination_bucket()
    }

    fn accept_routing_loaded(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                received: event,
            });
        };
        match result {
            Ok(inputs) => self.destination_inputs = inputs,
            Err(error) => {
                return self.fail(IncomingVersionError::RoutingInputsFailed(error));
            }
        }
        self.check_permissions(self.destination_group_id.unwrap_or(self.manifest.group_id))
    }

    fn accept_existing_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            existing_version_present = value.is_some(),
            kind = ?self.manifest.kind,
            "Loaded existing destination version metadata"
        );

        if let Some(value) = value {
            let existing = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(existing) => existing,
                Err(error) => return self.fail(error.into()),
            };
            if self.manifest.reference_advance.is_some() {
                let incoming = match self.reference_version() {
                    Ok(incoming) => incoming,
                    Err(error) => return self.reject_negotiation(error),
                };
                if existing != incoming {
                    return self.reject_negotiation(IncomingVersionError::InvalidReferenceAdvance);
                }
                self.replaced_reference_bytes = self
                    .manifest
                    .reference_metadata
                    .as_ref()
                    .map_or(0, |metadata| metadata.content_length);
                self.replaced_version = Some(existing);
                self.advance_version_exists = true;
                return self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly);
            }
            self.replaced_version = Some(existing.clone());
            match &existing.state {
                BlobVersionState::Reference {
                    cached_metadata, ..
                } => {
                    self.replaced_reference_bytes = cached_metadata.content_length;
                    if self.is_reference_item() {
                        let incoming = match self.reference_version() {
                            Ok(incoming) => incoming,
                            Err(error) => return self.fail(error),
                        };
                        if existing == incoming {
                            return self.send_negotiation(
                                ReplicationNegotiationResult::AlreadyReplicatedVersion,
                            );
                        }
                        return self
                            .send_negotiation(ReplicationNegotiationResult::NeedVersionOnly);
                    }
                }
                BlobVersionState::Materialized { blob_hash, .. } => {
                    if !self.is_reference_item()
                        && self
                            .manifest
                            .blob
                            .as_ref()
                            .is_some_and(|blob| blob.hash == *blob_hash)
                    {
                        return self.send_negotiation(
                            ReplicationNegotiationResult::AlreadyReplicatedVersion,
                        );
                    }
                    let Some(key) = existing.location_key() else {
                        return self.fail(IncomingVersionError::MissingBlobLocation);
                    };
                    return self.read_replaced_blob(key);
                }
                BlobVersionState::Deleted
                    if self.manifest.kind == ReplicationItemKind::DeleteMarker =>
                {
                    return self
                        .send_negotiation(ReplicationNegotiationResult::AlreadyReplicatedVersion);
                }
                BlobVersionState::Deleted => {}
            }
        }

        match self.manifest.kind {
            ReplicationItemKind::DeleteMarker => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            ReplicationItemKind::Materialized => self.read_quota_config(),
        }
    }

    fn accept_replaced_blob(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        self.replaced_logical_bytes = match value
            .as_ref()
            .map(|value| BackendLocation::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(location) => location.map_or(0, |location| location.blob_size),
            Err(error) => return self.fail(error.into()),
        };
        if self.is_reference_item() {
            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
        } else {
            self.read_quota_config()
        }
    }

    fn accept_quota_config(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationBucketNotFound);
        };
        let ceiling = match value
            .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(Some(config)) => config.quota.effective_group_ceiling(&group_id),
            Ok(None) => None,
            Err(err) => return self.fail(err.into()),
        };

        match ceiling {
            _ if self.is_reference_item() => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            Some(ceiling) => self.start_quota_check(ceiling),
            None => self.read_existing_blob(),
        }
    }

    fn accept_quota_transaction(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        self.state = IncomingVersionState::EnforceQuota;
        gate.start(txn_id)
    }

    fn accept_quota_step(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.finish_quota_check(),
            Err(err) => self.fail(err.into()),
        }
    }

    fn accept_quota_abort(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received: event,
            });
        };
        self.txn_id = None;
        if self.quota_gate.as_ref().is_some_and(QuotaGate::is_exceeded) {
            self.output = Some(Ok(self.result(false)));
            self.send_negotiation(ReplicationNegotiationResult::Rejected("quota".to_string()))
        } else if self.is_reference_item() {
            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
        } else {
            self.read_existing_blob()
        }
    }

    fn accept_existing_blob(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        if let Some(value) = value {
            match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) => {
                    if self.validate_materialized_location(&location).is_err() {
                        debug!(
                            bucket = %self.manifest.bucket,
                            key = %self.manifest.key,
                            version_id = %self.manifest.version_id,
                            stream_id = %self.stream_id,
                            existing_blob_size = location.blob_size,
                            "Existing destination blob differs; requesting blob and version"
                        );
                        return self.request_blob_version();
                    }
                    self.existing_blob_location = Some(location);
                    debug!(
                        bucket = %self.manifest.bucket,
                        key = %self.manifest.key,
                        version_id = %self.manifest.version_id,
                        stream_id = %self.stream_id,
                        "Existing destination blob matches manifest; requesting version only"
                    );
                    self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                }
                Err(_) => {
                    debug!(
                        bucket = %self.manifest.bucket,
                        key = %self.manifest.key,
                        version_id = %self.manifest.version_id,
                        stream_id = %self.stream_id,
                        "Destination blob missing or invalid; requesting blob and version"
                    );
                    self.request_blob_version()
                }
            }
        } else {
            debug!(
                bucket = %self.manifest.bucket,
                key = %self.manifest.key,
                version_id = %self.manifest.version_id,
                stream_id = %self.stream_id,
                "Destination blob absent; requesting blob and version"
            );
            self.request_blob_version()
        }
    }

    fn accept_reply_sent(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::MessageSent)",
                received: event,
            });
        };

        match self.negotiation_result.clone() {
            Some(ReplicationNegotiationResult::AlreadyReplicatedVersion)
            | Some(ReplicationNegotiationResult::Rejected(_)) => self.close_connection(),
            Some(ReplicationNegotiationResult::NeedVersionOnly) => {
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    decision = ?self.negotiation_result,
                    "Negotiation sent; awaiting version apply"
                );
                self.start_transaction()
            }
            Some(ReplicationNegotiationResult::NeedBlobAndVersion) => {
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    decision = ?self.negotiation_result,
                    "Negotiation sent; awaiting blob transfer"
                );
                self.receive_blob()
            }
            None => self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
    }
}

// Phase: receiving
// Accepting the transferred blob and opening the apply transaction with
// its purge-fence and destination-drift checks.
impl IncomingVersionOperation {
    fn accept_blob_finish(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::ReplicationFinished { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.received_blob = Some(ReceivedBlob::reserved(location));
                return self.fail(IncomingVersionError::ReplicationError(
                    ReplicationError::ReplicationFailed,
                ));
            }
            other => {
                return self.fail(IncomingVersionError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Blob(BlobEvent::{ReplicationFinished|WriteCleanup})",
                    received: other,
                });
            }
        };
        if let Err(err) = self.validate_materialized_location(&location) {
            self.received_blob = Some(ReceivedBlob::reserved(location));
            return self.fail(err);
        }
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            blob_size = location.blob_size,
            backend_path = %location.backend_path,
            "Received and validated replicated blob"
        );
        self.received_blob = Some(ReceivedBlob::reserved(location));
        self.start_transaction()
    }

    fn accept_transaction_start(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            txn_id = %txn_id,
            "Started incoming replication transaction"
        );
        self.check_purge_fence()
    }

    fn accept_purge_fence(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.manifest.bucket, &self.manifest.key) {
            return self.fail(error.into());
        }
        self.check_drift()
    }

    fn accept_drift_check(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        let (bucket, subject) = match split_drift_reads(values) {
            Ok(split) => split,
            Err(error) => return self.fail(error.into()),
        };
        let observed = GatedBucket::observe(bucket.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return self.fail(PolicyGateError::Drift.into());
            }
            if let Err(error) = gated.check_subject(subject.as_ref()) {
                return self.fail(error.into());
            }
        }
        self.verify_replaced()
    }
}

// Phase: apply/commit
// Exposing the replica: replacement cleanup, head transition, the version
// and its side records, then usage accounting and the commit.
impl IncomingVersionOperation {
    fn accept_replaced_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let current = match value
            .as_ref()
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        if current != self.replaced_version {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionConflict,
            ));
        }
        if self.advance_version_exists {
            return self.write_hash_lookup();
        }
        self.read_replaced_metadata()
    }

    fn accept_metadata_iterated(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        if next_start_after.is_some() {
            return self.fail(IncomingVersionError::MultipartMetadataOverflow);
        }
        self.delete_replaced_metadata(values)
    }

    fn accept_metadata_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };
        match self.replaced_reclaim_key() {
            Some(key) => self.write_replaced_candidate(key),
            None => {
                self.replaced_version = None;
                self.write_hash_lookup()
            }
        }
    }

    fn accept_reclaim_candidate(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.replaced_version = None;
        self.write_hash_lookup()
    }

    fn accept_backend_fence(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.verify_existing_blob(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_verified_blob(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let stored = match value
            .map(|value| BackendLocation::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(stored) => stored,
            Err(error) => return self.fail(error.into()),
        };
        if stored != self.existing_blob_location {
            return self.fail(IncomingVersionError::ExistingBlobChanged);
        }
        self.write_blob_location()
    }

    fn accept_blob_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.write_object_lookup()
    }

    fn accept_object_lookup(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let existing_pointer = value
            .as_ref()
            .and_then(|value| CurrentVersionPointer::from_bytes(value.as_ref()).ok());
        let incoming_generation = self.manifest.current_version_generation;
        let pointer_will_update = incoming_generation.is_some_and(|generation| {
            CurrentVersionPointer::new_with_generation(self.manifest.version_id, generation)
                .supersedes(existing_pointer.as_ref())
        });
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            existing_generation = existing_pointer.as_ref().map(|pointer| pointer.generation),
            existing_version_id = ?existing_pointer.as_ref().map(|pointer| pointer.version_id),
            incoming_generation = ?incoming_generation,
            pointer_will_update,
            "Compared destination current version pointer"
        );
        self.write_compared_object(value.as_deref())
    }

    fn accept_current_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(value) = value else {
            return self.fail(if self.manifest.reference_advance.is_some() {
                IncomingVersionError::InvalidReferenceAdvance
            } else {
                IncomingVersionError::CurrentVersionNotFound
            });
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = self.validate_advance(&version) {
            return self.fail(error);
        }
        self.apply_liveness(!version.is_deleted())
    }

    fn accept_head_progress(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. })
            | Event::Storage(StorageEvent::DeleteResult { .. }) => self.emit_head_transition(),
            _ => self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::{WriteResult|DeleteResult})",
                received: event,
            }),
        }
    }

    fn accept_blob_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        if let Some(effect) = self.pending_version_effects.pop_front() {
            return smallvec![effect];
        }
        self.write_multipart_metadata()
    }

    fn accept_multipart_write(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            multipart_parts = self.manifest.multipart.as_ref().map(|m| m.parts.len()).unwrap_or(0),
            "Wrote multipart replication metadata"
        );
        self.write_live_obligation()
    }

    fn accept_live_obligation(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.start_commit_quota()
    }

    fn accept_commit_quota(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) if gate.is_exceeded() => self.fail(IncomingVersionError::QuotaExceeded),
            Ok(None) => self.start_usage_update(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.commit_or_cleanup(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_transaction_commit(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                if let Some(received) = self.received_blob.as_mut() {
                    received.cleanup_on_abort = false;
                }
                self.apply_committed = true;
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    kind = ?self.manifest.kind,
                    "Committed incoming replication transaction"
                );
                if let Some(id) = self
                    .received_blob
                    .as_ref()
                    .map(|received| received.location.ulid)
                {
                    self.release_id = Some(id);
                    self.state = IncomingVersionState::ReleaseReservation;
                    smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
                } else {
                    self.state = IncomingVersionState::ScheduleUsage;
                    smallvec![schedule_snapshot_publish()]
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.handle_commit_failure(error),
            other => self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::{TransactionCommitted|Error})",
                received: other,
            }),
        }
    }

    fn accept_usage_schedule(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = IncomingVersionState::ScheduleLiveDrain;
                smallvec![schedule_blob_drain()]
            }
            other => {
                warn!(event = ?other, "Incoming replication committed but usage scheduling returned an unexpected event");
                self.state = IncomingVersionState::ScheduleLiveDrain;
                smallvec![schedule_blob_drain()]
            }
        }
    }

    fn accept_live_drain(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => self.finish_live_drain(),
            other => {
                warn!(event = ?other, "Incoming replication committed but drain scheduling returned an unexpected event");
                self.finish_live_drain()
            }
        }
    }

    fn accept_blob_registration(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
            _ => self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Net(NetEvent::Dht(DhtEvent::*))",
                received: event,
            }),
        }
    }

    fn accept_completion_sent(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::MessageSent)",
                received: event,
            });
        };
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            "Sent incoming replication apply-complete acknowledgement"
        );
        self.close_connection()
    }
}

// Phase: cleanup
// Rejecting, aborting, deleting unowned bytes and closing the stream.
impl IncomingVersionOperation {
    fn accept_apply_rejection(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::MessageSent)",
                received: event,
            });
        };
        self.abort_or_close()
    }

    fn accept_transaction_abort(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received: event,
            });
        };
        self.cleanup_or_close()
    }

    fn accept_blob_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = IncomingVersionState::Error;
                self.close_connection()
            }
            _ => {
                self.state = IncomingVersionState::Error;
                self.close_connection()
            }
        }
    }

    fn accept_connection_close(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
            return self.fail(IncomingVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::ConnectionClosed)",
                received: event,
            });
        };
        if self.output.as_ref().is_some_and(Result::is_err) {
            self.state = IncomingVersionState::Error;
        } else {
            self.state = IncomingVersionState::Finish;
        }
        if self.output.is_none() {
            self.output = Some(Ok(self.result(self.apply_committed)));
        }
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            state = %self.state_name(),
            "Closed incoming replication connection"
        );
        smallvec![]
    }
}

#[cfg(test)]
#[path = "incoming_pure_tests.rs"]
mod pure_tests;

/// Gate acceptance for an incoming replica: nothing governed is admitted
/// without a compliant local destination, and a reference registers nothing.
#[cfg(test)]
#[path = "incoming_decision_tests.rs"]
mod decision_tests;
