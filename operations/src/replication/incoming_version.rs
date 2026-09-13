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
    LiveReplicationObligationRecord, live_obligation_effect, schedule_blob_drain,
};
use crate::s3::create_bucket::CreateBucketOperation;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    HASH_PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    BackendLocation, BlobCleanupWork, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CopyOrigin, CurrentVersionPointer, GroupRoutingInputs, MultipartObjectMetadataKey,
    NodeRouting, PlacementPolicyRef, RealmConfigDocument, RealmId, ReclaimCandidate,
    ReclaimCandidateKey, ReplicationItemKind, ReplicationNegotiationResult, ResolvedBackend,
    RoCrateLimits, RoutingError, StorageRoutingRule, UsageDelta, VersionKey, WriteOwner,
    bucket_permission_path, object_permission_path, resolve_backend,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, NodeId};
use smallvec::smallvec;
use std::collections::VecDeque;
use std::time::SystemTime;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

/// One state machine per inbound stream; every accepted event advances exactly
/// one state and `step` names its handler after the phase and the accepted
/// result.
///
/// Negotiation
///   `Init` reads the destination bucket (auto-creating it once when absent),
///   loads its routing and checks permissions. It then probes the existing
///   version, quota and blob copy and ends in `SendNegotiation`.
/// Receiving
///   `NeedVersionOnly` and `NeedBlobAndVersion` replies open the apply
///   transaction; the latter receives the blob first (`ReceiveBlob`) and both
///   guard it with `CheckPurgeFence` and `CheckDrift`.
/// Apply/commit
///   The replacement is re-verified inside the transaction, the head
///   transition and version records are written, usage is accounted and
///   `CommitTransaction` settles the output. A committed receiver releases its
///   reservation, schedules usage and drain work, and registers the blob.
/// Cleanup
///   Any failure after the reply rejects the apply, aborts the transaction,
///   deletes bytes this receiver does not own and closes the stream;
///   `Finish` and `Error` are terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionReplicationState {
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
pub enum IncomingVersionReplicationError {
    #[error(transparent)]
    Policy(#[from] aruna_core::structs::PlacementPolicyError),
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
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingVersionReplicationResult {
    pub applied: bool,
    pub group_id: Option<GroupId>,
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
pub struct IncomingVersionReplicationOperation {
    state: IncomingVersionReplicationState,
    stream_id: Ulid,
    local_node_id: NodeId,
    /// The authenticated remote peer that pushed this stream, proven at Bao
    /// ingress. It is the accountable publisher, not the forgeable manifest.
    publisher_node_id: NodeId,
    local_realm_id: RealmId,
    manifest: VersionReplicationManifest,
    /// One wall-clock sample per operation: records this receiver creates
    /// (reclaim candidates) carry it instead of a fresh per-phase timestamp.
    now: SystemTime,
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
    output: Option<Result<IncomingVersionReplicationResult, IncomingVersionReplicationError>>,
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

impl IncomingVersionReplicationOperation {
    pub fn new(
        stream_id: Ulid,
        local_node_id: NodeId,
        local_realm_id: RealmId,
        manifest: VersionReplicationManifest,
    ) -> Self {
        Self {
            state: IncomingVersionReplicationState::Init,
            stream_id,
            local_node_id,
            // Defaults to the local node; the ingress handler overrides it with
            // the authenticated remote peer via `with_publisher_node`.
            publisher_node_id: local_node_id,
            local_realm_id,
            manifest,
            now: SystemTime::now(),
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

    /// Pins the operation-wide wall clock the receiver stamps generated records
    /// with. Production samples it once in [`Self::new`]; tests fix it.
    pub fn with_now(mut self, now: SystemTime) -> Self {
        self.now = now;
        self
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionReplicationState::Init => "Init",
            IncomingVersionReplicationState::ReadDestinationBucket => "ReadDestinationBucket",
            IncomingVersionReplicationState::CreateDestinationBucket => "CreateDestinationBucket",
            IncomingVersionReplicationState::LoadDestinationRouting => "LoadDestinationRouting",
            IncomingVersionReplicationState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionReplicationState::ReadReplacedBlob => "ReadReplacedBlob",
            IncomingVersionReplicationState::ReadQuotaConfig => "ReadQuotaConfig",
            IncomingVersionReplicationState::StartQuotaCheck => "StartQuotaCheck",
            IncomingVersionReplicationState::EnforceQuota => "EnforceQuota",
            IncomingVersionReplicationState::FinishQuotaCheck => "FinishQuotaCheck",
            IncomingVersionReplicationState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionReplicationState::PolicyGate => "PolicyGate",
            IncomingVersionReplicationState::SendNegotiation => "SendNegotiation",
            IncomingVersionReplicationState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionReplicationState::StartTransaction => "StartTransaction",
            IncomingVersionReplicationState::CheckPurgeFence => "CheckPurgeFence",
            IncomingVersionReplicationState::CheckDrift => "CheckDrift",
            IncomingVersionReplicationState::VerifyReplaced => "VerifyReplaced",
            IncomingVersionReplicationState::ReadReplacedMetadata => "ReadReplacedMetadata",
            IncomingVersionReplicationState::DeleteReplacedMetadata => "DeleteReplacedMetadata",
            IncomingVersionReplicationState::WriteReclaimCandidate => "WriteReclaimCandidate",
            IncomingVersionReplicationState::FenceBackend => "FenceBackend",
            IncomingVersionReplicationState::VerifyExistingBlob => "VerifyExistingBlob",
            IncomingVersionReplicationState::WriteBlobLocation => "WriteBlobLocation",
            IncomingVersionReplicationState::ReadObjectLookup => "ReadObjectLookup",
            IncomingVersionReplicationState::ReadCurrentVersion => "ReadCurrentVersion",
            IncomingVersionReplicationState::ApplyHeadTransition => "ApplyHeadTransition",
            IncomingVersionReplicationState::WriteBlobVersion => "WriteBlobVersion",
            IncomingVersionReplicationState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionReplicationState::WriteLiveObligation => "WriteLiveObligation",
            IncomingVersionReplicationState::CheckCommitQuota => "CheckCommitQuota",
            IncomingVersionReplicationState::UpdateUsage => "UpdateUsage",
            IncomingVersionReplicationState::WriteCleanupRow => "WriteCleanupRow",
            IncomingVersionReplicationState::CommitTransaction => "CommitTransaction",
            IncomingVersionReplicationState::ReleaseReservation => "ReleaseReservation",
            IncomingVersionReplicationState::ScheduleUsage => "ScheduleUsage",
            IncomingVersionReplicationState::ScheduleLiveDrain => "ScheduleLiveDrain",
            IncomingVersionReplicationState::SendApplyRejected => "SendApplyRejected",
            IncomingVersionReplicationState::AbortTransaction => "AbortTransaction",
            IncomingVersionReplicationState::CleanupReceivedBlob => "CleanupReceivedBlob",
            IncomingVersionReplicationState::RegisterBlobInDht => "RegisterBlobInDht",
            IncomingVersionReplicationState::SendApplyComplete => "SendApplyComplete",
            IncomingVersionReplicationState::CloseConnection => "CloseConnection",
            IncomingVersionReplicationState::Finish => "Finish",
            IncomingVersionReplicationState::Error => "Error",
        }
    }

    fn reject_negotiation(&mut self, err: IncomingVersionReplicationError) -> Effects {
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

    fn result(&self, applied: bool) -> IncomingVersionReplicationResult {
        IncomingVersionReplicationResult {
            applied,
            group_id: self.destination_group_id,
        }
    }

    fn fail(&mut self, err: IncomingVersionReplicationError) -> Effects {
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
                IncomingVersionReplicationState::SendApplyRejected
                    | IncomingVersionReplicationState::AbortTransaction
                    | IncomingVersionReplicationState::CleanupReceivedBlob
                    | IncomingVersionReplicationState::CloseConnection
                    | IncomingVersionReplicationState::Error
            );
        self.output = Some(Err(err));
        if should_reject {
            self.send_apply_rejected()
        } else {
            self.state = IncomingVersionReplicationState::Error;
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

    fn alias_context(&self) -> Result<HeadAliasContext, IncomingVersionReplicationError> {
        let Some(group_id) = self.destination_group_id else {
            return Err(IncomingVersionReplicationError::DestinationBucketNotFound);
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
        previous: &aruna_core::structs::VersionSourceBinding,
        incoming: &aruna_core::structs::VersionSourceBinding,
        advance: &ReferenceAdvance,
        version_id: Ulid,
    ) -> bool {
        if previous.descriptor.kind != aruna_core::structs::SourceConnectorKind::ArunaNative {
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

    fn validate_advance(
        &self,
        previous: &BlobVersion,
    ) -> Result<(), IncomingVersionReplicationError> {
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
            return Err(IncomingVersionReplicationError::InvalidReferenceAdvance);
        };
        // Each advance mints exactly one successor, so the count must be
        // continuous: a gap or repeat would let the publisher reset the cap.
        if previous_count.checked_add(1) != Some(*incoming_count) {
            return Err(IncomingVersionReplicationError::InvalidReferenceAdvance);
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
            return Err(IncomingVersionReplicationError::InvalidReferenceAdvance);
        }
        Ok(())
    }

    fn reference_version(&self) -> Result<BlobVersion, IncomingVersionReplicationError> {
        let source = self
            .manifest
            .source
            .clone()
            .ok_or(IncomingVersionReplicationError::MissingReferenceSource)?;
        if source.descriptor.kind == aruna_core::structs::SourceConnectorKind::LocalDirectory {
            return Err(IncomingVersionReplicationError::LocalReferenceSource);
        }
        let metadata = self
            .manifest
            .reference_metadata
            .clone()
            .ok_or(IncomingVersionReplicationError::MissingReferenceMetadata)?;
        let advance_count = self
            .manifest
            .reference_advance_count
            .ok_or(IncomingVersionReplicationError::MissingReferenceAdvanceCount)?;
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

    fn incoming_logical_bytes(&self) -> Result<u64, IncomingVersionReplicationError> {
        if self.is_reference_item() {
            return self
                .manifest
                .reference_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
                .ok_or(IncomingVersionReplicationError::MissingReferenceMetadata);
        }

        self.manifest
            .blob
            .as_ref()
            .map(|blob| blob.size)
            .ok_or(IncomingVersionReplicationError::MissingBlobInfo)
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
        self.state = IncomingVersionReplicationState::ApplyHeadTransition;
        self.emit_head_transition()
    }

    fn emit_head_transition(&mut self) -> Effects {
        if let Some(effect) = self.pending_head_transition_effects.pop_front() {
            return smallvec![effect];
        }

        self.write_version()
    }

    fn read_destination_bucket(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadDestinationBucket;
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
        self.state = IncomingVersionReplicationState::CreateDestinationBucket;
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
        self.state = IncomingVersionReplicationState::LoadDestinationRouting;
        smallvec![load_group_inputs(
            self.destination_group_id.unwrap_or(self.manifest.group_id)
        )]
    }

    fn check_permissions(&mut self, group_id: Ulid) -> Effects {
        let path = self.target_authorization_path(group_id);
        if self.manifest_policy.as_deref() != Some(path.as_str()) {
            return self
                .reject_negotiation(IncomingVersionReplicationError::ManifestPermissionDenied);
        }
        if self.manifest.reference_advance.is_some() {
            if !self.valid_advance_manifest() {
                return self
                    .reject_negotiation(IncomingVersionReplicationError::InvalidReferenceAdvance);
            }
            return self.read_existing_version();
        }
        match self.writer_policy.as_deref() {
            Some(allowed) if allowed == path.as_str() => self.read_existing_version(),
            _ => self.reject_negotiation(IncomingVersionReplicationError::WriterPermissionDenied),
        }
    }

    fn read_existing_version(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadExistingVersion;
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
        self.state = IncomingVersionReplicationState::ReadReplacedBlob;
        smallvec![blob_location_read(&key, None)]
    }

    fn read_quota_config(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadQuotaConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: DocumentSyncTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_keyspace()
            .to_string(),
            key: DocumentSyncTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_key(),
            txn_id: None,
        })]
    }

    fn start_quota_check(&mut self, ceiling: u64) -> Effects {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
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
        self.state = IncomingVersionReplicationState::StartQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true,
        })]
    }

    fn finish_quota_check(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::FinishQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    /// Asks only about the backend this node would route the blob to: a copy on
    /// any other backend cannot satisfy the destination placement.
    fn read_existing_blob(&mut self) -> Effects {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        let hash = blob.hash;
        // A full destination still probes, because a copy it already holds
        // costs it nothing; the cap only refuses the transfer itself.
        let backend = match self.resolve_destination() {
            Ok(resolved) => resolved.backend,
            Err(IncomingVersionReplicationError::RoutingFailed(RoutingError::BackendFull(
                backend,
            ))) => {
                self.destination_full = Some(RoutingError::BackendFull(backend.clone()));
                backend
            }
            // Still before the negotiation reply, so a node that cannot place
            // the blob owes the sender a reason rather than a dropped stream.
            Err(error) => return self.reject_negotiation(error),
        };
        self.state = IncomingVersionReplicationState::ReadExistingBlob;
        smallvec![blob_location_read(
            &BlobLocationKey::new(hash, backend),
            None
        )]
    }

    /// The only negotiation result that stores bytes, so the destination's cap
    /// is answered here rather than at the probe that keys the deduplication.
    fn request_blob_version(&mut self) -> Effects {
        match self.destination_full.take() {
            Some(error) => {
                self.reject_negotiation(IncomingVersionReplicationError::RoutingFailed(error))
            }
            None => self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion),
        }
    }

    fn resolve_destination(&self) -> Result<ResolvedBackend, IncomingVersionReplicationError> {
        let snapshot = self
            .routing
            .snapshot(self.destination_group_id.unwrap_or(self.manifest.group_id))
            .with_group_inputs(self.destination_inputs.clone())
            .with_bucket_rules(self.destination_rules.clone());
        resolve_backend(&snapshot, &self.manifest.bucket, &self.manifest.key)
            .map_err(IncomingVersionReplicationError::RoutingFailed)
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
                self.state = IncomingVersionReplicationState::PolicyGate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn handle_negotiation_gate_event(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
            return self.fail(IncomingVersionReplicationError::GateNotPending);
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
        self.state = IncomingVersionReplicationState::SendNegotiation;
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
        self.state = IncomingVersionReplicationState::ReceiveBlob;
        smallvec![Effect::Blob(BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            resolved,
            keep_alive: true,
        })]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn check_purge_fence(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::CheckPurgeFence;
        smallvec![write_fence_read(&self.manifest.bucket, self.txn_id)]
    }

    fn read_replaced_metadata(&mut self) -> Effects {
        if self.replaced_version.is_none() {
            return self.write_hash_lookup();
        }
        let prefix = match MultipartObjectMetadataKey::part_prefix(self.manifest.version_id) {
            Ok(prefix) => prefix.into(),
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionReplicationState::ReadReplacedMetadata;
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
        self.state = IncomingVersionReplicationState::CheckDrift;
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
        self.state = IncomingVersionReplicationState::VerifyReplaced;
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
        let summary_key =
            match MultipartObjectMetadataKey::summary(self.manifest.version_id).to_bytes() {
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
        self.state = IncomingVersionReplicationState::DeleteReplacedMetadata;
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
            enqueued_at: self.now,
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionReplicationState::WriteReclaimCandidate;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_RECLAIM_KEYSPACE.to_string(),
            key: key.to_bytes().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn effective_materialized_location(
        &self,
    ) -> Result<BackendLocation, IncomingVersionReplicationError> {
        self.received_blob
            .as_ref()
            .map(|received| received.location.clone())
            .or_else(|| self.existing_blob_location.clone())
            .ok_or(IncomingVersionReplicationError::MissingBlobLocation)
    }

    fn validate_materialized_location(
        &self,
        location: &BackendLocation,
    ) -> Result<(), IncomingVersionReplicationError> {
        let blob = self
            .manifest
            .blob
            .as_ref()
            .ok_or(IncomingVersionReplicationError::MissingBlobInfo)?;
        let blake3 = location
            .get_blake3()
            .ok_or(IncomingVersionReplicationError::MissingBlobLocation)?;

        if blake3 != blob.hash {
            return Err(IncomingVersionReplicationError::BlobHashMismatch);
        }
        if location.blob_size != blob.size {
            return Err(IncomingVersionReplicationError::BlobSizeMismatch);
        }
        if location.compressed != blob.compressed || location.encrypted != blob.encrypted {
            return Err(IncomingVersionReplicationError::BlobStorageFlagsMismatch);
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
            self.state = IncomingVersionReplicationState::FenceBackend;
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
        self.state = IncomingVersionReplicationState::VerifyExistingBlob;
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

        self.state = IncomingVersionReplicationState::WriteBlobLocation;
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
            return self.fail(IncomingVersionReplicationError::MissingCurrentVersionGeneration);
        }

        self.state = IncomingVersionReplicationState::ReadObjectLookup;
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
        self.state = IncomingVersionReplicationState::ReadCurrentVersion;
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
                return self.fail(IncomingVersionReplicationError::InvalidReferenceAdvance);
            };
            let Some(pointer) = existing_pointer else {
                return self.fail(IncomingVersionReplicationError::InvalidReferenceAdvance);
            };
            if incoming_generation != advance.generation {
                return self.fail(IncomingVersionReplicationError::InvalidReferenceAdvance);
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
                return self.fail(IncomingVersionReplicationError::InvalidReferenceAdvance);
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
        self.state = IncomingVersionReplicationState::WriteBlobVersion;
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
                        return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
                    };
                    let Some(blake3_hash) = location.get_blake3() else {
                        return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
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

        self.state = IncomingVersionReplicationState::WriteMultipartMetadata;
        let mut writes = Vec::with_capacity(multipart.parts.len() + 1);

        let summary_key =
            match MultipartObjectMetadataKey::summary(self.manifest.version_id).to_bytes() {
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
            let key =
                match MultipartObjectMetadataKey::part(self.manifest.version_id, part.part_number)
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
        self.state = IncomingVersionReplicationState::WriteLiveObligation;
        let mut record = LiveReplicationObligationRecord::new(
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

    fn prepare_cleanup(&mut self) -> Result<(), IncomingVersionReplicationError> {
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
            return Err(IncomingVersionReplicationError::MissingBlobLocation);
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
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(effect) = self.cleanup_effect(txn_id) else {
            return self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        self.state = IncomingVersionReplicationState::WriteCleanupRow;
        smallvec![effect]
    }

    fn usage_delta(&self) -> Result<UsageDelta, IncomingVersionReplicationError> {
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
            return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
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
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        self.usage_update = Some(match self.received_blob.as_ref() {
            None => UsageCounterUpdate::for_group(group_id, group_delta),
            Some(received) => match StoredDelta::for_location(&received.location, true) {
                Some(stored) => UsageCounterUpdate::with_stored(group_id, group_delta, stored),
                None => return self.fail(IncomingVersionReplicationError::MissingBlobInfo),
            },
        });
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
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
            self.state = IncomingVersionReplicationState::CheckCommitQuota;
            let effects = gate.start(txn_id);
            self.quota_gate = Some(gate);
            effects
        } else {
            self.start_usage_update()
        }
    }

    fn start_usage_update(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
    }

    fn commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::CommitTransaction;
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
        self.state = IncomingVersionReplicationState::ReleaseReservation;
        smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
    }

    fn handle_apply_reservation_released(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::ReservationReleased)",
                received: event,
            });
        };
        if self.release_id != Some(id) {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "matching reservation id",
                received: Event::Blob(BlobEvent::ReservationReleased { id }),
            });
        }
        self.release_id = None;
        if self.apply_committed {
            self.state = IncomingVersionReplicationState::ScheduleUsage;
            smallvec![schedule_snapshot_publish()]
        } else {
            self.send_apply_rejected()
        }
    }

    fn handle_apply_cleanup_row_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => self.commit_transaction(),
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            other => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

        self.state = IncomingVersionReplicationState::RegisterBlobInDht;
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
        self.state = IncomingVersionReplicationState::SendApplyComplete;
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
        self.state = IncomingVersionReplicationState::SendApplyRejected;
        let reason = self
            .output
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(ToString::to_string)
            .unwrap_or_else(|| "version replication apply failed".to_string());
        let payload = match VersionReplicationMessage::VersionApplyRejected(reason).to_bytes() {
            Ok(payload) => payload,
            Err(_) => {
                self.state = IncomingVersionReplicationState::Error;
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
            self.state = IncomingVersionReplicationState::AbortTransaction;
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
            self.state = IncomingVersionReplicationState::CleanupReceivedBlob;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = IncomingVersionReplicationState::Error;
            self.close_connection()
        }
    }

    fn close_connection(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::CloseConnection;
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }
}

// Phase: negotiation
// Destination resolution, the quota probe and the placement gate that
// together decide the reply sent to the sender.
impl IncomingVersionReplicationOperation {
    fn handle_negotiation_destination_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            if self.create_attempted {
                return self.reject_negotiation(
                    IncomingVersionReplicationError::DestinationBucketNotFound,
                );
            }
            if self.manifest.reference_advance.is_some() {
                return self.reject_negotiation(
                    IncomingVersionReplicationError::DestinationBucketNotFound,
                );
            }
            if self.manifest_policy.is_none() {
                return self
                    .reject_negotiation(IncomingVersionReplicationError::ManifestPermissionDenied);
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

    fn handle_negotiation_bucket_created(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::BucketCreated { result }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_negotiation_routing_loaded(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                received: event,
            });
        };
        match result {
            Ok(inputs) => self.destination_inputs = inputs,
            Err(error) => {
                return self.fail(IncomingVersionReplicationError::RoutingInputsFailed(error));
            }
        }
        self.check_permissions(self.destination_group_id.unwrap_or(self.manifest.group_id))
    }

    fn handle_negotiation_existing_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
                    return self.reject_negotiation(
                        IncomingVersionReplicationError::InvalidReferenceAdvance,
                    );
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
                        return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
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

    fn handle_negotiation_replaced_blob_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_negotiation_quota_config_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
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

    fn handle_negotiation_quota_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        self.state = IncomingVersionReplicationState::EnforceQuota;
        gate.start(txn_id)
    }

    fn handle_negotiation_quota_gate_stepped(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.finish_quota_check(),
            Err(err) => self.fail(err.into()),
        }
    }

    fn handle_negotiation_quota_check_aborted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_negotiation_existing_blob_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_negotiation_reply_sent(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
            None => self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
    }
}

// Phase: receiving
// Accepting the transferred blob and opening the apply transaction with
// its purge-fence and destination-drift checks.
impl IncomingVersionReplicationOperation {
    fn handle_receiving_blob_finished(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::ReplicationFinished { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.received_blob = Some(ReceivedBlob::reserved(location));
                return self.fail(IncomingVersionReplicationError::ReplicationError(
                    ReplicationError::ReplicationFailed,
                ));
            }
            other => {
                return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_receiving_apply_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_receiving_purge_fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.manifest.bucket, &self.manifest.key) {
            return self.fail(error.into());
        }
        self.check_drift()
    }

    fn handle_receiving_drift_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
impl IncomingVersionReplicationOperation {
    fn handle_apply_replaced_version_verified(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionConflict,
            ));
        }
        if self.advance_version_exists {
            return self.write_hash_lookup();
        }
        self.read_replaced_metadata()
    }

    fn handle_apply_replaced_metadata_iterated(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        if next_start_after.is_some() {
            return self.fail(IncomingVersionReplicationError::MultipartMetadataOverflow);
        }
        self.delete_replaced_metadata(values)
    }

    fn handle_apply_replaced_metadata_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_apply_reclaim_candidate_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.replaced_version = None;
        self.write_hash_lookup()
    }

    fn handle_apply_backend_fence_checked(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.verify_existing_blob(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_apply_existing_blob_verified(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
            return self.fail(IncomingVersionReplicationError::ExistingBlobChanged);
        }
        self.write_blob_location()
    }

    fn handle_apply_blob_location_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.write_object_lookup()
    }

    fn handle_apply_object_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_apply_current_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(value) = value else {
            return self.fail(if self.manifest.reference_advance.is_some() {
                IncomingVersionReplicationError::InvalidReferenceAdvance
            } else {
                IncomingVersionReplicationError::CurrentVersionNotFound
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

    fn handle_apply_head_transition_progressed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. })
            | Event::Storage(StorageEvent::DeleteResult { .. }) => self.emit_head_transition(),
            _ => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::{WriteResult|DeleteResult})",
                received: event,
            }),
        }
    }

    fn handle_apply_blob_version_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_apply_multipart_metadata_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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

    fn handle_apply_live_obligation_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        self.start_commit_quota()
    }

    fn handle_apply_commit_quota_checked(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match gate.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) if gate.is_exceeded() => {
                self.fail(IncomingVersionReplicationError::QuotaExceeded)
            }
            Ok(None) => self.start_usage_update(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_apply_usage_updated(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            ));
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.commit_or_cleanup(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_apply_transaction_committed(&mut self, event: Event) -> Effects {
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
                    self.state = IncomingVersionReplicationState::ReleaseReservation;
                    smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
                } else {
                    self.state = IncomingVersionReplicationState::ScheduleUsage;
                    smallvec![schedule_snapshot_publish()]
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.handle_commit_failure(error),
            other => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::{TransactionCommitted|Error})",
                received: other,
            }),
        }
    }

    fn handle_apply_usage_scheduled(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => {
                self.state = IncomingVersionReplicationState::ScheduleLiveDrain;
                smallvec![schedule_blob_drain()]
            }
            other => {
                warn!(event = ?other, "Incoming replication committed but usage scheduling returned an unexpected event");
                self.state = IncomingVersionReplicationState::ScheduleLiveDrain;
                smallvec![schedule_blob_drain()]
            }
        }
    }

    fn handle_apply_live_drain_scheduled(&mut self, event: Event) -> Effects {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. })
            | Event::Task(TaskEvent::Error { .. }) => self.finish_live_drain(),
            other => {
                warn!(event = ?other, "Incoming replication committed but drain scheduling returned an unexpected event");
                self.finish_live_drain()
            }
        }
    }

    fn handle_apply_blob_registration_settled(&mut self, event: Event) -> Effects {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
            _ => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Net(NetEvent::Dht(DhtEvent::*))",
                received: event,
            }),
        }
    }

    fn handle_apply_completion_sent(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
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
impl IncomingVersionReplicationOperation {
    fn handle_cleanup_apply_rejection_sent(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::MessageSent)",
                received: event,
            });
        };
        self.abort_or_close()
    }

    fn handle_cleanup_transaction_aborted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received: event,
            });
        };
        self.cleanup_or_close()
    }

    fn handle_cleanup_received_blob_cleaned(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = IncomingVersionReplicationState::Error;
                self.close_connection()
            }
            _ => {
                self.state = IncomingVersionReplicationState::Error;
                self.close_connection()
            }
        }
    }

    fn handle_cleanup_connection_closed(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
            return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::ConnectionClosed)",
                received: event,
            });
        };
        if self.output.as_ref().is_some_and(Result::is_err) {
            self.state = IncomingVersionReplicationState::Error;
        } else {
            self.state = IncomingVersionReplicationState::Finish;
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

impl Operation for IncomingVersionReplicationOperation {
    type Output = Result<IncomingVersionReplicationResult, IncomingVersionReplicationError>;
    type Error = IncomingVersionReplicationError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.manifest.validate() {
            return self.reject_negotiation(error.into());
        }
        if self.manifest.reference_advance.is_some() && !self.valid_advance_manifest() {
            return self
                .reject_negotiation(IncomingVersionReplicationError::InvalidReferenceAdvance);
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
            return self.reject_negotiation(IncomingVersionReplicationError::HopLimitExceeded);
        }
        if self.manifest.auth_context.realm_id != self.local_realm_id
            || self.manifest.auth_context.user_id.realm_id != self.local_realm_id
        {
            return self.reject_negotiation(IncomingVersionReplicationError::RealmMismatch);
        }
        if self.manifest.writer_auth_context.is_none() && self.manifest.reference_advance.is_none()
        {
            return self
                .reject_negotiation(IncomingVersionReplicationError::WriterPermissionDenied);
        }
        if self
            .manifest
            .writer_auth_context
            .as_ref()
            .is_some_and(|auth| {
                auth.realm_id != self.local_realm_id || auth.user_id.realm_id != self.local_realm_id
            })
        {
            return self.reject_negotiation(IncomingVersionReplicationError::RealmMismatch);
        }

        self.read_destination_bucket()
    }

    /// One accepted event per state, dispatched to the handler named
    /// after its phase and the accepted result. The phase order is documented
    /// on [`IncomingVersionReplicationState`].
    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            // Negotiation: decide and send the reply.
            IncomingVersionReplicationState::Init => self.start(),
            IncomingVersionReplicationState::ReadDestinationBucket => {
                self.handle_negotiation_destination_bucket_read(event)
            }
            IncomingVersionReplicationState::CreateDestinationBucket => {
                self.handle_negotiation_bucket_created(event)
            }
            IncomingVersionReplicationState::LoadDestinationRouting => {
                self.handle_negotiation_routing_loaded(event)
            }
            IncomingVersionReplicationState::ReadExistingVersion => {
                self.handle_negotiation_existing_version_read(event)
            }
            IncomingVersionReplicationState::ReadReplacedBlob => {
                self.handle_negotiation_replaced_blob_read(event)
            }
            IncomingVersionReplicationState::ReadQuotaConfig => {
                self.handle_negotiation_quota_config_read(event)
            }
            IncomingVersionReplicationState::StartQuotaCheck => {
                self.handle_negotiation_quota_transaction_started(event)
            }
            IncomingVersionReplicationState::EnforceQuota => {
                self.handle_negotiation_quota_gate_stepped(event)
            }
            IncomingVersionReplicationState::FinishQuotaCheck => {
                self.handle_negotiation_quota_check_aborted(event)
            }
            IncomingVersionReplicationState::ReadExistingBlob => {
                self.handle_negotiation_existing_blob_read(event)
            }
            IncomingVersionReplicationState::PolicyGate => {
                self.handle_negotiation_gate_event(event)
            }
            IncomingVersionReplicationState::SendNegotiation => {
                self.handle_negotiation_reply_sent(event)
            }
            // Receiving: accept the bytes and open the apply.
            IncomingVersionReplicationState::ReceiveBlob => {
                self.handle_receiving_blob_finished(event)
            }
            IncomingVersionReplicationState::StartTransaction => {
                self.handle_receiving_apply_transaction_started(event)
            }
            IncomingVersionReplicationState::CheckPurgeFence => {
                self.handle_receiving_purge_fence_checked(event)
            }
            IncomingVersionReplicationState::CheckDrift => {
                self.handle_receiving_drift_checked(event)
            }
            // Apply/commit: expose the version and settle ownership.
            IncomingVersionReplicationState::VerifyReplaced => {
                self.handle_apply_replaced_version_verified(event)
            }
            IncomingVersionReplicationState::ReadReplacedMetadata => {
                self.handle_apply_replaced_metadata_iterated(event)
            }
            IncomingVersionReplicationState::DeleteReplacedMetadata => {
                self.handle_apply_replaced_metadata_deleted(event)
            }
            IncomingVersionReplicationState::WriteReclaimCandidate => {
                self.handle_apply_reclaim_candidate_written(event)
            }
            IncomingVersionReplicationState::FenceBackend => {
                self.handle_apply_backend_fence_checked(event)
            }
            IncomingVersionReplicationState::VerifyExistingBlob => {
                self.handle_apply_existing_blob_verified(event)
            }
            IncomingVersionReplicationState::WriteBlobLocation => {
                self.handle_apply_blob_location_written(event)
            }
            IncomingVersionReplicationState::ReadObjectLookup => {
                self.handle_apply_object_lookup_read(event)
            }
            IncomingVersionReplicationState::ReadCurrentVersion => {
                self.handle_apply_current_version_read(event)
            }
            IncomingVersionReplicationState::ApplyHeadTransition => {
                self.handle_apply_head_transition_progressed(event)
            }
            IncomingVersionReplicationState::WriteBlobVersion => {
                self.handle_apply_blob_version_written(event)
            }
            IncomingVersionReplicationState::WriteMultipartMetadata => {
                self.handle_apply_multipart_metadata_written(event)
            }
            IncomingVersionReplicationState::WriteLiveObligation => {
                self.handle_apply_live_obligation_written(event)
            }
            IncomingVersionReplicationState::CheckCommitQuota => {
                self.handle_apply_commit_quota_checked(event)
            }
            IncomingVersionReplicationState::UpdateUsage => self.handle_apply_usage_updated(event),
            IncomingVersionReplicationState::WriteCleanupRow => {
                self.handle_apply_cleanup_row_written(event)
            }
            IncomingVersionReplicationState::CommitTransaction => {
                self.handle_apply_transaction_committed(event)
            }
            IncomingVersionReplicationState::ReleaseReservation => {
                self.handle_apply_reservation_released(event)
            }
            IncomingVersionReplicationState::ScheduleUsage => {
                self.handle_apply_usage_scheduled(event)
            }
            IncomingVersionReplicationState::ScheduleLiveDrain => {
                self.handle_apply_live_drain_scheduled(event)
            }
            IncomingVersionReplicationState::RegisterBlobInDht => {
                self.handle_apply_blob_registration_settled(event)
            }
            IncomingVersionReplicationState::SendApplyComplete => {
                self.handle_apply_completion_sent(event)
            }
            // Cleanup: reject, abort, delete and close.
            IncomingVersionReplicationState::SendApplyRejected => {
                self.handle_cleanup_apply_rejection_sent(event)
            }
            IncomingVersionReplicationState::AbortTransaction => {
                self.handle_cleanup_transaction_aborted(event)
            }
            IncomingVersionReplicationState::CleanupReceivedBlob => {
                self.handle_cleanup_received_blob_cleaned(event)
            }
            IncomingVersionReplicationState::CloseConnection => {
                self.handle_cleanup_connection_closed(event)
            }
            IncomingVersionReplicationState::Finish => smallvec![],
            IncomingVersionReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingVersionReplicationState::Finish | IncomingVersionReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let default = self.result(self.apply_committed);
        let output = self.output.unwrap_or(Ok(default));
        match (self.state, output) {
            (IncomingVersionReplicationState::Error, Err(error)) => Err(error),
            (_, output) => Ok(output),
        }
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];

        let cleanup_location = match &self.state {
            // An unknown commit outcome must preserve the copy: the commit may
            // have landed, so the reservation is released instead of deleted.
            IncomingVersionReplicationState::CommitTransaction => {
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

#[cfg(test)]
mod tests {
    use super::{
        IncomingVersionReplicationError, IncomingVersionReplicationOperation,
        IncomingVersionReplicationState, ReceivedBlob,
    };

    use crate::replication::protocol::{
        MAX_REPLICATION_VALUE_BYTES, MaterializedBlobInfo, ReferenceAdvance, SyncOrigin,
        VersionReplicationManifest, VersionReplicationMessage,
    };
    use crate::replication::queue::LiveReplicationObligationRecord;
    use crate::s3::purge_fence::PurgeFenceError;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::{BlobError, StorageError};
    use aruna_core::events::{
        BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent,
    };
    use aruna_core::id::DhtKeyId;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
        BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
        S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, BlobVersion,
        BlobVersionState, BucketInfo, CurrentVersionPointer, GroupRoutingInputs, HashPathIndexKey,
        JobId, MultipartObjectMetadataKey, NodeRouting, QuotaConfig, RealmConfigDocument, RealmId,
        ReclaimCandidateKey, ReplicationItemKind, ReplicationNegotiationResult, RoutingTarget,
        SourceConnectorKind, SourceMetadata, StagingStrategy, StoragePurgeFence, StoragePurgeScope,
        StorageRoutingRule, UsageDelta, VersionSourceBinding, WriteOwner,
    };
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::{NodeId, UserId};
    use std::collections::{BTreeSet, HashMap};
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn test_group_id() -> Ulid {
        Ulid::from_parts(7, 7)
    }

    /// Fixed wall clock for the traces; production samples once per operation.
    fn trace_now() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
    }

    /// Named fixed identities, one seed per role, so a persisted value in a
    /// trace is always traceable to the input that produced it.
    fn trace_stream_id() -> Ulid {
        Ulid::from_bytes([0x51; 16])
    }

    fn trace_txn_id() -> Ulid {
        Ulid::from_bytes([0x52; 16])
    }

    fn trace_version_id() -> Ulid {
        Ulid::from_bytes([0x53; 16])
    }

    fn fixed_created_at() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000)
    }

    fn make_location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![1u8; 32]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: "bucket/key".to_string(),
            ulid: Ulid::from_bytes([0x21; 16]),
            compressed: false,
            encrypted: false,
            created_by: test_user_id(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    fn make_bucket_info(group_id: Ulid) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: test_user_id(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    pub(super) fn make_manifest(kind: ReplicationItemKind) -> VersionReplicationManifest {
        let blob = match kind {
            ReplicationItemKind::Materialized => {
                let location = make_location();
                Some(MaterializedBlobInfo {
                    hash: [1u8; 32],
                    size: location.blob_size,
                    compressed: location.compressed,
                    encrypted: location.encrypted,
                    location,
                })
            }
            ReplicationItemKind::DeleteMarker => None,
        };

        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "dir/file.txt".to_string(),
            version_id: trace_version_id(),
            group_id: test_group_id(),
            kind,
            created_at: fixed_created_at(),
            created_by: test_user_id(),
            current_version: true,
            current_version_generation: Some(1),
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
                session: None,
            },
            blob,
            source: None,
            multipart: None,
            reference_intent: false,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: Some(AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
                session: None,
            }),
            reference_metadata: None,
            metadata: HashMap::new(),
            reference_advance: None,
            reference_advance_count: None,
            placement_policies: Vec::new(),
        }
    }

    fn make_source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "dir/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([0x22; 16])),
        }
    }

    pub(super) fn make_reference_manifest() -> VersionReplicationManifest {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut source = make_source_binding();
        source.descriptor.kind = SourceConnectorKind::ArunaNative;
        source.descriptor.origin_node_id = Some(iroh::SecretKey::from_bytes(&[8u8; 32]).public());
        source.connector_id = None;
        manifest.blob = None;
        manifest.source = Some(source);
        manifest.reference_intent = true;
        manifest.reference_metadata = Some(SourceMetadata {
            content_length: 1_000_000,
            content_type: Some("application/octet-stream".to_string()),
            etag: None,
            last_modified: Some(manifest.created_at),
            source_version: None,
        });
        manifest.reference_advance_count = Some(0);
        manifest
    }

    fn advance_fixture() -> (VersionReplicationManifest, BlobVersion, NodeId) {
        let predecessor = Ulid::from_bytes([21u8; 16]);
        let version_id = Ulid::from_bytes([22u8; 16]);
        let publisher = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
        let mut manifest = make_reference_manifest();
        manifest.version_id = version_id;
        manifest.created_at = SystemTime::UNIX_EPOCH;
        manifest.current_version_generation = Some(8);
        manifest.auth_context = AuthContext::anonymous(test_realm_id());
        manifest.writer_auth_context = None;
        manifest.metadata = HashMap::from([("s3-key".to_string(), "value".to_string())]);
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::from_bytes([26u8; 16]),
            hop_count: 1,
        });
        manifest.reference_advance = Some(ReferenceAdvance {
            generation: 8,
            predecessor,
        });
        manifest.reference_advance_count = Some(1);
        manifest
            .source
            .as_mut()
            .unwrap()
            .descriptor
            .version_selector = Some(format!("version:{version_id}"));

        let mut previous_source = manifest.source.clone().unwrap();
        previous_source.descriptor.version_selector = Some(format!("version:{predecessor}"));
        let mut previous_metadata = manifest.reference_metadata.clone().unwrap();
        previous_metadata.content_length = 42;
        let previous = BlobVersion::reference(
            previous_source,
            previous_metadata,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            manifest.created_by,
            SystemTime::UNIX_EPOCH + Duration::from_secs(2),
        )
        .with_metadata(manifest.metadata.clone())
        .with_publisher(publisher);

        (manifest, previous, publisher)
    }

    fn advance_operation(
        manifest: VersionReplicationManifest,
        publisher: NodeId,
    ) -> IncomingVersionReplicationOperation {
        IncomingVersionReplicationOperation::new(
            Ulid::from_bytes([24u8; 16]),
            iroh::SecretKey::from_bytes(&[25u8; 32]).public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(publisher)
    }

    fn assert_advance_invalid(
        manifest: VersionReplicationManifest,
        publisher: NodeId,
        previous: BlobVersion,
    ) {
        let op = advance_operation(manifest, publisher);
        assert_eq!(
            op.validate_advance(&previous),
            Err(IncomingVersionReplicationError::InvalidReferenceAdvance)
        );
    }

    fn message_from_effect(effect: &Effect) -> VersionReplicationMessage {
        let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = effect else {
            panic!("expected blob send message effect")
        };
        VersionReplicationMessage::from_bytes(payload).unwrap()
    }

    fn expect_rejected_negotiation(effect: &Effect, expected_reason: &str) {
        match message_from_effect(effect) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert_eq!(reason, expected_reason),
            other => panic!("expected rejected negotiation response, got {other:?}"),
        }
    }

    /// Answers the routing load that follows every destination bucket read.
    fn load_routing(
        op: &mut IncomingVersionReplicationOperation,
        inputs: GroupRoutingInputs,
    ) -> aruna_core::types::Effects {
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::LoadDestinationRouting
        );
        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(inputs),
        }))
    }

    fn advance_version_lookup(
        op: &mut IncomingVersionReplicationOperation,
        group_id: Ulid,
    ) -> Effect {
        op.manifest_policy = Some(op.target_authorization_path(group_id));
        op.writer_policy = Some(op.target_authorization_path(group_id));
        let effects = op.start();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let mut effects = load_routing(op, GroupRoutingInputs::default());
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadExistingVersion
        );
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        effects.remove(0)
    }

    fn advance_blob_lookup(
        op: &mut IncomingVersionReplicationOperation,
    ) -> aruna_core::types::Effects {
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));
        effects
    }

    /// The apply transaction's drift re-check answered with an absent bucket
    /// and an absent subject, which an ungoverned replica passes.
    fn no_drift() -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![0u8; 4].into(), None), (vec![1u8; 4].into(), None)],
        })
    }

    /// The drift re-check echoing the bucket the negotiation read, which a
    /// trace that really read a bucket must answer.
    fn bucket_drift(bucket_info: &BucketInfo) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    vec![0u8; 4].into(),
                    Some(bucket_info.to_bytes().unwrap().into()),
                ),
                (vec![1u8; 4].into(), None),
            ],
        })
    }

    fn start_apply_transaction(op: &mut IncomingVersionReplicationOperation) -> Ulid {
        start_apply_with(op, None)
    }

    /// `bucket` must echo what negotiation stored, or None when no bucket was
    /// read; the drift re-check compares the two.
    fn start_apply_with(
        op: &mut IncomingVersionReplicationOperation,
        bucket: Option<aruna_core::types::Value>,
    ) -> Ulid {
        let txn_id = Ulid::generate();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::generate());

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckPurgeFence);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { txn_id: read_txn_id, .. })]
                if *read_txn_id == Some(txn_id)
        ));
        let _effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckDrift);
        // The apply transaction re-reads the destination default and the local
        // subject before it exposes anything.
        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![0u8; 4].into(), bucket), (vec![1u8; 4].into(), None)],
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::VerifyReplaced);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE && *read_txn_id == Some(txn_id)
        ));
        let value = op
            .replaced_version
            .as_ref()
            .map(|version| version.to_bytes().unwrap().into());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
                if key_space == BLOB_HEAD_KEYSPACE && *read_txn_id == Some(txn_id)
        ));
        txn_id
    }

    #[test]
    fn purge_fence_rejects() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::generate());
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([12; 16]),
            scope: StoragePurgeScope::File {
                bucket: manifest.bucket,
                key: manifest.key,
            },
        };

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(fence.to_bytes().unwrap().into()),
        }));

        assert!(matches!(
            op.output,
            Some(Err(IncomingVersionReplicationError::PurgeFence(
                PurgeFenceError::Suspended
            )))
        ));
    }

    #[test]
    fn existing_version_skips() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );

        let _effects = advance_version_lookup(&mut op, Ulid::generate());

        let version = BlobVersion::materialized(
            manifest.blob.as_ref().unwrap().hash,
            BackendRef::node_default(),
            manifest.created_at,
            manifest.created_by,
            None,
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::AlreadyReplicatedVersion
            )
        ));
    }

    #[test]
    fn existing_delete_skips() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest.clone(),
        );

        let _effects = advance_version_lookup(&mut op, test_group_id());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::deleted(manifest.created_at, manifest.created_by)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::AlreadyReplicatedVersion
            )
        ));
    }

    #[test]
    fn reference_requests_metadata() {
        let manifest = make_reference_manifest();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let _effects = advance_version_lookup(&mut op, test_group_id());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn reference_writes_version() {
        let manifest = make_reference_manifest();
        let expected_source = manifest.source.clone().unwrap();
        let expected_metadata = manifest.reference_metadata.clone().unwrap();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.txn_id = Some(Ulid::generate());

        let effects = op.write_blob_version();
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected reference version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();

        assert!(matches!(
            version.state,
            BlobVersionState::Reference {
                source,
                cached_metadata,
                ..
            } if source == expected_source && cached_metadata == expected_metadata
        ));
        let usage = op.usage_delta().unwrap();
        assert_eq!(usage.logical_bytes, 0);
        assert_eq!(usage.referenced_bytes, 1_000_000);
    }

    #[test]
    fn version_binds_publisher() {
        // A forged manifest cannot forge attribution: the persisted version is
        // bound to the authenticated publisher, never to its self-asserted user.
        let publisher = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let forged = UserId::local(Ulid::from_bytes([9u8; 16]), test_realm_id());
        let mut manifest = make_reference_manifest();
        manifest.created_by = forged;
        manifest.auth_context.user_id = forged;
        manifest.writer_auth_context = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(publisher);
        op.txn_id = Some(Ulid::generate());

        let effects = op.write_blob_version();
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected reference version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert_eq!(version.published_by, Some(publisher));
        assert_eq!(version.created_by, forged);
    }

    #[test]
    fn valid_advance() {
        let (manifest, previous, publisher) = advance_fixture();
        let advance = manifest.reference_advance.unwrap();
        let version_id = manifest.version_id;
        assert!(manifest.writer_auth_context.is_none());
        let mut op = advance_operation(manifest, publisher);

        advance_version_lookup(&mut op, test_group_id());
        let existing = op.reference_version().unwrap();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
        let txn_id = start_apply_with(
            &mut op,
            Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
        );
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                CurrentVersionPointer::new_with_generation(
                    advance.predecessor,
                    advance.generation - 1,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(previous.to_bytes().unwrap().into()),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id: write_txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected head advance write")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        assert_eq!(*write_txn_id, Some(txn_id));
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(version_id, advance.generation)
        );
        assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        let [Effect::Storage(StorageEffect::Write { key_space, .. })] = effects.as_slice() else {
            panic!("expected successor version write")
        };
        assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);
    }

    // One advance mints exactly one successor: a repeated, skipped or overflowing
    // count would let a publisher reset the cap by replaying advances.
    #[test]
    fn advance_rejects_counts() {
        let (manifest, previous, publisher) = advance_fixture();
        for count in [Some(0), Some(2), None] {
            let mut replayed = manifest.clone();
            replayed.reference_advance_count = count;
            let op = advance_operation(replayed, publisher);
            assert!(op.validate_advance(&previous).is_err());
        }

        let mut exhausted = previous.clone();
        exhausted.state = BlobVersionState::Reference {
            source: manifest.source.clone().unwrap(),
            cached_metadata: manifest.reference_metadata.clone().unwrap(),
            last_refresh: SystemTime::UNIX_EPOCH,
            advance_count: u16::MAX,
        };
        assert_advance_invalid(manifest, publisher, exhausted);
    }

    // Repair and snapshot replication reconstruct the reference with the cap the
    // manifest carries, and refuse a manifest that omits it.
    #[test]
    fn reference_keeps_count() {
        let mut manifest = make_reference_manifest();
        manifest.reference_advance_count = Some(12);
        let publisher = iroh::SecretKey::from_bytes(&[23u8; 32]).public();
        let op = advance_operation(manifest.clone(), publisher);
        assert_eq!(op.reference_version().unwrap().advance_count(), Some(12));

        manifest.reference_advance_count = None;
        let op = advance_operation(manifest, publisher);
        assert_eq!(
            op.reference_version().unwrap_err(),
            IncomingVersionReplicationError::MissingReferenceAdvanceCount
        );
    }

    #[test]
    fn advance_needs_bucket() {
        let (manifest, _, publisher) = advance_fixture();
        let mut op = advance_operation(manifest, publisher);
        op.manifest_policy = Some(op.target_authorization_path(test_group_id()));

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(!op.create_attempted);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::DestinationBucketNotFound
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn advance_requires_predecessor() {
        let (manifest, _, publisher) = advance_fixture();
        let advance = manifest.reference_advance.unwrap();
        let other = Ulid::from_bytes([33u8; 16]);
        let pointers = [
            None,
            Some(CurrentVersionPointer::new_with_generation(
                advance.predecessor,
                advance.generation - 2,
            )),
            Some(CurrentVersionPointer::new_with_generation(
                other,
                advance.generation - 1,
            )),
            Some(CurrentVersionPointer::new_with_generation(
                advance.predecessor,
                advance.generation,
            )),
        ];

        for pointer in pointers {
            let mut op = advance_operation(manifest.clone(), publisher);
            start_apply_transaction(&mut op);
            op.step(Event::Storage(StorageEvent::ReadResult {
                key: vec![0u8; 4].into(),
                value: pointer.map(|pointer| pointer.to_bytes().unwrap().into()),
            }));
            assert!(matches!(
                op.output,
                Some(Err(
                    IncomingVersionReplicationError::InvalidReferenceAdvance
                ))
            ));
        }

        let mut op = advance_operation(manifest, publisher);
        start_apply_transaction(&mut op);
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                CurrentVersionPointer::new_with_generation(
                    advance.predecessor,
                    advance.generation - 1,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert!(matches!(
            op.output,
            Some(Err(
                IncomingVersionReplicationError::InvalidReferenceAdvance
            ))
        ));
    }

    #[test]
    fn advance_checks_publisher() {
        let (manifest, mut previous, publisher) = advance_fixture();
        previous.published_by = Some(iroh::SecretKey::from_bytes(&[36u8; 32]).public());

        assert_advance_invalid(manifest, publisher, previous);
    }

    #[test]
    fn advance_preserves_identity() {
        let (manifest, previous, publisher) = advance_fixture();

        let mut changed_creator = previous.clone();
        changed_creator.created_by = UserId::local(Ulid::from_bytes([37u8; 16]), test_realm_id());
        assert_advance_invalid(manifest.clone(), publisher, changed_creator);

        let mut changed_metadata = previous.clone();
        changed_metadata
            .metadata
            .insert("s3-key".to_string(), "changed".to_string());
        assert_advance_invalid(manifest.clone(), publisher, changed_metadata);

        let mut changed_binding = previous.clone();
        let BlobVersionState::Reference { source, .. } = &mut changed_binding.state else {
            panic!("expected reference predecessor")
        };
        source.descriptor.source_path = "changed/path".to_string();
        assert_advance_invalid(manifest.clone(), publisher, changed_binding);

        let mut non_native_manifest = manifest;
        non_native_manifest.source.as_mut().unwrap().descriptor.kind = SourceConnectorKind::Http;
        let mut non_native_previous = previous;
        let BlobVersionState::Reference { source, .. } = &mut non_native_previous.state else {
            panic!("expected reference predecessor")
        };
        source.descriptor.kind = SourceConnectorKind::Http;
        assert_advance_invalid(non_native_manifest, publisher, non_native_previous);
    }

    #[test]
    fn advance_rejects_collision() {
        let (manifest, _, publisher) = advance_fixture();
        let mut op = advance_operation(manifest, publisher);
        let mut collision = op.reference_version().unwrap();
        collision
            .metadata
            .insert("collision".to_string(), "true".to_string());
        advance_version_lookup(&mut op, test_group_id());

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(collision.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::InvalidReferenceAdvance
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn later_head_noop() {
        let (manifest, _, publisher) = advance_fixture();
        let advance = manifest.reference_advance.unwrap();
        let mut op = advance_operation(manifest, publisher);
        let duplicate = op.reference_version().unwrap();
        advance_version_lookup(&mut op, test_group_id());

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(duplicate.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
        let txn_id = start_apply_with(
            &mut op,
            Some(make_bucket_info(test_group_id()).to_bytes().unwrap().into()),
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                CurrentVersionPointer::new_with_generation(
                    Ulid::from_bytes([50u8; 16]),
                    advance.generation + 1,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id: write_txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected downstream obligation write")
        };
        assert_eq!(key_space, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE);
        assert_eq!(*write_txn_id, Some(txn_id));
        let obligation = LiveReplicationObligationRecord::from_bytes(value).unwrap();
        assert_eq!(obligation.reference_advance, Some(advance));
        assert_eq!(op.usage_delta().unwrap(), UsageDelta::default());

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    #[test]
    fn replacement_cleans_metadata() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let version_id = manifest.version_id;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        let txn_id = Ulid::generate();
        op.txn_id = Some(txn_id);
        op.destination_group_id = Some(test_group_id());
        op.replaced_version = Some(BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            SystemTime::now(),
            test_user_id(),
            None,
        ));

        let effects = op.read_replaced_metadata();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, txn_id: effect_txn, .. })]
                if key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE
                    && *effect_txn == Some(txn_id)
        ));

        let part_key = MultipartObjectMetadataKey::part(version_id, 3)
            .to_bytes()
            .unwrap();
        let mut effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(part_key.clone().into(), vec![1u8].into())],
            next_start_after: None,
        }));
        let Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: effect_txn,
        }) = effects.remove(0)
        else {
            panic!("expected replacement metadata batch delete")
        };
        assert_eq!(effect_txn, Some(txn_id));
        let summary_key = MultipartObjectMetadataKey::summary(version_id)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE && key.as_ref() == summary_key
        }));
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE && key.as_ref() == part_key
        }));
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == HASH_PATHS_INDEX_KEYSPACE
                && HashPathIndexKey::from_bytes(key.as_ref())
                    .is_ok_and(|index| index.blake3_hash == [9u8; 32])
        }));

        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: deletes,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteReclaimCandidate
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_RECLAIM_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
    }

    #[test]
    fn replacement_queues_reclaim() {
        // The copy a replaced materialized version named is unreferenced once
        // the replacement names a different one, and only this enqueue frees it.
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        op.txn_id = Some(Ulid::generate());
        op.replaced_version = Some(BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            SystemTime::now(),
            test_user_id(),
            None,
        ));

        assert_eq!(
            op.replaced_reclaim_key(),
            Some(ReclaimCandidateKey::new(
                BackendRef::node_default(),
                [9u8; 32]
            ))
        );

        // The replacement adopting the very same copy must not queue it.
        let mut adopted = make_location();
        adopted.backend = BackendRef::node_default();
        adopted
            .hashes
            .insert("blake3".to_string(), [9u8; 32].to_vec());
        op.received_blob = Some(ReceivedBlob::reserved(adopted));
        assert_eq!(op.replaced_reclaim_key(), None);
    }

    #[test]
    fn replaced_version_fenced() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let prior = BlobVersion::deleted(manifest.created_at, manifest.created_by);
        op.replaced_version = Some(prior);
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::generate());

        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        let effects = op.step(no_drift());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let current = BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            manifest.created_at,
            manifest.created_by,
            None,
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(current.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            op.output,
            Some(Err(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionConflict
            )))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn changed_reference_updates() {
        let manifest = make_reference_manifest();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest.clone(),
        );
        let _effects = advance_version_lookup(&mut op, test_group_id());
        let mut metadata = manifest.reference_metadata.clone().unwrap();
        metadata.etag = Some("old-etag".to_string());
        let existing = BlobVersion::reference(
            manifest.source.clone().unwrap(),
            metadata,
            manifest.created_at,
            manifest.created_by,
            manifest.created_at,
        );

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn hop_limit_rejects() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 5,
        });
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::HopLimitExceeded
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn rejects_manifest_size() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.metadata.insert(
            "metadata".to_string(),
            "x".repeat(MAX_REPLICATION_VALUE_BYTES + 1),
        );
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            "Failed to convert from str: replication manifest entry is too large",
        );
    }

    #[test]
    fn rejects_user_realm() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.auth_context.user_id =
            UserId::local(Ulid::generate(), RealmId::from_bytes([8u8; 32]));
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::RealmMismatch
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn obligation_keeps_origin() {
        let origin = SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 2,
        };
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.origin = Some(origin.clone());
        manifest.upstream_sources.push(
            aruna_core::structs::ArunaArn::s3_bucket(
                test_realm_id(),
                iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
                "source",
            )
            .unwrap(),
        );
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.manifest.writer_auth_context = Some(op.manifest.auth_context.clone());

        let effects = op.write_live_obligation();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected live replication obligation write")
        };
        let obligation = LiveReplicationObligationRecord::from_bytes(value).unwrap();
        assert_eq!(obligation.origin, Some(origin));
        assert_eq!(obligation.upstream_sources, op.manifest.upstream_sources);
    }

    #[test]
    fn obligation_keeps_lineage() {
        let (mut manifest, _, publisher) = advance_fixture();
        let reader = manifest.auth_context.clone();
        let advance = manifest.reference_advance.unwrap();
        let origin = SyncOrigin {
            relationship_id: Ulid::from_bytes([44u8; 16]),
            hop_count: 2,
        };
        let source = aruna_core::structs::ArunaArn::s3_bucket(
            test_realm_id(),
            iroh::SecretKey::from_bytes(&[45u8; 32]).public(),
            "source",
        )
        .unwrap();
        manifest.origin = Some(origin.clone());
        manifest.upstream_sources = vec![source.clone()];
        let txn_id = Ulid::from_bytes([47u8; 16]);
        let mut op = advance_operation(manifest, publisher);
        op.txn_id = Some(txn_id);

        let effects = op.write_live_obligation();

        let [
            Effect::Storage(StorageEffect::Write {
                value,
                txn_id: write_txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected live replication obligation write")
        };
        assert_eq!(*write_txn_id, Some(txn_id));
        let obligation = LiveReplicationObligationRecord::from_bytes(value).unwrap();
        assert_eq!(obligation.auth_context, reader);
        assert_eq!(obligation.reference_advance, Some(advance));
        assert_eq!(obligation.origin, Some(origin));
        assert_eq!(obligation.upstream_sources, vec![source]);
    }

    #[test]
    fn quota_excess_rejects() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let group_id = test_group_id();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        let _effects = advance_version_lookup(&mut op, group_id);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
        config.quota = QuotaConfig {
            default_group_quota_bytes: Some(1),
            grace_factor_percent: 100,
            ..QuotaConfig::default()
        };
        let config_bytes = postcard::to_allocvec(&config).unwrap();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(config_bytes.clone().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::StartQuotaCheck);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));

        let txn_id = Ulid::generate();
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::EnforceQuota);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { txn_id: read_txn_id, .. })]
                if *read_txn_id == Some(txn_id)
        ));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(config_bytes.into()),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::FinishQuotaCheck);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        match message_from_effect(&effects[0]) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert_eq!(reason, "quota"),
            other => panic!("expected quota rejection, got {other:?}"),
        }
    }

    #[test]
    fn full_backend_rejects() {
        // Replication now routes through the quota-marked catalog, so a full
        // destination backend owes the sender a reason before any transfer.
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_routing(routing);

        let _effects = advance_version_lookup(&mut op, group_id);
        let effects = advance_blob_lookup(&mut op);
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        match message_from_effect(&effects[0]) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert!(reason.contains("quota"), "unexpected reason: {reason}"),
            other => panic!("expected a rejected negotiation, got {other:?}"),
        }
    }

    #[test]
    fn full_backend_dedupes() {
        // A blob the destination already holds stores no bytes, so its cap has
        // nothing left to protect.
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let existing = manifest
            .blob
            .as_ref()
            .map(|blob| blob.location.clone())
            .unwrap();
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_routing(routing);

        let _effects = advance_version_lookup(&mut op, group_id);
        advance_blob_lookup(&mut op);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn marker_ignores_quota() {
        // A delete marker stores no bytes, so a full destination must still let
        // the tombstone converge.
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::DeleteMarker),
        )
        .with_routing(routing);

        let _effects = advance_version_lookup(&mut op, group_id);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn stale_pointer_skips() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = Some(10);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let txn_id = start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 20);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, txn_id: write_txn_id, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE && *write_txn_id == Some(txn_id)
        ));
        assert_eq!(op.object_delta, 0);
        assert_eq!(op.usage_delta().unwrap().objects, 0);
    }

    #[test]
    fn rejects_missing_generation() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let txn_id = Ulid::generate();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);

        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        let effects = op.step(no_drift());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            &op.output,
            Some(Err(
                IncomingVersionReplicationError::MissingCurrentVersionGeneration
            ))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn rejects_bad_pointer() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(vec![255, 255, 255].into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            &op.output,
            Some(Err(IncomingVersionReplicationError::ConversionError(_)))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn stale_pointer_writes() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = Some(1);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 2);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert!(version.is_deleted());
        assert_eq!(version.created_by, manifest.created_by);
    }

    #[test]
    fn preserves_source_binding() {
        let source = make_source_binding();
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.source = Some(source.clone());
        manifest
            .metadata
            .insert("mtime".to_string(), "1753272000.123456789".to_string());
        let expected_metadata = manifest.metadata.clone();
        manifest.current_version = false;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.txn_id = Some(Ulid::generate());
        op.destination_group_id = Some(Ulid::generate());
        op.existing_blob_location = Some(make_location());

        let effects = op.write_version();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert_eq!(version.source_binding(), Some(&source));
        assert_eq!(version.metadata, expected_metadata);
    }

    #[test]
    fn indexes_noncurrent_version() {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.current_version = false;
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let group_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        op.txn_id = Some(Ulid::generate());
        op.destination_group_id = Some(group_id);
        op.existing_blob_location = Some(make_location());

        let effects = op.write_version();
        let [Effect::Storage(StorageEffect::Write { key_space, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        let [Effect::Storage(StorageEffect::Write { key_space, key, .. })] = effects.as_slice()
        else {
            panic!("expected hash path index write")
        };
        assert_eq!(key_space, HASH_PATHS_INDEX_KEYSPACE);
        let index_key = HashPathIndexKey::from_bytes(key.as_ref()).unwrap();
        assert_eq!(index_key.blake3_hash, [1u8; 32]);
        assert_eq!(index_key.version_id, manifest.version_id);
        assert_eq!(index_key.group_id, group_id);
        assert_eq!(index_key.bucket, manifest.bucket);
        assert_eq!(index_key.key, manifest.key);

        // The replica registers its managed copy in the same transaction.
        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the managed-copy registration")
        };
        assert_eq!(key_space, aruna_core::keyspaces::MANAGED_COPY_KEYSPACE);
        assert_eq!(
            aruna_core::structs::ManagedCopyRecord::from_bytes(value.as_ref())
                .unwrap()
                .version,
            aruna_core::structs::VersionKey::new(
                &manifest.bucket,
                &manifest.key,
                manifest.version_id
            )
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteLiveObligation
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::UpdateUsage);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![0].into(), None), (vec![1].into(), None)],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { .. })]
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CommitTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
    }

    #[test]
    fn newer_generation_rollback() {
        let existing_version_id = Ulid::from_bytes([9u8; 16]);
        let incoming_version_id = Ulid::from_bytes([1u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(20);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let txn_id = start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 10);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadCurrentVersion
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::materialized(
                    [2u8; 32],
                    BackendRef::node_default(),
                    SystemTime::now(),
                    test_user_id(),
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id: write_txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected blob head write")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        assert_eq!(*write_txn_id, Some(txn_id));
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(
                incoming_version_id,
                manifest.current_version_generation.unwrap()
            )
        );
        assert_eq!(op.object_delta, -1);
        assert_eq!(op.usage_delta().unwrap().objects, -1);
    }

    #[test]
    fn materialized_restores_object() {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.current_version_generation = Some(2);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 1);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadCurrentVersion
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::deleted(SystemTime::now(), test_user_id())
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        let delta = op.usage_delta().unwrap();
        assert_eq!(delta.objects, 1);
        assert_eq!(delta.logical_bytes, 42);
    }

    #[test]
    fn higher_ulid_skips() {
        // A same-generation incoming version cannot replace the node-local head.
        let existing_version_id = Ulid::from_bytes([1u8; 16]);
        let incoming_version_id = Ulid::from_bytes([9u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(7);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
    }

    #[test]
    fn lower_ulid_skips() {
        let existing_version_id = Ulid::from_bytes([9u8; 16]);
        let incoming_version_id = Ulid::from_bytes([1u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(7);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
    }

    #[test]
    fn canonical_auth_path() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.bucket = "bucket-a".to_string();
        manifest.key = "nested/file.txt".to_string();
        let local_node_id = iroh::SecretKey::generate().public();
        let local_realm_id = RealmId::from_bytes([7u8; 32]);
        let op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            local_node_id,
            local_realm_id,
            manifest,
        );
        let group_id = Ulid::from_bytes([4u8; 16]);

        assert_eq!(
            op.target_authorization_path(group_id),
            aruna_core::structs::object_permission_path(
                local_realm_id,
                group_id,
                local_node_id,
                "bucket-a",
                "nested/file.txt",
            )
        );
    }

    #[test]
    fn mismatch_requests_transfer() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));

        let mut mismatched_location = make_location();
        mismatched_location.blob_size += 1;
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(mismatched_location.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));
    }

    #[test]
    fn missing_blob_location() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);

        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));
    }

    /// Drives an incoming materialized version to the existing-copy probe under
    /// the given bucket rules and group inputs.
    fn probe_backend(
        rules: Vec<StorageRoutingRule>,
        inputs: GroupRoutingInputs,
    ) -> (
        IncomingVersionReplicationOperation,
        aruna_core::types::Effects,
    ) {
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        let mut bucket_info = make_bucket_info(test_group_id());
        bucket_info.storage_routing = rules;
        op.manifest_policy = Some(op.target_authorization_path(bucket_info.group_id));
        op.writer_policy = Some(op.target_authorization_path(bucket_info.group_id));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(bucket_info.to_bytes().unwrap().into()),
        }));
        load_routing(&mut op, inputs);
        let effects = advance_blob_lookup(&mut op);
        (op, effects)
    }

    fn group_backend_key(backend_id: Ulid) -> Vec<u8> {
        BlobLocationKey::new([1u8; 32], BackendRef::Group(backend_id)).to_bytes()
    }

    fn probed_key(effects: &aruna_core::types::Effects) -> Vec<u8> {
        let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
            panic!("expected one location read, got {effects:?}")
        };
        key.to_vec()
    }

    #[test]
    fn refuses_vanished_copy() {
        // The adopted copy is re-read in the transaction, so a sweep that
        // removed it in between must fail the apply instead of committing.
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        let txn_id = Ulid::generate();
        op.txn_id = Some(txn_id);
        op.destination_group_id = Some(test_group_id());
        op.existing_blob_location = Some(make_location());

        let effects = op.begin_blob_location();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::VerifyExistingBlob
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE && *read_txn == Some(txn_id)
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(
            op.output,
            Some(Err(IncomingVersionReplicationError::ExistingBlobChanged))
        );
    }

    #[test]
    fn probes_rule_backend() {
        // The probe must ask about the backend the bucket rule names.
        let backend_id = Ulid::from_bytes([4u8; 16]);
        let (op, effects) = probe_backend(
            vec![StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTarget::Backend(BackendRef::Group(backend_id)),
            }],
            GroupRoutingInputs {
                default_target: None,
                backend_ids: BTreeSet::from([backend_id]),
            },
        );

        assert_eq!(
            op.resolve_destination().unwrap().backend,
            BackendRef::Group(backend_id)
        );
        assert_eq!(probed_key(&effects), group_backend_key(backend_id));
    }

    #[test]
    fn probes_group_default() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let (_op, effects) = probe_backend(
            Vec::new(),
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            },
        );

        assert_eq!(probed_key(&effects), group_backend_key(backend_id));
    }

    #[test]
    fn keeps_loaded_inputs() {
        // The version-only path resolves from the same inputs the probe used.
        let backend_id = Ulid::from_bytes([6u8; 16]);
        let (mut op, _effects) = probe_backend(
            Vec::new(),
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            },
        );
        let mut location = make_location();
        location.backend = BackendRef::Group(backend_id);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: group_backend_key(backend_id).into(),
            value: Some(location.to_bytes().unwrap().into()),
        }));

        assert_eq!(
            op.negotiation_result,
            Some(ReplicationNegotiationResult::NeedVersionOnly)
        );
        assert_eq!(
            op.resolve_destination().unwrap().backend,
            BackendRef::Group(backend_id)
        );
    }

    #[test]
    fn rejects_mismatched_blob() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let mut mismatched_location = make_location();
        mismatched_location.blob_size += 1;

        op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::ReceiveBlob;

        let effects = op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: mismatched_location.clone(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete {
                location: mismatched_location
            })
        );
    }

    #[test]
    fn write_cleanup_rejects() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
        let received = make_location();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::ReceiveBlob;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
            location: received.clone(),
            message: "marker write failed".to_string(),
        })));
        assert_eq!(
            op.received_blob.as_ref().map(|blob| blob.location.clone()),
            Some(received.clone())
        );
        assert!(op.received_blob.as_ref().unwrap().cleanup_on_abort);
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete { location: received })
        );
    }

    #[test]
    fn unbuildable_bucket_rejects() {
        // One create attempt, still missing, then reject and close the stream.
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.manifest_policy = Some(op.target_authorization_path(test_group_id()));
        op.writer_policy = Some(op.target_authorization_path(test_group_id()));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CreateDestinationBucket
        );
        op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
            result: Err("boom".to_string()),
        }));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::DestinationBucketNotFound
                .to_string()
                .as_str(),
        );

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn rejects_denied_writer() {
        // A replica whose original writer lacks WRITE on the destination path
        // is refused during negotiation.
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let stream_id = Ulid::generate();
        let group_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        )
        .with_writer_policy(None);
        op.manifest_policy = Some(op.target_authorization_path(group_id));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let effects = load_routing(&mut op, GroupRoutingInputs::default());
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WriterPermissionDenied
                .to_string()
                .as_str(),
        );

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn rejects_missing_policy() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let group_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.manifest_policy = Some(op.target_authorization_path(group_id));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let effects = load_routing(&mut op, GroupRoutingInputs::default());
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WriterPermissionDenied
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn rejects_manifest_policy() {
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::DeleteMarker),
        )
        .with_manifest_policy(None);
        let group_id = Ulid::generate();
        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let effects = load_routing(&mut op, GroupRoutingInputs::default());
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::ManifestPermissionDenied
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn rejects_missing_writer() {
        // Ordinary replication must carry its durable original writer.
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WriterPermissionDenied
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn rejects_relationship_writer() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 0,
        });
        manifest.writer_auth_context = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WriterPermissionDenied
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn allows_writer_policy() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let group_id = Ulid::generate();
        let path = op.target_authorization_path(group_id);
        op = op
            .with_manifest_policy(Some(path.clone()))
            .with_writer_policy(Some(path));

        let _effects = advance_version_lookup(&mut op, group_id);
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadExistingVersion
        );
    }

    #[test]
    fn delete_marker_only() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_version_lookup(&mut op, Ulid::generate());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn missing_blob_transfer() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));
    }

    #[test]
    fn failure_rejects_first() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::generate();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.state = IncomingVersionReplicationState::ApplyHeadTransition;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn failure_deletes_blobs() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
        let received = make_location();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::WriteBlobLocation;
        op.txn_id = Some(txn_id);
        op.received_blob = Some(ReceivedBlob::reserved(received.clone()));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete { location: received })
        );

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn unknown_commit_preserves() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let received = make_location();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobAndVersion);
        op.txn_id = Some(txn_id);
        op.received_blob = Some(ReceivedBlob::reserved(received.clone()));
        let release_id = received.ulid;

        let effects = op.commit_or_cleanup();
        let [
            Effect::Storage(StorageEffect::Write {
                key,
                value,
                txn_id: write_txn,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected transactional reconciliation row, got {effects:?}")
        };
        assert_eq!(*write_txn, Some(txn_id));
        assert_eq!(key.as_ref(), received.ulid.to_bytes().as_slice());
        assert!(matches!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::ReconcileWrite {
                owner: WriteOwner::Blob {
                    blake3,
                    realm_id,
                    ..
                },
                ..
            } if blake3 == [1u8; 32] && realm_id == test_realm_id()
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CommitTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: id })] if *id == txn_id
        ));
        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReleaseReservation
        );
        assert_eq!(op.txn_id, None);
        assert!(!op.received_blob.as_ref().unwrap().cleanup_on_abort);
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: release_id
            })]
        );
        let effects = op.step(Event::Blob(BlobEvent::ReservationReleased {
            id: release_id,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
        assert!(
            !effects
                .iter()
                .any(|effect| { matches!(effect, Effect::Storage(StorageEffect::Write { .. })) })
        );
        let effects = op.abort();
        assert!(
            !effects
                .iter()
                .any(|effect| { matches!(effect, Effect::Blob(BlobEffect::Delete { .. })) })
        );
    }

    #[test]
    fn release_after_commit() {
        let received = make_location();
        let id = received.ulid;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        op.state = IncomingVersionReplicationState::CommitTransaction;
        op.txn_id = Some(Ulid::generate());
        op.received_blob = Some(ReceivedBlob::reserved(received));

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::generate(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation { id: observed })] if *observed == id
        ));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReleaseReservation
        );

        let effects = op.step(Event::Blob(BlobEvent::ReservationReleased { id }));
        assert_eq!(op.state, IncomingVersionReplicationState::ScheduleUsage);
        assert_eq!(effects.len(), 1);
    }

    #[test]
    fn conflict_commit_deletes() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
        let received = make_location();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::CommitTransaction;
        op.txn_id = Some(txn_id);
        op.received_blob = Some(ReceivedBlob::reserved(received.clone()));

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );
        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete { location: received })
        );
    }

    #[test]
    fn commit_abort_preserves() {
        let received = make_location();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        op.state = IncomingVersionReplicationState::CommitTransaction;
        op.txn_id = Some(txn_id);
        op.received_blob = Some(ReceivedBlob::reserved(received));

        let effects = op.abort();
        assert!(
            !effects
                .iter()
                .any(|effect| { matches!(effect, Effect::Blob(BlobEffect::Delete { .. })) })
        );
        assert!(effects.contains(&Effect::Storage(StorageEffect::AbortTransaction { txn_id })));
    }

    #[test]
    fn failure_without_delete() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::generate();
        let txn_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly);
        op.state = IncomingVersionReplicationState::ApplyHeadTransition;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn commit_preserves_blob() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
        let received = make_location();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::RegisterBlobInDht;
        op.received_blob = Some(ReceivedBlob::owned(received));
        op.apply_committed = true;

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    fn missing_bucket_op() -> IncomingVersionReplicationOperation {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.manifest_policy = Some(op.target_authorization_path(test_group_id()));
        op.writer_policy = Some(op.target_authorization_path(test_group_id()));
        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        op
    }

    #[test]
    fn missing_bucket_autocreates() {
        let op = missing_bucket_op();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CreateDestinationBucket
        );
        assert!(op.create_attempted);
        let info = op.destination_bucket_info();
        assert_eq!(info.group_id, test_group_id());
        assert_eq!(info.created_by, test_user_id());
        assert!(info.cors_configuration.is_none());
    }

    #[test]
    fn autocreate_rereads_bucket() {
        let mut op = missing_bucket_op();
        let effects = op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
            result: Ok(()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == S3_BUCKET_KEYSPACE
        ));
    }

    #[test]
    fn create_invalid_event() {
        let mut op = missing_bucket_op();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
    }

    /// Constructor-to-finalize trace of a materialized replica: every real
    /// transition runs, from the bucket probe through the blob transfer, the
    /// apply transaction and the committed apply acknowledgement.
    #[test]
    fn materialized_apply_traces_to_finalize() {
        let stream_id = trace_stream_id();
        let txn_id = trace_txn_id();
        let group_id = test_group_id();
        let received = make_location();
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(iroh::SecretKey::from_bytes(&[0x31; 32]).public())
        .with_now(trace_now());
        op.manifest_policy = Some(op.target_authorization_path(group_id));
        op.writer_policy = Some(op.target_authorization_path(group_id));

        let effects = op.start();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::LoadDestinationRouting
        );
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadExistingVersion
        );

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReceiveBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::HandleReplication { stream_id: id, .. })] if *id == stream_id
        ));

        let effects = op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: received.clone(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::StartTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckPurgeFence);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { txn_id: Some(id), .. })] if *id == txn_id
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckDrift);

        op.step(bucket_drift(&make_bucket_info(group_id)));
        assert_eq!(op.state, IncomingVersionReplicationState::VerifyReplaced);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobLocation);
        assert_eq!(op.received_blob.as_ref().unwrap().location, received);

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"location".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ApplyHeadTransition
        );

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"head-index".to_vec().into(),
        }));
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"version".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"index".to_vec().into(),
        }));
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"copy".to_vec().into(),
        }));
        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"obligation".to_vec().into(),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteLiveObligation
        );

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"obligation".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::UpdateUsage);

        op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![0].into(), None), (vec![1].into(), None)],
        }));
        op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteCleanupRow);

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CommitTransaction);

        op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReleaseReservation
        );

        op.step(Event::Blob(BlobEvent::ReservationReleased {
            id: received.ulid,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ScheduleUsage);

        op.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishUsageSnapshots,
            after: Duration::from_secs(1),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ScheduleLiveDrain);

        op.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainBlobReplicationQueue,
            after: Duration::from_secs(1),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::RegisterBlobInDht);

        op.step(Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
            key: DhtKeyId::from_bytes([1u8; 32]),
            remote_attempt_count: 0,
            remote_store_count: 0,
        })));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyComplete);

        op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);

        op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::Finish);
        assert!(op.is_complete());

        let result = op
            .finalize()
            .expect("trace finalizes")
            .expect("trace commits successfully");
        assert!(result.applied);
        assert_eq!(result.group_id, Some(group_id));
    }

    /// Constructor-to-finalize trace of a delete marker: no blob is
    /// transferred, the head is cleared and the apply acknowledgement closes
    /// the stream.
    #[test]
    fn delete_marker_apply_traces_to_finalize() {
        let stream_id = trace_stream_id();
        let txn_id = trace_txn_id();
        let group_id = test_group_id();
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            manifest,
        )
        .with_now(trace_now());
        op.manifest_policy = Some(op.target_authorization_path(group_id));
        op.writer_policy = Some(op.target_authorization_path(group_id));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        }));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));

        op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::StartTransaction);
        op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        op.step(bucket_drift(&make_bucket_info(group_id)));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ApplyHeadTransition
        );

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"head".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"version".to_vec().into(),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteLiveObligation
        );

        op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"obligation".to_vec().into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CommitTransaction);

        op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ScheduleUsage);
        op.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishUsageSnapshots,
            after: Duration::from_secs(1),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ScheduleLiveDrain);
        op.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainBlobReplicationQueue,
            after: Duration::from_secs(1),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyComplete);

        op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::Finish);

        let result = op
            .finalize()
            .expect("trace finalizes")
            .expect("delete marker commits successfully");
        assert!(result.applied);
        assert_eq!(result.group_id, Some(group_id));
    }

    /// A refused negotiation runs constructor-to-finalize without entering the
    /// apply at all and reports `applied: false`.
    #[test]
    fn rejected_negotiation_traces_to_finalize() {
        let stream_id = trace_stream_id();
        let group_id = test_group_id();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        )
        .with_now(trace_now());
        op.manifest_policy = Some(op.target_authorization_path(group_id));
        op.writer_policy = Some("/other/path".to_string());

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let effects = op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(&effects[0], "writer_access_denied");

        op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        op.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::Finish);

        let result = op
            .finalize()
            .expect("trace finalizes")
            .expect("rejection is a clean result");
        assert!(!result.applied);
        assert_eq!(result.group_id, Some(group_id));
    }

    /// Reclaim-candidate creation carries the one wall clock sampled when the
    /// operation was constructed, never a fresh per-phase timestamp.
    #[test]
    fn reclaim_candidate_pins_operation_time() {
        let mut op = IncomingVersionReplicationOperation::new(
            trace_stream_id(),
            iroh::SecretKey::from_bytes(&[0x30; 32]).public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        )
        .with_now(trace_now());
        op.txn_id = Some(trace_txn_id());
        op.replaced_version = Some(BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            fixed_created_at(),
            test_user_id(),
            None,
        ));

        let effects = op.write_replaced_candidate(ReclaimCandidateKey::new(
            BackendRef::node_default(),
            [9u8; 32],
        ));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected reclaim candidate write");
        };
        let candidate = aruna_core::structs::ReclaimCandidate::from_bytes(value.as_ref()).unwrap();
        assert_eq!(candidate.enqueued_at, trace_now());
    }
}

/// Gate acceptance for an incoming replica: nothing governed is admitted
/// without a compliant local destination, and a reference registers nothing.
#[cfg(test)]
mod gate_tests {
    use super::tests::{make_manifest, make_reference_manifest};
    use super::*;
    use crate::placement::policy::PolicyCacheEntry;
    use aruna_core::keyspaces::MANAGED_COPY_KEYSPACE;
    use aruna_core::structs::{
        PlacementPolicy, PlacementSelector, PlacementSubject, ReplicationItemKind, VerifiedPolicy,
    };
    use std::collections::BTreeMap;

    fn realm() -> RealmId {
        RealmId::from_bytes([3u8; 32])
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
                node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
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

    fn governed(rule: &VerifiedPolicy) -> IncomingVersionReplicationOperation {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.placement_policies = vec![rule.policy_ref()];
        IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            realm(),
            manifest,
        )
    }

    fn rejected(operation: &IncomingVersionReplicationOperation) -> bool {
        matches!(
            operation.negotiation_result,
            Some(ReplicationNegotiationResult::Rejected(_))
        )
    }

    #[test]
    fn denies_incoming_replica() {
        // The manifest's rule admits another location, so the negotiation that
        // would invite bytes is refused instead.
        let rule = policy("us-east");
        let mut operation = governed(&rule).with_gate(gate("eu-west"));
        operation.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion);

        let document = crate::tests::fixtures::policy::signed_document(realm(), &rule, 9);
        let cached = PolicyCacheEntry::verified(&document, 10)
            .to_bytes()
            .expect("entry encodes");
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(cached.into()),
        }));
        operation.step(crate::tests::fixtures::policy::authority(realm()));

        assert!(rejected(&operation));
    }

    #[test]
    fn missing_subject_refuses() {
        // A node that advertises no subject may hold nothing governed, so it
        // never invites the bytes.
        let rule = policy("eu-west");
        let mut operation = governed(&rule);
        operation.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion);
        assert!(rejected(&operation));
    }

    #[test]
    fn reference_registers_nothing() {
        // A reference materializes no bytes here, so no managed copy may claim
        // this node holds one.
        let manifest = make_reference_manifest();
        let mut operation = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            realm(),
            manifest,
        );
        operation.txn_id = Some(Ulid::generate());
        operation.destination_group_id = Some(Ulid::generate());

        operation.write_version();
        assert!(
            operation
                .pending_version_effects
                .iter()
                .all(|effect| !matches!(
                    effect,
                    Effect::Storage(StorageEffect::Write { key_space, .. })
                        if key_space == MANAGED_COPY_KEYSPACE
                ))
        );
    }
}
