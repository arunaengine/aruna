//! Owns the state machine that accepts an inbound object version and stores it locally.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::holders::dht_registration_effect;
use crate::blob::records::{HeadAliasContext, release_rows};
use crate::connectors::root_record::{compact_version, root_write};
use crate::distribution::inventory::{AddedVersion, InventoryChange, version_written};
use crate::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use crate::groups::storage_routing::load_group_inputs;
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, ReadPolicyError, gate_decision,
    split_drift_reads, union_refs, write_gate,
};
use crate::replication::error::ReplicationError;
use crate::replication::protocol::{VersionReplicationManifest, VersionReplicationMessage};
use crate::replication::queue::schedule_blob_drain;
use crate::s3::abe_copies::copy_row;
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::object::put::abe::{abe_reads, keyed, located_rows, parse_abe};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{AuthorizationError, BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    ABE_ARCHIVE_KEYSPACE, ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_CLEANUP_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, COPY_OWNER_KEYSPACE,
    NODE_SUBJECT_KEYSPACE, OBJECT_METADATA_KEYSPACE, PATHS_INDEX_KEYSPACE, PENDING_CLAIM_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
    SYNC_RECEIPT_KEYSPACE,
};
use aruna_core::managed_copy::{CopyRegistration, ManagedCopyError, register_entry};
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::table::searchable;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::node_subject::NODE_SUBJECT_KEY;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::abe::{
    AbeEffect, AbeEvent, EnvelopePlan, ObjectEnvelope, envelope_charge,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobCleanupWork, BlobHeadKey, BlobLocationKey,
    BlobVersion, BlobVersionState, BucketInfo, CopyOrigin, CopyOwner, CurrentVersionPointer,
    ResolvedBackend, VersionKey, WriteOwner, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::bucket_metadata::strip_release_keys;
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::distribution::inventory::{VersionEntry, logical_identity};
use aruna_core::structs::storage::distribution::source_root::RootEntry;
use aruna_core::structs::storage::distribution::{
    AdmissionError, DistributionBinding, OriginalEpoch,
};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan,
};
use aruna_core::structs::storage::format::{Compression, EncodingClass, StoredLayout};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::multipart::MultipartObjectKey;
use aruna_core::structs::storage::origin::{OriginVersion, SyncHop};
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::replication::{
    ReplicationItemKind, ReplicationNegotiationResult,
};
use aruna_core::structs::storage::replication_queue::LiveObligationRecord;
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
use aruna_core::structs::storage::replication_queue::live_obligation_entry;
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingError, StorageRoutingRule, resolve_backend,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::tagged_receipt;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{GroupId, Key, Value};
use std::time::SystemTime;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

/// One operation per inbound stream: every answer advances exactly one state, `next` names its
/// handler after the phase, and any failure after the reply rejects and deletes unowned bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionState {
    Init,
    ReadDestinationBucket,
    /// Reads the active key of an encrypting destination to capture its seal plan.
    ReadDestinationKey,
    CreateDestinationBucket,
    LoadDestinationRouting,
    ReadExistingVersion,
    /// Reads the claimed content hash of a pending version with the same id.
    ReadExistingClaim,
    ReadReplacedBlob,
    ReadQuotaConfig,
    /// Waits for the child that sums the group's usage in one read snapshot.
    EnforceQuota,
    ReadExistingBlob,
    /// Reads the admitted ABE parameters and epoch of an encrypting destination.
    ReadAbe,
    CreateEnvelope,
    PolicyGate,
    SendNegotiation,
    AwaitApproval,
    ReceiveBlob,
    /// Waits for the child that applies the version in one write transaction.
    Apply,
    CheckPurgeFence,
    CheckDrift,
    /// Reads the record of the destination's set to admit a publication.
    ReadPublicationRecord,
    /// Reads that record again inside the apply transaction.
    RecheckPublication,
    VerifyReplaced,
    ReadReplacedEnvelope,
    ReadReplacedRows,
    ReadReplacedMetadata,
    DeleteReplacedMetadata,
    WriteReclaimCandidate,
    FenceBackend,
    VerifyExistingBlob,
    WriteBlobLocation,
    ReadObjectLookup,
    /// Reads the head of a key whose row a historical item replaces with the other kind.
    ReadNamedHead,
    ReadCurrentVersion,
    ApplyHeadTransition,
    WriteBlobVersion,
    WriteMultipartMetadata,
    WriteLiveObligation,
    CheckCommitQuota,
    UpdateUsage,
    WriteCleanupRow,
    ReleaseReservation,
    ScheduleUsage,
    ScheduleLiveDrain,
    /// Hashes a pending replica at once when its key is already unlocked here.
    SchedulePromotion,
    RegisterBlobDht,
    SendApplyComplete,
    CloseConnection,
}

type Turn = Result<Next<IncomingVersionResult>, IncomingVersionError>;

impl From<IoError> for IncomingVersionError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

/// Sums a group's usage against its ceiling in one read snapshot; outputs whether it is exceeded.
#[derive(Debug, PartialEq)]
struct QuotaProbe {
    gate: QuotaGate,
}

impl Operation for QuotaProbe {
    type Output = bool;
    type Error = QuotaGateError;

    fn first(&mut self) -> Result<Next<bool>, QuotaGateError> {
        Ok(Next::Call(self.gate.start_calls()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, QuotaGateError> {
        Ok(match self.gate.step_calls(result)? {
            Some(call) => Next::Call(call),
            None => Next::Done(self.gate.is_exceeded()),
        })
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingVersionError {
    #[error(transparent)]
    Policy(#[from] aruna_core::structs::placement::policy::PlacementPolicyError),
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
    DestinationNotFound,
    #[error("could not load the destination group's routing inputs: {0}")]
    RoutingInputsFailed(String),
    #[error("writer_access_denied")]
    WriterPermissionDenied,
    #[error("manifest_access_denied")]
    ManifestPermissionDenied,
    #[error("Replication hop limit exceeded")]
    HopLimitExceeded,
    #[error("encrypted versions are not replicated")]
    EncryptedVersion,
    #[error("A bucket stored 1:1 in a provider bucket cannot receive replicas yet.")]
    MappedDestination,
    #[error("Reference replication manifest is missing source metadata")]
    MissingReferenceMetadata,
    #[error("Reference replication manifest is missing source binding")]
    MissingReferenceSource,
    #[error("Reference replication manifest is missing its advance count")]
    ReferenceCountMissing,
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
    MissingVersionGeneration,
    #[error("Destination current version not found")]
    CurrentVersionMissing,
    #[error("Invalid reference advance")]
    InvalidReferenceAdvance,
    #[error("A version with this id is stored with another origin")]
    OriginMismatch,
    #[error("Materialized replication manifest is missing blob info")]
    MissingBlobInfo,
    #[error("Materialized replication manifest is missing local blob location")]
    MissingBlobLocation,
    #[error("Replicated blob hash does not match manifest")]
    BlobHashMismatch,
    #[error("Replicated blob size does not match manifest")]
    BlobSizeMismatch,
    #[error("Existing blob copy changed before the version committed")]
    ExistingBlobChanged,
    #[error("Replaced multipart metadata exceeds the supported part limit")]
    MultipartMetadataOverflow,
    #[error("Unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: &'static str,
    },
    #[error("the destination bucket encrypts its objects, so it accepts no references")]
    EncryptedDestination,
    #[error(transparent)]
    BucketKey(#[from] BucketKeyError),
    /// The destination's key or stored format changed since the negotiation; retryable.
    #[error("the destination seal plan is stale")]
    StalePlan,
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Admission(#[from] AdmissionError),
    /// A member bucket takes only its coordinators' publications, and only it takes them.
    #[error("only a distributed bucket's coordinators publish to its member buckets")]
    ForeignPublication,
    #[error("the record of the destination's distributed bucket has not reached this node yet")]
    RecordMissing,
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
#[derive(Clone, Debug, PartialEq)]
struct ReceivedBlob {
    location: BackendLocation,
    /// Set until the apply commits; every abort before that must delete the
    /// copy, every failure after it must preserve the copy the commit owns.
    cleanup_on_abort: bool,
}

impl ReceivedBlob {
    fn reserved(location: BackendLocation) -> Self {
        Self {
            location,
            cleanup_on_abort: true,
        }
    }

    /// The copy is already owned by this node, so no abort may delete it.
    #[cfg(test)]
    fn owned(location: BackendLocation) -> Self {
        Self {
            location,
            cleanup_on_abort: false,
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
    /// This instance is the apply: one write transaction over the rows of the version.
    applying: bool,
    destination_group_id: Option<GroupId>,
    /// The destination bucket's own rules, so this receiver routes its replica
    /// with the tenant's rules and its own class table.
    destination_rules: Vec<StorageRoutingRule>,
    /// The destination bucket's compression: the replica is stored with this node's setting.
    destination_compression: Compression,
    /// Only a bucket bound to a distributed set orders its heads by original epoch.
    destination_bound: bool,
    /// The destination's binding, whose inventory the apply transaction maintains.
    destination_binding: Option<DistributionBinding>,
    /// The head this apply installs and the content of the written version, for the inventory.
    installed_head: Option<CurrentVersionPointer>,
    written_identity: Option<[u8; 32]>,
    written_pending: bool,
    destination_inputs: GroupRoutingInputs,
    create_attempted: bool,
    negotiation_result: Option<ReplicationNegotiationResult>,
    quota_ceiling: Option<u64>,
    quota_gate: Option<QuotaGate>,
    usage_update: Option<UsageCounterUpdate>,
    existing_blob_location: Option<BackendLocation>,
    replaced_version: Option<BlobVersion>,
    /// Whether the row this version id already had at the destination is a delete marker;
    /// `None` without such a row. The write replaces it.
    existing_marker: Option<bool>,
    advance_version_exists: bool,
    received_blob: Option<ReceivedBlob>,
    existing_current_pointer: Option<CurrentVersionPointer>,
    object_delta: i128,
    replaced_logical_bytes: u64,
    /// Set when an encrypted replacement leaves quota to the commit check, which knows the old
    /// envelope charge.
    defer_quota: bool,
    replaced_reference_bytes: u64,
    /// Envelope rows of the replaced version, deleted with its metadata.
    replaced_envelope: Vec<(String, Key)>,
    /// The rows the last replaced-envelope read asked for, in order.
    replaced_reads: Vec<(String, Key)>,
    pending_head: Option<PendingHeadTransition>,
    release_id: Option<Ulid>,
    apply_committed: bool,
    /// The apply may have committed: the received copy is kept and goes to reconciliation.
    outcome_unknown: bool,
    /// Why the run failed, in the words the sender classifies.
    failure: Option<String>,
    /// The result a run that ends without applying reports once its stream is closed.
    output: Option<IncomingVersionResult>,
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
    /// Refs the gate admitted: the manifest's set unioned with what the local
    /// version already carried, so a sender cannot drop an inherited ref.
    gated_refs: Vec<PlacementPolicyRef>,
    /// Destination details the gate decided on, re-read inside the apply
    /// transaction so a default or subject change cannot expose a stale replica.
    gated_bucket: Option<GatedBucket>,
    pending_negotiation: Option<ReplicationNegotiationResult>,
    /// How an encrypting destination seals the replica, captured at negotiation.
    seal_plan: Option<SealPlan>,
    destination_settings: Option<BucketEncryption>,
    /// The replica's own envelope, created before the transfer and published with its version.
    envelope: Option<ObjectEnvelope>,
    envelope_bytes: u64,
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
            applying: false,
            destination_group_id: None,
            destination_rules: Vec::new(),
            destination_compression: Compression::Off,
            destination_bound: false,
            destination_inputs: GroupRoutingInputs::default(),
            create_attempted: false,
            negotiation_result: None,
            quota_ceiling: None,
            quota_gate: None,
            usage_update: None,
            existing_blob_location: None,
            replaced_version: None,
            existing_marker: None,
            advance_version_exists: false,
            received_blob: None,
            existing_current_pointer: None,
            object_delta: 0,
            replaced_logical_bytes: 0,
            defer_quota: false,
            replaced_reference_bytes: 0,
            replaced_envelope: Vec::new(),
            replaced_reads: Vec::new(),
            pending_head: None,
            destination_binding: None,
            installed_head: None,
            written_identity: None,
            written_pending: false,
            release_id: None,
            apply_committed: false,
            outcome_unknown: false,
            failure: None,
            output: None,
            rocrate_limits: RoCrateLimits::default(),
            routing: NodeRouting::default(),
            destination_full: None,
            manifest_policy: None,
            writer_policy: None,
            gate_context: None,
            gated_refs: Vec::new(),
            gated_bucket: None,
            pending_negotiation: None,
            seal_plan: None,
            destination_settings: None,
            envelope: None,
            envelope_bytes: 0,
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
    #[cfg(test)]
    pub fn with_clock(mut self, clock: fn() -> SystemTime) -> Self {
        self.clock = EnqueueClock(clock);
        self
    }
}

impl IncomingVersionOperation {
    fn negotiate(&mut self) -> Turn {
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
        // The user may belong to another realm, but not anonymously; the serving realm is this one.
        let user_id = self.manifest.auth_context.user_id;
        if self.manifest.auth_context.realm_id != self.local_realm_id
            || (user_id.is_nil() && user_id.realm_id != self.local_realm_id)
        {
            return self.reject_negotiation(IncomingVersionError::RealmMismatch);
        }
        // A publication is admitted against the record instead of a writer's permissions.
        if self.manifest.writer_auth_context.is_none()
            && self.manifest.reference_advance.is_none()
            && self.manifest.distribution.is_none()
        {
            return self.reject_negotiation(IncomingVersionError::WriterPermissionDenied);
        }
        if self
            .manifest
            .writer_auth_context
            .as_ref()
            .is_some_and(|auth| auth.realm_id != self.local_realm_id)
        {
            return self.reject_negotiation(IncomingVersionError::RealmMismatch);
        }

        self.read_destination_bucket()
    }

    /// Whether the sender still waits for the outcome of an apply it was invited to.
    fn owes_rejection(&self) -> bool {
        !self.apply_committed
            && matches!(
                self.negotiation_result,
                Some(
                    ReplicationNegotiationResult::NeedVersionOnly
                        | ReplicationNegotiationResult::NeedBlobVersion
                        | ReplicationNegotiationResult::NeedSealedBlob(_)
                        | ReplicationNegotiationResult::NeedAbeBlob(..)
                )
            )
    }

    /// An apply whose commit outcome is unknown may own the received copy: it is kept, and the
    /// reconciliation row decides its fate from the committed rows.
    fn keep_received(&mut self) {
        if let Some(received) = self.received_blob.as_mut() {
            received.cleanup_on_abort = false;
            self.outcome_unknown = true;
        }
    }

    fn reconcile_call(&self) -> Option<IoCall> {
        match self.cleanup_row() {
            Ok(row) => row.map(|row| IoCall::Write(vec![row])),
            Err(error) => {
                warn!(%error, "A received copy could not be queued for reconciliation");
                None
            }
        }
    }
}

impl Operation for IncomingVersionOperation {
    type Output = IncomingVersionResult;
    type Error = IncomingVersionError;

    fn first(&mut self) -> Turn {
        match self.applying {
            true => self.check_purge_fence(),
            false => self.negotiate(),
        }
    }

    /// One answer per state, dispatched to the handler named after the accepted result. The
    /// phase order is documented on [`IncomingVersionState`].
    fn next(&mut self, result: IoResult) -> Turn {
        use IncomingVersionState as State;
        match self.state {
            // Negotiation: decide and send the reply.
            State::Init => self.negotiate(),
            State::ReadDestinationBucket => self.accept_destination_bucket(result.values()?),
            State::ReadDestinationKey => self.accept_destination_key(result.value()?),
            State::ReadExistingClaim => self.accept_existing_claim(result.value()?),
            State::CreateDestinationBucket => self.accept_bucket_created(result.event()?),
            State::LoadDestinationRouting => self.accept_routing_loaded(result.event()?),
            State::ReadExistingVersion => self.accept_existing_version(result.value()?),
            State::ReadReplacedBlob => self.accept_replaced_blob(result.value()?),
            State::ReadQuotaConfig => self.accept_quota_config(result.value()?),
            State::EnforceQuota => self.accept_quota_probe(result.child::<bool, QuotaGateError>()?),
            State::ReadExistingBlob => self.accept_existing_blob(result.value()?),
            State::ReadAbe => self.accept_abe(result.values()?),
            State::CreateEnvelope => self.accept_envelope(result.event()?),
            State::PolicyGate => {
                self.accept_policy_gate(result.child::<PolicyGateOutcome, ReadPolicyError>()?)
            }
            State::SendNegotiation => self.accept_reply_sent(result.event()?),
            State::AwaitApproval => self.accept_source_approval(result.event()?),
            // Receiving: accept the bytes and run the apply.
            State::ReceiveBlob => self.accept_blob_finish(result.event()?),
            State::Apply => {
                let applied = result.child::<IncomingVersionResult, IncomingVersionError>()?;
                self.accept_applied(applied)
            }
            State::ReadPublicationRecord => self.accept_publication_record(result.event()?),
            // Apply: expose the version; the runner commits with the outcome.
            State::CheckPurgeFence => self.accept_purge_fence(result.value()?),
            State::CheckDrift => self.accept_drift_check(result.values()?),
            State::RecheckPublication => self.accept_publication_recheck(result.event()?),
            State::VerifyReplaced => self.accept_replaced_version(result.value()?),
            State::ReadReplacedEnvelope => self.accept_replaced_envelope(result.values()?),
            State::ReadReplacedRows => self.accept_replaced_rows(result.values()?),
            State::ReadReplacedMetadata => self.accept_metadata_iterated(result.page()?),
            State::DeleteReplacedMetadata => {
                result.done()?;
                self.accept_metadata_deleted()
            }
            State::WriteReclaimCandidate => {
                result.done()?;
                self.replaced_version = None;
                self.write_hash_lookup()
            }
            State::FenceBackend => self.accept_backend_fence(result.value()?),
            State::VerifyExistingBlob => self.accept_verified_blob(result.value()?),
            State::WriteBlobLocation => {
                result.done()?;
                self.write_object_lookup()
            }
            State::ReadObjectLookup => self.accept_object_lookup(result.value()?),
            State::ReadNamedHead => self.accept_named_head(result.value()?),
            State::ReadCurrentVersion => self.accept_current_version(result.value()?),
            State::ApplyHeadTransition => {
                result.done()?;
                self.write_version()
            }
            State::WriteBlobVersion => {
                result.done()?;
                self.write_multipart_metadata()
            }
            State::WriteMultipartMetadata => {
                result.done()?;
                self.write_live_obligation()
            }
            State::WriteLiveObligation => {
                result.done()?;
                self.start_commit_quota()
            }
            State::CheckCommitQuota => self.accept_commit_quota(result),
            State::UpdateUsage => self.accept_usage_update(result),
            State::WriteCleanupRow => {
                result.done()?;
                self.finish_apply()
            }
            // After the commit: settle ownership and tell the sender.
            State::ReleaseReservation => self.accept_reservation_release(result.event()?),
            State::ScheduleUsage => self.accept_usage_schedule(result.event()?),
            State::ScheduleLiveDrain => self.accept_live_drain(result.event()?),
            State::SchedulePromotion => self.accept_promotion_scheduled(result.event()?),
            State::RegisterBlobDht => self.accept_blob_registration(result.event()?),
            State::SendApplyComplete => self.accept_completion_sent(result.event()?),
            State::CloseConnection => self.accept_connection_close(result.event()?),
        }
    }

    fn transaction(&self) -> TransactionMode {
        match self.applying {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }

    /// A failed run tells a waiting sender, deletes bytes no commit owns and closes the stream.
    /// While the apply may have committed the copy stays and goes to reconciliation.
    fn abort(&mut self) -> Vec<IoCall> {
        if self.applying {
            return Vec::new();
        }
        // A child failure was already judged; only a cut-off leaves the commit outcome open.
        if self.state == IncomingVersionState::Apply && self.failure.is_none() {
            self.keep_received();
        }
        let stream_id = self.stream_id;
        let mut calls = Vec::new();
        if self.outcome_unknown {
            calls.extend(self.reconcile_call());
            if let Some(received) = self.received_blob.as_ref() {
                let id = received.location.ulid;
                calls.push(Effect::Blob(BlobEffect::ReleaseReservation { id }).into());
            }
        }
        if self.owes_rejection() {
            let reason = self.failure.take();
            let reason = reason.unwrap_or_else(|| "version replication apply failed".to_string());
            if let Ok(payload) = VersionReplicationMessage::VersionApplyRejected(reason).to_bytes()
            {
                let send = BlobEffect::SendMessage { stream_id, payload };
                calls.push(Effect::Blob(send).into());
            }
        }
        let cleanup = self.received_blob.as_mut();
        if let Some(location) = cleanup.and_then(ReceivedBlob::take_cleanup) {
            calls.push(Effect::Blob(BlobEffect::Delete { location }).into());
        }
        calls.push(Effect::Blob(BlobEffect::CloseConnection { stream_id }).into());
        calls
    }
}

impl IncomingVersionOperation {
    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionState::Init => "Init",
            IncomingVersionState::ReadDestinationBucket => "ReadDestinationBucket",
            IncomingVersionState::ReadDestinationKey => "ReadDestinationKey",
            IncomingVersionState::ReadExistingClaim => "ReadExistingClaim",
            IncomingVersionState::CreateDestinationBucket => "CreateDestinationBucket",
            IncomingVersionState::LoadDestinationRouting => "LoadDestinationRouting",
            IncomingVersionState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionState::ReadReplacedBlob => "ReadReplacedBlob",
            IncomingVersionState::ReadQuotaConfig => "ReadQuotaConfig",
            IncomingVersionState::EnforceQuota => "EnforceQuota",
            IncomingVersionState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionState::ReadAbe => "ReadAbe",
            IncomingVersionState::CreateEnvelope => "CreateEnvelope",
            IncomingVersionState::PolicyGate => "PolicyGate",
            IncomingVersionState::SendNegotiation => "SendNegotiation",
            IncomingVersionState::AwaitApproval => "AwaitApproval",
            IncomingVersionState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionState::Apply => "Apply",
            IncomingVersionState::CheckPurgeFence => "CheckPurgeFence",
            IncomingVersionState::CheckDrift => "CheckDrift",
            IncomingVersionState::ReadPublicationRecord => "ReadPublicationRecord",
            IncomingVersionState::RecheckPublication => "RecheckPublication",
            IncomingVersionState::VerifyReplaced => "VerifyReplaced",
            IncomingVersionState::ReadReplacedEnvelope => "ReadReplacedEnvelope",
            IncomingVersionState::ReadReplacedRows => "ReadReplacedRows",
            IncomingVersionState::ReadReplacedMetadata => "ReadReplacedMetadata",
            IncomingVersionState::DeleteReplacedMetadata => "DeleteReplacedMetadata",
            IncomingVersionState::WriteReclaimCandidate => "WriteReclaimCandidate",
            IncomingVersionState::FenceBackend => "FenceBackend",
            IncomingVersionState::VerifyExistingBlob => "VerifyExistingBlob",
            IncomingVersionState::WriteBlobLocation => "WriteBlobLocation",
            IncomingVersionState::ReadObjectLookup => "ReadObjectLookup",
            IncomingVersionState::ReadNamedHead => "ReadNamedHead",
            IncomingVersionState::ReadCurrentVersion => "ReadCurrentVersion",
            IncomingVersionState::ApplyHeadTransition => "ApplyHeadTransition",
            IncomingVersionState::WriteBlobVersion => "WriteBlobVersion",
            IncomingVersionState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionState::WriteLiveObligation => "WriteLiveObligation",
            IncomingVersionState::CheckCommitQuota => "CheckCommitQuota",
            IncomingVersionState::UpdateUsage => "UpdateUsage",
            IncomingVersionState::WriteCleanupRow => "WriteCleanupRow",
            IncomingVersionState::ReleaseReservation => "ReleaseReservation",
            IncomingVersionState::ScheduleUsage => "ScheduleUsage",
            IncomingVersionState::ScheduleLiveDrain => "ScheduleLiveDrain",
            IncomingVersionState::SchedulePromotion => "SchedulePromotion",
            IncomingVersionState::RegisterBlobDht => "RegisterBlobInDht",
            IncomingVersionState::SendApplyComplete => "SendApplyComplete",
            IncomingVersionState::CloseConnection => "CloseConnection",
        }
    }

    fn reject_negotiation(&mut self, err: IncomingVersionError) -> Turn {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            reason = %err,
            "Rejecting incoming version replication negotiation"
        );
        let reason = err.to_string();
        self.output = Some(self.result(false));
        self.send_negotiation(ReplicationNegotiationResult::Rejected(reason))
    }

    fn result(&self, applied: bool) -> IncomingVersionResult {
        IncomingVersionResult {
            applied,
            group_id: self.destination_group_id,
        }
    }

    fn fail(&mut self, err: IncomingVersionError) -> Turn {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            state = %self.state_name(),
            error = %err,
            "Incoming version replication failed"
        );
        self.failure = Some(err.to_string());
        Err(err)
    }

    fn invalid(&mut self, expected: &'static str, received: &'static str) -> Turn {
        self.fail(IncomingVersionError::InvalidStateEvent {
            state: self.state_name(),
            expected,
            received,
        })
    }

    fn call(&mut self, state: IncomingVersionState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read(&mut self, state: IncomingVersionState, space: &str, key: impl Into<Key>) -> Turn {
        self.call(state, read_row(space, key))
    }

    fn reads(&mut self, state: IncomingVersionState, reads: Vec<(String, Key)>) -> Turn {
        self.call(state, IoCall::Read(reads))
    }

    fn write(&mut self, state: IncomingVersionState, rows: Vec<(String, Key, Value)>) -> Turn {
        self.call(state, IoCall::Write(rows))
    }

    fn blob(&mut self, state: IncomingVersionState, effect: BlobEffect) -> Turn {
        self.call(state, Effect::Blob(effect).into())
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
            return Err(IncomingVersionError::DestinationNotFound);
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
            || self.pending_replica()
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
        if previous.descriptor.kind
            != aruna_core::structs::execution::source_connector::SourceConnectorKind::ArunaNative
        {
            return RootEntry::normalized(previous) == RootEntry::normalized(incoming);
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
        RootEntry::normalized(&previous) == RootEntry::normalized(&incoming)
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
        if source.descriptor.kind
            == aruna_core::structs::execution::source_connector::SourceConnectorKind::LocalDirectory
        {
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
            .ok_or(IncomingVersionError::ReferenceCountMissing)?;
        let version = BlobVersion::reference(
            source,
            metadata,
            self.manifest.created_at,
            self.manifest.created_by,
            self.manifest.created_at,
        )
        .with_metadata(self.manifest.metadata.clone())
        .with_advance_count(advance_count)
        .with_publisher(self.publisher_node_id)
        .with_policies(self.manifest.placement_policies.clone())?;
        Ok(self.stamped(version))
    }

    /// The version as this destination stores it: epoch, generation and, in a distributed
    /// bucket, the identity the set fixed, so a retry compares equal to what it applied.
    fn stamped(&self, version: BlobVersion) -> BlobVersion {
        let version = version
            .with_epoch(self.original_epoch())
            .with_generation(self.version_generation())
            .with_origin(self.manifest.version_origin.clone())
            .with_hop(self.hop());
        match self.destination_bound {
            true => {
                let identity = self.manifest.logical_identity;
                let identity = identity.or_else(|| logical_identity(&version));
                version.with_identity(identity)
            }
            false => version,
        }
    }

    /// A sync destination records the hop from the sender's exact version, named by the
    /// sender's own coordinates; member replication keeps the hop the version carries.
    fn hop(&self) -> Option<SyncHop> {
        let Some(sync) = &self.manifest.origin else {
            return self.manifest.version_hop.clone();
        };
        let from = if self.manifest.original_epoch.0 > 0 {
            OriginVersion::Global(GlobalId::Version {
                realm_id: self.local_realm_id,
                version_id: self.manifest.version_id,
            })
        } else {
            let (bucket, key) = self.manifest.sync_source.as_ref()?;
            OriginVersion::Node(
                VersionedObjectArn::new(
                    self.local_realm_id,
                    self.publisher_node_id,
                    bucket,
                    key,
                    self.manifest.version_id,
                )
                .ok()?,
            )
        };
        Some(SyncHop {
            relationship_id: sync.relationship_id,
            from,
        })
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

    /// The head row of the transition, with the hash index of a current materialized version.
    fn prepare_head_transition(&mut self) -> Turn {
        let context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.fail(err),
        };
        let (pointer, current_hash) = match self.pending_head.take() {
            Some(pending) => (Some(pending.pointer), pending.current_hash),
            None => (None, None),
        };
        self.installed_head = pointer.clone();
        let head_key = match context.head_key().to_bytes() {
            Ok(key) => Key::from(key),
            Err(err) => return self.fail(err.into()),
        };
        let Some(pointer) = pointer else {
            let delete = vec![(BLOB_HEAD_KEYSPACE.to_string(), head_key)];
            return self.call(
                IncomingVersionState::ApplyHeadTransition,
                IoCall::Delete(delete),
            );
        };
        let head = match pointer.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        let mut rows = vec![(BLOB_HEAD_KEYSPACE.to_string(), head_key, head.into())];
        if let Some(hash) = current_hash {
            match self.index_row(&context, hash) {
                Ok(row) => rows.push(row),
                Err(err) => return self.fail(err),
            }
        }
        self.write(IncomingVersionState::ApplyHeadTransition, rows)
    }

    fn index_row(
        &self,
        context: &HeadAliasContext,
        hash: [u8; 32],
    ) -> Result<(String, Key, Value), IncomingVersionError> {
        let key = context.path_index_key(hash, self.manifest.version_id);
        let key = key.to_bytes()?.into();
        Ok((PATHS_INDEX_KEYSPACE.to_string(), key, Vec::new().into()))
    }

    /// Reads the bucket with its encryption settings, so an encrypting bucket captures its seal
    /// plan before any byte arrives.
    fn read_destination_bucket(&mut self) -> Turn {
        let key: Key = self.manifest.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
        ];
        self.reads(IncomingVersionState::ReadDestinationBucket, reads)
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
            compression: Compression::Off,
            distribution: None,
        }
    }

    fn create_destination_bucket(&mut self) -> Turn {
        self.create_attempted = true;
        let child = boxed_suboperation(
            CreateBucketOperation::new(
                self.manifest.bucket.clone(),
                self.destination_bucket_info(),
            ),
            |result| {
                Event::SubOperation(SubOperationEvent::BucketCreated {
                    result: match result {
                        Ok(_) => Ok(()),
                        Err(err) => Err(err.to_string()),
                    },
                })
            },
        );
        self.call(
            IncomingVersionState::CreateDestinationBucket,
            IoCall::Child(child),
        )
    }

    /// The receiver's own group default and registered backend ids, loaded once
    /// the destination group is known so that the existing-copy probe and the
    /// transfer resolve the destination from identical inputs.
    fn load_destination_routing(&mut self) -> Turn {
        let group_id = self.destination_group_id.unwrap_or(self.manifest.group_id);
        self.call(
            IncomingVersionState::LoadDestinationRouting,
            load_group_inputs(group_id).into(),
        )
    }

    fn check_permissions(&mut self, group_id: Ulid) -> Turn {
        if self.manifest.distribution.is_some() {
            return self.read_publication_record(false);
        }
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

    fn read_existing_version(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(
            IncomingVersionState::ReadExistingVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    fn read_replaced_blob(&mut self, key: BlobLocationKey) -> Turn {
        self.read(
            IncomingVersionState::ReadReplacedBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    fn read_quota_config(&mut self) -> Turn {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.local_realm_id,
        };
        self.read(
            IncomingVersionState::ReadQuotaConfig,
            target.storage_keyspace(),
            target.storage_key(),
        )
    }

    fn start_quota_check(&mut self, ceiling: u64) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let logical_bytes = match self.incoming_logical_bytes() {
            Ok(logical_bytes) => logical_bytes,
            Err(error) => return self.fail(error),
        }
        .saturating_sub(self.replaced_logical_bytes);
        self.quota_ceiling = Some(ceiling);
        let gate = QuotaGate::new_for_realm(
            ceiling,
            logical_bytes,
            group_id,
            self.local_node_id,
            self.local_realm_id,
        );
        self.call(
            IncomingVersionState::EnforceQuota,
            IoCall::child(QuotaProbe { gate }),
        )
    }

    /// Asks only about the backend this node would route the blob to: a copy on
    /// any other backend cannot satisfy the destination placement.
    fn read_existing_blob(&mut self) -> Turn {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        let hash = blob.hash;
        let bucket = &self.manifest.bucket;
        let mut imports = self.destination_inputs.imports.iter();
        if imports.any(|rule| &rule.bucket == bucket && rule.mapped_bucket().is_some()) {
            return self.reject_negotiation(IncomingVersionError::MappedDestination);
        }
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
        // An encrypting destination never adopts a plain or foreign copy of the content.
        if self.seal_plan.is_some() {
            return self.request_blob_version();
        }
        // Only a copy in this node's own encoding for the bucket can be reused.
        let key = BlobLocationKey::new(hash, self.destination_compression.into(), backend);
        self.read(
            IncomingVersionState::ReadExistingBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    /// The only negotiation result that stores bytes, so the destination's cap
    /// is answered here rather than at the probe that keys the deduplication.
    fn request_blob_version(&mut self) -> Turn {
        match self.destination_full.take() {
            Some(error) => self.reject_negotiation(IncomingVersionError::RoutingFailed(error)),
            None => match self.seal_plan {
                Some(plan) => self.reads(IncomingVersionState::ReadAbe, abe_reads(plan.key)),
                None => self.send_negotiation(ReplicationNegotiationResult::NeedBlobVersion),
            },
        }
    }

    /// Admitted ABE parameters give the replica its own object key and envelope before the
    /// transfer; a generation without them keeps bucket-only grants.
    fn accept_abe(&mut self, values: Vec<Option<Value>>) -> Turn {
        let Some(plan) = self.seal_plan else {
            return self.fail(IncomingVersionError::StalePlan);
        };
        if values.first().is_some_and(Option::is_none) {
            return self.send_negotiation(ReplicationNegotiationResult::NeedSealedBlob(plan));
        }
        let (parameters, epoch) = match parse_abe(&keyed(&values), plan.key) {
            Ok(parsed) => parsed,
            Err(error) => return self.reject_negotiation(BlobError::from(error).into()),
        };
        let envelope = AbeEffect::Envelope(EnvelopePlan {
            parameters,
            epoch,
            write_id: Ulid::generate(),
            object_key: self.manifest.key.clone(),
            bucket_public: plan.public_key,
        });
        self.blob(
            IncomingVersionState::CreateEnvelope,
            BlobEffect::Abe(Box::new(envelope)),
        )
    }

    fn accept_envelope(&mut self, event: Event) -> Turn {
        let envelope = match event {
            Event::Blob(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Envelope(envelope) => envelope,
                event => {
                    let received = Event::Blob(BlobEvent::Abe(Box::new(event))).label();
                    return self.invalid("AbeEvent::Envelope", received);
                }
            },
            Event::Blob(BlobEvent::Error(error)) => return self.reject_negotiation(error.into()),
            event => {
                return self.invalid("Event::Blob(BlobEvent::Abe)", event.label());
            }
        };
        let Some(plan) = self.seal_plan else {
            return self.fail(IncomingVersionError::StalePlan);
        };
        let object = envelope.context.public_key;
        self.envelope = Some(envelope);
        self.send_negotiation(ReplicationNegotiationResult::NeedAbeBlob(plan, object))
    }

    fn resolve_destination(&self) -> Result<ResolvedBackend, IncomingVersionError> {
        let snapshot = self
            .routing
            .snapshot(self.destination_group_id.unwrap_or(self.manifest.group_id))
            .with_group_inputs(self.destination_inputs.clone())
            .with_bucket_rules(self.destination_rules.clone())
            .with_compression(self.destination_compression);
        resolve_backend(&snapshot, &self.manifest.bucket, &self.manifest.key)
            .map_err(IncomingVersionError::RoutingFailed)
    }

    /// Nothing is admitted before the destination is evaluated: the reply that
    /// invites bytes is itself behind the gate.
    fn send_negotiation(&mut self, result: ReplicationNegotiationResult) -> Turn {
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
            Ok(Some(gate)) => {
                self.pending_negotiation = Some(result);
                self.call(IncomingVersionState::PolicyGate, IoCall::child(gate))
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn accept_policy_gate(&mut self, outcome: Result<PolicyGateOutcome, ReadPolicyError>) -> Turn {
        let Some(result) = self.pending_negotiation.take() else {
            return self.fail(IncomingVersionError::GateNotPending);
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => return self.reject_negotiation(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => self.reply_negotiation(result),
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn reply_negotiation(&mut self, result: ReplicationNegotiationResult) -> Turn {
        self.negotiation_result = Some(result.clone());
        let payload = match VersionReplicationMessage::VersionNegotiationResponse(result).to_bytes()
        {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::SendNegotiation,
            BlobEffect::SendMessage { stream_id, payload },
        )
    }

    fn receive_blob(&mut self) -> Turn {
        // The receiver routes with its own snapshot; the sender's stamped
        // backend crossed the wire but is ignored.
        let resolved = match self.resolve_destination() {
            Ok(resolved) => resolved.with_encryption(self.seal_plan),
            Err(error) => return self.fail(error),
        };
        let receive = BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            resolved,
            keep_alive: true,
            object: self.envelope.as_ref().map(|e| e.context.public_key),
        };
        self.blob(IncomingVersionState::ReceiveBlob, receive)
    }

    /// The apply runs as a child, in one write transaction of its own.
    fn start_apply(&mut self) -> Turn {
        let child = IoCall::child(self.apply_child());
        self.call(IncomingVersionState::Apply, child)
    }

    fn apply_child(&self) -> Self {
        Self {
            applying: true,
            publisher_node_id: self.publisher_node_id,
            clock: self.clock,
            destination_group_id: self.destination_group_id,
            destination_compression: self.destination_compression,
            destination_bound: self.destination_bound,
            destination_binding: self.destination_binding.clone(),
            quota_ceiling: self.quota_ceiling,
            existing_blob_location: self.existing_blob_location.clone(),
            replaced_version: self.replaced_version.clone(),
            existing_marker: self.existing_marker,
            advance_version_exists: self.advance_version_exists,
            received_blob: self.received_blob.clone(),
            replaced_logical_bytes: self.replaced_logical_bytes,
            replaced_reference_bytes: self.replaced_reference_bytes,
            rocrate_limits: self.rocrate_limits.clone(),
            gated_refs: self.gated_refs.clone(),
            gated_bucket: self.gated_bucket.clone(),
            seal_plan: self.seal_plan,
            envelope: self.envelope.clone(),
            ..Self::new(
                self.stream_id,
                self.local_node_id,
                self.local_realm_id,
                self.manifest.clone(),
            )
        }
    }

    /// The apply committed, or failed; an error that does not prove the commit never landed
    /// leaves the received copy to reconciliation.
    fn accept_applied(
        &mut self,
        applied: Result<IncomingVersionResult, IncomingVersionError>,
    ) -> Turn {
        if let Err(error) = applied {
            if let IncomingVersionError::StorageError(storage) = &error
                && !storage.proves_no_commit()
            {
                self.keep_received();
            }
            return self.fail(error);
        }
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
        match self.received_blob.as_ref().map(|it| it.location.ulid) {
            Some(id) => {
                self.release_id = Some(id);
                self.blob(
                    IncomingVersionState::ReleaseReservation,
                    BlobEffect::ReleaseReservation { id },
                )
            }
            None => self.call(
                IncomingVersionState::ScheduleUsage,
                schedule_snapshot_publish().into(),
            ),
        }
    }

    fn check_purge_fence(&mut self) -> Turn {
        let key = fence_key(&self.manifest.bucket);
        self.read(
            IncomingVersionState::CheckPurgeFence,
            PURGE_FENCE_KEYSPACE,
            key,
        )
    }

    fn read_replaced_metadata(&mut self) -> Turn {
        if self.replaced_version.is_none() {
            return self.write_hash_lookup();
        }
        let prefix = match MultipartObjectKey::part_prefix(self.manifest.version_id) {
            Ok(prefix) => prefix.into(),
            Err(error) => return self.fail(error.into()),
        };
        let scan = IoCall::Page {
            space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix,
            after: None,
            limit: 10_000,
        };
        self.call(IncomingVersionState::ReadReplacedMetadata, scan)
    }

    /// Re-reads the details the negotiation gate decided on, inside the very
    /// transaction that exposes the replica.
    fn check_drift(&mut self) -> Turn {
        let bucket: Key = self.manifest.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
            // Encryption enabled since the negotiation must still refuse the replica.
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
        ];
        // The envelope is published only under the parameters and epoch read here.
        if let (Some(plan), Some(_)) = (self.seal_plan, &self.envelope) {
            reads.extend(abe_reads(plan.key));
        }
        self.reads(IncomingVersionState::CheckDrift, reads)
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

    fn verify_replaced(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.read(
            IncomingVersionState::VerifyReplaced,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    fn delete_replaced_metadata(&mut self, values: Vec<(Key, Value)>) -> Turn {
        let mut deletes = Vec::with_capacity(values.len() + 2);
        let summary_key = match MultipartObjectKey::summary(self.manifest.version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(error) => return self.fail(error.into()),
        };
        deletes.push((OBJECT_METADATA_KEYSPACE.to_string(), summary_key));
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (OBJECT_METADATA_KEYSPACE.to_string(), key)),
        );
        deletes.append(&mut self.replaced_envelope);
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
            deletes.push((PATHS_INDEX_KEYSPACE.to_string(), key));
        }
        self.call(
            IncomingVersionState::DeleteReplacedMetadata,
            IoCall::Delete(deletes),
        )
    }

    /// The copy the replaced version named, once the replacement stops naming
    /// it. Nothing else drops that reference, so without this the bytes stay
    /// charged to the backend forever.
    fn replaced_reclaim_key(&self) -> Option<ReclaimCandidateKey> {
        let replaced = self.replaced_version.as_ref()?.location_key()?;
        let replacement = self.effective_materialized_location().ok().and_then(|it| {
            let hash: [u8; 32] = it.get_blake3()?.try_into().ok()?;
            Some(BlobLocationKey::new(hash, it.format.encoding(), it.backend))
        });
        (replacement.as_ref() != Some(&replaced)).then(|| {
            ReclaimCandidateKey::new(replaced.backend, replaced.encoding, replaced.blake3_hash)
        })
    }

    fn write_replaced_candidate(&mut self, key: ReclaimCandidateKey) -> Turn {
        let candidate = ReclaimCandidate {
            enqueued_at: self.clock.now(),
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        let row = (
            BLOB_RECLAIM_KEYSPACE.to_string(),
            key.to_bytes().into(),
            value.into(),
        );
        self.write(IncomingVersionState::WriteReclaimCandidate, vec![row])
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

        Ok(())
    }

    /// Received bytes for an encrypting destination must be sealed to its captured plan. An
    /// archive kept as sent has no content hash yet: only its original size is checked here.
    fn validate_received(&self, location: &BackendLocation) -> Result<(), IncomingVersionError> {
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            return self.validate_materialized_location(location);
        };
        let plan = self.seal_plan.ok_or(IncomingVersionError::StalePlan)?;
        if location.format.bucket_key() != Some(plan.key)
            || layout.storage_generation != plan.storage_generation
        {
            return Err(IncomingVersionError::StalePlan);
        }
        if location.get_blake3().is_some() {
            return self.validate_materialized_location(location);
        }
        let blob = self
            .manifest
            .blob
            .as_ref()
            .ok_or(IncomingVersionError::MissingBlobInfo)?;
        if location.blob_size != blob.size {
            return Err(IncomingVersionError::BlobSizeMismatch);
        }
        Ok(())
    }

    /// The replica is an archive kept as the sender granted it; its content hash is pending.
    fn pending_replica(&self) -> bool {
        self.received_blob.as_ref().is_some_and(|received| {
            received.location.format.bucket_key().is_some()
                && received.location.get_blake3().is_none()
        })
    }

    /// Rows that publish an archive: its pending location with the claimed hash, or its
    /// location, and the owner row of this version in both cases.
    fn archive_rows(
        &self,
        location: &BackendLocation,
    ) -> Result<Vec<(String, Key, Value)>, IncomingVersionError> {
        let archive = ArchiveKey::of(location);
        let version = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let owner = CopyOwner::new(archive.clone(), version);
        let mut rows = vec![(
            COPY_OWNER_KEYSPACE.to_string(),
            owner.key()?.into(),
            Vec::new().into(),
        )];
        match location.get_blake3() {
            Some(hash) => {
                let key = BlobLocationKey::from_blake3(
                    hash,
                    location.format.encoding(),
                    location.backend.clone(),
                )?;
                rows.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key.to_bytes().into(),
                    location.to_bytes()?.into(),
                ));
            }
            None => {
                let claimed = self
                    .manifest
                    .blob
                    .as_ref()
                    .ok_or(IncomingVersionError::MissingBlobInfo)?
                    .hash;
                rows.push((
                    PENDING_LOCATION_KEYSPACE.to_string(),
                    archive.to_bytes().into(),
                    location.to_bytes()?.into(),
                ));
                rows.push((
                    PENDING_CLAIM_KEYSPACE.to_string(),
                    archive.to_bytes().into(),
                    claimed.to_vec().into(),
                ));
            }
        }
        Ok(rows)
    }

    fn write_hash_lookup(&mut self) -> Turn {
        if self.is_reference_item() {
            return self.write_object_lookup();
        }
        if let Some(received) = self.received_blob.as_ref()
            && let Err(err) = self.validate_received(&received.location)
        {
            return self.fail(err);
        }

        if self.received_blob.is_none() && self.existing_blob_location.is_none() {
            return self.write_object_lookup();
        }

        self.begin_blob_location()
    }

    fn begin_blob_location(&mut self) -> Turn {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        if let BackendRef::Group(backend_id) = location.backend {
            return self.read(
                IncomingVersionState::FenceBackend,
                STORAGE_BACKEND_KEYSPACE,
                backend_key(backend_id),
            );
        }
        self.verify_existing_blob()
    }

    /// A negotiation that adopted an existing copy read it outside the
    /// transaction. Re-reading it inside makes the commit fail rather than
    /// leave the version naming bytes another writer has since removed.
    fn verify_existing_blob(&mut self) -> Turn {
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
        let key = BlobLocationKey::new(hash, location.format.encoding(), location.backend);
        self.read(
            IncomingVersionState::VerifyExistingBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    fn write_blob_location(&mut self) -> Turn {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        if location.format.bucket_key().is_some() {
            let writes = match self.archive_rows(&location) {
                Ok(writes) => writes,
                Err(error) => return self.fail(error),
            };
            return self.write(IncomingVersionState::WriteBlobLocation, writes);
        }
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_object_lookup();
        };
        let hash: [u8; 32] = match blake3_hash.try_into() {
            Ok(hash) => hash,
            Err(err) => return self.fail(ConversionError::from(err).into()),
        };
        let key = BlobLocationKey::new(hash, location.format.encoding(), location.backend.clone());
        let value = match location.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            key.to_bytes().into(),
            value.into(),
        );
        self.write(IncomingVersionState::WriteBlobLocation, vec![row])
    }

    fn write_object_lookup(&mut self) -> Turn {
        if !self.manifest.current_version {
            // The head may still name the replaced row, whose liveness then changes.
            let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
            if self
                .existing_marker
                .is_some_and(|existing| existing != marker)
            {
                return self.read_head(IncomingVersionState::ReadNamedHead);
            }
            return self.write_version();
        }
        if self.manifest.current_version_generation.is_none() {
            return self.fail(IncomingVersionError::MissingVersionGeneration);
        }
        self.read_head(IncomingVersionState::ReadObjectLookup)
    }

    fn read_head(&mut self, state: IncomingVersionState) -> Turn {
        let key = match BlobHeadKey::new(&self.manifest.bucket, &self.manifest.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(state, BLOB_HEAD_KEYSPACE, key)
    }

    fn read_current(&mut self, version_id: Ulid) -> Turn {
        let key = match VersionKey::new(&self.manifest.bucket, &self.manifest.key, version_id)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(
            IncomingVersionState::ReadCurrentVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    /// The liveness change of a replaced row the head names while the head stays: a version
    /// replaced by a marker deletes the object, the reverse revives it.
    fn named_head_delta(&self, head: Option<&CurrentVersionPointer>) -> i128 {
        let names = head.is_some_and(|head| head.version_id == self.manifest.version_id);
        match self.existing_marker.filter(|_| names) {
            Some(was_marker) => {
                let live = self.manifest.kind == ReplicationItemKind::Materialized;
                i128::from(u8::from(live)) - i128::from(u8::from(!was_marker))
            }
            None => 0,
        }
    }

    fn accept_named_head(&mut self, value: Option<Value>) -> Turn {
        let head = value.as_deref().map(CurrentVersionPointer::from_bytes);
        match head.transpose() {
            Ok(head) => {
                self.object_delta = self.named_head_delta(head.as_ref());
                self.write_version()
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn apply_liveness(&mut self, previous_live: bool) -> Turn {
        let next_live = self.manifest.kind == ReplicationItemKind::Materialized;
        self.object_delta = i128::from(u8::from(next_live)) - i128::from(u8::from(previous_live));
        self.prepare_head_transition()
    }

    fn write_compared_object(&mut self, existing: Option<&[u8]>) -> Turn {
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
            )
            .with_epoch(self.original_epoch());
            if self.advance_version_exists
                && (advanced == pointer || pointer.order() > advanced.order())
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
        )
        .with_epoch(self.original_epoch());
        let should_write = candidate.supersedes(existing_pointer.as_ref());

        self.existing_current_pointer = existing_pointer.clone();
        if should_write {
            self.pending_head = Some(PendingHeadTransition {
                pointer: candidate.clone(),
                current_hash: self.current_manifest_hash(),
            });
        } else {
            self.pending_head = None;
            self.object_delta = self.named_head_delta(existing_pointer.as_ref());
        }
        self.resume_head_transition()
    }

    /// Continues exactly where `write_compared_object` left off; the
    /// stored pointers carry the decision, so no continuation state is needed.
    fn resume_head_transition(&mut self) -> Turn {
        if self.pending_head.is_none() {
            return self.write_version();
        }
        match self.existing_current_pointer.clone() {
            Some(pointer) => self.read_current(pointer.version_id),
            None => self.apply_liveness(false),
        }
    }

    fn write_version(&mut self) -> Turn {
        self.write_blob_version()
    }

    /// The manifest's epoch inside a distributed bucket; an ordinary bucket keeps 0, so its
    /// heads order by generation exactly as before.
    fn original_epoch(&self) -> OriginalEpoch {
        match self.destination_bound {
            true => self.manifest.original_epoch,
            false => OriginalEpoch::default(),
        }
    }

    /// The generation the version was written as, kept inside a distributed bucket whether
    /// or not it is the current version; an ordinary bucket keeps 0.
    fn version_generation(&self) -> u64 {
        match (self.destination_bound, self.manifest.version_generation) {
            (false, _) => 0,
            (true, 0) => self.manifest.current_version_generation.unwrap_or_default(),
            (true, generation) => generation,
        }
    }

    fn write_blob_version(&mut self) -> Turn {
        match self.version_rows() {
            Ok(rows) => self.write(IncomingVersionState::WriteBlobVersion, rows),
            Err(error) => self.fail(error),
        }
    }

    fn copy_registration<'a>(
        &'a self,
        version: VersionKey,
        location: &'a BackendLocation,
    ) -> CopyRegistration<'a> {
        CopyRegistration {
            version,
            node_id: self.local_node_id,
            location,
            policies: &self.gated_refs,
            origin: self.copy_origin(),
            subject_generation: self.stored_subject(),
            registered_at_ms: self.manifest.version_id.timestamp_ms(),
        }
    }

    /// The version and every row that commits with it, written in one call.
    fn version_rows(&mut self) -> Result<Vec<(String, Key, Value)>, IncomingVersionError> {
        let version_key = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let mut extra = Vec::new();
        let (version, materialized_hash) = match self.manifest.kind {
            ReplicationItemKind::Materialized if self.is_reference_item() => {
                let version = self.reference_version()?;
                (version.with_policies(self.gated_refs.clone())?, None)
            }
            ReplicationItemKind::Materialized if self.pending_replica() => {
                let location = self.effective_materialized_location()?;
                let pending = BlobVersion::pending(
                    ArchiveKey::of(&location),
                    self.manifest.created_at,
                    self.manifest.created_by,
                    self.manifest.source.clone(),
                )
                .with_metadata(self.manifest.metadata.clone())
                .with_publisher(self.publisher_node_id)
                .with_policies(self.gated_refs.clone())?;
                // No hash alias and no DHT entry until promotion verified the claimed hash.
                let copy = self.copy_registration(version_key.clone(), &location);
                extra.push(register_entry(copy)?);
                (pending, None)
            }
            ReplicationItemKind::Materialized => {
                let location = self.effective_materialized_location()?;
                let blake3_hash = location.get_blake3();
                let blake3_hash = blake3_hash.ok_or(IncomingVersionError::MissingBlobLocation)?;
                let hash: [u8; 32] = blake3_hash.try_into().map_err(ConversionError::from)?;
                let materialized = BlobVersion::materialized(
                    hash,
                    location.backend.clone(),
                    location.format.encoding(),
                    self.manifest.created_at,
                    self.manifest.created_by,
                    self.manifest.source.clone(),
                )
                .with_metadata(self.manifest.metadata.clone())
                .with_publisher(self.publisher_node_id)
                .with_policies(self.gated_refs.clone())?;
                (materialized, Some(hash))
            }
            ReplicationItemKind::DeleteMarker => (
                BlobVersion::deleted(self.manifest.created_at, self.manifest.created_by)
                    .with_publisher(self.publisher_node_id),
                None,
            ),
        };

        let version = self.stamped(version);
        self.written_identity = logical_identity(&version);
        self.written_pending = matches!(version.state, BlobVersionState::PendingContent { .. });
        // A member stores the binding compact too, with the root record it names.
        let (version, root) = compact_version(version)?;
        let mut rows = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            version.to_bytes()?.into(),
        )];
        rows.append(&mut extra);
        if let Some(root) = root {
            rows.push(root_write(&root)?);
        }
        // A member keeps the receipt of a pulled version, so a later coordinator answers replays.
        rows.extend(self.pull_receipt());
        // Only a same-set publication keeps release tags; its row commits with the version.
        rows.extend(release_rows(&version_key, &version)?);
        // The envelope, its mappings and usage are published with the version they name.
        if let (Some(envelope), Some(received)) = (&self.envelope, &self.received_blob) {
            let metadata = (&self.manifest.metadata, self.rocrate_limits.metadata_bytes);
            let located = located_rows(envelope, &version_key, &received.location, metadata);
            let (written, charge) = located.map_err(BlobError::from)?;
            self.envelope_bytes = charge;
            rows.extend(written);
        }
        if let Some(hash) = materialized_hash {
            let context = self.alias_context()?;
            rows.push(self.index_row(&context, hash)?);
            // The replica becomes serveable exactly when its version commits.
            // A reference item materializes nothing and registers nothing.
            let location = self.effective_materialized_location()?;
            let copy = self.copy_registration(version_key, &location);
            rows.push(register_entry(copy)?);
        }
        // A version new to this node is journaled last in its transaction. A delete marker is
        // not an upload, and a rewrite of a version already here keeps its first anchor.
        if self.manifest.kind != ReplicationItemKind::DeleteMarker
            && self.replaced_version.is_none()
        {
            let group_id = self.destination_group_id.unwrap_or(self.manifest.group_id);
            let uploaded = SourceEvent::data_uploaded(
                (self.local_realm_id, group_id, self.local_node_id),
                (&self.manifest.bucket, &self.manifest.key),
                self.manifest.version_id,
                unix_timestamp_millis(),
            );
            rows.extend(journal_entries(&uploaded)?);
        }
        Ok(rows)
    }

    fn write_multipart_metadata(&mut self) -> Turn {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.write_live_obligation();
        };

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
            OBJECT_METADATA_KEYSPACE.to_string(),
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
                OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        self.write(IncomingVersionState::WriteMultipartMetadata, writes)
    }

    fn write_live_obligation(&mut self) -> Turn {
        let auth_context = match self.manifest.reference_advance.as_ref() {
            Some(_) => self.manifest.auth_context.clone(),
            None => {
                let Some(auth_context) = self.manifest.writer_auth_context.clone() else {
                    return self.start_commit_quota();
                };
                auth_context
            }
        };
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
        match live_obligation_entry(&record) {
            Ok(row) => self.write(IncomingVersionState::WriteLiveObligation, vec![row]),
            Err(error) => self.fail(error.into()),
        }
    }

    /// The row that reconciles the received write when its commit outcome is never learned.
    fn cleanup_row(&self) -> Result<Option<(String, Key, Value)>, IncomingVersionError> {
        let Some(received) = self.received_blob.as_ref() else {
            return Ok(None);
        };
        let location = &received.location;
        let blake3 = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok());
        let owner = match blake3 {
            Some(blake3) => WriteOwner::Blob {
                blake3,
                realm_id: self.local_realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
                announce: !self.destination_bound,
            },
            // A pending archive is kept while its pending location names it.
            None if location.format.bucket_key().is_some() => WriteOwner::Pending,
            None => return Err(IncomingVersionError::MissingBlobLocation),
        };
        let work = BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner,
        };
        Ok(Some((
            BLOB_CLEANUP_KEYSPACE.to_string(),
            location.ulid.to_bytes().to_vec().into(),
            work.to_bytes()?.into(),
        )))
    }

    fn commit_or_cleanup(&mut self) -> Turn {
        if self.manifest.kind != ReplicationItemKind::Materialized
            || self.is_reference_item()
            || self.received_blob.is_none()
        {
            return self.finish_apply();
        }
        match self.cleanup_row() {
            Ok(Some(row)) => self.write(IncomingVersionState::WriteCleanupRow, vec![row]),
            Ok(None) => self.fail(ReplicationError::ReplicationFailed.into()),
            Err(error) => self.fail(error),
        }
    }

    /// The apply's outcome; the runner commits its transaction with it.
    fn finish_apply(&mut self) -> Turn {
        Ok(Next::Done(self.result(true)))
    }

    fn usage_delta(&self) -> Result<UsageDelta, IncomingVersionError> {
        let bytes = match self.manifest.kind {
            ReplicationItemKind::Materialized => i128::from(self.incoming_logical_bytes()?),
            ReplicationItemKind::DeleteMarker => 0,
        };
        let envelope = i128::from(self.envelope_bytes);
        Ok(UsageDelta {
            objects: self.object_delta,
            logical_bytes: if self.is_reference_item() {
                0
            } else {
                bytes + envelope
            } - i128::from(self.replaced_logical_bytes),
            referenced_bytes: if self.is_reference_item() { bytes } else { 0 }
                - i128::from(self.replaced_reference_bytes),
            ..Default::default()
        })
    }

    /// The receipt row of a device pull the received version of a distributed bucket records.
    fn pull_receipt(&self) -> Option<(String, Key, Value)> {
        self.destination_binding.as_ref()?;
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let target = (self.manifest.bucket.as_str(), self.manifest.key.as_str());
        let key = tagged_receipt(target, &self.manifest.metadata, marker)?;
        Some((
            SYNC_RECEIPT_KEYSPACE.to_string(),
            key.into(),
            self.manifest.version_id.to_bytes().to_vec().into(),
        ))
    }

    /// The received version and, when this apply installs it, the head of a distributed bucket.
    fn inventory_change(&self) -> Option<InventoryChange> {
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let size = match marker {
            true => 0,
            false => self.incoming_logical_bytes().unwrap_or_default(),
        };
        let added = AddedVersion {
            version_id: self.manifest.version_id,
            epoch: self.original_epoch(),
            generation: self.version_generation(),
            entry: VersionEntry {
                marker,
                size,
                referenced: self.is_reference_item(),
                identity: self.written_identity,
                pending: self.written_pending,
                searchable: searchable(&self.manifest.metadata),
            },
        };
        let head = self.installed_head.is_some();
        let mut change = version_written(
            self.destination_binding.as_ref(),
            &self.manifest.key,
            added,
            head,
        )?;
        // A reference carries its source root, which a member taking over the range checks.
        let source = self.manifest.source.as_ref();
        change.root = source
            .filter(|_| self.is_reference_item())
            .and_then(RootEntry::of);
        Some(change)
    }

    fn start_commit_quota(&mut self) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let group_delta = match self.usage_delta() {
            Ok(delta) => delta,
            Err(error) => return self.fail(error),
        };
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let row = RowChange::written(self.existing_marker, marker);
        let inventory = self.inventory_change();
        if marker {
            let update = UsageCounterUpdate::for_group(group_id, group_delta)
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory);
            if update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        if self.is_reference_item() {
            let update = UsageCounterUpdate::for_group(group_id, group_delta)
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory);
            if self.manifest.reference_advance.is_some() && update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        let update = match self.received_blob.as_ref() {
            None => UsageCounterUpdate::for_group(group_id, group_delta),
            Some(received) => match StoredDelta::for_location(&received.location, true) {
                Some(stored) => UsageCounterUpdate::with_stored(group_id, group_delta, stored),
                None => return self.fail(IncomingVersionError::MissingBlobInfo),
            },
        };
        self.usage_update = Some(
            update
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory),
        );
        let quota_bytes = (blob.size.saturating_add(self.envelope_bytes))
            .saturating_sub(self.replaced_logical_bytes);
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
            let call = gate.start_calls();
            self.quota_gate = Some(gate);
            self.call(IncomingVersionState::CheckCommitQuota, call)
        } else {
            self.start_usage_update()
        }
    }

    fn start_usage_update(&mut self) -> Turn {
        match self.usage_update.as_mut() {
            Some(update) => {
                let call = update.start_calls().into_call();
                self.call(IncomingVersionState::UpdateUsage, call)
            }
            None => self.fail(ReplicationError::ReplicationFailed.into()),
        }
    }

    fn accept_reservation_release(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            return self.invalid("Event::Blob(BlobEvent::ReservationReleased)", event.label());
        };
        if self.release_id != Some(id) {
            let received = Event::Blob(BlobEvent::ReservationReleased { id }).label();
            return self.invalid("matching reservation id", received);
        }
        self.release_id = None;
        self.call(
            IncomingVersionState::ScheduleUsage,
            schedule_snapshot_publish().into(),
        )
    }

    fn register_blob_dht(&mut self) -> Turn {
        // Distributed buckets are found through their content indexes, not the DHT.
        if self.is_reference_item() || self.destination_bound {
            return self.send_apply_complete();
        }
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        let effect =
            match dht_registration_effect(blake3_hash, self.local_realm_id, &self.rocrate_limits) {
                Ok(effect) => effect,
                Err(_) => return self.send_apply_complete(),
            };
        self.call(IncomingVersionState::RegisterBlobDht, effect.into())
    }

    fn finish_live_drain(&mut self) -> Turn {
        match self.manifest.kind {
            ReplicationItemKind::Materialized if self.pending_replica() => {
                self.schedule_promotion()
            }
            ReplicationItemKind::Materialized => self.register_blob_dht(),
            ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
        }
    }

    /// A pending replica registers nothing here; its promotion does once the key is usable.
    fn schedule_promotion(&mut self) -> Turn {
        let Some(key) = self.seal_plan.map(|plan| plan.key) else {
            return self.send_apply_complete();
        };
        let wake = TaskEffect::ShortenTimer {
            key: TaskKey::PromotePending {
                bucket_id: key.bucket_id,
                generation: key.generation,
            },
            after: std::time::Duration::ZERO,
        };
        self.call(
            IncomingVersionState::SchedulePromotion,
            Effect::Task(wake).into(),
        )
    }

    fn accept_promotion_scheduled(&mut self, event: Event) -> Turn {
        if !matches!(event, Event::Task(_)) {
            warn!(event = ?event, "Incoming replication committed but promotion scheduling returned an unexpected event");
        }
        self.send_apply_complete()
    }

    fn send_apply_complete(&mut self) -> Turn {
        let payload = match VersionReplicationMessage::VersionApplyComplete.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::SendApplyComplete,
            BlobEffect::SendMessage { stream_id, payload },
        )
    }

    fn close_connection(&mut self) -> Turn {
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::CloseConnection,
            BlobEffect::CloseConnection { stream_id },
        )
    }
}

// Phase: negotiation
// Destination resolution, the quota probe and the placement gate that
// together decide the reply sent to the sender.
impl IncomingVersionOperation {
    fn accept_destination_bucket(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [value, settings] = match <[_; 2]>::try_from(values) {
            Ok(values) => values,
            Err(_) => return self.fail(PolicyGateError::InvalidEvent.into()),
        };
        let settings = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        // A reference would serve bytes this bucket never sealed.
        if settings.is_encrypted()
            && (self.is_reference_item() || self.manifest.reference_advance.is_some())
        {
            return self.reject_negotiation(IncomingVersionError::EncryptedDestination);
        }

        let Some(value) = value else {
            if self.create_attempted || self.manifest.distribution.is_some() {
                return self.reject_negotiation(IncomingVersionError::DestinationNotFound);
            }
            if self.manifest.reference_advance.is_some() {
                return self.reject_negotiation(IncomingVersionError::DestinationNotFound);
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
        self.destination_compression = bucket_info.compression;
        self.destination_bound = bucket_info.distribution.is_some();
        self.destination_binding = bucket_info.distribution.clone();
        if self.destination_bound != self.manifest.distribution.is_some() {
            return self.reject_negotiation(IncomingVersionError::ForeignPublication);
        }
        // A version from another bucket is never a release here.
        if self.manifest.distribution.is_none() {
            strip_release_keys(&mut self.manifest.metadata);
        }
        let Some(key) = settings
            .active_key()
            .filter(|_| self.manifest.kind == ReplicationItemKind::Materialized)
        else {
            return self.load_destination_routing();
        };
        self.destination_settings = Some(settings);
        self.read(
            IncomingVersionState::ReadDestinationKey,
            BUCKET_KEY_KEYSPACE,
            key.key(),
        )
    }

    /// Captures how this encrypting destination seals: its active key and stored format.
    fn accept_destination_key(&mut self, value: Option<Value>) -> Turn {
        let Some(settings) = self.destination_settings.take() else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let record = match value.map(|value| BucketKeyRecord::from_bytes(&value)) {
            Some(Ok(record)) => record,
            Some(Err(error)) => return self.fail(error.into()),
            None => return self.reject_negotiation(BucketKeyError::Unsupported.into()),
        };
        match SealPlan::capture(&settings, &record) {
            Ok(plan) => {
                self.seal_plan = plan;
                self.load_destination_routing()
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    fn accept_bucket_created(&mut self, event: Event) -> Turn {
        let Event::SubOperation(SubOperationEvent::BucketCreated { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::BucketCreated)",
                event.label(),
            );
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

    fn accept_routing_loaded(&mut self, event: Event) -> Turn {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                event.label(),
            );
        };
        match result {
            Ok(inputs) => self.destination_inputs = inputs,
            Err(error) => {
                return self.fail(IncomingVersionError::RoutingInputsFailed(error));
            }
        }
        self.check_permissions(self.destination_group_id.unwrap_or(self.manifest.group_id))
    }

    fn accept_existing_version(&mut self, value: Option<Value>) -> Turn {
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
            self.existing_marker = Some(existing.is_deleted());
            // The origin is fixed at mint, so a replay must carry the stored one.
            if existing.origin.is_some() && existing.origin != self.manifest.version_origin {
                return self.reject_negotiation(IncomingVersionError::OriginMismatch);
            }
            if self.manifest.reference_advance.is_some() {
                let incoming = match self.reference_version() {
                    Ok(incoming) => incoming,
                    Err(error) => return self.reject_negotiation(error),
                };
                if !same_reference(&existing, &incoming) {
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
                        if same_reference(&existing, &incoming) {
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
                // Only the same content, as the earlier sender claimed it, is already here.
                BlobVersionState::PendingContent { archive, .. }
                    if self.manifest.blob.is_some() && !self.is_reference_item() =>
                {
                    return self.read(
                        IncomingVersionState::ReadExistingClaim,
                        PENDING_CLAIM_KEYSPACE,
                        archive.to_bytes(),
                    );
                }
                BlobVersionState::PendingContent { .. } => {
                    return self.reject_negotiation(IncomingVersionError::EncryptedVersion);
                }
            }
        }

        match self.manifest.kind {
            ReplicationItemKind::DeleteMarker => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            ReplicationItemKind::Materialized => self.read_quota_config(),
        }
    }

    fn accept_existing_claim(&mut self, value: Option<Value>) -> Turn {
        let claimed = self.manifest.blob.as_ref().map(|blob| blob.hash.to_vec());
        if value.is_some_and(|value| Some(value.to_vec()) == claimed) {
            return self.send_negotiation(ReplicationNegotiationResult::AlreadyReplicatedVersion);
        }
        self.reject_negotiation(IncomingVersionError::EncryptedVersion)
    }

    fn accept_replaced_blob(&mut self, value: Option<Value>) -> Turn {
        self.replaced_logical_bytes = match value
            .as_ref()
            .map(|value| BackendLocation::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(location) => location.map_or(0, |location| location.blob_size),
            Err(error) => return self.fail(error.into()),
        };
        self.defer_quota = self.seal_plan.is_some();
        if self.is_reference_item() {
            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
        } else {
            self.read_quota_config()
        }
    }

    fn accept_quota_config(&mut self, value: Option<Value>) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let ceiling = match value
            .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
            .transpose()
        {
            // A bound destination only takes its coordinator's publications: always a copy.
            Ok(Some(config))
                if self.destination_bound && !config.replica_quota.counts(&group_id) =>
            {
                None
            }
            Ok(Some(config)) => config.quota.effective_group_ceiling(&group_id),
            Ok(None) => None,
            Err(err) => return self.fail(err.into()),
        };

        match ceiling {
            _ if self.is_reference_item() => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            Some(ceiling) if self.defer_quota => {
                self.quota_ceiling = Some(ceiling);
                self.read_existing_blob()
            }
            Some(ceiling) => self.start_quota_check(ceiling),
            None => self.read_existing_blob(),
        }
    }

    fn accept_quota_probe(&mut self, exceeded: Result<bool, QuotaGateError>) -> Turn {
        match exceeded {
            Ok(true) => {
                self.output = Some(self.result(false));
                self.send_negotiation(ReplicationNegotiationResult::Rejected("quota".to_string()))
            }
            Ok(false) if self.is_reference_item() => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            Ok(false) => self.read_existing_blob(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_existing_blob(&mut self, value: Option<Value>) -> Turn {
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

    fn accept_reply_sent(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.invalid("Event::Blob(BlobEvent::MessageSent)", event.label());
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
                if self.manifest.blob.is_some() {
                    let stream_id = self.stream_id;
                    self.blob(
                        IncomingVersionState::AwaitApproval,
                        BlobEffect::ReadMessage { stream_id },
                    )
                } else {
                    self.start_apply()
                }
            }
            Some(
                ReplicationNegotiationResult::NeedBlobVersion
                | ReplicationNegotiationResult::NeedSealedBlob(_)
                | ReplicationNegotiationResult::NeedAbeBlob(..),
            ) => {
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
// Accepting the transferred blob, then the apply's purge-fence and destination-drift checks.
impl IncomingVersionOperation {
    fn accept_source_approval(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::MessageReceived { payload, .. }) = event else {
            return self.invalid("VersionApplyApproved", event.label());
        };
        match VersionReplicationMessage::from_bytes(&payload) {
            Ok(VersionReplicationMessage::VersionApplyApproved) => self.start_apply(),
            Ok(_) => {
                let stream_id = self.stream_id;
                let received = Event::Blob(BlobEvent::MessageReceived { stream_id, payload });
                self.invalid("VersionApplyApproved", received.label())
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_blob_finish(&mut self, event: Event) -> Turn {
        let location = match event {
            Event::Blob(BlobEvent::ReplicationFinished { location }) => location,
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.received_blob = Some(ReceivedBlob::reserved(location));
                return self.fail(IncomingVersionError::ReplicationError(
                    ReplicationError::ReplicationFailed,
                ));
            }
            other => {
                return self.invalid(
                    "Event::Blob(BlobEvent::{ReplicationFinished|WriteCleanup})",
                    other.label(),
                );
            }
        };
        if let Err(err) = self.validate_received(&location) {
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
        self.start_apply()
    }

    fn accept_purge_fence(&mut self, value: Option<Value>) -> Turn {
        let checked = check_fence_row(value.as_deref(), &self.manifest.bucket, &self.manifest.key);
        if let Err(error) = checked {
            return self.fail(error.into());
        }
        self.check_drift()
    }

    fn accept_drift_check(&mut self, mut values: Vec<Option<Value>>) -> Turn {
        if let (Some(plan), Some(envelope)) = (self.seal_plan, &self.envelope) {
            let abe = values.split_off(values.len().saturating_sub(2));
            let anchored = parse_abe(&keyed(&abe), plan.key)
                .and_then(|(parameters, epoch)| envelope.anchored(&parameters, epoch));
            if anchored.is_err() {
                return self.fail(IncomingVersionError::StalePlan);
            }
        }
        let Some(settings) = values.pop().filter(|_| values.len() == 2) else {
            return self.fail(PolicyGateError::InvalidEvent.into());
        };
        let settings = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        // The captured plan must still be current; a bucket that started encrypting since the
        // negotiation refuses content that was not sealed for it.
        let stale = match self.seal_plan {
            Some(plan) => plan.still_current(&settings).is_err(),
            None => {
                settings.is_encrypted() && self.manifest.kind == ReplicationItemKind::Materialized
            }
        };
        if stale {
            return self.fail(IncomingVersionError::StalePlan);
        }
        let (bucket, subject) = match split_drift_reads(keyed(&values)) {
            Ok(split) => split,
            Err(error) => return self.fail(error.into()),
        };
        // A bucket bound or replaced since the negotiation takes nothing from this stream.
        let binding = bucket
            .as_ref()
            .and_then(|bucket| bucket.distribution.clone());
        if binding != self.destination_binding {
            return self.fail(IncomingVersionError::ForeignPublication);
        }
        let observed = GatedBucket::observe(bucket.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return self.fail(PolicyGateError::Drift.into());
            }
            if let Err(error) = gated.check_subject(subject.as_ref()) {
                return self.fail(error.into());
            }
        }
        // A replica encoded under an older setting is never published: the bucket's
        // migration may already have passed this key.
        let written = self
            .effective_materialized_location()
            .ok()
            .filter(|written| written.format.bucket_key().is_none());
        if bucket
            .as_ref()
            .zip(written)
            .is_some_and(|(bucket, written)| {
                EncodingClass::from(bucket.compression) != written.format.encoding()
            })
        {
            return self.fail(StorageError::TransactionConflict.into());
        }
        match self.manifest.distribution {
            Some(_) => self.read_publication_record(true),
            None => self.verify_replaced(),
        }
    }
}

// Phase: apply/commit
// Exposing the replica: replacement cleanup, head transition, the version
// and its side records, then usage accounting and the commit.
impl IncomingVersionOperation {
    fn accept_replaced_version(&mut self, value: Option<Value>) -> Turn {
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
        if current.is_some_and(|version| version.location_key().is_some()) {
            return self.read_replaced_envelope();
        }
        self.read_replaced_metadata()
    }

    /// Like a delete, a replacement removes the replaced envelope or pending copy and its charge.
    fn read_replaced_envelope(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        let bucket = self.manifest.bucket.as_bytes().to_vec();
        let reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), key.into()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
        ];
        self.replaced_reads = reads.clone();
        self.reads(IncomingVersionState::ReadReplacedEnvelope, reads)
    }

    fn accept_replaced_envelope(&mut self, values: Vec<Option<Value>>) -> Turn {
        let asked = std::mem::take(&mut self.replaced_reads);
        let ([id, settings], [(_, key), _]) = (values.as_slice(), asked.as_slice()) else {
            return self.fail(PolicyGateError::InvalidEvent.into());
        };
        let reads = match id {
            Some(id) => {
                self.replaced_envelope = vec![(ABE_VERSION_KEYSPACE.to_string(), key.clone())];
                vec![
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
                ]
            }
            None => {
                let settings = match BucketEncryption::from_row(settings.as_deref()) {
                    Ok(settings) => settings,
                    Err(error) => return self.fail(error.into()),
                };
                let Some(bucket_id) = settings.bucket_id else {
                    return self.read_replaced_metadata();
                };
                let version = VersionKey::new(
                    &self.manifest.bucket,
                    &self.manifest.key,
                    self.manifest.version_id,
                );
                match copy_row(bucket_id, &version) {
                    Ok(row) => vec![(ABE_COPY_KEYSPACE.to_string(), row.into())],
                    Err(error) => return self.fail(error.into()),
                }
            }
        };
        self.replaced_reads = reads.clone();
        self.reads(IncomingVersionState::ReadReplacedRows, reads)
    }

    fn accept_replaced_rows(&mut self, values: Vec<Option<Value>>) -> Turn {
        let asked = std::mem::take(&mut self.replaced_reads);
        let charge = match (values.as_slice(), asked.as_slice()) {
            ([pending], [(_, row)]) => {
                let Some(pending) = pending else {
                    return self.read_replaced_metadata();
                };
                self.replaced_envelope = vec![(ABE_COPY_KEYSPACE.to_string(), row.clone())];
                pending.len() as u64
            }
            ([envelope, archive], [(_, id), _]) => {
                self.replaced_envelope.extend([
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
                ]);
                envelope_charge(
                    envelope.as_deref().unwrap_or_default(),
                    archive.as_deref().unwrap_or_default(),
                )
            }
            _ => return self.fail(PolicyGateError::InvalidEvent.into()),
        };
        self.replaced_logical_bytes = self.replaced_logical_bytes.saturating_add(charge);
        self.read_replaced_metadata()
    }

    fn accept_metadata_iterated(
        &mut self,
        (values, next_start_after): (Vec<(Key, Value)>, Option<Key>),
    ) -> Turn {
        if next_start_after.is_some() {
            return self.fail(IncomingVersionError::MultipartMetadataOverflow);
        }
        self.delete_replaced_metadata(values)
    }

    fn accept_metadata_deleted(&mut self) -> Turn {
        match self.replaced_reclaim_key() {
            Some(key) => self.write_replaced_candidate(key),
            None => {
                self.replaced_version = None;
                self.write_hash_lookup()
            }
        }
    }

    fn accept_backend_fence(&mut self, value: Option<Value>) -> Turn {
        let record = (value.as_deref()).map(GroupStorage::from_bytes).transpose();
        let record = record.map_err(RecordReadError::Conversion);
        match record {
            Ok(Some(record)) if !record.disabled => self.verify_existing_blob(),
            Ok(_) => self.fail(BackendFenceError::Unavailable.into()),
            Err(error) => self.fail(BackendFenceError::Read(error).into()),
        }
    }

    fn accept_verified_blob(&mut self, value: Option<Value>) -> Turn {
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

    fn accept_object_lookup(&mut self, value: Option<Value>) -> Turn {
        let existing_pointer = value
            .as_ref()
            .and_then(|value| CurrentVersionPointer::from_bytes(value.as_ref()).ok());
        let incoming_generation = self.manifest.current_version_generation;
        let pointer_will_update = incoming_generation.is_some_and(|generation| {
            CurrentVersionPointer::new_with_generation(self.manifest.version_id, generation)
                .with_epoch(self.original_epoch())
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

    fn accept_current_version(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.fail(if self.manifest.reference_advance.is_some() {
                IncomingVersionError::InvalidReferenceAdvance
            } else {
                IncomingVersionError::CurrentVersionMissing
            });
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = self.validate_advance(&version) {
            return self.fail(error);
        }
        // A new head releases an offered directory observation. A replaced row is
        // already debited, so the same version is not released twice.
        let replaced = self
            .existing_current_pointer
            .as_ref()
            .is_some_and(|pointer| pointer.version_id == self.manifest.version_id);
        if !replaced {
            let released = version.offered_size(&self.manifest.bucket, &self.manifest.key);
            self.replaced_reference_bytes = self
                .replaced_reference_bytes
                .saturating_add(released.unwrap_or(0));
        }
        self.apply_liveness(!version.is_deleted())
    }

    fn accept_commit_quota(&mut self, result: IoResult) -> Turn {
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(ReplicationError::ReplicationFailed.into());
        };
        match gate.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call)),
            Ok(None) if gate.is_exceeded() => self.fail(IncomingVersionError::QuotaExceeded),
            Ok(None) => self.start_usage_update(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_usage_update(&mut self, result: IoResult) -> Turn {
        let Some(update) = self.usage_update.as_mut() else {
            return self.fail(ReplicationError::ReplicationFailed.into());
        };
        match update.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call.into_call())),
            Ok(None) => self.commit_or_cleanup(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_usage_schedule(&mut self, event: Event) -> Turn {
        if !matches!(
            event,
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. })
        ) {
            warn!(event = ?event, "Incoming replication committed but usage scheduling returned an unexpected event");
        }
        self.call(
            IncomingVersionState::ScheduleLiveDrain,
            schedule_blob_drain().into(),
        )
    }

    fn accept_live_drain(&mut self, event: Event) -> Turn {
        if !matches!(
            event,
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. })
        ) {
            warn!(event = ?event, "Incoming replication committed but drain scheduling returned an unexpected event");
        }
        self.finish_live_drain()
    }

    fn accept_blob_registration(&mut self, event: Event) -> Turn {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
            _ => self.invalid("Event::Net(NetEvent::Dht(DhtEvent::*))", event.label()),
        }
    }

    fn accept_completion_sent(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
            return self.invalid("Event::Blob(BlobEvent::MessageSent)", event.label());
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

// Phase: close
impl IncomingVersionOperation {
    fn accept_connection_close(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
            return self.invalid("Event::Blob(BlobEvent::ConnectionClosed)", event.label());
        };
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            "Closed incoming replication connection"
        );
        let output = self.output.take();
        Ok(Next::Done(
            output.unwrap_or_else(|| self.result(self.apply_committed)),
        ))
    }
}

/// The reference version `manifest` publishes as a bound member stores it, before compaction.
pub(crate) fn member_reference(
    manifest: VersionReplicationManifest,
    publisher: NodeId,
    realm_id: RealmId,
) -> Result<BlobVersion, IncomingVersionError> {
    let mut operation = IncomingVersionOperation::new(Ulid::nil(), publisher, realm_id, manifest);
    operation.destination_bound = true;
    operation.reference_version()
}

/// Whether a stored version is the incoming one; a local refresh changes only `last_refresh`.
pub(crate) fn same_reference(existing: &BlobVersion, incoming: &BlobVersion) -> bool {
    let mut existing = existing.clone();
    if let (
        BlobVersionState::Reference { last_refresh, .. },
        BlobVersionState::Reference {
            last_refresh: incoming_refresh,
            ..
        },
    ) = (&mut existing.state, &incoming.state)
    {
        *last_refresh = *incoming_refresh;
    }
    existing == *incoming
}

mod admission;
pub(crate) mod stream;

#[cfg(test)]
mod pure_tests;

/// Gate acceptance for an incoming replica: nothing governed is admitted
/// without a compliant local destination, and a reference registers nothing.
#[cfg(test)]
mod decision_tests;

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;
