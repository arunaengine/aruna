//! Owns the state machine that accepts an inbound object version and stores it locally.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::root_record::{compact_version, root_write};
use crate::distribution::inventory::{AddedVersion, InventoryChange, version_written};
use crate::replication::error::ReplicationError;
use crate::s3::abe_copies::copy_row;
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::object::put::abe::{abe_reads, keyed, located_rows, parse_abe};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::usage::{
    QuotaGate, QuotaGateError, RowChange, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_snapshot_publish,
};
use aruna_core::blob_records::{HeadAliasContext, dht_registration_effect, release_rows};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{AuthorizationError, BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
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
use aruna_core::replication_protocol::{VersionReplicationManifest, VersionReplicationMessage};
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::input::RoCrateLimits;
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
use aruna_core::structs::storage::distribution::delivery::AdmissionError;
use aruna_core::structs::storage::distribution::inventory::{VersionEntry, logical_identity};
use aruna_core::structs::storage::distribution::source_root::RootEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
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
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
use aruna_core::structs::storage::replication_queue::live_obligation_entry;
use aruna_core::structs::storage::replication_queue::{LiveObligationRecord, schedule_blob_drain};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingError, StorageRoutingRule, resolve_backend,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::structs::synced_folder::tagged_receipt;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use aruna_ops_identity::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use aruna_ops_identity::groups::storage_routing::load_group_inputs;
use aruna_ops_identity::placement::policy::gate::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, gate_decision, split_drift_reads,
    union_refs, write_gate,
};
use aruna_ops_identity::placement::policy::read::ReadPolicyError;
use std::time::{Duration, SystemTime};
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
    /// The runner clock of the current step. A reclaim candidate is stamped when it is
    /// enqueued, so a long apply still gets its full grace window.
    now_ms: u64,
    ids: IdMint,
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
            now_ms: 0,
            ids: IdMint::default(),
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
                calls.push(Effect::blob(BlobEffect::ReleaseReservation { id }).into());
            }
        }
        if self.owes_rejection() {
            let reason = self.failure.take();
            let reason = reason.unwrap_or_else(|| "version replication apply failed".to_string());
            if let Ok(payload) = VersionReplicationMessage::VersionApplyRejected(reason).to_bytes()
            {
                let send = BlobEffect::SendMessage { stream_id, payload };
                calls.push(Effect::blob(send).into());
            }
        }
        let cleanup = self.received_blob.as_mut();
        if let Some(location) = cleanup.and_then(ReceivedBlob::take_cleanup) {
            calls.push(Effect::blob(BlobEffect::Delete { location }).into());
        }
        calls.push(Effect::blob(BlobEffect::CloseConnection { stream_id }).into());
        calls
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
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
        self.call(state, Effect::blob(effect).into())
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

    /// The replica is an archive kept as the sender granted it; its content hash is pending.
    fn pending_replica(&self) -> bool {
        self.received_blob.as_ref().is_some_and(|received| {
            received.location.format.bucket_key().is_some()
                && received.location.get_blake3().is_none()
        })
    }
}

/// The reference version `manifest` publishes as a bound member stores it, before compaction.
pub fn member_reference(
    manifest: VersionReplicationManifest,
    publisher: NodeId,
    realm_id: RealmId,
) -> Result<BlobVersion, IncomingVersionError> {
    let mut operation = IncomingVersionOperation::new(Ulid::nil(), publisher, realm_id, manifest);
    operation.destination_bound = true;
    operation.reference_version()
}

/// Whether a stored version is the incoming one; a local refresh changes only `last_refresh`.
pub fn same_reference(existing: &BlobVersion, incoming: &BlobVersion) -> bool {
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
mod commit;
mod negotiation;
mod receiving;
mod replaced;
pub mod stream;

#[cfg(test)]
mod pure_tests;

/// Gate acceptance for an incoming replica: nothing governed is admitted
/// without a compliant local destination, and a reference registers nothing.
#[cfg(test)]
mod decision_tests;

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;
