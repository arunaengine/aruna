//! Declares the events adapters return for each effect, one variant group per domain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::audit::AuditPageBatch;
use crate::compute::{SecretBytes, SharedSecret};
use crate::effects::{
    FrameBoundsError, JOB_PAGE_BYTES, JobRecordFrame, MAX_RECORD_PAGE, encoded_len,
};
use crate::errors::{BlobError, SourceResolutionError, StagingSourceError};
use crate::key_index::IndexEvent;
use crate::metadata::MetadataEvent;
use crate::node_vault::VaultEntry;
use crate::outbound::OutboundEvent;
use crate::stream::{BackendStream, StreamError as BackendStreamError};
use crate::structs::execution::source_access::{
    ListCursor, ResolvedSourceAccess, ResolvedSourceConnector, SourceEntry, SourceMetadata,
    SourceStats,
};
use crate::structs::identity::realm::RealmId;
use crate::structs::identity::user::vault::VaultRecords;
use crate::structs::placement::policy::document::{PlacementPolicyDocument, PolicyPublication};
use crate::structs::placement::policy::{MAX_REF_INPUT, PlacementDecision};
use crate::structs::storage::blob::{BackendLocation, HiddenBlobEntry};
use crate::structs::storage::encryption::{
    BucketKeyRef, KeyTicket, ReadLease, SealedCopy, UnlockStatus,
};
use crate::structs::storage::multipart::{BackendUpload, PartPiece, WorkingShare};
use crate::structs::storage::replication::{ReplicationItemError, ReplicationSuboperationResult};
use crate::structs::storage::routing::GroupRoutingInputs;
use crate::{
    document::DocumentNetEvent,
    errors::{AuthorizationError, DhtError, StorageError, StreamError},
    id::{DhtKeyId, NodeId},
    jobs::JobResponse,
    task::TaskEvent,
    types::{Key, KeySpace, TxnId, Value},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Blob(BlobEvent),
    StagingSource(StagingSourceEvent),
    LocalFile(LocalFileEvent),
    Storage(StorageEvent),
    Net(NetEvent),
    Metadata(MetadataEvent),
    SubOperation(SubOperationEvent),
    Task(TaskEvent),
    Search(),
    Stream(),
    Index(IndexEvent),
    Group(GroupEvent),
    Session(Box<crate::compute::session::SessionAnswer>),
    Outbound(OutboundEvent),
    /// Answers `Effect::Delay` once its time passed.
    Delayed,
    /// The adapter of this effect kind did not answer within the runner's hang limit.
    Hung {
        effect: &'static str,
    },
    /// The typed outcome of a child made with `IoCall::child`; never persisted.
    Child(crate::calls::ChildOutcome),
}

/// Results of a [`crate::effects::EffectGroup`]; never persisted.
#[derive(Debug, PartialEq)]
pub enum GroupEvent {
    /// Every member result of an all-at-once group, in emission order.
    Finished { results: Vec<GroupResult> },
    /// One finished window member; `index` counts the window's members in emission order.
    Member { index: usize, result: GroupResult },
    /// The window has no running or waiting member left.
    Closed,
}

/// What one group member returned.
#[derive(Debug, PartialEq)]
pub enum GroupResult {
    Done(Box<Event>),
    /// The group deadline passed before the member finished.
    Unfinished,
    /// The member is a storage transaction effect or a group, which a group cannot run.
    Refused,
}

#[derive(Debug, PartialEq)]
pub enum SubOperationEvent {
    GroupWritten {
        result: Result<(), crate::structs::identity::group_delete::GroupWriteError>,
    },
    DepthLimitExceeded {
        max_depth: usize,
    },
    AuthorizationResult {
        allowed: Result<bool, AuthorizationError>,
    },
    RealmNodesResult {
        result: Result<Vec<NodeId>, String>,
    },
    DocumentSyncResult {
        result: Result<(), String>,
    },
    SourceConnectorResolved {
        result: Box<Result<ResolvedSourceConnector, SourceResolutionError>>,
    },
    VersionAccessResolved {
        result: Result<ResolvedSourceAccess, SourceResolutionError>,
    },
    ReplicationItemResult {
        result: Result<ReplicationSuboperationResult, ReplicationItemError>,
    },
    ReplicationTransferResult {
        result: Result<(), String>,
    },
    ReplicationApplyResult {
        result: Result<(), String>,
    },
    LiveReplicationQueued {
        result: Result<(), String>,
    },
    RefreshQueued {
        result: Result<(), String>,
    },
    BucketCreated {
        result: Result<(), String>,
    },
    GroupRoutingLoaded {
        result: Result<GroupRoutingInputs, String>,
    },
    NotificationsEmitted,
    /// A share link's access log entry and use counters were written.
    ShareLogged {
        result: Result<(), String>,
    },
    TokenRevoked {
        result: Result<(), String>,
    },
    UserUpdated {
        result: Result<(), String>,
    },
    /// BLAKE3 digests of the saved payloads of a user's vault heads; empty without a vault.
    VaultHeld {
        result: Result<Vec<[u8; 32]>, String>,
    },
    KeyRequestsOpened {
        request_ids: Vec<Ulid>,
    },
    /// A key request run failed and may succeed on retry.
    KeyRequestsFailed,
    /// One version of a re-key page ended.
    VersionRekeyed {
        outcome: RekeyOutcome,
    },
    /// A kept bucket key audit record was stored and its retry timer row removed.
    AuditStored {
        result: Result<(), String>,
    },
    /// The key directory answer for one user.
    KeysLooked {
        lookup: crate::structs::storage::holders::KeyLookup,
    },
    /// Whether a bucket sealed a missing key copy for a holder's new user key.
    CopySealed {
        sealed: bool,
    },
    /// Whether one parked copy job woke; the row of its wait is gone either way.
    CopyWoken {
        result: Result<bool, String>,
    },
    /// The emission stored no notification; its failure as text.
    NotificationsFailed {
        reason: String,
    },
}

/// How the re-key of one version ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RekeyOutcome {
    Moved,
    Skipped,
    /// The bucket key is locked on this node.
    Locked,
    Failed,
    /// The version needs a later page; this page stops before it.
    Unfinished,
}

#[derive(Debug, PartialEq)]
pub enum BlobEvent {
    WriteFinished {
        location: BackendLocation,
    },
    ReadFinished {
        blob: BackendStream<Result<Bytes, BackendStreamError>>,
        stream_size: u64,
    },
    DeleteFinished,
    ReservationReleased {
        id: Ulid,
    },
    HiddenSpooled {
        location: BackendLocation,
        blake3: [u8; 32],
        size: u64,
    },
    HiddenRead {
        blob: BackendStream<Result<Bytes, BackendStreamError>>,
        stream_size: u64,
    },
    /// The hashes of all spooled bytes and a stream of the requested range of them.
    LocalSpooled {
        hashes: std::collections::HashMap<String, Vec<u8>>,
        size: u64,
        blob: BackendStream<Result<Bytes, BackendStreamError>>,
    },
    HiddenDeleted,
    HiddenListed {
        entries: Vec<HiddenBlobEntry>,
        next_cursor: Option<Vec<u8>>,
    },
    ConnectionEstablished {
        stream_id: Ulid,
    },
    ConnectionClosed {
        stream_id: Ulid,
    },
    MessageReceived {
        stream_id: Ulid,
        payload: Vec<u8>,
    },
    MessageSent {
        stream_id: Ulid,
    },
    ReplicationFinished {
        location: BackendLocation,
    },
    ReadServed {
        stream_id: Ulid,
    },
    GroupBackendChecked,
    UploadOpened {
        backend_upload: Option<BackendUpload>,
    },
    /// An in-place part: its location is partial and names the provider upload's target.
    PartWritten {
        location: BackendLocation,
        backend_etag: String,
    },
    UploadAborted,
    BucketKeyGenerated {
        public_key: [u8; 32],
        private_key: SharedSecret,
    },
    CopiesSealed {
        copies: Vec<SealedCopy>,
    },
    KeyPrepared {
        ticket: KeyTicket,
    },
    KeyActivated {
        status: UnlockStatus,
    },
    KeyDiscarded {
        ticket: KeyTicket,
    },
    KeyStatus {
        generations: Vec<UnlockStatus>,
    },
    KeyExtended {
        status: UnlockStatus,
    },
    /// The generations whose keys were removed; already locked buckets report none.
    KeyLocked {
        locked: Vec<KeyTicket>,
        sequence: Ulid,
        live: Option<Box<UnlockStatus>>,
    },
    ReadAdmitted {
        lease: ReadLease,
    },
    Error(BlobError),
    /// The new copy of a rewrite; its reservation is held until released.
    CopyRewritten {
        location: BackendLocation,
    },
    /// A part sealed as a Pithos piece; the location holds the original size and checksums.
    PieceWritten {
        location: BackendLocation,
        piece: PartPiece,
    },
    UnlockedKeyRead {
        key: BucketKeyRef,
        private_key: SharedSecret,
    },
    /// The raw hashes and size of the verified plaintext of a sealed copy.
    ArchiveHashed {
        hashes: std::collections::HashMap<String, Vec<u8>>,
        size: u64,
    },
    /// The working set of one composition; the completion keeps it until composition ends.
    ComposeReserved {
        share: WorkingShare,
    },
    Abe(Box<crate::structs::storage::abe::AbeEvent>),
    /// One listing page; `next` is the `start_after` of the following page.
    ExternalListed {
        objects: Vec<crate::structs::storage::backend_import::ExternalObject>,
        next: Option<String>,
    },
    /// `version` names the bytes read on a backend that keeps versions; `etag` is the
    /// backend's ETag of the bytes read, when it reports one.
    ExternalHashed {
        location: BackendLocation,
        version: Option<String>,
        etag: Option<String>,
    },
    /// Whether a file is at the path, and its version id on a backend that keeps versions.
    PlainStat {
        exists: bool,
        version: Option<String>,
    },
    /// The metadata of an external file; its location has no hashes, and is `None` without a
    /// file at the path.
    ExternalStat {
        location: Option<BackendLocation>,
        version: Option<String>,
        etag: Option<String>,
    },
    CopyStat {
        exists: bool,
    },
    /// `version` is the backend's id of the bytes written, on a backend that keeps versions;
    /// `etag` is the backend's ETag of them, when it reports one.
    PlainWritten {
        location: BackendLocation,
        version: Option<String>,
        etag: Option<String>,
    },
    /// The bucket is ready; `created` when Aruna created it.
    BucketReady {
        created: bool,
    },
    /// `writable` says whether a test write and delete worked; `None` for a missing bucket.
    BucketExists {
        exists: bool,
        writable: Option<bool>,
    },
    /// `deleted` is false when the bucket still held objects and stays.
    BucketDeleted {
        deleted: bool,
    },
}

#[derive(Debug, PartialEq)]
pub enum StagingSourceEvent {
    CheckResult,
    HeadResult {
        metadata: SourceMetadata,
    },
    ListResult {
        entries: Vec<SourceEntry>,
        /// `None` once the listing is exhausted or reached its stop key.
        next_cursor: Option<ListCursor>,
    },
    ReadResult {
        metadata: SourceMetadata,
        stream: BackendStream<Result<Bytes, BackendStreamError>>,
    },
    HealthResult {
        stats: Vec<SourceStats>,
        /// Uniformly random, so the operation can pick among sources deterministically.
        draw: u64,
    },
    Error {
        error: StagingSourceError,
    },
}

/// Why the adapter refused to touch a file on the owner's disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocalFileRefusal {
    /// The target no longer carries the bytes the guard named.
    Drifted,
    /// The target exists and the guard forbids replacing anything.
    Exists,
    Missing,
    /// The path resolved outside the folder root.
    Escaped,
    /// The target is not a regular file, so no guard can vouch for its bytes.
    NotRegular,
}

/// Reply to a [`crate::effects::LocalFileEffect`]. A refusal is an outcome, not
/// a fault: the operation records it and reports the entry to the owner.
#[derive(Debug, PartialEq)]
pub enum LocalFileEvent {
    Written {
        fingerprint: String,
        blake3: [u8; 32],
        size: u64,
    },
    /// The incoming bytes landed beside the file under this relative path.
    Copied {
        relative: String,
        fingerprint: String,
        blake3: [u8; 32],
        size: u64,
    },
    Moved {
        to: String,
    },
    Hashed {
        fingerprint: String,
        blake3: [u8; 32],
        size: u64,
    },
    Refused {
        reason: LocalFileRefusal,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum StorageEvent {
    TransactionStarted {
        txn_id: TxnId,
    },
    TransactionCommitted {
        txn_id: TxnId,
    },
    TransactionAborted {
        txn_id: TxnId,
    },
    ReadResult {
        key: Key,
        value: Option<Value>,
    },
    /// Values in the same order as the requested reads.
    BatchReadResult {
        values: Vec<(Key, Option<Value>)>,
    },
    WriteResult {
        key: Key,
    },
    BatchWriteResult {
        entries: Vec<(KeySpace, Key)>,
    },
    DeleteResult {
        key: Key,
    },
    BatchDeleteResult {
        entries: Vec<(KeySpace, Key)>,
    },
    SyncAllFinished,
    /// Result of an iteration request with optional pagination cursor.
    IterResult {
        values: Vec<(Key, Value)>,
        next_start_after: Option<Key>,
    },
    /// The opened node vault record of `entry`, if it exists.
    VaultResult {
        entry: VaultEntry,
        secret: Option<SecretBytes>,
    },
    Error {
        error: StorageError,
    },
    /// The opened realm secret, or why this node could not open it.
    RealmOpened {
        result: Result<SecretBytes, crate::realm_vault::RealmVaultError>,
    },
}

#[derive(Debug, PartialEq)]
pub enum NetEvent {
    Dht(DhtEvent),
    DocumentSync(DocumentNetEvent),
    Stream(StreamEvent),
    JobControl(JobControlEvent),
    AuditPages(AuditPageBatch),
    PolicyFetch(PolicyFetchEvent),
    PolicySign(PolicySignEvent),
    VaultFetch(VaultFetchEvent),
    VaultSign(VaultSignEvent),
    SnapshotSign(SnapshotSignEvent),
    StatementSign(StatementSignEvent),
    ExecutionSign(ExecutionSignEvent),
    Release(crate::structs::storage::release::ReleaseEvent),
    Error(NetError),
    /// Whether the node holds the complete artifact after the chunk, or why it was not stored.
    ArtifactStored(Result<bool, String>),
    /// The holder's answer frame, or why none came.
    Notification(
        Result<Box<crate::structs::execution::notification::NotificationTransportMessage>, String>,
    ),
    InboxWoken,
    Heartbeat(crate::heartbeat::HeartbeatEvent),
    MetadataAnswer(Box<MetadataAnswer>),
    /// The signed read grant, or `None` when this node cannot sign it.
    GrantSign(Option<Box<crate::structs::storage::read_grant::SignedGrant>>),
    /// The caller a forwarded token stands for, or why it is not accepted from that peer.
    WritePeerAuth(Result<Box<crate::structs::identity::auth::AuthContext>, PeerAuthRefusal>),
    /// The JSON-LD summary of a dataset, or why it could not be read.
    DatasetSummary(Result<String, crate::structs::storage::dataset_description::DescriptionError>),
    /// A local signal or change was taken; nothing is read back.
    Recorded,
}

/// Reply to a [`crate::effects::MetadataRequestEffect`]: the encoded answer or why there is none,
/// and how long the request took, so one deadline can span the requests to several peers.
#[derive(Debug, PartialEq)]
pub struct MetadataAnswer {
    pub peer: NodeId,
    pub elapsed: Duration,
    pub result: Result<Vec<u8>, PeerRequestError>,
}

/// Why a forwarded token gives a peer no caller.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PeerAuthRefusal {
    #[error("the forwarded token is not accepted")]
    Unauthorized,
    #[error("{0}")]
    Unavailable(String),
}

impl From<crate::calls::IoError> for PeerAuthRefusal {
    fn from(error: crate::calls::IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// Why a peer request has no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRequestError {
    /// The request failed before an answer arrived.
    Failed(crate::metadata::MetadataError),
    /// No answer arrived within the request's timeout.
    TimedOut,
}

/// Reply to a [`crate::effects::NetEffect::PolicySign`]. The adapter signs only
/// a claim naming this node, so no operation can mint foreign provenance.
#[derive(Debug, PartialEq)]
pub enum PolicySignEvent {
    Signed(Box<PolicyPublication>),
    Unavailable(String),
}

/// Result of a policy fetch from the holders of a [`crate::effects::PolicyFetchEffect`]. A
/// fetched document is a candidate only; the operation verifies its definition and publication
/// authority before it may be cached or matched against a subject.
#[derive(Debug, PartialEq)]
pub enum PolicyFetchEvent {
    Fetched {
        publisher: NodeId,
        document: Box<PlacementPolicyDocument>,
    },
    /// Every reached holder answered without the document.
    NotFound,
    /// No holder answered, so the miss is an availability hint, never a denial.
    Unavailable(String),
}

/// Reply to a [`crate::effects::NetEffect::VaultSign`]: the same records, signed. The
/// adapter signs only records naming this node, so no operation can mint another origin.
#[derive(Debug, PartialEq)]
pub enum VaultSignEvent {
    Signed(Box<VaultRecords>),
    Unavailable(String),
}

/// Reply to a [`crate::effects::NetEffect::SnapshotSign`]. The adapter signs only a snapshot
/// whose set names this node as publisher.
#[derive(Debug, PartialEq)]
pub enum SnapshotSignEvent {
    Signed(Box<crate::structs::storage::distribution::SignedSnapshot>),
    Unavailable(String),
}

/// Reply to a [`crate::effects::NetEffect::StatementSign`]. The adapter signs only a claim
/// that names this node as signer.
#[derive(Debug, PartialEq)]
pub enum StatementSignEvent {
    Signed(Box<crate::structs::storage::distribution::SignedStatement>),
    Unavailable(String),
}

/// Reply to a [`crate::effects::NetEffect::ExecutionSign`]. The adapter signs only a claim
/// that names this node as publisher.
#[derive(Debug, PartialEq)]
pub enum ExecutionSignEvent {
    Signed(Box<crate::structs::execution::job::JobRecordEnvelope>),
    Unavailable(String),
}

/// Result of a vault fetch from the holders of a [`crate::effects::VaultFetchEffect`].
#[derive(Debug, PartialEq)]
pub enum VaultFetchEvent {
    Fetched {
        holder: NodeId,
        records: VaultRecords,
    },
    /// Every reached holder answered without records.
    NotFound,
    /// A holder refused the forwarded caller.
    Denied,
    /// No holder answered, so the miss is an availability hint, never a denial.
    Unavailable(String),
}

/// Bounded page of immutable job-family records, oldest key first. Its bounds
/// hold for a decoded page too, so a holder cannot answer with an unbounded one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<JobRecordFrame>")]
pub struct JobRecordPage(Vec<JobRecordFrame>);

impl JobRecordPage {
    pub fn new(records: Vec<JobRecordFrame>) -> Result<Self, FrameBoundsError> {
        if records.len() > MAX_RECORD_PAGE {
            return Err(FrameBoundsError::RecordCount);
        }
        if encoded_len(&records)? > JOB_PAGE_BYTES {
            return Err(FrameBoundsError::PageBytes);
        }
        Ok(Self(records))
    }

    /// Rejects an oversized frame before it is decoded, so a peer cannot force
    /// the allocation of a page it is not allowed to send.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FrameBoundsError> {
        if bytes.len() > JOB_PAGE_BYTES {
            return Err(FrameBoundsError::PageBytes);
        }
        Self::new(postcard::from_bytes(bytes)?)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, FrameBoundsError> {
        Ok(postcard::to_allocvec(&self.0)?)
    }

    pub fn records(&self) -> &[JobRecordFrame] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<JobRecordFrame> {
        self.0
    }
}

impl TryFrom<Vec<JobRecordFrame>> for JobRecordPage {
    type Error = FrameBoundsError;

    fn try_from(records: Vec<JobRecordFrame>) -> Result<Self, Self::Error> {
        Self::new(records)
    }
}

/// Why a holder refused an append-only job record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobRecordRejection {
    /// The responder does not hold that family placement; resolve holders again.
    NotHolder,
    /// A record with the same key and different bytes is already retained.
    Conflict,
    /// The publisher signature or the kind's exact author rule failed.
    Unauthorized,
    /// The record failed contract validation, such as budget or chain rules.
    Invalid,
}

/// The placement detail one decline may carry. Decoding rejects an oversized
/// ref or id list and an `Allowed` decision, so a peer can neither force an
/// unbounded allocation nor answer a refusal with a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PlacementDecision")]
pub struct DeclinedPolicy(PlacementDecision);

impl DeclinedPolicy {
    pub fn new(decision: PlacementDecision) -> Result<Self, FrameBoundsError> {
        let listed = match &decision {
            PlacementDecision::Allowed => return Err(FrameBoundsError::RecordKind),
            PlacementDecision::Required { refs } | PlacementDecision::DigestMismatch { refs } => {
                refs.len()
            }
            PlacementDecision::Unavailable { policy_ids }
            | PlacementDecision::Invalid { policy_ids }
            | PlacementDecision::Denied { policy_ids } => policy_ids.len(),
            PlacementDecision::InvalidInput { .. } => 0,
        };
        match listed <= MAX_REF_INPUT {
            true => Ok(Self(decision)),
            false => Err(FrameBoundsError::RecordCount),
        }
    }

    pub fn decision(&self) -> &PlacementDecision {
        &self.0
    }
}

impl TryFrom<PlacementDecision> for DeclinedPolicy {
    type Error = FrameBoundsError;

    fn try_from(decision: PlacementDecision) -> Result<Self, Self::Error> {
        Self::new(decision)
    }
}

/// Why an execution target refused a launch offer. The policy detail travels
/// with the decline in its own bounded wrapper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LaunchDecline {
    /// The offering scheduler is not a holder in the target's current view.
    NotHolder,
    /// Realm or group authorization denied the stored submitter.
    Unauthorized,
    /// Placement evaluation blocked the execution subject; never `Allowed`.
    Policy(DeclinedPolicy),
    /// Exact local admission found no capacity for the stored resources.
    Capacity,
    /// The target is draining or leaving and accepts no new work.
    Draining,
    /// That launch id is already bound to a different launch digest.
    LaunchConflict,
    /// The request family was cancelled before the offer arrived.
    Cancelled,
    /// This node already runs, or already ran successfully, an execution of the
    /// same family. Retryable: another target may still take the launch.
    AlreadyRunning,
}

/// Reply to a [`crate::effects::JobControlEffect`]: the owner's response, or an
/// unreachable-owner failure the routing operation maps to `Unavailable` (503).
#[derive(Debug, PartialEq)]
pub enum JobControlEvent {
    Response(Box<JobResponse>),
    Unavailable(String),
}

#[derive(Debug, PartialEq)]
pub enum DhtEvent {
    PutComplete {
        key: DhtKeyId,
        remote_attempt_count: usize,
        remote_store_count: usize,
    },
    GetResult {
        key: DhtKeyId,
        values: Vec<DhtEntry>,
        /// Answered from a bounded-stale snapshot instead of a completed lookup.
        /// Stale values are candidates, never proof that a peer is reachable.
        stale: bool,
    },
    Error {
        error: DhtError,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DhtEntry {
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Debug, PartialEq)]
pub enum StreamEvent {
    Opened { stream_id: u64, node_id: NodeId },
    Closed { stream_id: u64 },
    Error { stream_id: u64, error: StreamError },
}

#[derive(Debug, PartialEq)]
pub enum NetError {
    InvalidEffect,
    ChannelClosed,
}

/// Expands to the `Kind::Variant` name of a value, matched exhaustively over the listed variants.
macro_rules! variant_label {
    ($value:expr, $kind:ident { $($variant:ident),* $(,)? }) => {
        match $value {
            $($kind::$variant { .. } => concat!(stringify!($kind), "::", stringify!($variant)),)*
        }
    };
}

impl MetadataEvent {
    /// Names the metadata event variant without its values.
    pub fn label(&self) -> &'static str {
        variant_label!(
            self,
            MetadataEvent {
                ValidationResult,
                CreateCrateResult,
                ApplyRoCrateResult,
                EntityUpsertResult,
                GraphPolicySet,
                GraphPeerAdded,
                GraphSyncScheduled,
                GraphPolicyResult,
                RoCrateExportResult,
                RoCrateSummaryResult,
                RoCratePageResult,
                SearchResult,
                QueryResult,
                GraphDeleted,
                GraphListResult,
                ContainsGraphResult,
                ContainsDotResult,
                VersionedExport,
                GraphSnapshotResult,
                SnapshotInstalled,
                BatchPlanned,
                BatchMerged,
                Error,
            }
        )
    }
}

impl Event {
    /// Names the event variant without its values, for logs and unexpected-event errors.
    pub fn label(&self) -> &'static str {
        match self {
            Event::Blob(event) => variant_label!(
                event,
                BlobEvent {
                    WriteFinished,
                    ReadFinished,
                    DeleteFinished,
                    ReservationReleased,
                    HiddenSpooled,
                    HiddenRead,
                    LocalSpooled,
                    HiddenDeleted,
                    HiddenListed,
                    ConnectionEstablished,
                    ConnectionClosed,
                    MessageReceived,
                    MessageSent,
                    ReplicationFinished,
                    ReadServed,
                    GroupBackendChecked,
                    UploadOpened,
                    PartWritten,
                    UploadAborted,
                    BucketKeyGenerated,
                    CopiesSealed,
                    KeyPrepared,
                    KeyActivated,
                    KeyDiscarded,
                    KeyStatus,
                    KeyExtended,
                    KeyLocked,
                    ReadAdmitted,
                    Error,
                    CopyRewritten,
                    PieceWritten,
                    UnlockedKeyRead,
                    ArchiveHashed,
                    ComposeReserved,
                    Abe,
                    ExternalListed,
                    ExternalHashed,
                    PlainStat,
                    ExternalStat,
                    CopyStat,
                    PlainWritten,
                    BucketReady,
                    BucketExists,
                    BucketDeleted,
                }
            ),
            Event::StagingSource(event) => variant_label!(
                event,
                StagingSourceEvent {
                    CheckResult,
                    HeadResult,
                    ListResult,
                    ReadResult,
                    HealthResult,
                    Error,
                }
            ),
            Event::LocalFile(event) => variant_label!(
                event,
                LocalFileEvent {
                    Written,
                    Copied,
                    Moved,
                    Hashed,
                    Refused,
                    Error,
                }
            ),
            Event::Storage(event) => variant_label!(
                event,
                StorageEvent {
                    TransactionStarted,
                    TransactionCommitted,
                    TransactionAborted,
                    ReadResult,
                    BatchReadResult,
                    WriteResult,
                    BatchWriteResult,
                    DeleteResult,
                    BatchDeleteResult,
                    SyncAllFinished,
                    IterResult,
                    VaultResult,
                    Error,
                    RealmOpened,
                }
            ),
            Event::Net(event) => variant_label!(
                event,
                NetEvent {
                    Dht,
                    DocumentSync,
                    Stream,
                    JobControl,
                    AuditPages,
                    PolicyFetch,
                    PolicySign,
                    VaultFetch,
                    VaultSign,
                    SnapshotSign,
                    StatementSign,
                    ExecutionSign,
                    Release,
                    Error,
                    ArtifactStored,
                    MetadataAnswer,
                    DatasetSummary,
                    WritePeerAuth,
                    GrantSign,
                    Notification,
                    InboxWoken,
                    Heartbeat,
                    Recorded,
                }
            ),
            Event::Metadata(event) => event.label(),
            Event::SubOperation(event) => variant_label!(
                event,
                SubOperationEvent {
                    GroupWritten,
                    DepthLimitExceeded,
                    AuthorizationResult,
                    RealmNodesResult,
                    DocumentSyncResult,
                    SourceConnectorResolved,
                    VersionAccessResolved,
                    ReplicationItemResult,
                    ReplicationTransferResult,
                    ReplicationApplyResult,
                    LiveReplicationQueued,
                    RefreshQueued,
                    BucketCreated,
                    GroupRoutingLoaded,
                    NotificationsEmitted,
                    ShareLogged,
                    TokenRevoked,
                    UserUpdated,
                    VaultHeld,
                    KeyRequestsOpened,
                    KeyRequestsFailed,
                    VersionRekeyed,
                    AuditStored,
                    KeysLooked,
                    CopySealed,
                    CopyWoken,
                    NotificationsFailed,
                }
            ),
            Event::Task(event) => variant_label!(
                event,
                TaskEvent {
                    TimerScheduled,
                    TimerCancelled,
                    RunningHandlersAborted,
                    Error,
                }
            ),
            Event::Group(event) => variant_label!(
                event,
                GroupEvent {
                    Finished,
                    Member,
                    Closed
                }
            ),
            Event::Delayed => "Event::Delayed",
            Event::Hung { .. } => "Event::Hung",
            Event::Child(_) => "Event::Child",
            Event::Search() => "Event::Search",
            Event::Stream() => "Event::Stream",
            Event::Index(event) => variant_label!(
                event,
                IndexEvent {
                    Applied,
                    Searched,
                    Exists,
                    Outdated,
                    Counted,
                    Pruned,
                    Error,
                }
            ),
            Event::Session(_) => "Event::Session",
            Event::Outbound(event) => variant_label!(
                event,
                OutboundEvent {
                    Posted,
                    MailSent,
                    Backoff,
                    Answered,
                    ConnectionAnswered
                }
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::effects::sized_envelope;

    fn frame(objects: usize, key_bytes: usize) -> JobRecordFrame {
        JobRecordFrame::new(sized_envelope(objects, key_bytes)).expect("bounded record")
    }

    #[test]
    fn label_omits_values() {
        let event = Event::Task(TaskEvent::Error {
            key: None,
            message: "private detail".to_string(),
        });
        assert_eq!(event.label(), "TaskEvent::Error");
        assert_eq!(Event::Search().label(), "Event::Search");
    }

    #[test]
    fn rejects_wide_page() {
        let records = vec![frame(1, 8); MAX_RECORD_PAGE + 1];
        assert_eq!(
            JobRecordPage::new(records.clone()),
            Err(FrameBoundsError::RecordCount)
        );
        let encoded = postcard::to_allocvec(&records).expect("records encode");
        assert_eq!(
            JobRecordPage::from_bytes(&encoded),
            Err(FrameBoundsError::RecordCount)
        );
        assert!(postcard::from_bytes::<JobRecordPage>(&encoded).is_err());
    }

    #[test]
    fn rejects_decoded_record() {
        // A page frame may not smuggle a record past the per-record byte bound.
        let encoded =
            postcard::to_allocvec(&vec![sized_envelope(1024, 1200)]).expect("records encode");
        assert!(JobRecordPage::from_bytes(&encoded).is_err());
    }

    #[test]
    fn rejects_heavy_page() {
        // A record count within the page bound still cannot exceed the byte bound.
        let records = vec![frame(1024, 800); 5];
        assert_eq!(
            JobRecordPage::new(records),
            Err(FrameBoundsError::PageBytes)
        );
        let frame = vec![0u8; JOB_PAGE_BYTES + 1];
        assert_eq!(
            JobRecordPage::from_bytes(&frame),
            Err(FrameBoundsError::PageBytes)
        );
    }

    #[test]
    fn page_round_trips() {
        let page = JobRecordPage::new(vec![frame(1, 8), frame(2, 8)]).expect("bounded page");
        let bytes = page.to_bytes().expect("page encodes");
        assert_eq!(JobRecordPage::from_bytes(&bytes), Ok(page.clone()));
        assert_eq!(page.records().len(), 2);
    }

    #[test]
    fn page_keeps_publishers() {
        // Paging is transport: every relayed record keeps its own publisher.
        let envelope = sized_envelope(3, 8);
        let page = JobRecordPage::new(vec![frame(3, 8)]).expect("bounded page");
        let bytes = page.to_bytes().expect("page encodes");
        let relayed = JobRecordPage::from_bytes(&bytes).expect("page decodes");
        let first = relayed.records()[0].envelope();
        assert_eq!(first.published_by, envelope.published_by);
        assert!(first.verify_signature().is_ok());
    }
}
