use crate::audit::AuditPageBatch;
use crate::effects::JobFamilyRecord;
use crate::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use crate::metadata::MetadataEvent;
use crate::stream::{BackendStream, StreamError as BackendStreamError};
use crate::structs::{
    BackendLocation, ExecutionReceipt, GroupRoutingInputs, HiddenBlobEntry, PlacementDecision,
    PlacementPolicy, RealmId, ReplicationSuboperationResult, ResolvedSourceAccess,
    ResolvedSourceConnector, SourceEntry, SourceMetadata,
};
use crate::{
    document::DocumentSyncNetEvent,
    errors::{AuthorizationError, DhtError, StorageError, StreamError},
    id::{DhtKeyId, NodeId},
    jobs::JobResponse,
    task::TaskEvent,
    types::{Key, KeySpace, TxnId, Value},
};
use bytes::Bytes;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Event {
    Blob(BlobEvent),
    StagingSource(StagingSourceEvent),
    Storage(StorageEvent),
    Net(NetEvent),
    Metadata(MetadataEvent),
    SubOperation(SubOperationEvent),
    Task(TaskEvent),
    Search(),
    Stream(),
}

#[derive(Debug, PartialEq)]
pub enum SubOperationEvent {
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
        result: Box<Result<ResolvedSourceConnector, SourceConnectorResolutionError>>,
    },
    VersionSourceAccessResolved {
        result: Result<ResolvedSourceAccess, SourceConnectorResolutionError>,
    },
    ReplicationItemResult {
        result: Result<ReplicationSuboperationResult, String>,
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
    BucketCreated {
        result: Result<(), String>,
    },
    GroupRoutingLoaded {
        result: Result<GroupRoutingInputs, String>,
    },
    NotificationsEmitted,
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
    Error(BlobError),
}

#[derive(Debug, PartialEq)]
pub enum StagingSourceEvent {
    CheckResult,
    HeadResult {
        metadata: SourceMetadata,
    },
    ListResult {
        entries: Vec<SourceEntry>,
        truncated: bool,
    },
    ReadResult {
        metadata: SourceMetadata,
        stream: BackendStream<Result<Bytes, BackendStreamError>>,
    },
    Error {
        error: StagingSourceError,
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
    Error {
        error: StorageError,
    },
}

#[derive(Debug, PartialEq)]
pub enum NetEvent {
    Dht(DhtEvent),
    DocumentSync(DocumentSyncNetEvent),
    Stream(StreamEvent),
    JobControl(JobControlEvent),
    AuditPages(AuditPageBatch),
    PolicyFetch(PolicyFetchEvent),
    JobRecord(JobRecordEvent),
    LaunchOffer(LaunchOfferEvent),
    Error(NetError),
}

/// Reply to a [`crate::effects::PolicyFetchEffect`]. A fetched document is a
/// candidate only; the operation verifies it against the requested ref.
#[derive(Debug, PartialEq)]
pub enum PolicyFetchEvent {
    Fetched {
        publisher: NodeId,
        policy: Box<PlacementPolicy>,
    },
    /// Every reached holder answered without the document.
    NotFound,
    /// No holder answered, so the miss is an availability hint, never a denial.
    Unavailable(String),
}

/// Reply to a [`crate::effects::JobRecordEffect`].
#[derive(Debug, PartialEq)]
pub enum JobRecordEvent {
    /// One current holder durably accepted the immutable record.
    Published {
        holder: NodeId,
    },
    Rejected {
        holder: NodeId,
        reason: JobRecordRejection,
    },
    /// At most the requested page of records, oldest key first.
    Fetched {
        records: Vec<JobFamilyRecord>,
        next_cursor: Option<Vec<u8>>,
    },
    Unavailable(String),
}

/// Why a holder refused an append-only job record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRecordRejection {
    /// The responder does not hold that family placement; resolve holders again.
    NotHolder,
    /// A record with the same key and different bytes is already retained.
    Conflict,
    /// The publisher is not the record's only permitted author.
    Unauthorized,
    /// The record failed contract validation, such as budget or chain rules.
    Invalid,
}

/// Reply to a [`crate::effects::LaunchOfferEffect`].
#[derive(Debug, PartialEq)]
pub enum LaunchOfferEvent {
    Accepted(Box<ExecutionReceipt>),
    Declined(LaunchDecline),
    /// The target was unreachable; it may still have accepted the launch.
    Unavailable(String),
}

/// Why an execution target refused a launch offer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchDecline {
    /// The offering scheduler is not a holder in the target's current view.
    NotHolder,
    /// Realm or group authorization denied the sealed submitter.
    Unauthorized,
    /// Placement evaluation blocked the execution subject; never `Allowed`.
    Policy(PlacementDecision),
    /// Exact local admission found no capacity for the sealed resources.
    Capacity,
    /// The target is draining or leaving and accepts no new work.
    Draining,
    /// That launch id is already bound to a different launch digest.
    LaunchConflict,
    /// The request family was cancelled before the offer arrived.
    Cancelled,
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
