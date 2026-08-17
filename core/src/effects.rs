use std::time::{Duration, Instant};

use crate::alpn::Alpn;
use crate::audit::AuditPageRequest;
use crate::compute::ExecutionTargetId;
use crate::document::DocumentSyncEffect;
use crate::id::{DhtKeyId, NodeId};
use crate::jobs::JobRequest;
use crate::metadata::MetadataEffect;
use crate::operation::SubOperation;
use crate::stream::{BackendStream, StreamError};
use crate::structs::{
    BackendLocation, ExecutionReceipt, ExecutionUpdate, GroupStorageBackend,
    GroupStorageBackendSecret, HiddenBlobKey, JobCancelRecord, LaunchIntent, LogicalJobSpec,
    PlacementPolicyRef, PlacementRef, RealmId, ResolvedBackend, ResolvedSourceAccess,
    SubmissionClaim, SubmissionId, WitnessBudgetRecord,
};
use crate::task::TaskEffect;
use crate::types::UserId;
use crate::types::{Key, KeySpace, TxnId, Value};
use bytes::Bytes;
use std::ops::Range;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Effect {
    Blob(BlobEffect),
    StagingSource(StagingSourceEffect),
    Storage(StorageEffect),
    Net(NetEffect),
    Metadata(MetadataEffect),
    SubOperation(Box<dyn SubOperation>),
    Task(TaskEffect),
    Search(),
    Stream(),
}

#[derive(Debug, PartialEq)]
pub enum BlobEffect {
    //GetOperator { bucket: Option<String>, },
    // ----- Blob read & write -----
    Write {
        bucket: String,
        key: String,
        /// Backend chosen by the operation. The adapter executes, never routes.
        resolved: ResolvedBackend,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    WritePart {
        upload_id: Ulid,
        part_number: u16,
        /// Pinned at CreateMultipartUpload and carried by every part.
        resolved: ResolvedBackend,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    Compose {
        bucket: String,
        key: String,
        resolved: ResolvedBackend,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    },
    Read {
        location: BackendLocation,
    },
    ReadRange {
        location: BackendLocation,
        range: Range<u64>,
    },
    Delete {
        location: BackendLocation,
    },
    ReleaseReservation {
        id: Ulid,
    },
    SpoolHidden {
        namespace: Ulid,
        name: String,
        created_by: UserId,
        max_bytes: Option<u64>,
        deadline: Option<Instant>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    ReadHiddenRange {
        location: BackendLocation,
        range: Range<u64>,
    },
    DeleteHidden {
        key: HiddenBlobKey,
    },
    ListHidden {
        namespace: Option<Ulid>,
        cursor: Option<Vec<u8>>,
    },
    // ----- Replication -----
    OpenConnection {
        node_id: NodeId,
    },
    SendMessage {
        stream_id: Ulid,
        payload: Vec<u8>,
    },
    ReadMessage {
        stream_id: Ulid,
    },
    CloseConnection {
        stream_id: Ulid,
    },
    Replicate {
        replication_id: Ulid,
        stream_id: Ulid,
        location: BackendLocation,
        keep_alive: bool,
    },
    HandleReplication {
        replication_id: Option<Ulid>,
        stream_id: Ulid,
        /// Each node routes its own replica; the sender's backend is ignored.
        resolved: ResolvedBackend,
        keep_alive: bool,
    },
    ServeRead {
        stream_id: Ulid,
        location: BackendLocation,
        expected_blake3: [u8; 32],
    },
    ReceiveRead {
        stream_id: Ulid,
        size: u64,
        expected_blake3: [u8; 32],
    },
    /// Create-time reachability proof for a tenant backend: build the guarded
    /// operator and round-trip a sentinel object.
    CheckGroupBackend {
        record: GroupStorageBackend,
        secret: GroupStorageBackendSecret,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagingSourceEffect {
    Check {
        access: ResolvedSourceAccess,
    },
    Head {
        access: ResolvedSourceAccess,
    },
    List {
        access: ResolvedSourceAccess,
        offset: usize,
        limit: usize,
        recursive: bool,
        files_only: bool,
    },
    Read {
        access: ResolvedSourceAccess,
        range: Option<Range<u64>>,
    },
}

#[derive(Debug, PartialEq)]
pub enum StorageEffect {
    StartTransaction {
        read: bool,
    },
    CommitTransaction {
        txn_id: TxnId,
    },
    Read {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    BatchRead {
        reads: Vec<(KeySpace, Key)>,
        txn_id: Option<TxnId>,
    },
    Write {
        key_space: KeySpace,
        key: Key,
        value: Value,
        txn_id: Option<TxnId>,
    },
    BatchWrite {
        writes: Vec<(KeySpace, Key, Value)>,
        txn_id: Option<TxnId>,
    },
    Delete {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    BatchDelete {
        deletes: Vec<(KeySpace, Key)>,
        txn_id: Option<TxnId>,
    },
    AbortTransaction {
        txn_id: TxnId,
    },
    /// Persist all pending storage data with `SyncAll` durability.
    SyncAll,
    /// Iterate over keys in a keyspace with optional prefix and pagination.
    ///
    /// Iteration order is lexicographic by key bytes.
    /// - `prefix`: restricts results to keys with this prefix
    /// - `start`: lower bound for the first returned key
    /// - `limit`: maximum number of entries to return
    Iter {
        key_space: KeySpace,
        prefix: Option<Key>,
        start: Option<IterStart>,
        limit: usize,
        txn_id: Option<TxnId>,
    },
    /// Read the lexicographically last entry in a keyspace, optionally by prefix.
    Last {
        key_space: KeySpace,
        prefix: Option<Key>,
        txn_id: Option<TxnId>,
    },
}

/// Dispatch lane for a storage effect. Foreground is always served before Bulk,
/// so sync traffic is never starved by background materialization work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StoragePriority {
    #[default]
    Foreground,
    Bulk,
}

/// Lower bound for a [`StorageEffect::Iter`] scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IterStart {
    /// Exclusive cursor: iteration begins at the first key greater than this.
    After(Key),
    /// Inclusive seek: iteration begins at this key if it exists.
    At(Key),
}

impl IterStart {
    pub fn key(&self) -> &Key {
        match self {
            IterStart::After(key) | IterStart::At(key) => key,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NetEffect {
    Dht(DhtEffect),
    DocumentSync(DocumentSyncEffect),
    Stream(StreamEffect),
    JobControl(Box<JobControlEffect>),
    AuditPage(Box<AuditPageEffect>),
    PolicyFetch(Box<PolicyFetchEffect>),
    JobRecord(Box<JobRecordEffect>),
    LaunchOffer(Box<LaunchOfferEffect>),
}

/// Holders one policy fetch may consult. The operation resolves them from its
/// local placement view; a longer list would turn a cache miss into a fan-out.
pub const MAX_POLICY_FETCH_HOLDERS: usize = 8;

/// Holders one job-record publish or fetch may consult.
pub const MAX_JOB_RECORD_HOLDERS: usize = 8;

/// Job-family records one fetch may return, so a family with many executions is
/// always read as bounded pages.
pub const MAX_JOB_RECORD_PAGE: usize = 64;

/// Fetch of one immutable placement-policy document from the holders the
/// operation resolved. The adapter tries them in order and never routes; the
/// operation verifies id, realm, and digest before it caches anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyFetchEffect {
    pub realm_id: RealmId,
    /// At most [`MAX_POLICY_FETCH_HOLDERS`], in preference order.
    pub holders: Vec<NodeId>,
    pub policy_ref: PlacementPolicyRef,
    pub deadline: Duration,
}

/// Replication of the append-only job-family records: publish one immutable
/// record to the family holders, or read a bounded page back from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRecordEffect {
    Publish {
        realm_id: RealmId,
        /// Family placement derived from the submission id, never from an alias.
        placement: PlacementRef,
        /// At most [`MAX_JOB_RECORD_HOLDERS`], in preference order.
        holders: Vec<NodeId>,
        record: JobFamilyRecord,
        deadline: Duration,
    },
    Fetch {
        realm_id: RealmId,
        placement: PlacementRef,
        holders: Vec<NodeId>,
        submission_id: SubmissionId,
        /// `None` reads every request family under this submission.
        request_digest: Option<[u8; 32]>,
        /// Opaque holder cursor returned by the previous page.
        cursor: Option<Vec<u8>>,
        /// Capped at [`MAX_JOB_RECORD_PAGE`] by the responder.
        limit: usize,
    },
}

/// One immutable record of a job family. Each variant is published by exactly
/// one authorized author and is never rewritten under the same key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobFamilyRecord {
    Spec(Box<LogicalJobSpec>),
    Claim(SubmissionClaim),
    Budget(WitnessBudgetRecord),
    Launch(Box<LaunchIntent>),
    Receipt(Box<ExecutionReceipt>),
    Update(ExecutionUpdate),
    Cancel(JobCancelRecord),
}

/// One scheduler's launch offer to an execution target. It carries no caller
/// token: the target fetches and verifies the sealed spec itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchOfferEffect {
    pub realm_id: RealmId,
    pub target: ExecutionTargetId,
    pub launch: LaunchIntent,
    pub deadline: Duration,
}

/// Discrete job-control request to a job's immutable owner. The adapter performs
/// the frame round-trip; the routing operation only emits this.
#[derive(Debug, Clone, PartialEq)]
pub struct JobControlEffect {
    pub owner: NodeId,
    pub request: JobRequest,
}

/// The audit fan-out: one page request asked of every listed node. The adapter
/// performs the frame round-trips concurrently and answers with one aggregated
/// event, so an unreachable node cannot delay the others.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditPageEffect {
    pub nodes: Vec<NodeId>,
    pub request: AuditPageRequest,
}

/// Default wall-clock budget for a DHT read whose caller has no tighter
/// contract of its own.
pub const DHT_GET_DEADLINE: Duration = Duration::from_secs(10);

/// When a DHT read is allowed to stop early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DhtCompletion {
    /// Runs the lookup frontier to exhaustion, keeping multi-publisher reads
    /// whole. This is the only policy valid for realm presence and blob-holder
    /// discovery.
    Exhaustive,
    /// Stops at the first signature-valid, unexpired entry published by
    /// `publisher` in `realm_id`. Only a caller that knows both may use it.
    FirstUsable {
        realm_id: RealmId,
        publisher: NodeId,
    },
}

/// Deadline and completion policy the DHT driver enforces for one read. The
/// driver owns the deadline, so a timed-out or cancelled read releases its
/// operation and pending RPC state instead of running on detached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DhtGetOptions {
    pub deadline: Duration,
    pub completion: DhtCompletion,
    /// Realm whose bounded-stale presence snapshot may answer this read. Every
    /// other read always reaches the DHT.
    pub presence: Option<RealmId>,
}

impl DhtGetOptions {
    pub fn exhaustive(deadline: Duration) -> Self {
        Self {
            deadline,
            completion: DhtCompletion::Exhaustive,
            presence: None,
        }
    }

    pub fn first_usable(deadline: Duration, realm_id: RealmId, publisher: NodeId) -> Self {
        Self {
            deadline,
            completion: DhtCompletion::FirstUsable {
                realm_id,
                publisher,
            },
            presence: None,
        }
    }

    pub fn presence(deadline: Duration, realm_id: RealmId) -> Self {
        Self {
            presence: Some(realm_id),
            ..Self::exhaustive(deadline)
        }
    }
}

impl Default for DhtGetOptions {
    fn default() -> Self {
        Self::exhaustive(DHT_GET_DEADLINE)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DhtEffect {
    Put {
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
    },
    Get {
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
        options: DhtGetOptions,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamEffect {
    Open { node_id: NodeId, alpn: Alpn },
    Close { stream_id: u64 },
}
