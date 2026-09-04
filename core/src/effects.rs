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
    BackendLocation, GroupStorageBackend, GroupStorageBackendSecret, HiddenBlobKey,
    JobRecordEnvelope, JobRecordKind, PlacementPolicyRef, PlacementRef, PolicyPublicationClaim,
    RealmId, ResolvedBackend, ResolvedSourceAccess, SubmissionId, WriteGuard,
};
use crate::task::TaskEffect;
use crate::types::UserId;
use crate::types::{Key, KeySpace, TxnId, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Effect {
    Blob(BlobEffect),
    StagingSource(StagingSourceEffect),
    LocalFile(LocalFileEffect),
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
    /// Serves one version this node never copied into its blob store: a device
    /// streams the owner's own file. The file is refused unless it still
    /// carries `fingerprint` and hashes to `expected_blake3`.
    ServeSourceRead {
        stream_id: Ulid,
        access: ResolvedSourceAccess,
        size: u64,
        expected_blake3: [u8; 32],
        fingerprint: String,
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

/// Writes into a folder the owner bound on their own machine. Every variant is
/// non-destructive by construction: a write only lands through a guard the
/// adapter re-verifies at rename time, a conflicted copy never replaces
/// anything, and a removal moves the file aside instead of unlinking it.
#[derive(Debug)]
pub enum LocalFileEffect {
    Write {
        root: String,
        relative: String,
        guard: WriteGuard,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Adds the incoming bytes beside the file under a free conflicted-copy
    /// name. The file the owner has is never touched.
    WriteConflicted {
        root: String,
        relative: String,
        /// Timestamp the copy is named after, so the name is reproducible.
        at_ms: u64,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    /// Moves one file into the folder's trash directory, but only while it
    /// still carries the bytes the owner decided about.
    MoveAside {
        root: String,
        relative: String,
        guard: WriteGuard,
    },
    /// Weak fingerprint and blake3 of one file, read as one stable observation.
    Hash { root: String, relative: String },
}

impl PartialEq for LocalFileEffect {
    /// Streams carry no identity, so two write effects compare by their target
    /// and their guard alone.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                LocalFileEffect::Write {
                    root,
                    relative,
                    guard,
                    ..
                },
                LocalFileEffect::Write {
                    root: other_root,
                    relative: other_relative,
                    guard: other_guard,
                    ..
                },
            ) => root == other_root && relative == other_relative && guard == other_guard,
            (
                LocalFileEffect::WriteConflicted {
                    root,
                    relative,
                    at_ms,
                    ..
                },
                LocalFileEffect::WriteConflicted {
                    root: other_root,
                    relative: other_relative,
                    at_ms: other_at,
                    ..
                },
            ) => root == other_root && relative == other_relative && at_ms == other_at,
            (
                LocalFileEffect::MoveAside {
                    root,
                    relative,
                    guard,
                },
                LocalFileEffect::MoveAside {
                    root: other_root,
                    relative: other_relative,
                    guard: other_guard,
                },
            ) => root == other_root && relative == other_relative && guard == other_guard,
            (
                LocalFileEffect::Hash { root, relative },
                LocalFileEffect::Hash {
                    root: other_root,
                    relative: other_relative,
                },
            ) => root == other_root && relative == other_relative,
            _ => false,
        }
    }
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
    /// Signs one policy publication claim with this node's key, after the
    /// operation checked the authorizing user's realm-admin permission.
    PolicySign(Box<PolicyPublicationClaim>),
}

/// Holders one policy fetch may consult. The operation resolves them from its
/// local placement view; a longer list would turn a cache miss into a fan-out.
pub const MAX_POLICY_FETCH_HOLDERS: usize = 8;

/// Holders one job-record publish or fetch may consult.
pub const MAX_JOB_RECORD_HOLDERS: usize = 8;

/// Job-family records one fetch may return, so a family with many executions is
/// always read as bounded pages.
pub const MAX_JOB_RECORD_PAGE: usize = 64;

/// Bytes of one opaque page cursor: it carries a holder's record key only.
pub const MAX_JOB_RECORD_CURSOR_BYTES: usize = 128;

/// Encoded bytes of one immutable job-family record.
pub const MAX_JOB_RECORD_BYTES: usize = 1024 * 1024;

/// Encoded bytes of one fetched record page.
pub const MAX_JOB_RECORD_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// Why a bounded policy-fetch or job-record frame was refused. Every bound holds
/// at construction and again at decode, because a peer supplies bytes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FrameBoundsError {
    #[error("holder list must name 1..={max} nodes")]
    HolderCount { max: usize },
    #[error("cursor must be at most {MAX_JOB_RECORD_CURSOR_BYTES} bytes")]
    CursorBytes,
    #[error("page limit must be 1..={MAX_JOB_RECORD_PAGE}")]
    PageLimit,
    #[error("page must carry at most {MAX_JOB_RECORD_PAGE} records")]
    RecordCount,
    #[error("record must encode to at most {MAX_JOB_RECORD_BYTES} bytes")]
    RecordBytes,
    #[error("frame must carry the one record kind it is defined for")]
    RecordKind,
    #[error("page must encode to at most {MAX_JOB_RECORD_PAGE_BYTES} bytes")]
    PageBytes,
    #[error(transparent)]
    Encoding(#[from] postcard::Error),
}

/// Postcard size of a value without materializing its encoding.
pub(crate) fn encoded_len<T>(value: &T) -> Result<usize, FrameBoundsError>
where
    T: Serialize + ?Sized,
{
    Ok(postcard::experimental::serialized_size(value)?)
}

/// Holders resolved from the local placement view, in preference order. The
/// bound is part of the type, so no caller can widen one request into a fan-out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderList<const MAX: usize>(Vec<NodeId>);

impl<const MAX: usize> HolderList<MAX> {
    pub fn new(holders: Vec<NodeId>) -> Result<Self, FrameBoundsError> {
        if holders.is_empty() || holders.len() > MAX {
            return Err(FrameBoundsError::HolderCount { max: MAX });
        }
        Ok(Self(holders))
    }

    pub fn as_slice(&self) -> &[NodeId] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<NodeId> {
        self.0
    }
}

/// Opaque page cursor a holder minted. Decoding rejects an oversized cursor, so
/// a peer cannot return a marker the next request would have to carry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>")]
pub struct FetchCursor(Vec<u8>);

impl FetchCursor {
    pub fn new(cursor: Vec<u8>) -> Result<Self, FrameBoundsError> {
        if cursor.is_empty() || cursor.len() > MAX_JOB_RECORD_CURSOR_BYTES {
            return Err(FrameBoundsError::CursorBytes);
        }
        Ok(Self(cursor))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for FetchCursor {
    type Error = FrameBoundsError;

    fn try_from(cursor: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(cursor)
    }
}

/// Requested page size. A requester-supplied value is clamped to the documented
/// maximum; a decoded value outside the range is a malformed frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "usize")]
pub struct PageLimit(usize);

impl PageLimit {
    pub fn new(limit: usize) -> Self {
        Self(limit.clamp(1, MAX_JOB_RECORD_PAGE))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl Default for PageLimit {
    fn default() -> Self {
        Self(MAX_JOB_RECORD_PAGE)
    }
}

impl TryFrom<usize> for PageLimit {
    type Error = FrameBoundsError;

    fn try_from(limit: usize) -> Result<Self, Self::Error> {
        if limit == 0 || limit > MAX_JOB_RECORD_PAGE {
            return Err(FrameBoundsError::PageLimit);
        }
        Ok(Self(limit))
    }
}

/// One authenticated job-family record bounded by its encoded size, so neither a
/// publisher nor a fetched page can carry an unbounded record. The envelope is
/// carried verbatim: bounding a frame never rewrites what its publisher signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct JobRecordFrame(JobRecordEnvelope);

impl JobRecordFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if encoded_len(&envelope)? > MAX_JOB_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for JobRecordFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// One bounded launch offer. Decoding rejects an oversized frame and any record
/// that is not a launch, so a peer's offer is refused before the target does
/// any admission work on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct LaunchFrame(JobRecordEnvelope);

impl LaunchFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if envelope.kind() != JobRecordKind::Launch {
            return Err(FrameBoundsError::RecordKind);
        }
        if encoded_len(&envelope)? > MAX_JOB_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for LaunchFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// One bounded execution receipt. A target's acceptance travels in its own
/// signed envelope, so the reply to a launch offer is bounded and kind-checked
/// before the scheduler does any work on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "JobRecordEnvelope")]
pub struct ReceiptFrame(JobRecordEnvelope);

impl ReceiptFrame {
    pub fn new(envelope: JobRecordEnvelope) -> Result<Self, FrameBoundsError> {
        if envelope.kind() != JobRecordKind::Receipt {
            return Err(FrameBoundsError::RecordKind);
        }
        if encoded_len(&envelope)? > MAX_JOB_RECORD_BYTES {
            return Err(FrameBoundsError::RecordBytes);
        }
        Ok(Self(envelope))
    }

    pub fn envelope(&self) -> &JobRecordEnvelope {
        &self.0
    }

    pub fn into_inner(self) -> JobRecordEnvelope {
        self.0
    }
}

impl TryFrom<JobRecordEnvelope> for ReceiptFrame {
    type Error = FrameBoundsError;

    fn try_from(envelope: JobRecordEnvelope) -> Result<Self, Self::Error> {
        Self::new(envelope)
    }
}

/// Fetch of one immutable placement-policy document from the holders the
/// operation resolved. The adapter tries them in order and never routes; the
/// operation verifies id, realm, and digest before it caches anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyFetchEffect {
    pub realm_id: RealmId,
    pub holders: HolderList<MAX_POLICY_FETCH_HOLDERS>,
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
        holders: HolderList<MAX_JOB_RECORD_HOLDERS>,
        record: Box<JobRecordFrame>,
        deadline: Duration,
    },
    Fetch {
        realm_id: RealmId,
        placement: PlacementRef,
        holders: HolderList<MAX_JOB_RECORD_HOLDERS>,
        submission_id: SubmissionId,
        /// `None` reads every request family under this submission.
        request_digest: Option<[u8; 32]>,
        /// Opaque holder cursor returned by the previous page.
        cursor: Option<FetchCursor>,
        limit: PageLimit,
        deadline: Duration,
    },
}

/// One scheduler's launch offer to an execution target. It carries no caller
/// token: the target verifies the signed launch and fetches the stored spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchOfferEffect {
    pub realm_id: RealmId,
    pub target: ExecutionTargetId,
    pub launch: Box<LaunchFrame>,
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

/// Test fixture shared with the event frames: one signed output record whose
/// encoded size grows with the output count and key width.
#[cfg(test)]
pub(crate) fn sized_envelope(objects: usize, key_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{
        ExecutionOutputRecord, JobFamilyRecord, JobId, OutputObject, OutputSet, SubmissionId,
    };

    let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
    let execution_id = Ulid::from_bytes([7u8; 16]);
    let outputs = (0..objects)
        .map(|index| OutputObject {
            node_id: secret.public(),
            bucket: "bucket".to_string(),
            key: format!("{index:08}-{}", "k".repeat(key_bytes)),
            version_id: Ulid::from_bytes([9u8; 16]),
            execution_id,
            container_path: "/out".to_string(),
            size: 1,
            digest: None,
        })
        .collect();
    let record = JobFamilyRecord::Output(Box::new(ExecutionOutputRecord {
        execution_id,
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        executor_node_id: secret.public(),
        spec_digest: [3u8; 32],
        receipt_digest: [4u8; 32],
        outputs: OutputSet::canonical(outputs).expect("canonical outputs"),
        committed_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}

/// One signed launch whose encoded size grows with the executor-kind width.
#[cfg(test)]
fn sized_launch(kind_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{JobFamilyRecord, JobId, LaunchIntent, PlacementRef, SubmissionId};

    let secret = iroh::SecretKey::from_bytes(&[4u8; 32]);
    let record = JobFamilyRecord::Launch(Box::new(LaunchIntent {
        launch_id: Ulid::from_bytes([8u8; 16]),
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        scheduler_node_id: secret.public(),
        scheduler_seq: 0,
        witness_placement: PlacementRef {
            strategy_id: Ulid::from_bytes([9u8; 16]),
            shard: 2,
        },
        holder_generation: 3,
        target: ExecutionTargetId {
            node_id: iroh::SecretKey::from_bytes(&[5u8; 32]).public(),
            executor_kind: "d".repeat(kind_bytes),
        },
        inputs: Vec::new(),
        output_policies: Vec::new(),
        plan_digest: [6u8; 32],
        spec_digest: [7u8; 32],
        created_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}

/// One signed receipt whose encoded size grows with the executor-kind width.
#[cfg(test)]
pub(crate) fn sized_receipt(kind_bytes: usize) -> JobRecordEnvelope {
    use crate::structs::{ExecutionReceipt, JobFamilyRecord, JobId, SubmissionId};

    let secret = iroh::SecretKey::from_bytes(&[6u8; 32]);
    let record = JobFamilyRecord::Receipt(Box::new(ExecutionReceipt {
        execution_id: Ulid::from_bytes([8u8; 16]),
        launch_id: Ulid::from_bytes([9u8; 16]),
        launch_digest: [1u8; 32],
        submission_id: SubmissionId([1u8; 32]),
        request_digest: [2u8; 32],
        job_id: JobId::from_bytes([5u8; 16]),
        executor_node_id: secret.public(),
        target: ExecutionTargetId {
            node_id: secret.public(),
            executor_kind: "d".repeat(kind_bytes),
        },
        spec_digest: [7u8; 32],
        membership_generation: 4,
        subject_generation: 5,
        subject_digest: [8u8; 32],
        accepted_at_ms: 1,
    }));
    JobRecordEnvelope::sign(RealmId([6u8; 32]), record, &secret).expect("record signs")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn bounds_receipt_reply() {
        // A launch reply is bounded and kind-checked before the scheduler acts.
        assert_eq!(
            ReceiptFrame::new(sized_receipt(MAX_JOB_RECORD_BYTES)),
            Err(FrameBoundsError::RecordBytes)
        );
        assert_eq!(
            ReceiptFrame::new(sized_launch(8)),
            Err(FrameBoundsError::RecordKind)
        );
        let accepted = sized_receipt(8);
        let frame = ReceiptFrame::new(accepted.clone()).expect("bounded receipt");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: ReceiptFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &accepted);
        assert!(relayed.envelope().verify_signature().is_ok());

        let launch = postcard::to_allocvec(&sized_launch(8)).expect("record encodes");
        assert!(postcard::from_bytes::<ReceiptFrame>(&launch).is_err());
    }

    #[test]
    fn rejects_wide_holders() {
        let holders: Vec<NodeId> = (0..=MAX_JOB_RECORD_HOLDERS as u8).map(node).collect();
        assert_eq!(
            HolderList::<MAX_JOB_RECORD_HOLDERS>::new(holders),
            Err(FrameBoundsError::HolderCount {
                max: MAX_JOB_RECORD_HOLDERS
            })
        );
        assert_eq!(
            HolderList::<MAX_POLICY_FETCH_HOLDERS>::new(Vec::new()),
            Err(FrameBoundsError::HolderCount {
                max: MAX_POLICY_FETCH_HOLDERS
            })
        );
        let holders = vec![node(1), node(2)];
        assert_eq!(
            HolderList::<MAX_POLICY_FETCH_HOLDERS>::new(holders.clone())
                .expect("bounded holders")
                .as_slice(),
            holders.as_slice()
        );
    }

    #[test]
    fn rejects_long_cursor() {
        let cursor = vec![9u8; MAX_JOB_RECORD_CURSOR_BYTES + 1];
        assert_eq!(
            FetchCursor::new(cursor.clone()),
            Err(FrameBoundsError::CursorBytes)
        );
        let encoded = postcard::to_allocvec(&cursor).expect("cursor bytes encode");
        assert!(postcard::from_bytes::<FetchCursor>(&encoded).is_err());
    }

    #[test]
    fn cursor_round_trips() {
        let cursor = FetchCursor::new(vec![3u8; 32]).expect("bounded cursor");
        let encoded = postcard::to_allocvec(&cursor).expect("cursor encodes");
        assert_eq!(postcard::from_bytes::<FetchCursor>(&encoded), Ok(cursor));
    }

    #[test]
    fn clamps_page_limit() {
        assert_eq!(PageLimit::new(usize::MAX).get(), MAX_JOB_RECORD_PAGE);
        assert_eq!(PageLimit::new(0).get(), 1);
        assert_eq!(PageLimit::new(8).get(), 8);
        assert_eq!(PageLimit::default().get(), MAX_JOB_RECORD_PAGE);
    }

    #[test]
    fn rejects_decoded_limit() {
        // A requester clamps its own limit; a peer's frame is refused instead.
        let over = postcard::to_allocvec(&(MAX_JOB_RECORD_PAGE + 1)).expect("limit encodes");
        assert!(postcard::from_bytes::<PageLimit>(&over).is_err());
        let zero = postcard::to_allocvec(&0usize).expect("limit encodes");
        assert!(postcard::from_bytes::<PageLimit>(&zero).is_err());
        let valid = postcard::to_allocvec(&MAX_JOB_RECORD_PAGE).expect("limit encodes");
        assert_eq!(
            postcard::from_bytes::<PageLimit>(&valid),
            Ok(PageLimit::new(MAX_JOB_RECORD_PAGE))
        );
    }

    #[test]
    fn rejects_large_record() {
        let envelope = sized_envelope(1024, 1200);
        assert_eq!(
            JobRecordFrame::new(envelope.clone()),
            Err(FrameBoundsError::RecordBytes)
        );
        let encoded = postcard::to_allocvec(&envelope).expect("record encodes");
        assert!(postcard::from_bytes::<JobRecordFrame>(&encoded).is_err());
        assert!(JobRecordFrame::new(sized_envelope(2, 8)).is_ok());
    }

    #[test]
    fn bounds_launch_offer() {
        // An offer is refused for size or kind before any admission work runs.
        assert_eq!(
            LaunchFrame::new(sized_launch(MAX_JOB_RECORD_BYTES)),
            Err(FrameBoundsError::RecordBytes)
        );
        assert_eq!(
            LaunchFrame::new(sized_envelope(1, 8)),
            Err(FrameBoundsError::RecordKind)
        );
        let offered = sized_launch(8);
        let frame = LaunchFrame::new(offered.clone()).expect("bounded launch");
        assert_eq!(frame.envelope(), &offered);

        let encoded = postcard::to_allocvec(&sized_envelope(1, 8)).expect("record encodes");
        assert!(postcard::from_bytes::<LaunchFrame>(&encoded).is_err());
        let oversized =
            postcard::to_allocvec(&sized_launch(MAX_JOB_RECORD_BYTES)).expect("record encodes");
        assert!(postcard::from_bytes::<LaunchFrame>(&oversized).is_err());
    }

    #[test]
    fn launch_keeps_publisher() {
        // Bounding an offer never rewrites what its scheduler signed.
        let offered = sized_launch(8);
        let frame = LaunchFrame::new(offered.clone()).expect("bounded launch");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: LaunchFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &offered);
        assert!(relayed.envelope().verify_signature().is_ok());
    }

    #[test]
    fn frame_keeps_publisher() {
        // A frame is a size bound, never a rewrite of what the publisher signed.
        let envelope = sized_envelope(2, 8);
        let frame = JobRecordFrame::new(envelope.clone()).expect("bounded record");
        let bytes = postcard::to_allocvec(&frame).expect("frame encodes");
        let relayed: JobRecordFrame = postcard::from_bytes(&bytes).expect("frame decodes");
        assert_eq!(relayed.envelope(), &envelope);
        assert!(relayed.envelope().verify_signature().is_ok());
    }
}
