use crate::audit::AuditPageBatch;
use crate::effects::{
    FetchCursor, FrameBoundsError, JobRecordFrame, MAX_JOB_RECORD_PAGE, MAX_JOB_RECORD_PAGE_BYTES,
    ReceiptFrame, encoded_len,
};
use crate::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use crate::metadata::MetadataEvent;
use crate::stream::{BackendStream, StreamError as BackendStreamError};
use crate::structs::{
    BackendLocation, GroupRoutingInputs, HiddenBlobEntry, MAX_POLICY_REF_INPUT, PlacementDecision,
    PlacementPolicyDocument, PolicyPublication, RealmId, ReplicationSuboperationResult,
    ResolvedSourceAccess, ResolvedSourceConnector, SourceEntry, SourceMetadata,
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
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, PartialEq)]
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
    TokenRevoked {
        result: Result<(), String>,
    },
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
    PolicySign(PolicySignEvent),
    Error(NetError),
}

/// Reply to a [`crate::effects::NetEffect::PolicySign`]. The adapter signs only
/// a claim naming this node, so no operation can mint foreign provenance.
#[derive(Debug, PartialEq)]
pub enum PolicySignEvent {
    Signed(Box<PolicyPublication>),
    Unavailable(String),
}

/// Reply to a [`crate::effects::PolicyFetchEffect`]. A fetched document is a
/// candidate only; the operation verifies its definition and publication
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

/// Bounded page of immutable job-family records, oldest key first. Its bounds
/// hold for a decoded page too, so a holder cannot answer with an unbounded one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<JobRecordFrame>")]
pub struct JobRecordPage(Vec<JobRecordFrame>);

impl JobRecordPage {
    pub fn new(records: Vec<JobRecordFrame>) -> Result<Self, FrameBoundsError> {
        if records.len() > MAX_JOB_RECORD_PAGE {
            return Err(FrameBoundsError::RecordCount);
        }
        if encoded_len(&records)? > MAX_JOB_RECORD_PAGE_BYTES {
            return Err(FrameBoundsError::PageBytes);
        }
        Ok(Self(records))
    }

    /// Rejects an oversized frame before it is decoded, so a peer cannot force
    /// the allocation of a page it is not allowed to send.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FrameBoundsError> {
        if bytes.len() > MAX_JOB_RECORD_PAGE_BYTES {
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

/// Reply to a [`crate::effects::JobRecordEffect`]. Each holder is authenticated
/// by the transport peer; records keep their own publisher inside the envelope,
/// so relaying a record never makes a holder its author.
#[derive(Debug, PartialEq)]
pub enum JobRecordEvent {
    /// One current holder durably accepted the immutable record.
    Published {
        holder: NodeId,
    },
    /// Several current holders durably accepted the immutable record in one
    /// fan-out. The publisher keeps any holders absent from this list queued.
    PublishedMany {
        holders: Vec<NodeId>,
    },
    Rejected {
        holder: NodeId,
        reason: JobRecordRejection,
    },
    /// At most the requested page of records, oldest key first.
    Fetched {
        holder: NodeId,
        records: JobRecordPage,
        next_cursor: Option<FetchCursor>,
    },
    Unavailable(String),
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

/// Reply to a [`crate::effects::LaunchOfferEffect`]. The receipt arrives in the
/// target's own signed envelope, so the scheduler replicates it with its author.
#[derive(Debug, PartialEq)]
pub enum LaunchOfferEvent {
    Accepted {
        target: NodeId,
        /// Bounded and kind-checked: an acceptance is refused before the
        /// scheduler replicates it, exactly like the offer it answers.
        receipt: Box<ReceiptFrame>,
    },
    Declined {
        target: NodeId,
        reason: LaunchDecline,
    },
    /// The target was unreachable; it may still have accepted the launch.
    Unavailable(String),
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
        match listed <= MAX_POLICY_REF_INPUT {
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
    /// Realm or group authorization denied the sealed submitter.
    Unauthorized,
    /// Placement evaluation blocked the execution subject; never `Allowed`.
    Policy(DeclinedPolicy),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::sized_envelope;

    fn frame(objects: usize, key_bytes: usize) -> JobRecordFrame {
        JobRecordFrame::new(sized_envelope(objects, key_bytes)).expect("bounded record")
    }

    #[test]
    fn rejects_wide_page() {
        let records = vec![frame(1, 8); MAX_JOB_RECORD_PAGE + 1];
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
        let frame = vec![0u8; MAX_JOB_RECORD_PAGE_BYTES + 1];
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
