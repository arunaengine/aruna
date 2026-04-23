use crate::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use crate::metadata::MetadataEvent;
use crate::stream::{BackendStream, StreamError as BackendStreamError};
use crate::structs::{
    BackendLocation, RealmId, ReplicationSuboperationResult, ResolvedSourceConnector,
    SourceMetadata,
};
use crate::{
    automerge::AutomergeEvent,
    errors::{AuthorizationError, DhtError, GossipError, StorageError, StreamError},
    id::NodeId,
    task::TaskEvent,
    types::{DhtKey, Key, KeySpace, TopicId, TxnId, Value},
};
use bytes::Bytes;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Event {
    Blob(BlobEvent),
    StagingSource(StagingSourceEvent),
    Storage(StorageEvent),
    Net(NetEvent),
    Automerge(AutomergeEvent),
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
    AutomergeSyncResult {
        result: Result<(), String>,
    },
    TopicAnnouncementResult {
        result: Result<(), String>,
    },
    SourceConnectorResolved {
        result: Result<ResolvedSourceConnector, SourceConnectorResolutionError>,
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
    Error(BlobError),
}

#[derive(Debug, PartialEq)]
pub enum StagingSourceEvent {
    HeadResult {
        metadata: SourceMetadata,
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
    Gossip(GossipEvent),
    Stream(StreamEvent),
    Error(NetError),
}

#[derive(Debug, PartialEq)]
pub enum DhtEvent {
    PutComplete { key: DhtKey },
    GetResult { key: DhtKey, values: Vec<DhtEntry> },
    Error { error: DhtError },
}

#[derive(Debug, Clone, PartialEq)]
pub struct DhtEntry {
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Debug, PartialEq)]
pub enum GossipEvent {
    Subscribed { topic: TopicId },
    BroadcastComplete { topic: TopicId },
    Unsubscribed { topic: TopicId },
    Error { error: GossipError },
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
