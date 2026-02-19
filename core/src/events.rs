use crate::errors::BlobError;
use crate::stream::BackendStream;
use crate::stream::BoxError;
use crate::structs::BackendLocation;
use crate::{
    errors::{DhtError, GossipError, StorageError, StreamError},
    id::NodeId,
    types::{DhtKey, Key, TopicId, TxnId, Value},
};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
pub enum Event {
    Blob(BlobEvent),
    Storage(StorageEvent),
    Net(NetEvent),
    SubOperation(SubOperationEvent),
    Task(),
    Search(),
    Stream(),
}

#[derive(Debug)]
pub enum SubOperationEvent {
    DepthLimitExceeded { max_depth: usize },
}

#[derive(Debug)]
pub enum BlobEvent {
    //TicketReceived { ticket: String, },
    //OperatorCreated { operator: Operator, },
    WriteFinished {
        location: BackendLocation,
        bytes_written: u64,
        hashes: HashMap<String, Vec<u8>>,
        //checksums: HashMap<String, String>,
    },
    ReadFinished {
        blob: BackendStream<Result<Bytes, BoxError>>,
        stream_size: u64,
    },
    DeleteFinished,
    Error(BlobError),
}

#[derive(Debug)]
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
    DeleteResult {
        key: Key,
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

#[derive(Debug)]
pub enum NetEvent {
    Dht(DhtEvent),
    Gossip(GossipEvent),
    Stream(StreamEvent),
    Error(NetError),
}

#[derive(Debug)]
pub enum DhtEvent {
    PutComplete { key: DhtKey },
    GetResult { key: DhtKey, values: Vec<DhtEntry> },
    Error { error: DhtError },
}

#[derive(Debug, Clone)]
pub struct DhtEntry {
    pub node_id: NodeId,
    pub value: Vec<u8>,
    pub expires_at: u64,
}

#[derive(Debug)]
pub enum GossipEvent {
    Subscribed { topic: TopicId },
    BroadcastComplete { topic: TopicId },
    Unsubscribed { topic: TopicId },
    Error { error: GossipError },
}

#[derive(Debug)]
pub enum StreamEvent {
    Opened { stream_id: u64, node_id: NodeId },
    Closed { stream_id: u64 },
    Error { stream_id: u64, error: StreamError },
}

#[derive(Debug)]
pub enum NetError {
    InvalidEffect,
    ChannelClosed,
}
