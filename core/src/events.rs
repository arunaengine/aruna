use crate::{
    errors::{DhtError, GossipError, StorageError, StreamError},
    id::NodeId,
    types::{DhtKey, Key, TopicId, TxnId, Value},
};

#[derive(Debug)]
pub enum Event {
    Storage(StorageEvent),
    Net(NetEvent),
    Task(),
    Search(),
    Stream(),
}

#[derive(Debug)]
pub enum StorageEvent {
    TransactionStarted { txn_id: TxnId },
    TransactionCommitted { txn_id: TxnId },
    TransactionAborted { txn_id: TxnId },
    ReadResult { key: Key, value: Option<Value> },
    IterResult { values: Vec<(Key, Value)> },
    WriteResult { key: Key },
    DeleteResult { key: Key },
    /// Result of a scan operation, returns all key-value pairs
    ScanResult { entries: Vec<(Key, Value)> },
    Error { error: StorageError },
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
    Subscribed {
        topic: TopicId,
    },
    BroadcastComplete {
        topic: TopicId,
    },
    Message {
        topic: TopicId,
        sender: NodeId,
        data: Vec<u8>,
    },
    Unsubscribed {
        topic: TopicId,
    },
    Error {
        error: GossipError,
    },
}

#[derive(Debug)]
pub enum StreamEvent {
    Opened { stream_id: u64, node_id: NodeId },
    Sent { stream_id: u64, bytes_sent: usize },
    Received { stream_id: u64, data: Vec<u8> },
    Closed { stream_id: u64 },
    OwnershipReady { stream_id: u64 },
    Error { stream_id: u64, error: StreamError },
}

#[derive(Debug)]
pub enum NetError {
    InvalidEffect,
    ChannelClosed,
}
