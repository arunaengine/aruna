use std::time::Duration;

use crate::alpn::Alpn;
use crate::id::NodeId;
use crate::types::{DhtKey, Key, KeySpace, TopicId, TxnId, Value};

pub enum Effect {
    Storage(StorageEffect),
    Net(NetEffect),
    Task(),
    Search(),
    Stream(),
}

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
    Write {
        key_space: KeySpace,
        key: Key,
        value: Value,
        txn_id: Option<TxnId>,
    },
    Delete {
        key_space: KeySpace,
        key: Key,
        txn_id: Option<TxnId>,
    },
    AbortTransaction {
        txn_id: TxnId,
    },
    /// Iterate over keys in a keyspace with optional prefix and pagination.
    ///
    /// Iteration order is lexicographic by key bytes.
    /// - `prefix`: restricts results to keys with this prefix
    /// - `start_after`: exclusive cursor key
    /// - `limit`: maximum number of entries to return
    Iter {
        key_space: KeySpace,
        prefix: Option<Key>,
        start_after: Option<Key>,
        limit: usize,
        txn_id: Option<TxnId>,
    },
}

#[derive(Debug, Clone)]
pub enum NetEffect {
    Dht(DhtEffect),
    Gossip(GossipEffect),
    Stream(StreamEffect),
}

#[derive(Debug, Clone)]
pub enum DhtEffect {
    Put {
        key: DhtKey,
        value: Vec<u8>,
        ttl: Duration,
    },
    Get {
        key: DhtKey,
    },
}

#[derive(Debug, Clone)]
pub enum GossipEffect {
    Subscribe { topic: TopicId },
    Broadcast { topic: TopicId, message: Vec<u8> },
    Unsubscribe { topic: TopicId },
}

#[derive(Debug, Clone)]
pub enum StreamEffect {
    Open { node_id: NodeId, alpn: Alpn },
    Send { stream_id: u64, data: Vec<u8> },
    Recv { stream_id: u64, max_bytes: usize },
    Close { stream_id: u64 },
    RequestOwned { stream_id: u64 },
}
