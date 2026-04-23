use std::time::Duration;

use crate::alpn::Alpn;
use crate::automerge::AutomergeEffect;
use crate::id::NodeId;
use crate::metadata::MetadataEffect;
use crate::operation::SubOperation;
use crate::stream::{BackendStream, StreamError};
use crate::structs::{BackendLocation, RealmId};
use crate::task::TaskEffect;
use crate::types::UserId;
use crate::types::{DhtKey, Key, KeySpace, TopicId, TxnId, Value};
use bytes::Bytes;
use std::ops::Range;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub enum Effect {
    Blob(BlobEffect),
    Storage(StorageEffect),
    Net(NetEffect),
    Automerge(AutomergeEffect),
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
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    WritePart {
        upload_id: Ulid,
        part_number: u16,
        created_by: UserId,
        compressed: bool,
        encrypted: bool,
        blob: BackendStream<Result<Bytes, StreamError>>,
    },
    Compose {
        bucket: String,
        key: String,
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
        keep_alive: bool,
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

#[derive(Debug, Clone, PartialEq)]
pub enum NetEffect {
    Dht(DhtEffect),
    Gossip(GossipEffect),
    Stream(StreamEffect),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DhtEffect {
    Put {
        key: DhtKey,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
    },
    Get {
        key: DhtKey,
        realm_filter: Option<RealmId>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum GossipEffect {
    Subscribe { topic: TopicId },
    Broadcast { topic: TopicId, message: Vec<u8> },
    Unsubscribe { topic: TopicId },
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamEffect {
    Open { node_id: NodeId, alpn: Alpn },
    Close { stream_id: u64 },
}
