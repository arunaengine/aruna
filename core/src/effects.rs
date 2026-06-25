use std::time::Duration;

use crate::alpn::Alpn;
use crate::document::DocumentSyncEffect;
use crate::id::NodeId;
use crate::metadata::MetadataEffect;
use crate::operation::SubOperation;
use crate::stream::{BackendStream, StreamError};
use crate::structs::{BackendLocation, RealmId, ResolvedSourceAccess};
use crate::task::TaskEffect;
use crate::types::UserId;
use crate::types::{DhtKey, Key, KeySpace, TxnId, Value};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagingSourceEffect {
    Head {
        access: ResolvedSourceAccess,
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
pub enum StreamEffect {
    Open { node_id: NodeId, alpn: Alpn },
    Close { stream_id: u64 },
}
