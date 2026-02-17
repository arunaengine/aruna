use std::time::Duration;

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId};
use thiserror::Error;

use super::rpc::{DhtRequest, DhtResponse};
use super::storage::StoredEntry;

pub type CallerId = u64;
pub type OpId = u64;
pub type RequestId = u64;
pub type StorageId = u64;
pub type InboundId = u64;

#[derive(Debug, Clone)]
pub enum DhtInput {
    Cmd(DhtCmd),
    Io(DhtIo),
    Tick { now_tick: u64 },
    ShutdownRequested,
    ContinueOp { op_id: OpId },
}

#[derive(Debug, Clone)]
pub enum DhtCmd {
    Put {
        key: DhtKeyId,
        value: Vec<u8>,
        ttl: Duration,
        caller_id: Option<CallerId>,
    },
    Get {
        key: DhtKeyId,
        caller_id: Option<CallerId>,
    },
    Bootstrap {
        nodes: Vec<NodeId>,
        caller_id: Option<CallerId>,
    },
    RoutingTableSize {
        caller_id: Option<CallerId>,
    },
    AddPeer {
        node_id: NodeId,
    },
    Shutdown {
        caller_id: Option<CallerId>,
    },
}

#[derive(Debug, Clone)]
pub enum DhtEffect {
    IoRequest(DhtIoRequest),
    Output(DhtOutput),
}

#[derive(Debug, Clone)]
pub enum DhtOutput {
    Completed {
        caller_id: CallerId,
        result: DhtOutputValue,
    },
    Failed {
        caller_id: CallerId,
        error: DhtIoError,
    },
    ShutdownComplete,
}

#[derive(Debug, Clone)]
pub enum DhtOutputValue {
    Unit,
    GetValues(Vec<DhtEntry>),
    RoutingTableSize(usize),
}

#[derive(Debug, Clone)]
pub enum DhtIoRequest {
    RpcRequest {
        request_id: RequestId,
        peer: NodeId,
        request: DhtRequest,
        deadline_tick: u64,
    },
    RpcResponse {
        inbound_id: InboundId,
        response: DhtResponse,
    },
    DropInbound {
        inbound_id: InboundId,
    },
    StorageRead {
        storage_id: StorageId,
        key: DhtKeyId,
    },
    StorageWrite {
        storage_id: StorageId,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
    },
    StorageDelete {
        storage_id: StorageId,
        key: DhtKeyId,
    },
    StorageIter {
        storage_id: StorageId,
        start_after: Option<Vec<u8>>,
        limit: usize,
    },
}

#[derive(Debug, Clone)]
pub enum DhtIo {
    RpcResponse {
        request_id: RequestId,
        peer: NodeId,
        response: DhtResponse,
    },
    RpcError {
        request_id: RequestId,
        peer: NodeId,
        error: DhtIoError,
    },
    InboundRequest {
        inbound_id: InboundId,
        peer: NodeId,
        request: DhtRequest,
    },
    InboundDropped {
        inbound_id: InboundId,
    },
    StorageReadResult {
        storage_id: StorageId,
        entries: Vec<StoredEntry>,
    },
    StorageWriteResult {
        storage_id: StorageId,
    },
    StorageDeleteResult {
        storage_id: StorageId,
    },
    StorageIterResult {
        storage_id: StorageId,
        values: Vec<(Vec<u8>, Vec<StoredEntry>)>,
        next_start_after: Option<Vec<u8>>,
    },
    StorageError {
        storage_id: StorageId,
        error: DhtIoError,
    },
    PeerSeen {
        peer: NodeId,
    },
    DispatcherClosed,
}

#[derive(Debug, Clone, Error)]
pub enum DhtIoError {
    #[error("queue full")]
    QueueFull,
    #[error("shutdown")]
    Shutdown,
    #[error("timeout")]
    Timeout,
    #[error("dispatcher closed")]
    DispatcherClosed,
    #[error("network error: {0}")]
    Network(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("internal error: {0}")]
    Internal(String),
}
