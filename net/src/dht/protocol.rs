use std::time::Duration;

use aruna_core::events::DhtEntry;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::RealmId;
use thiserror::Error;

use super::rpc::{DhtRequest, DhtResponse};
use super::storage::StoredEntry;

pub type OpId = u64;
pub type InboundId = u64;

pub const INTERNAL_OP_START: OpId = 1 << 63;
pub const CLEANUP_OP_ID: OpId = 0;

#[derive(Debug, Clone)]
pub enum DhtInput {
    Cmd(DhtCmd),
    Io(DhtIo),
    Tick { now_tick: u64 },
}

#[derive(Debug, Clone)]
pub enum DhtCmd {
    Put {
        op_id: OpId,
        key: DhtKeyId,
        realm_id: RealmId,
        value: Vec<u8>,
        ttl: Duration,
    },
    Get {
        op_id: OpId,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
    },
    Bootstrap {
        op_id: OpId,
        nodes: Vec<NodeId>,
    },
    RoutingTableSize {
        op_id: OpId,
    },
    AddPeer {
        node_id: NodeId,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum DhtEffect {
    IoRequest(Box<DhtIoRequest>),
    Output(DhtOutput),
}

#[derive(Debug, Clone)]
pub enum DhtOutput {
    Completed { op_id: OpId, result: DhtOutputValue },
    Failed { op_id: OpId, error: DhtIoError },
}

#[derive(Debug, Clone)]
pub enum DhtOutputValue {
    Unit,
    GetValues(Vec<DhtEntry>),
    RoutingTableSize(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcPhase {
    PutLookup,
    PutStore,
    GetLookup,
    Bootstrap,
    EvictionPing,
    MaintenancePing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageStage {
    PutLocalRead,
    PutLocalWrite,
    GetLocalRead,
    InboundGetRead,
    InboundPutRead,
    InboundPutWrite,
    CleanupIter,
    CleanupWrite,
    CleanupDelete,
}

#[derive(Debug, Clone)]
pub enum DhtIoRequest {
    RpcRequest {
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        request: DhtRequest,
    },
    RpcResponse {
        inbound_id: InboundId,
        response: DhtResponse,
    },
    DropInbound {
        inbound_id: InboundId,
    },
    StorageRead {
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        realm_filter: Option<RealmId>,
    },
    StorageWrite {
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
        entries: Vec<StoredEntry>,
    },
    StorageDelete {
        op_id: OpId,
        stage: StorageStage,
        key: DhtKeyId,
    },
    StorageIter {
        op_id: OpId,
        stage: StorageStage,
        start_after: Option<Vec<u8>>,
        limit: usize,
    },
}

#[derive(Debug, Clone)]
pub enum DhtIo {
    RpcResponse {
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        response: DhtResponse,
    },
    RpcError {
        op_id: OpId,
        phase: RpcPhase,
        peer: NodeId,
        error: DhtIoError,
    },
    InboundRequest {
        inbound_id: InboundId,
        peer: NodeId,
        request: DhtRequest,
    },
    InboundReadError {
        inbound_id: InboundId,
        error: DhtIoError,
    },
    InboundDropped {
        inbound_id: InboundId,
    },
    StorageReadResult {
        op_id: OpId,
        stage: StorageStage,
        entries: Vec<StoredEntry>,
    },
    StorageWriteResult {
        op_id: OpId,
        stage: StorageStage,
    },
    StorageDeleteResult {
        op_id: OpId,
        stage: StorageStage,
    },
    StorageIterResult {
        op_id: OpId,
        stage: StorageStage,
        values: Vec<(Vec<u8>, Vec<StoredEntry>)>,
        next_start_after: Option<Vec<u8>>,
    },
    StorageError {
        op_id: OpId,
        stage: StorageStage,
        error: DhtIoError,
    },
    PeerSeen {
        peer: NodeId,
    },
}

#[derive(Debug, Clone, Error)]
pub enum DhtIoError {
    #[error("queue full")]
    QueueFull,
    #[error("shutdown")]
    Shutdown,
    #[error("timeout")]
    Timeout,
    #[error("network error: {0}")]
    Network(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

impl DhtIoError {
    pub fn network(error: impl std::fmt::Display) -> Self {
        Self::Network(error.to_string())
    }

    pub fn storage(error: impl std::fmt::Display) -> Self {
        Self::Storage(error.to_string())
    }

    pub fn invalid_response(error: impl std::fmt::Display) -> Self {
        Self::InvalidResponse(error.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for DhtIoError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::Timeout
    }
}

impl From<std::io::Error> for DhtIoError {
    fn from(error: std::io::Error) -> Self {
        Self::network(error)
    }
}

impl From<postcard::Error> for DhtIoError {
    fn from(error: postcard::Error) -> Self {
        Self::invalid_response(error)
    }
}
