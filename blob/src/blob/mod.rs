use crate::egress::EgressGuard;
use aruna_core::NodeId;
use aruna_core::structs::BackendConfig;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use bao_tree::BlockSize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{Mutex, RwLock, Semaphore};
use ulid::Ulid;

mod backend;
mod control_plane;
mod io;
mod replication;
mod runtime;
mod source;

#[cfg(test)]
mod tests;

pub const BAO_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); // 2^16 bytes

type SharedBiStream = Arc<Mutex<BiStream>>;

#[derive(Clone, Debug)]
struct Connection {
    peer: NodeId,
    stream: SharedBiStream,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlPlaneTimeoutKind {
    Connection,
    Read,
    Write,
}

#[derive(Clone, Debug)]
pub struct BlobHandler {
    backend_config: BackendConfig,
    egress: EgressGuard,
    storage: StorageHandle,
    net: NetHandle,
    connections: Arc<Mutex<HashMap<Ulid, Connection>>>,
    operator_status: Arc<RwLock<aruna_core::structs::Status>>,
    transfer_slots: Arc<Semaphore>,
    read_slots: Arc<Semaphore>,
    spool_slots: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
pub struct BlobHandle {
    handler: BlobHandler,
}
