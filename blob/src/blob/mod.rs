use crate::egress::EgressGuard;
use aruna_core::NodeId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use bao_tree::BlockSize;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, Semaphore};
use ulid::Ulid;

mod backend;
mod control_plane;
mod group;
mod io;
mod registry;
mod replication;
mod runtime;
mod source;

pub use group::{BackendClaim, GroupHold};
pub use registry::{BackendRegistry, NodeBackend};

#[cfg(test)]
mod tests;

pub const BAO_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); // 2^16 bytes

type SharedBiStream = Arc<Mutex<BiStream>>;

pub(super) const CONNECTION_SLOTS: usize = 256;
pub(super) const PEER_CONNECTIONS: usize = 32;

#[derive(Debug)]
struct Connection {
    peer: NodeId,
    stream: SharedBiStream,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlPlaneTimeoutKind {
    Connection,
    Read,
    Write,
}

#[derive(Clone, Debug)]
pub struct BlobHandler {
    registry: BackendRegistry,
    egress: EgressGuard,
    storage: StorageHandle,
    net: NetHandle,
    connections: Arc<Mutex<HashMap<Ulid, Connection>>>,
    connection_slots: Arc<Semaphore>,
    transfer_slots: Arc<Semaphore>,
    read_slots: Arc<Semaphore>,
    spool_slots: Arc<Semaphore>,
    inflight: Arc<AtomicUsize>,
    /// Holds and removal claims on tenant backends. Erasing credentials an
    /// operation still holds would leave that work unable to roll back.
    group_effects: Arc<StdMutex<HashMap<Ulid, group::GroupBackendUse>>>,
}

#[derive(Clone, Debug)]
pub struct BlobHandle {
    handler: BlobHandler,
}
