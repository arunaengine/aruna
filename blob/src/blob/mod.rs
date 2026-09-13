//! The blob control plane: repository and group handles, staging writers,
//! replication, and read streams over the configured backends.
use crate::egress::EgressGuard;
use aruna_core::NodeId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use bao_tree::BlockSize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
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
    reservation_active: Arc<StdMutex<HashSet<Ulid>>>,
    /// Shutdown close for the blob write path, mirroring the storage close so a
    /// mutation cannot land on a backend behind the final storage sync.
    closed: Arc<AtomicBool>,
    /// Serializes closing against the mutating dispatch: `close_writes` locks it,
    /// each mutation read-locks across the close check and registering itself.
    close_lock: Arc<StdRwLock<()>>,
    rejected_writes: Arc<AtomicU64>,
    writes_in_flight: Arc<AtomicUsize>,
    writes_drained: Arc<Notify>,
    /// Stops the backend-status monitor at shutdown so its handler clone, and
    /// with it the storage handle, is released before storage closes.
    monitor_cancel: CancellationToken,
    monitor_task: Arc<StdMutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Clone, Debug)]
pub struct BlobHandle {
    handler: BlobHandler,
}
