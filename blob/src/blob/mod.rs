use aruna_core::effects::BlobEffect;
use aruna_core::events::BlobEvent;
use aruna_core::structs::BackendConfig;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use aruna_storage::storage::StorageHandle;
use bao_tree::BlockSize;
use crossfire::{mpsc, oneshot};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use ulid::Ulid;

mod backend;
mod control_plane;
mod io;
mod replication;
mod runtime;

#[cfg(test)]
mod tests;

pub const BAO_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(16); // 2^16 bytes

pub type EffectHandle = (BlobEffect, oneshot::TxOneshot<BlobEvent>);
pub type EffectSender = crossfire::MAsyncTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::AsyncRx<mpsc::Array<EffectHandle>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlPlaneTimeoutKind {
    Connection,
    Read,
    Write,
}

#[derive(Clone, Debug)]
pub struct BlobHandler {
    backend_config: BackendConfig,
    storage: StorageHandle,
    net: NetHandle,
    connections: Arc<Mutex<HashMap<Ulid, BiStream>>>,
}

#[derive(Clone, Debug)]
pub struct BlobHandle {
    handler: BlobHandler,
    write_channel: EffectSender,
}
