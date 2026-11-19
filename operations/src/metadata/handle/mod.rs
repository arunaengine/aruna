//! Owns the metadata handle: its caches, peer state and node keys, beside the graph store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::sync::Arc;
#[cfg(feature = "test-hooks")]
use std::sync::atomic::AtomicBool;

use aruna_net::NetHandle;
use aruna_storage::StorageHandle;
use aruna_storage::key_index::RangeIndexes;

use super::protocol::METADATA_IO_TIMEOUT;

mod effects;
mod forward_serve;
pub(crate) mod frames;
pub use self::effects::{export_rocrate_jsonld, send_metadata_effect};
pub use self::pull_serve::{accept_pull, pull_status};
mod ingress;
mod pull_serve;
mod transport;

const METADATA_CHUNK_SIZE: usize = 64 * 1024;

/// The directory below the metadata storage path that holds the range search indexes.
const RANGE_INDEX_DIR: &str = "range-indexes";

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<MetadataInner>,
}

#[derive(Clone, Debug, Default)]
pub struct MetadataHandleOptions {}

struct MetadataInner {
    net_handle: Option<NetHandle>,
    inbound_frame_bytes: Arc<tokio::sync::Semaphore>,
    sync_pulls: self::pull_serve::PullRegistry,
    #[cfg(feature = "test-hooks")]
    repair_hold: AtomicBool,
}

impl std::fmt::Debug for MetadataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataHandle").finish_non_exhaustive()
    }
}

impl MetadataHandle {
    /// A handle whose range indexes live under `path`.
    pub fn new(
        path: impl AsRef<Path>,
        storage_handle: StorageHandle,
        net_handle: Option<NetHandle>,
        _options: MetadataHandleOptions,
    ) -> Self {
        // Range indexes sit beside the metadata store, so only a node with metadata keeps them.
        storage_handle
            .install_range_indexes(RangeIndexes::new(path.as_ref().join(RANGE_INDEX_DIR)));
        Self {
            inner: Arc::new(MetadataInner {
                net_handle,
                inbound_frame_bytes: Arc::new(tokio::sync::Semaphore::new(
                    super::protocol::INBOUND_FRAME_BYTES,
                )),
                sync_pulls: Default::default(),
                #[cfg(feature = "test-hooks")]
                repair_hold: AtomicBool::new(false),
            }),
        }
    }

    /// Test hook: sync mirror repair on this node waits until `release_repair`.
    #[cfg(feature = "test-hooks")]
    pub fn hold_repair(&self) {
        self.inner
            .repair_hold
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub fn release_repair(&self) {
        self.inner
            .repair_hold
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(feature = "test-hooks")]
    pub(crate) fn repair_held(&self) -> bool {
        self.inner
            .repair_hold
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The device pulls this node is running.
    pub(crate) fn sync_pulls(&self) -> &self::pull_serve::PullRegistry {
        &self.inner.sync_pulls
    }
}

#[cfg(test)]
mod tests;
