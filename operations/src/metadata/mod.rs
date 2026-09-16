pub mod api;
pub mod audit;
pub(crate) mod builtin;
pub mod contact;
pub mod create_document;
pub mod delete_document;
pub mod device_pull;
pub mod forward;
pub mod get_document;
pub(crate) mod handle;
mod iri_index;
pub mod list_documents;
pub mod materialization_queue;
pub mod persistent_id;
pub mod profile;
pub mod projector;
pub(crate) mod protocol;
pub mod prune_queue;
pub mod public_preview;
mod query_cache;
mod queue_storage;
pub mod raw_revision;
pub mod repository;
mod search_cursor;
mod search_enrichment;
pub mod stats;
mod summary_cache;
#[cfg(test)]
mod tests;
pub mod timestamp_index;
pub mod update_document;
pub mod visibility_index;

use std::sync::Arc;

use aruna_core::shutdown::Shutdown;
use tracing::warn;

use crate::driver::DriverContext;

/// The phase-time and identity samples a metadata operation stamps records
/// with. Production keeps the defaults and samples per phase; tests replace the
/// source so fixed inputs produce byte-identical records.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MetadataPhaseSource {
    now_ms: fn() -> u64,
    next_id: fn() -> ulid::Ulid,
}

impl PartialEq for MetadataPhaseSource {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::fn_addr_eq(self.now_ms, other.now_ms)
            && std::ptr::fn_addr_eq(self.next_id, other.next_id)
    }
}

impl Eq for MetadataPhaseSource {}

impl Default for MetadataPhaseSource {
    fn default() -> Self {
        Self {
            now_ms: aruna_core::time::unix_timestamp_millis,
            next_id: ulid::Ulid::generate,
        }
    }
}

impl MetadataPhaseSource {
    /// A source with explicit samplers, for tests that fix a trace.
    #[cfg(test)]
    pub(crate) fn fixed(now_ms: fn() -> u64, next_id: fn() -> ulid::Ulid) -> Self {
        Self { now_ms, next_id }
    }

    /// The current phase's wall clock in milliseconds.
    pub(crate) fn now_ms(self) -> u64 {
        (self.now_ms)()
    }

    /// A fresh identity for one record this phase mints.
    pub(crate) fn next_id(self) -> ulid::Ulid {
        (self.next_id)()
    }
}

pub use contact::{PEER_CONTACT_WINDOW, PeerContacts};
pub use handle::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
pub(crate) use handle::{WritePeerError, transport_message_kind};
pub use protocol::{
    AuthToken, AuthTokenError, MetadataPathWinner, MetadataReadError, PersistentIdResolution,
};

/// Primes the metadata caches off the boot path so the first user query
/// finds them warm. Never blocks startup.
pub fn spawn_metadata_warmup(context: Arc<DriverContext>, shutdown: &Shutdown) {
    timestamp_index::spawn_index_sweep(Arc::clone(&context), shutdown);
    visibility_index::spawn_visibility_index(Arc::clone(&context), shutdown);
    shutdown.spawn(async move {
        let Some(handle) = context.metadata_handle.clone() else {
            return;
        };
        if let Err(error) = handle.warm_caches().await {
            warn!(error = %error, "Metadata visibility cache warmup failed");
            return;
        }
        if let Err(error) = iri_index::rebuild_index(&context).await {
            warn!(error = %error, "Metadata IRI reference index rebuild failed");
        }
    });
}
