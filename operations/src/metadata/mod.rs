pub mod api;
pub mod audit;
pub mod forward;
mod handle;
mod iri_index;
pub mod materialization_queue;
pub mod profile_validation;
pub mod projector;
pub(crate) mod protocol;
pub mod prune_queue;
mod query_cache;
mod queue_storage;
pub mod raw;
pub mod repository;
mod search_cursor;
mod search_enrichment;
pub mod stats;
mod summary_cache;
pub mod timestamp_index;
pub mod visibility_index;

use std::sync::Arc;

use aruna_core::shutdown::Shutdown;
use tracing::{info, warn};

use crate::driver::DriverContext;

pub use handle::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
pub(crate) use handle::{MetadataWritePeerError, transport_message_kind};
pub use protocol::{
    MetadataAuthToken, MetadataAuthTokenError, MetadataPathWinner, PersistentIdResolution,
};

/// Primes the metadata caches off the boot path so the first user query
/// finds them warm. Never blocks startup.
pub fn spawn_metadata_warmup(context: Arc<DriverContext>, shutdown: &Shutdown) {
    timestamp_index::spawn_index_sweep(Arc::clone(&context), shutdown);
    visibility_index::spawn_visibility_index(Arc::clone(&context), shutdown);
    let token = shutdown.token();
    shutdown.spawn(async move {
        let Some(handle) = context.metadata_handle.clone() else {
            return;
        };
        if let Err(error) = handle.warm_caches().await {
            warn!(error = %error, "Metadata visibility cache warmup failed");
            return;
        }
        if let Err(error) = iri_index::rebuild_metadata_iri_reference_index(&context).await {
            warn!(error = %error, "Metadata IRI reference index rebuild failed");
        }
        let Some(realm_id) = context
            .net_handle
            .as_ref()
            .map(|net_handle| *net_handle.realm_id())
        else {
            return;
        };
        loop {
            match crate::persistent_id::backfill_persistent_ids(&context, realm_id).await {
                Ok(result) if result.assigned > 0 => info!(
                    assigned = result.assigned,
                    pending = result.pending,
                    "Reconciled automatic persistent identifiers"
                ),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Persistent identifier backfill failed"),
            }
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            }
        }
    });
}
