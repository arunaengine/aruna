pub mod api;
mod handle;
pub mod materialization_queue;
pub mod projector;
mod protocol;
pub mod prune_queue;
mod queue_storage;
pub mod repository;

use std::sync::Arc;

use tracing::warn;

use crate::driver::DriverContext;

pub use handle::{MetadataHandle, MetadataHandleOptions, MetadataSearchStorage};
pub use protocol::{MetadataAuthToken, MetadataAuthTokenError};

/// Primes the metadata caches off the boot path so the first user query
/// finds them warm. Never blocks startup.
pub fn spawn_metadata_warmup(context: Arc<DriverContext>) {
    tokio::spawn(async move {
        if let Some(handle) = context.metadata_handle.clone()
            && let Err(error) = handle.warm_caches().await
        {
            warn!(error = %error, "Metadata visibility cache warmup failed");
        }
    });
}
