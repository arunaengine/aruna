//! Starts the metadata index sweeps and primes the caches off the boot path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use crate::driver::DriverContext;

/// Primes the metadata caches off the boot path so the first user query
/// finds them warm. Never blocks startup.
pub fn spawn_metadata_warmup(context: Arc<DriverContext>, shutdown: &Shutdown) {
    super::timestamp_index::spawn_index_sweep(Arc::clone(&context), shutdown);
    super::visibility_index::spawn_visibility_index(Arc::clone(&context), shutdown);
    shutdown.spawn(async move {
        if context.metadata_handle.is_none() {
            return;
        }
        if let Err(error) = super::warm_caches(&context).await {
            warn!(error = %error, "Metadata visibility cache warmup failed");
            return;
        }
        if let Err(error) = super::iri_index::rebuild_index(&context).await {
            warn!(error = %error, "Metadata IRI reference index rebuild failed");
        }
    });
}
