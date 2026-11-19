//! Runs the metadata index sweeps on the shutdown supervisor and primes the metadata caches
//! off the boot path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_tasks::shutdown::Shutdown;
use tracing::warn;

use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::timestamp_sweep::{SWEEP_SCAN_LIMIT, TimestampSweepOperation};
use aruna_operations::metadata::visibility_index::{
    PassOutcome, REBUILD_INTERVAL, VisibilityPassOperation, WORK_INTERVAL,
};

/// Pause between two passes of the timestamp index sweep.
const SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Starts both index sweeps and primes the metadata caches so the first user query finds
/// them warm. Never blocks startup.
pub fn spawn_metadata_warmup(context: Arc<DriverContext>, shutdown: &Shutdown) {
    spawn_index_sweep(Arc::clone(&context), shutdown);
    spawn_visibility_index(Arc::clone(&context), shutdown);
    shutdown.spawn(async move {
        if context.metadata_handle.is_none() {
            return;
        }
        if let Err(error) = aruna_operations::metadata::warm_caches(&context).await {
            warn!(error = %error, "Metadata visibility cache warmup failed");
            return;
        }
        let rebuild = aruna_operations::metadata::iri_index::RebuildIrisOperation::new();
        if let Err(error) = drive(rebuild, &context).await {
            warn!(error = %error, "Metadata IRI reference index rebuild failed");
        }
    });
}

/// Runs the timestamp index sweep, resuming from the previous pass so no single pass walks
/// the whole index.
fn spawn_index_sweep(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        let mut cursor = None;
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
            let sweep = TimestampSweepOperation::new(cursor.take(), SWEEP_SCAN_LIMIT);
            match drive(sweep, &context).await {
                Ok(pass) => cursor = pass.next_after,
                Err(error) => warn!(error = ?error, "Metadata timestamp-index sweep failed"),
            }
        }
    });
}

/// Keeps the anonymous visibility index current, one bounded pass per tick. Changed
/// visibility is picked up next cycle; until then the reader's per-record re-check keeps a
/// newly hidden record from being served.
fn spawn_visibility_index(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let token = shutdown.token();
    shutdown.spawn(async move {
        while !token.is_cancelled() {
            let delay = match drive(VisibilityPassOperation::new(), &context).await {
                Ok(PassOutcome::Continue) => WORK_INTERVAL,
                Ok(PassOutcome::Idle) => REBUILD_INTERVAL,
                Err(error) => {
                    warn!(error = ?error, "Anonymous visibility index pass failed");
                    REBUILD_INTERVAL
                }
            };
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    });
}
