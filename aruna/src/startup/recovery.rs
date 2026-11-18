//! Starts remote recovery in the background and reports when it stops early.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::node::recovery_run::{RecoveryConfig, RecoveryOperation};
use aruna_operations::shard::recovery::{
    RecoveryError, RecoveryOutcome, RecoveryStatus, RecoveryStop,
};
use tokio_util::sync::CancellationToken;

/// Runs recovery until it converges or `cancelled` fires. A failure or panic of the run is
/// reported as degraded rather than as completion.
pub async fn run_recovery(
    context: Arc<DriverContext>,
    config: RecoveryConfig,
    status: RecoveryStatus,
    cancelled: CancellationToken,
) {
    let stop = RecoveryStop::new(cancelled);
    let serving = context.node_and_realm().map(|(_, realm_id)| realm_id);
    let recovery = RecoveryOperation::new(config, serving, status.clone(), stop);
    let run = tokio::spawn(async move { drive(recovery, &context).await });
    let stopped = match run.await {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error.to_string(),
        Err(error) => error.to_string(),
    };
    status.finish_pass(RecoveryOutcome::Failed, Some(RecoveryError::Panicked));
    tracing::error!(
        event = "startup.recovery.degraded",
        error_class = RecoveryError::Panicked.label(),
        error = %stopped,
        "Remote recovery driver stopped; convergence now depends on durable retry timers"
    );
}
