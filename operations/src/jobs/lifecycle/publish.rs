//! Drives the execution chain publications and the terminal settle run from async callers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobId, JobRecord, PhysicalExecutionState};
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;

use super::LifecycleError;
use super::updates::{PublishProgressOperation, PublishTerminalOperation};
use crate::driver::{DriverContext, drive};

/// Publishes a non-terminal state of a receipted execution. A job with no
/// receipt here is a purely local execution and publishes nothing.
pub async fn publish_progress(
    context: &DriverContext,
    job_id: JobId,
    state: PhysicalExecutionState,
) {
    let node = context.node_and_realm();
    let publish = PublishProgressOperation::new(job_id, state, node, unix_timestamp_millis());
    if let Err(error) = drive(publish, context).await {
        warn!(error = %error, "Execution publication failed");
    }
}

/// Publishes the terminal state of a receipted execution and releases its reservation; see
/// [`PublishTerminalOperation`].
pub async fn publish_terminal(context: &DriverContext, record: &JobRecord) -> bool {
    let operation = PublishTerminalOperation::new(
        record.clone(),
        context.node_and_realm(),
        unix_timestamp_millis(),
    );
    published(drive(operation, context).await)
}

/// Whether a publication met its obligation.
fn published(outcome: Result<bool, LifecycleError>) -> bool {
    match outcome {
        Ok(done) => done,
        Err(error) => {
            warn!(error = %error, "Execution publication failed");
            false
        }
    }
}
