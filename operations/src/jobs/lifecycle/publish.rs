//! Drives the execution chain publications and the terminal settle run from async callers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobId, PhysicalExecutionState};
use aruna_core::time::unix_timestamp_millis;
use tracing::warn;

use super::updates::PublishProgressOperation;
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
