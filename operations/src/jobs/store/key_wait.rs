//! Parks jobs that need locked bucket keys and wakes them after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::KeyWait;

use super::park_wake::AwaitOutcome;
use super::row_calls;
pub use super::row_calls::WAKE_PAGE;
use super::*;
use crate::driver::drive;

/// Parks a claimed job until every key in `waits` is unlocked.
pub async fn park_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> Result<AwaitOutcome, JobMutationError> {
    drive(row_calls::park_job(job_id, token, now_ms, waits), context).await
}

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
