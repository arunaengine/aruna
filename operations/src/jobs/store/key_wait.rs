//! Parks jobs that need locked bucket keys and wakes them after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::KeyWait;
use aruna_core::structs::storage::encryption::BucketKeyRef;

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

/// The keys a parked job still waits for; empty when it is not parked.
pub async fn read_key_waits(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<KeyWait>, String> {
    drive(row_calls::read_key_waits(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

/// Marks `key` available for one job.
pub async fn satisfy_key_wait(
    context: &DriverContext,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<bool, JobMutationError> {
    drive(row_calls::satisfy_key_wait(job_id, key, now_ms), context).await
}

/// Wakes one page of jobs waiting for `key`.
pub async fn wake_key_page(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<(usize, bool), JobMutationError> {
    drive(row_calls::wake_key_page(key, now_ms), context).await
}

/// Wakes every job waiting for `key`; call after the key is installed.
pub async fn wake_key_waits(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, JobMutationError> {
    drive(row_calls::wake_key_waits(key, now_ms), context).await
}

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
