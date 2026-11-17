//! Parks jobs that need locked bucket keys and wakes them after unlock.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{KeyWait, parse_wait_key};
use aruna_core::structs::storage::encryption::BucketKeyRef;

use super::park_wake::{AwaitOutcome, ParkJobOperation, SatisfyWaitOperation};
use super::settle_attempt;
use super::*;
use crate::driver::drive;

/// Wake rows handled per transaction page.
pub const WAKE_PAGE: usize = 64;

/// Parks a claimed job until every key in `waits` is unlocked; see [`ParkJobOperation`].
pub async fn park_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> Result<AwaitOutcome, JobMutationError> {
    if waits.is_empty() {
        return Err(JobMutationError::Storage("park without key waits".into()));
    }
    let mut mutate = park_mutation(token, now_ms);
    let mut attempt = 0;
    loop {
        let result = drive(
            ParkJobOperation::new(job_id, &mut mutate, waits.clone()),
            context,
        )
        .await;
        match settle_attempt(result, attempt, "job park exhausted retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// Parks the claimed job unless it settled or a cancel was requested meanwhile.
fn park_mutation(
    token: Ulid,
    now_ms: u64,
) -> impl FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send {
    move |record: &mut JobRecord| {
        guard_token(record, token)?;
        if record.is_settled() || record.cancel_requested {
            return Ok(JobMutation::Skip);
        }
        // An attempt intent may already have reached an executor; it cannot wait here.
        if record.attempt_intent.is_some() {
            return Err(JobMutationError::IntentConflict);
        }
        record.state = JobState::AwaitingKey;
        record.claim = None;
        record.updated_at_ms = now_ms;
        Ok(JobMutation::Persist)
    }
}

/// The keys a parked job still waits for; empty when it is not parked.
pub async fn read_key_waits(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<KeyWait>, String> {
    read_state(context, KEY_WAIT_KEYSPACE, job_wait_key(job_id), "key wait")
        .await
        .map(Option::unwrap_or_default)
}

/// Marks `key` available for one job; see [`SatisfyWaitOperation`].
pub async fn satisfy_key_wait(
    context: &DriverContext,
    job_id: JobId,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<bool, JobMutationError> {
    let mut attempt = 0;
    loop {
        let result = drive(SatisfyWaitOperation::new(job_id, key, now_ms), context).await;
        match settle_attempt(result, attempt, "key wake exhausted retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// Wakes one page of jobs waiting for `key`. Handled rows are deleted, so callers repeat
/// until this reports no further page.
pub async fn wake_key_page(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<(usize, bool), JobMutationError> {
    let prefix = ByteView::from([&b"k"[..], &key.key()].concat());
    let (rows, _) = iter_prefix_page(
        &context.storage_handle,
        KEY_WAIT_KEYSPACE,
        Some(prefix),
        None,
        WAKE_PAGE,
        None,
    )
    .await
    .map_err(JobMutationError::Storage)?;
    let mut woken = 0;
    for (row, _) in &rows {
        let Some((_, job_id)) = parse_wait_key(row.as_ref()) else {
            warn!("Deleting malformed key wait row");
            delete_raw(context, KEY_WAIT_KEYSPACE, row.clone(), None)
                .await
                .map_err(JobMutationError::Storage)?;
            continue;
        };
        if satisfy_key_wait(context, job_id, key, now_ms).await? {
            woken += 1;
        }
    }
    Ok((woken, rows.len() == WAKE_PAGE))
}

/// Wakes every job waiting for `key` in bounded pages; call after the key is installed.
pub async fn wake_key_waits(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, JobMutationError> {
    let mut total = 0;
    loop {
        let (woken, more) = wake_key_page(context, key, now_ms).await?;
        total += woken;
        if !more {
            return Ok(total);
        }
    }
}

#[cfg(test)]
#[path = "key_wait_tests.rs"]
mod tests;
