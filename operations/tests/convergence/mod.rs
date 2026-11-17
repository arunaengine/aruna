//! Shares the test polling helpers that wait for convergence and cap a hung check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::metadata::get_document::GetDocumentOperation;
use aruna_operations::metadata::materialization_queue::materialization_jobs_exist;
use tokio::time::{Instant, sleep, timeout};
use ulid::Ulid;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// A single poll that never returns is a deadlock, not slowness: no honest check
/// against local state takes minutes. Elapsing panics and names the culprit.
pub const HANG_CAP: Duration = Duration::from_secs(300);

/// Lost-progress window, not a total budget: it resets on every observed step
/// forward, so a slow run is never failed for slowness, only for being stuck.
pub const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Polls until no work remains, resetting the deadline after each observed decrease.
pub async fn wait_for_convergence<F, Fut, E>(context: &str, check: F) -> Result<(), E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<usize, E>>,
    E: From<String>,
{
    let mut best = usize::MAX;
    let mut deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
    let mut poll_interval = POLL_INTERVAL;
    loop {
        let pending = match timeout(HANG_CAP, check()).await {
            Ok(result) => result?,
            Err(_) => panic!("hang cap fired: `{context}` poll exceeded {HANG_CAP:?}"),
        };
        if pending == 0 {
            return Ok(());
        }
        if pending < best {
            best = pending;
            deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
            poll_interval = POLL_INTERVAL;
        }
        if Instant::now() >= deadline {
            return Err(format!("{context} (still pending: {pending})").into());
        }
        sleep(poll_interval).await;
        poll_interval = poll_interval.saturating_mul(2).min(MAX_POLL_INTERVAL);
    }
}

/// Polls every node until each document reads back. Fails only when a whole
/// lost-progress window passes without one more document becoming visible.
pub async fn wait_for_visibility(
    contexts: &[Arc<DriverContext>],
    pairs: &[(GroupId, Ulid)],
    poll_interval: Duration,
) -> Result<(), String> {
    let mut remaining: Vec<Vec<(GroupId, Ulid)>> =
        contexts.iter().map(|_| pairs.to_vec()).collect();
    let mut best = usize::MAX;
    let mut deadline = Instant::now() + NO_PROGRESS_TIMEOUT;

    loop {
        for (context, missing) in contexts.iter().zip(remaining.iter_mut()) {
            let mut still_missing = Vec::new();
            for &(group_id, document_id) in missing.iter() {
                if drive(
                    GetDocumentOperation::new(group_id, document_id),
                    context.as_ref(),
                )
                .await
                .is_err()
                {
                    still_missing.push((group_id, document_id));
                }
            }
            *missing = still_missing;
        }
        let pending: usize = remaining.iter().map(Vec::len).sum();
        if pending == 0 {
            return Ok(());
        }
        if pending < best {
            best = pending;
            deadline = Instant::now() + NO_PROGRESS_TIMEOUT;
        }
        if Instant::now() >= deadline {
            let counts: Vec<usize> = remaining.iter().map(Vec::len).collect();
            return Err(format!("visibility stalled; missing per node: {counts:?}"));
        }
        sleep(poll_interval).await;
    }
}

/// Waits until no node has a metadata materialization job left.
pub async fn wait_empty_queues(contexts: &[Arc<DriverContext>]) -> Result<(), String> {
    wait_for_convergence("materialization queues never drained", || async {
        let mut busy = 0usize;
        for context in contexts {
            if materialization_jobs_exist(&context.storage_handle)
                .await
                .map_err(|error| format!("materialization probe failed: {error:?}"))?
            {
                busy += 1;
            }
        }
        Ok(busy)
    })
    .await
}

/// Bounds a one-shot harness operation that could block forever under starvation.
/// On elapse it panics naming `context` so a deadlock is reported against its
/// culprit rather than hanging. `HANG_CAP` is a deadlock cap, not a deadline.
pub async fn hang_cap<F, T>(context: &str, op: F) -> T
where
    F: Future<Output = T>,
{
    match timeout(HANG_CAP, op).await {
        Ok(value) => value,
        Err(_) => panic!("hang cap fired: `{context}` did not finish within {HANG_CAP:?}"),
    }
}

/// Waits until every clone of `storage` is gone and its worker released the
/// directory, so a restart on the same path does not find fjall locked. The
/// deferred metadata persist thread keeps a clone until its flush loop ends.
pub async fn wait_storage_released(storage: aruna_storage::StorageHandle) -> Result<(), String> {
    timeout(HANG_CAP, storage.close())
        .await
        .map_err(|_| "storage still held after shutdown".to_string())
}
