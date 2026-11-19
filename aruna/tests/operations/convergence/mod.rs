//! Shares the test polling helpers that wait for convergence and cap a hung check.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_clock::testing::poll_until;
use aruna_core::types::GroupId;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::metadata::get_document::GetDocumentOperation;
use aruna_test_support::metadata::materialization_jobs_exist;
use tokio::time::timeout;
use ulid::Ulid;

pub type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// A single poll that never returns is a deadlock, not slowness: no honest check
/// against local state takes minutes. Elapsing panics and names the culprit.
pub const HANG_CAP: Duration = Duration::from_secs(300);

/// Lost-progress window, not a total budget: it resets on every observed step
/// forward, so a slow run is never failed for slowness, only for being stuck.
pub const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Polls until no work remains. Each lost-progress window ends early on an observed decrease,
/// so only a whole window without one fails.
pub async fn wait_for_convergence<F, Fut, E>(context: &str, check: F) -> Result<(), E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<usize, E>>,
    E: From<String>,
{
    let (best, last) = (AtomicUsize::new(usize::MAX), AtomicUsize::new(usize::MAX));
    let (check, best, last) = (&check, &best, &last);
    loop {
        let seen = poll_until(NO_PROGRESS_TIMEOUT, move || async move {
            let pending = match timeout(HANG_CAP, check()).await {
                Ok(Ok(pending)) => pending,
                Ok(Err(error)) => return Some(Err(error)),
                Err(_) => panic!("hang cap fired: `{context}` poll exceeded {HANG_CAP:?}"),
            };
            last.store(pending, Relaxed);
            match pending {
                0 => Some(Ok(true)),
                fewer if fewer < best.load(Relaxed) => {
                    best.store(fewer, Relaxed);
                    Some(Ok(false))
                }
                _ => None,
            }
        })
        .await;
        match seen {
            Some(Ok(true)) => return Ok(()),
            Some(Ok(false)) => {}
            Some(Err(error)) => return Err(error),
            None => {
                let pending = last.load(Relaxed);
                return Err(format!("{context} (still pending: {pending})").into());
            }
        }
    }
}

/// Polls every node until each document reads back. Fails only when a whole
/// lost-progress window passes without one more document becoming visible.
pub async fn wait_for_visibility(
    contexts: &[Arc<DriverContext>],
    pairs: &[(GroupId, Ulid)],
) -> Result<(), String> {
    let remaining: Mutex<Vec<Vec<(GroupId, Ulid)>>> =
        Mutex::new(contexts.iter().map(|_| pairs.to_vec()).collect());
    let remaining = &remaining;
    let waited = wait_for_convergence("visibility stalled", move || async move {
        let missing_before = remaining.lock().unwrap().clone();
        let mut missing_now = Vec::with_capacity(contexts.len());
        for (context, missing) in contexts.iter().zip(missing_before) {
            let mut still_missing = Vec::new();
            for (group_id, document_id) in missing {
                let read = GetDocumentOperation::new(group_id, document_id);
                if drive(read, context.as_ref()).await.is_err() {
                    still_missing.push((group_id, document_id));
                }
            }
            missing_now.push(still_missing);
        }
        let pending = missing_now.iter().map(Vec::len).sum();
        *remaining.lock().unwrap() = missing_now;
        Ok::<usize, String>(pending)
    })
    .await;
    waited.map_err(|_| {
        let counts: Vec<usize> = remaining.lock().unwrap().iter().map(Vec::len).collect();
        format!("visibility stalled; missing per node: {counts:?}")
    })
}

/// Waits until no node has a metadata materialization job left.
pub async fn wait_empty_queues(contexts: &[Arc<DriverContext>]) -> Result<(), String> {
    wait_for_convergence("materialization queues never drained", || async {
        let mut busy = 0usize;
        for context in contexts {
            if materialization_jobs_exist(context)
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
pub async fn wait_storage_released(
    storage: aruna_storage::storage::handle::StorageHandle,
) -> Result<(), String> {
    timeout(HANG_CAP, storage.close())
        .await
        .map_err(|_| "storage still held after shutdown".to_string())
}
