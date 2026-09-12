//! Progress-detecting convergence waits and a per-poll hang cap, shared by the multi-node
//! integration tests.

#![allow(dead_code)]

use std::time::Duration;

use tokio::time::{Instant, sleep, timeout};

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
