//! Progress-detecting convergence waits and a per-poll hang cap, shared by the
//! multi-node integration tests. A slow-but-converging run keeps waiting; a
//! stuck run fails after a lost-progress window, and a deadlocked poll panics
//! naming its own context instead of hanging until the CI job timeout.

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

/// Polls `check` until it reports zero still-pending units. The lost-progress
/// deadline resets whenever the pending count strictly decreases, so the wait
/// fails only after `NO_PROGRESS_TIMEOUT` with no step forward, never at a fixed
/// wall-clock budget. `context` names the wait in the lost-progress error. A
/// single poll exceeding `HANG_CAP` is treated as a deadlock and panics.
///
/// Generic over the caller's error so both `Box<dyn Error>` and its `Send + Sync`
/// variant can flow through unchanged.
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
