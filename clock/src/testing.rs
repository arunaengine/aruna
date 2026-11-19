//! Waits on progress in tests with a hang cap: the one bounded poll every crate's tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

/// The wait after the first miss of [`poll_until`].
const POLL_FIRST: Duration = Duration::from_millis(10);
/// The longest wait between two asks of [`poll_until`].
const POLL_MOST: Duration = Duration::from_millis(500);

/// Asks `check` until it answers `Some`, waiting twice as long after each miss up to half a
/// second. Answers `None` once `cap` has passed: a hang cap for tests, never a speed bound.
pub async fn poll_until<T>(cap: Duration, mut check: impl AsyncFnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + cap;
    let mut wait = POLL_FIRST;
    loop {
        if let Some(found) = check().await {
            return Some(found);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return None;
        }
        tokio::time::sleep(wait.min(deadline - now)).await;
        wait = wait.saturating_mul(2).min(POLL_MOST);
    }
}
