//! Waits on progress in tests with a hang cap: the one bounded poll every crate's tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

/// The wait after the first miss of [`poll_until`].
const POLL_FIRST: Duration = Duration::from_millis(10);
/// The longest wait between two asks of [`poll_until`].
const POLL_MOST: Duration = Duration::from_millis(500);

/// Asks `check` until it answers `Some`, waiting twice as long after each miss up to half a
/// second. Answers `None` once `cap` has passed, also while an ask still runs: a hang cap for
/// tests, never a speed bound.
pub async fn poll_until<T>(cap: Duration, mut check: impl AsyncFnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + cap;
    let mut wait = POLL_FIRST;
    loop {
        match tokio::time::timeout_at(deadline, check()).await {
            Ok(Some(found)) => return Some(found),
            Ok(None) => {}
            Err(_) => return None,
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return None;
        }
        tokio::time::sleep(wait.min(deadline - now)).await;
        wait = wait.saturating_mul(2).min(POLL_MOST);
    }
}

#[cfg(test)]
mod tests {
    use super::poll_until;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn hung_ask_capped() {
        let hung = async || std::future::pending::<Option<()>>().await;
        assert_eq!(poll_until(Duration::from_secs(1), hung).await, None);
    }
}
