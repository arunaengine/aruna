//! Single cancellation path for node shutdown.
//!
//! Subsystems register their background children here instead of detaching them
//! with a bare `tokio::spawn`, so an ordered shutdown can stop admission, drain
//! what is still running, and know when nothing can write any more.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Cancellation token plus the set of background children that observe it.
#[derive(Clone, Debug, Default)]
pub struct Shutdown {
    token: CancellationToken,
    tracker: TaskTracker,
}

impl Shutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn is_triggered(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves once shutdown has been triggered.
    pub async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    pub fn trigger(&self) {
        self.token.cancel();
    }

    /// Spawns a tracked child. Children spawned here are awaited by `drain`.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tracker.spawn(future);
    }

    pub fn tracked_children(&self) -> usize {
        self.tracker.len()
    }

    /// Triggers cancellation and waits for tracked children, bounded by
    /// `timeout`. Returns `true` when every child finished in time.
    pub async fn drain(&self, timeout: Duration) -> bool {
        self.token.cancel();
        self.tracker.close();
        tokio::time::timeout(timeout, self.tracker.wait())
            .await
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn drain_awaits_children() {
        let shutdown = Shutdown::new();
        let stopped = Arc::new(AtomicBool::new(false));
        let child_stopped = stopped.clone();
        let child = shutdown.clone();
        shutdown.spawn(async move {
            child.cancelled().await;
            child_stopped.store(true, Ordering::SeqCst);
        });

        assert!(shutdown.drain(Duration::from_secs(5)).await);
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drain_times_out_on_stuck_child() {
        let shutdown = Shutdown::new();
        shutdown.spawn(std::future::pending());

        assert!(!shutdown.drain(Duration::from_millis(50)).await);
        assert!(shutdown.is_triggered());
    }
}
