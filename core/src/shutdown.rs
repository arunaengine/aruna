//! Single cancellation path for node shutdown.
//!
//! Subsystems register their background children here instead of detaching them
//! with a bare `tokio::spawn`, so an ordered shutdown can stop admission, drain
//! what is still running, and know when nothing can write any more.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::warn;

/// Cancellation token plus the set of background children that observe it.
#[derive(Clone, Debug, Default)]
pub struct Shutdown {
    token: CancellationToken,
    tracker: TaskTracker,
    /// Admission gate. `TaskTracker::close` does not stop later spawns, so a
    /// racer could otherwise slip a child in behind a completed `drain`.
    closed: Arc<RwLock<bool>>,
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
    /// Once `drain` has closed admission the future is dropped without ever
    /// running, so nothing can start writing behind the drain.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let closed = self
            .closed
            .read()
            .expect("shutdown admission lock poisoned");
        if *closed {
            warn!("Rejected a background child spawned after the shutdown drain");
            return;
        }
        self.tracker.spawn(future);
    }

    pub fn tracked_children(&self) -> usize {
        self.tracker.len()
    }

    /// Closes admission, triggers cancellation, then waits for tracked children
    /// bounded by `timeout`. Returns `true` when every child finished in time.
    pub async fn drain(&self, timeout: Duration) -> bool {
        // Closing first serializes against `spawn`: a spawn already inside the
        // read guard is tracked, and every later one is rejected.
        *self
            .closed
            .write()
            .expect("shutdown admission lock poisoned") = true;
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

    // A child admitted after the drain returned would write behind the seal.
    #[tokio::test]
    async fn rejects_late_spawn() {
        let shutdown = Shutdown::new();
        assert!(shutdown.drain(Duration::from_secs(5)).await);

        let started = Arc::new(AtomicBool::new(false));
        let child_started = started.clone();
        shutdown.spawn(async move {
            child_started.store(true, Ordering::SeqCst);
        });

        assert_eq!(shutdown.tracked_children(), 0);
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drain_times_out_on_stuck_child() {
        let shutdown = Shutdown::new();
        shutdown.spawn(std::future::pending());

        assert!(!shutdown.drain(Duration::from_millis(50)).await);
        assert!(shutdown.is_triggered());
    }
}
