//! Ordered shutdown for the storage handle: refuse new work, drain admitted work
//! (including deferred cleanup and drop aborts), fence what is still queued, sync
//! the backend, then join the worker. `close` is last; it releases the file lock.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;

use super::handle::StorageHandle;

impl StorageHandle {
    /// Closes the store and waits until the worker released it, including the
    /// file lock. Blocks forever when another handle clone is still alive.
    pub async fn close(self) {
        let worker = self
            .worker
            .lock()
            .expect("storage worker mutex poisoned")
            .take();
        drop(self);
        if let Some(worker) = worker {
            let _ = tokio::task::spawn_blocking(move || worker.join()).await;
        }
    }

    /// Waits for effects accepted on either lane, including deferred cleanup and
    /// drop aborts, before the final sync. Bounded by `timeout`; `false` means work
    /// remains outstanding.
    pub async fn drain_accepted(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.in_flight() == 0 {
                return true;
            }
            let notified = self.metrics.drained.notified();
            // Re-check after arming the wait so a wake between the two is not lost.
            if self.in_flight() == 0 {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return self.in_flight() == 0;
            }
        }
    }

    /// True once the effect channel has permanently closed because the worker
    /// died. Latched and unrecoverable in-process; a cheap, lock-free read.
    pub fn channel_closed(&self) -> bool {
        self.metrics.channel_closed.load(Ordering::Relaxed)
    }

    /// Closes the write path before the final sync. Every mutating effect that
    /// arrives afterwards is rejected and counted, so a leaked child task can
    /// never commit behind a completed `sync_all`.
    pub fn close_writes(&self) {
        let _guard = self
            .metrics
            .close_lock
            .write()
            .expect("storage close lock poisoned");
        self.metrics.closed.store(true, Ordering::SeqCst);
    }

    pub fn writes_closed(&self) -> bool {
        self.metrics.closed.load(Ordering::SeqCst)
    }

    /// Stops the worker from starting any further mutation. Used when the
    /// shutdown drain timed out: work still queued must not commit after the
    /// final `sync_all`. Permanent for the life of the process.
    pub fn fence_mutations(&self) {
        self.metrics.mutations_fenced.store(true, Ordering::SeqCst);
    }

    /// Mutating effects rejected because storage was already closed.
    pub fn rejected_writes(&self) -> u64 {
        self.metrics.rejected_writes.load(Ordering::Relaxed)
    }

    /// Final backend sync after the write path is closed and the drain resolved.
    pub async fn sync_all(&self) -> Result<(), StorageError> {
        match self.dispatch_storage_effect(StorageEffect::SyncAll).await {
            StorageEvent::SyncAllFinished => Ok(()),
            StorageEvent::Error { error } => Err(error),
            _ => Err(StorageError::InvalidEffect),
        }
    }
}
