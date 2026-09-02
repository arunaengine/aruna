//! Keyed detached work that a later caller can join.
//!
//! A long operation started by one request must survive that request: the work
//! runs detached under a key, a concurrent caller for the same key shares the
//! running task, and a finished value stays joinable for a retention window.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub struct Joined<V> {
    finished: Instant,
    pub value: V,
}

pub type JoinWatch<V> = watch::Receiver<Option<Joined<V>>>;

#[derive(Debug)]
pub struct JoinRegistry<K, V> {
    retention: Duration,
    entries: Mutex<HashMap<K, JoinWatch<V>>>,
}

impl<K, V> JoinRegistry<K, V>
where
    K: Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    /// A finished value stays joinable for `retention` after it arrived.
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Joins the work already running for `key`, or spawns `work` for it. The
    /// work is detached, so dropping the returned watch never cancels it.
    pub fn join<F>(&self, key: K, work: F) -> JoinWatch<V>
    where
        F: Future<Output = V> + Send + 'static,
    {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let retention = self.retention;
        entries.retain(|_, watch| {
            watch
                .borrow()
                .as_ref()
                .is_none_or(|joined| joined.finished.elapsed() < retention)
        });
        if let Some(watch) = entries.get(&key) {
            return watch.clone();
        }
        let (sender, receiver) = watch::channel(None);
        entries.insert(key, receiver.clone());
        tokio::spawn(async move {
            let value = work.await;
            let _ = sender.send(Some(Joined {
                finished: Instant::now(),
                value,
            }));
        });
        receiver
    }
}

/// Waits for the joined value. `None` means the detached task ended without
/// producing one, which is a failure of the task, not of the caller.
pub async fn await_joined<V: Clone>(mut watch: JoinWatch<V>) -> Option<V> {
    loop {
        if let Some(joined) = watch.borrow_and_update().clone() {
            return Some(joined.value);
        }
        if watch.changed().await.is_err() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const RETENTION: Duration = Duration::from_secs(600);

    // Two callers for one key must run the work once and share the value.
    #[tokio::test(start_paused = true)]
    async fn joins_running_work() {
        let registry = JoinRegistry::new(RETENTION);
        let runs = Arc::new(AtomicUsize::new(0));
        let (gate, blocked) = tokio::sync::oneshot::channel();

        let first = registry.join("key", {
            let runs = runs.clone();
            async move {
                let _ = blocked.await;
                runs.fetch_add(1, Ordering::SeqCst);
                "first"
            }
        });
        let second = registry.join("key", {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                "second"
            }
        });

        let _ = gate.send(());
        assert_eq!(await_joined(first).await, Some("first"));
        assert_eq!(await_joined(second).await, Some("first"));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    // A caller inside the retention window gets the finished value; one after
    // it starts fresh work.
    #[tokio::test(start_paused = true)]
    async fn retains_finished_value() {
        let registry = JoinRegistry::new(RETENTION);

        assert_eq!(
            await_joined(registry.join("key", async { "first" })).await,
            Some("first")
        );

        tokio::time::advance(RETENTION / 2).await;
        assert_eq!(
            await_joined(registry.join("key", async { "second" })).await,
            Some("first")
        );

        tokio::time::advance(RETENTION).await;
        assert_eq!(
            await_joined(registry.join("key", async { "second" })).await,
            Some("second")
        );
    }
}
