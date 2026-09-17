//! Keyed detached work that survives its initiating request.
//! Concurrent callers share work, and finished values remain briefly joinable.

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
    /// Decides which finished values stay joinable; `None` keeps every value.
    retain_if: Option<fn(&V) -> bool>,
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
            retain_if: None,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Keeps a finished value joinable only while `keep` accepts it. Callers
    /// already waiting still receive a rejected value; a later caller for the
    /// same key starts fresh work instead of joining it.
    pub fn retain_if(mut self, keep: fn(&V) -> bool) -> Self {
        self.retain_if = Some(keep);
        self
    }

    /// Joins the work already running for `key`, or spawns `work` for it. The
    /// work is detached, so dropping the returned watch never cancels it. Work
    /// that ended without a value, such as by panicking, is not joined again.
    pub fn join<F>(&self, key: K, work: F) -> JoinWatch<V>
    where
        F: Future<Output = V> + Send + 'static,
    {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let retention = self.retention;
        let retain_if = self.retain_if;
        entries.retain(|_, watch| {
            // Read closed first, so a value sent just before the producer ended is seen.
            // An ended producer without a value failed, so its key must start fresh work.
            let ended = watch.has_changed().is_err();
            watch.borrow().as_ref().map_or(!ended, |joined| {
                joined.finished.elapsed() < retention
                    && retain_if.is_none_or(|keep| keep(&joined.value))
            })
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

    // A rejected value must not be served to a later caller, so a corrected
    // retry runs again inside the retention window.
    #[tokio::test(start_paused = true)]
    async fn drops_failed_value() {
        let registry: JoinRegistry<&str, Result<&str, &str>> =
            JoinRegistry::new(RETENTION).retain_if(|value| value.is_ok());

        let failed = registry.join("key", async { Err("failed") });
        assert_eq!(await_joined(failed).await, Some(Err("failed")));

        let retried = registry.join("key", async { Ok("retried") });
        assert_eq!(await_joined(retried).await, Some(Ok("retried")));
    }

    // A panicked producer leaves no value, so the key must not stay joined to it.
    #[tokio::test(start_paused = true)]
    async fn retries_panicked_work() {
        let registry: JoinRegistry<&str, &str> = JoinRegistry::new(RETENTION);

        let failed = registry.join("key", async { panic!("producer failed") });
        assert_eq!(await_joined(failed).await, None);

        let retried = registry.join("key", async { "retried" });
        assert_eq!(await_joined(retried).await, Some("retried"));
    }

    // Callers arriving after a failure must share one fresh run and its value.
    #[tokio::test(start_paused = true)]
    async fn shares_retried_work() {
        let registry: JoinRegistry<&str, usize> = JoinRegistry::new(RETENTION);
        let runs = Arc::new(AtomicUsize::new(0));
        let failed = registry.join("key", async { panic!("producer failed") });
        assert_eq!(await_joined(failed).await, None);

        let (gate, blocked) = tokio::sync::oneshot::channel();
        let first = registry.join("key", {
            let runs = runs.clone();
            async move {
                let _ = blocked.await;
                runs.fetch_add(1, Ordering::SeqCst);
                1
            }
        });
        let later: Vec<_> = (2..5)
            .map(|value| {
                let runs = runs.clone();
                registry.join("key", async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    value
                })
            })
            .collect();

        let _ = gate.send(());
        assert_eq!(await_joined(first).await, Some(1));
        for watch in later {
            assert_eq!(await_joined(watch).await, Some(1));
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    // Evicting the failed entry must spare its running replacement, which a later
    // cleanup pass must keep so its finished value stays joinable.
    #[tokio::test(start_paused = true)]
    async fn keeps_replacement_entry() {
        let registry: JoinRegistry<&str, usize> = JoinRegistry::new(RETENTION);
        let failed = registry.join("key", async { panic!("producer failed") });
        assert_eq!(await_joined(failed).await, None);

        let (gate, blocked) = tokio::sync::oneshot::channel();
        let replacement = registry.join("key", async move {
            let _ = blocked.await;
            1
        });
        let other = registry.join("other", async { 2 });
        assert_eq!(await_joined(other).await, Some(2));

        let _ = gate.send(());
        assert_eq!(await_joined(replacement).await, Some(1));
        let later = registry.join("key", async { 3 });
        assert_eq!(await_joined(later).await, Some(1));
    }
}
