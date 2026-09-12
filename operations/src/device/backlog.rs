//! Retry bookkeeping, page draining and timer arming shared by the device
//! publish queue and the synced-folder upload outbox. The stored state
//! enums stay distinct; only their arithmetic and control flow live here.

use std::future::Future;
use std::time::Duration;

use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::Key;
use aruna_core::time::unix_timestamp_millis;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::tasks::queue_backoff::retry_after_ms;

use super::drain::DrainOutcome;
use super::publish_queue::{PublishEntry, PublishState};
use super::sync::repository::{SyncUpload, UploadState};

/// The retry view both device queue states expose.
pub(crate) trait RetryView {
    /// When the entry may be forwarded again; `None` while it is terminal.
    fn due_at_ms(&self) -> Option<u64>;
    /// Forwards already spent; zero while the entry is terminal.
    fn attempts(&self) -> u32;

    /// Whether the drain may pick the entry up now.
    fn is_due(&self, now_ms: u64) -> bool {
        self.due_at_ms()
            .is_some_and(|due_at_ms| due_at_ms <= now_ms)
    }
}

impl RetryView for PublishState {
    fn due_at_ms(&self) -> Option<u64> {
        match self {
            Self::Pending { due_at_ms, .. } | Self::Publishing { due_at_ms, .. } => {
                Some(*due_at_ms)
            }
            Self::Published { .. } | Self::Failed { .. } => None,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Pending { attempts, .. } | Self::Publishing { attempts, .. } => *attempts,
            Self::Published { .. } | Self::Failed { .. } => 0,
        }
    }
}

impl RetryView for UploadState {
    fn due_at_ms(&self) -> Option<u64> {
        match self {
            Self::Pending { due_at_ms, .. } => Some(*due_at_ms),
            Self::Failed { .. } => None,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Pending { attempts, .. } => *attempts,
            Self::Failed { .. } => 0,
        }
    }
}

impl RetryView for PublishEntry {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

impl RetryView for SyncUpload {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

/// Due time for the next attempt after `attempts` failures.
pub(crate) fn retry_due_ms(attempts: u32) -> u64 {
    unix_timestamp_millis().saturating_add(retry_after_ms(attempts))
}

/// Whether `attempts` spent exhausts `max_attempts` and parks the row.
pub(crate) fn exhausted(attempts: u32, max_attempts: u32) -> bool {
    attempts >= max_attempts
}

/// What forwarding one due queue entry asks of the drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForwardOutcome {
    /// The entry may need another pass; keep the drain awake.
    Recheck,
    /// This pass is done with the entry.
    Settled,
}

/// One device queue as the page drain drives it.
pub(crate) trait QueueDrain {
    type Entry: RetryView;

    fn read(
        &mut self,
        cursor: Option<Key>,
    ) -> impl Future<Output = Option<(Vec<Self::Entry>, Option<Key>)>> + Send;

    fn forward(&mut self, entry: Self::Entry) -> impl Future<Output = ForwardOutcome> + Send;
}

/// Drains one queue page by page: entries that are not due are skipped, the
/// rest go to `forward`. `forward` reports whether the entry asks for another
/// pass, so each queue keeps its own claim ordering.
pub(crate) async fn drain_queue<Q: QueueDrain>(now_ms: u64, mut queue: Q) -> DrainOutcome {
    let mut cursor: Option<Key> = None;
    let mut needs_recheck = false;
    loop {
        let Some((entries, next)) = queue.read(cursor).await else {
            return DrainOutcome::Deferred;
        };
        for entry in entries {
            if !entry.is_due(now_ms) {
                continue;
            }
            needs_recheck |= matches!(queue.forward(entry).await, ForwardOutcome::Recheck);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    match needs_recheck {
        true => DrainOutcome::Recheck,
        false => DrainOutcome::Idle,
    }
}

/// Arms `task_key` now, reporting a scheduling failure under `failure`.
pub(crate) async fn arm_timer(task_handle: &TaskHandle, task_key: TaskKey, failure: &str) {
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_idle_timer(task_key, Duration::ZERO)
        .await
    {
        warn!(message = %message, "{}", failure);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aruna_core::types::Key;

    use super::{ForwardOutcome, QueueDrain, RetryView, drain_queue};
    use crate::device::drain::DrainOutcome;

    #[derive(Clone, Debug)]
    struct Entry {
        due_at_ms: u64,
        recheck: bool,
    }

    impl RetryView for Entry {
        fn due_at_ms(&self) -> Option<u64> {
            Some(self.due_at_ms)
        }

        fn attempts(&self) -> u32 {
            0
        }
    }

    /// A queue whose each `read` takes the next scripted page.
    struct FakeQueue {
        pages: Vec<Option<Vec<Entry>>>,
        forwarded: Arc<AtomicUsize>,
    }

    impl FakeQueue {
        fn new(pages: Vec<Option<Vec<Entry>>>, forwarded: Arc<AtomicUsize>) -> Self {
            Self { pages, forwarded }
        }
    }

    impl QueueDrain for FakeQueue {
        type Entry = Entry;

        async fn read(&mut self, _cursor: Option<Key>) -> Option<(Vec<Entry>, Option<Key>)> {
            match self.pages.remove(0) {
                Some(entries) => {
                    let next = (!self.pages.is_empty()).then(|| Key::from(vec![1]));
                    Some((entries, next))
                }
                None => None,
            }
        }

        async fn forward(&mut self, entry: Entry) -> ForwardOutcome {
            self.forwarded.fetch_add(1, Ordering::SeqCst);
            match entry.recheck {
                true => ForwardOutcome::Recheck,
                false => ForwardOutcome::Settled,
            }
        }
    }

    fn entry(due_at_ms: u64, recheck: bool) -> Entry {
        Entry { due_at_ms, recheck }
    }

    #[tokio::test]
    async fn rechecks_last_entry() {
        // Processing the last entry may still ask for another pass, so the
        // outcome must not claim that entries remain or that none do.
        let queue = FakeQueue::new(vec![Some(vec![entry(0, true)])], Arc::default());
        assert_eq!(drain_queue(0, queue).await, DrainOutcome::Recheck);
    }

    #[tokio::test]
    async fn rechecks_failed_claim() {
        // A failed claim also asks for another pass: the entry may have
        // advanced rather than the queue being empty.
        let queue = FakeQueue::new(vec![Some(vec![entry(0, true)])], Arc::default());
        assert_eq!(drain_queue(0, queue).await, DrainOutcome::Recheck);
    }

    #[tokio::test]
    async fn skips_future_entries() {
        let queue = FakeQueue::new(vec![Some(vec![entry(1_000, true)])], Arc::default());
        assert_eq!(drain_queue(999, queue).await, DrainOutcome::Idle);
    }

    #[tokio::test]
    async fn defers_partial_pass() {
        // A later page that cannot be read must not hide work already
        // forwarded from an earlier one.
        let forwarded = Arc::default();
        let queue = FakeQueue::new(
            vec![Some(vec![entry(0, true)]), None],
            Arc::clone(&forwarded),
        );
        assert_eq!(drain_queue(0, queue).await, DrainOutcome::Deferred);
        assert_eq!(forwarded.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn settles_without_recheck() {
        let queue = FakeQueue::new(vec![Some(vec![entry(0, false)])], Arc::default());
        assert_eq!(drain_queue(0, queue).await, DrainOutcome::Idle);
    }
}
