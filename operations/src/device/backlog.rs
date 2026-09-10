//! Retry bookkeeping, page draining and timer arming shared by the device
//! authoring intake and the synced-folder upload outbox. The stored state
//! enums stay distinct; only their arithmetic and control flow live here.

use std::future::Future;
use std::time::Duration;

use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::Key;
use aruna_core::util::unix_timestamp_millis;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::tasks::queue_backoff::queue_retry_after_ms;

use super::drain::DrainOutcome;
use super::intake::{IntakeEntry, IntakeState};
use super::sync::repository::{SyncUpload, UploadState};

/// The due/attempt view both device backlog states expose.
pub(crate) trait BacklogState {
    /// When the row may be forwarded again; `None` while it is terminal.
    fn due_at_ms(&self) -> Option<u64>;
    /// Forwards already spent; zero while the row is terminal.
    fn attempts(&self) -> u32;

    /// Whether the drain may pick the row up now.
    fn is_due(&self, now_ms: u64) -> bool {
        self.due_at_ms()
            .is_some_and(|due_at_ms| due_at_ms <= now_ms)
    }
}

impl BacklogState for IntakeState {
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

impl BacklogState for UploadState {
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

impl BacklogState for IntakeEntry {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

impl BacklogState for SyncUpload {
    fn due_at_ms(&self) -> Option<u64> {
        self.state.due_at_ms()
    }

    fn attempts(&self) -> u32 {
        self.state.attempts()
    }
}

/// Due time for the next attempt after `attempts` failures.
pub(crate) fn retry_due_ms(attempts: u32) -> u64 {
    unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts))
}

/// Whether `attempts` spent exhausts `max_attempts` and parks the row.
pub(crate) fn exhausted(attempts: u32, max_attempts: u32) -> bool {
    attempts >= max_attempts
}

/// One backlog plane as the page drain drives it.
pub(crate) trait BacklogDrain {
    type Row: BacklogState;

    fn read(
        &mut self,
        cursor: Option<Key>,
    ) -> impl Future<Output = Option<(Vec<Self::Row>, Option<Key>)>> + Send;

    fn forward(&mut self, row: Self::Row) -> impl Future<Output = bool> + Send;
}

/// Drains one backlog page by page: rows that are not due are skipped, the
/// rest go to `forward`. `forward` answers whether the row counted as progress,
/// so each plane keeps its own claim ordering.
pub(crate) async fn drain_backlog<D: BacklogDrain>(now_ms: u64, mut plane: D) -> DrainOutcome {
    let mut cursor: Option<Key> = None;
    let mut due = false;
    loop {
        let Some((rows, next)) = plane.read(cursor).await else {
            return DrainOutcome::Deferred;
        };
        for row in rows {
            if !row.is_due(now_ms) {
                continue;
            }
            due |= plane.forward(row).await;
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    match due {
        true => DrainOutcome::More,
        false => DrainOutcome::Idle,
    }
}

/// Arms `task_key` now, reporting a scheduling failure under `failure`.
pub(crate) async fn arm_timer(task_handle: &TaskHandle, task_key: TaskKey, failure: &str) {
    if let TaskEvent::Error { message, .. } = task_handle
        .schedule_timer_if_idle(task_key, Duration::ZERO)
        .await
    {
        warn!(message = %message, "{}", failure);
    }
}
