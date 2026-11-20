//! Restores the durable task timers into the task handle when the node starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskEvent};
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::task_timers::delete_timer_key;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

const RESTORE_PAGE_SIZE: usize = 256;

/// Where a timer restoration stopped: the row after which a retry starts, and why.
#[derive(Debug)]
pub struct RestoreStopped {
    pub resume_after: Option<ByteView>,
    pub error: String,
}

/// Re-arms the persisted timers after the row `start_after`, page by page. A failed page or a
/// timer the scheduler refused stops it there, so a retry resumes without skipping a row. A timer
/// only moves earlier, so a retry never pushes back a newer live deadline.
pub async fn restore_task_timers(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    start_after: Option<ByteView>,
) -> Result<(), RestoreStopped> {
    let mut accepted = start_after;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                prefix: None,
                start: accepted.clone().map(IterStart::After),
                limit: RESTORE_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let stopped = |resume_after, error| Err(RestoreStopped { resume_after, error });
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return stopped(accepted, error.to_string());
            }
            other => return stopped(accepted, format!("unexpected storage event: {other:?}")),
        };

        for (key_bytes, value) in values {
            match postcard::from_bytes::<PersistedTaskTimer>(&value) {
                Ok(record) => {
                    let after = Duration::from_millis(
                        record
                            .due_unix_millis
                            .saturating_sub(unix_timestamp_millis()),
                    );
                    let event = task_handle
                        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                            key: record.key,
                            after,
                        }))
                        .await;
                    if let Event::Task(TaskEvent::Error { message, .. }) = event {
                        return stopped(accepted, message);
                    }
                }
                Err(error) => {
                    warn!(error = %error, "Failed to decode persisted task timer");
                    if let Err(error) = delete_timer_key(storage, key_bytes.clone()).await {
                        warn!(%error, "Failed to delete an undecodable persisted task timer");
                    }
                }
            }
            accepted = Some(key_bytes);
        }

        match next_start_after {
            Some(next) => accepted = Some(next),
            None => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::task::TaskKey;
    use aruna_storage::storage::worker::FjallStorage;
    use aruna_storage::task_timers::{persist_task_effect, read_timer};
    use aruna_tasks::InboundTaskHandler;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify};

    struct RecordingHandler {
        observed: Arc<Mutex<Option<TaskKey>>>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            *self.observed.lock().await = Some(key);
            self.notify.notify_one();
        }
    }

    #[tokio::test]
    async fn restores_persisted_handle() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(1),
            },
        )
        .await
        .expect("timer persists");

        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }

    // A failed page stops the restoration with its resume point instead of ending it as done, and
    // a retry from there arms the timer.
    #[tokio::test]
    async fn failed_page_resumes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_millis(1),
        };
        persist_task_effect(&storage, &reset).await.expect("timer persists");
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_page = move |effect: &StorageEffect| {
            let page = matches!(effect, StorageEffect::Iter { .. });
            page && !failed.swap(true, std::sync::atomic::Ordering::SeqCst)
        };
        let flaky = aruna_test_support::metadata::refusing_when(storage.clone(), first_page);
        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        let restored = restore_task_timers(&flaky, &task_handle, None).await;
        let stopped = restored.expect_err("the failed page stops the restoration");
        assert_eq!(stopped.resume_after, None);
        restore_task_timers(&flaky, &task_handle, stopped.resume_after)
            .await
            .expect("the retry restores");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");
        assert_eq!(*observed.lock().await, Some(key));
    }

    // A cancellation whose row could not be deleted fails, so no caller acknowledges it while the
    // row would schedule the work again at the next restart.
    #[tokio::test]
    async fn failed_cancel_errs() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        persist_task_effect(&storage, &reset).await.expect("timer persists");
        let refused = aruna_test_support::metadata::refusing(storage.clone(), TASK_TIMER_KEYSPACE);

        let cancel = TaskEffect::CancelTimer { key: key.clone() };
        assert!(persist_task_effect(&refused, &cancel).await.is_err());
        assert!(read_timer(&storage, &key).await.expect("timer reads").is_some());
    }

    #[tokio::test]
    async fn outbox_timer_restores() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::DrainSyncOutbox;
        let reset_after = Duration::from_secs(3600);

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: reset_after,
            },
        )
        .await
        .expect("drain timer persists");

        let reset_timer = read_timer(&storage, &key)
            .await
            .expect("reset timer reads")
            .expect("reset timer exists");

        let shorten_after = Duration::from_secs(1800);
        persist_task_effect(
            &storage,
            &TaskEffect::ShortenTimer {
                key: key.clone(),
                after: shorten_after,
            },
        )
        .await
        .expect("shortened drain timer persists");

        let shortened_timer = read_timer(&storage, &key)
            .await
            .expect("shortened timer reads")
            .expect("shortened timer exists");
        assert_eq!(shortened_timer.key, key);
        assert!(shortened_timer.due_unix_millis < reset_timer.due_unix_millis);

        let task_handle = TaskHandle::new();
        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        let aruna_core::task::TaskEvent::TimerScheduled { after, .. } =
            task_handle.schedule_idle_timer(key, Duration::ZERO).await
        else {
            panic!("expected restored timer");
        };
        assert!(
            after > Duration::ZERO,
            "restored deadline must retain a non-immediate retry"
        );
    }

    #[tokio::test]
    async fn metadata_graph_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainPruneQueue,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("prune drain timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn drain_notification_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("notification drain timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn prune_timer_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::PruneNotifications,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("prune timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn metadata_projection_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainProjectionQueue,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("projection timer persists");

        let timer = read_timer(&storage, &TaskKey::DrainProjectionQueue)
            .await
            .expect("timer reads")
            .expect("projection timer exists");
        assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
    }

    #[tokio::test]
    async fn timed_locks_ephemeral() {
        // A restart locks the bucket anyway, so its timed lock never reaches storage.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::LockBucket {
            bucket_id: ulid::Ulid::from_bytes([1; 16]),
            generation: 2,
            session_id: ulid::Ulid::from_bytes([3; 16]),
        };
        let after = Duration::from_secs(60);
        for effect in [
            TaskEffect::ResetTimer {
                key: key.clone(),
                after,
            },
            TaskEffect::ShortenTimer {
                key: key.clone(),
                after,
            },
        ] {
            persist_task_effect(&storage, &effect)
                .await
                .expect("nothing to persist");
        }
        assert_eq!(read_timer(&storage, &key).await.expect("timer read"), None);
    }

    #[tokio::test]
    async fn hidden_sweep_persists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::SweepHiddenBlobs,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("sweep timer persists");

        let timer = read_timer(&storage, &TaskKey::SweepHiddenBlobs)
            .await
            .expect("timer read")
            .expect("timer exists");
        assert_eq!(timer.key, TaskKey::SweepHiddenBlobs);
    }

    #[tokio::test]
    async fn metadata_projection_handle() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::DrainProjectionQueue;

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(1),
            },
        )
        .await
        .expect("timer persists");

        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }
}
