//! Restores the durable task timers into the task handle when the node starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEffect};
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_storage::task_timers::delete_timer_key;
use aruna_tasks::TaskHandle;
use tracing::warn;

const RESTORE_PAGE_SIZE: usize = 256;

pub async fn restore_task_timers(storage: &StorageHandle, task_handle: &TaskHandle) {
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: RESTORE_PAGE_SIZE,
                txn_id: None,
            })
            .await;

        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                warn!(error = %error, "Failed to restore persisted task timers");
                return;
            }
            other => {
                warn!(event = ?other, "Unexpected event while restoring persisted task timers");
                return;
            }
        };

        for (key_bytes, value) in values {
            let record = match postcard::from_bytes::<PersistedTaskTimer>(&value) {
                Ok(record) => record,
                Err(error) => {
                    warn!(error = %error, "Failed to decode persisted task timer");
                    delete_timer_key(storage, key_bytes).await;
                    continue;
                }
            };
            let after = Duration::from_millis(
                record
                    .due_unix_millis
                    .saturating_sub(unix_timestamp_millis()),
            );
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: record.key,
                    after,
                }))
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore persisted task timer");
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::task::TaskKey;
    use aruna_storage::FjallStorage;
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

        restore_task_timers(&storage, &task_handle).await;
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
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
        restore_task_timers(&storage, &task_handle).await;
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

        restore_task_timers(&storage, &task_handle).await;
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }
}
