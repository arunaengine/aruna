use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEffect, TaskKey};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

const TASK_TIMER_RESTORE_PAGE_SIZE: usize = 256;

pub(crate) async fn persist_task_effect(storage: &StorageHandle, effect: &TaskEffect) {
    let result = match effect {
        TaskEffect::ResetTimer { key, after } => write_timer(storage, key, *after).await,
        TaskEffect::ShortenTimer { key, after } => shorten_timer(storage, key, *after).await,
        TaskEffect::CancelTimer { key } => delete_timer(storage, key).await,
        TaskEffect::AbortRunningHandlers { .. } => Ok(()),
    };

    if let Err(error) = result {
        warn!(error = %error, effect = ?effect, "Failed to persist task timer effect");
    }
}

pub(crate) async fn delete_persisted_timer(storage: &StorageHandle, key: &TaskKey) {
    if let Err(error) = delete_timer(storage, key).await {
        warn!(error = %error, key = ?key, "Failed to delete persisted task timer");
    }
}

pub async fn restore_persisted_task_timers(storage: &StorageHandle, task_handle: &TaskHandle) {
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                prefix: None,
                start_after: start_after.take(),
                limit: TASK_TIMER_RESTORE_PAGE_SIZE,
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
                    delete_timer_by_key(storage, key_bytes).await;
                    continue;
                }
            };
            let after =
                Duration::from_millis(record.due_at_unix_millis.saturating_sub(now_millis()));
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

async fn write_timer(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
) -> Result<(), String> {
    let due_at_unix_millis = due_at_millis(after)?;
    write_record(
        storage,
        &PersistedTaskTimer {
            key: key.clone(),
            due_at_unix_millis,
        },
    )
    .await
}

async fn shorten_timer(
    storage: &StorageHandle,
    key: &TaskKey,
    after: Duration,
) -> Result<(), String> {
    let requested_due_at = due_at_millis(after)?;
    match read_timer(storage, key).await? {
        Some(existing) if existing.due_at_unix_millis <= requested_due_at => Ok(()),
        _ => {
            write_record(
                storage,
                &PersistedTaskTimer {
                    key: key.clone(),
                    due_at_unix_millis: requested_due_at,
                },
            )
            .await
        }
    }
}

async fn read_timer(
    storage: &StorageHandle,
    key: &TaskKey,
) -> Result<Option<PersistedTaskTimer>, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_key_storage_key(key)?,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(|error| error.to_string()))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn write_record(storage: &StorageHandle, record: &PersistedTaskTimer) -> Result<(), String> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: task_key_storage_key(&record.key)?,
            value: ByteView::from(
                postcard::to_allocvec(record).map_err(|error| error.to_string())?,
            ),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn delete_timer(storage: &StorageHandle, key: &TaskKey) -> Result<(), String> {
    delete_timer_by_key(storage, task_key_storage_key(key)?).await;
    Ok(())
}

async fn delete_timer_by_key(storage: &StorageHandle, key: ByteView) {
    if let Event::Storage(StorageEvent::Error { error }) = storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        warn!(error = %error, "Failed to delete persisted task timer");
    }
}

fn task_key_storage_key(key: &TaskKey) -> Result<ByteView, String> {
    postcard::to_allocvec(key)
        .map(ByteView::from)
        .map_err(|error| error.to_string())
}

fn due_at_millis(after: Duration) -> Result<u64, String> {
    let after_millis =
        u64::try_from(after.as_millis()).map_err(|_| "timer duration is too large".to_string())?;
    now_millis()
        .checked_add(after_millis)
        .ok_or_else(|| "timer deadline overflow".to_string())
}

fn now_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
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
    async fn restores_persisted_timer_to_new_task_handle() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let key = TaskKey::SyncPlacements { realm_id, node_id };

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(1),
            },
        )
        .await;

        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        restore_persisted_task_timers(&storage, &task_handle).await;
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }
}
