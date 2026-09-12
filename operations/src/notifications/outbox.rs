use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NOTIFICATION_OUTBOX_KEYSPACE;
use aruna_core::structs::{NotificationOutboxRecord, NotificationRecord};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::TxnId;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

pub const NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE: usize = 512;
pub const NOTIFICATION_DELIVERY_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const NOTIFICATION_OUTBOX_RETENTION_MS: u64 = 48 * 60 * 60 * 1000;

pub fn new_outbox_record(record: NotificationRecord) -> NotificationOutboxRecord {
    NotificationOutboxRecord {
        outbox_id: Ulid::generate(),
        record,
    }
}

pub fn schedule_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainNotificationOutbox,
        after: Duration::ZERO,
    })
}

pub struct NotificationOutboxBatch {
    pub records: Vec<(Vec<u8>, NotificationOutboxRecord)>,
    pub has_more: bool,
    pub next_start_after: Option<Vec<u8>>,
}

pub async fn read_outbox_batch(
    storage: &StorageHandle,
    start_after: Option<Vec<u8>>,
    limit: usize,
    txn_id: Option<TxnId>,
) -> Result<NotificationOutboxBatch, String> {
    let read_limit = limit.saturating_add(1);
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(|key| IterStart::After(ByteView::from(key))),
            limit: read_limit,
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let has_more = values.len() > limit;
            let page: Vec<_> = values.into_iter().take(limit).collect();
            let next_start_after = page.last().map(|(key, _)| key.to_vec());
            let mut records = Vec::with_capacity(page.len());
            for (key, value) in page {
                match NotificationOutboxRecord::from_bytes(&value) {
                    Ok(record) => records.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed notification outbox record");
                        delete_outbox_records(storage, vec![key]).await?;
                    }
                }
            }
            Ok(NotificationOutboxBatch {
                records,
                has_more,
                next_start_after,
            })
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn delete_outbox_records(
    storage: &StorageHandle,
    keys: Vec<Vec<u8>>,
) -> Result<(), String> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = keys
        .into_iter()
        .map(|key| {
            (
                NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                ByteView::from(key),
            )
        })
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

// ShortenTimer, never ResetTimer: this path may wake earlier, but must not push
// an existing retry deadline later.
pub async fn restore_outbox_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    after: Duration,
) {
    restore_timer_with(storage, task_handle, after, false).await;
}

pub async fn restore_idle_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    after: Duration,
) {
    restore_timer_with(storage, task_handle, after, true).await;
}

async fn restore_timer_with(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    after: Duration,
    if_idle: bool,
) {
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;

    let has_records = match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan notification outbox");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning notification outbox");
            return;
        }
    };

    if has_records {
        let event = if if_idle {
            Event::Task(
                task_handle
                    .schedule_idle_timer(TaskKey::DrainNotificationOutbox, after)
                    .await,
            )
        } else {
            task_handle
                .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::DrainNotificationOutbox,
                    after,
                }))
                .await
        };
        if let Event::Task(TaskEvent::Error { message, .. }) = event {
            warn!(message = %message, "Failed to restore notification outbox timer");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::fixtures::notifications::{record, temp_storage, user};
    use aruna_core::structs::{NotificationClass, notification_outbox_key};
    use aruna_tasks::InboundTaskHandler;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::{Semaphore, mpsc};

    struct RecordingHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    struct BlockingHandler {
        seen: mpsc::Sender<TaskKey>,
        release: Arc<Semaphore>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    #[async_trait]
    impl InboundTaskHandler for BlockingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
            let _permit = self
                .release
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore open");
        }
    }

    fn record_with_id(id: Ulid) -> NotificationOutboxRecord {
        NotificationOutboxRecord {
            outbox_id: id,
            record: record(user(1, 2), NotificationClass::Direct, 1_000),
        }
    }

    async fn write_outbox(storage: &StorageHandle, record: &NotificationOutboxRecord) {
        let value = ByteView::from(postcard::to_allocvec(record).expect("record serializes"));
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                key: notification_outbox_key(record.outbox_id),
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    async fn start_read_snapshot(storage: &StorageHandle) -> TxnId {
        match storage
            .send_storage_effect(StorageEffect::StartTransaction { read: true })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            other => panic!("unexpected transaction start event: {other:?}"),
        }
    }

    async fn close_read_snapshot(storage: &StorageHandle, txn_id: TxnId) {
        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {}
            other => panic!("unexpected transaction commit event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn batch_reads_fifo() {
        let (_dir, storage) = temp_storage();
        let records: Vec<NotificationOutboxRecord> = (1..=3u128)
            .map(|seq| record_with_id(Ulid::from_parts(seq as u64, 0)))
            .collect();
        for record in &records {
            write_outbox(&storage, record).await;
        }

        let batch = read_outbox_batch(&storage, None, 3, None)
            .await
            .expect("outbox read succeeds");
        assert!(!batch.has_more);
        let ids: Vec<Ulid> = batch.records.iter().map(|(_, r)| r.outbox_id).collect();
        assert_eq!(
            ids,
            vec![
                Ulid::from_parts(1, 0),
                Ulid::from_parts(2, 0),
                Ulid::from_parts(3, 0),
            ]
        );

        let partial = read_outbox_batch(&storage, None, 2, None)
            .await
            .expect("outbox read succeeds");
        assert!(partial.has_more);
        assert_eq!(partial.records.len(), 2);
    }

    #[tokio::test]
    async fn snapshot_excludes_appends() {
        let (_dir, storage) = temp_storage();
        let records: Vec<NotificationOutboxRecord> = (1..=3u128)
            .map(|seq| record_with_id(Ulid::from_parts(seq as u64, 0)))
            .collect();
        write_outbox(&storage, &records[0]).await;
        write_outbox(&storage, &records[1]).await;

        let txn_id = start_read_snapshot(&storage).await;
        let first = read_outbox_batch(&storage, None, 1, Some(txn_id))
            .await
            .expect("first snapshot page");
        assert_eq!(first.records[0].1, records[0]);
        assert!(first.has_more);

        write_outbox(&storage, &records[2]).await;

        let second = read_outbox_batch(&storage, first.next_start_after, 1, Some(txn_id))
            .await
            .expect("second snapshot page");
        assert_eq!(second.records[0].1, records[1]);
        assert!(!second.has_more);
        close_read_snapshot(&storage, txn_id).await;

        let appended = read_outbox_batch(&storage, second.next_start_after, 1, None)
            .await
            .expect("live page after snapshot");
        assert_eq!(appended.records[0].1, records[2]);
    }

    #[tokio::test]
    async fn cursor_skips_malformed() {
        let (_dir, storage) = temp_storage();
        let valid = record_with_id(Ulid::from_parts(2, 0));
        write_outbox(&storage, &valid).await;
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                key: notification_outbox_key(Ulid::from_parts(1, 0)),
                value: ByteView::from(vec![1, 2, 3]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }

        let txn_id = start_read_snapshot(&storage).await;
        let malformed = read_outbox_batch(&storage, None, 1, Some(txn_id))
            .await
            .expect("malformed snapshot page");
        assert!(malformed.records.is_empty());
        assert!(malformed.has_more);
        assert!(malformed.next_start_after.is_some());

        let next = read_outbox_batch(&storage, malformed.next_start_after, 1, Some(txn_id))
            .await
            .expect("snapshot page after malformed row");
        assert_eq!(next.records[0].1, valid);
        assert!(!next.has_more);
        close_read_snapshot(&storage, txn_id).await;
    }

    #[tokio::test]
    async fn malformed_record_deleted() {
        let (_dir, storage) = temp_storage();
        let valid = record_with_id(Ulid::from_parts(2, 0));
        write_outbox(&storage, &valid).await;
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                key: notification_outbox_key(Ulid::from_parts(1, 0)),
                value: ByteView::from(vec![1, 2, 3]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let batch = read_outbox_batch(&storage, None, NOTIFICATION_OUTBOX_DRAIN_BATCH_SIZE, None)
            .await
            .expect("outbox read succeeds");
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].1, valid);

        let stored = match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_OUTBOX_KEYSPACE.to_string(),
                key: notification_outbox_key(Ulid::from_parts(1, 0)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|v| v.to_vec()),
            other => panic!("unexpected read event: {other:?}"),
        };
        assert_eq!(stored, None);
    }

    async fn recording_task_handle() -> (TaskHandle, mpsc::Receiver<TaskKey>) {
        let task_handle = TaskHandle::new();
        let (seen_tx, seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler { seen: seen_tx }))
            .await;
        (task_handle, seen_rx)
    }

    #[tokio::test]
    async fn restore_arms_timer() {
        let (_dir, storage) = temp_storage();
        write_outbox(&storage, &record_with_id(Ulid::from_parts(1, 0))).await;
        let (task_handle, mut seen_rx) = recording_task_handle().await;

        restore_outbox_timer(&storage, &task_handle, Duration::ZERO).await;

        let key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("restored drain timer should fire")
            .expect("recording handler should receive timer key");
        assert_eq!(key, TaskKey::DrainNotificationOutbox);
    }

    #[tokio::test]
    async fn empty_restore_silent() {
        let (_dir, storage) = temp_storage();
        let (task_handle, mut seen_rx) = recording_task_handle().await;

        restore_outbox_timer(&storage, &task_handle, Duration::ZERO).await;

        // A zero-delay timer armed by restore is dispatched before this abort
        // command, so the running count witnesses whether a timer was armed.
        let TaskEvent::RunningHandlersAborted { count, .. } = task_handle
            .abort_running_handlers(TaskKey::DrainNotificationOutbox)
            .await
        else {
            panic!("expected running handler abort event");
        };
        assert_eq!(count, 0, "empty outbox must not arm a drain timer");
        assert!(
            seen_rx.try_recv().is_err(),
            "empty outbox must not invoke the drain handler"
        );
    }

    #[tokio::test]
    async fn restore_shortens_timer() {
        let (_dir, storage) = temp_storage();
        write_outbox(&storage, &record_with_id(Ulid::from_parts(1, 0))).await;
        let task_handle = TaskHandle::new();

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::from_secs(3600),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };

        restore_outbox_timer(&storage, &task_handle, Duration::from_secs(7200)).await;

        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::from_secs(10_000),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        assert!(
            after <= Duration::from_secs(3600),
            "restore must ShortenTimer, not ResetTimer to the later deadline"
        );
    }

    #[tokio::test]
    async fn running_drain_unchanged() {
        let (_dir, storage) = temp_storage();
        write_outbox(&storage, &record_with_id(Ulid::from_parts(1, 0))).await;
        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(2);
        let release = Arc::new(Semaphore::new(0));
        task_handle
            .set_inbound_handler(Arc::new(BlockingHandler {
                seen: seen_tx,
                release: release.clone(),
            }))
            .await;

        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::ZERO,
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        let first = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("running drain should start")
            .expect("handler should send first key");
        assert_eq!(first, TaskKey::DrainNotificationOutbox);

        restore_idle_timer(&storage, &task_handle, Duration::from_secs(10)).await;

        // A timer left by restore_if_idle would report its ~10s deadline here;
        // a running drain returns the 3600s requested by the probe instead.
        let TaskEvent::TimerScheduled { after, .. } = task_handle
            .schedule_idle_timer(TaskKey::DrainNotificationOutbox, Duration::from_secs(3600))
            .await
        else {
            panic!("expected timer scheduled");
        };
        assert!(
            after > Duration::from_secs(3000),
            "restore_if_idle must not arm a drain timer while the drain runs"
        );
        release.add_permits(1);
    }
}
