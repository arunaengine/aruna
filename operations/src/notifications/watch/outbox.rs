use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NOTIFICATION_WATCH_OUTBOX_KEYSPACE;
use aruna_core::structs::{WatchEvent, WatchForwardOutboxRecord};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

pub const WATCH_FORWARD_OUTBOX_DRAIN_BATCH_SIZE: usize = 512;
pub const WATCH_FORWARD_DELIVERY_RETRY_AFTER: Duration = Duration::from_secs(30);
pub const WATCH_FORWARD_OUTBOX_RETENTION_MS: u64 = 48 * 60 * 60 * 1000;

pub fn new_watch_forward_outbox_record(
    holder_node: NodeId,
    event: WatchEvent,
) -> WatchForwardOutboxRecord {
    WatchForwardOutboxRecord {
        outbox_id: Ulid::new(),
        holder_node,
        event,
    }
}

pub fn schedule_watch_forward_outbox_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainNotificationWatchOutbox,
        after: Duration::ZERO,
    })
}

pub struct WatchForwardOutboxBatch {
    pub records: Vec<(Vec<u8>, WatchForwardOutboxRecord)>,
    pub has_more: bool,
}

pub async fn read_watch_forward_outbox_batch(
    storage: &StorageHandle,
    start_after: Option<Vec<u8>>,
    limit: usize,
) -> Result<WatchForwardOutboxBatch, String> {
    let read_limit = limit.saturating_add(1);
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(|key| IterStart::After(ByteView::from(key))),
            limit: read_limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let has_more = values.len() > limit;
            let mut records = Vec::with_capacity(values.len().min(limit));
            for (key, value) in values.into_iter().take(limit) {
                match postcard::from_bytes(&value) {
                    Ok(record) => records.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed watch forward outbox record");
                        delete_watch_forward_outbox_records(storage, vec![key]).await?;
                    }
                }
            }
            Ok(WatchForwardOutboxBatch { records, has_more })
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

pub async fn delete_watch_forward_outbox_records(
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
                NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
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

// ShortenTimer, never ResetTimer: the durable-queue re-arm loop calls this too,
// and a ResetTimer there would clobber a pending 30s retry deadline back to `after`.
pub async fn restore_watch_forward_outbox_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    after: Duration,
) {
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;

    let has_records = match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan watch forward outbox");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning watch forward outbox");
            return;
        }
    };

    if has_records {
        let event = task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::DrainNotificationWatchOutbox,
                after,
            }))
            .await;
        if let Event::Task(TaskEvent::Error { message, .. }) = event {
            warn!(message = %message, "Failed to restore watch forward outbox timer");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{
        RealmId, WatchEventDetail, WatchEventKind, watch_forward_outbox_key,
    };
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn make_event() -> WatchEvent {
        WatchEvent {
            event_id: Ulid::new(),
            realm_id: RealmId([1u8; 32]),
            kind: WatchEventKind::DataUploaded,
            path: "bucket/object".to_string(),
            actor: UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            occurred_at_ms: 1_000,
            detail: WatchEventDetail::DataUploaded {
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                size_bytes: 4,
            },
        }
    }

    fn outbox_record_with_id(id: Ulid) -> WatchForwardOutboxRecord {
        WatchForwardOutboxRecord {
            outbox_id: id,
            holder_node: node(3),
            event: make_event(),
        }
    }

    async fn write_outbox(storage: &StorageHandle, record: &WatchForwardOutboxRecord) {
        let value = ByteView::from(record.to_bytes().expect("record serializes"));
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
                key: watch_forward_outbox_key(record.outbox_id),
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected outbox write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn outbox_batch_reads_fifo() {
        let (_dir, storage) = temp_storage();
        let records: Vec<WatchForwardOutboxRecord> = (1..=3u64)
            .map(|seq| outbox_record_with_id(Ulid::from_parts(seq, 0)))
            .collect();
        for record in &records {
            write_outbox(&storage, record).await;
        }

        let batch = read_watch_forward_outbox_batch(&storage, None, 3)
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

        let partial = read_watch_forward_outbox_batch(&storage, None, 2)
            .await
            .expect("outbox read succeeds");
        assert!(partial.has_more);
        assert_eq!(partial.records.len(), 2);
    }

    #[tokio::test]
    async fn malformed_outbox_record_is_deleted() {
        let (_dir, storage) = temp_storage();
        let valid = outbox_record_with_id(Ulid::from_parts(2, 0));
        write_outbox(&storage, &valid).await;
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NOTIFICATION_WATCH_OUTBOX_KEYSPACE.to_string(),
                key: watch_forward_outbox_key(Ulid::from_parts(1, 0)),
                value: ByteView::from(vec![1, 2, 3]),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let batch =
            read_watch_forward_outbox_batch(&storage, None, WATCH_FORWARD_OUTBOX_DRAIN_BATCH_SIZE)
                .await
                .expect("outbox read succeeds");
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].1, valid);
    }
}
