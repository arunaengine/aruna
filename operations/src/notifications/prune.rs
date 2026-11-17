//! Removes expired inbox notifications and enforces the transient per-user cap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::notifications::{context, record, temp_storage, user};
    use aruna_core::UserId;
    use aruna_core::storage_entries::{inbox_update_entry, inbox_write_entries};
    use aruna_tasks::InboundTaskHandler;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    const DAY_MS: u64 = 24 * 60 * 60 * 1000;

    struct RecordingHandler {
        seen: mpsc::Sender<TaskKey>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            let _ = self.seen.send(key).await;
        }
    }

    /// Writes rows directly so fixtures can exceed the upsert-time transient cap.
    async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
        let writes = records
            .iter()
            .flat_map(|record| inbox_write_entries(record).expect("write entries"))
            .collect();
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected seed write event: {other:?}"),
        }
    }

    async fn write_primary_only(storage: &StorageHandle, record: &NotificationRecord) {
        let (key_space, key, value) = inbox_update_entry(record).expect("update entry");
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    async fn write_index_only(storage: &StorageHandle, record: &NotificationRecord) {
        let (key_space, key, value) = inbox_write_entries(record)
            .expect("write entries")
            .into_iter()
            .nth(1)
            .expect("prune index entry");
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }
    }

    async fn mark_read(storage: &StorageHandle, record: &NotificationRecord, read_at_ms: u64) {
        let mut read = record.clone();
        read.read_at_ms = Some(read_at_ms);
        write_primary_only(storage, &read).await;
    }

    async fn key_exists(storage: &StorageHandle, key_space: &str, key: Key) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    async fn primary_exists(storage: &StorageHandle, record: &NotificationRecord) -> bool {
        key_exists(
            storage,
            NOTIFICATION_INBOX_KEYSPACE,
            notification_inbox_key(
                record.recipient,
                record.created_at_ms,
                record.notification_id,
            ),
        )
        .await
    }

    async fn index_exists(storage: &StorageHandle, record: &NotificationRecord) -> bool {
        key_exists(
            storage,
            PRUNE_INDEX_KEYSPACE,
            notification_prune_key(record),
        )
        .await
    }

    async fn records_for(storage: &StorageHandle, recipient: UserId) -> Vec<NotificationRecord> {
        let prefix = recipient.to_storage_key();
        let mut out = Vec::new();
        let mut start_after: Option<Key> = None;
        loop {
            let (values, next) = iter_page(
                storage,
                NOTIFICATION_INBOX_KEYSPACE,
                start_after.take(),
                512,
            )
            .await
            .expect("iter page");
            if values.is_empty() {
                break;
            }
            for (key, value) in &values {
                if key.as_ref().starts_with(prefix.as_slice()) {
                    out.push(NotificationRecord::from_bytes(value.as_ref()).expect("decodes"));
                }
            }
            match next {
                Some(next) => start_after = Some(next),
                None => break,
            }
        }
        out
    }

    async fn probe_shorten_after(task_handle: &TaskHandle, after: Duration) -> Duration {
        match task_handle
            .send_effect(Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::PruneNotifications,
                after,
            }))
            .await
        {
            Event::Task(TaskEvent::TimerScheduled { after, .. }) => after,
            other => panic!("unexpected task event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn direct_records_pruned() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        let live = record(recipient, NotificationClass::Direct, now - DAY_MS);
        seed(&storage, &[expired.clone(), live.clone()]).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &expired).await);
        assert!(!index_exists(&storage, &expired).await);
        assert!(primary_exists(&storage, &live).await);
        assert!(index_exists(&storage, &live).await);
    }

    #[tokio::test]
    async fn outlived_cutoffs_pruned() {
        // A deletion time goes once every notice it could drop has expired.
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let row = |seed: u8, at: u64| {
            let key = vec![seed; 4].into();
            let value = at.to_be_bytes().to_vec().into();
            (NOTIFICATION_CUTOFF_KEYSPACE.to_string(), key, value)
        };
        let writes = vec![row(1, now - 91 * DAY_MS), row(2, now - DAY_MS)];
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        storage.send_storage_effect(write).await;

        process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        let key = |seed: u8| vec![seed; 4].into();
        assert!(!key_exists(&storage, NOTIFICATION_CUTOFF_KEYSPACE, key(1)).await);
        assert!(key_exists(&storage, NOTIFICATION_CUTOFF_KEYSPACE, key(2)).await);

        // A deletion renews a time the sweep saw expired: the renewed time stays.
        let renewed = StorageEffect::BatchWrite {
            writes: vec![row(3, now)],
            txn_id: None,
        };
        storage.send_storage_effect(renewed).await;
        delete_expired(&storage, vec![key(3)], now).await.unwrap();
        assert!(key_exists(&storage, NOTIFICATION_CUTOFF_KEYSPACE, key(3)).await);
    }

    #[tokio::test]
    async fn transient_records_pruned() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let transient = record(recipient, NotificationClass::Transient, now - 31 * DAY_MS);
        let direct = record(recipient, NotificationClass::Direct, now - 31 * DAY_MS);
        seed(&storage, &[transient.clone(), direct.clone()]).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &transient).await);
        assert!(primary_exists(&storage, &direct).await);
        assert!(index_exists(&storage, &direct).await);
    }

    #[tokio::test]
    async fn read_records_expire() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        seed(&storage, std::slice::from_ref(&expired)).await;
        mark_read(&storage, &expired, now).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &expired).await);
        assert!(!index_exists(&storage, &expired).await);
    }

    #[tokio::test]
    async fn future_records_due() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let soon = record(recipient, NotificationClass::Transient, now - 20 * DAY_MS);
        let later = record(recipient, NotificationClass::Direct, now);
        seed(&storage, &[soon.clone(), later]).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 0);
        assert!(!outcome.has_more);
        let next = outcome.next_due_after.expect("next due present");
        let expected = Duration::from_millis(soon.expires_at_ms() - now);
        assert!(next <= expected);
        assert!(expected.saturating_sub(next) <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn orphan_indexes_deleted() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let orphan = record(user(1, 1), NotificationClass::Direct, now - 91 * DAY_MS);
        write_index_only(&storage, &orphan).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 0);
        assert!(!index_exists(&storage, &orphan).await);
    }

    #[tokio::test]
    async fn transient_cap_keeps() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let mut records = Vec::with_capacity(510);
        for i in 0..510u64 {
            records.push(record(
                recipient,
                NotificationClass::Transient,
                now - 1_000_000 + i * 1000,
            ));
        }
        seed(&storage, &records).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.capped, 10);
        assert_eq!(outcome.expired, 0);
        for r in &records[..10] {
            assert!(!primary_exists(&storage, r).await);
            assert!(!index_exists(&storage, r).await);
        }
        for r in &records[10..] {
            assert!(primary_exists(&storage, r).await);
        }
    }

    #[tokio::test]
    async fn transient_cap_scoped() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let user_a = user(1, 1);
        let user_b = user(1, 2);

        let mut a_records = Vec::new();
        for i in 0..501u64 {
            a_records.push(record(
                user_a,
                NotificationClass::Transient,
                now - 1_000_000 + i * 1000,
            ));
        }
        for j in 0..5u64 {
            a_records.push(record(
                user_a,
                NotificationClass::Direct,
                now - 2_000_000 + j * 1000,
            ));
        }
        let mut b_records = Vec::new();
        for i in 0..499u64 {
            b_records.push(record(
                user_b,
                NotificationClass::Transient,
                now - 1_000_000 + i * 1000,
            ));
        }
        seed(&storage, &a_records).await;
        seed(&storage, &b_records).await;

        let outcome = process_prune_page(&context(&storage), 64)
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.capped, 1);
        assert_eq!(outcome.expired, 0);

        let a = records_for(&storage, user_a).await;
        let transient_a = a
            .iter()
            .filter(|r| r.class == NotificationClass::Transient)
            .count();
        let direct_a = a
            .iter()
            .filter(|r| r.class == NotificationClass::Direct)
            .count();
        assert_eq!(transient_a, 500);
        assert_eq!(direct_a, 5);
        assert_eq!(records_for(&storage, user_b).await.len(), 499);
    }

    #[tokio::test]
    async fn orphan_primary_reclaimed() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        write_primary_only(&storage, &expired).await;

        let outcome = process_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &expired).await);
    }

    #[tokio::test]
    async fn prune_is_idempotent() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        let live = record(recipient, NotificationClass::Direct, now - DAY_MS);
        seed(&storage, &[expired, live]).await;

        let first = process_prune_batch(&context(&storage))
            .await
            .expect("first prune succeeds");
        assert_eq!(first.expired, 1);

        let second = process_prune_batch(&context(&storage))
            .await
            .expect("second prune succeeds");
        assert_eq!(second.expired, 0);
        assert_eq!(second.capped, 0);
    }

    #[tokio::test]
    async fn due_restore_immediate() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let due = record(user(1, 1), NotificationClass::Direct, now - 91 * DAY_MS);
        write_index_only(&storage, &due).await;

        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler { seen: seen_tx }))
            .await;

        restore_prune_timer(&storage, &task_handle).await;

        let key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("due restore should fire immediately")
            .expect("recording handler receives key");
        assert_eq!(key, TaskKey::PruneNotifications);
    }

    #[tokio::test]
    async fn empty_restore_polls() {
        let (_dir, storage) = temp_storage();
        let task_handle = TaskHandle::new();

        restore_prune_timer(&storage, &task_handle).await;

        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(after <= NOTIFICATION_POLL_AFTER);
        assert!(NOTIFICATION_POLL_AFTER.saturating_sub(after) <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn future_restore_scheduled() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let future = record(user(1, 1), NotificationClass::Transient, now);
        write_index_only(&storage, &future).await;

        let task_handle = TaskHandle::new();
        restore_prune_timer(&storage, &task_handle).await;
        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(after <= NOTIFICATION_POLL_AFTER);
        assert!(NOTIFICATION_POLL_AFTER.saturating_sub(after) <= Duration::from_secs(5));

        let task_handle = TaskHandle::new();
        let Event::Task(TaskEvent::TimerScheduled { .. }) = task_handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::PruneNotifications,
                after: Duration::from_secs(10),
            }))
            .await
        else {
            panic!("expected timer scheduled");
        };
        restore_prune_timer(&storage, &task_handle).await;
        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(
            after <= Duration::from_secs(10),
            "restore must ShortenTimer, not ResetTimer to the later POLL deadline"
        );
    }
}
