use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE};
use aruna_core::structs::{
    NOTIFICATION_DIRECT_TTL_MS, NOTIFICATION_TRANSIENT_PER_USER_CAP, NOTIFICATION_TRANSIENT_TTL_MS,
    NotificationClass, NotificationRecord, notification_inbox_key, notification_prune_index_key,
    parse_notification_prune_index_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::driver::DriverContext;

pub const NOTIFICATION_PRUNE_SCAN_PAGE_SIZE: usize = 512;
pub const NOTIFICATION_PRUNE_POLL_AFTER: Duration = Duration::from_secs(60 * 60);
pub const NOTIFICATION_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(30);

pub fn schedule_notification_prune_effect(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::PruneNotifications,
        after,
    })
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct NotificationPruneOutcome {
    pub expired: usize,
    pub capped: usize,
    pub has_more: bool,
    pub next_due_after: Option<Duration>,
}

pub async fn restore_notification_prune_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let after = match first_prune_index_arm_after(storage).await {
        Ok(after) => after,
        Err(error) => {
            warn!(error = %error, "Failed to scan notification prune index");
            return;
        }
    };
    // ShortenTimer, never ResetTimer: this restore runs both at startup and inside the
    // 5s durable-queue re-arm loop, where a ResetTimer would replace the handler's
    // post-run re-arm every 5 seconds and push the deadline forward forever.
    let event = task_handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::PruneNotifications,
            after,
        }))
        .await;
    if let Event::Task(TaskEvent::Error { message, .. }) = event {
        warn!(message = %message, "Failed to restore notification prune timer");
    }
}

pub async fn process_notification_prune_batch(
    context: &DriverContext,
) -> Result<NotificationPruneOutcome, String> {
    process_notification_prune_batch_with_page_size(context, NOTIFICATION_PRUNE_SCAN_PAGE_SIZE)
        .await
}

pub(crate) async fn process_notification_prune_batch_with_page_size(
    context: &DriverContext,
    page_size: usize,
) -> Result<NotificationPruneOutcome, String> {
    let storage = &context.storage_handle;
    let now_ms = unix_timestamp_millis();
    let phase_a = prune_expired_index_rows(storage, now_ms, page_size).await?;
    let phase_b = sweep_primary_keyspace(storage, now_ms, page_size).await?;
    Ok(NotificationPruneOutcome {
        expired: phase_a.expired.saturating_add(phase_b.expired),
        capped: phase_b.capped,
        has_more: phase_a.has_more,
        next_due_after: phase_a.next_due_after,
    })
}

struct PhaseAOutcome {
    expired: usize,
    has_more: bool,
    next_due_after: Option<Duration>,
}

async fn prune_expired_index_rows(
    storage: &StorageHandle,
    now_ms: u64,
    page_size: usize,
) -> Result<PhaseAOutcome, String> {
    let deletion_cap = page_size.saturating_mul(4);
    let mut deletes: Vec<(KeySpace, Key)> = Vec::new();
    let mut expired = 0usize;
    let mut processed = 0usize;
    let mut has_more = false;
    let mut next_due_after: Option<Duration> = None;
    let mut start_after: Option<Key> = None;

    'scan: loop {
        let (values, next_start_after) = iter_page(
            storage,
            NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE,
            start_after.take(),
            page_size,
        )
        .await?;
        if values.is_empty() {
            break;
        }
        for (key, _) in values {
            let (expires_at_ms, recipient, notification_id) =
                match parse_notification_prune_index_key(key.as_ref()) {
                    Ok(parsed) => parsed,
                    Err(error) => {
                        let raw = key.to_vec();
                        warn!(error = %error, key = ?raw, "Deleting malformed notification prune index row");
                        deletes.push((NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(), key));
                        continue;
                    }
                };
            if expires_at_ms > now_ms {
                next_due_after = Some(Duration::from_millis(expires_at_ms.saturating_sub(now_ms)));
                break 'scan;
            }
            // created_at_ms is not stored in the index key, but equals
            // expires_at_ms - class.ttl_ms(); the two class TTLs differ, so at most one
            // candidate can hold a real record (notification_id is unique per record). A
            // saturated expires_at_ms (u64::MAX) yields candidates that cannot match the
            // stored created_at_ms and would fall into the orphan arm, but such a row is
            // always > now, so phase A stops before reaching it.
            let direct_key = notification_inbox_key(
                recipient,
                expires_at_ms.saturating_sub(NOTIFICATION_DIRECT_TTL_MS),
                notification_id,
            );
            let transient_key = notification_inbox_key(
                recipient,
                expires_at_ms.saturating_sub(NOTIFICATION_TRANSIENT_TTL_MS),
                notification_id,
            );
            let read = batch_read(
                storage,
                vec![
                    (NOTIFICATION_INBOX_KEYSPACE.to_string(), direct_key.clone()),
                    (
                        NOTIFICATION_INBOX_KEYSPACE.to_string(),
                        transient_key.clone(),
                    ),
                ],
            )
            .await?;
            let direct_exists = read.first().is_some_and(|(_, value)| value.is_some());
            let transient_exists = read.get(1).is_some_and(|(_, value)| value.is_some());
            deletes.push((NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(), key));
            if direct_exists {
                deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), direct_key));
                expired = expired.saturating_add(1);
            } else if transient_exists {
                deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), transient_key));
                expired = expired.saturating_add(1);
            } else {
                warn!(recipient = %recipient, notification_id = %notification_id, "Deleting orphan notification prune index row");
            }
            processed = processed.saturating_add(1);
            if processed >= deletion_cap {
                has_more = true;
                break 'scan;
            }
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    batch_delete(storage, deletes).await?;
    Ok(PhaseAOutcome {
        expired,
        has_more,
        next_due_after,
    })
}

struct PhaseBOutcome {
    expired: usize,
    capped: usize,
}

async fn sweep_primary_keyspace(
    storage: &StorageHandle,
    now_ms: u64,
    page_size: usize,
) -> Result<PhaseBOutcome, String> {
    let mut expired = 0usize;
    let mut capped = 0usize;
    let mut current_recipient: Option<Vec<u8>> = None;
    let mut transient_seen = 0usize;
    let mut start_after: Option<Key> = None;

    loop {
        let (values, next_start_after) = iter_page(
            storage,
            NOTIFICATION_INBOX_KEYSPACE,
            start_after.take(),
            page_size,
        )
        .await?;
        if values.is_empty() {
            break;
        }
        let mut deletes: Vec<(KeySpace, Key)> = Vec::new();
        for (key, value) in values {
            if key.len() != 72 {
                let raw = key.to_vec();
                warn!(key = ?raw, "Skipping malformed notification inbox key during prune sweep");
                continue;
            }
            let recipient_prefix = &key.as_ref()[..48];
            if current_recipient.as_deref() != Some(recipient_prefix) {
                current_recipient = Some(recipient_prefix.to_vec());
                transient_seen = 0;
            }
            let record = match NotificationRecord::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(error) => {
                    let raw = key.to_vec();
                    warn!(error = %error, key = ?raw, "Skipping malformed notification inbox record during prune sweep");
                    continue;
                }
            };
            if record.expires_at_ms() <= now_ms {
                deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), key));
                deletes.push((
                    NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(),
                    notification_prune_index_key(&record),
                ));
                expired = expired.saturating_add(1);
                continue;
            }
            if record.class == NotificationClass::Transient {
                transient_seen = transient_seen.saturating_add(1);
                if transient_seen > NOTIFICATION_TRANSIENT_PER_USER_CAP {
                    deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), key));
                    deletes.push((
                        NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(),
                        notification_prune_index_key(&record),
                    ));
                    capped = capped.saturating_add(1);
                }
            }
        }
        batch_delete(storage, deletes).await?;
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    Ok(PhaseBOutcome { expired, capped })
}

async fn first_prune_index_arm_after(storage: &StorageHandle) -> Result<Duration, String> {
    let (values, _) = iter_page(storage, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE, None, 1).await?;
    let Some((key, _)) = values.into_iter().next() else {
        return Ok(NOTIFICATION_PRUNE_POLL_AFTER);
    };
    match parse_notification_prune_index_key(key.as_ref()) {
        Ok((expires_at_ms, _, _)) => {
            let due_after =
                Duration::from_millis(expires_at_ms.saturating_sub(unix_timestamp_millis()));
            Ok(due_after.min(NOTIFICATION_PRUNE_POLL_AFTER))
        }
        Err(error) => {
            let raw = key.to_vec();
            warn!(error = %error, key = ?raw, "Deleting malformed notification prune index row during restore");
            batch_delete(
                storage,
                vec![(NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE.to_string(), key)],
            )
            .await?;
            Ok(Duration::ZERO)
        }
    }
}

async fn iter_page(
    storage: &StorageHandle,
    key_space: &str,
    start_after: Option<Key>,
    limit: usize,
) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn batch_read(
    storage: &StorageHandle,
    reads: Vec<(KeySpace, Key)>,
) -> Result<Vec<(Key, Option<Value>)>, String> {
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

async fn batch_delete(
    storage: &StorageHandle,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::storage_entries::{
        notification_inbox_update_entry, notification_inbox_write_entries,
    };
    use aruna_core::structs::{NotificationKind, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use aruna_tasks::InboundTaskHandler;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    use crate::notifications::inbox::upsert_inbox_records;

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

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn context(storage: &StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn user(realm: u8, u: u8) -> UserId {
        UserId::new(Ulid::from_bytes([u; 16]), RealmId([realm; 32]))
    }

    fn record(
        recipient: UserId,
        class: NotificationClass,
        created_at_ms: u64,
    ) -> NotificationRecord {
        NotificationRecord::new(
            recipient,
            class,
            NotificationKind::AddedToGroup {
                group_id: Ulid::r#gen(),
                actor_user_id: user(1, 200),
            },
            created_at_ms,
        )
    }

    async fn seed(storage: &StorageHandle, records: &[NotificationRecord]) {
        upsert_inbox_records(storage, records)
            .await
            .expect("seed upsert succeeds");
    }

    async fn write_primary_only(storage: &StorageHandle, record: &NotificationRecord) {
        let (key_space, key, value) =
            notification_inbox_update_entry(record).expect("update entry");
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
        let (key_space, key, value) = notification_inbox_write_entries(record)
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
            NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE,
            notification_prune_index_key(record),
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
    async fn expired_direct_records_are_pruned() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        let live = record(recipient, NotificationClass::Direct, now - DAY_MS);
        seed(&storage, &[expired.clone(), live.clone()]).await;

        let outcome = process_notification_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &expired).await);
        assert!(!index_exists(&storage, &expired).await);
        assert!(primary_exists(&storage, &live).await);
        assert!(index_exists(&storage, &live).await);
    }

    #[tokio::test]
    async fn expired_transient_records_are_pruned_at_30d() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let transient = record(recipient, NotificationClass::Transient, now - 31 * DAY_MS);
        let direct = record(recipient, NotificationClass::Direct, now - 31 * DAY_MS);
        seed(&storage, &[transient.clone(), direct.clone()]).await;

        let outcome = process_notification_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &transient).await);
        assert!(primary_exists(&storage, &direct).await);
        assert!(index_exists(&storage, &direct).await);
    }

    #[tokio::test]
    async fn read_records_expire_like_unread() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        seed(&storage, std::slice::from_ref(&expired)).await;
        mark_read(&storage, &expired, now).await;

        let outcome = process_notification_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 1);
        assert!(!primary_exists(&storage, &expired).await);
        assert!(!index_exists(&storage, &expired).await);
    }

    #[tokio::test]
    async fn future_records_yield_next_due() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let soon = record(recipient, NotificationClass::Transient, now - 20 * DAY_MS);
        let later = record(recipient, NotificationClass::Direct, now);
        seed(&storage, &[soon.clone(), later]).await;

        let outcome = process_notification_prune_batch(&context(&storage))
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
    async fn orphan_index_rows_are_deleted_with_warn() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let orphan = record(user(1, 1), NotificationClass::Direct, now - 91 * DAY_MS);
        write_index_only(&storage, &orphan).await;

        let outcome = process_notification_prune_batch(&context(&storage))
            .await
            .expect("prune succeeds");

        assert_eq!(outcome.expired, 0);
        assert!(!index_exists(&storage, &orphan).await);
    }

    #[tokio::test]
    async fn transient_cap_keeps_newest_500() {
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

        let outcome = process_notification_prune_batch(&context(&storage))
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
    async fn transient_cap_is_per_user_and_ignores_direct() {
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

        let outcome = process_notification_prune_batch_with_page_size(&context(&storage), 64)
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
    async fn phase_b_reclaims_expired_primary_without_index() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let recipient = user(1, 1);
        let expired = record(recipient, NotificationClass::Direct, now - 91 * DAY_MS);
        write_primary_only(&storage, &expired).await;

        let outcome = process_notification_prune_batch(&context(&storage))
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

        let first = process_notification_prune_batch(&context(&storage))
            .await
            .expect("first prune succeeds");
        assert_eq!(first.expired, 1);

        let second = process_notification_prune_batch(&context(&storage))
            .await
            .expect("second prune succeeds");
        assert_eq!(second.expired, 0);
        assert_eq!(second.capped, 0);
    }

    #[tokio::test]
    async fn restore_arms_zero_when_due() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let due = record(user(1, 1), NotificationClass::Direct, now - 91 * DAY_MS);
        write_index_only(&storage, &due).await;

        let task_handle = TaskHandle::new();
        let (seen_tx, mut seen_rx) = mpsc::channel(1);
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler { seen: seen_tx }))
            .await;

        restore_notification_prune_timer(&storage, &task_handle).await;

        let key = tokio::time::timeout(Duration::from_secs(1), seen_rx.recv())
            .await
            .expect("due restore should fire immediately")
            .expect("recording handler receives key");
        assert_eq!(key, TaskKey::PruneNotifications);
    }

    #[tokio::test]
    async fn restore_arms_poll_when_empty() {
        let (_dir, storage) = temp_storage();
        let task_handle = TaskHandle::new();

        restore_notification_prune_timer(&storage, &task_handle).await;

        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(after <= NOTIFICATION_PRUNE_POLL_AFTER);
        assert!(NOTIFICATION_PRUNE_POLL_AFTER.saturating_sub(after) <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn restore_arms_due_after_for_future() {
        let (_dir, storage) = temp_storage();
        let now = unix_timestamp_millis();
        let future = record(user(1, 1), NotificationClass::Transient, now);
        write_index_only(&storage, &future).await;

        let task_handle = TaskHandle::new();
        restore_notification_prune_timer(&storage, &task_handle).await;
        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(after <= NOTIFICATION_PRUNE_POLL_AFTER);
        assert!(NOTIFICATION_PRUNE_POLL_AFTER.saturating_sub(after) <= Duration::from_secs(5));

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
        restore_notification_prune_timer(&storage, &task_handle).await;
        let after = probe_shorten_after(&task_handle, Duration::from_secs(2 * 60 * 60)).await;
        assert!(
            after <= Duration::from_secs(10),
            "restore must ShortenTimer, not ResetTimer to the later POLL deadline"
        );
    }
}
