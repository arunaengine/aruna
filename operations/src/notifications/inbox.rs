use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::NOTIFICATION_INBOX_KEYSPACE;
use aruna_core::storage_entries::notification_inbox_write_entries;
use aruna_core::structs::{NotificationRecord, notification_inbox_key};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use aruna_storage::StorageHandle;

enum UpsertFailure {
    Conflict,
    Fatal(String),
}

/// Idempotent holder-local upsert. Records whose primary key already exists are
/// skipped inside the write transaction, so a duplicate delivery never clobbers
/// a read_at_ms set by a concurrent mark-read. Returns the number of newly
/// written records.
pub async fn upsert_inbox_records(
    storage: &StorageHandle,
    records: &[NotificationRecord],
) -> Result<usize, String> {
    if records.is_empty() {
        return Ok(0);
    }
    match upsert_once(storage, records).await {
        Ok(written) => Ok(written),
        Err(UpsertFailure::Fatal(error)) => Err(error),
        Err(UpsertFailure::Conflict) => match upsert_once(storage, records).await {
            Ok(written) => Ok(written),
            Err(UpsertFailure::Fatal(error)) => Err(error),
            Err(UpsertFailure::Conflict) => {
                Err("notification inbox upsert conflicted twice".to_string())
            }
        },
    }
}

async fn upsert_once(
    storage: &StorageHandle,
    records: &[NotificationRecord],
) -> Result<usize, UpsertFailure> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let reads: Vec<(KeySpace, Key)> = records
        .iter()
        .map(|record| {
            (
                NOTIFICATION_INBOX_KEYSPACE.to_string(),
                notification_inbox_key(
                    record.recipient,
                    record.created_at_ms,
                    record.notification_id,
                ),
            )
        })
        .collect();

    let existing = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(abort_and_classify(storage, txn_id, error).await);
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let mut writes: Vec<(KeySpace, Key, Value)> = Vec::new();
    let mut new_count = 0usize;
    for (record, (_, existing_value)) in records.iter().zip(existing) {
        if existing_value.is_some() {
            continue;
        }
        match notification_inbox_write_entries(record) {
            Ok(entries) => {
                writes.extend(entries);
                new_count += 1;
            }
            Err(error) => {
                abort_txn(storage, txn_id).await;
                return Err(UpsertFailure::Fatal(error.to_string()));
            }
        }
    }

    if writes.is_empty() {
        abort_txn(storage, txn_id).await;
        return Ok(0);
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(abort_and_classify(storage, txn_id, error).await);
        }
        other => {
            abort_txn(storage, txn_id).await;
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(new_count),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

fn classify(error: StorageError) -> UpsertFailure {
    if matches!(error, StorageError::TransactionConflict) {
        UpsertFailure::Conflict
    } else {
        UpsertFailure::Fatal(error.to_string())
    }
}

async fn abort_txn(storage: &StorageHandle, txn_id: TxnId) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn abort_and_classify(
    storage: &StorageHandle,
    txn_id: TxnId,
    error: StorageError,
) -> UpsertFailure {
    abort_txn(storage, txn_id).await;
    classify(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{
        NOTIFICATION_INBOX_KEYSPACE, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE,
    };
    use aruna_core::storage_entries::notification_inbox_update_entry;
    use aruna_core::structs::{NotificationClass, NotificationKind, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        (dir, storage)
    }

    fn make_record() -> NotificationRecord {
        NotificationRecord::new(
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            NotificationClass::Direct,
            NotificationKind::AddedToGroup {
                group_id: Ulid::from_bytes([9u8; 16]),
                actor_user_id: UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([1u8; 32])),
            },
            1_000,
        )
    }

    async fn read_primary(storage: &StorageHandle, record: &NotificationRecord) -> Option<Vec<u8>> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                key: notification_inbox_key(
                    record.recipient,
                    record.created_at_ms,
                    record.notification_id,
                ),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|v| v.to_vec()),
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    async fn count_keyspace(storage: &StorageHandle, key_space: &str) -> usize {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iter event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn upsert_writes_primary_and_prune_index() {
        let (_dir, storage) = temp_storage();
        let record = make_record();

        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(1)
        );

        let stored = read_primary(&storage, &record).await.expect("primary row");
        assert_eq!(
            NotificationRecord::from_bytes(&stored).expect("decodes"),
            record
        );
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_PRUNE_INDEX_KEYSPACE).await,
            1
        );
    }

    #[tokio::test]
    async fn duplicate_upsert_is_noop() {
        let (_dir, storage) = temp_storage();
        let record = make_record();

        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(1)
        );
        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(0)
        );
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_KEYSPACE).await,
            1
        );
    }

    #[tokio::test]
    async fn duplicate_upsert_preserves_read_state() {
        let (_dir, storage) = temp_storage();
        let record = make_record();
        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(1)
        );

        let mut read_marked = record.clone();
        read_marked.read_at_ms = Some(5);
        let (key_space, key, value) =
            notification_inbox_update_entry(&read_marked).expect("update entry");
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

        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(0)
        );

        let stored = read_primary(&storage, &record).await.expect("primary row");
        assert_eq!(
            NotificationRecord::from_bytes(&stored)
                .expect("decodes")
                .read_at_ms,
            Some(5)
        );
    }
}
