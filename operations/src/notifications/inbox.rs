//! Upserts notification records into the holder inbox and reports new recipients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::effects::IterStart;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_CUTOFF_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
use aruna_core::storage_entries::{inbox_delete_entries, inbox_write_entries};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord, TRANSIENT_USER_CAP,
    notification_cutoff_key, notification_inbox_key, notification_inbox_prefix,
};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use aruna_storage::StorageHandle;

#[derive(Debug)]
pub(crate) enum UpsertFailure {
    Conflict,
    Fatal(String),
}

/// Result of a holder-local inbox upsert: `recipients` lists, in first-seen order, those that
/// gained or lost a record, so a caller wakes each once; a pure redelivery reports zero writes.
/// `purged` counts records a bucket deletion removed, `cutoffs` the deletion times it stored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InboxWriteOutcome {
    pub written: usize,
    pub recipients: Vec<UserId>,
    pub purged: usize,
    pub cutoffs: usize,
}

/// Idempotent holder-local upsert: existing primary keys are skipped inside the
/// write transaction, so a duplicate never clobbers a concurrent mark-read's
/// `read_at_ms`. Returns the number of newly written records.
pub async fn upsert_inbox_records(
    storage: &StorageHandle,
    records: &[NotificationRecord],
) -> Result<usize, String> {
    upsert_with_report(storage, records)
        .await
        .map(|outcome| outcome.written)
}

/// Like [`upsert_inbox_records`] but reports the distinct recipients actually
/// written so a caller with net access can wake their live streams.
pub async fn upsert_with_report(
    storage: &StorageHandle,
    records: &[NotificationRecord],
) -> Result<InboxWriteOutcome, String> {
    if records.is_empty() {
        return Ok(InboxWriteOutcome::default());
    }
    match upsert_once(storage, records).await {
        Ok(outcome) => Ok(outcome),
        Err(UpsertFailure::Fatal(error)) => Err(error),
        Err(UpsertFailure::Conflict) => match upsert_once(storage, records).await {
            Ok(outcome) => Ok(outcome),
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
) -> Result<InboxWriteOutcome, UpsertFailure> {
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

    let outcome = match upsert_transactionally(storage, records, txn_id).await {
        Ok(outcome) => outcome,
        Err(error) => {
            abort_txn(storage, txn_id).await;
            return Err(error);
        }
    };
    if outcome.written == 0 && outcome.purged == 0 && outcome.cutoffs == 0 {
        abort_txn(storage, txn_id).await;
        return Ok(outcome);
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(outcome),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

pub(crate) async fn upsert_transactionally(
    storage: &StorageHandle,
    records: &[NotificationRecord],
    txn_id: TxnId,
) -> Result<InboxWriteOutcome, UpsertFailure> {
    // A bucket deletion is never stored: it removes the notices about its bucket instead, and
    // keeps its time, so a notice from before it that arrives late is dropped too.
    let (purges, records): (Vec<&NotificationRecord>, Vec<&NotificationRecord>) = records
        .iter()
        .partition(|record| matches!(record.kind, NotificationKind::BucketDeleted { .. }));
    let mut kept = Vec::new();
    for record in records {
        let cutoff = match cutoff_key(record) {
            Some(key) => read_cutoff(storage, key, txn_id).await?,
            None => None,
        };
        let late = cutoff.is_some_and(|cutoff| record.created_at_ms <= cutoff);
        if !late && !purges.iter().any(|purge| purge.purges(record)) {
            kept.push(record);
        }
    }
    let records = kept;
    let mut outcome = InboxWriteOutcome::default();
    let mut deletes = Vec::new();
    let mut times = BTreeMap::new();
    for purge in &purges {
        let purged = purged_records(storage, purge, txn_id).await?;
        if !purged.is_empty() && !outcome.recipients.contains(&purge.recipient) {
            outcome.recipients.push(purge.recipient);
        }
        outcome.purged += purged.len();
        deletes.extend(purged.iter().flat_map(inbox_delete_entries));
        if let Some(key) = cutoff_key(purge) {
            let time = times.entry(key).or_insert(purge.created_at_ms);
            *time = (*time).max(purge.created_at_ms);
        }
    }
    let mut cutoffs = Vec::new();
    for (key, time) in times {
        let stored = read_cutoff(storage, key.clone(), txn_id).await?;
        if stored.is_none_or(|stored| stored < time) {
            let time = time.to_be_bytes().to_vec();
            cutoffs.push((NOTIFICATION_CUTOFF_KEYSPACE.to_string(), key, time.into()));
            outcome.cutoffs += 1;
        }
    }
    if !cutoffs.is_empty() {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: cutoffs,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
            other => {
                return Err(UpsertFailure::Fatal(format!(
                    "unexpected storage event: {other:?}"
                )));
            }
        }
    }
    if !deletes.is_empty() {
        match storage
            .send_storage_effect(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
            other => {
                return Err(UpsertFailure::Fatal(format!(
                    "unexpected storage event: {other:?}"
                )));
            }
        }
    }
    if records.is_empty() {
        return Ok(outcome);
    }

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
        Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    };

    let mut writes: Vec<(KeySpace, Key, Value)> = Vec::new();
    let mut transient_counts = BTreeMap::new();
    for (record, (_, existing_value)) in records.iter().zip(existing) {
        if existing_value.is_some() {
            continue;
        }
        match inbox_write_entries(record) {
            Ok(entries) => {
                writes.extend(entries);
                outcome.written += 1;
                if !outcome.recipients.contains(&record.recipient) {
                    outcome.recipients.push(record.recipient);
                }
                if record.class == NotificationClass::Transient {
                    *transient_counts.entry(record.recipient).or_insert(0) += 1;
                }
            }
            Err(error) => {
                return Err(UpsertFailure::Fatal(error.to_string()));
            }
        }
    }

    for (recipient, added) in transient_counts {
        let existing = count_transient(storage, recipient, txn_id).await?;
        if existing.saturating_add(added) > TRANSIENT_USER_CAP {
            return Err(UpsertFailure::Fatal(format!(
                "notification transient count for recipient {recipient} exceeds cap {TRANSIENT_USER_CAP}"
            )));
        }
    }

    if writes.is_empty() {
        return Ok(outcome);
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
        other => {
            return Err(UpsertFailure::Fatal(format!(
                "unexpected storage event: {other:?}"
            )));
        }
    }

    Ok(outcome)
}

async fn count_transient(
    storage: &StorageHandle,
    recipient: UserId,
    txn_id: TxnId,
) -> Result<usize, UpsertFailure> {
    let mut count = 0usize;
    let mut start_after = None;
    let prefix = notification_inbox_prefix(recipient);

    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.take().map(IterStart::After),
                limit: TRANSIENT_USER_CAP.saturating_add(1),
                txn_id: Some(txn_id),
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
            other => {
                return Err(UpsertFailure::Fatal(format!(
                    "unexpected storage event: {other:?}"
                )));
            }
        };

        for (_, value) in values {
            let record = NotificationRecord::from_bytes(&value)
                .map_err(|error| UpsertFailure::Fatal(error.to_string()))?;
            if record.class == NotificationClass::Transient {
                count = count.saturating_add(1);
                if count >= TRANSIENT_USER_CAP {
                    return Ok(count);
                }
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(count),
        }
    }
}

/// The deletion cutoff row of the bucket a notice names, if it names one.
fn cutoff_key(record: &NotificationRecord) -> Option<Key> {
    let (node_id, bucket) = record.kind.bucket()?;
    Some(notification_cutoff_key(record.recipient, node_id, bucket))
}

/// The newest deletion time stored for a recipient's bucket.
async fn read_cutoff(
    storage: &StorageHandle,
    key: Key,
    txn_id: TxnId,
) -> Result<Option<u64>, UpsertFailure> {
    let read = StorageEffect::Read {
        key_space: NOTIFICATION_CUTOFF_KEYSPACE.to_string(),
        key,
        txn_id: Some(txn_id),
    };
    match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
            .map(u64::from_be_bytes)),
        Event::Storage(StorageEvent::Error { error }) => Err(classify(error)),
        other => Err(UpsertFailure::Fatal(format!(
            "unexpected storage event: {other:?}"
        ))),
    }
}

/// The stored notices of the purge's recipient that its bucket deletion removes.
async fn purged_records(
    storage: &StorageHandle,
    purge: &NotificationRecord,
    txn_id: TxnId,
) -> Result<Vec<NotificationRecord>, UpsertFailure> {
    let mut purged = Vec::new();
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: Some(notification_inbox_prefix(purge.recipient)),
                start: start_after.take().map(IterStart::After),
                limit: TRANSIENT_USER_CAP,
                txn_id: Some(txn_id),
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(classify(error)),
            other => {
                return Err(UpsertFailure::Fatal(format!(
                    "unexpected storage event: {other:?}"
                )));
            }
        };
        for (_, value) in values {
            let record = NotificationRecord::from_bytes(&value)
                .map_err(|error| UpsertFailure::Fatal(error.to_string()))?;
            if purge.purges(&record) {
                purged.push(record);
            }
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(purged),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::notifications::{record, temp_storage, user};
    use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, PRUNE_INDEX_KEYSPACE};
    use aruna_core::storage_entries::inbox_update_entry;
    use aruna_core::structs::execution::notification::NotificationClass;

    fn make_record() -> NotificationRecord {
        record(user(1, 2), NotificationClass::Direct, 1_000)
    }

    fn transient_record() -> NotificationRecord {
        let mut record = make_record();
        record.class = NotificationClass::Transient;
        record
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
    async fn upsert_writes_indexes() {
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
        assert_eq!(count_keyspace(&storage, PRUNE_INDEX_KEYSPACE).await, 1);
    }

    /// A bucket deletion removes the recipient's earlier notices about the bucket, stored or in
    /// the same batch, wakes the recipient and is itself never stored.
    #[tokio::test]
    async fn deletion_purges_bucket() {
        let (_dir, storage) = temp_storage();
        let (recipient, other) = (user(1, 2), user(1, 3));
        let node_id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group_id = ulid::Ulid::from_parts(1, 1);
        let notice = |recipient, bucket: &str, at| {
            let kind = NotificationKind::ShareUpload {
                share_id: ulid::Ulid::generate(),
                node_id,
                group_id,
                bucket: bucket.to_string(),
                key: "key".to_string(),
                size_bytes: 1,
            };
            NotificationRecord::new(recipient, NotificationClass::Direct, kind, at)
        };
        let gone = notice(recipient, "raw", 1_000);
        let kept = [
            notice(recipient, "other", 1_000),
            notice(other, "raw", 1_000),
            make_record(),
        ];
        let stored = [vec![gone.clone()], kept.to_vec()].concat();
        assert_eq!(upsert_inbox_records(&storage, &stored).await, Ok(4));

        let deletion = NotificationRecord::new(
            recipient,
            NotificationClass::Direct,
            NotificationKind::BucketDeleted {
                bucket: "raw".to_string(),
                node_id,
                group_id,
            },
            2_000,
        );
        let late = notice(recipient, "raw", 1_500);
        let batch = [late.clone(), deletion.clone()];
        let outcome = upsert_with_report(&storage, &batch).await.unwrap();
        assert_eq!(outcome.written, 0);
        assert_eq!(outcome.purged, 1);
        assert_eq!(outcome.recipients, vec![recipient]);
        for record in [&gone, &late, &deletion] {
            assert_eq!(read_primary(&storage, record).await, None);
        }
        for record in &kept {
            assert!(read_primary(&storage, record).await.is_some());
        }
        assert_eq!(count_keyspace(&storage, PRUNE_INDEX_KEYSPACE).await, 3);
        // A redelivered deletion finds nothing left.
        let again = upsert_with_report(&storage, &[deletion]).await.unwrap();
        assert_eq!(again, InboxWriteOutcome::default());
    }

    /// A notice from before a bucket's deletion that arrives after it stays dropped, while a
    /// later notice about a bucket of the same name is stored.
    #[tokio::test]
    async fn late_notice_dropped() {
        let (_dir, storage) = temp_storage();
        let recipient = user(1, 2);
        let node_id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let group_id = ulid::Ulid::from_parts(1, 1);
        let record =
            |kind, at| NotificationRecord::new(recipient, NotificationClass::Direct, kind, at);
        let notice = |bucket: &str, at| {
            let kind = NotificationKind::ShareUpload {
                share_id: ulid::Ulid::generate(),
                node_id,
                group_id,
                bucket: bucket.to_string(),
                key: "key".to_string(),
                size_bytes: 1,
            };
            record(kind, at)
        };
        let deleted = NotificationKind::BucketDeleted {
            bucket: "raw".to_string(),
            node_id,
            group_id,
        };
        let deletion = upsert_with_report(&storage, &[record(deleted, 2_000)]).await;
        assert_eq!(deletion.unwrap().cutoffs, 1);

        let late = upsert_with_report(&storage, &[notice("raw", 1_500)]).await;
        assert_eq!(late.unwrap().written, 0);
        let kept = [notice("raw", 2_500), notice("other", 1_500)];
        assert_eq!(upsert_inbox_records(&storage, &kept).await, Ok(2));
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_KEYSPACE).await,
            2
        );
    }

    #[tokio::test]
    async fn duplicate_upsert_noop() {
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
    async fn reporting_lists_recipients() {
        let (_dir, storage) = temp_storage();
        let record = make_record();

        let fresh = upsert_with_report(&storage, std::slice::from_ref(&record))
            .await
            .expect("fresh write");
        assert_eq!(fresh.written, 1);
        assert_eq!(fresh.recipients, vec![record.recipient]);

        let rewrite = upsert_with_report(&storage, std::slice::from_ref(&record))
            .await
            .expect("rewrite");
        assert_eq!(rewrite.written, 0);
        assert!(rewrite.recipients.is_empty());
    }

    #[tokio::test]
    async fn reporting_dedupes_recipients() {
        let (_dir, storage) = temp_storage();
        let mut second = make_record();
        second.created_at_ms += 1;
        let first = make_record();
        assert_eq!(first.recipient, second.recipient);

        let outcome = upsert_with_report(&storage, &[first.clone(), second])
            .await
            .expect("write");
        assert_eq!(outcome.written, 2);
        assert_eq!(outcome.recipients, vec![first.recipient]);
    }

    #[tokio::test]
    async fn duplicate_preserves_read() {
        let (_dir, storage) = temp_storage();
        let record = make_record();
        assert_eq!(
            upsert_inbox_records(&storage, std::slice::from_ref(&record)).await,
            Ok(1)
        );

        let mut read_marked = record.clone();
        read_marked.read_at_ms = Some(5);
        let (key_space, key, value) = inbox_update_entry(&read_marked).expect("update entry");
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

    #[tokio::test]
    async fn transient_cap_exact() {
        let (_dir, storage) = temp_storage();
        let records: Vec<_> = (0..TRANSIENT_USER_CAP)
            .map(|_| transient_record())
            .collect();

        assert_eq!(
            upsert_inbox_records(&storage, &records).await,
            Ok(TRANSIENT_USER_CAP)
        );
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_KEYSPACE).await,
            TRANSIENT_USER_CAP
        );
    }

    #[tokio::test]
    async fn transient_cap_rejects() {
        let (_dir, storage) = temp_storage();
        let records: Vec<_> = (0..TRANSIENT_USER_CAP)
            .map(|_| transient_record())
            .collect();
        upsert_inbox_records(&storage, &records)
            .await
            .expect("cap-sized transient batch succeeds");

        let error = upsert_inbox_records(&storage, &[transient_record()])
            .await
            .expect_err("transient cap must reject extra records");
        assert!(error.contains("exceeds cap"));
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_KEYSPACE).await,
            TRANSIENT_USER_CAP
        );
    }

    #[tokio::test]
    async fn concurrent_cap_holds() {
        let (_dir, storage) = temp_storage();
        let records: Vec<_> = (0..TRANSIENT_USER_CAP - 1)
            .map(|_| transient_record())
            .collect();
        upsert_inbox_records(&storage, &records)
            .await
            .expect("initial transient records succeed");

        let left = [transient_record()];
        let right = [transient_record()];
        let (first, second) = tokio::join!(
            upsert_inbox_records(&storage, &left),
            upsert_inbox_records(&storage, &right),
        );
        assert!(first.is_ok() ^ second.is_ok());
        assert_eq!(
            count_keyspace(&storage, NOTIFICATION_INBOX_KEYSPACE).await,
            TRANSIENT_USER_CAP
        );
    }
}
