//! Upserts notification records into the holder inbox and reports new recipients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
