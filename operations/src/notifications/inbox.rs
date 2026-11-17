//! Upserts notification records into the holder inbox and reports new recipients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_CUTOFF_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{inbox_delete_entries, inbox_write_entries};
use aruna_core::structs::execution::notification::{
    NotificationClass, NotificationKind, NotificationRecord, TRANSIENT_USER_CAP,
    notification_cutoff_key, notification_inbox_key, notification_inbox_prefix,
};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum InboxError {
    #[error("notification inbox upsert conflicted")]
    Conflict,
    #[error("notification inbox upsert conflicted twice")]
    ConflictedTwice,
    #[error(transparent)]
    Storage(StorageError),
    #[error("stored notification record: {0}")]
    Decode(String),
    #[error("notification record could not be encoded: {0}")]
    Encode(String),
    #[error("notification transient count for recipient {0} exceeds cap {TRANSIENT_USER_CAP}")]
    TransientCap(UserId),
    #[error("unexpected event in {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("the notification inbox upsert did not finish")]
    NotFinished,
}

impl From<StorageError> for InboxError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::TransactionConflict => InboxError::Conflict,
            error => InboxError::Storage(error),
        }
    }
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
    /// Batch records the inbox holds after the write, new or already present, never discarded.
    pub held: Vec<NotificationRecord>,
}

/// One paged scan of a recipient's inbox: the purge it serves, or the transient count.
#[derive(Debug, PartialEq)]
struct InboxScan {
    recipient: UserId,
    next: Option<Key>,
}

#[derive(Debug, Default, PartialEq)]
enum UpsertState {
    #[default]
    Init,
    Start,
    /// Reads the deletion time of each notice's bucket.
    ReadCutoffs {
        checked: Vec<usize>,
    },
    /// Scans the inbox of `purges[at]` for the notices its bucket deletion removes.
    Purge {
        at: usize,
        scan: InboxScan,
    },
    /// Reads the stored deletion times the purges may raise.
    ReadTimes {
        times: Vec<(Key, u64)>,
    },
    WriteTimes,
    DeleteRows,
    ReadExisting,
    /// Counts the stored transient notices of `counts[at]`.
    Count {
        at: usize,
        seen: usize,
        scan: InboxScan,
    },
    WriteRows,
    Commit,
    Abort,
    /// Aborts the conflicted transaction before the one more run.
    Retry,
    Done,
    Failed(InboxError),
}

impl UpsertState {
    fn name(&self) -> &'static str {
        match self {
            UpsertState::Init => "init",
            UpsertState::Start => "start",
            UpsertState::ReadCutoffs { .. } => "read cutoffs",
            UpsertState::Purge { .. } => "purge scan",
            UpsertState::ReadTimes { .. } => "read deletion times",
            UpsertState::WriteTimes => "write deletion times",
            UpsertState::DeleteRows => "delete purged rows",
            UpsertState::ReadExisting => "read existing rows",
            UpsertState::Count { .. } => "count transient rows",
            UpsertState::WriteRows => "write rows",
            UpsertState::Commit => "commit",
            UpsertState::Abort => "abort",
            UpsertState::Retry => "retry",
            UpsertState::Done => "done",
            UpsertState::Failed(_) => "failed",
        }
    }
}

/// Idempotent holder-local upsert: existing primary keys are skipped inside the write
/// transaction, so a duplicate never clobbers a concurrent mark-read's `read_at_ms`. A bucket
/// deletion is never stored: it removes the notices about its bucket and keeps its time, so a
/// notice from before it that arrives late is dropped too. An upsert in its own transaction
/// runs once more after a conflict.
#[derive(Debug, PartialEq)]
pub struct UpsertInboxOperation {
    records: Vec<NotificationRecord>,
    /// The caller's transaction; `None` when the operation opens and ends its own.
    outer_txn: Option<TxnId>,
    txn_id: Option<TxnId>,
    retried: bool,
    state: UpsertState,
    kept: Vec<usize>,
    purges: Vec<usize>,
    deletes: Vec<(KeySpace, Key)>,
    times: BTreeMap<Key, u64>,
    writes: Vec<(KeySpace, Key, Value)>,
    counts: Vec<(UserId, usize)>,
    outcome: InboxWriteOutcome,
}

impl UpsertInboxOperation {
    pub fn new(records: Vec<NotificationRecord>) -> Self {
        Self::build(records, None)
    }

    /// Upserts inside the caller's open transaction; the caller commits and retries.
    pub fn within(records: Vec<NotificationRecord>, txn_id: TxnId) -> Self {
        Self::build(records, Some(txn_id))
    }

    fn build(records: Vec<NotificationRecord>, outer_txn: Option<TxnId>) -> Self {
        Self {
            records,
            outer_txn,
            txn_id: outer_txn,
            retried: false,
            state: UpsertState::Init,
            kept: Vec::new(),
            purges: Vec::new(),
            deletes: Vec::new(),
            times: BTreeMap::new(),
            writes: Vec::new(),
            counts: Vec::new(),
            outcome: InboxWriteOutcome::default(),
        }
    }

    fn txn(&self) -> TxnId {
        self.txn_id.unwrap_or_default()
    }

    fn owned_txn(&self) -> Option<TxnId> {
        self.txn_id.filter(|_| self.outer_txn.is_none())
    }

    fn begin(&mut self) -> Effects {
        self.kept.clear();
        self.purges.clear();
        self.deletes.clear();
        self.times.clear();
        self.writes.clear();
        self.counts.clear();
        self.outcome = InboxWriteOutcome::default();
        match self.outer_txn {
            Some(_) => self.read_cutoffs(),
            None => {
                self.txn_id = None;
                self.state = UpsertState::Start;
                smallvec![Effect::Storage(StorageEffect::StartTransaction { read: false })]
            }
        }
    }

    fn fail(&mut self, error: InboxError) -> Effects {
        if error == InboxError::Conflict && self.outer_txn.is_none() && !self.retried {
            self.retried = true;
            let abort = self.abort_txn();
            if abort.is_empty() {
                return self.begin();
            }
            self.state = UpsertState::Retry;
            return abort;
        }
        let error = match error {
            InboxError::Conflict if self.outer_txn.is_none() => InboxError::ConflictedTwice,
            error => error,
        };
        let abort = self.abort_txn();
        self.state = UpsertState::Failed(error);
        abort
    }

    fn abort_txn(&mut self) -> Effects {
        match self.owned_txn() {
            Some(txn_id) => {
                self.txn_id = None;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, state: UpsertState, event: Event) -> Effects {
        self.state = state;
        let error = match event {
            Event::Storage(StorageEvent::Error { error }) => error.into(),
            other => InboxError::UnexpectedEvent {
                state: self.state.name(),
                got: format!("{other:?}"),
            },
        };
        self.fail(error)
    }

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
