//! Upserts notification records into the holder inbox and reports new recipients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_CUTOFF_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{inbox_delete_entries, inbox_write_entries};
use aruna_core::structs::execution::notification::{
    InboxWriteOutcome, NotificationClass, NotificationKind, NotificationRecord, TRANSIENT_USER_CAP,
    notification_cutoff_key, notification_inbox_key, notification_inbox_prefix,
};
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use thiserror::Error;

use crate::notifications::unexpected;

#[derive(Debug, Error, PartialEq)]
pub enum InboxError {
    #[error("notification inbox upsert conflicted")]
    Conflict,
    #[error(transparent)]
    Storage(StorageError),
    #[error("stored notification record: {0}")]
    Decode(String),
    #[error("notification record could not be encoded: {0}")]
    Encode(String),
    #[error("notification transient count for recipient {0} exceeds cap {TRANSIENT_USER_CAP}")]
    TransientCap(UserId),
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for InboxError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => InboxError::Conflict,
            IoError::Storage(StorageError::TransactionConflict) => InboxError::Conflict,
            IoError::Storage(error) => InboxError::Storage(error),
            error => InboxError::Io(error),
        }
    }
}

/// One paged scan of a recipient's inbox: the purge it serves, or the transient count.
#[derive(Debug, PartialEq)]
struct InboxScan {
    recipient: UserId,
    next: Option<Key>,
}

#[derive(Debug, Default, PartialEq)]
enum UpsertState {
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
    #[default]
    WriteRows,
}

type Turn = Result<Next<InboxWriteOutcome>, InboxError>;

/// Idempotent holder-local upsert that skips stored keys, so a duplicate keeps a mark-read. A
/// bucket deletion removes its bucket's notices and keeps its time to drop late ones. It runs
/// in its own transaction, or in the caller's open one, which the caller commits and retries.
#[derive(Debug, PartialEq)]
pub struct UpsertInboxOperation {
    records: Vec<NotificationRecord>,
    /// The caller's transaction; `None` when the runner opens and ends one.
    outer_txn: Option<TxnId>,
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
            state: UpsertState::default(),
            kept: Vec::new(),
            purges: Vec::new(),
            deletes: Vec::new(),
            times: BTreeMap::new(),
            writes: Vec::new(),
            counts: Vec::new(),
            outcome: InboxWriteOutcome::default(),
        }
    }

    /// The storage call, in the caller's transaction when there is one.
    fn call(&self, call: IoCall) -> Turn {
        let Some(txn_id) = self.outer_txn else {
            return Ok(Next::Call(call));
        };
        let txn_id = Some(txn_id);
        let effect = match call {
            IoCall::Read(reads) => StorageEffect::BatchRead { reads, txn_id },
            IoCall::Page {
                space,
                prefix,
                after,
                limit,
            } => StorageEffect::Iter {
                key_space: space,
                prefix: Some(prefix),
                start: after.map(IterStart::After),
                limit,
                txn_id,
            },
            IoCall::Write(writes) => StorageEffect::BatchWrite { writes, txn_id },
            IoCall::Delete(deletes) => StorageEffect::BatchDelete { deletes, txn_id },
            other => return Ok(Next::Call(other)),
        };
        Ok(Next::Call(IoCall::Effect(Effect::Storage(effect))))
    }

    fn read_cutoffs(&mut self) -> Turn {
        let (purges, rest): (Vec<usize>, Vec<usize>) = (0..self.records.len()).partition(|&at| {
            matches!(
                self.records[at].kind,
                NotificationKind::BucketDeleted { .. }
            )
        });
        self.purges = purges;
        let mut checked = Vec::new();
        let mut reads = Vec::new();
        for at in rest {
            match cutoff_key(&self.records[at]) {
                Some(key) => {
                    checked.push(at);
                    reads.push((NOTIFICATION_CUTOFF_KEYSPACE.to_string(), key));
                }
                None => self.keep(at, None),
            }
        }
        if reads.is_empty() {
            return self.next_purge(0);
        }
        self.state = UpsertState::ReadCutoffs { checked };
        self.call(IoCall::Read(reads))
    }

    fn keep(&mut self, at: usize, cutoff: Option<u64>) {
        let record = &self.records[at];
        let late = cutoff.is_some_and(|cutoff| record.created_at_ms <= cutoff);
        let purged = (self.purges.iter()).any(|&purge| self.records[purge].purges(record));
        if !late && !purged {
            self.kept.push(at);
        }
    }

    fn scan(&self, scan: &InboxScan, limit: usize) -> Turn {
        self.call(IoCall::Page {
            space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: notification_inbox_prefix(scan.recipient),
            after: scan.next.clone(),
            limit,
        })
    }

    fn next_purge(&mut self, at: usize) -> Turn {
        let Some(&purge) = self.purges.get(at) else {
            return self.read_times();
        };
        let scan = InboxScan {
            recipient: self.records[purge].recipient,
            next: None,
        };
        let turn = self.scan(&scan, TRANSIENT_USER_CAP);
        self.state = UpsertState::Purge { at, scan };
        turn
    }

    fn purge_page(
        &mut self,
        at: usize,
        scan: InboxScan,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Turn {
        let purge = &self.records[self.purges[at]];
        let mut removed = 0;
        for (_, value) in values {
            let record = NotificationRecord::from_bytes(&value)
                .map_err(|error| InboxError::Decode(error.to_string()))?;
            if purge.purges(&record) {
                self.deletes.extend(inbox_delete_entries(&record));
                removed += 1;
            }
        }
        if removed > 0 && !self.outcome.recipients.contains(&purge.recipient) {
            self.outcome.recipients.push(purge.recipient);
        }
        self.outcome.purged += removed;
        if let Some(next) = next {
            let scan = InboxScan {
                recipient: scan.recipient,
                next: Some(next),
            };
            let turn = self.scan(&scan, TRANSIENT_USER_CAP);
            self.state = UpsertState::Purge { at, scan };
            return turn;
        }
        if let Some(key) = cutoff_key(purge) {
            let time = self.times.entry(key).or_insert(purge.created_at_ms);
            *time = (*time).max(purge.created_at_ms);
        }
        self.next_purge(at + 1)
    }

    fn read_times(&mut self) -> Turn {
        let times: Vec<(Key, u64)> = std::mem::take(&mut self.times).into_iter().collect();
        if times.is_empty() {
            return self.delete_rows();
        }
        let space = || NOTIFICATION_CUTOFF_KEYSPACE.to_string();
        let reads = times
            .iter()
            .map(|(key, _)| (space(), key.clone()))
            .collect();
        self.state = UpsertState::ReadTimes { times };
        self.call(IoCall::Read(reads))
    }

    fn write_times(&mut self, times: Vec<(Key, u64)>, values: Vec<Option<Value>>) -> Turn {
        let mut writes = Vec::new();
        for ((key, time), stored) in times.into_iter().zip(values) {
            if stored_time(stored.as_ref()).is_none_or(|stored| stored < time) {
                let time = Value::from(time.to_be_bytes().to_vec());
                writes.push((NOTIFICATION_CUTOFF_KEYSPACE.to_string(), key, time));
                self.outcome.cutoffs += 1;
            }
        }
        if writes.is_empty() {
            return self.delete_rows();
        }
        self.state = UpsertState::WriteTimes;
        self.call(IoCall::Write(writes))
    }

    fn delete_rows(&mut self) -> Turn {
        let deletes = std::mem::take(&mut self.deletes);
        if deletes.is_empty() {
            return self.read_existing();
        }
        self.state = UpsertState::DeleteRows;
        self.call(IoCall::Delete(deletes))
    }

    fn read_existing(&mut self) -> Turn {
        if self.kept.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        }
        let reads = (self.kept.iter())
            .map(|&at| {
                let record = &self.records[at];
                let key = notification_inbox_key(
                    record.recipient,
                    record.created_at_ms,
                    record.notification_id,
                );
                (NOTIFICATION_INBOX_KEYSPACE.to_string(), key)
            })
            .collect();
        self.state = UpsertState::ReadExisting;
        self.call(IoCall::Read(reads))
    }

    fn plan_writes(&mut self, values: Vec<Option<Value>>) -> Turn {
        let kept = std::mem::take(&mut self.kept);
        for (at, existing) in kept.into_iter().zip(values) {
            let record = &self.records[at];
            self.outcome.held.push(record.clone());
            if existing.is_some() {
                continue;
            }
            let entries = inbox_write_entries(record)
                .map_err(|error| InboxError::Encode(error.to_string()))?;
            self.writes.extend(entries);
            self.outcome.written += 1;
            if !self.outcome.recipients.contains(&record.recipient) {
                self.outcome.recipients.push(record.recipient);
            }
            if record.class == NotificationClass::Transient {
                match self
                    .counts
                    .iter_mut()
                    .find(|(user, _)| *user == record.recipient)
                {
                    Some((_, added)) => *added += 1,
                    None => self.counts.push((record.recipient, 1)),
                }
            }
        }
        self.counts.sort_by_key(|(user, _)| *user);
        self.next_count(0)
    }

    fn next_count(&mut self, at: usize) -> Turn {
        let Some(&(recipient, _)) = self.counts.get(at) else {
            return self.write_rows();
        };
        let scan = InboxScan {
            recipient,
            next: None,
        };
        let turn = self.scan(&scan, TRANSIENT_USER_CAP.saturating_add(1));
        self.state = UpsertState::Count { at, seen: 0, scan };
        turn
    }

    fn count_page(
        &mut self,
        at: usize,
        mut seen: usize,
        scan: InboxScan,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Turn {
        for (_, value) in values {
            let record = NotificationRecord::from_bytes(&value)
                .map_err(|error| InboxError::Decode(error.to_string()))?;
            if record.class == NotificationClass::Transient {
                seen = seen.saturating_add(1);
                if seen >= TRANSIENT_USER_CAP {
                    return self.check_count(at, seen);
                }
            }
        }
        match next {
            Some(next) => {
                let scan = InboxScan {
                    recipient: scan.recipient,
                    next: Some(next),
                };
                let turn = self.scan(&scan, TRANSIENT_USER_CAP.saturating_add(1));
                self.state = UpsertState::Count { at, seen, scan };
                turn
            }
            None => self.check_count(at, seen),
        }
    }

    fn check_count(&mut self, at: usize, seen: usize) -> Turn {
        let (recipient, added) = self.counts[at];
        if seen.saturating_add(added) > TRANSIENT_USER_CAP {
            return Err(InboxError::TransientCap(recipient));
        }
        self.next_count(at + 1)
    }

    fn write_rows(&mut self) -> Turn {
        let writes = std::mem::take(&mut self.writes);
        if writes.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.outcome)));
        }
        self.state = UpsertState::WriteRows;
        self.call(IoCall::Write(writes))
    }
}

/// The plain result of a storage effect run in the caller's transaction.
fn plain(result: IoResult) -> Result<IoResult, InboxError> {
    let IoResult::Event(Event::Storage(event)) = result else {
        return Ok(result);
    };
    match event {
        StorageEvent::BatchReadResult { values } => Ok(IoResult::Values(
            values.into_iter().map(|(_, value)| value).collect(),
        )),
        StorageEvent::IterResult {
            values,
            next_start_after,
        } => Ok(IoResult::Page {
            rows: values,
            next: next_start_after,
        }),
        StorageEvent::BatchWriteResult { .. } | StorageEvent::BatchDeleteResult { .. } => {
            Ok(IoResult::Done)
        }
        StorageEvent::Error { error } => Err(IoError::Storage(error).into()),
        other => {
            Err(unexpected("a storage result", &IoResult::Event(Event::Storage(other))).into())
        }
    }
}

impl Operation for UpsertInboxOperation {
    type Output = InboxWriteOutcome;
    type Error = InboxError;

    fn first(&mut self) -> Turn {
        if self.records.is_empty() {
            return Ok(Next::Done(InboxWriteOutcome::default()));
        }
        self.read_cutoffs()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (std::mem::take(&mut self.state), plain(result)?) {
            (UpsertState::ReadCutoffs { checked }, IoResult::Values(values)) => {
                for (at, value) in checked.into_iter().zip(values) {
                    self.keep(at, stored_time(value.as_ref()));
                }
                self.kept.sort_unstable();
                self.next_purge(0)
            }
            (UpsertState::Purge { at, scan }, IoResult::Page { rows, next }) => {
                self.purge_page(at, scan, rows, next)
            }
            (UpsertState::ReadTimes { times }, IoResult::Values(values)) => {
                self.write_times(times, values)
            }
            (UpsertState::WriteTimes, IoResult::Done) => self.delete_rows(),
            (UpsertState::DeleteRows, IoResult::Done) => self.read_existing(),
            (UpsertState::ReadExisting, IoResult::Values(values)) => self.plan_writes(values),
            (UpsertState::Count { at, seen, scan }, IoResult::Page { rows, next }) => {
                self.count_page(at, seen, scan, rows, next)
            }
            (UpsertState::WriteRows, IoResult::Done) => {
                Ok(Next::Done(std::mem::take(&mut self.outcome)))
            }
            (_, other) => Err(unexpected("an inbox storage result", &other).into()),
        }
    }

    fn transaction(&self) -> bool {
        self.outer_txn.is_none()
    }
}

/// The deletion cutoff row of the bucket a notice names, if it names one.
fn cutoff_key(record: &NotificationRecord) -> Option<Key> {
    let (node_id, bucket) = record.kind.bucket()?;
    Some(notification_cutoff_key(record.recipient, node_id, bucket))
}

/// A stored deletion time; a malformed one counts as none.
fn stored_time(value: Option<&Value>) -> Option<u64> {
    value
        .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
        .map(u64::from_be_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use crate::tests::notifications::{context, record, temp_storage, user};
    use aruna_core::keyspaces::{NOTIFICATION_INBOX_KEYSPACE, PRUNE_INDEX_KEYSPACE};
    use aruna_core::storage_entries::inbox_update_entry;
    use aruna_core::structs::execution::notification::NotificationClass;
    use aruna_storage::StorageHandle;

    async fn upsert_with_report(
        storage: &StorageHandle,
        records: &[NotificationRecord],
    ) -> Result<InboxWriteOutcome, InboxError> {
        let upsert = UpsertInboxOperation::new(records.to_vec());
        drive(upsert, &context(storage)).await
    }

    async fn upsert_inbox_records(
        storage: &StorageHandle,
        records: &[NotificationRecord],
    ) -> Result<usize, InboxError> {
        upsert_with_report(storage, records)
            .await
            .map(|outcome| outcome.written)
    }

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
        assert!(matches!(error, InboxError::TransientCap(_)));
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

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::notifications::{record, user};

    const TXN: TxnId = TxnId::from_parts(5, 5);

    fn notice() -> NotificationRecord {
        record(user(1, 2), NotificationClass::Direct, 1_000)
    }

    fn inbox_key(record: &NotificationRecord) -> Key {
        notification_inbox_key(
            record.recipient,
            record.created_at_ms,
            record.notification_id,
        )
    }

    #[test]
    fn upsert_success_calls() {
        let record = notice();
        let mut upsert = UpsertInboxOperation::new(vec![record.clone()]);
        assert!(upsert.transaction());
        let space = NOTIFICATION_INBOX_KEYSPACE.to_string();
        assert_eq!(
            upsert.first(),
            Ok(Next::Call(IoCall::Read(vec![(space, inbox_key(&record))])))
        );
        let Ok(Next::Call(IoCall::Write(writes))) = upsert.next(IoResult::Values(vec![None]))
        else {
            panic!("one write");
        };
        assert_eq!(writes, inbox_write_entries(&record).unwrap());
        let Ok(Next::Done(outcome)) = upsert.next(IoResult::Done) else {
            panic!("done");
        };
        assert_eq!(
            (outcome.written, outcome.recipients),
            (1, vec![record.recipient])
        );
    }

    #[test]
    fn redelivery_writes_nothing() {
        let record = notice();
        let mut upsert = UpsertInboxOperation::new(vec![record.clone()]);
        upsert.first().unwrap();
        let stored = IoResult::Values(vec![Some(Value::from(vec![1]))]);
        let Ok(Next::Done(outcome)) = upsert.next(stored) else {
            panic!("done");
        };
        assert_eq!((outcome.written, outcome.held), (0, vec![record]));
    }

    #[test]
    fn within_caller_transaction() {
        let mut upsert = UpsertInboxOperation::within(vec![notice()], TXN);
        assert!(!upsert.transaction());
        assert!(matches!(
            upsert.first(),
            Ok(Next::Call(IoCall::Effect(Effect::Storage(
                StorageEffect::BatchRead {
                    txn_id: Some(TXN),
                    ..
                }
            ))))
        ));
        let conflict = Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
        assert_eq!(
            upsert.next(IoResult::Event(conflict)),
            Err(InboxError::Conflict)
        );
    }

    #[test]
    fn empty_batch_done() {
        let mut upsert = UpsertInboxOperation::new(Vec::new());
        assert_eq!(upsert.first(), Ok(Next::Done(InboxWriteOutcome::default())));
        let mut other = UpsertInboxOperation::new(vec![notice()]);
        other.first().unwrap();
        assert!(other.next(IoResult::Woke).is_err());
    }
}
