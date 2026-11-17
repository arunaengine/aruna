//! Upserts notification records into the holder inbox and reports new recipients.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NOTIFICATION_CUTOFF_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE};
use aruna_core::operation::EffectOperation;
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

/// Idempotent holder-local upsert that skips stored keys, so a duplicate keeps a mark-read. A
/// bucket deletion removes its bucket's notices and keeps its time to drop late ones. An upsert
/// in its own transaction runs once more after a conflict.
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
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
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

    fn read_cutoffs(&mut self) -> Effects {
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
        self.batch_read(reads)
    }

    fn keep(&mut self, at: usize, cutoff: Option<u64>) {
        let record = &self.records[at];
        let late = cutoff.is_some_and(|cutoff| record.created_at_ms <= cutoff);
        let purged = (self.purges.iter()).any(|&purge| self.records[purge].purges(record));
        if !late && !purged {
            self.kept.push(at);
        }
    }

    fn batch_read(&self, reads: Vec<(KeySpace, Key)>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(self.txn()),
        })]
    }

    fn scan(&self, scan: &InboxScan, limit: usize) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
            prefix: Some(notification_inbox_prefix(scan.recipient)),
            start: scan.next.clone().map(IterStart::After),
            limit,
            txn_id: Some(self.txn()),
        })]
    }

    fn next_purge(&mut self, at: usize) -> Effects {
        let Some(&purge) = self.purges.get(at) else {
            return self.read_times();
        };
        let scan = InboxScan {
            recipient: self.records[purge].recipient,
            next: None,
        };
        let effects = self.scan(&scan, TRANSIENT_USER_CAP);
        self.state = UpsertState::Purge { at, scan };
        effects
    }

    fn purge_page(
        &mut self,
        at: usize,
        scan: InboxScan,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Effects {
        let purge = &self.records[self.purges[at]];
        let mut removed = 0;
        for (_, value) in values {
            let record = match NotificationRecord::from_bytes(&value) {
                Ok(record) => record,
                Err(error) => return self.fail(InboxError::Decode(error.to_string())),
            };
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
            let effects = self.scan(&scan, TRANSIENT_USER_CAP);
            self.state = UpsertState::Purge { at, scan };
            return effects;
        }
        if let Some(key) = cutoff_key(purge) {
            let time = self.times.entry(key).or_insert(purge.created_at_ms);
            *time = (*time).max(purge.created_at_ms);
        }
        self.next_purge(at + 1)
    }

    fn read_times(&mut self) -> Effects {
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
        self.batch_read(reads)
    }

    fn write_times(
        &mut self,
        times: Vec<(Key, u64)>,
        values: Vec<(Key, Option<Value>)>,
    ) -> Effects {
        let mut writes = Vec::new();
        for ((key, time), (_, stored)) in times.into_iter().zip(values) {
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
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(self.txn()),
        })]
    }

    fn delete_rows(&mut self) -> Effects {
        let deletes = std::mem::take(&mut self.deletes);
        if deletes.is_empty() {
            return self.read_existing();
        }
        self.state = UpsertState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(self.txn()),
        })]
    }

    fn read_existing(&mut self) -> Effects {
        if self.kept.is_empty() {
            return self.finish();
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
        self.batch_read(reads)
    }

    fn plan_writes(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let kept = std::mem::take(&mut self.kept);
        for (at, (_, existing)) in kept.into_iter().zip(values) {
            let record = &self.records[at];
            self.outcome.held.push(record.clone());
            if existing.is_some() {
                continue;
            }
            match inbox_write_entries(record) {
                Ok(entries) => self.writes.extend(entries),
                Err(error) => return self.fail(InboxError::Encode(error.to_string())),
            }
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

    fn next_count(&mut self, at: usize) -> Effects {
        let Some(&(recipient, _)) = self.counts.get(at) else {
            return self.write_rows();
        };
        let scan = InboxScan {
            recipient,
            next: None,
        };
        let effects = self.scan(&scan, TRANSIENT_USER_CAP.saturating_add(1));
        self.state = UpsertState::Count { at, seen: 0, scan };
        effects
    }

    fn count_page(
        &mut self,
        at: usize,
        mut seen: usize,
        scan: InboxScan,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Effects {
        for (_, value) in values {
            let record = match NotificationRecord::from_bytes(&value) {
                Ok(record) => record,
                Err(error) => return self.fail(InboxError::Decode(error.to_string())),
            };
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
                let effects = self.scan(&scan, TRANSIENT_USER_CAP.saturating_add(1));
                self.state = UpsertState::Count { at, seen, scan };
                effects
            }
            None => self.check_count(at, seen),
        }
    }

    fn check_count(&mut self, at: usize, seen: usize) -> Effects {
        let (recipient, added) = self.counts[at];
        if seen.saturating_add(added) > TRANSIENT_USER_CAP {
            return self.fail(InboxError::TransientCap(recipient));
        }
        self.next_count(at + 1)
    }

    fn write_rows(&mut self) -> Effects {
        let writes = std::mem::take(&mut self.writes);
        if writes.is_empty() {
            return self.finish();
        }
        self.state = UpsertState::WriteRows;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(self.txn()),
        })]
    }

    /// Ends the caller's run, or commits its own transaction; one that changed nothing aborts.
    fn finish(&mut self) -> Effects {
        let Some(txn_id) = self.owned_txn() else {
            self.state = UpsertState::Done;
            return smallvec![];
        };
        let outcome = &self.outcome;
        if outcome.written == 0 && outcome.purged == 0 && outcome.cutoffs == 0 {
            self.state = UpsertState::Abort;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = UpsertState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl EffectOperation for UpsertInboxOperation {
    type Output = InboxWriteOutcome;
    type Error = InboxError;

    fn start(&mut self) -> Effects {
        if self.records.is_empty() {
            self.state = UpsertState::Done;
            return smallvec![];
        }
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        let state = std::mem::take(&mut self.state);
        match (state, event) {
            (UpsertState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.read_cutoffs()
            }
            (
                UpsertState::ReadCutoffs { checked },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                for (at, (_, value)) in checked.into_iter().zip(values) {
                    self.keep(at, stored_time(value.as_ref()));
                }
                self.kept.sort_unstable();
                self.next_purge(0)
            }
            (
                UpsertState::Purge { at, scan },
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.purge_page(at, scan, values, next_start_after),
            (
                UpsertState::ReadTimes { times },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.write_times(times, values),
            (UpsertState::WriteTimes, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.delete_rows()
            }
            (UpsertState::DeleteRows, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.read_existing()
            }
            (
                UpsertState::ReadExisting,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.plan_writes(values),
            (
                UpsertState::Count { at, seen, scan },
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.count_page(at, seen, scan, values, next_start_after),
            (UpsertState::WriteRows, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.finish()
            }
            (UpsertState::Retry, Event::Storage(_)) => self.begin(),
            (UpsertState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. }))
            | (
                UpsertState::Abort,
                Event::Storage(
                    StorageEvent::TransactionAborted { .. } | StorageEvent::Error { .. },
                ),
            ) => {
                self.txn_id = None;
                self.state = UpsertState::Done;
                smallvec![]
            }
            // A failed commit ended the transaction already.
            (UpsertState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.txn_id = None;
                self.fail(error.into())
            }
            (state, event) => self.unexpected(state, event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, UpsertState::Done | UpsertState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            UpsertState::Done => Ok(self.outcome),
            UpsertState::Failed(error) => Err(error),
            _ => Err(InboxError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.owned_txn() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
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
mod state_machine_tests {
    use super::*;
    use crate::tests::notifications::{record, user};
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

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

    fn conflict() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        })
    }

    #[test]
    fn upsert_success_trace() {
        let record = notice();
        let mut upsert = UpsertInboxOperation::new(vec![record.clone()]);
        let writes = inbox_write_entries(&record).unwrap();
        check_trace(
            &mut upsert,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::BatchRead {
                        reads: vec![(NOTIFICATION_INBOX_KEYSPACE.to_string(), inbox_key(&record))],
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchReadResult {
                        values: vec![(inbox_key(&record), None)],
                    }),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes,
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    vec![],
                ),
            ],
        );
        let outcome = upsert.finalize().unwrap();
        assert_eq!(
            (outcome.written, outcome.recipients, outcome.held),
            (1, vec![record.recipient], vec![record])
        );
    }

    #[test]
    fn redelivery_aborts() {
        let record = notice();
        let mut upsert = UpsertInboxOperation::new(vec![record.clone()]);
        upsert.start();
        upsert.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let effects = upsert.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(inbox_key(&record), Some(Value::from(vec![1])))],
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN
            })]
        );
        upsert.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        let outcome = upsert.finalize().unwrap();
        assert_eq!((outcome.written, outcome.held.len()), (0, 1));
    }

    #[test]
    fn conflict_runs_twice() {
        let mut upsert = UpsertInboxOperation::new(vec![notice()]);
        upsert.start();
        upsert.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let abort = upsert.step(conflict());
        assert_eq!(
            abort.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN
            })]
        );
        let again = upsert.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        assert_eq!(
            again.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        upsert.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        upsert.step(conflict());
        assert_eq!(upsert.finalize(), Err(InboxError::ConflictedTwice));
    }

    #[test]
    fn within_reports_conflict() {
        let mut upsert = UpsertInboxOperation::within(vec![notice()], TXN);
        assert!(matches!(
            upsert.start().as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));
        assert!(upsert.step(conflict()).is_empty());
        assert_eq!(upsert.finalize(), Err(InboxError::Conflict));
    }

    #[test]
    fn empty_batch_done() {
        let mut upsert = UpsertInboxOperation::new(Vec::new());
        assert!(upsert.start().is_empty());
        assert_eq!(upsert.finalize(), Ok(InboxWriteOutcome::default()));
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut upsert = UpsertInboxOperation::new(vec![notice()]);
            upsert.start();
            upsert
        };
        let started = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::TransactionStarted { .. })
            )
        };
        let refusals = rejects_events(make, sample_events, started);
        assert!(!refusals.is_empty());
    }

    #[test]
    fn abort_per_phase() {
        let mut upsert = UpsertInboxOperation::new(vec![notice()]);
        upsert.start();
        check_abort(&mut upsert, &[]);
        upsert.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        check_abort(
            &mut upsert,
            &[Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN,
            })],
        );
        let mut within = UpsertInboxOperation::within(vec![notice()], TXN);
        within.start();
        check_abort(&mut within, &[]);
        rejects_finalize(within);
    }
}
