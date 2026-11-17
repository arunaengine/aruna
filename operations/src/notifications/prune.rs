//! Removes expired inbox notifications and enforces the transient per-user cap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    NOTIFICATION_CUTOFF_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE, PRUNE_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification::{
    DIRECT_TTL_MS, NotificationClass, NotificationRecord, TRANSIENT_TTL_MS, TRANSIENT_USER_CAP,
    notification_inbox_key, notification_prune_key, parse_prune_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

pub const PRUNE_PAGE_SIZE: usize = 512;
pub const NOTIFICATION_POLL_AFTER: Duration = Duration::from_secs(60 * 60);
pub const NOTIFICATION_RETRY_AFTER: Duration = Duration::from_secs(30);

#[derive(Debug, Default, PartialEq, Eq)]
pub struct NotificationPruneOutcome {
    pub expired: usize,
    pub capped: usize,
    pub has_more: bool,
    pub next_due_after: Option<Duration>,
}

impl NotificationPruneOutcome {
    /// When the prune timer runs again: at once while due rows remain, else at the next due
    /// row, at the latest after the poll interval.
    pub fn rearm(&self) -> Duration {
        if self.has_more {
            return Duration::ZERO;
        }
        (self.next_due_after)
            .unwrap_or(NOTIFICATION_POLL_AFTER)
            .min(NOTIFICATION_POLL_AFTER)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum PruneError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected event in {state}: {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("the notification prune did not finish")]
    NotFinished,
}

/// One due row of the prune index and the two inbox rows it may name; only one class matches.
#[derive(Debug, PartialEq)]
struct Candidate {
    index: Key,
    direct: Key,
    transient: Key,
}

/// Where a phase continues after its current page.
#[derive(Debug, Default, PartialEq)]
struct Scan {
    next: Option<Key>,
    stopped: bool,
}

#[derive(Debug, Default, PartialEq)]
enum PruneState {
    #[default]
    Init,
    IndexPage,
    IndexRows {
        candidates: Vec<Candidate>,
        scan: Scan,
    },
    IndexDelete,
    InboxPage,
    InboxDelete {
        next: Option<Key>,
    },
    CutoffPage,
    CutoffStart {
        keys: Vec<Key>,
        next: Option<Key>,
    },
    CutoffRead {
        txn_id: TxnId,
        next: Option<Key>,
    },
    CutoffDelete {
        txn_id: TxnId,
        next: Option<Key>,
    },
    CutoffCommit {
        txn_id: TxnId,
        next: Option<Key>,
    },
    Done,
    Failed(PruneError),
}

impl PruneState {
    fn name(&self) -> &'static str {
        match self {
            PruneState::Init => "init",
            PruneState::IndexPage => "index page",
            PruneState::IndexRows { .. } => "index rows",
            PruneState::IndexDelete => "index delete",
            PruneState::InboxPage => "inbox page",
            PruneState::InboxDelete { .. } => "inbox delete",
            PruneState::CutoffPage => "cutoff page",
            PruneState::CutoffStart { .. } => "cutoff start",
            PruneState::CutoffRead { .. } => "cutoff read",
            PruneState::CutoffDelete { .. } => "cutoff delete",
            PruneState::CutoffCommit { .. } => "cutoff commit",
            PruneState::Done => "done",
            PruneState::Failed(_) => "failed",
        }
    }

    fn txn_id(&self) -> Option<TxnId> {
        match self {
            PruneState::CutoffRead { txn_id, .. }
            | PruneState::CutoffDelete { txn_id, .. }
            | PruneState::CutoffCommit { txn_id, .. } => Some(*txn_id),
            _ => None,
        }
    }
}

/// One prune run: the due rows of the prune index (at most four pages of them), then a sweep of
/// the inbox for expired and over-cap transient rows, then a sweep of outlived deletion times.
#[derive(Debug, PartialEq)]
pub struct PruneNotificationsOperation {
    now_ms: u64,
    page_size: usize,
    state: PruneState,
    outcome: NotificationPruneOutcome,
    /// Index rows processed so far; at four pages the run stops and asks to run again.
    processed: usize,
    deletes: Vec<(KeySpace, Key)>,
    recipient: Option<Vec<u8>>,
    transient_seen: usize,
}

impl PruneNotificationsOperation {
    pub fn new(now_ms: u64, page_size: usize) -> Self {
        Self {
            now_ms,
            page_size,
            state: PruneState::Init,
            outcome: NotificationPruneOutcome::default(),
            processed: 0,
            deletes: Vec::new(),
            recipient: None,
            transient_seen: 0,
        }
    }

    fn iter(&self, key_space: &str, start_after: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: self.page_size,
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: PruneError) -> Effects {
        let txn_id = self.state.txn_id();
        self.state = PruneState::Failed(error);
        match txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        let error = match event {
            Event::Storage(StorageEvent::Error { error }) => PruneError::Storage(error),
            other => PruneError::UnexpectedEvent {
                state: self.state.name(),
                got: format!("{other:?}"),
            },
        };
        self.fail(error)
    }

    fn index_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if values.is_empty() {
            return self.delete_index();
        }
        let deletion_cap = self.page_size.saturating_mul(4);
        let mut candidates = Vec::new();
        let mut scan = Scan {
            next,
            stopped: false,
        };
        for (key, _) in values {
            let (expires_at_ms, recipient, notification_id) = match parse_prune_key(key.as_ref()) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(%error, key = ?key.as_ref(), "Deleting malformed notification prune index row");
                    self.deletes.push((PRUNE_INDEX_KEYSPACE.to_string(), key));
                    continue;
                }
            };
            if expires_at_ms > self.now_ms {
                let due = expires_at_ms.saturating_sub(self.now_ms);
                self.outcome.next_due_after = Some(Duration::from_millis(due));
                scan.stopped = true;
                break;
            }
            // Saturated expiry is future, so the scan stops before unmatched candidates.
            let inbox_key = |ttl_ms: u64| {
                let created_ms = expires_at_ms.saturating_sub(ttl_ms);
                notification_inbox_key(recipient, created_ms, notification_id)
            };
            candidates.push(Candidate {
                index: key,
                direct: inbox_key(DIRECT_TTL_MS),
                transient: inbox_key(TRANSIENT_TTL_MS),
            });
            self.processed = self.processed.saturating_add(1);
            if self.processed >= deletion_cap {
                self.outcome.has_more = true;
                scan.stopped = true;
                break;
            }
        }
        if candidates.is_empty() {
            return self.index_continue(scan);
        }
        let space = || NOTIFICATION_INBOX_KEYSPACE.to_string();
        let reads = (candidates.iter())
            .flat_map(|row| [(space(), row.direct.clone()), (space(), row.transient.clone())])
            .collect();
        self.state = PruneState::IndexRows { candidates, scan };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }

    fn index_rows(
        &mut self,
        candidates: Vec<Candidate>,
        scan: Scan,
        values: Vec<(Key, Option<Value>)>,
    ) -> Effects {
        let exists = |at: usize| values.get(at).is_some_and(|(_, value)| value.is_some());
        for (at, row) in candidates.into_iter().enumerate() {
            self.deletes.push((PRUNE_INDEX_KEYSPACE.to_string(), row.index));
            let inbox = if exists(at * 2) {
                Some(row.direct)
            } else if exists(at * 2 + 1) {
                Some(row.transient)
            } else {
                None
            };
            match inbox {
                Some(key) => {
                    self.deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), key));
                    self.outcome.expired = self.outcome.expired.saturating_add(1);
                }
                None => warn!("Deleting orphan notification prune index row"),
            }
        }
        self.index_continue(scan)
    }

    fn index_continue(&mut self, scan: Scan) -> Effects {
        match (scan.stopped, scan.next) {
            (false, Some(next)) => {
                self.state = PruneState::IndexPage;
                self.iter(PRUNE_INDEX_KEYSPACE, Some(next))
            }
            _ => self.delete_index(),
        }
    }

    fn delete_index(&mut self) -> Effects {
        let deletes = std::mem::take(&mut self.deletes);
        if deletes.is_empty() {
            return self.inbox_next(None);
        }
        self.state = PruneState::IndexDelete;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None
        })]
    }

    fn inbox_next(&mut self, start_after: Option<Key>) -> Effects {
        self.state = PruneState::InboxPage;
        self.iter(NOTIFICATION_INBOX_KEYSPACE, start_after)
    }

    fn inbox_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        if values.is_empty() {
            return self.cutoff_next(None);
        }
        let mut deletes = Vec::new();
        for (key, value) in values {
            if key.len() != 72 {
                warn!(key = ?key.as_ref(), "Skipping malformed notification inbox key during prune sweep");
                continue;
            }
            let recipient_prefix = &key.as_ref()[..48];
            if self.recipient.as_deref() != Some(recipient_prefix) {
                self.recipient = Some(recipient_prefix.to_vec());
                self.transient_seen = 0;
            }
            let record = match NotificationRecord::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(error) => {
                    warn!(%error, key = ?key.as_ref(), "Skipping malformed notification inbox record during prune sweep");
                    continue;
                }
            };
            let index = || (PRUNE_INDEX_KEYSPACE.to_string(), notification_prune_key(&record));
            if record.expires_at_ms() <= self.now_ms {
                deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), key));
                deletes.push(index());
                self.outcome.expired = self.outcome.expired.saturating_add(1);
                continue;
            }
            if record.class == NotificationClass::Transient {
                self.transient_seen = self.transient_seen.saturating_add(1);
                if self.transient_seen > TRANSIENT_USER_CAP {
                    deletes.push((NOTIFICATION_INBOX_KEYSPACE.to_string(), key));
                    deletes.push(index());
                    self.outcome.capped = self.outcome.capped.saturating_add(1);
                }
            }
        }
        if deletes.is_empty() {
            return self.inbox_after(next);
        }
        self.state = PruneState::InboxDelete { next };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None
        })]
    }

    fn inbox_after(&mut self, next: Option<Key>) -> Effects {
        match next {
            Some(next) => self.inbox_next(Some(next)),
            None => self.cutoff_next(None),
        }
    }

    fn cutoff_next(&mut self, start_after: Option<Key>) -> Effects {
        self.state = PruneState::CutoffPage;
        self.iter(NOTIFICATION_CUTOFF_KEYSPACE, start_after)
    }

    fn cutoff_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        let keys: Vec<Key> = (values.into_iter())
            .filter(|(_, value)| cutoff_expired(Some(value), self.now_ms))
            .map(|(key, _)| key)
            .collect();
        if keys.is_empty() {
            return self.cutoff_after(next);
        }
        self.state = PruneState::CutoffStart { keys, next };
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn cutoff_after(&mut self, next: Option<Key>) -> Effects {
        match next {
            Some(next) => self.cutoff_next(Some(next)),
            None => {
                self.state = PruneState::Done;
                smallvec![]
            }
        }
    }

    /// Deletes only the deletion times still outlived when read again inside the transaction,
    /// so one a bucket deletion renewed meanwhile stays.
    fn cutoff_read(&mut self, txn_id: TxnId, next: Option<Key>, values: Vec<(Key, Option<Value>)>) -> Effects {
        let deletes: Vec<(KeySpace, Key)> = (values.into_iter())
            .filter(|(_, value)| cutoff_expired(value.as_ref(), self.now_ms))
            .map(|(key, _)| (NOTIFICATION_CUTOFF_KEYSPACE.to_string(), key))
            .collect();
        if deletes.is_empty() {
            return self.cutoff_commit(txn_id, next);
        }
        self.state = PruneState::CutoffDelete { txn_id, next };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id)
        })]
    }

    fn cutoff_commit(&mut self, txn_id: TxnId, next: Option<Key>) -> Effects {
        self.state = PruneState::CutoffCommit { txn_id, next };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}


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
