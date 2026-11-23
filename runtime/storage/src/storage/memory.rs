//! Keeps storage rows in memory with snapshot reads and optimistic commits, for tests only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every commit gets the next sequence and each key keeps its versions, so a transaction reads
// the rows of its start. A commit with changes conflicts when a key it read changed since then.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::iter::Peekable;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, USAGE_PREIMAGE_KEYSPACE};
use aruna_core::structs::storage::usage::{
    USAGE_COMPLETE_KEY, USAGE_SHADOW_PREFIX, dirty_share_key, usage_shadow_key,
};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::handle::{
    CleanupEntry, CleanupKind, MAX_CLEANUP_ATTEMPTS, StorageHandle, cleanup_effect, effect_txn_id,
    finish_cleanup, observe_cleanup, storage_effect_mutates,
};
use super::hot_cache::{CacheWriter, HotCache};
use super::metrics::StorageMetrics;
use super::preimage::{is_source, kept_cursor, kept_row, kept_value, merge_kept, tagged};
use super::rebuild::{rebuild_seed, shadow_copies, swap_row};
use super::records::page_event;
use super::transactions::{
    PendingChanges, abort_blocked, commit_gate, counter_row, pend_into, range_changes, reserve_txn,
    usage_changes,
};
use super::worker::{
    KeyBounds, budget_page, collect_page, page_bounds, range_bounds, touches_config,
};

/// Opens storage that keeps every row in memory, without files or threads. It answers storage
/// effects as `FjallStorage` does, apart from durability. One mutex runs each effect whole, so
/// tests of a read racing a commit in the same write transaction use `FjallStorage`.
pub struct MemoryStorage;

impl MemoryStorage {
    /// A new, empty store behind a handle with a hot cache, as `FjallStorage::open` returns.
    pub fn open() -> StorageHandle {
        let (mut handle, _receivers) = StorageHandle::new();
        handle.cache = Some(Arc::new(HotCache::default()));
        handle.memory = Some(Arc::new(MemoryStore::new(&handle)));
        handle
    }
}

/// One committed version of a key; `transactional` distinguishes commits from keyspace clears.
#[derive(Debug, PartialEq, Eq)]
struct Version {
    sequence: u64,
    value: Option<ByteView>,
    transactional: bool,
}

/// The versions of one key, oldest first.
type Versions = Vec<Version>;
/// Changed rows of one keyspace; `None` deletes the row.
type Rows = BTreeMap<ByteView, Option<ByteView>>;
/// Changed rows per keyspace, applied as one commit.
type Changes = BTreeMap<String, Rows>;
type Bounds = (Bound<ByteView>, Bound<ByteView>);
type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);
type RowResult = Result<(ByteView, ByteView), StorageError>;
/// One side of a merged scan: keys in scan order with their value, `None` for no row.
type Side<'a> = Box<dyn Iterator<Item = (ByteView, Option<ByteView>)> + 'a>;

/// The store a memory handle runs its effects on, one effect at a time.
pub(super) struct MemoryStore(Mutex<State>);

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemoryStore")
    }
}

impl MemoryStore {
    fn new(handle: &StorageHandle) -> Self {
        Self(Mutex::new(State {
            rows: Committed::default(),
            txns: HashMap::new(),
            commits: 0,
            usage: HashMap::new(),
            config_txns: HashSet::new(),
            usage_shadow: HashSet::new(),
            usage_views: HashSet::new(),
            txn_rows: HashMap::new(),
            cache: CacheWriter::new(handle.cache.clone()),
            cleanup: handle.transaction_cleanup.clone(),
            metrics: handle.metrics.clone(),
        }))
    }

    /// Answers one effect as the worker does: the drain fence, the effect, then the cleanup.
    pub(super) fn run(&self, effect: StorageEffect) -> StorageEvent {
        let mut state = self.0.lock().expect("memory storage mutex poisoned");
        let cleanup = cleanup_effect(&effect);
        let fenced = state.metrics.mutations_fenced.load(Ordering::SeqCst);
        let event = match storage_effect_mutates(&effect) && fenced {
            true => state.reject_fenced(&effect),
            false => state.process(effect),
        };
        observe_cleanup(&state.cleanup, cleanup, &event);
        state.retry_cleanup();
        event
    }
}

/// An open transaction and the commit sequence its snapshot reads at.
enum Txn {
    Read(u64),
    Write(WriteTxn),
}

struct WriteTxn {
    snapshot: u64,
    changes: Changes,
    /// Keys and ranges read, which conflict with any later commit that changed them.
    reads: Vec<(String, Bounds)>,
}

impl Txn {
    fn snapshot(&self) -> u64 {
        match self {
            Self::Read(snapshot) => *snapshot,
            Self::Write(txn) => txn.snapshot,
        }
    }
}

struct State {
    rows: Committed,
    txns: HashMap<Ulid, Txn>,
    /// Write commits applied so far, the sequence a commit answers.
    commits: u64,
    usage: HashMap<Ulid, PendingChanges>,
    /// Open write transactions that wrote or deleted realm config rows.
    config_txns: HashSet<Ulid>,
    /// Keyspaces whose committed usage deltas also go to their rebuild rows.
    usage_shadow: HashSet<String>,
    /// Snapshots of usage rebuilds, which read source rows as the rebuild began.
    usage_views: HashSet<Ulid>,
    /// Source rows each open write transaction changed, for their kept values at commit.
    txn_rows: HashMap<Ulid, Vec<(String, ByteView)>>,
    cache: CacheWriter,
    cleanup: Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    metrics: Arc<StorageMetrics>,
}

impl State {
    fn process(&mut self, effect: StorageEffect) -> StorageEvent {
        let config_txn = touches_config(&effect).then(|| effect_txn_id(&effect));
        let rows = self.cache.rows(&effect);
        let rows_txn = effect_txn_id(&effect);
        if rows_txn.is_none() {
            self.cache.begin(&rows);
        }
        let event = self.answer(effect);
        match rows_txn {
            Some(txn_id) if self.txns.contains_key(&txn_id) => self.cache.hold(txn_id, rows),
            Some(_) => {}
            None => self.cache.finish(&rows),
        }
        match config_txn {
            Some(Some(txn_id)) if self.txns.contains_key(&txn_id) => {
                self.config_txns.insert(txn_id);
            }
            Some(Some(_)) => {}
            Some(None) => self.note_config(),
            None => {}
        }
        event
    }

    fn answer(&mut self, effect: StorageEffect) -> StorageEvent {
        let result = match effect {
            StorageEffect::StartTransaction { read } => return self.start(read),
            StorageEffect::AbortTransaction { txn_id } => return self.abort(txn_id),
            StorageEffect::CommitTransaction { txn_id } => return self.commit(txn_id),
            StorageEffect::SyncAll => return StorageEvent::SyncAllFinished,
            StorageEffect::Read {
                key_space,
                key,
                txn_id,
            } => self
                .read(txn_id, &key_space, &key)
                .map(|value| StorageEvent::ReadResult { key, value }),
            StorageEffect::BatchRead { reads, txn_id } => self.batch_read(reads, txn_id),
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            } => self
                .change(vec![(key_space, key.clone(), Some(value))], txn_id)
                .map(|_| StorageEvent::WriteResult { key }),
            StorageEffect::BatchWrite { writes, txn_id } => {
                let rows = (writes.into_iter())
                    .map(|(key_space, key, value)| (key_space, key, Some(value)))
                    .collect();
                self.change(rows, txn_id)
                    .map(|entries| StorageEvent::BatchWriteResult { entries })
            }
            StorageEffect::Delete {
                key_space,
                key,
                txn_id,
            } => self
                .change(vec![(key_space, key.clone(), None)], txn_id)
                .map(|_| StorageEvent::DeleteResult { key }),
            StorageEffect::BatchDelete { deletes, txn_id } => {
                let rows = (deletes.into_iter())
                    .map(|(key_space, key)| (key_space, key, None))
                    .collect();
                self.change(rows, txn_id)
                    .map(|entries| StorageEvent::BatchDeleteResult { entries })
            }
            StorageEffect::AddUsage {
                key_space,
                deltas,
                buckets,
                txn_id,
            } => return self.pend(key_space, usage_changes(deltas, buckets), txn_id),
            StorageEffect::AddCounters {
                key_space,
                deltas,
                txn_id,
            } => return self.pend(key_space, range_changes(deltas), txn_id),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id,
            } => return page_event(self.iterate(&key_space, prefix, start, limit, txn_id)),
            StorageEffect::IterRange {
                key_space,
                start,
                end,
                limit,
                max_bytes,
                txn_id,
            } => {
                let range = (&start, &end);
                let page = self.iterate_range(&key_space, range, (limit, max_bytes), txn_id);
                return page_event(page);
            }
            StorageEffect::Last {
                key_space,
                prefix,
                txn_id,
            } => self.last(&key_space, prefix, txn_id),
            // The handle runs vault and realm key effects before anything is dispatched.
            StorageEffect::VaultWrite { .. }
            | StorageEffect::VaultRead { .. }
            | StorageEffect::VaultDelete { .. }
            | StorageEffect::RealmOpen(_)
            | StorageEffect::CompilePolicies(_) => Err(StorageError::InvalidEffect),
            StorageEffect::BeginUsageRebuild { key_space } => {
                return self.begin_rebuild(key_space);
            }
            StorageEffect::FinishUsageRebuild { key_space } => self
                .finish_rebuild(&key_space)
                .map(|entries| StorageEvent::BatchWriteResult { entries }),
        };
        result.unwrap_or_else(|error| StorageEvent::Error { error })
    }

    fn note_config(&self) {
        self.metrics.config_changes.fetch_add(1, Ordering::Release);
    }

    /// The oldest sequence an open snapshot reads at; older versions are no longer read.
    fn floor(&self) -> u64 {
        self.txns
            .values()
            .map(Txn::snapshot)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn commit_rows(&mut self, changes: Changes) {
        if !changes.is_empty() {
            let floor = self.floor();
            self.rows.apply(changes, floor);
        }
    }

    /// Fails unless `txn_id` names an open transaction.
    fn find(&self, txn_id: Option<Ulid>) -> Result<(), StorageError> {
        match txn_id {
            Some(txn_id) if !self.txns.contains_key(&txn_id) => {
                Err(StorageError::TransactionNotFound)
            }
            _ => Ok(()),
        }
    }

    /// The snapshot of a usage rebuild's view, which reads rows as the rebuild began.
    fn view(&self, txn_id: Option<Ulid>) -> Option<u64> {
        let txn_id = txn_id.filter(|txn_id| self.usage_views.contains(txn_id))?;
        match self.txns.get(&txn_id) {
            Some(Txn::Read(snapshot)) => Some(*snapshot),
            _ => None,
        }
    }

    /// The sequence a read in `txn_id` sees; a write transaction adds `bounds` to its reads.
    fn mark(
        &mut self,
        txn_id: Option<Ulid>,
        key_space: &str,
        bounds: &Bounds,
    ) -> Result<u64, StorageError> {
        match txn_id.map(|txn_id| self.txns.get_mut(&txn_id)) {
            None => Ok(self.rows.sequence),
            Some(None) => Err(StorageError::TransactionNotFound),
            Some(Some(Txn::Read(snapshot))) => Ok(*snapshot),
            Some(Some(Txn::Write(txn))) => {
                txn.reads.push((key_space.to_string(), bounds.clone()));
                Ok(txn.snapshot)
            }
        }
    }

    /// The rows write transaction `txn_id` changed in `key_space`.
    fn own(&self, txn_id: Option<Ulid>, key_space: &str) -> Option<&Rows> {
        match self.txns.get(&txn_id?)? {
            Txn::Write(txn) => txn.changes.get(key_space),
            Txn::Read(_) => None,
        }
    }

    fn scan(
        &self,
        txn_id: Option<Ulid>,
        key_space: &str,
        snapshot: u64,
        bounds: &Bounds,
        reverse: bool,
    ) -> impl Iterator<Item = RowResult> + '_ {
        let own = self.own(txn_id, key_space);
        self.rows.scan(key_space, snapshot, own, bounds, reverse)
    }

    fn read(
        &mut self,
        txn_id: Option<Ulid>,
        key_space: &str,
        key: &ByteView,
    ) -> Result<Option<ByteView>, StorageError> {
        if let Some(snapshot) = self.view(txn_id) {
            return self.view_get(snapshot, key_space, key);
        }
        let bounds = (Included(key.clone()), Included(key.clone()));
        let snapshot = self.mark(txn_id, key_space, &bounds)?;
        if let Some(value) = self.own(txn_id, key_space).and_then(|own| own.get(key)) {
            return Ok(value.clone());
        }
        Ok(self.rows.get(key_space, key, snapshot))
    }

    fn batch_read(
        &mut self,
        reads: Vec<(String, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> Result<StorageEvent, StorageError> {
        self.find(txn_id)?;
        let mut values = Vec::with_capacity(reads.len());
        for (key_space, key) in reads {
            let value = self.read(txn_id, &key_space, &key)?;
            values.push((key, value));
        }
        Ok(StorageEvent::BatchReadResult { values })
    }

    /// Writes or deletes rows in `txn_id`, or commits them at once without one.
    fn change(
        &mut self,
        rows: Vec<(String, ByteView, Option<ByteView>)>,
        txn_id: Option<Ulid>,
    ) -> Result<Vec<(String, ByteView)>, StorageError> {
        let entries: Vec<_> = (rows.iter())
            .map(|(key_space, key, _)| (key_space.clone(), key.clone()))
            .collect();
        let changes = match txn_id {
            Some(txn_id) => match self.txns.get_mut(&txn_id) {
                Some(Txn::Write(txn)) => &mut txn.changes,
                _ => return Err(StorageError::TransactionNotFound),
            },
            None => &mut Changes::new(),
        };
        for (key_space, key, value) in rows {
            changes.entry(key_space).or_default().insert(key, value);
        }
        let noted = entries
            .iter()
            .map(|(key_space, key)| (key_space.as_str(), key));
        match txn_id {
            Some(txn_id) => self.note_rows(txn_id, noted),
            None => {
                let mut changes = std::mem::take(changes);
                self.keep_preimages(&mut changes, noted);
                self.commit_rows(changes);
            }
        }
        Ok(entries)
    }

    fn pend(
        &mut self,
        key_space: String,
        changes: Vec<(ByteView, super::transactions::CounterChange)>,
        txn_id: Ulid,
    ) -> StorageEvent {
        if !matches!(self.txns.get(&txn_id), Some(Txn::Write(_))) {
            let error = StorageError::TransactionNotFound;
            return StorageEvent::Error { error };
        }
        pend_into(self.usage.entry(txn_id).or_default(), key_space, changes)
    }

    fn iterate(
        &mut self,
        key_space: &str,
        prefix: Option<ByteView>,
        start: Option<IterStart>,
        limit: usize,
        txn_id: Option<Ulid>,
    ) -> Result<PageResult, StorageError> {
        if let Some(snapshot) = self.view(txn_id) {
            return match limit {
                0 => Ok((Vec::new(), None)),
                _ => self.view_page(snapshot, key_space, prefix.as_ref(), start.as_ref(), limit),
            };
        }
        self.find(txn_id)?;
        if limit == 0 {
            return Ok((Vec::new(), None));
        }
        let Some(bounds) = page_bounds(prefix.as_ref(), start.as_ref()) else {
            return Ok((Vec::new(), None));
        };
        let bounds = byte_bounds(bounds);
        let snapshot = self.mark(txn_id, key_space, &bounds)?;
        collect_page(
            self.scan(txn_id, key_space, snapshot, &bounds, false),
            limit,
            |row| row,
        )
    }

    fn iterate_range(
        &mut self,
        key_space: &str,
        (start, end): (&IterStart, &IterEnd),
        budget: (usize, usize),
        txn_id: Option<Ulid>,
    ) -> Result<PageResult, StorageError> {
        if self.view(txn_id).is_some() {
            return Err(StorageError::InvalidEffect);
        }
        self.find(txn_id)?;
        let Some(bounds) = range_bounds(start, end).filter(|_| budget.0 > 0) else {
            return Ok((Vec::new(), None));
        };
        let bounds = byte_bounds(bounds);
        let snapshot = self.mark(txn_id, key_space, &bounds)?;
        budget_page(
            self.scan(txn_id, key_space, snapshot, &bounds, false),
            budget,
            |row| row,
        )
    }

    fn last(
        &mut self,
        key_space: &str,
        prefix: Option<ByteView>,
        txn_id: Option<Ulid>,
    ) -> Result<StorageEvent, StorageError> {
        if self.view(txn_id).is_some() {
            return Err(StorageError::InvalidEffect);
        }
        let mut values = Vec::new();
        if let Some(bounds) = page_bounds(prefix.as_ref(), None) {
            let bounds = byte_bounds(bounds);
            let snapshot = self.mark(txn_id, key_space, &bounds)?;
            let last = self.scan(txn_id, key_space, snapshot, &bounds, true).next();
            values.extend(last.transpose()?);
        }
        Ok(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })
    }

    fn start(&mut self, read: bool) -> StorageEvent {
        let txn_id = match reserve_txn(&self.cleanup) {
            Ok(txn_id) => txn_id,
            Err(error) => return StorageEvent::Error { error },
        };
        let snapshot = self.rows.sequence;
        let txn = match read {
            true => Txn::Read(snapshot),
            false => Txn::Write(WriteTxn {
                snapshot,
                changes: Changes::new(),
                reads: Vec::new(),
            }),
        };
        self.txns.insert(txn_id, txn);
        StorageEvent::TransactionStarted { txn_id }
    }

    fn abort(&mut self, txn_id: Ulid) -> StorageEvent {
        if abort_blocked(&self.cleanup, txn_id) {
            let error = StorageError::TransactionConflict;
            return StorageEvent::Error { error };
        }
        self.usage.remove(&txn_id);
        self.txn_rows.remove(&txn_id);
        self.usage_views.remove(&txn_id);
        self.config_txns.remove(&txn_id);
        self.cache.take(txn_id);
        match self.txns.remove(&txn_id) {
            Some(_) => StorageEvent::TransactionAborted { txn_id },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    fn commit(&mut self, txn_id: Ulid) -> StorageEvent {
        let open = self.txns.contains_key(&txn_id);
        if let Some(event) = commit_gate(&self.cleanup, txn_id, open) {
            return event;
        }
        let config_txn = self.config_txns.remove(&txn_id);
        let cached = self.cache.take(txn_id);
        self.usage_views.remove(&txn_id);
        let rows = self.txn_rows.remove(&txn_id).unwrap_or_default();
        let mut txn = match self.txns.remove(&txn_id) {
            Some(Txn::Write(txn)) => txn,
            Some(Txn::Read(_)) => {
                let sequence = self.commits;
                return StorageEvent::TransactionCommitted { txn_id, sequence };
            }
            None => {
                let error = StorageError::TransactionNotFound;
                return StorageEvent::Error { error };
            }
        };
        let rows = rows
            .iter()
            .map(|(key_space, key)| (key_space.as_str(), key));
        self.keep_preimages(&mut txn.changes, rows);
        if let Err(error) = self.stage_usage(txn_id, &mut txn.changes) {
            // Unlike an unknown commit, a confirmed rollback lets the caller undo work.
            let error = StorageError::RolledBack(Box::new(error));
            return StorageEvent::Error { error };
        }
        self.cache.begin(&cached);
        // A transaction that changed nothing commits without a conflict check.
        let conflict = !txn.changes.is_empty()
            && (txn.reads.iter())
                .any(|(key_space, bounds)| self.rows.changed(key_space, bounds, txn.snapshot));
        if !conflict {
            self.commit_rows(txn.changes);
        }
        self.cache.finish(&cached);
        if config_txn && !conflict {
            self.note_config();
        }
        if conflict {
            let error = StorageError::TransactionConflict;
            return StorageEvent::Error { error };
        }
        self.commits += 1;
        let sequence = self.commits;
        StorageEvent::TransactionCommitted { txn_id, sequence }
    }

    /// Adds the transaction's usage deltas to the latest committed counters.
    fn stage_usage(&mut self, txn_id: Ulid, changes: &mut Changes) -> Result<(), StorageError> {
        let Some(mut pending) = self.usage.remove(&txn_id) else {
            return Ok(());
        };
        shadow_copies(&self.usage_shadow, &mut pending)?;
        let latest = self.rows.sequence;
        for ((key_space, key), change) in pending {
            let stored = self.rows.get(&key_space, &key, latest);
            let value = counter_row(&key, stored.as_deref(), change)?;
            let rows = changes.entry(key_space).or_default();
            rows.insert(key, Some(ByteView::from(value)));
        }
        Ok(())
    }

    /// Stages the value each source row had at the last commit, unless a value is kept already.
    fn keep_preimages<'a>(
        &self,
        changes: &mut Changes,
        rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>,
    ) {
        if self.usage_shadow.is_empty() {
            return;
        }
        let latest = self.rows.sequence;
        for (key_space, key) in rows.into_iter().filter(|(space, _)| is_source(space)) {
            let row = tagged(key_space, key);
            if (self.rows.get(USAGE_PREIMAGE_KEYSPACE, &row, latest)).is_some() {
                continue;
            }
            let value = self.rows.get(key_space, key, latest);
            let kept = changes.entry(USAGE_PREIMAGE_KEYSPACE.to_string());
            kept.or_default()
                .insert(row, Some(kept_row(value.as_deref())));
        }
    }

    fn note_rows<'a>(
        &mut self,
        txn_id: Ulid,
        rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>,
    ) {
        let rows = rows.into_iter().filter(|(space, _)| is_source(space));
        let mut noted = rows
            .map(|(space, key)| (space.to_string(), key.clone()))
            .peekable();
        if noted.peek().is_some() {
            self.txn_rows.entry(txn_id).or_default().extend(noted);
        }
    }

    /// Reads `key` as it was when the rebuild began.
    fn view_get(
        &self,
        snapshot: u64,
        key_space: &str,
        key: &[u8],
    ) -> Result<Option<ByteView>, StorageError> {
        let row = tagged(key_space, key);
        if is_source(key_space)
            && let Some(kept) = self.rows.get(USAGE_PREIMAGE_KEYSPACE, &row, snapshot)
        {
            return kept_value(kept.as_ref());
        }
        Ok(self.rows.get(key_space, key, snapshot))
    }

    /// One page of `key_space` as it was when the rebuild began.
    fn view_page(
        &self,
        snapshot: u64,
        key_space: &str,
        prefix: Option<&ByteView>,
        start: Option<&IterStart>,
        limit: usize,
    ) -> Result<PageResult, StorageError> {
        let page = match page_bounds(prefix, start) {
            Some(bounds) => {
                let bounds = byte_bounds(bounds);
                let rows = self.rows.scan(key_space, snapshot, None, &bounds, false);
                collect_page(rows, limit, |row| row)?
            }
            None => (Vec::new(), None),
        };
        if !is_source(key_space) {
            return Ok(page);
        }
        let (kept_prefix, kept_start) = kept_cursor(key_space, prefix, start);
        let kept = match page_bounds(Some(&kept_prefix), kept_start.as_ref()) {
            Some(bounds) => {
                let bounds = byte_bounds(bounds);
                let rows =
                    (self.rows).scan(USAGE_PREIMAGE_KEYSPACE, snapshot, None, &bounds, false);
                collect_page(rows, limit, |row| row)?
            }
            None => (Vec::new(), None),
        };
        merge_kept(key_space, page, kept, limit)
    }

    fn begin_rebuild(&mut self, key_space: String) -> StorageEvent {
        if !self.usage_shadow.contains(&key_space) {
            let floor = self.floor();
            self.rows.clear(USAGE_PREIMAGE_KEYSPACE, floor);
            if let Err(error) = self.clear_rebuild(&key_space) {
                return StorageEvent::Error { error };
            }
            self.usage_shadow.insert(key_space);
        }
        let started = self.start(true);
        if let StorageEvent::TransactionStarted { txn_id } = &started {
            self.usage_views.insert(*txn_id);
        }
        started
    }

    /// Deletes old rebuild rows and writes the seed rows of a new generation in one commit.
    fn clear_rebuild(&mut self, key_space: &str) -> Result<(), StorageError> {
        let prefix = ByteView::from(USAGE_SHADOW_PREFIX);
        let latest = self.rows.sequence;
        let mut rows = Rows::new();
        if let Some(bounds) = page_bounds(Some(&prefix), None) {
            let old = (self.rows).scan(key_space, latest, None, &byte_bounds(bounds), false);
            for row in old {
                rows.insert(row?.0, None);
            }
        }
        for (row, value) in rebuild_seed()? {
            rows.insert(row, Some(value));
        }
        self.commit_rows(Changes::from([(key_space.to_string(), rows)]));
        Ok(())
    }

    fn finish_rebuild(&mut self, key_space: &str) -> Result<Vec<(String, ByteView)>, StorageError> {
        if !self.usage_shadow.contains(key_space) {
            let message = "no usage rebuild is running".to_string();
            return Err(StorageError::WriteError(message));
        }
        let entries = self.replace_counters(key_space)?;
        self.usage_shadow.remove(key_space);
        // A fresh rebuild clears what is left.
        let floor = self.floor();
        self.rows.clear(USAGE_PREIMAGE_KEYSPACE, floor);
        Ok(entries)
    }

    /// Replaces every live counter row by its rebuild row in one commit.
    fn replace_counters(
        &mut self,
        key_space: &str,
    ) -> Result<Vec<(String, ByteView)>, StorageError> {
        let latest = self.rows.sequence;
        let (mut changes, mut entries, mut groups) = (Changes::new(), Vec::new(), BTreeSet::new());
        let swapped = changes.entry(key_space.to_string()).or_default();
        for row in (self.rows).scan(key_space, latest, None, &(Unbounded, Unbounded), false) {
            let (key, value) = row?;
            let shadow = usage_shadow_key(key.as_ref());
            let kept = || Ok(self.rows.get(key_space, &shadow, latest).is_some());
            let swap = swap_row(&key, value.as_ref(), kept)?;
            groups.extend(swap.group);
            if let Some((live, live_value)) = swap.live {
                swapped.insert(live.clone(), Some(live_value));
                entries.push((key_space.to_string(), live));
            }
            if swap.remove {
                swapped.insert(key, None);
            }
        }
        // Corrected group counters may undercut replica shares published from the old ones.
        let generation = ByteView::from(Ulid::generate().to_bytes().to_vec());
        let stats = changes.entry(NODE_STATS_KEYSPACE.to_string()).or_default();
        for group in groups {
            stats.insert(
                ByteView::from(dirty_share_key(group)),
                Some(generation.clone()),
            );
        }
        let complete = (
            ByteView::from(USAGE_COMPLETE_KEY),
            Some(ByteView::from(&[][..])),
        );
        changes
            .entry(key_space.to_string())
            .or_default()
            .extend([complete]);
        changes.retain(|_, rows| !rows.is_empty());
        self.commit_rows(changes);
        Ok(entries)
    }

    /// Rejects a mutation the drain fence caught, leaving no open transaction behind it.
    fn reject_fenced(&mut self, effect: &StorageEffect) -> StorageEvent {
        if let Some(txn_id) = effect_txn_id(effect) {
            self.retire(txn_id);
        }
        self.metrics.rejected_writes.fetch_add(1, Ordering::Relaxed);
        warn!(
            event = "storage.write.after_fence",
            "Rejected a memory storage mutation issued after the shutdown drain fence"
        );
        StorageEvent::Error {
            error: StorageError::Closed,
        }
    }

    /// Rolls a fenced transaction back and retires its cleanup entry through `Aborted`.
    fn retire(&mut self, txn_id: Ulid) {
        self.usage.remove(&txn_id);
        self.txn_rows.remove(&txn_id);
        self.config_txns.remove(&txn_id);
        self.cache.take(txn_id);
        self.txns.remove(&txn_id);
        let mut pending = self
            .cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned");
        if let Some(entry) = pending.get_mut(&txn_id) {
            (entry.kind, entry.attempts, entry.queued) = (CleanupKind::Aborted, 0, false);
        }
        drop(pending);
        finish_cleanup(&self.cleanup, txn_id);
    }

    /// Retries aborts and unknown commits the handle left behind, as the worker does.
    fn retry_cleanup(&mut self) {
        let pending = self
            .cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned");
        let retry: Vec<_> = (pending.iter())
            .filter(|(_, entry)| {
                matches!(entry.kind, CleanupKind::Abort | CleanupKind::CommitUnknown)
                    && entry.attempts < MAX_CLEANUP_ATTEMPTS
                    && !entry.queued
            })
            .map(|(txn_id, entry)| (*txn_id, entry.kind))
            .collect();
        drop(pending);
        for (txn_id, kind) in retry {
            let fenced = self.metrics.mutations_fenced.load(Ordering::SeqCst);
            let event = match kind {
                CleanupKind::CommitUnknown if fenced => {
                    self.retire(txn_id);
                    continue;
                }
                CleanupKind::CommitUnknown => self.commit(txn_id),
                _ => self.abort(txn_id),
            };
            if observe_cleanup(&self.cleanup, Some((txn_id, kind)), &event) {
                finish_cleanup(&self.cleanup, txn_id);
            }
        }
    }
}

/// Committed rows with their versions; a snapshot reads the newest version up to its sequence.
#[derive(Default)]
struct Committed {
    spaces: HashMap<String, BTreeMap<ByteView, Versions>>,
    /// The sequence of the last commit.
    sequence: u64,
}

impl Committed {
    fn get(&self, key_space: &str, key: &[u8], snapshot: u64) -> Option<ByteView> {
        visible(self.spaces.get(key_space)?.get(key)?, snapshot)
    }

    /// Rows within `bounds` as of `snapshot`, with the transaction's `own` changes laid over.
    fn scan<'a>(
        &'a self,
        key_space: &str,
        snapshot: u64,
        own: Option<&'a Rows>,
        bounds: &Bounds,
        reverse: bool,
    ) -> impl Iterator<Item = RowResult> + 'a {
        let valid = ordered(bounds);
        let base: Side<'a> = match self.spaces.get(key_space).filter(|_| valid) {
            Some(space) => {
                let rows = space.range(bounds.clone());
                sided(
                    rows.map(move |(key, versions)| (key.clone(), visible(versions, snapshot))),
                    reverse,
                )
            }
            None => Box::new(std::iter::empty()),
        };
        let top: Side<'a> = match own.filter(|_| valid) {
            Some(own) => {
                let rows = own.range(bounds.clone());
                sided(
                    rows.map(|(key, value)| (key.clone(), value.clone())),
                    reverse,
                )
            }
            None => Box::new(std::iter::empty()),
        };
        let merged = Merge {
            base: base.peekable(),
            top: top.peekable(),
            reverse,
        };
        merged.filter_map(|(key, value)| Some(Ok((key, value?))))
    }

    /// True when a transactional commit after `snapshot` changed a key within `bounds`.
    fn changed(&self, key_space: &str, bounds: &Bounds, snapshot: u64) -> bool {
        let Some(space) = self.spaces.get(key_space).filter(|_| ordered(bounds)) else {
            return false;
        };
        let mut rows = space.range(bounds.clone());
        rows.any(|(_, versions)| {
            versions
                .iter()
                .rev()
                .take_while(|version| version.sequence > snapshot)
                .any(|version| version.transactional)
        })
    }

    /// Applies `changes` as the next commit, keeping only versions a snapshot from `floor` on
    /// may still read.
    fn apply(&mut self, changes: Changes, floor: u64) {
        self.sequence += 1;
        for (key_space, rows) in changes {
            let space = self.spaces.entry(key_space).or_default();
            for (key, value) in rows {
                let versions = space.entry(key.clone()).or_default();
                versions.push(Version {
                    sequence: self.sequence,
                    value,
                    transactional: true,
                });
                prune(versions, floor);
                if versions.is_empty() {
                    space.remove(&key);
                }
            }
        }
    }

    /// Drops every live row of a keyspace outside optimistic conflict tracking; like Fjall, a
    /// snapshot from `floor` on still reads the old rows, and closed delete histories are pruned.
    fn clear(&mut self, key_space: &str, floor: u64) {
        let Some(space) = self.spaces.get_mut(key_space) else {
            return;
        };
        let any_live = space.values().any(|versions| {
            versions
                .last()
                .is_some_and(|version| version.value.is_some())
        });
        if any_live {
            self.sequence += 1;
            let sequence = self.sequence;
            for versions in space.values_mut() {
                if versions
                    .last()
                    .is_some_and(|version| version.value.is_some())
                {
                    versions.push(Version {
                        sequence,
                        value: None,
                        transactional: false,
                    });
                }
            }
        }
        space.retain(|_, versions| {
            prune(versions, floor);
            !versions.is_empty()
        });
    }
}

/// Merges committed rows with a transaction's own changes in scan order; its own change wins.
struct Merge<'a> {
    base: Peekable<Side<'a>>,
    top: Peekable<Side<'a>>,
    reverse: bool,
}

impl Iterator for Merge<'_> {
    type Item = (ByteView, Option<ByteView>);

    fn next(&mut self) -> Option<Self::Item> {
        let order = match (self.base.peek(), self.top.peek()) {
            (Some((base, _)), Some((top, _))) if self.reverse => top.cmp(base),
            (Some((base, _)), Some((top, _))) => base.cmp(top),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => return None,
        };
        match order {
            std::cmp::Ordering::Less => self.base.next(),
            std::cmp::Ordering::Greater => self.top.next(),
            std::cmp::Ordering::Equal => {
                self.base.next();
                self.top.next()
            }
        }
    }
}

fn sided<'a>(
    rows: impl DoubleEndedIterator<Item = (ByteView, Option<ByteView>)> + 'a,
    reverse: bool,
) -> Side<'a> {
    match reverse {
        true => Box::new(rows.rev()),
        false => Box::new(rows),
    }
}

/// The value of the newest version at or before `snapshot`.
fn visible(versions: &Versions, snapshot: u64) -> Option<ByteView> {
    let version = versions
        .iter()
        .rev()
        .find(|version| version.sequence <= snapshot);
    version.and_then(|version| version.value.clone())
}

/// Keeps the versions after `floor` and the newest one at or before it, unless that is a delete.
fn prune(versions: &mut Versions, floor: u64) {
    let older = versions
        .iter()
        .take_while(|version| version.sequence <= floor)
        .count();
    let deleted = older > 0 && versions[older - 1].value.is_none();
    let keep_from = if deleted {
        older
    } else {
        older.saturating_sub(1)
    };
    versions.drain(..keep_from);
}

/// False for bounds a `BTreeMap` range rejects, which hold no key.
fn ordered(bounds: &Bounds) -> bool {
    match bounds {
        (Included(start) | Excluded(start), Included(end) | Excluded(end)) => {
            start < end || (start == end && !matches!(bounds, (Excluded(_), Excluded(_))))
        }
        _ => true,
    }
}

fn byte_bounds((start, end): KeyBounds) -> Bounds {
    (start.map(ByteView::from), end.map(ByteView::from))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(sequence: u64, value: Option<&str>) -> Version {
        Version {
            sequence,
            value: value.map(|value| ByteView::from(value.as_bytes())),
            transactional: true,
        }
    }

    #[test]
    fn prune_keeps_readable() {
        let mut versions = vec![
            version(1, Some("a")),
            version(3, Some("b")),
            version(5, None),
        ];
        prune(&mut versions, 4);
        assert_eq!(versions, [version(3, Some("b")), version(5, None)]);
        prune(&mut versions, u64::MAX);
        assert!(versions.is_empty(), "a delete nobody reads behind goes");
    }

    #[test]
    fn clear_prunes_closed() {
        let key = |text: &str| ByteView::from(text.as_bytes());
        let mut rows = Committed::default();
        let write = |k: &str, v: &str| {
            Changes::from([(SPACE.to_string(), Rows::from([(key(k), Some(key(v)))]))])
        };
        rows.apply(write("a", "1"), 1);
        rows.clear(SPACE, 1);
        let all = (Unbounded, Unbounded);
        assert_eq!(rows.get(SPACE, b"a", 1), Some(key("1")));
        assert_eq!(rows.get(SPACE, b"a", 2), None);
        assert!(!rows.changed(SPACE, &all, 1));
        assert_eq!(rows.spaces[SPACE].len(), 1);

        let before = rows.sequence;
        rows.clear(SPACE, u64::MAX);
        assert_eq!(rows.sequence, before, "pruning dead rows does not bump seq");
        assert!(rows.spaces[SPACE].is_empty(), "closed delete goes away");

        // A transactional write before a clear still conflicts with an earlier snapshot.
        rows.apply(write("b", "1"), 2);
        let snap = rows.sequence;
        rows.apply(write("b", "2"), snap);
        rows.clear(SPACE, snap);
        assert!(rows.changed(SPACE, &all, snap));
    }

    #[test]
    fn empty_bounds_skipped() {
        let key = |text: &str| ByteView::from(text.as_bytes());
        assert!(!ordered(&(Excluded(key("b")), Excluded(key("b")))));
        assert!(!ordered(&(Included(key("c")), Excluded(key("b")))));
        assert!(ordered(&(Included(key("b")), Included(key("b")))));
        let mut rows = Committed::default();
        let changes =
            Changes::from([(SPACE.to_string(), Rows::from([(key("b"), Some(key("v")))]))]);
        rows.apply(changes, u64::MAX);
        let bounds = (Excluded(key("b")), Excluded(key("b")));
        assert_eq!(rows.scan(SPACE, 1, None, &bounds, false).count(), 0);
        assert!(!rows.changed(SPACE, &bounds, 0));
    }

    const SPACE: &str = "rows";
}
