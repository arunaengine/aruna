//! Runs the single write actor thread, the read pools and the worker-side transaction table.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::effects::{IterStart, StorageEffect, StoragePriority};
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::prefix_upper_bound;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::telemetry::duration_ms;
use byteview::ByteView;
use crossfire::select::Select;
use crossfire::{RecvError, TryRecvError, TrySendError, mpsc};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use tracing::warn;
use ulid::Ulid;

use super::handle::{
    CleanupEntry, CleanupKind, EffectHandle, EffectReceiver, EffectSender, MAX_CLEANUP_ATTEMPTS,
    ResponseSender, StorageReceivers, cleanup_effect, effect_txn_id, finish_cleanup,
    observe_cleanup, storage_effect_mutates,
};
use super::metrics::{StorageMetrics, record_storage_call};
use super::persistence::FjallPersistPolicy;
use super::telemetry::{storage_effect_kind, storage_event_kind};
use crate::compaction::Compactor;

pub(super) enum Txn {
    Read(fjall::Snapshot),
    Write(Box<fjall::OptimisticWriteTx>),
}
type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);
pub(super) fn effect_keyspace(effect: &StorageEffect) -> Option<&str> {
    match effect {
        StorageEffect::Read { key_space, .. }
        | StorageEffect::Write { key_space, .. }
        | StorageEffect::Delete { key_space, .. }
        | StorageEffect::Iter { key_space, .. }
        | StorageEffect::Last { key_space, .. } => Some(key_space),
        StorageEffect::BatchRead { reads, .. } => {
            reads.first().map(|(key_space, _)| key_space.as_str())
        }
        StorageEffect::BatchWrite { writes, .. } => {
            writes.first().map(|(key_space, _, _)| key_space.as_str())
        }
        StorageEffect::AddUsage { key_space, .. } => Some(key_space),
        StorageEffect::BatchDelete { deletes, .. } => {
            deletes.first().map(|(key_space, _)| key_space.as_str())
        }
        StorageEffect::StartTransaction { .. }
        | StorageEffect::CommitTransaction { .. }
        | StorageEffect::AbortTransaction { .. }
        | StorageEffect::SyncAll => None,
    }
}
const STORAGE_EFFECT_THRESHOLD: Duration = Duration::from_millis(50);
const QUEUE_LOG_INTERVAL: Duration = Duration::from_secs(1);
pub(super) const MAX_GROUP_COMMIT: usize = 256;
pub(super) const READ_POOL_THREADS: usize = 4;
pub(super) const BULK_POOL_THREADS: usize = 2;
// Count foreground effects so large batches cannot starve the bulk lane under load.
pub(super) const FOREGROUND_PER_BULK: usize = 8;
#[derive(Clone)]
pub(super) struct Store {
    pub(super) db: OptimisticTxDatabase,
    pub(super) keyspaces: Arc<Mutex<HashMap<String, OptimisticTxKeyspace>>>,
}

impl Store {
    pub(super) fn new(db: OptimisticTxDatabase) -> Self {
        Self {
            db,
            keyspaces: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn resolve_keyspace(
        &self,
        name: &str,
    ) -> Result<OptimisticTxKeyspace, StorageError> {
        if let Some(ks) = self
            .keyspaces
            .lock()
            .expect("storage keyspace cache mutex poisoned")
            .get(name)
        {
            return Ok(ks.clone());
        }

        match self.db.keyspace(name, KeyspaceCreateOptions::default) {
            Ok(ks) => {
                let mut keyspaces = self
                    .keyspaces
                    .lock()
                    .expect("storage keyspace cache mutex poisoned");
                Ok(keyspaces
                    .entry(name.to_string())
                    .or_insert_with(|| ks.clone())
                    .clone())
            }
            Err(error) => Err(StorageError::KeyspaceError(error.to_string())),
        }
    }
}

pub struct FjallStorage {
    pub(super) store: Store,
    pub(super) persist_policy: FjallPersistPolicy,
    pub(super) txns: HashMap<Ulid, Txn>,
    pub(super) transaction_cleanup: Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    pub(super) metrics: Arc<StorageMetrics>,
    pub(super) read_pool: Vec<EffectSender>,
    pub(super) next_reader: usize,
    pub(super) bulk_read_pool: Vec<EffectSender>,
    pub(super) next_bulk_reader: usize,
    pub(super) pool_threads: Vec<thread::JoinHandle<()>>,
    pub(super) compactor: Compactor,
    /// Committed deletes per keyspace since its last compaction.
    pub(super) deletes: HashMap<String, u64>,
    pub(super) txn_deletes: HashMap<Ulid, HashMap<String, u64>>,
    pub(super) txn_usage: HashMap<Ulid, Vec<(String, ByteView, UsageDelta)>>,
}

impl FjallStorage {
    /// True when the drain fence is up and this effect would start a mutation
    /// that has not begun executing yet.
    fn fenced_mutation(&self, effect: &StorageEffect) -> bool {
        storage_effect_mutates(effect) && self.metrics.mutations_fenced.load(Ordering::SeqCst)
    }

    /// Rejects a mutation the fence caught before it started, leaving no open
    /// transaction and no cleanup entry behind it.
    fn reject_fenced(&mut self, effect: &StorageEffect) -> StorageEvent {
        if let Some(txn_id) = effect_txn_id(effect) {
            self.retire_fenced_txn(txn_id);
        }
        self.metrics.rejected_writes.fetch_add(1, Ordering::Relaxed);
        warn!(
            event = "storage.write.after_fence",
            operation = storage_effect_kind(effect),
            "Rejected a storage mutation issued after the shutdown drain fence"
        );
        StorageEvent::Error {
            error: StorageError::Closed,
        }
    }

    /// Rolls a fenced transaction back and retires its cleanup entry through the
    /// terminal `Aborted` state, whether it was open or had a commit queued.
    fn retire_fenced_txn(&mut self, txn_id: Ulid) {
        self.take_txn_deletes(txn_id, false);
        self.txn_usage.remove(&txn_id);
        if let Some(Txn::Write(txn)) = self.txns.remove(&txn_id) {
            txn.rollback();
        }
        if let Some(entry) = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get_mut(&txn_id)
        {
            entry.kind = CleanupKind::Aborted;
            entry.attempts = 0;
            entry.queued = false;
        }
        finish_cleanup(&self.transaction_cleanup, txn_id);
    }

    pub(super) fn process_effect(&mut self, effect: StorageEffect) -> StorageEvent {
        match effect {
            StorageEffect::StartTransaction { read } => self.start_transaction(read),
            StorageEffect::AbortTransaction { txn_id } => self.abort_transaction(txn_id),
            StorageEffect::SyncAll => self.sync_all(),
            StorageEffect::Read {
                key_space,
                key,
                txn_id,
            } => self.read(key_space, key, txn_id),
            StorageEffect::BatchRead { reads, txn_id } => self.batch_read(reads, txn_id),
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            } => self.write(key_space, key, value, txn_id),
            StorageEffect::BatchWrite { writes, txn_id } => self.batch_write(writes, txn_id),
            StorageEffect::AddUsage {
                key_space,
                deltas,
                txn_id,
            } => self.add_usage(key_space, deltas, txn_id),
            StorageEffect::CommitTransaction { txn_id } => self.commit_transaction(txn_id),
            StorageEffect::Delete {
                key_space,
                key,
                txn_id,
            } => self.delete(key_space, key, txn_id),
            StorageEffect::BatchDelete { deletes, txn_id } => self.batch_delete(deletes, txn_id),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id,
            } => self.iterate(key_space, prefix, start, limit, txn_id),
            StorageEffect::Last {
                key_space,
                prefix,
                txn_id,
            } => self.last(key_space, prefix, txn_id),
        }
    }

    pub(super) fn observe_cleanup(
        &self,
        cleanup: Option<(Ulid, CleanupKind)>,
        event: &StorageEvent,
    ) -> bool {
        observe_cleanup(&self.transaction_cleanup, cleanup, event)
    }

    pub(super) fn retry_cleanup(&mut self) {
        let retry = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .iter()
            .filter_map(|(txn_id, entry)| {
                // Probe unknown commits with abort: open transactions abort, resolved ones return NotFound.
                // Do not race queued entries owned by an in-flight effect.
                (matches!(entry.kind, CleanupKind::Abort | CleanupKind::CommitUnknown)
                    && entry.attempts < MAX_CLEANUP_ATTEMPTS
                    && !entry.queued)
                    .then_some((*txn_id, entry.kind))
            })
            .collect::<Vec<_>>();
        for (txn_id, kind) in retry {
            // Past the fence an unknown commit must not be reissued: roll the
            // transaction back if it is still open and retire the entry.
            if matches!(kind, CleanupKind::CommitUnknown)
                && self.metrics.mutations_fenced.load(Ordering::SeqCst)
            {
                self.retire_fenced_txn(txn_id);
                continue;
            }
            // A commit with an unknown outcome is re-issued, never aborted: the
            // original commit may still sit in the queue behind this retry.
            let event = match kind {
                CleanupKind::CommitUnknown => self.commit_transaction(txn_id),
                _ => self.abort_transaction(txn_id),
            };
            if self.observe_cleanup(Some((txn_id, kind)), &event) {
                finish_cleanup(&self.transaction_cleanup, txn_id);
            }
        }
    }

    #[tracing::instrument(name = "storage.receive_loop", level = "debug", skip(self, receivers))]
    pub fn receive_loop(&mut self, receivers: StorageReceivers) {
        let mut slow_queue = SlowQueueAggregator::default();
        let mut lanes = LaneScheduler::default();
        loop {
            let (first, priority) = match lanes.next(&receivers) {
                Ok(pair) => pair,
                Err(_) => {
                    tracing::warn!(
                        "Storage receiver channel closed, shutting down storage thread."
                    );
                    break;
                }
            };
            match priority {
                StoragePriority::Foreground => {
                    let served = self.serve_foreground_batch(&receivers, first, &mut slow_queue);
                    lanes.record_foreground(served);
                }
                StoragePriority::Bulk => {
                    if is_poolable_read(&first.0) {
                        self.forward_read(first, StoragePriority::Bulk, &mut slow_queue);
                    } else {
                        self.process_single(first, &mut slow_queue);
                    }
                }
            }
        }
    }

    /// Returns how many foreground effects were served, which is what the lane
    /// scheduler credits the bulk lane against.
    fn serve_foreground_batch(
        &mut self,
        receivers: &StorageReceivers,
        first: EffectHandle,
        slow_queue: &mut SlowQueueAggregator,
    ) -> usize {
        let mut pending = Vec::with_capacity(8);
        pending.push(first);
        while pending.len() < MAX_GROUP_COMMIT {
            match receivers.foreground.try_recv() {
                Ok(item) => pending.push(item),
                Err(_) => break,
            }
        }
        let served = pending.len();

        let mut group: Vec<EffectHandle> = Vec::new();
        let mut group_index: Option<PendingWriteIndex> = None;
        for item in pending {
            if is_groupable_write(&item.0) {
                if let Some(index) = &mut group_index {
                    index.insert(&item.0);
                }
                group.push(item);
                continue;
            }
            if is_poolable_read(&item.0) {
                let conflicts = !group.is_empty()
                    && group_index
                        .get_or_insert_with(|| PendingWriteIndex::from_group(&group))
                        .conflicts_with_read(&item.0);
                if conflicts {
                    self.flush_write_group(&mut group, slow_queue);
                    group_index = None;
                }
                self.forward_read(item, StoragePriority::Foreground, slow_queue);
                continue;
            }
            self.flush_write_group(&mut group, slow_queue);
            group_index = None;
            self.process_single(item, slow_queue);
        }
        self.flush_write_group(&mut group, slow_queue);
        self.retry_cleanup();
        served
    }

    pub(super) fn forward_read(
        &mut self,
        item: EffectHandle,
        priority: StoragePriority,
        slow_queue: &mut SlowQueueAggregator,
    ) {
        match priority {
            StoragePriority::Foreground => {
                let reader = self.next_reader % self.read_pool.len();
                self.next_reader = self.next_reader.wrapping_add(1);
                match self.read_pool[reader].try_send(item) {
                    Ok(()) => {}
                    Err(TrySendError::Full(item)) | Err(TrySendError::Disconnected(item)) => {
                        self.process_single(item, slow_queue);
                    }
                }
            }
            StoragePriority::Bulk => {
                let reader = self.next_bulk_reader % self.bulk_read_pool.len();
                self.next_bulk_reader = self.next_bulk_reader.wrapping_add(1);
                match self.bulk_read_pool[reader].try_send(item) {
                    Ok(()) => {}
                    // A full bulk read queue backpressures rather than stealing
                    // the write actor thread from foreground sync traffic.
                    Err(TrySendError::Full(item)) => reject_bulk_read(item),
                    Err(TrySendError::Disconnected(item)) => self.process_single(item, slow_queue),
                }
            }
        }
    }

    pub(super) fn process_single(
        &mut self,
        item: EffectHandle,
        slow_queue: &mut SlowQueueAggregator,
    ) {
        let (effect, mut response_tx, span, enqueued_at, in_flight) = item;
        let _guard = span.enter();
        let operation = storage_effect_kind(&effect);
        let key_space = effect_keyspace(&effect).map(str::to_string);
        let cleanup = cleanup_effect(&effect);
        let queue_wait = enqueued_at.elapsed();
        span.record("queue_wait_ms", duration_ms(queue_wait));

        if response_tx.is_closed()
            && !matches!(
                effect,
                StorageEffect::AbortTransaction { .. } | StorageEffect::CommitTransaction { .. }
            )
        {
            warn!(
                event = "storage.request.abandoned",
                operation, "Skipping abandoned storage request"
            );
            return;
        }

        let service_started = Instant::now();
        let event = if self.fenced_mutation(&effect) {
            self.reject_fenced(&effect)
        } else {
            self.process_effect(effect)
        };
        self.observe_cleanup(cleanup, &event);
        response_tx.observe(&event);
        self.retry_cleanup();
        let service_elapsed = service_started.elapsed();
        let total_elapsed = enqueued_at.elapsed();
        let result = storage_event_kind(&event);
        span.record("service_ms", duration_ms(service_elapsed));
        span.record("total_elapsed_ms", duration_ms(total_elapsed));
        span.record("result", result);
        slow_queue.observe(
            operation,
            key_space.as_deref(),
            queue_wait,
            service_elapsed,
            result,
        );
        drop(in_flight);
        Self::deliver_response(response_tx, event, operation, result);
    }

    pub(super) fn deliver_response(
        response_tx: ResponseSender,
        event: StorageEvent,
        operation: &'static str,
        result: &'static str,
    ) {
        if !response_tx.send(event) {
            warn!(
                event = "storage.response.abandoned",
                operation, result, "Dropping storage response for failed delivery"
            );
        }
    }

    fn flush_write_group(
        &mut self,
        group: &mut Vec<EffectHandle>,
        slow_queue: &mut SlowQueueAggregator,
    ) {
        if group.is_empty() {
            return;
        }
        // A batch the fence caught before it started never applies: its members
        // are rejected one by one instead.
        if self.metrics.mutations_fenced.load(Ordering::SeqCst) {
            for item in std::mem::take(group) {
                self.process_single(item, slow_queue);
            }
            return;
        }
        if group.len() == 1 {
            let item = group.pop().expect("group has one item");
            self.process_single(item, slow_queue);
            return;
        }

        let mut members = std::mem::take(group);
        members.retain(|item| {
            if item.1.is_closed() {
                let _guard = item.2.enter();
                warn!(
                    event = "storage.request.abandoned",
                    operation = storage_effect_kind(&item.0),
                    "Skipping abandoned storage request"
                );
                false
            } else {
                true
            }
        });
        if members.is_empty() {
            return;
        }
        if members.len() == 1 {
            let item = members.pop().expect("group has one item");
            self.process_single(item, slow_queue);
            return;
        }

        let service_started = Instant::now();
        let tx = match self.buffered_write_tx() {
            Ok(tx) => tx,
            Err(_) => {
                for item in members {
                    self.process_single(item, slow_queue);
                }
                return;
            }
        };

        let mut tx = tx;
        let mut prepared = Vec::with_capacity(members.len());
        for item in members {
            match self.apply_group_member(&mut tx, &item.0) {
                Ok(event) => prepared.push((item, Ok(event))),
                Err(error) => prepared.push((item, Err(error))),
            }
        }

        let group_error = match self.commit_buffered(tx) {
            Ok(()) => self.persist_journal().err(),
            Err(StorageError::TransactionConflict) => {
                for (item, _) in prepared {
                    self.process_single(item, slow_queue);
                }
                return;
            }
            Err(error) => Some(error),
        };

        if group_error.is_none() {
            for ((effect, ..), outcome) in &prepared {
                if outcome.is_ok() {
                    self.note_effect_deletes(effect);
                }
            }
        }

        let service_elapsed = service_started.elapsed();
        for ((effect, response_tx, span, enqueued_at, in_flight), outcome) in prepared {
            let _guard = span.enter();
            let queue_wait = enqueued_at.elapsed().saturating_sub(service_elapsed);
            let event = match outcome {
                Ok(event) => match &group_error {
                    Some(error) => StorageEvent::Error {
                        error: error.clone(),
                    },
                    None => event,
                },
                Err(error) => StorageEvent::Error { error },
            };
            let result = storage_event_kind(&event);
            span.record("queue_wait_ms", duration_ms(queue_wait));
            span.record("service_ms", duration_ms(service_elapsed));
            span.record("result", result);
            span.record("path", "group_commit");
            slow_queue.observe(
                storage_effect_kind(&effect),
                effect_keyspace(&effect),
                queue_wait,
                service_elapsed,
                result,
            );
            drop(in_flight);
            let _ = response_tx.send(event);
        }
    }

    fn apply_group_member(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        effect: &StorageEffect,
    ) -> Result<StorageEvent, StorageError> {
        match effect {
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            } => {
                let keyspace = self.store.resolve_keyspace(key_space)?;
                tx.insert(keyspace, key.clone(), value.clone());
                Ok(StorageEvent::WriteResult { key: key.clone() })
            }
            StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            } => {
                let mut entries = Vec::with_capacity(writes.len());
                let mut resolved = Vec::with_capacity(writes.len());
                for (key_space, key, value) in writes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
                    resolved.push((keyspace, key_space, key, value));
                }
                for (keyspace, key_space, key, value) in resolved {
                    tx.insert(keyspace, key.clone(), value.clone());
                    entries.push((key_space.clone(), key.clone()));
                }
                Ok(StorageEvent::BatchWriteResult { entries })
            }
            StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            } => {
                let keyspace = self.store.resolve_keyspace(key_space)?;
                tx.remove(keyspace, key.clone());
                Ok(StorageEvent::DeleteResult { key: key.clone() })
            }
            StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            } => {
                let mut entries = Vec::with_capacity(deletes.len());
                let mut resolved = Vec::with_capacity(deletes.len());
                for (key_space, key) in deletes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
                    resolved.push((keyspace, key_space, key));
                }
                for (keyspace, key_space, key) in resolved {
                    tx.remove(keyspace, key.clone());
                    entries.push((key_space.clone(), key.clone()));
                }
                Ok(StorageEvent::BatchDeleteResult { entries })
            }
            _ => Err(StorageError::InvalidEffect),
        }
    }
}
pub(super) fn store_read(
    store: &Store,
    keyspace: OptimisticTxKeyspace,
    key: ByteView,
) -> StorageEvent {
    let snapshot = store.db.read_tx();
    match snapshot.get(&keyspace, &key) {
        Ok(value_opt) => StorageEvent::ReadResult {
            key,
            value: value_opt.map(|v| v.into()),
        },
        Err(error) => StorageEvent::Error {
            error: StorageError::ReadError(error.to_string()),
        },
    }
}

pub(super) fn batch_read_with<R: Readable>(
    store: &Store,
    reader: &R,
    reads: Vec<(String, ByteView)>,
) -> StorageEvent {
    let mut values = Vec::with_capacity(reads.len());
    for (key_space, key) in reads {
        let keyspace = match store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(error) => return StorageEvent::Error { error },
        };
        match reader.get(&keyspace, &key) {
            Ok(value_opt) => values.push((key, value_opt.map(Into::into))),
            Err(error) => {
                return StorageEvent::Error {
                    error: StorageError::ReadError(error.to_string()),
                };
            }
        }
    }
    StorageEvent::BatchReadResult { values }
}

pub(super) fn store_batch_read(store: &Store, reads: Vec<(String, ByteView)>) -> StorageEvent {
    let snapshot = store.db.read_tx();
    batch_read_with(store, &snapshot, reads)
}

pub(super) fn read_last_with<R: Readable>(
    reader: &R,
    keyspace: &OptimisticTxKeyspace,
    prefix: Option<&ByteView>,
) -> StorageEvent {
    let guard = match prefix {
        Some(prefix) => {
            let prefix = prefix.as_ref().to_vec();
            let mut range = match prefix_upper_bound(&prefix) {
                Some(end) => reader.range(keyspace, (Included(prefix), Excluded(end))),
                None => reader.range(keyspace, (Included(prefix), Unbounded::<Vec<u8>>)),
            };
            range.next_back()
        }
        None => reader.last_key_value(keyspace),
    };
    let Some(guard) = guard else {
        return StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        };
    };
    match guard.into_inner() {
        Ok((key, value)) => StorageEvent::IterResult {
            values: vec![(ByteView::from(key.as_ref()), ByteView::from(value.as_ref()))],
            next_start_after: None,
        },
        Err(error) => StorageEvent::Error {
            error: StorageError::ReadError(error.to_string()),
        },
    }
}

pub(super) fn store_last(
    store: &Store,
    keyspace: OptimisticTxKeyspace,
    prefix: Option<ByteView>,
) -> StorageEvent {
    read_last_with(&store.db.read_tx(), &keyspace, prefix.as_ref())
}

pub(super) fn store_iterate(
    store: &Store,
    keyspace: OptimisticTxKeyspace,
    prefix: Option<ByteView>,
    start: Option<IterStart>,
    limit: usize,
) -> StorageEvent {
    let snapshot = store.db.read_tx();
    match iterate_page(&snapshot, &keyspace, prefix.as_ref(), start.as_ref(), limit) {
        Ok((values, next_start_after)) => StorageEvent::IterResult {
            values,
            next_start_after,
        },
        Err(error) => StorageEvent::Error { error },
    }
}

fn is_groupable_write(effect: &StorageEffect) -> bool {
    matches!(
        effect,
        StorageEffect::Write { txn_id: None, .. }
            | StorageEffect::BatchWrite { txn_id: None, .. }
            | StorageEffect::Delete { txn_id: None, .. }
            | StorageEffect::BatchDelete { txn_id: None, .. }
    )
}

fn is_poolable_read(effect: &StorageEffect) -> bool {
    matches!(
        effect,
        StorageEffect::Read { txn_id: None, .. }
            | StorageEffect::BatchRead { txn_id: None, .. }
            | StorageEffect::Iter { txn_id: None, .. }
            | StorageEffect::Last { txn_id: None, .. }
    )
}

#[derive(Default)]
struct PendingWriteIndex {
    key_spaces: Vec<String>,
    keys: Vec<PendingWriteKey>,
    sorted: bool,
}

struct PendingWriteKey {
    key_space: usize,
    key: ByteView,
}

impl PendingWriteIndex {
    fn from_group(group: &[EffectHandle]) -> Self {
        let mut index = Self {
            key_spaces: Vec::with_capacity(group.len()),
            keys: Vec::with_capacity(group.len()),
            sorted: true,
        };
        for (effect, _, _, _, _) in group {
            index.insert(effect);
        }
        index
    }

    fn insert(&mut self, effect: &StorageEffect) {
        match effect {
            StorageEffect::Write {
                key_space,
                key,
                txn_id: None,
                ..
            }
            | StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            } => self.insert_key(key_space, key),
            StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            } => {
                for (key_space, key, _) in writes {
                    self.insert_key(key_space, key);
                }
            }
            StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            } => {
                for (key_space, key) in deletes {
                    self.insert_key(key_space, key);
                }
            }
            _ => {}
        }
    }

    fn insert_key(&mut self, key_space: &str, key: &ByteView) {
        let key_space = self.keyspace_slot(key_space);
        self.keys.push(PendingWriteKey {
            key_space,
            key: key.clone(),
        });
        self.sorted = false;
    }

    fn conflicts_with_read(&mut self, read: &StorageEffect) -> bool {
        match read {
            StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            } => self.contains_key(key_space, key),
            StorageEffect::BatchRead {
                reads,
                txn_id: None,
            } => reads
                .iter()
                .any(|(key_space, key)| self.contains_key(key_space, key)),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id: None,
            } => *limit != 0 && self.contains_iter_key(key_space, prefix.as_ref(), start.as_ref()),
            StorageEffect::Last {
                key_space,
                prefix,
                txn_id: None,
            } => self.contains_iter_key(key_space, prefix.as_ref(), None),
            _ => false,
        }
    }

    fn contains_key(&mut self, key_space: &str, key: &ByteView) -> bool {
        let Some(key_space) = self.key_space_index(key_space) else {
            return false;
        };
        self.sort_keys();
        self.keys
            .binary_search_by(|pending| compare_pending_key(pending, key_space, key.as_ref()))
            .is_ok()
    }

    fn contains_iter_key(
        &mut self,
        key_space: &str,
        prefix: Option<&ByteView>,
        start: Option<&IterStart>,
    ) -> bool {
        let Some(key_space) = self.key_space_index(key_space) else {
            return false;
        };
        self.sort_keys();
        let start_index = self
            .keys
            .partition_point(|pending| pending.key_space < key_space);
        let end_index = self
            .keys
            .partition_point(|pending| pending.key_space <= key_space);
        self.keys[start_index..end_index]
            .iter()
            .any(|pending| iter_matches_key(prefix, start, &pending.key))
    }

    fn key_space_index(&self, key_space: &str) -> Option<usize> {
        self.key_spaces
            .iter()
            .position(|existing| existing.as_str() == key_space)
    }

    fn keyspace_slot(&mut self, key_space: &str) -> usize {
        match self.key_space_index(key_space) {
            Some(index) => index,
            None => {
                self.key_spaces.push(key_space.to_string());
                self.key_spaces.len() - 1
            }
        }
    }

    fn sort_keys(&mut self) {
        if self.sorted {
            return;
        }
        self.keys.sort_unstable_by(|left, right| {
            left.key_space
                .cmp(&right.key_space)
                .then_with(|| left.key.as_ref().cmp(right.key.as_ref()))
        });
        self.keys.dedup_by(|left, right| {
            left.key_space == right.key_space && left.key.as_ref() == right.key.as_ref()
        });
        self.sorted = true;
    }
}

fn compare_pending_key(
    pending: &PendingWriteKey,
    key_space: usize,
    key: &[u8],
) -> std::cmp::Ordering {
    pending
        .key_space
        .cmp(&key_space)
        .then_with(|| pending.key.as_ref().cmp(key))
}

fn iter_matches_key(prefix: Option<&ByteView>, start: Option<&IterStart>, key: &ByteView) -> bool {
    let key = key.as_ref();
    if let Some(prefix) = prefix
        && !key.starts_with(prefix.as_ref())
    {
        return false;
    }
    match start {
        Some(IterStart::After(start)) if key <= start.as_ref() => false,
        Some(IterStart::At(start)) if key < start.as_ref() => false,
        _ => true,
    }
}

/// Lane picker for the write actor. Foreground is preferred, and every
/// [`FOREGROUND_PER_BULK`] foreground effects earn one bulk effect, so the drain
/// keeps a fixed share of the actor no matter how deep the foreground queue is.
#[derive(Debug, Default)]
pub(super) struct LaneScheduler {
    credit: usize,
}

impl LaneScheduler {
    pub(super) fn record_foreground(&mut self, served: usize) {
        self.credit = self.credit.saturating_add(served);
    }

    pub(super) fn next(
        &mut self,
        receivers: &StorageReceivers,
    ) -> Result<(EffectHandle, StoragePriority), RecvError> {
        loop {
            if self.credit >= FOREGROUND_PER_BULK
                && let Ok(item) = receivers.bulk.try_recv()
            {
                self.credit = self.credit.saturating_sub(FOREGROUND_PER_BULK);
                return Ok((item, StoragePriority::Bulk));
            }
            let foreground = receivers.foreground.try_recv();
            if let Ok(item) = foreground {
                return Ok((item, StoragePriority::Foreground));
            }
            let bulk = receivers.bulk.try_recv();
            if let Ok(item) = bulk {
                self.credit = 0;
                return Ok((item, StoragePriority::Bulk));
            }
            match (foreground, bulk) {
                (Err(TryRecvError::Disconnected), Err(TryRecvError::Disconnected)) => {
                    return Err(RecvError);
                }
                (Err(TryRecvError::Disconnected), _) => {
                    self.credit = 0;
                    return receivers
                        .bulk
                        .recv()
                        .map(|item| (item, StoragePriority::Bulk));
                }
                (_, Err(TryRecvError::Disconnected)) => {
                    self.credit = 0;
                    return receivers
                        .foreground
                        .recv()
                        .map(|item| (item, StoragePriority::Foreground));
                }
                _ => {}
            }
            // Both lanes are open but empty: block on a biased select so an idle
            // node sleeps with zero wakeups and foreground stays preferred.
            self.credit = 0;
            let mut select = Select::new_bias();
            select.add(&receivers.foreground);
            select.add(&receivers.bulk);
            let received = match select.select() {
                Ok(result) if result == receivers.foreground => receivers
                    .foreground
                    .read_select(result)
                    .map(|item| (item, StoragePriority::Foreground)),
                Ok(result) => receivers
                    .bulk
                    .read_select(result)
                    .map(|item| (item, StoragePriority::Bulk)),
                Err(RecvError) => return Err(RecvError),
            };
            if let Ok(pair) = received {
                return Ok(pair);
            }
        }
    }
}

fn reject_bulk_read(item: EffectHandle) {
    let (effect, response_tx, span, _enqueued_at, in_flight) = item;
    let _guard = span.enter();
    warn!(
        event = "storage.bulk_read.queue_full",
        operation = storage_effect_kind(&effect),
        "Rejecting bulk read: bulk read pool queue full"
    );
    drop(in_flight);
    let _ = response_tx.send(StorageEvent::Error {
        error: StorageError::QueueFull,
    });
}

pub(super) fn spawn_read_pool(
    store: Store,
    threads: usize,
    capacity: usize,
) -> (Vec<EffectSender>, Vec<thread::JoinHandle<()>>) {
    let mut senders = Vec::with_capacity(threads);
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let (sender, receiver) = mpsc::bounded_blocking(capacity);
        let store = store.clone();
        handles.push(thread::spawn(move || read_pool_loop(store, receiver)));
        senders.push(sender);
    }
    (senders, handles)
}

fn read_pool_loop(store: Store, receiver: EffectReceiver) {
    while let Ok((effect, response_tx, span, enqueued_at, in_flight)) = receiver.recv() {
        let _guard = span.enter();
        if response_tx.is_closed() {
            continue;
        }
        let operation = storage_effect_kind(&effect);
        let key_space = effect_keyspace(&effect).map(str::to_string);
        let queue_wait = enqueued_at.elapsed();
        let service_started = Instant::now();
        let event = match effect {
            StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            } => match store.resolve_keyspace(&key_space) {
                Ok(keyspace) => store_read(&store, keyspace, key),
                Err(error) => StorageEvent::Error { error },
            },
            StorageEffect::Last {
                key_space,
                prefix,
                txn_id: None,
            } => match store.resolve_keyspace(&key_space) {
                Ok(keyspace) => store_last(&store, keyspace, prefix),
                Err(error) => StorageEvent::Error { error },
            },
            StorageEffect::BatchRead {
                reads,
                txn_id: None,
            } => store_batch_read(&store, reads),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id: None,
            } => match store.resolve_keyspace(&key_space) {
                Ok(keyspace) => {
                    if limit == 0 {
                        StorageEvent::IterResult {
                            values: Vec::new(),
                            next_start_after: None,
                        }
                    } else {
                        store_iterate(&store, keyspace, prefix, start, limit)
                    }
                }
                Err(error) => StorageEvent::Error { error },
            },
            _ => StorageEvent::Error {
                error: StorageError::InvalidEffect,
            },
        };
        let service_elapsed = service_started.elapsed();
        span.record("queue_wait_ms", duration_ms(queue_wait));
        span.record("service_ms", duration_ms(service_elapsed));
        span.record("result", storage_event_kind(&event));
        span.record("path", "read_pool");
        record_storage_call(operation, key_space.as_deref(), queue_wait, service_elapsed);
        if service_elapsed >= STORAGE_EFFECT_THRESHOLD {
            warn!(
                event = "storage.effect.slow",
                operation = storage_event_kind(&event),
                service_ms = duration_ms(service_elapsed),
                queue_wait_ms = duration_ms(queue_wait),
                "Slow storage read"
            );
        }
        drop(in_flight);
        let _ = response_tx.send(event);
    }
}

#[derive(Default)]
pub(super) struct SlowQueueAggregator {
    queued_count: u64,
    max_queue_wait: Duration,
    last_flush: Option<Instant>,
}

impl SlowQueueAggregator {
    fn observe(
        &mut self,
        operation: &'static str,
        key_space: Option<&str>,
        queue_wait: Duration,
        service_elapsed: Duration,
        result: &'static str,
    ) {
        record_storage_call(operation, key_space, queue_wait, service_elapsed);
        if service_elapsed >= STORAGE_EFFECT_THRESHOLD {
            warn!(
                event = "storage.effect.slow",
                operation,
                result,
                queue_wait_ms = duration_ms(queue_wait),
                service_ms = duration_ms(service_elapsed),
                threshold_ms = duration_ms(STORAGE_EFFECT_THRESHOLD),
                "Slow storage effect"
            );
        }
        if queue_wait < STORAGE_EFFECT_THRESHOLD {
            return;
        }
        self.queued_count += 1;
        self.max_queue_wait = self.max_queue_wait.max(queue_wait);
        let now = Instant::now();
        let due = self
            .last_flush
            .is_none_or(|last| now.duration_since(last) >= QUEUE_LOG_INTERVAL);
        if due {
            warn!(
                event = "storage.queue.backlog",
                slow_queued_effects = self.queued_count,
                max_queue_wait_ms = duration_ms(self.max_queue_wait),
                threshold_ms = duration_ms(STORAGE_EFFECT_THRESHOLD),
                "Storage effects waited longer than threshold in queue"
            );
            self.queued_count = 0;
            self.max_queue_wait = Duration::ZERO;
            self.last_flush = Some(now);
        }
    }
}
pub(super) fn iterate_page<R: Readable>(
    reader: &R,
    keyspace: &OptimisticTxKeyspace,
    prefix: Option<&ByteView>,
    start: Option<&IterStart>,
    limit: usize,
) -> Result<PageResult, StorageError> {
    let prefix_bytes = prefix.map(|p| p.as_ref().to_vec());
    let start_bound = start.map(|start| match start {
        IterStart::After(key) => Excluded(key.as_ref().to_vec()),
        IterStart::At(key) => Included(key.as_ref().to_vec()),
    });

    let iter = match (prefix_bytes.as_ref(), start_bound) {
        (Some(prefix), Some(start_bound)) => {
            let start_bound = match start_bound {
                Excluded(key) | Included(key) if &key < prefix => Included(prefix.clone()),
                bound => bound,
            };

            match prefix_upper_bound(prefix) {
                Some(end) => reader.range(keyspace, (start_bound, Excluded(end))),
                None => reader.range(keyspace, (start_bound, Unbounded::<Vec<u8>>)),
            }
        }
        (Some(prefix), None) => match prefix_upper_bound(prefix) {
            Some(end) => reader.range(keyspace, (Included(prefix.clone()), Excluded(end))),
            None => reader.range(keyspace, (Included(prefix.clone()), Unbounded::<Vec<u8>>)),
        },
        (None, Some(start_bound)) => reader.range(keyspace, (start_bound, Unbounded::<Vec<u8>>)),
        (None, None) => reader.iter(keyspace),
    };

    collect_page(iter, limit)
}

pub(super) fn collect_page(iter: fjall::Iter, limit: usize) -> Result<PageResult, StorageError> {
    let mut iter = iter.peekable();
    let mut values: Vec<(ByteView, ByteView)> = Vec::with_capacity(limit.min(1024));

    while let Some(guard) = iter.next() {
        let (key, value) = guard
            .into_inner()
            .map_err(|error| StorageError::ReadError(error.to_string()))?;
        values.push((ByteView::from(key.as_ref()), ByteView::from(value.as_ref())));

        if values.len() == limit {
            let next_start_after = if iter.peek().is_some() {
                values.last().map(|(k, _)| k.clone())
            } else {
                None
            };
            return Ok((values, next_start_after));
        }
    }

    Ok((values, None))
}
