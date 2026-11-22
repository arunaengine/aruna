//! Caches committed point reads of read-mostly keyspaces outside transactions. The write actor
//! drops changed rows before it replies, so a read after an answered write never sees old data.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::{AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE};
use byteview::ByteView;
use ulid::Ulid;

/// Keyspaces whose point reads are cached; a row names one by its index here.
const CACHED_KEYSPACES: [&str; 3] = [REALM_CONFIG_KEYSPACE, AUTH_KEYSPACE, S3_BUCKET_KEYSPACE];
/// Entries across all cached keyspaces; a full cache is cleared before the next new entry.
pub(super) const CAPACITY: usize = 1024;

/// The index of a cached keyspace and a key in it.
pub(super) type Row = (usize, ByteView);

/// The index of `key_space` in the cached keyspaces, if it is one.
pub(super) fn cached_slot(key_space: &str) -> Option<usize> {
    CACHED_KEYSPACES
        .iter()
        .position(|cached| *cached == key_space)
}

#[derive(Debug, Default)]
pub(super) struct HotCache {
    /// Moves at the start and end of every change, so a read fetched before it is never kept.
    generation: AtomicU64,
    state: RwLock<State>,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<Row, Option<ByteView>>,
    /// Changes whose commit is running; every lookup misses meanwhile.
    writing: usize,
}

impl HotCache {
    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The cached value of `row`: `None` on a miss, `Some(None)` for a cached absent row.
    pub(super) fn get(&self, row: &Row) -> Option<Option<ByteView>> {
        let state = self.state.read().expect("hot cache lock poisoned");
        match state.writing {
            0 => state.entries.get(row).cloned(),
            _ => None,
        }
    }

    /// Looks up many rows under one lock, so all hits come from the same committed state.
    pub(super) fn lookup(&self, rows: &[Option<Row>]) -> Vec<Option<Option<ByteView>>> {
        let state = self.state.read().expect("hot cache lock poisoned");
        let hit = |row: &Option<Row>| match (row, state.writing) {
            (Some(row), 0) => state.entries.get(row).cloned(),
            _ => None,
        };
        rows.iter().map(hit).collect()
    }

    /// Keeps values read after `generation` was taken, unless a change began since then.
    pub(super) fn fill(
        &self,
        generation: u64,
        values: impl IntoIterator<Item = (Row, Option<ByteView>)>,
    ) {
        let mut state = self.state.write().expect("hot cache lock poisoned");
        if state.writing > 0 || self.generation() != generation {
            return;
        }
        for (row, value) in values {
            if state.entries.len() >= CAPACITY && !state.entries.contains_key(&row) {
                state.entries.clear();
            }
            state.entries.insert(row, value);
        }
    }

    /// Marks a commit of `rows` as running; it must be followed by `finish` with the same rows.
    pub(super) fn begin(&self, rows: &[Row]) {
        if rows.is_empty() {
            return;
        }
        let mut state = self.state.write().expect("hot cache lock poisoned");
        state.writing += 1;
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Drops `rows` once their commit ended, whatever its outcome.
    pub(super) fn finish(&self, rows: &[Row]) {
        if rows.is_empty() {
            return;
        }
        let mut state = self.state.write().expect("hot cache lock poisoned");
        for row in rows {
            state.entries.remove(row);
        }
        state.writing = state.writing.saturating_sub(1);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.state
            .read()
            .expect("hot cache lock poisoned")
            .entries
            .len()
    }
}

/// The write actor's side of the cache: the shared cache and the cached rows each open
/// transaction changed, dropped from the cache when it commits.
#[derive(Default)]
pub(super) struct CacheWriter {
    shared: Option<Arc<HotCache>>,
    txns: HashMap<Ulid, Vec<Row>>,
}

impl CacheWriter {
    pub(super) fn new(shared: Option<Arc<HotCache>>) -> Self {
        Self {
            shared,
            txns: HashMap::new(),
        }
    }

    /// Cached rows `effect` writes or deletes; none without a cache.
    pub(super) fn rows(&self, effect: &StorageEffect) -> Vec<Row> {
        if self.shared.is_none() {
            return Vec::new();
        }
        let row = |key_space: &str, key: &ByteView| Some((cached_slot(key_space)?, key.clone()));
        match effect {
            StorageEffect::Write { key_space, key, .. }
            | StorageEffect::Delete { key_space, key, .. } => {
                row(key_space, key).into_iter().collect()
            }
            StorageEffect::BatchWrite { writes, .. } => writes
                .iter()
                .filter_map(|(key_space, key, _)| row(key_space, key))
                .collect(),
            StorageEffect::BatchDelete { deletes, .. } => deletes
                .iter()
                .filter_map(|(key_space, key)| row(key_space, key))
                .collect(),
            _ => Vec::new(),
        }
    }

    pub(super) fn begin(&self, rows: &[Row]) {
        if let Some(cache) = &self.shared {
            cache.begin(rows);
        }
    }

    pub(super) fn finish(&self, rows: &[Row]) {
        if let Some(cache) = &self.shared {
            cache.finish(rows);
        }
    }

    /// Remembers rows an open transaction changed until it commits or aborts.
    pub(super) fn hold(&mut self, txn_id: Ulid, rows: Vec<Row>) {
        if !rows.is_empty() {
            self.txns.entry(txn_id).or_default().extend(rows);
        }
    }

    /// Takes the rows a transaction changed, at its commit or abort.
    pub(super) fn take(&mut self, txn_id: Ulid) -> Vec<Row> {
        self.txns.remove(&txn_id).unwrap_or_default()
    }
}
