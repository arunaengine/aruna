//! Begins and finishes usage rebuilds and copies committed usage deltas onto rebuild rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::effects::IterStart;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
use aruna_core::structs::storage::usage::{
    RebuildProgress, ShadowCounters, USAGE_COMPLETE_KEY, USAGE_PROGRESS_KEY, USAGE_REBUILD_KEY,
    USAGE_SHADOW_PREFIX, UsageDelta, global_shard_keys, usage_shadow_key,
};
use byteview::ByteView;
use fjall::Readable;
use tracing::warn;
use ulid::Ulid;

use super::transactions::{CounterChange, warn_clamped};
use super::{FjallStorage, Store, iterate_page};

/// Rows read per page while a rebuild clears or replaces counter rows.
const REBUILD_PAGE: usize = 1024;

type PendingChanges = Vec<(String, ByteView, CounterChange)>;

/// The usage keyspace whose rebuild was begun before the last stop and saved its progress.
pub(super) fn running_rebuild(store: &Store) -> HashSet<String> {
    let marker = store
        .existing_keyspace(USAGE_STATS_KEYSPACE)
        .ok()
        .flatten()
        .and_then(|keyspace| store.db.read_tx().get(&keyspace, USAGE_PROGRESS_KEY).ok())
        .flatten();
    marker
        .filter(|value| RebuildProgress::from_bytes(value.as_ref()).is_ok())
        .map(|_| USAGE_STATS_KEYSPACE.to_string())
        .into_iter()
        .collect()
}

/// True for the rebuild marker and the rebuild rows, which are never copied themselves.
pub(super) fn is_rebuild_row(key: &[u8]) -> bool {
    key == USAGE_REBUILD_KEY || key == USAGE_PROGRESS_KEY || key.starts_with(USAGE_SHADOW_PREFIX)
}

/// The new value of a rebuild row after `change`; rebuild rows hold signed sums.
pub(super) fn shadow_row(
    stored: Option<&[u8]>,
    change: CounterChange,
) -> Result<Vec<u8>, StorageError> {
    let failed = |message: String| StorageError::WriteError(message);
    let change = match change {
        CounterChange::Usage(delta) => ShadowCounters::Usage(delta),
        CounterChange::Bucket(delta) => ShadowCounters::Bucket(delta),
        CounterChange::Range(_) => return Err(failed("range change on a rebuild row".into())),
    };
    let row = match stored {
        Some(bytes) => ShadowCounters::from_bytes(bytes)
            .map_err(|error| failed(error.to_string()))?
            .merge(change)
            .ok_or_else(|| failed("rebuild row kinds differ".into()))?,
        None => change,
    };
    row.to_bytes().map_err(|error| failed(error.to_string()))
}

impl FjallStorage {
    /// Adds each usage change of a rebuilding keyspace to its rebuild row in the same commit.
    pub(super) fn shadow_changes(&self, pending: &mut PendingChanges) -> Result<(), StorageError> {
        if self.usage_shadow.is_empty() {
            return Ok(());
        }
        let copies: PendingChanges = pending
            .iter()
            .filter(|(key_space, key, change)| {
                self.usage_shadow.contains(key_space)
                    && !is_rebuild_row(key)
                    && !matches!(change, CounterChange::Range(_))
            })
            .map(|(key_space, key, change)| {
                (
                    key_space.clone(),
                    ByteView::from(usage_shadow_key(key)),
                    *change,
                )
            })
            .collect();
        for (key_space, key, change) in copies {
            match pending
                .iter_mut()
                .find(|(space, existing, _)| *space == key_space && *existing == key)
            {
                Some((_, _, existing)) => *existing = existing.merge(change)?,
                None => pending.push((key_space, key, change)),
            }
        }
        Ok(())
    }

    /// Answers a view that reads source rows as the rebuild began. A rebuild already running,
    /// also one begun before a restart, resumes with its rows and kept values.
    #[tracing::instrument(name = "storage.begin_usage_rebuild", level = "debug", skip(self))]
    pub(super) fn begin_usage_rebuild(&mut self, key_space: String) -> StorageEvent {
        if !self.usage_shadow.contains(&key_space) {
            let cleared = self.clear_preimages();
            if let Err(error) = cleared.and_then(|()| self.clear_rebuild_rows(&key_space)) {
                return StorageEvent::Error { error };
            }
            // No commit can land between turning shadowing on and taking the snapshot.
            self.usage_shadow.insert(key_space.clone());
        }
        let started = self.start_transaction(true);
        if let StorageEvent::TransactionStarted { txn_id } = &started {
            self.usage_views.insert(*txn_id);
        }
        started
    }

    /// Deletes old rebuild rows, seeds every global shard row, and writes the marker and the
    /// progress of a new generation in one commit.
    fn clear_rebuild_rows(&mut self, key_space: &str) -> Result<(), StorageError> {
        let keyspace = self.store.resolve_keyspace(key_space)?;
        let snapshot = self.store.db.read_tx();
        let mut tx = self.buffered_write_tx()?;
        let prefix = ByteView::from(USAGE_SHADOW_PREFIX);
        let mut start = None;
        loop {
            let (values, next) = iterate_page(
                &snapshot,
                &keyspace,
                Some(&prefix),
                start.as_ref(),
                REBUILD_PAGE,
            )?;
            for (key, _) in values {
                tx.remove(keyspace.clone(), key);
            }
            match next {
                Some(key) => start = Some(IterStart::After(key)),
                None => break,
            }
        }
        let seed = ShadowCounters::Usage(UsageDelta::default()).to_bytes();
        let seed = seed.map_err(|error| StorageError::WriteError(error.to_string()))?;
        for shard in global_shard_keys() {
            let row = ByteView::from(usage_shadow_key(&shard));
            tx.insert(keyspace.clone(), row, ByteView::from(seed.as_slice()));
        }
        let progress = RebuildProgress::new(Ulid::generate()).to_bytes();
        let progress = progress.map_err(|error| StorageError::WriteError(error.to_string()))?;
        tx.insert(
            keyspace.clone(),
            ByteView::from(USAGE_PROGRESS_KEY),
            ByteView::from(progress),
        );
        tx.insert(
            keyspace,
            ByteView::from(USAGE_REBUILD_KEY),
            ByteView::from(&[][..]),
        );
        self.commit_buffered(tx)?;
        self.persist_journal()
    }

    #[tracing::instrument(name = "storage.finish_usage_rebuild", level = "debug", skip(self))]
    pub(super) fn finish_usage_rebuild(&mut self, key_space: String) -> StorageEvent {
        if !self.usage_shadow.contains(&key_space) {
            return StorageEvent::Error {
                error: StorageError::WriteError("no usage rebuild is running".to_string()),
            };
        }
        match self.replace_counter_rows(&key_space) {
            Ok(entries) => {
                self.usage_shadow.remove(&key_space);
                // A fresh rebuild clears what is left.
                if let Err(error) = self.clear_preimages() {
                    warn!(%error, "Kept rebuild values stay until the next usage rebuild");
                }
                StorageEvent::BatchWriteResult { entries }
            }
            Err(error) => StorageEvent::Error { error },
        }
    }

    /// Replaces every live counter row by its rebuild row in one commit.
    fn replace_counter_rows(
        &mut self,
        key_space: &str,
    ) -> Result<Vec<(String, ByteView)>, StorageError> {
        let keyspace = self.store.resolve_keyspace(key_space)?;
        let snapshot = self.store.db.read_tx();
        let mut tx = self.buffered_write_tx()?;
        let failed = |message: String| StorageError::WriteError(message);
        let mut entries = Vec::new();
        let mut start = None;
        loop {
            let (values, next) =
                iterate_page(&snapshot, &keyspace, None, start.as_ref(), REBUILD_PAGE)?;
            for (key, value) in values {
                if key.as_ref() == USAGE_COMPLETE_KEY {
                    continue;
                }
                if key.as_ref() == USAGE_REBUILD_KEY || key.as_ref() == USAGE_PROGRESS_KEY {
                    tx.remove(keyspace.clone(), key);
                } else if let Some(live) = key.as_ref().strip_prefix(USAGE_SHADOW_PREFIX) {
                    let row = ShadowCounters::from_bytes(value.as_ref())
                        .map_err(|error| failed(error.to_string()))?;
                    let (live_value, shortfalls) =
                        row.live_row().map_err(|error| failed(error.to_string()))?;
                    warn_clamped(live, shortfalls);
                    let live = ByteView::from(live);
                    tx.insert(keyspace.clone(), live.clone(), ByteView::from(live_value));
                    tx.remove(keyspace.clone(), key);
                    entries.push((key_space.to_string(), live));
                } else {
                    let shadow = usage_shadow_key(key.as_ref());
                    let kept = snapshot
                        .get(&keyspace, &shadow)
                        .map_err(|error| StorageError::ReadError(error.to_string()))?;
                    // A counter row the rebuild did not produce is stale.
                    if kept.is_none() {
                        tx.remove(keyspace.clone(), key);
                    }
                }
            }
            match next {
                Some(key) => start = Some(IterStart::After(key)),
                None => break,
            }
        }
        let complete = ByteView::from(USAGE_COMPLETE_KEY);
        tx.insert(keyspace, complete, ByteView::from(&[][..]));
        self.commit_buffered(tx)?;
        self.persist_journal()?;
        Ok(entries)
    }
}
