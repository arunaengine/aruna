//! Keeps the range search indexes of distributed buckets current: applies the feed rows, builds
//! an index per live range from inventory heads and drops the indexes of ranges that are gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Feed rows and the build cursor are durable.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, SEARCH_FEED_KEYSPACE,
    SEARCH_INDEX_KEYSPACE,
};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::execution::table::search_terms;
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::structs::storage::distribution::accounting::{Charge, RangeAccount};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, range_interval,
};
use aruna_core::structs::storage::distribution::search::{IndexState, decode_feed};
use aruna_core::types::{Key, Value};
use aruna_storage::storage::StorageHandle;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::feed_store::{
    Located, delete_row, drop_settled, iter, locate, put_row, read_many, split_key, storage_error,
};
use super::key_index::{IndexedKey, KeyChange, KeyIndexError, MetaTerms, RangeIndexes};
use super::usage::{held_ranges, sum_held};
use crate::driver::DriverContext;
use crate::replication::members::read_record;

/// The key changes of one range and the feed rows they settle.
type RangeBatch = (Vec<KeyChange>, Vec<(Key, Value)>);

const IDLE_INTERVAL: Duration = Duration::from_secs(2);
const BUSY_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Error)]
pub enum FeedError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Index(#[from] KeyIndexError),
    #[error("search feed storage failed: {0}")]
    Storage(String),
}

/// How much one pass may do.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeedLimits {
    pub feed_rows: usize,
    pub account_rows: usize,
    pub state_rows: usize,
    pub rebuild_rows: usize,
    pub rebuild_bytes: usize,
    /// Inventory pages one pass adds to the range index being built.
    pub rebuild_pages: usize,
}

impl Default for FeedLimits {
    fn default() -> Self {
        Self {
            feed_rows: 16_384,
            account_rows: 256,
            state_rows: 64,
            rebuild_rows: 4_096,
            rebuild_bytes: 4 << 20,
            rebuild_pages: 8,
        }
    }
}

/// The feeder of one node. Its walks keep their cursors in memory and wrap to the start;
/// every durable decision lives in the feed rows and the index state rows.
pub struct Feeder {
    indexes: Arc<RangeIndexes>,
    limits: FeedLimits,
    feed: Option<Vec<u8>>,
    accounts: Option<Vec<u8>>,
    states: Option<Vec<u8>>,
}

impl Feeder {
    pub fn new(indexes: Arc<RangeIndexes>, limits: FeedLimits) -> Self {
        Self {
            indexes,
            limits,
            feed: None,
            accounts: None,
            states: None,
        }
    }

    /// One bounded pass over each walk; true when it changed an index or a row.
    pub async fn pass(&mut self, context: &DriverContext) -> bool {
        let storage = context.storage_handle.bulk();
        let mut changed = 0;
        for (walk, result) in [
            ("feed", self.feed_page(context, &storage).await),
            ("open", self.open_page(context, &storage).await),
            ("build", self.state_page(context, &storage).await),
        ] {
            match result {
                Ok(count) => changed += count,
                Err(error) => warn!(walk, error = %error, "Range search index pass failed"),
            }
        }
        changed > 0
    }

    /// Applies a page of feed rows to their ranges' indexes, then drops the applied rows
    /// that no writer changed meanwhile.
    async fn feed_page(
        &mut self,
        context: &DriverContext,
        storage: &StorageHandle,
    ) -> Result<usize, FeedError> {
        let (rows, next) = iter(
            storage,
            SEARCH_FEED_KEYSPACE,
            &self.feed,
            self.limits.feed_rows,
        )
        .await?;
        self.feed = next;
        let mut located: Vec<Located> = Vec::new();
        let mut batches: BTreeMap<(Ulid, Ulid), RangeBatch> = BTreeMap::new();
        let mut settled = Vec::new();
        for (key, value) in rows {
            let (position, head) = match (Position::decode(&key), decode_feed(&value)) {
                (Ok(position), Ok(head)) => (position, head),
                _ => {
                    warn!("Dropping an undecodable search feed row");
                    settled.push((key, value));
                    continue;
                }
            };
            let set_id = position.set_id;
            let found = located
                .iter()
                .find(|range| range.contains(set_id, &position.key));
            let range_id = match found {
                Some(range) => range.range_id,
                None => match locate(storage, set_id, &position.key).await? {
                    Some(range) => {
                        let range_id = range.range_id;
                        located.push(range);
                        range_id
                    }
                    // A key outside every installed range has no index to feed.
                    None => {
                        settled.push((key, value));
                        continue;
                    }
                },
            };
            let batch = batches.entry((set_id, range_id)).or_default();
            batch.0.push(change_of(position.key, head));
            batch.1.push((key, value));
        }
        let keys = batches
            .keys()
            .map(|(set_id, range_id)| state_key(*set_id, *range_id));
        let states = read_many(storage, SEARCH_INDEX_KEYSPACE, keys.collect()).await?;
        for (((set_id, range_id), (changes, rows)), state) in batches.into_iter().zip(states) {
            let state = state
                .map(|value| IndexState::from_bytes(&value))
                .transpose()?;
            let (generation, (changes, rows)): (u64, RangeBatch) = match state {
                // Without an index the build reads the current heads later.
                None => {
                    settled.extend(rows);
                    continue;
                }
                Some(IndexState::Ready { generation })
                    if !self.indexes.exists(set_id, range_id, generation).await? =>
                {
                    let state = IndexState::Building {
                        generation: generation + 1,
                        cursor: None,
                    };
                    put_row(
                        storage,
                        SEARCH_INDEX_KEYSPACE,
                        state_key(set_id, range_id),
                        state.to_bytes()?,
                    )
                    .await?;
                    settled.extend(rows);
                    continue;
                }
                Some(IndexState::Building { generation, cursor }) => {
                    // The build reads the heads after its cursor itself, so their rows settle.
                    let read = |key: &Key| cursor.as_deref().is_some_and(|c| key.as_ref() <= c);
                    let (read, unread): (Vec<_>, Vec<_>) =
                        (changes.into_iter().zip(rows)).partition(|(_, (key, _))| read(key));
                    settled.extend(unread.into_iter().map(|(_, row)| row));
                    (generation, read.into_iter().unzip())
                }
                Some(IndexState::Ready { generation }) => (generation, (changes, rows)),
            };
            let mut changes = changes;
            describe(context, set_id, &mut changes).await?;
            match self
                .indexes
                .apply(set_id, range_id, generation, changes)
                .await
            {
                Ok(()) => settled.extend(rows),
                Err(error) => warn!(%range_id, error = %error, "Range search index update failed"),
            }
        }
        drop_settled(storage, settled).await
    }

    /// Opens a building index for every live range of a page of account rows without one,
    /// in sets that keep a search index.
    async fn open_page(
        &mut self,
        context: &DriverContext,
        storage: &StorageHandle,
    ) -> Result<usize, FeedError> {
        let (rows, next) = iter(
            storage,
            RANGE_ACCOUNT_KEYSPACE,
            &self.accounts,
            self.limits.account_rows,
        )
        .await?;
        self.accounts = next;
        let mut live = Vec::new();
        for (key, value) in rows {
            let account = RangeAccount::from_bytes(&value)?;
            if !matches!(account.charge, Charge::Retired { .. }) {
                live.push(key.to_vec());
            }
        }
        let states = read_many(storage, SEARCH_INDEX_KEYSPACE, live.clone()).await?;
        let mut opened = 0;
        let mut searched = BTreeMap::new();
        for (key, state) in live.into_iter().zip(states) {
            if state.is_some() {
                continue;
            }
            let (set_id, range_id) = split_key(&key)?;
            if !search_on(context, &mut searched, set_id).await? {
                continue;
            }
            // Leftovers of an earlier index of the range would hold stale keys.
            self.indexes.prune(set_id, range_id, None).await?;
            let state = IndexState::Building {
                generation: 1,
                cursor: None,
            };
            put_row(storage, SEARCH_INDEX_KEYSPACE, key, state.to_bytes()?).await?;
            opened += 1;
        }
        Ok(opened)
    }

    /// Drops the indexes of ranges without a live account or whose set keeps no search index,
    /// and builds the first building index of the page; the walk stays at that range until its
    /// index is ready.
    async fn state_page(
        &mut self,
        context: &DriverContext,
        storage: &StorageHandle,
    ) -> Result<usize, FeedError> {
        let (rows, next) = iter(
            storage,
            SEARCH_INDEX_KEYSPACE,
            &self.states,
            self.limits.state_rows,
        )
        .await?;
        let keys = rows.iter().map(|(key, _)| key.to_vec()).collect();
        let accounts = read_many(storage, RANGE_ACCOUNT_KEYSPACE, keys).await?;
        let mut previous = self.states.take();
        let mut changed = 0;
        let mut searched = BTreeMap::new();
        for ((key, state), account) in rows.into_iter().zip(accounts) {
            let (set_id, range_id) = split_key(&key)?;
            let account = account
                .map(|value| RangeAccount::from_bytes(&value))
                .transpose()?;
            let account =
                account.filter(|account| !matches!(account.charge, Charge::Retired { .. }));
            let account = match account {
                Some(account) if search_on(context, &mut searched, set_id).await? => Some(account),
                _ => None,
            };
            match (account, IndexState::from_bytes(&state)?) {
                (None, _) => {
                    self.indexes.prune(set_id, range_id, None).await?;
                    delete_row(storage, SEARCH_INDEX_KEYSPACE, key.to_vec()).await?;
                    changed += 1;
                }
                (Some(account), IndexState::Building { generation, cursor }) => {
                    let build = Build {
                        key: key.to_vec(),
                        set_id,
                        range_id,
                        generation,
                    };
                    let ready = self.build(context, &build, &account, cursor).await?;
                    if !ready {
                        self.states = previous;
                        return Ok(changed + 1);
                    }
                    changed += 1;
                }
                (Some(_), IndexState::Ready { .. }) => {}
            }
            previous = Some(key.to_vec());
        }
        self.states = next;
        Ok(changed)
    }

    /// Adds up to `rebuild_pages` inventory pages of heads to a building index in one commit;
    /// true once the walk reached the range's end and the index switched to ready.
    async fn build(
        &self,
        context: &DriverContext,
        build: &Build,
        account: &RangeAccount,
        mut cursor: Option<Vec<u8>>,
    ) -> Result<bool, FeedError> {
        let storage = &context.storage_handle.bulk();
        let (lower, end) = range_interval(build.set_id, &account.lower, account.upper.as_deref());
        let mut changes = Vec::new();
        let mut done = false;
        for _ in 0..self.limits.rebuild_pages.max(1) {
            let start = match &cursor {
                Some(cursor) => IterStart::After(cursor.clone().into()),
                None => IterStart::At(lower.clone().into()),
            };
            let effect = StorageEffect::IterRange {
                key_space: INVENTORY_KEYSPACE.to_string(),
                start,
                end: IterEnd::Before(end.clone().into()),
                limit: self.limits.rebuild_rows,
                max_bytes: self.limits.rebuild_bytes,
                txn_id: None,
            };
            let (rows, truncated) = match storage.send_storage_effect(effect).await {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => (values, next_start_after),
                event => return Err(storage_error(event)),
            };
            for (key, value) in &rows {
                let position = Position::decode(key)?;
                if position.entry == Entry::Head {
                    let head = HeadEntry::from_bytes(value)?;
                    changes.push(change_of(position.key, Some(head)));
                }
            }
            match truncated {
                Some(truncated) => cursor = Some(truncated.to_vec()),
                None => {
                    done = true;
                    break;
                }
            }
        }
        let (set_id, range_id, generation) = (build.set_id, build.range_id, build.generation);
        describe(context, set_id, &mut changes).await?;
        self.indexes
            .apply(set_id, range_id, generation, changes)
            .await?;
        // The cursor moves only after the commit, so a crash repeats pages, never skips them.
        let state = match done {
            true => IndexState::Ready { generation },
            false => IndexState::Building { generation, cursor },
        };
        put_row(
            storage,
            SEARCH_INDEX_KEYSPACE,
            build.key.clone(),
            state.to_bytes()?,
        )
        .await?;
        if done {
            self.indexes
                .prune(set_id, range_id, Some(generation))
                .await?;
        }
        Ok(done)
    }
}

struct Build {
    key: Vec<u8>,
    set_id: Ulid,
    range_id: Ulid,
    generation: u64,
}

/// Runs the feeder of this node until shutdown; nodes without metadata keep no indexes.
pub fn spawn_index_feed(context: Arc<DriverContext>, shutdown: &Shutdown) {
    let Some(handle) = context.metadata_handle.as_ref() else {
        return;
    };
    let mut feeder = Feeder::new(handle.range_indexes(), FeedLimits::default());
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let wait = match feeder.pass(&context).await {
                true => BUSY_INTERVAL,
                false => IDLE_INTERVAL,
            };
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    });
}

/// Whether set `set_id` keeps a search index, from this node's record; without one it does not.
async fn search_on(
    context: &DriverContext,
    searched: &mut BTreeMap<Ulid, bool>,
    set_id: Ulid,
) -> Result<bool, FeedError> {
    if let Some(search) = searched.get(&set_id) {
        return Ok(*search);
    }
    let view = read_record(context, set_id)
        .await
        .map_err(FeedError::Storage)?;
    let search = view.is_some_and(|view| view.snapshot().config.settings.search_index);
    searched.insert(set_id, search);
    Ok(search)
}

/// How far this node's search index of one range is.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexProgress {
    pub ready: bool,
    /// Keys in the index as of its last commit.
    pub indexed: u64,
    /// Keys of the range by this node's range counters; a split child counts its parent's too.
    pub keys: u64,
}

/// The search index progress of every range of `bucket` this node holds, while its set keeps
/// a search index; a range whose index is not opened yet has none indexed.
pub async fn range_progress(
    context: &DriverContext,
    indexes: &Arc<RangeIndexes>,
    bucket: &str,
) -> Result<BTreeMap<Ulid, IndexProgress>, FeedError> {
    let held = held_ranges(context, bucket).await;
    let held = held.map_err(|error| FeedError::Storage(error.to_string()))?;
    let Some((binding, snapshot, held)) = held else {
        return Ok(BTreeMap::new());
    };
    if !snapshot.config.settings.search_index {
        return Ok(BTreeMap::new());
    }
    let set_id = binding.set_id;
    let keys = (held.iter())
        .map(|range| state_key(set_id, range.range_id))
        .collect();
    let states = read_many(&context.storage_handle, SEARCH_INDEX_KEYSPACE, keys).await?;
    let mut progress = BTreeMap::new();
    for (range, state) in held.iter().zip(states) {
        let state = state
            .map(|value| IndexState::from_bytes(&value))
            .transpose()?;
        let indexed = match &state {
            Some(state) => (indexes.count(set_id, range.range_id, state.generation())).await?,
            None => 0,
        };
        let counters = sum_held([range]).map_err(|error| FeedError::Storage(error.to_string()))?;
        let entry = IndexProgress {
            ready: matches!(state, Some(IndexState::Ready { .. })),
            indexed,
            keys: u64::try_from(counters.objects).unwrap_or_default(),
        };
        progress.insert(range.range_id, entry);
    }
    Ok(progress)
}

/// The index state row of a range, keyed like its account row.
pub fn state_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    aruna_core::structs::storage::distribution::accounting::account_key(set_id, range_id)
}

fn change_of(key: String, head: Option<HeadEntry>) -> KeyChange {
    match head {
        Some(head) if !head.version.marker => KeyChange::Put(IndexedKey {
            key,
            size: head.version.size,
            version_id: head.version_id,
            terms: head.version.searchable.then(MetaTerms::default),
        }),
        _ => KeyChange::Remove(key),
    }
}

/// Fills the search terms of searchable versions from their own version rows, one point read
/// each; a version row this node lacks leaves its terms empty.
async fn describe(
    context: &DriverContext,
    set_id: Ulid,
    changes: &mut [KeyChange],
) -> Result<(), FeedError> {
    let mut wanted: Vec<&mut IndexedKey> = (changes.iter_mut())
        .filter_map(|change| match change {
            KeyChange::Put(indexed) if indexed.terms.is_some() => Some(indexed),
            _ => None,
        })
        .collect();
    if wanted.is_empty() {
        return Ok(());
    }
    let Some(view) = read_record(context, set_id)
        .await
        .map_err(FeedError::Storage)?
    else {
        return Ok(());
    };
    let bucket = &view.snapshot().config.bucket;
    let keys = (wanted.iter())
        .map(|indexed| VersionKey::new(bucket, &indexed.key, indexed.version_id).to_bytes())
        .collect::<Result<Vec<_>, _>>()?;
    let storage = context.storage_handle.bulk();
    let versions = read_many(&storage, BLOB_VERSIONS_KEYSPACE, keys).await?;
    for (indexed, version) in wanted.iter_mut().zip(versions) {
        let Some(version) = version else {
            continue;
        };
        let (text, exact) = search_terms(&BlobVersion::from_bytes(&version)?.metadata);
        indexed.terms = Some(MetaTerms { text, exact });
    }
    Ok(())
}

#[cfg(test)]
#[path = "index_feed_tests.rs"]
mod tests;
