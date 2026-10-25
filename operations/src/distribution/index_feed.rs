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
    INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, SEARCH_FEED_KEYSPACE, SEARCH_INDEX_KEYSPACE,
};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::storage::distribution::accounting::{Charge, RangeAccount};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, range_interval,
};
use aruna_core::structs::storage::distribution::search::{IndexState, decode_feed};
use aruna_core::types::{Key, Value};
use aruna_storage::storage::StorageHandle;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::feed_store::{
    Located, delete_row, drop_settled, iter, locate, put_row, read_many, split_key, storage_error,
};
use super::key_index::{IndexedKey, KeyChange, KeyIndexError, RangeIndexes};
use crate::driver::DriverContext;

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
            feed_rows: 256,
            account_rows: 256,
            state_rows: 64,
            rebuild_rows: 1_000,
            rebuild_bytes: 1 << 20,
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
            ("feed", self.feed_page(&storage).await),
            ("open", self.open_page(&storage).await),
            ("build", self.state_page(&storage).await),
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
    async fn feed_page(&mut self, storage: &StorageHandle) -> Result<usize, FeedError> {
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
            let generation = match state {
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
                Some(state) => state.generation(),
            };
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

    /// Opens a building index for every live range of a page of account rows without one.
    async fn open_page(&mut self, storage: &StorageHandle) -> Result<usize, FeedError> {
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
        for (key, state) in live.into_iter().zip(states) {
            if state.is_some() {
                continue;
            }
            let (set_id, range_id) = split_key(&key)?;
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

    /// Drops the indexes of ranges without a live account and builds the first building
    /// index of the page; the walk stays at that range until its index is ready.
    async fn state_page(&mut self, storage: &StorageHandle) -> Result<usize, FeedError> {
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
        for ((key, state), account) in rows.into_iter().zip(accounts) {
            let (set_id, range_id) = split_key(&key)?;
            let account = account
                .map(|value| RangeAccount::from_bytes(&value))
                .transpose()?;
            let account =
                account.filter(|account| !matches!(account.charge, Charge::Retired { .. }));
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
                    let ready = self.build(storage, &build, &account, cursor).await?;
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

    /// Adds up to `rebuild_pages` inventory pages of heads to a building index; true once the
    /// walk reached the range's end and the index switched to ready.
    async fn build(
        &self,
        storage: &StorageHandle,
        build: &Build,
        account: &RangeAccount,
        mut cursor: Option<Vec<u8>>,
    ) -> Result<bool, FeedError> {
        let (lower, end) = range_interval(build.set_id, &account.lower, account.upper.as_deref());
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
            let mut changes = Vec::new();
            for (key, value) in &rows {
                let position = Position::decode(key)?;
                if position.entry == Entry::Head {
                    let head = HeadEntry::from_bytes(value)?;
                    changes.push(change_of(position.key, Some(head)));
                }
            }
            let (set_id, range_id, generation) = (build.set_id, build.range_id, build.generation);
            self.indexes
                .apply(set_id, range_id, generation, changes)
                .await?;
            let Some(truncated) = truncated else {
                let state = IndexState::Ready { generation };
                put_row(
                    storage,
                    SEARCH_INDEX_KEYSPACE,
                    build.key.clone(),
                    state.to_bytes()?,
                )
                .await?;
                self.indexes
                    .prune(set_id, range_id, Some(generation))
                    .await?;
                return Ok(true);
            };
            cursor = Some(truncated.to_vec());
            let state = IndexState::Building {
                generation,
                cursor: cursor.clone(),
            };
            put_row(
                storage,
                SEARCH_INDEX_KEYSPACE,
                build.key.clone(),
                state.to_bytes()?,
            )
            .await?;
        }
        Ok(false)
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
        }),
        _ => KeyChange::Remove(key),
    }
}

#[cfg(test)]
#[path = "index_feed_tests.rs"]
mod tests;
