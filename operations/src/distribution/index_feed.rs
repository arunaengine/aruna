//! Keeps the range search indexes of distributed buckets current: applies the durable feed
//! rows, opens an index for every live range, builds it from the inventory heads with a
//! durable cursor and drops the indexes of ranges that are gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{SEARCH_FEED_KEYSPACE, SEARCH_INDEX_KEYSPACE};
use aruna_core::shutdown::Shutdown;
use aruna_core::structs::storage::distribution::inventory::{HeadEntry, Position};
use aruna_core::structs::storage::distribution::search::{IndexState, decode_feed};
use aruna_core::types::{Key, Value};
use aruna_storage::storage::StorageHandle;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::feed_store::{Located, drop_settled, iter, locate, put_row, read_many};
use super::key_index::{IndexedKey, KeyChange, KeyIndexError, RangeIndexes};
use crate::driver::DriverContext;

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
}

impl Feeder {
    pub fn new(indexes: Arc<RangeIndexes>, limits: FeedLimits) -> Self {
        Self {
            indexes,
            limits,
            feed: None,
        }
    }

    /// One bounded pass over each walk; true when it changed an index or a row.
    pub async fn pass(&mut self, context: &DriverContext) -> bool {
        let storage = context.storage_handle.bulk();
        let mut changed = 0;
        for (walk, result) in [("feed", self.feed_page(&storage).await)] {
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
        let mut batches: BTreeMap<(Ulid, Ulid), (Vec<KeyChange>, Vec<(Key, Value)>)> =
            BTreeMap::new();
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
