//! Runs the range search index feeder of this node and reads the index progress of a bucket's
//! ranges; the feeder's walks themselves are operations in `feed_walks`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::key_index::{IndexEffect, IndexEvent};
use aruna_core::keyspaces::SEARCH_INDEX_KEYSPACE;
use aruna_core::structs::storage::distribution::search::IndexState;
use aruna_core::types::Key;
use aruna_storage::key_index::RangeIndexes;
use aruna_tasks::shutdown::Shutdown;
use tracing::warn;
use ulid::Ulid;

use super::feed_walks::{
    FeedCursors, FeedError, FeedLimits, FeedPassOperation, IndexProgress, state_key,
};
use super::usage::{held_ranges, sum_held};
use crate::driver::{DriverContext, drive};

const IDLE_INTERVAL: Duration = Duration::from_secs(2);
const BUSY_INTERVAL: Duration = Duration::from_millis(50);

/// The feeder of one node: its limits and where its walks go on.
pub struct Feeder {
    limits: FeedLimits,
    cursors: FeedCursors,
}

impl Feeder {
    pub fn new(limits: FeedLimits) -> Self {
        Self {
            limits,
            cursors: FeedCursors::default(),
        }
    }

    /// One bounded pass over each walk; true when it changed an index or a row.
    pub async fn pass(&mut self, context: &DriverContext) -> bool {
        let cursors = std::mem::take(&mut self.cursors);
        match drive(FeedPassOperation::new(self.limits, cursors), context).await {
            Ok(pass) => {
                self.cursors = pass.cursors;
                pass.busy
            }
            Err(error) => {
                warn!(%error, "Range search index pass failed");
                false
            }
        }
    }
}

/// Runs the feeder of this node until shutdown on the bulk storage lane; nodes without range
/// indexes feed none.
pub fn spawn_index_feed(context: Arc<DriverContext>, shutdown: &Shutdown) {
    if context.storage_handle.range_indexes().is_none() {
        return;
    }
    let mut bulk = (*context).clone();
    bulk.storage_handle = context.storage_handle.bulk();
    let mut feeder = Feeder::new(FeedLimits::default());
    let token = shutdown.token();
    shutdown.spawn(async move {
        loop {
            let wait = match feeder.pass(&bulk).await {
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
        .map(|range| Key::from(state_key(set_id, range.range_id)))
        .collect();
    let states = read_states(context, keys).await?;
    let mut progress = BTreeMap::new();
    for (range, state) in held.iter().zip(states) {
        let state = state
            .map(|value| IndexState::from_bytes(&value))
            .transpose()?;
        let indexed = match &state {
            Some(state) => {
                let count = IndexEffect::Count {
                    set_id,
                    range_id: range.range_id,
                    generation: state.generation(),
                };
                match indexes.execute(count).await {
                    IndexEvent::Counted(count) => count,
                    IndexEvent::Error(error) => return Err(error.into()),
                    _ => return Err(FeedError::NotFinished),
                }
            }
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

/// The index state rows of `keys`, one answer per key.
async fn read_states(
    context: &DriverContext,
    keys: Vec<Key>,
) -> Result<Vec<Option<aruna_core::types::Value>>, FeedError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let count = keys.len();
    let reads = (keys.into_iter())
        .map(|key| (SEARCH_INDEX_KEYSPACE.to_string(), key))
        .collect();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == count => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(FeedError::Storage(error.to_string())),
        event => Err(FeedError::Storage(format!(
            "unexpected storage event: {}",
            event.label()
        ))),
    }
}

#[cfg(test)]
#[path = "index_feed_tests.rs"]
mod tests;
