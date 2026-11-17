//! Runs the three bounded walks of one feeder pass as operations: feed rows into the range
//! indexes, open indexes for new ranges, and build, check or drop the indexes of state rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Feed rows and index state rows are durable; the walk cursors live in the caller's memory.

use std::collections::{BTreeMap, VecDeque};

use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect, abort_effects};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::key_index::{
    IndexEffect, IndexError, IndexEvent, IndexedKey, KeyChange, MetaTerms,
};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, DISTRIBUTION_KEYSPACE, INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_INDEX_KEYSPACE, SEARCH_FEED_KEYSPACE, SEARCH_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::table::search_terms;
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeSlot, account_key, slot_seek,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, Position, range_interval,
};
use aruna_core::structs::storage::distribution::search::{IndexState, decode_feed};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// The open end of a range in its range index key.
const OPEN_END: [u8; 1] = [0xFF];

#[derive(Debug, Error, PartialEq)]
pub enum FeedError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Index(#[from] IndexError),
    #[error("search feed storage failed: {0}")]
    Storage(String),
    #[error("the feeder walk did not finish")]
    NotFinished,
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

/// Where each walk goes on at the next pass; every walk wraps to the start.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FeedCursors {
    pub feed: Option<Vec<u8>>,
    pub accounts: Option<Vec<u8>>,
    pub states: Option<Vec<u8>>,
}

/// What one pass did: whether it changed an index or a row, and where the walks go on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeedPass {
    pub busy: bool,
    pub cursors: FeedCursors,
}

/// The index state row of a range, keyed like its account row.
pub fn state_key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
    account_key(set_id, range_id)
}

pub(super) fn split_key(key: &[u8]) -> Result<(Ulid, Ulid), ConversionError> {
    let invalid = || ConversionError::InvalidLength("range state key".to_string());
    let set_id = key.get(..16).ok_or_else(invalid)?.try_into()?;
    let range_id = key.get(16..32).ok_or_else(invalid)?.try_into()?;
    Ok((Ulid::from_bytes(set_id), Ulid::from_bytes(range_id)))
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

fn storage_error(event: Event) -> FeedError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => FeedError::Storage(error.to_string()),
        event => FeedError::Storage(format!("unexpected storage event: {event:?}")),
    }
}

fn index(effect: IndexEffect) -> Effects {
    smallvec![Effect::Index(Box::new(effect))]
}

fn storage(effect: StorageEffect) -> Effects {
    smallvec![Effect::Storage(effect)]
}

fn iter(key_space: &str, after: Option<Vec<u8>>, limit: usize) -> Effects {
    storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: None,
        start: after.map(|key| IterStart::After(key.into())),
        limit,
        txn_id: None,
    })
}

fn batch_read(key_space: &str, keys: impl IntoIterator<Item = Vec<u8>>) -> Effects {
    let reads = (keys.into_iter())
        .map(|key| (key_space.to_string(), Key::from(key)))
        .collect();
    storage(StorageEffect::BatchRead {
        reads,
        txn_id: None,
    })
}

fn put_row(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> Effects {
    storage(StorageEffect::Write {
        key_space: key_space.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id: None,
    })
}

fn record_read(set_id: Ulid) -> Effects {
    storage(StorageEffect::Read {
        key_space: DISTRIBUTION_KEYSPACE.to_string(),
        key: set_id.to_bytes().to_vec().into(),
        txn_id: None,
    })
}

/// The values of a batch read with one answer per key, in key order.
fn read_values(event: Event, count: usize) -> Result<Vec<Option<Value>>, FeedError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == count => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        event => Err(storage_error(event)),
    }
}

/// Whether a set keeps a search index, from this node's record of it.
fn search_on(value: Option<Value>) -> Result<bool, FeedError> {
    let view = (value
        .map(|value| DistributionView::from_bytes(&value))
        .transpose())
    .map_err(|error| FeedError::Storage(error.to_string()))?;
    Ok(view.is_some_and(|view| view.snapshot().config.settings.search_index))
}

/// A located range: set, bounds and id, read from the range index.
#[derive(Debug, PartialEq)]
struct Located {
    set_id: Ulid,
    lower: String,
    upper: Option<String>,
    range_id: Ulid,
}

impl Located {
    fn contains(&self, set_id: Ulid, key: &str) -> bool {
        self.set_id == set_id
            && self.lower.as_str() <= key
            && self.upper.as_deref().is_none_or(|upper| key < upper)
    }
}

/// The range index row after `slot_seek(set_id, key)`, as the inventory update finds it.
fn locate_effect(set_id: Ulid, key: &str) -> Effects {
    storage(StorageEffect::Iter {
        key_space: RANGE_INDEX_KEYSPACE.to_string(),
        prefix: Some(set_id.to_bytes().to_vec().into()),
        start: Some(IterStart::After(slot_seek(set_id, key).into())),
        limit: 1,
        txn_id: None,
    })
}

/// The installed range of the set that contains `key`, from the row `locate_effect` read.
fn located(
    set_id: Ulid,
    key: &str,
    values: &[(Key, Value)],
) -> Result<Option<Located>, ConversionError> {
    let Some((slot_key, value)) = values.first() else {
        return Ok(None);
    };
    let slot = RangeSlot::from_bytes(value)?;
    if slot.lower.as_str() > key {
        return Ok(None);
    }
    let upper = match slot_key.get(16..).unwrap_or_default() {
        bytes if bytes == OPEN_END => None,
        bytes => Some(String::from_utf8(bytes.to_vec()).map_err(ConversionError::from)?),
    };
    Ok(Some(Located {
        set_id,
        lower: slot.lower,
        upper,
        range_id: slot.range_id,
    }))
}

/// Fills the search terms of searchable versions from their own version rows, one batch read;
/// a version row this node lacks leaves its terms empty.
#[derive(Debug, PartialEq)]
struct Describe {
    set_id: Ulid,
    changes: Vec<KeyChange>,
    /// Positions in `changes` of the puts that want terms.
    wanted: Vec<usize>,
    reading_versions: bool,
}

impl Describe {
    /// Starts describing `changes`; `Err` hands them back at once when none wants terms.
    fn start(set_id: Ulid, changes: Vec<KeyChange>) -> Result<(Self, Effects), Vec<KeyChange>> {
        let wanted: Vec<usize> = (changes.iter().enumerate())
            .filter(|(_, change)| matches!(change, KeyChange::Put(put) if put.terms.is_some()))
            .map(|(position, _)| position)
            .collect();
        if wanted.is_empty() {
            return Err(changes);
        }
        let describe = Self {
            set_id,
            changes,
            wanted,
            reading_versions: false,
        };
        Ok((describe, record_read(set_id)))
    }

    /// The next effects, or the described changes once done.
    fn step(&mut self, event: Event) -> Result<Result<Effects, Vec<KeyChange>>, FeedError> {
        if !self.reading_versions {
            let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                return Err(storage_error(event));
            };
            let view = (value
                .map(|value| DistributionView::from_bytes(&value))
                .transpose())
            .map_err(|error| FeedError::Storage(error.to_string()))?;
            let Some(view) = view else {
                return Ok(Err(std::mem::take(&mut self.changes)));
            };
            let bucket = &view.snapshot().config.bucket;
            let mut keys = Vec::with_capacity(self.wanted.len());
            for position in &self.wanted {
                if let Some(KeyChange::Put(put)) = self.changes.get(*position) {
                    keys.push(VersionKey::new(bucket, &put.key, put.version_id).to_bytes()?);
                }
            }
            self.reading_versions = true;
            return Ok(Ok(batch_read(BLOB_VERSIONS_KEYSPACE, keys)));
        }
        let versions = read_values(event, self.wanted.len())?;
        for (position, version) in self.wanted.iter().zip(versions) {
            let (Some(KeyChange::Put(put)), Some(version)) =
                (self.changes.get_mut(*position), version)
            else {
                continue;
            };
            let (text, exact) = search_terms(&BlobVersion::from_bytes(&version)?.metadata);
            put.terms = Some(MetaTerms { text, exact });
        }
        Ok(Err(std::mem::take(&mut self.changes)))
    }
}

