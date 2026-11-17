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
use aruna_core::operation::EffectOperation;
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
use serde::{Deserialize, Serialize};
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

/// How far this node's search index of one range is.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexProgress {
    pub ready: bool,
    /// Keys in the index as of its last commit.
    pub indexed: u64,
    /// Keys of the range by this node's range counters; a split child counts its parent's too.
    pub keys: u64,
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
        event => FeedError::Storage(format!("unexpected storage event: {}", event.label())),
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

/// The key changes of one range and the feed rows they settle.
type RangeBatch = (Vec<KeyChange>, Vec<(Key, Value)>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeedState {
    Init,
    Rows,
    Locate,
    States,
    Exists,
    MarkBuilding,
    Describe,
    Apply,
    Begin,
    ReadSettled,
    DeleteSettled,
    Commit,
    Abort,
    Finish,
    Error,
}

/// One feed row waiting for the range lookup of its key.
#[derive(Debug, PartialEq)]
struct Parked {
    row: (Key, Value),
    set_id: Ulid,
    key: String,
    head: Option<HeadEntry>,
}

/// The batch of one range being applied.
#[derive(Debug, PartialEq)]
struct Batch {
    set_id: Ulid,
    range_id: Ulid,
    generation: u64,
    changes: Vec<KeyChange>,
    rows: Vec<(Key, Value)>,
}

/// Applies a page of feed rows to their ranges' indexes, then drops the applied rows that no
/// writer changed meanwhile. Outputs the dropped row count.
#[derive(Debug, PartialEq)]
struct FeedWalk {
    limit: usize,
    cursor: Option<Vec<u8>>,
    state: FeedState,
    rows: VecDeque<(Key, Value)>,
    parked: Option<Parked>,
    located: Vec<Located>,
    batches: BTreeMap<(Ulid, Ulid), RangeBatch>,
    pending: VecDeque<((Ulid, Ulid), RangeBatch, Option<Value>)>,
    batch: Option<Batch>,
    describe: Option<Describe>,
    settled: Vec<(Key, Value)>,
    dropped: usize,
    txn_id: Option<TxnId>,
    output: Option<Result<usize, FeedError>>,
}

impl FeedWalk {
    fn new(limit: usize, cursor: Option<Vec<u8>>) -> Self {
        Self {
            limit,
            cursor,
            state: FeedState::Init,
            rows: VecDeque::new(),
            parked: None,
            located: Vec::new(),
            batches: BTreeMap::new(),
            pending: VecDeque::new(),
            batch: None,
            describe: None,
            settled: Vec::new(),
            dropped: 0,
            txn_id: None,
            output: None,
        }
    }

    fn cursor(&self) -> Option<Vec<u8>> {
        self.cursor.clone()
    }

    fn finish(&mut self, output: Result<usize, FeedError>) -> Effects {
        self.state = match output {
            Ok(_) => FeedState::Finish,
            Err(_) => FeedState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn fail(&mut self, error: FeedError) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.output = Some(Err(error));
                self.state = FeedState::Abort;
                storage(StorageEffect::AbortTransaction { txn_id })
            }
            None => self.finish(Err(error)),
        }
    }

    fn add(&mut self, range_id: Ulid, parked: Parked) {
        let batch = self.batches.entry((parked.set_id, range_id)).or_default();
        batch.0.push(change_of(parked.key, parked.head));
        batch.1.push(parked.row);
    }

    /// Sorts the rows into range batches until a key needs its range looked up.
    fn next_row(&mut self) -> Effects {
        while let Some((key, value)) = self.rows.pop_front() {
            let (position, head) = match (Position::decode(&key), decode_feed(&value)) {
                (Ok(position), Ok(head)) => (position, head),
                _ => {
                    warn!("Dropping an undecodable search feed row");
                    self.settled.push((key, value));
                    continue;
                }
            };
            let (set_id, name) = (position.set_id, position.key);
            let parked = Parked {
                row: (key, value),
                set_id,
                key: name,
                head,
            };
            let found = (self.located.iter()).find(|range| range.contains(set_id, &parked.key));
            if let Some(range_id) = found.map(|range| range.range_id) {
                self.add(range_id, parked);
                continue;
            }
            let effects = locate_effect(set_id, &parked.key);
            self.parked = Some(parked);
            self.state = FeedState::Locate;
            return effects;
        }
        let batches = std::mem::take(&mut self.batches);
        if batches.is_empty() {
            return self.settle();
        }
        let keys = batches
            .keys()
            .map(|(set_id, range_id)| state_key(*set_id, *range_id));
        let effects = batch_read(SEARCH_INDEX_KEYSPACE, keys.collect::<Vec<_>>());
        self.pending = (batches.into_iter())
            .map(|(range, batch)| (range, batch, None))
            .collect();
        self.state = FeedState::States;
        effects
    }

    /// Applies the next batch whose range has an index; rows of ranges without one settle.
    fn next_batch(&mut self) -> Effects {
        while let Some(((set_id, range_id), (changes, rows), state)) = self.pending.pop_front() {
            let state = match state
                .map(|value| IndexState::from_bytes(&value))
                .transpose()
            {
                Ok(state) => state,
                Err(error) => return self.fail(error.into()),
            };
            let (generation, changes, rows) = match state {
                // Without an index the build reads the current heads later.
                None => {
                    self.settled.extend(rows);
                    continue;
                }
                Some(IndexState::Ready { generation }) => {
                    self.batch = Some(Batch {
                        set_id,
                        range_id,
                        generation,
                        changes,
                        rows,
                    });
                    self.state = FeedState::Exists;
                    return index(IndexEffect::Exists {
                        set_id,
                        range_id,
                        generation,
                    });
                }
                Some(IndexState::Building { generation, cursor }) => {
                    // The build reads the heads after its cursor itself, so their rows settle.
                    let read = |key: &Key| cursor.as_deref().is_some_and(|c| key.as_ref() <= c);
                    let (read, unread): (Vec<_>, Vec<_>) =
                        (changes.into_iter().zip(rows)).partition(|(_, (key, _))| read(key));
                    self.settled.extend(unread.into_iter().map(|(_, row)| row));
                    let (changes, rows) = read.into_iter().unzip();
                    (generation, changes, rows)
                }
            };
            self.batch = Some(Batch {
                set_id,
                range_id,
                generation,
                changes,
                rows,
            });
            return self.describe();
        }
        self.settle()
    }

    fn describe(&mut self) -> Effects {
        let Some(batch) = self.batch.as_mut() else {
            return self.fail(FeedError::NotFinished);
        };
        let changes = std::mem::take(&mut batch.changes);
        match Describe::start(batch.set_id, changes) {
            Ok((describe, effects)) => {
                self.describe = Some(describe);
                self.state = FeedState::Describe;
                effects
            }
            Err(changes) => self.apply(changes),
        }
    }

    fn apply(&mut self, changes: Vec<KeyChange>) -> Effects {
        let Some(batch) = self.batch.as_ref() else {
            return self.fail(FeedError::NotFinished);
        };
        self.state = FeedState::Apply;
        index(IndexEffect::Apply {
            set_id: batch.set_id,
            range_id: batch.range_id,
            generation: batch.generation,
            changes,
        })
    }

    /// Deletes the settled feed rows whose value is still the one applied, in one transaction;
    /// a conflicting writer leaves them for the next pass.
    fn settle(&mut self) -> Effects {
        if self.settled.is_empty() {
            return self.finish(Ok(0));
        }
        self.state = FeedState::Begin;
        storage(StorageEffect::StartTransaction { read: false })
    }

    fn step_inner(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                FeedState::Rows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.cursor = next_start_after.map(|key| key.to_vec());
                self.rows = values.into();
                self.next_row()
            }
            (FeedState::Locate, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let Some(parked) = self.parked.take() else {
                    return self.fail(FeedError::NotFinished);
                };
                match located(parked.set_id, &parked.key, &values) {
                    Ok(Some(range)) => {
                        let range_id = range.range_id;
                        self.located.push(range);
                        self.add(range_id, parked);
                    }
                    // A key outside every installed range has no index to feed.
                    Ok(None) => self.settled.push(parked.row),
                    Err(error) => return self.fail(error.into()),
                }
                self.next_row()
            }
            (FeedState::States, event) => match read_values(event, self.pending.len()) {
                Ok(states) => {
                    for (entry, state) in self.pending.iter_mut().zip(states) {
                        entry.2 = state;
                    }
                    self.next_batch()
                }
                Err(error) => self.fail(error),
            },
            (FeedState::Exists, Event::Index(IndexEvent::Exists(true))) => self.describe(),
            (FeedState::Exists, Event::Index(IndexEvent::Exists(false))) => {
                let Some(batch) = self.batch.as_ref() else {
                    return self.fail(FeedError::NotFinished);
                };
                let state = IndexState::Building {
                    generation: batch.generation + 1,
                    cursor: None,
                };
                let key = state_key(batch.set_id, batch.range_id);
                match state.to_bytes() {
                    Ok(value) => {
                        self.state = FeedState::MarkBuilding;
                        put_row(SEARCH_INDEX_KEYSPACE, key, value)
                    }
                    Err(error) => self.fail(error.into()),
                }
            }
            (FeedState::MarkBuilding, Event::Storage(StorageEvent::WriteResult { .. })) => {
                if let Some(batch) = self.batch.take() {
                    self.settled.extend(batch.rows);
                }
                self.next_batch()
            }
            (FeedState::Describe, event) => {
                let Some(describe) = self.describe.as_mut() else {
                    return self.fail(FeedError::NotFinished);
                };
                match describe.step(event) {
                    Ok(Ok(effects)) => effects,
                    Ok(Err(changes)) => {
                        self.describe = None;
                        self.apply(changes)
                    }
                    Err(error) => self.fail(error),
                }
            }
            (FeedState::Apply, Event::Index(IndexEvent::Applied)) => {
                if let Some(batch) = self.batch.take() {
                    self.settled.extend(batch.rows);
                }
                self.next_batch()
            }
            (FeedState::Apply, Event::Index(IndexEvent::Error(error))) => {
                if let Some(batch) = self.batch.take() {
                    let range_id = batch.range_id;
                    warn!(%range_id, %error, "Range search index update failed");
                }
                self.next_batch()
            }
            (FeedState::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                let reads = (self.settled.iter())
                    .map(|(key, _)| (SEARCH_FEED_KEYSPACE.to_string(), key.clone()))
                    .collect();
                self.state = FeedState::ReadSettled;
                storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })
            }
            (FeedState::ReadSettled, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let settled = std::mem::take(&mut self.settled);
                let deletes: Vec<(String, Key)> = (settled.into_iter().zip(values))
                    .filter(|((_, seen), (_, now))| now.as_ref().is_some_and(|now| now == seen))
                    .map(|((key, _), _)| (SEARCH_FEED_KEYSPACE.to_string(), key))
                    .collect();
                self.dropped = deletes.len();
                let Some(txn_id) = self.txn_id else {
                    return self.fail(FeedError::NotFinished);
                };
                if deletes.is_empty() {
                    return self.commit();
                }
                self.state = FeedState::DeleteSettled;
                storage(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: Some(txn_id),
                })
            }
            (FeedState::DeleteSettled, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (FeedState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                let dropped = self.dropped;
                self.finish(Ok(dropped))
            }
            // A failed commit leaves the rows for the next pass and nothing to abort.
            (FeedState::Commit, Event::Storage(_)) => self.finish(Ok(0)),
            (FeedState::Abort, _) => {
                self.state = FeedState::Error;
                smallvec![]
            }
            (_, event) => self.fail(storage_error(event)),
        }
    }

    fn commit(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = FeedState::Commit;
                storage(StorageEffect::CommitTransaction { txn_id })
            }
            None => self.fail(FeedError::NotFinished),
        }
    }
}

impl EffectOperation for FeedWalk {
    type Output = usize;
    type Error = FeedError;

    fn start(&mut self) -> Effects {
        self.state = FeedState::Rows;
        iter(SEARCH_FEED_KEYSPACE, self.cursor.clone(), self.limit)
    }

    fn step(&mut self, event: Event) -> Effects {
        self.step_inner(event)
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, FeedState::Finish | FeedState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(FeedError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenState {
    Init,
    Rows,
    States,
    Record,
    Prune,
    Write,
    Finish,
    Error,
}

/// Opens a building index for every live range of a page of account rows without one, in sets
/// that keep a search index. Outputs the opened count.
#[derive(Debug, PartialEq)]
struct OpenWalk {
    limit: usize,
    cursor: Option<Vec<u8>>,
    state: OpenState,
    live: Vec<Vec<u8>>,
    unopened: VecDeque<Vec<u8>>,
    current: Option<(Vec<u8>, Ulid, Ulid)>,
    searched: BTreeMap<Ulid, bool>,
    opened: usize,
    output: Option<Result<usize, FeedError>>,
}

impl OpenWalk {
    fn new(limit: usize, cursor: Option<Vec<u8>>) -> Self {
        Self {
            limit,
            cursor,
            state: OpenState::Init,
            live: Vec::new(),
            unopened: VecDeque::new(),
            current: None,
            searched: BTreeMap::new(),
            opened: 0,
            output: None,
        }
    }

    fn cursor(&self) -> Option<Vec<u8>> {
        self.cursor.clone()
    }

    fn finish(&mut self, output: Result<usize, FeedError>) -> Effects {
        self.state = match output {
            Ok(_) => OpenState::Finish,
            Err(_) => OpenState::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn next_key(&mut self) -> Effects {
        while let Some(key) = self.unopened.pop_front() {
            let (set_id, range_id) = match split_key(&key) {
                Ok(ids) => ids,
                Err(error) => return self.finish(Err(error.into())),
            };
            self.current = Some((key, set_id, range_id));
            match self.searched.get(&set_id) {
                Some(true) => return self.prune(),
                Some(false) => continue,
                None => {
                    self.state = OpenState::Record;
                    return record_read(set_id);
                }
            }
        }
        let opened = self.opened;
        self.finish(Ok(opened))
    }

    /// Leftovers of an earlier index of the range would hold stale keys.
    fn prune(&mut self) -> Effects {
        let Some((_, set_id, range_id)) = self.current else {
            return self.finish(Err(FeedError::NotFinished));
        };
        self.state = OpenState::Prune;
        index(IndexEffect::Prune {
            set_id,
            range_id,
            keep: None,
        })
    }
}

impl EffectOperation for OpenWalk {
    type Output = usize;
    type Error = FeedError;

    fn start(&mut self) -> Effects {
        self.state = OpenState::Rows;
        iter(RANGE_ACCOUNT_KEYSPACE, self.cursor.clone(), self.limit)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                OpenState::Rows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.cursor = next_start_after.map(|key| key.to_vec());
                for (key, value) in values {
                    match RangeAccount::from_bytes(&value) {
                        Ok(account) if matches!(account.charge, Charge::Retired { .. }) => {}
                        Ok(_) => self.live.push(key.to_vec()),
                        Err(error) => return self.finish(Err(error.into())),
                    }
                }
                if self.live.is_empty() {
                    return self.finish(Ok(0));
                }
                self.state = OpenState::States;
                batch_read(SEARCH_INDEX_KEYSPACE, self.live.clone())
            }
            (OpenState::States, event) => match read_values(event, self.live.len()) {
                Ok(states) => {
                    let live = std::mem::take(&mut self.live).into_iter().zip(states);
                    let unopened = live.filter(|(_, state)| state.is_none());
                    self.unopened = unopened.map(|(key, _)| key).collect();
                    self.next_key()
                }
                Err(error) => self.finish(Err(error)),
            },
            (OpenState::Record, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let Some((_, set_id, _)) = self.current else {
                    return self.finish(Err(FeedError::NotFinished));
                };
                match search_on(value) {
                    Ok(search) => {
                        self.searched.insert(set_id, search);
                        match search {
                            true => self.prune(),
                            false => self.next_key(),
                        }
                    }
                    Err(error) => self.finish(Err(error)),
                }
            }
            (OpenState::Prune, Event::Index(IndexEvent::Pruned)) => {
                let Some((key, _, _)) = self.current.take() else {
                    return self.finish(Err(FeedError::NotFinished));
                };
                let state = IndexState::Building {
                    generation: 1,
                    cursor: None,
                };
                match state.to_bytes() {
                    Ok(value) => {
                        self.state = OpenState::Write;
                        put_row(SEARCH_INDEX_KEYSPACE, key, value)
                    }
                    Err(error) => self.finish(Err(error.into())),
                }
            }
            (OpenState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.opened += 1;
                self.next_key()
            }
            (_, Event::Index(IndexEvent::Error(error))) => self.finish(Err(error.into())),
            (_, event) => self.finish(Err(storage_error(event))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, OpenState::Finish | OpenState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(FeedError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StateStep {
    Init,
    Rows,
    Accounts,
    Record,
    PruneGone,
    DeleteGone,
    Outdated,
    BuildPage,
    BuildDescribe,
    BuildApply,
    BuildState,
    BuildPrune,
    Exists,
    MarkBuilding,
    PruneOld,
    Finish,
    Error,
}

/// The state row being looked at.
#[derive(Debug, PartialEq)]
struct Current {
    key: Vec<u8>,
    set_id: Ulid,
    range_id: Ulid,
    state: Value,
    account: Option<RangeAccount>,
}

/// The building index being filled from inventory heads.
#[derive(Debug, PartialEq)]
struct Build {
    generation: u64,
    cursor: Option<Vec<u8>>,
    lower: Vec<u8>,
    end: Vec<u8>,
    pages: usize,
    changes: Vec<KeyChange>,
    done: bool,
}

/// Drops the indexes of ranges without a live account or whose set keeps no search index, and
/// builds the first building index of the page; the walk stays at that range until its index is
/// ready. Outputs the count of changed ranges.
#[derive(Debug, PartialEq)]
struct StateWalk {
    limits: FeedLimits,
    cursor: Option<Vec<u8>>,
    next: Option<Vec<u8>>,
    previous: Option<Vec<u8>>,
    state: StateStep,
    rows: Vec<(Key, Value)>,
    pending: VecDeque<((Key, Value), Option<Value>)>,
    current: Option<Current>,
    build: Option<Build>,
    describe: Option<Describe>,
    searched: BTreeMap<Ulid, bool>,
    changed: usize,
    output: Option<Result<usize, FeedError>>,
}

impl StateWalk {
    fn new(limits: FeedLimits, cursor: Option<Vec<u8>>) -> Self {
        Self {
            limits,
            cursor,
            next: None,
            previous: None,
            state: StateStep::Init,
            rows: Vec::new(),
            pending: VecDeque::new(),
            current: None,
            build: None,
            describe: None,
            searched: BTreeMap::new(),
            changed: 0,
            output: None,
        }
    }

    fn cursor(&self) -> Option<Vec<u8>> {
        self.cursor.clone()
    }

    fn finish(&mut self, output: Result<usize, FeedError>) -> Effects {
        self.state = match output {
            Ok(_) => StateStep::Finish,
            Err(_) => StateStep::Error,
        };
        self.output = Some(output);
        smallvec![]
    }

    fn next_range(&mut self) -> Effects {
        if let Some(current) = self.current.take() {
            self.previous = Some(current.key);
        }
        let Some(((key, state), account)) = self.pending.pop_front() else {
            self.cursor = self.next.take();
            let changed = self.changed;
            return self.finish(Ok(changed));
        };
        let (set_id, range_id) = match split_key(&key) {
            Ok(ids) => ids,
            Err(error) => return self.finish(Err(error.into())),
        };
        let account = match account
            .map(|value| RangeAccount::from_bytes(&value))
            .transpose()
        {
            Ok(account) => {
                account.filter(|account| !matches!(account.charge, Charge::Retired { .. }))
            }
            Err(error) => return self.finish(Err(error.into())),
        };
        let live = account.is_some();
        self.current = Some(Current {
            key: key.to_vec(),
            set_id,
            range_id,
            state,
            account,
        });
        match (live, self.searched.get(&set_id)) {
            (true, None) => {
                self.state = StateStep::Record;
                record_read(set_id)
            }
            (true, Some(false)) => {
                if let Some(current) = self.current.as_mut() {
                    current.account = None;
                }
                self.decide()
            }
            _ => self.decide(),
        }
    }

    fn decide(&mut self) -> Effects {
        let Some(current) = self.current.as_ref() else {
            return self.finish(Err(FeedError::NotFinished));
        };
        let (set_id, range_id) = (current.set_id, current.range_id);
        let state = match IndexState::from_bytes(&current.state) {
            Ok(state) => state,
            Err(error) => return self.finish(Err(error.into())),
        };
        match (current.account.is_some(), state) {
            (false, _) => {
                self.state = StateStep::PruneGone;
                index(IndexEffect::Prune {
                    set_id,
                    range_id,
                    keep: None,
                })
            }
            (true, IndexState::Building { generation, cursor }) => {
                self.build = Some(Build {
                    generation,
                    cursor,
                    lower: Vec::new(),
                    end: Vec::new(),
                    pages: 0,
                    changes: Vec::new(),
                    done: false,
                });
                self.state = StateStep::Outdated;
                index(IndexEffect::Outdated {
                    set_id,
                    range_id,
                    generation,
                })
            }
            (true, IndexState::Ready { generation }) => {
                self.build = Some(Build {
                    generation,
                    cursor: None,
                    lower: Vec::new(),
                    end: Vec::new(),
                    pages: 0,
                    changes: Vec::new(),
                    done: false,
                });
                self.state = StateStep::Exists;
                index(IndexEffect::Exists {
                    set_id,
                    range_id,
                    generation,
                })
            }
        }
    }

    /// Reads the next inventory page of the range being built.
    fn build_page(&mut self) -> Effects {
        let Some(build) = self.build.as_ref() else {
            return self.finish(Err(FeedError::NotFinished));
        };
        let start = match &build.cursor {
            Some(cursor) => IterStart::After(cursor.clone().into()),
            None => IterStart::At(build.lower.clone().into()),
        };
        self.state = StateStep::BuildPage;
        storage(StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(build.end.clone().into()),
            limit: self.limits.rebuild_rows,
            max_bytes: self.limits.rebuild_bytes,
            txn_id: None,
        })
    }

    fn build_apply(&mut self, changes: Vec<KeyChange>) -> Effects {
        let (Some(current), Some(build)) = (self.current.as_ref(), self.build.as_ref()) else {
            return self.finish(Err(FeedError::NotFinished));
        };
        self.state = StateStep::BuildApply;
        index(IndexEffect::Apply {
            set_id: current.set_id,
            range_id: current.range_id,
            generation: build.generation,
            changes,
        })
    }

    fn write_state(&mut self, next: StateStep, state: IndexState) -> Effects {
        let Some(current) = self.current.as_ref() else {
            return self.finish(Err(FeedError::NotFinished));
        };
        let key = current.key.clone();
        match state.to_bytes() {
            Ok(value) => {
                self.state = next;
                put_row(SEARCH_INDEX_KEYSPACE, key, value)
            }
            Err(error) => self.finish(Err(error.into())),
        }
    }

    fn prune_keep(&mut self, next: StateStep) -> Effects {
        let (Some(current), Some(build)) = (self.current.as_ref(), self.build.as_ref()) else {
            return self.finish(Err(FeedError::NotFinished));
        };
        self.state = next;
        index(IndexEffect::Prune {
            set_id: current.set_id,
            range_id: current.range_id,
            keep: Some(build.generation),
        })
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, FeedError> {
        Ok(match (self.state, event) {
            (
                StateStep::Rows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.next = next_start_after.map(|key| key.to_vec());
                let keys: Vec<Vec<u8>> = values.iter().map(|(key, _)| key.to_vec()).collect();
                self.rows = values;
                if keys.is_empty() {
                    return Ok(self.accounts(Vec::new()));
                }
                self.state = StateStep::Accounts;
                batch_read(RANGE_ACCOUNT_KEYSPACE, keys)
            }
            (StateStep::Accounts, event) => {
                let accounts = read_values(event, self.rows.len())?;
                self.accounts(accounts)
            }
            (StateStep::Record, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let search = search_on(value)?;
                let current = self.current.as_mut().ok_or(FeedError::NotFinished)?;
                self.searched.insert(current.set_id, search);
                if !search {
                    current.account = None;
                }
                self.decide()
            }
            (StateStep::PruneGone, Event::Index(IndexEvent::Pruned)) => {
                let current = self.current.as_ref().ok_or(FeedError::NotFinished)?;
                self.state = StateStep::DeleteGone;
                storage(StorageEffect::Delete {
                    key_space: SEARCH_INDEX_KEYSPACE.to_string(),
                    key: current.key.clone().into(),
                    txn_id: None,
                })
            }
            (StateStep::DeleteGone, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.changed += 1;
                self.next_range()
            }
            (StateStep::Outdated, Event::Index(IndexEvent::Outdated(outdated))) => {
                let current = self.current.as_ref().ok_or(FeedError::NotFinished)?;
                let account = current.account.as_ref().ok_or(FeedError::NotFinished)?;
                let (lower, end) =
                    range_interval(current.set_id, &account.lower, account.upper.as_deref());
                let build = self.build.as_mut().ok_or(FeedError::NotFinished)?;
                // A build begun with an older schema starts over in the next generation.
                if outdated {
                    build.generation += 1;
                    build.cursor = None;
                }
                (build.lower, build.end) = (lower, end);
                self.build_page()
            }
            (
                StateStep::BuildPage,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                let pages = self.limits.rebuild_pages.max(1);
                let build = self.build.as_mut().ok_or(FeedError::NotFinished)?;
                for (key, value) in &values {
                    let position = Position::decode(key)?;
                    if position.entry == Entry::Head {
                        let head = HeadEntry::from_bytes(value)?;
                        build.changes.push(change_of(position.key, Some(head)));
                    }
                }
                build.pages += 1;
                match next_start_after {
                    Some(truncated) => {
                        build.cursor = Some(truncated.to_vec());
                        if build.pages < pages {
                            return Ok(self.build_page());
                        }
                    }
                    None => build.done = true,
                }
                let changes = std::mem::take(&mut build.changes);
                let set_id = self.current.as_ref().ok_or(FeedError::NotFinished)?.set_id;
                match Describe::start(set_id, changes) {
                    Ok((describe, effects)) => {
                        self.describe = Some(describe);
                        self.state = StateStep::BuildDescribe;
                        effects
                    }
                    Err(changes) => self.build_apply(changes),
                }
            }
            (StateStep::BuildDescribe, event) => {
                let describe = self.describe.as_mut().ok_or(FeedError::NotFinished)?;
                match describe.step(event)? {
                    Ok(effects) => effects,
                    Err(changes) => {
                        self.describe = None;
                        self.build_apply(changes)
                    }
                }
            }
            (StateStep::BuildApply, Event::Index(IndexEvent::Applied)) => {
                // The cursor moves only after the commit, so a crash repeats pages, never skips.
                let build = self.build.as_ref().ok_or(FeedError::NotFinished)?;
                let state = match build.done {
                    true => IndexState::Ready {
                        generation: build.generation,
                    },
                    false => IndexState::Building {
                        generation: build.generation,
                        cursor: build.cursor.clone(),
                    },
                };
                self.write_state(StateStep::BuildState, state)
            }
            (StateStep::BuildState, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let done = self.build.as_ref().ok_or(FeedError::NotFinished)?.done;
                if done {
                    return Ok(self.prune_keep(StateStep::BuildPrune));
                }
                // The walk stays at this range until its index is ready.
                self.cursor = self.previous.take();
                let changed = self.changed + 1;
                self.finish(Ok(changed))
            }
            (StateStep::BuildPrune, Event::Index(IndexEvent::Pruned)) => {
                self.build = None;
                self.changed += 1;
                self.next_range()
            }
            (StateStep::Exists, Event::Index(IndexEvent::Exists(true))) => {
                // Generations a crash or a failed prune left after a rebuild go now.
                self.prune_keep(StateStep::PruneOld)
            }
            (StateStep::Exists, Event::Index(IndexEvent::Exists(false))) => {
                // A lost index, or one of an older schema, is built again beside it; searches
                // report the range incomplete until the new generation is ready.
                let build = self.build.take().ok_or(FeedError::NotFinished)?;
                let state = IndexState::Building {
                    generation: build.generation + 1,
                    cursor: None,
                };
                self.write_state(StateStep::MarkBuilding, state)
            }
            (StateStep::MarkBuilding, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.changed += 1;
                self.next_range()
            }
            (StateStep::PruneOld, Event::Index(IndexEvent::Pruned)) => {
                self.build = None;
                self.next_range()
            }
            (_, Event::Index(IndexEvent::Error(error))) => return Err(error.into()),
            (_, event) => return Err(storage_error(event)),
        })
    }

    /// Pairs the state rows with their accounts and walks them; the walk cursor is held back
    /// until the walk ends, so a failed walk starts over.
    fn accounts(&mut self, accounts: Vec<Option<Value>>) -> Effects {
        let rows = std::mem::take(&mut self.rows);
        self.pending = rows.into_iter().zip(accounts).collect();
        self.previous = self.cursor.take();
        self.next_range()
    }
}

impl EffectOperation for StateWalk {
    type Output = usize;
    type Error = FeedError;

    fn start(&mut self) -> Effects {
        self.state = StateStep::Rows;
        iter(
            SEARCH_INDEX_KEYSPACE,
            self.cursor.clone(),
            self.limits.state_rows,
        )
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, StateStep::Finish | StateStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(FeedError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// One bounded pass over each walk, one after another; a failed walk is logged and the next
/// one runs. Outputs whether the pass changed an index or a row, and the new cursors.
#[derive(Debug, PartialEq)]
pub struct FeedPassOperation {
    limits: FeedLimits,
    cursors: FeedCursors,
    walk: Option<Walk>,
    changed: usize,
    done: bool,
    refused: Option<String>,
}

#[derive(Debug, PartialEq)]
enum Walk {
    Feed(FeedWalk),
    Open(OpenWalk),
    States(StateWalk),
}

impl Walk {
    fn name(&self) -> &'static str {
        match self {
            Walk::Feed(_) => "feed",
            Walk::Open(_) => "open",
            Walk::States(_) => "build",
        }
    }
}

impl FeedPassOperation {
    pub fn new(limits: FeedLimits, cursors: FeedCursors) -> Self {
        Self {
            limits,
            cursors,
            walk: None,
            changed: 0,
            done: false,
            refused: None,
        }
    }

    /// Starts `walk`, and the walks after it while one finishes without effects.
    fn begin(&mut self, mut walk: Walk) -> Effects {
        let effects = match &mut walk {
            Walk::Feed(feed) => feed.start(),
            Walk::Open(open) => open.start(),
            Walk::States(states) => states.start(),
        };
        self.settle(walk, effects)
    }

    /// Keeps a running walk; a finished one records its cursor and result, then the next starts.
    fn settle(&mut self, walk: Walk, effects: Effects) -> Effects {
        let complete = match &walk {
            Walk::Feed(feed) => feed.is_complete(),
            Walk::Open(open) => open.is_complete(),
            Walk::States(states) => states.is_complete(),
        };
        if !complete {
            self.walk = Some(walk);
            return effects;
        }
        let name = walk.name();
        let (result, next) = match walk {
            Walk::Feed(feed) => {
                self.cursors.feed = feed.cursor();
                let next = OpenWalk::new(self.limits.account_rows, self.cursors.accounts.clone());
                (feed.finalize(), Some(Walk::Open(next)))
            }
            Walk::Open(open) => {
                self.cursors.accounts = open.cursor();
                let next = StateWalk::new(self.limits, self.cursors.states.clone());
                (open.finalize(), Some(Walk::States(next)))
            }
            Walk::States(states) => {
                self.cursors.states = states.cursor();
                (states.finalize(), None)
            }
        };
        match result {
            Ok(count) => self.changed += count,
            Err(error) => warn!(walk = name, %error, "Range search index pass failed"),
        }
        match next {
            Some(next) => self.begin(next),
            None => {
                self.done = true;
                smallvec![]
            }
        }
    }
}

impl EffectOperation for FeedPassOperation {
    type Output = FeedPass;
    type Error = FeedError;

    fn start(&mut self) -> Effects {
        let feed = FeedWalk::new(self.limits.feed_rows, self.cursors.feed.clone());
        self.begin(Walk::Feed(feed))
    }

    fn step(&mut self, event: Event) -> Effects {
        let Some(mut walk) = self.walk.take() else {
            self.done = true;
            self.refused = Some(event.label().to_string());
            return smallvec![];
        };
        let effects = match &mut walk {
            Walk::Feed(feed) => feed.step(event),
            Walk::Open(open) => open.step(event),
            Walk::States(states) => states.step(event),
        };
        self.settle(walk, effects)
    }

    fn is_complete(&self) -> bool {
        self.done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match (self.done, self.refused) {
            (true, None) => Ok(FeedPass {
                busy: self.changed > 0,
                cursors: self.cursors,
            }),
            (true, Some(event)) => Err(FeedError::Storage(format!("unexpected event: {event}"))),
            (false, _) => Err(FeedError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.walk.as_mut() {
            Some(Walk::Feed(feed)) => feed.abort(),
            _ => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{record, searched};
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

    fn rows(values: Vec<(Vec<u8>, Vec<u8>)>, next: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: (values.into_iter())
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            next_start_after: next.map(Key::from),
        })
    }

    fn read(values: Vec<Option<Vec<u8>>>) -> Event {
        let values = values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.map(Value::from)));
        Event::Storage(StorageEvent::BatchReadResult {
            values: values.collect(),
        })
    }

    fn record_value(searching: bool) -> Option<Vec<u8>> {
        let (_, bytes) = match searching {
            true => searched(),
            false => record(true),
        };
        Some(bytes)
    }

    fn error() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    }

    // An undecodable feed row settles; it is deleted only while it still holds the value read.
    #[test]
    fn settled_rows_guarded() {
        let row = (vec![1], vec![0xFF]);
        for (now, dropped) in [(vec![0xFF], 1), (vec![0xFE], 0)] {
            let mut walk = FeedWalk::new(8, None);
            let mut steps = vec![
                (
                    rows(vec![row.clone()], Some(vec![1])),
                    vec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::BatchRead {
                        reads: vec![(SEARCH_FEED_KEYSPACE.to_string(), vec![1].into())],
                        txn_id: Some(TXN),
                    })],
                ),
            ];
            let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id: TXN });
            let current = read(vec![Some(now)]);
            match dropped {
                1 => steps.extend([
                    (
                        current,
                        vec![Effect::Storage(StorageEffect::BatchDelete {
                            deletes: vec![(SEARCH_FEED_KEYSPACE.to_string(), vec![1].into())],
                            txn_id: Some(TXN),
                        })],
                    ),
                    (
                        Event::Storage(StorageEvent::BatchDeleteResult {
                            entries: Vec::new(),
                        }),
                        vec![commit],
                    ),
                ]),
                _ => steps.push((current, vec![commit])),
            }
            steps.push((
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                Vec::new(),
            ));
            check_trace(&mut walk, &iter(SEARCH_FEED_KEYSPACE, None, 8), steps);
            assert_eq!(walk.cursor(), Some(vec![1]));
            assert_eq!(walk.finalize(), Ok(dropped));
        }
    }

    // A failed read of the settled rows aborts the transaction and fails the walk.
    #[test]
    fn failed_settle_aborts() {
        let mut walk = FeedWalk::new(8, None);
        walk.start();
        walk.step(rows(vec![(vec![1], vec![0xFF])], None));
        walk.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let aborted = walk.step(error());
        assert_eq!(
            aborted.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: TXN
            })]
        );
        assert!(walk.abort().is_empty());
        walk.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TXN,
        }));
        let failed = FeedError::Storage(StorageError::Timeout.to_string());
        assert_eq!(walk.finalize(), Err(failed));
    }

    // A live range without a state row in a searched set gets its leftovers pruned and a first
    // building generation; one in a set without a search index is skipped.
    #[test]
    fn opens_searched_ranges() {
        let (snapshot, _) = searched();
        let key = state_key(snapshot.set_id(), Ulid::from_bytes([1; 16]));
        let account = RangeAccount::fresh(String::new(), None).to_bytes().unwrap();
        let building = IndexState::Building {
            generation: 1,
            cursor: None,
        };
        let mut walk = OpenWalk::new(4, None);
        check_trace(
            &mut walk,
            &iter(RANGE_ACCOUNT_KEYSPACE, None, 4),
            vec![
                (
                    rows(vec![(key.clone(), account.clone())], None),
                    batch_read(SEARCH_INDEX_KEYSPACE, [key.clone()]).into_vec(),
                ),
                (read(vec![None]), record_read(snapshot.set_id()).into_vec()),
                (
                    Event::Storage(StorageEvent::ReadResult {
                        key: Vec::new().into(),
                        value: record_value(true).map(Value::from),
                    }),
                    index(IndexEffect::Prune {
                        set_id: snapshot.set_id(),
                        range_id: Ulid::from_bytes([1; 16]),
                        keep: None,
                    })
                    .into_vec(),
                ),
                (
                    Event::Index(IndexEvent::Pruned),
                    put_row(
                        SEARCH_INDEX_KEYSPACE,
                        key.clone(),
                        building.to_bytes().unwrap(),
                    )
                    .into_vec(),
                ),
                (
                    Event::Storage(StorageEvent::WriteResult {
                        key: key.clone().into(),
                    }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(walk.finalize(), Ok(1));

        let mut walk = OpenWalk::new(4, None);
        walk.start();
        walk.step(rows(vec![(key, account)], None));
        walk.step(read(vec![None]));
        let effects = walk.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: record_value(false).map(Value::from),
        }));
        assert!(effects.is_empty());
        assert_eq!(walk.finalize(), Ok(0));
    }

    // A state row without a live account loses its index and its row; the walk then moves on.
    #[test]
    fn drops_gone_ranges() {
        let (set_id, range_id) = (Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        let key = state_key(set_id, range_id);
        let ready = IndexState::Ready { generation: 2 }.to_bytes().unwrap();
        let mut walk = StateWalk::new(FeedLimits::default(), Some(vec![0]));
        let state_rows = FeedLimits::default().state_rows;
        check_trace(
            &mut walk,
            &iter(SEARCH_INDEX_KEYSPACE, Some(vec![0]), state_rows),
            vec![
                (
                    rows(vec![(key.clone(), ready)], Some(vec![7])),
                    batch_read(RANGE_ACCOUNT_KEYSPACE, [key.clone()]).into_vec(),
                ),
                (
                    read(vec![None]),
                    index(IndexEffect::Prune {
                        set_id,
                        range_id,
                        keep: None,
                    })
                    .into_vec(),
                ),
                (
                    Event::Index(IndexEvent::Pruned),
                    vec![Effect::Storage(StorageEffect::Delete {
                        key_space: SEARCH_INDEX_KEYSPACE.to_string(),
                        key: key.clone().into(),
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::DeleteResult { key: key.into() }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(walk.cursor(), Some(vec![7]));
        assert_eq!(walk.finalize(), Ok(1));
    }

    // A failed walk starts over at the next pass, as the cursor is held back meanwhile.
    #[test]
    fn failed_walk_restarts() {
        let key = state_key(Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        let ready = IndexState::Ready { generation: 2 }.to_bytes().unwrap();
        let mut walk = StateWalk::new(FeedLimits::default(), Some(vec![0]));
        walk.start();
        walk.step(rows(vec![(key, ready)], Some(vec![7])));
        walk.step(read(vec![None]));
        let failed = IndexError::Failed("disk".to_string());
        walk.step(Event::Index(IndexEvent::Error(failed.clone())));
        assert_eq!(walk.cursor(), None);
        assert_eq!(walk.finalize(), Err(failed.into()));
    }

    // A pass runs the three walks in order and reports the cursors they reached.
    #[test]
    fn pass_runs_walks() {
        let limits = FeedLimits::default();
        let mut pass = FeedPassOperation::new(limits, FeedCursors::default());
        check_trace(
            &mut pass,
            &iter(SEARCH_FEED_KEYSPACE, None, limits.feed_rows),
            vec![
                (
                    rows(Vec::new(), None),
                    iter(RANGE_ACCOUNT_KEYSPACE, None, limits.account_rows).into_vec(),
                ),
                (
                    rows(Vec::new(), Some(vec![5])),
                    iter(SEARCH_INDEX_KEYSPACE, None, limits.state_rows).into_vec(),
                ),
                (rows(Vec::new(), None), Vec::new()),
            ],
        );
        let cursors = FeedCursors {
            feed: None,
            accounts: Some(vec![5]),
            states: None,
        };
        assert_eq!(
            pass.finalize(),
            Ok(FeedPass {
                busy: false,
                cursors
            })
        );
    }

    // A failed walk is logged and the next walk still runs.
    #[test]
    fn pass_survives_failure() {
        let limits = FeedLimits::default();
        let mut pass = FeedPassOperation::new(limits, FeedCursors::default());
        pass.start();
        let next = pass.step(error());
        assert_eq!(
            next.as_slice(),
            iter(RANGE_ACCOUNT_KEYSPACE, None, limits.account_rows).as_slice()
        );
        pass.step(rows(Vec::new(), None));
        pass.step(rows(Vec::new(), None));
        assert!(pass.finalize().is_ok_and(|pass| !pass.busy));
    }

    // Unstarted walks and passes refuse every event; none finishes early.
    #[test]
    fn rejects_unexpected() {
        let all = sample_events().len();
        let feed = || FeedWalk::new(1, None);
        assert_eq!(rejects_events(feed, sample_events, |_| false).len(), all);
        let open = || OpenWalk::new(1, None);
        assert_eq!(rejects_events(open, sample_events, |_| false).len(), all);
        let states = || StateWalk::new(FeedLimits::default(), None);
        assert_eq!(rejects_events(states, sample_events, |_| false).len(), all);
        let pass = || FeedPassOperation::new(FeedLimits::default(), FeedCursors::default());
        assert_eq!(rejects_events(pass, sample_events, |_| false).len(), all);
        rejects_finalize(feed());
        rejects_finalize(open());
        rejects_finalize(states());
        rejects_finalize(pass());
        assert!(pass().abort().is_empty());
    }
}
