//! Runs the three bounded walks of one feeder pass as operations: feed rows into the range
//! indexes, open indexes for new ranges, and build, check or drop the indexes of state rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Feed rows and index state rows are durable; the walk cursors live in the caller's memory.

use std::collections::{BTreeMap, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::Event;
use aruna_core::key_index::{
    IndexEffect, IndexError, IndexEvent, IndexedKey, KeyChange, MetaTerms,
};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, DISTRIBUTION_KEYSPACE, INVENTORY_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_INDEX_KEYSPACE, SEARCH_FEED_KEYSPACE, SEARCH_INDEX_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, read_row, value};
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
use aruna_core::types::{Key, Value};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::charged_ranges::{
    HeldRange, HeldRanges, HeldRangesOperation, UsageError, sum_held,
};
use crate::metadata::call_results::page_rows;

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

impl From<IoError> for FeedError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
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

fn unexpected(event: Event) -> FeedError {
    FeedError::Storage(format!("unexpected storage event: {}", event.label()))
}

fn index(effect: IndexEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Index(Box::new(effect))))
}

fn page(key_space: &str, after: Option<Vec<u8>>, limit: usize) -> IoCall {
    IoCall::Page {
        space: key_space.to_string(),
        prefix: Key::from(Vec::new()),
        after: after.map(Key::from),
        limit,
    }
}

fn batch_read(key_space: &str, keys: impl IntoIterator<Item = Vec<u8>>) -> IoCall {
    let reads = (keys.into_iter())
        .map(|key| (key_space.to_string(), Key::from(key)))
        .collect();
    IoCall::Read(reads)
}

fn put_row(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> IoCall {
    IoCall::Write(vec![(key_space.to_string(), key.into(), value.into())])
}

fn record_read(set_id: Ulid) -> IoCall {
    read_row(DISTRIBUTION_KEYSPACE, set_id.to_bytes().to_vec())
}

/// The answer of an index effect; an index failure fails the walk.
fn index_event(result: IoResult) -> Result<IndexEvent, FeedError> {
    match result.event()? {
        Event::Index(IndexEvent::Error(error)) => Err(error.into()),
        Event::Index(event) => Ok(event),
        other => Err(unexpected(other)),
    }
}

fn wrong_index(event: IndexEvent) -> FeedError {
    unexpected(Event::Index(event))
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
fn locate_call(set_id: Ulid, key: &str) -> IoCall {
    IoCall::Page {
        space: RANGE_INDEX_KEYSPACE.to_string(),
        prefix: set_id.to_bytes().to_vec().into(),
        after: Some(slot_seek(set_id, key).into()),
        limit: 1,
    }
}

/// The installed range of the set that contains `key`, from the row `locate_call` read.
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
    fn start(set_id: Ulid, changes: Vec<KeyChange>) -> Result<(Self, IoCall), Vec<KeyChange>> {
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

    /// The next call, or the described changes once done.
    fn step(&mut self, result: IoResult) -> Result<Result<IoCall, Vec<KeyChange>>, FeedError> {
        if !self.reading_versions {
            let view = (value(result)?
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
        for (position, version) in self.wanted.iter().zip(result.values()?) {
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

/// The rows of one walk's page and the key the next pass continues after.
type RowPage = (Vec<(Key, Value)>, Option<Key>);

/// Reads the page a walk works on, so a failed read costs only that walk.
#[derive(Debug, PartialEq)]
struct ReadPageOperation {
    key_space: &'static str,
    after: Option<Vec<u8>>,
    limit: usize,
}

impl Operation for ReadPageOperation {
    type Output = RowPage;
    type Error = FeedError;

    fn first(&mut self) -> Result<Next<RowPage>, FeedError> {
        let after = self.after.take();
        Ok(Next::Call(page(self.key_space, after, self.limit)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RowPage>, FeedError> {
        Ok(Next::Done(result.page()?))
    }
}

/// Deletes the settled feed rows whose value is still the one applied, in one transaction, and
/// outputs how many went; a conflicting writer leaves them for the next pass.
#[derive(Debug, PartialEq)]
struct SettleFeedOperation {
    settled: Vec<(Key, Value)>,
    dropped: Option<usize>,
}

impl Operation for SettleFeedOperation {
    type Output = usize;
    type Error = FeedError;

    fn first(&mut self) -> Result<Next<usize>, FeedError> {
        let reads = (self.settled.iter())
            .map(|(key, _)| (SEARCH_FEED_KEYSPACE.to_string(), key.clone()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<usize>, FeedError> {
        if let Some(dropped) = self.dropped {
            done(result)?;
            return Ok(Next::Done(dropped));
        }
        let settled = std::mem::take(&mut self.settled);
        let deletes: Vec<(String, Key)> = (settled.into_iter().zip(result.values()?))
            .filter(|((_, seen), now)| now.as_ref().is_some_and(|now| now == seen))
            .map(|((key, _), _)| (SEARCH_FEED_KEYSPACE.to_string(), key))
            .collect();
        if deletes.is_empty() {
            return Ok(Next::Done(0));
        }
        self.dropped = Some(deletes.len());
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// The key changes of one range and the feed rows they settle.
type RangeBatch = (Vec<KeyChange>, Vec<(Key, Value)>);

/// What the pending call of a feed walk answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeedState {
    Locate,
    States,
    Exists,
    MarkBuilding,
    Describe,
    Apply,
    Settle,
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

type WalkNext = Result<Next<usize>, FeedError>;

/// Applies a page of feed rows to their ranges' indexes, then drops the applied rows that no
/// writer changed meanwhile. Outputs the dropped row count.
#[derive(Debug, PartialEq)]
struct FeedWalk {
    state: FeedState,
    rows: VecDeque<(Key, Value)>,
    parked: Option<Parked>,
    located: Vec<Located>,
    batches: BTreeMap<(Ulid, Ulid), RangeBatch>,
    pending: VecDeque<((Ulid, Ulid), RangeBatch, Option<Value>)>,
    batch: Option<Batch>,
    describe: Option<Describe>,
    settled: Vec<(Key, Value)>,
}

impl FeedWalk {
    fn new(rows: Vec<(Key, Value)>) -> Self {
        Self {
            state: FeedState::Locate,
            rows: rows.into(),
            parked: None,
            located: Vec::new(),
            batches: BTreeMap::new(),
            pending: VecDeque::new(),
            batch: None,
            describe: None,
            settled: Vec::new(),
        }
    }

    fn call(&mut self, state: FeedState, call: IoCall) -> WalkNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn add(&mut self, range_id: Ulid, parked: Parked) {
        let batch = self.batches.entry((parked.set_id, range_id)).or_default();
        batch.0.push(change_of(parked.key, parked.head));
        batch.1.push(parked.row);
    }

    /// Sorts the rows into range batches until a key needs its range looked up.
    fn next_row(&mut self) -> WalkNext {
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
            let locate = locate_call(set_id, &parked.key);
            self.parked = Some(parked);
            return self.call(FeedState::Locate, locate);
        }
        let batches = std::mem::take(&mut self.batches);
        if batches.is_empty() {
            return self.settle();
        }
        let keys = batches
            .keys()
            .map(|(set_id, range_id)| state_key(*set_id, *range_id));
        let read = batch_read(SEARCH_INDEX_KEYSPACE, keys.collect::<Vec<_>>());
        self.pending = (batches.into_iter())
            .map(|(range, batch)| (range, batch, None))
            .collect();
        self.call(FeedState::States, read)
    }

    /// Applies the next batch whose range has an index; rows of ranges without one settle.
    fn next_batch(&mut self) -> WalkNext {
        while let Some(((set_id, range_id), (changes, rows), state)) = self.pending.pop_front() {
            let state = state
                .map(|value| IndexState::from_bytes(&value))
                .transpose()?;
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
                    let exists = index(IndexEffect::Exists {
                        set_id,
                        range_id,
                        generation,
                    });
                    return self.call(FeedState::Exists, exists);
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

    fn describe(&mut self) -> WalkNext {
        let batch = self.batch.as_mut().ok_or(FeedError::NotFinished)?;
        let changes = std::mem::take(&mut batch.changes);
        match Describe::start(batch.set_id, changes) {
            Ok((describe, read)) => {
                self.describe = Some(describe);
                self.call(FeedState::Describe, read)
            }
            Err(changes) => self.apply(changes),
        }
    }

    fn apply(&mut self, changes: Vec<KeyChange>) -> WalkNext {
        let batch = self.batch.as_ref().ok_or(FeedError::NotFinished)?;
        let apply = index(IndexEffect::Apply {
            set_id: batch.set_id,
            range_id: batch.range_id,
            generation: batch.generation,
            changes,
        });
        self.call(FeedState::Apply, apply)
    }

    fn settle_batch(&mut self) -> WalkNext {
        if let Some(batch) = self.batch.take() {
            self.settled.extend(batch.rows);
        }
        self.next_batch()
    }

    fn settle(&mut self) -> WalkNext {
        if self.settled.is_empty() {
            return Ok(Next::Done(0));
        }
        let settle = SettleFeedOperation {
            settled: std::mem::take(&mut self.settled),
            dropped: None,
        };
        self.call(FeedState::Settle, IoCall::child(settle))
    }
}

impl Operation for FeedWalk {
    type Output = usize;
    type Error = FeedError;

    fn first(&mut self) -> WalkNext {
        self.next_row()
    }

    fn next(&mut self, result: IoResult) -> WalkNext {
        match self.state {
            FeedState::Locate => {
                let (rows, _) = result.page()?;
                let parked = self.parked.take().ok_or(FeedError::NotFinished)?;
                match located(parked.set_id, &parked.key, &rows)? {
                    Some(range) => {
                        let range_id = range.range_id;
                        self.located.push(range);
                        self.add(range_id, parked);
                    }
                    // A key outside every installed range has no index to feed.
                    None => self.settled.push(parked.row),
                }
                self.next_row()
            }
            FeedState::States => {
                for (entry, state) in self.pending.iter_mut().zip(result.values()?) {
                    entry.2 = state;
                }
                self.next_batch()
            }
            FeedState::Exists => match index_event(result)? {
                IndexEvent::Exists(true) => self.describe(),
                IndexEvent::Exists(false) => {
                    let batch = self.batch.as_ref().ok_or(FeedError::NotFinished)?;
                    let state = IndexState::Building {
                        generation: batch.generation + 1,
                        cursor: None,
                    };
                    let key = state_key(batch.set_id, batch.range_id);
                    let write = put_row(SEARCH_INDEX_KEYSPACE, key, state.to_bytes()?);
                    self.call(FeedState::MarkBuilding, write)
                }
                other => Err(wrong_index(other)),
            },
            FeedState::MarkBuilding => {
                done(result)?;
                self.settle_batch()
            }
            FeedState::Describe => {
                let describe = self.describe.as_mut().ok_or(FeedError::NotFinished)?;
                match describe.step(result)? {
                    Ok(call) => Ok(Next::Call(call)),
                    Err(changes) => {
                        self.describe = None;
                        self.apply(changes)
                    }
                }
            }
            FeedState::Apply => match result.event()? {
                Event::Index(IndexEvent::Applied) => self.settle_batch(),
                Event::Index(IndexEvent::Error(error)) => {
                    if let Some(batch) = self.batch.take() {
                        let range_id = batch.range_id;
                        warn!(%range_id, %error, "Range search index update failed");
                    }
                    self.next_batch()
                }
                other => Err(unexpected(other)),
            },
            FeedState::Settle => match result.child::<usize, FeedError>()? {
                Ok(dropped) => Ok(Next::Done(dropped)),
                // A lost commit leaves the rows for the next pass.
                Err(error) if error == IoError::Conflict.into() => Ok(Next::Done(0)),
                Err(error) => Err(error),
            },
        }
    }
}

/// What the pending call of an open walk answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenState {
    States,
    Record,
    Prune,
    Write,
}

/// Opens a building index for every live range of a page of account rows without one, in sets
/// that keep a search index. Outputs the opened count.
#[derive(Debug, PartialEq)]
struct OpenWalk {
    state: OpenState,
    rows: Vec<(Key, Value)>,
    live: Vec<Vec<u8>>,
    unopened: VecDeque<Vec<u8>>,
    current: Option<(Vec<u8>, Ulid, Ulid)>,
    searched: BTreeMap<Ulid, bool>,
    opened: usize,
}

impl OpenWalk {
    fn new(rows: Vec<(Key, Value)>) -> Self {
        Self {
            state: OpenState::States,
            rows,
            live: Vec::new(),
            unopened: VecDeque::new(),
            current: None,
            searched: BTreeMap::new(),
            opened: 0,
        }
    }

    fn call(&mut self, state: OpenState, call: IoCall) -> WalkNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn next_key(&mut self) -> WalkNext {
        while let Some(key) = self.unopened.pop_front() {
            let (set_id, range_id) = split_key(&key)?;
            self.current = Some((key, set_id, range_id));
            match self.searched.get(&set_id) {
                Some(true) => return self.prune(),
                Some(false) => continue,
                None => return self.call(OpenState::Record, record_read(set_id)),
            }
        }
        Ok(Next::Done(self.opened))
    }

    /// Leftovers of an earlier index of the range would hold stale keys.
    fn prune(&mut self) -> WalkNext {
        let (_, set_id, range_id) = *self.current.as_ref().ok_or(FeedError::NotFinished)?;
        let prune = index(IndexEffect::Prune {
            set_id,
            range_id,
            keep: None,
        });
        self.call(OpenState::Prune, prune)
    }
}

impl Operation for OpenWalk {
    type Output = usize;
    type Error = FeedError;

    fn first(&mut self) -> WalkNext {
        for (key, value) in std::mem::take(&mut self.rows) {
            let account = RangeAccount::from_bytes(&value)?;
            if !matches!(account.charge, Charge::Retired { .. }) {
                self.live.push(key.to_vec());
            }
        }
        if self.live.is_empty() {
            return Ok(Next::Done(0));
        }
        let read = batch_read(SEARCH_INDEX_KEYSPACE, self.live.clone());
        self.call(OpenState::States, read)
    }

    fn next(&mut self, result: IoResult) -> WalkNext {
        match self.state {
            OpenState::States => {
                let live = std::mem::take(&mut self.live).into_iter();
                let unopened = live.zip(result.values()?);
                let unopened = unopened.filter(|(_, state)| state.is_none());
                self.unopened = unopened.map(|(key, _)| key).collect();
                self.next_key()
            }
            OpenState::Record => {
                let (_, set_id, _) = *self.current.as_ref().ok_or(FeedError::NotFinished)?;
                let search = search_on(value(result)?)?;
                self.searched.insert(set_id, search);
                match search {
                    true => self.prune(),
                    false => self.next_key(),
                }
            }
            OpenState::Prune => match index_event(result)? {
                IndexEvent::Pruned => {
                    let (key, _, _) = self.current.take().ok_or(FeedError::NotFinished)?;
                    let state = IndexState::Building {
                        generation: 1,
                        cursor: None,
                    };
                    let write = put_row(SEARCH_INDEX_KEYSPACE, key, state.to_bytes()?);
                    self.call(OpenState::Write, write)
                }
                other => Err(wrong_index(other)),
            },
            OpenState::Write => {
                done(result)?;
                self.opened += 1;
                self.next_key()
            }
        }
    }
}

/// What the pending call of a state walk answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StateStep {
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

impl Build {
    fn new(generation: u64, cursor: Option<Vec<u8>>) -> Self {
        Self {
            generation,
            cursor,
            lower: Vec::new(),
            end: Vec::new(),
            pages: 0,
            changes: Vec::new(),
            done: false,
        }
    }
}

/// How a state walk ended: the ranges it changed and where the next pass goes on.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StateEnd {
    changed: usize,
    cursor: Option<Vec<u8>>,
}

type StateNext = Result<Next<StateEnd>, FeedError>;

/// Drops the indexes of ranges without a live account or whose set keeps no search index, and
/// builds the first building index of the page; the walk stays at that range until its index is
/// ready. Outputs the count of changed ranges and the walk's next cursor.
#[derive(Debug, PartialEq)]
struct StateWalk {
    limits: FeedLimits,
    /// Where the walk goes on when it reaches the end of its page.
    next: Option<Vec<u8>>,
    /// The last row behind the walk, where it stays while a range is built.
    previous: Option<Vec<u8>>,
    state: StateStep,
    rows: Vec<(Key, Value)>,
    pending: VecDeque<((Key, Value), Option<Value>)>,
    current: Option<Current>,
    build: Option<Build>,
    describe: Option<Describe>,
    searched: BTreeMap<Ulid, bool>,
    changed: usize,
}

impl StateWalk {
    /// Walks the state rows of one page read after `previous`.
    fn new(
        limits: FeedLimits,
        rows: Vec<(Key, Value)>,
        next: Option<Vec<u8>>,
        previous: Option<Vec<u8>>,
    ) -> Self {
        Self {
            limits,
            next,
            previous,
            state: StateStep::Accounts,
            rows,
            pending: VecDeque::new(),
            current: None,
            build: None,
            describe: None,
            searched: BTreeMap::new(),
            changed: 0,
        }
    }

    fn call(&mut self, state: StateStep, call: IoCall) -> StateNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn current(&self) -> Result<&Current, FeedError> {
        self.current.as_ref().ok_or(FeedError::NotFinished)
    }

    fn build_mut(&mut self) -> Result<&mut Build, FeedError> {
        self.build.as_mut().ok_or(FeedError::NotFinished)
    }

    fn next_range(&mut self) -> StateNext {
        if let Some(current) = self.current.take() {
            self.previous = Some(current.key);
        }
        let Some(((key, state), account)) = self.pending.pop_front() else {
            return Ok(Next::Done(StateEnd {
                changed: self.changed,
                cursor: self.next.take(),
            }));
        };
        let (set_id, range_id) = split_key(&key)?;
        let account = account
            .map(|value| RangeAccount::from_bytes(&value))
            .transpose()?
            .filter(|account| !matches!(account.charge, Charge::Retired { .. }));
        let live = account.is_some();
        self.current = Some(Current {
            key: key.to_vec(),
            set_id,
            range_id,
            state,
            account,
        });
        match (live, self.searched.get(&set_id)) {
            (true, None) => self.call(StateStep::Record, record_read(set_id)),
            (true, Some(false)) => {
                if let Some(current) = self.current.as_mut() {
                    current.account = None;
                }
                self.decide()
            }
            _ => self.decide(),
        }
    }

    fn decide(&mut self) -> StateNext {
        let current = self.current()?;
        let (set_id, range_id) = (current.set_id, current.range_id);
        let live = current.account.is_some();
        match (live, IndexState::from_bytes(&current.state)?) {
            (false, _) => {
                let prune = index(IndexEffect::Prune {
                    set_id,
                    range_id,
                    keep: None,
                });
                self.call(StateStep::PruneGone, prune)
            }
            (true, IndexState::Building { generation, cursor }) => {
                self.build = Some(Build::new(generation, cursor));
                let outdated = index(IndexEffect::Outdated {
                    set_id,
                    range_id,
                    generation,
                });
                self.call(StateStep::Outdated, outdated)
            }
            (true, IndexState::Ready { generation }) => {
                self.build = Some(Build::new(generation, None));
                let exists = index(IndexEffect::Exists {
                    set_id,
                    range_id,
                    generation,
                });
                self.call(StateStep::Exists, exists)
            }
        }
    }

    /// Reads the next inventory page of the range being built.
    fn build_page(&mut self) -> StateNext {
        let build = self.build.as_ref().ok_or(FeedError::NotFinished)?;
        let start = match &build.cursor {
            Some(cursor) => IterStart::After(cursor.clone().into()),
            None => IterStart::At(build.lower.clone().into()),
        };
        let range = StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(build.end.clone().into()),
            limit: self.limits.rebuild_rows,
            max_bytes: self.limits.rebuild_bytes,
            txn_id: None,
        };
        self.call(
            StateStep::BuildPage,
            IoCall::Effect(Box::new(Effect::Storage(range))),
        )
    }

    fn build_read(&mut self, rows: Vec<(Key, Value)>, truncated: Option<Key>) -> StateNext {
        let pages = self.limits.rebuild_pages.max(1);
        let build = self.build_mut()?;
        for (key, value) in &rows {
            let position = Position::decode(key)?;
            if position.entry == Entry::Head {
                let head = HeadEntry::from_bytes(value)?;
                build.changes.push(change_of(position.key, Some(head)));
            }
        }
        build.pages += 1;
        match truncated {
            Some(truncated) => {
                build.cursor = Some(truncated.to_vec());
                if build.pages < pages {
                    return self.build_page();
                }
            }
            None => build.done = true,
        }
        let changes = std::mem::take(&mut build.changes);
        let set_id = self.current()?.set_id;
        match Describe::start(set_id, changes) {
            Ok((describe, read)) => {
                self.describe = Some(describe);
                self.call(StateStep::BuildDescribe, read)
            }
            Err(changes) => self.build_apply(changes),
        }
    }

    fn build_apply(&mut self, changes: Vec<KeyChange>) -> StateNext {
        let current = self.current()?;
        let build = self.build.as_ref().ok_or(FeedError::NotFinished)?;
        let apply = index(IndexEffect::Apply {
            set_id: current.set_id,
            range_id: current.range_id,
            generation: build.generation,
            changes,
        });
        self.call(StateStep::BuildApply, apply)
    }

    fn write_state(&mut self, next: StateStep, state: IndexState) -> StateNext {
        let key = self.current()?.key.clone();
        let write = put_row(SEARCH_INDEX_KEYSPACE, key, state.to_bytes()?);
        self.call(next, write)
    }

    fn prune_keep(&mut self, next: StateStep) -> StateNext {
        let current = self.current()?;
        let build = self.build.as_ref().ok_or(FeedError::NotFinished)?;
        let prune = index(IndexEffect::Prune {
            set_id: current.set_id,
            range_id: current.range_id,
            keep: Some(build.generation),
        });
        self.call(next, prune)
    }

    /// Pairs the state rows with their accounts and walks them.
    fn accounts(&mut self, accounts: Vec<Option<Value>>) -> StateNext {
        let rows = std::mem::take(&mut self.rows);
        self.pending = rows.into_iter().zip(accounts).collect();
        self.next_range()
    }

    /// Starts the build of an outdated or current building index over the range's interval.
    fn outdated(&mut self, outdated: bool) -> StateNext {
        let current = self.current()?;
        let account = current.account.as_ref().ok_or(FeedError::NotFinished)?;
        let (lower, end) = range_interval(current.set_id, &account.lower, account.upper.as_deref());
        let build = self.build_mut()?;
        // A build begun with an older schema starts over in the next generation.
        if outdated {
            build.generation += 1;
            build.cursor = None;
        }
        (build.lower, build.end) = (lower, end);
        self.build_page()
    }
}

impl Operation for StateWalk {
    type Output = StateEnd;
    type Error = FeedError;

    fn first(&mut self) -> StateNext {
        if self.rows.is_empty() {
            return self.accounts(Vec::new());
        }
        let keys = self.rows.iter().map(|(key, _)| key.to_vec());
        let read = batch_read(RANGE_ACCOUNT_KEYSPACE, keys.collect::<Vec<_>>());
        self.call(StateStep::Accounts, read)
    }

    fn next(&mut self, result: IoResult) -> StateNext {
        match self.state {
            StateStep::Accounts => self.accounts(result.values()?),
            StateStep::Record => {
                let search = search_on(value(result)?)?;
                let current = self.current.as_mut().ok_or(FeedError::NotFinished)?;
                self.searched.insert(current.set_id, search);
                if !search {
                    current.account = None;
                }
                self.decide()
            }
            StateStep::PruneGone => match index_event(result)? {
                IndexEvent::Pruned => {
                    let key = self.current()?.key.clone();
                    let delete = (SEARCH_INDEX_KEYSPACE.to_string(), key.into());
                    self.call(StateStep::DeleteGone, IoCall::Delete(vec![delete]))
                }
                other => Err(wrong_index(other)),
            },
            StateStep::DeleteGone => {
                done(result)?;
                self.changed += 1;
                self.next_range()
            }
            StateStep::Outdated => match index_event(result)? {
                IndexEvent::Outdated(outdated) => self.outdated(outdated),
                other => Err(wrong_index(other)),
            },
            StateStep::BuildPage => {
                let (rows, truncated) = page_rows(result)?;
                self.build_read(rows, truncated)
            }
            StateStep::BuildDescribe => {
                let describe = self.describe.as_mut().ok_or(FeedError::NotFinished)?;
                match describe.step(result)? {
                    Ok(call) => Ok(Next::Call(call)),
                    Err(changes) => {
                        self.describe = None;
                        self.build_apply(changes)
                    }
                }
            }
            StateStep::BuildApply => match index_event(result)? {
                IndexEvent::Applied => {
                    // The cursor moves only after the commit, so a crash repeats pages, never
                    // skips.
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
                other => Err(wrong_index(other)),
            },
            StateStep::BuildState => {
                done(result)?;
                if self.build.as_ref().ok_or(FeedError::NotFinished)?.done {
                    return self.prune_keep(StateStep::BuildPrune);
                }
                // The walk stays at this range until its index is ready.
                Ok(Next::Done(StateEnd {
                    changed: self.changed + 1,
                    cursor: self.previous.take(),
                }))
            }
            StateStep::BuildPrune => match index_event(result)? {
                IndexEvent::Pruned => {
                    self.build = None;
                    self.changed += 1;
                    self.next_range()
                }
                other => Err(wrong_index(other)),
            },
            StateStep::Exists => match index_event(result)? {
                // Generations a crash or a failed prune left after a rebuild go now.
                IndexEvent::Exists(true) => self.prune_keep(StateStep::PruneOld),
                // A lost index, or one of an older schema, is built again beside it; searches
                // report the range incomplete until the new generation is ready.
                IndexEvent::Exists(false) => {
                    let build = self.build.take().ok_or(FeedError::NotFinished)?;
                    let state = IndexState::Building {
                        generation: build.generation + 1,
                        cursor: None,
                    };
                    self.write_state(StateStep::MarkBuilding, state)
                }
                other => Err(wrong_index(other)),
            },
            StateStep::MarkBuilding => {
                done(result)?;
                self.changed += 1;
                self.next_range()
            }
            StateStep::PruneOld => match index_event(result)? {
                IndexEvent::Pruned => {
                    self.build = None;
                    self.next_range()
                }
                other => Err(wrong_index(other)),
            },
        }
    }
}

/// The walks of one pass, in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Walk {
    Feed,
    Open,
    States,
}

impl Walk {
    fn name(self) -> &'static str {
        match self {
            Walk::Feed => "feed",
            Walk::Open => "open",
            Walk::States => "build",
        }
    }
}

/// What the pending child of a pass does for its walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PassStep {
    ReadPage(Walk),
    RunWalk(Walk),
}

type PassNext = Result<Next<FeedPass>, FeedError>;

/// One bounded pass over each walk, one after another: a child reads the walk's page, another
/// runs the walk over it. A failed child is logged and the next walk runs. Outputs whether the
/// pass changed an index or a row, and the new cursors.
#[derive(Debug, PartialEq)]
pub struct FeedPassOperation {
    limits: FeedLimits,
    cursors: FeedCursors,
    step: PassStep,
    changed: usize,
}

impl FeedPassOperation {
    pub fn new(limits: FeedLimits, cursors: FeedCursors) -> Self {
        Self {
            limits,
            cursors,
            step: PassStep::ReadPage(Walk::Feed),
            changed: 0,
        }
    }

    fn read_page(&mut self, walk: Walk) -> Next<FeedPass> {
        self.step = PassStep::ReadPage(walk);
        let (key_space, after, limit) = match walk {
            Walk::Feed => (
                SEARCH_FEED_KEYSPACE,
                self.cursors.feed.clone(),
                self.limits.feed_rows,
            ),
            Walk::Open => (
                RANGE_ACCOUNT_KEYSPACE,
                self.cursors.accounts.clone(),
                self.limits.account_rows,
            ),
            Walk::States => (
                SEARCH_INDEX_KEYSPACE,
                self.cursors.states.clone(),
                self.limits.state_rows,
            ),
        };
        Next::Call(IoCall::child(ReadPageOperation {
            key_space,
            after,
            limit,
        }))
    }

    /// Moves the walk's cursor and runs the walk over its page. A state walk holds its cursor
    /// back until it ends, so a failed one starts over at the next pass.
    fn run_walk(&mut self, walk: Walk, (rows, next): RowPage) -> Next<FeedPass> {
        self.step = PassStep::RunWalk(walk);
        let next = next.map(|key| key.to_vec());
        Next::Call(match walk {
            Walk::Feed => {
                self.cursors.feed = next;
                IoCall::child(FeedWalk::new(rows))
            }
            Walk::Open => {
                self.cursors.accounts = next;
                IoCall::child(OpenWalk::new(rows))
            }
            Walk::States => {
                let previous = self.cursors.states.take();
                IoCall::child(StateWalk::new(self.limits, rows, next, previous))
            }
        })
    }

    /// Starts the walk after `walk`, or ends the pass after the last one.
    fn after(&mut self, walk: Walk) -> Next<FeedPass> {
        match walk {
            Walk::Feed => self.read_page(Walk::Open),
            Walk::Open => self.read_page(Walk::States),
            Walk::States => Next::Done(FeedPass {
                busy: self.changed > 0,
                cursors: std::mem::take(&mut self.cursors),
            }),
        }
    }

    fn failed(&mut self, walk: Walk, error: FeedError) -> Next<FeedPass> {
        let name = walk.name();
        warn!(walk = name, %error, "Range search index pass failed");
        self.after(walk)
    }
}

impl Operation for FeedPassOperation {
    type Output = FeedPass;
    type Error = FeedError;

    fn first(&mut self) -> PassNext {
        Ok(self.read_page(Walk::Feed))
    }

    fn next(&mut self, result: IoResult) -> PassNext {
        Ok(match self.step {
            PassStep::ReadPage(walk) => match result.child::<RowPage, FeedError>()? {
                Ok(rows) => self.run_walk(walk, rows),
                Err(error) => self.failed(walk, error),
            },
            PassStep::RunWalk(Walk::States) => match result.child::<StateEnd, FeedError>()? {
                Ok(end) => {
                    self.changed += end.changed;
                    self.cursors.states = end.cursor;
                    self.after(Walk::States)
                }
                Err(error) => self.failed(Walk::States, error),
            },
            PassStep::RunWalk(walk) => match result.child::<usize, FeedError>()? {
                Ok(count) => {
                    self.changed += count;
                    self.after(walk)
                }
                Err(error) => self.failed(walk, error),
            },
        })
    }
}

/// What the pending call of a progress read answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgressStep {
    Held,
    States,
    Count,
}

/// The search index progress of every range of a bucket this node holds, while its set keeps a
/// search index; a range whose index is not opened yet has none indexed.
#[derive(Debug, PartialEq)]
pub(super) struct RangeProgressOperation {
    bucket: String,
    step: ProgressStep,
    set_id: Ulid,
    pending: VecDeque<(HeldRange, Option<IndexState>)>,
    current: Option<(HeldRange, Option<IndexState>)>,
    held: Vec<HeldRange>,
    progress: BTreeMap<Ulid, IndexProgress>,
}

impl RangeProgressOperation {
    pub(super) fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            step: ProgressStep::Held,
            set_id: Ulid::nil(),
            pending: VecDeque::new(),
            current: None,
            held: Vec::new(),
            progress: BTreeMap::new(),
        }
    }

    fn add(
        &mut self,
        range: &HeldRange,
        state: Option<&IndexState>,
        indexed: u64,
    ) -> Result<(), FeedError> {
        let counters = sum_held([range]).map_err(|error| FeedError::Storage(error.to_string()))?;
        let entry = IndexProgress {
            ready: matches!(state, Some(IndexState::Ready { .. })),
            indexed,
            keys: u64::try_from(counters.objects).unwrap_or_default(),
        };
        self.progress.insert(range.range_id, entry);
        Ok(())
    }

    /// Counts the next range with an index; one without has none indexed.
    fn next_range(&mut self) -> Result<Next<BTreeMap<Ulid, IndexProgress>>, FeedError> {
        while let Some((range, state)) = self.pending.pop_front() {
            let Some(generation) = state.as_ref().map(IndexState::generation) else {
                self.add(&range, None, 0)?;
                continue;
            };
            let count = index(IndexEffect::Count {
                set_id: self.set_id,
                range_id: range.range_id,
                generation,
            });
            self.current = Some((range, state));
            self.step = ProgressStep::Count;
            return Ok(Next::Call(count));
        }
        Ok(Next::Done(std::mem::take(&mut self.progress)))
    }
}

impl Operation for RangeProgressOperation {
    type Output = BTreeMap<Ulid, IndexProgress>;
    type Error = FeedError;

    fn first(&mut self) -> Result<Next<BTreeMap<Ulid, IndexProgress>>, FeedError> {
        let held = HeldRangesOperation::new(&self.bucket);
        Ok(Next::Call(IoCall::child(held)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<BTreeMap<Ulid, IndexProgress>>, FeedError> {
        match self.step {
            ProgressStep::Held => {
                let held = result.child::<Option<HeldRanges>, UsageError>()?;
                let held = held.map_err(|error| FeedError::Storage(error.to_string()))?;
                let Some((binding, snapshot, held)) = held else {
                    return Ok(Next::Done(BTreeMap::new()));
                };
                if !snapshot.config.settings.search_index {
                    return Ok(Next::Done(BTreeMap::new()));
                }
                self.set_id = binding.set_id;
                let keys = held.iter().map(|range| state_key(binding.set_id, range.range_id));
                let read = batch_read(SEARCH_INDEX_KEYSPACE, keys.collect::<Vec<_>>());
                self.held = held;
                self.step = ProgressStep::States;
                Ok(Next::Call(read))
            }
            ProgressStep::States => {
                let held = std::mem::take(&mut self.held);
                for (range, state) in held.into_iter().zip(result.values()?) {
                    let state = state
                        .map(|value| IndexState::from_bytes(&value))
                        .transpose()?;
                    self.pending.push_back((range, state));
                }
                self.next_range()
            }
            ProgressStep::Count => {
                let (range, state) = self.current.take().ok_or(FeedError::NotFinished)?;
                let IndexEvent::Counted(count) = index_event(result)? else {
                    return Err(FeedError::NotFinished);
                };
                self.add(&range, state.as_ref(), count)?;
                self.next_range()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distribution::test_record::{binding, record, searched};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;
    use aruna_core::structs::storage::distribution::accounting::RangeCounters;

    fn rows(values: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Key, Value)> {
        (values.into_iter())
            .map(|(key, value)| (key.into(), value.into()))
            .collect()
    }

    fn read(values: Vec<Option<Vec<u8>>>) -> IoResult {
        IoResult::Values(values.into_iter().map(|value| value.map(Value::from)).collect())
    }

    fn record_value(searching: bool) -> Option<Vec<u8>> {
        let (_, bytes) = match searching {
            true => searched(),
            false => record(true),
        };
        Some(bytes)
    }

    fn indexed(event: IndexEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Index(event)))
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn is_child<T>(next: Result<Next<T>, FeedError>) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    fn timeout() -> FeedError {
        IoError::Storage(StorageError::Timeout).into()
    }

    // An undecodable feed row settles; it is deleted only while it still holds the value read.
    #[test]
    fn settled_rows_guarded() {
        let row = (vec![1], vec![0xFF]);
        let mut walk = FeedWalk::new(rows(vec![row.clone()]));
        assert!(is_child(walk.first()));
        assert_eq!(walk.state, FeedState::Settle);

        let settle = || SettleFeedOperation {
            settled: rows(vec![row.clone()]),
            dropped: None,
        };
        let mut same = settle();
        assert_eq!(same.transaction(), TransactionMode::Write);
        let reads = vec![(SEARCH_FEED_KEYSPACE.to_string(), Key::from(vec![1]))];
        assert_eq!(same.first(), Ok(Next::Call(IoCall::Read(reads.clone()))));
        assert_eq!(
            same.next(read(vec![Some(vec![0xFF])])),
            Ok(Next::Call(IoCall::Delete(reads)))
        );
        assert_eq!(same.next(IoResult::Done), Ok(Next::Done(1)));

        let mut changed = settle();
        changed.first().unwrap();
        let moved = changed.next(read(vec![Some(vec![0xFE])]));
        assert_eq!(moved, Ok(Next::Done(0)));
    }

    // A lost settle commit leaves the rows for the next pass; another failure fails the walk.
    #[test]
    fn settle_conflict_counts_nothing() {
        let settling = || {
            let mut walk = FeedWalk::new(rows(vec![(vec![1], vec![0xFF])]));
            walk.first().unwrap();
            walk
        };
        let lost: Result<usize, FeedError> = Err(IoError::Conflict.into());
        assert_eq!(settling().next(child(lost)), Ok(Next::Done(0)));
        let failed: Result<usize, FeedError> = Err(timeout());
        assert_eq!(settling().next(child(failed)), Err(timeout()));
    }

    // A live range without a state row in a searched set gets its leftovers pruned and a first
    // building generation; one in a set without a search index is skipped.
    #[test]
    fn opens_searched_ranges() {
        let (snapshot, _) = searched();
        let range_id = Ulid::from_bytes([1; 16]);
        let key = state_key(snapshot.set_id(), range_id);
        let account = RangeAccount::fresh(String::new(), None).to_bytes().unwrap();
        let building = IndexState::Building {
            generation: 1,
            cursor: None,
        };
        let page = || rows(vec![(key.clone(), account.clone())]);
        let mut walk = OpenWalk::new(page());
        let states = batch_read(SEARCH_INDEX_KEYSPACE, [key.clone()]);
        assert_eq!(walk.first(), Ok(Next::Call(states)));
        let record_call = record_read(snapshot.set_id());
        assert_eq!(walk.next(read(vec![None])), Ok(Next::Call(record_call)));
        let prune = index(IndexEffect::Prune {
            set_id: snapshot.set_id(),
            range_id,
            keep: None,
        });
        assert_eq!(
            walk.next(read(vec![record_value(true)])),
            Ok(Next::Call(prune))
        );
        let write = put_row(
            SEARCH_INDEX_KEYSPACE,
            key.clone(),
            building.to_bytes().unwrap(),
        );
        assert_eq!(
            walk.next(indexed(IndexEvent::Pruned)),
            Ok(Next::Call(write))
        );
        assert_eq!(walk.next(IoResult::Done), Ok(Next::Done(1)));

        let mut walk = OpenWalk::new(page());
        walk.first().unwrap();
        walk.next(read(vec![None])).unwrap();
        assert_eq!(walk.next(read(vec![record_value(false)])), Ok(Next::Done(0)));
    }

    // A state row without a live account loses its index and its row; the walk then moves on.
    #[test]
    fn drops_gone_ranges() {
        let (set_id, range_id) = (Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        let key = state_key(set_id, range_id);
        let ready = IndexState::Ready { generation: 2 }.to_bytes().unwrap();
        let mut walk = StateWalk::new(
            FeedLimits::default(),
            rows(vec![(key.clone(), ready)]),
            Some(vec![7]),
            Some(vec![0]),
        );
        let accounts = batch_read(RANGE_ACCOUNT_KEYSPACE, [key.clone()]);
        assert_eq!(walk.first(), Ok(Next::Call(accounts)));
        let prune = index(IndexEffect::Prune {
            set_id,
            range_id,
            keep: None,
        });
        assert_eq!(walk.next(read(vec![None])), Ok(Next::Call(prune)));
        let delete = (SEARCH_INDEX_KEYSPACE.to_string(), key.into());
        assert_eq!(
            walk.next(indexed(IndexEvent::Pruned)),
            Ok(Next::Call(IoCall::Delete(vec![delete])))
        );
        let end = StateEnd {
            changed: 1,
            cursor: Some(vec![7]),
        };
        assert_eq!(walk.next(IoResult::Done), Ok(Next::Done(end)));
    }

    // A building range writes its cursor and keeps the walk at the row before it.
    #[test]
    fn build_stays_at_range() {
        let (set_id, range_id) = (Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        let key = state_key(set_id, range_id);
        let building = IndexState::Building {
            generation: 2,
            cursor: None,
        };
        let account = RangeAccount::fresh(String::new(), None).to_bytes().unwrap();
        let state_rows = rows(vec![(key.clone(), building.to_bytes().unwrap())]);
        let limits = FeedLimits {
            rebuild_pages: 1,
            ..FeedLimits::default()
        };
        let mut walk = StateWalk::new(limits, state_rows, None, Some(vec![0]));
        walk.first().unwrap();
        walk.next(read(vec![Some(account)])).unwrap();
        walk.next(read(vec![record_value(true)])).unwrap();
        let Ok(Next::Call(IoCall::Effect(_))) = walk.next(indexed(IndexEvent::Outdated(false)))
        else {
            panic!("expected the inventory page");
        };
        let more = IoResult::Page {
            rows: Vec::new(),
            next: Some(Key::from(vec![9])),
        };
        let apply = index(IndexEffect::Apply {
            set_id,
            range_id,
            generation: 2,
            changes: Vec::new(),
        });
        assert_eq!(walk.next(more), Ok(Next::Call(apply)));
        let state = IndexState::Building {
            generation: 2,
            cursor: Some(vec![9]),
        };
        let write = put_row(SEARCH_INDEX_KEYSPACE, key, state.to_bytes().unwrap());
        assert_eq!(
            walk.next(indexed(IndexEvent::Applied)),
            Ok(Next::Call(write))
        );
        let end = StateEnd {
            changed: 1,
            cursor: Some(vec![0]),
        };
        assert_eq!(walk.next(IoResult::Done), Ok(Next::Done(end)));
    }

    // An index failure fails a state walk.
    #[test]
    fn index_failure_fails_walk() {
        let key = state_key(Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
        let ready = IndexState::Ready { generation: 2 }.to_bytes().unwrap();
        let page = rows(vec![(key, ready)]);
        let mut walk = StateWalk::new(FeedLimits::default(), page, None, None);
        walk.first().unwrap();
        walk.next(read(vec![None])).unwrap();
        let failed = IndexError::Failed("disk".to_string());
        let answer = indexed(IndexEvent::Error(failed.clone()));
        assert_eq!(walk.next(answer), Err(failed.into()));
    }

    fn empty_page(next: Option<Vec<u8>>) -> IoResult {
        let page: Result<RowPage, FeedError> = Ok((Vec::new(), next.map(Key::from)));
        child(page)
    }

    // A pass reads and runs the three walks in order and reports the cursors they reached.
    #[test]
    fn pass_runs_walks() {
        let limits = FeedLimits::default();
        let mut pass = FeedPassOperation::new(limits, FeedCursors::default());
        assert!(is_child(pass.first()));
        assert!(is_child(pass.next(empty_page(None))));
        let fed: Result<usize, FeedError> = Ok(0);
        assert!(is_child(pass.next(child(fed))));
        assert_eq!(pass.step, PassStep::ReadPage(Walk::Open));
        assert!(is_child(pass.next(empty_page(Some(vec![5])))));
        let opened: Result<usize, FeedError> = Ok(0);
        assert!(is_child(pass.next(child(opened))));
        assert!(is_child(pass.next(empty_page(None))));
        let states: Result<StateEnd, FeedError> = Ok(StateEnd {
            changed: 0,
            cursor: None,
        });
        let cursors = FeedCursors {
            feed: None,
            accounts: Some(vec![5]),
            states: None,
        };
        let done = FeedPass {
            busy: false,
            cursors,
        };
        assert_eq!(pass.next(child(states)), Ok(Next::Done(done)));
    }

    // A failed page read keeps the walk's cursor and the next walk still runs.
    #[test]
    fn pass_survives_failure() {
        let cursors = FeedCursors {
            feed: Some(vec![1]),
            ..FeedCursors::default()
        };
        let mut pass = FeedPassOperation::new(FeedLimits::default(), cursors);
        pass.first().unwrap();
        let failed: Result<RowPage, FeedError> = Err(timeout());
        assert!(is_child(pass.next(child(failed))));
        assert_eq!(pass.step, PassStep::ReadPage(Walk::Open));
        assert_eq!(pass.cursors.feed, Some(vec![1]));
    }

    // A failed state walk starts over at the next pass, as its cursor is held back meanwhile.
    #[test]
    fn failed_walk_restarts() {
        let cursors = FeedCursors {
            states: Some(vec![0]),
            ..FeedCursors::default()
        };
        let mut pass = FeedPassOperation::new(FeedLimits::default(), cursors);
        pass.step = PassStep::ReadPage(Walk::States);
        assert!(is_child(pass.next(empty_page(Some(vec![7])))));
        let failed: Result<StateEnd, FeedError> = Err(timeout());
        let Ok(Next::Done(done)) = pass.next(child(failed)) else {
            panic!("expected the pass to end");
        };
        assert_eq!(done.cursors.states, None);
    }

    // A result of another kind than the pending child fails the pass.
    #[test]
    fn rejects_unexpected() {
        let mut pass = FeedPassOperation::new(FeedLimits::default(), FeedCursors::default());
        pass.first().unwrap();
        assert!(pass.next(IoResult::Done).is_err());
    }

    fn held(seed: u8, objects: i128) -> HeldRange {
        HeldRange {
            range_id: Ulid::from_bytes([seed; 16]),
            complete: true,
            coordinated: true,
            rows: vec![(
                vec![seed],
                RangeCounters {
                    objects,
                    ..RangeCounters::default()
                },
            )],
        }
    }

    /// A progress read of two held ranges, waiting for their state rows.
    fn progress_read() -> (RangeProgressOperation, Ulid) {
        let (snapshot, _) = searched();
        let set_id = snapshot.set_id();
        let bound = (binding(&snapshot, 1, 5), snapshot, vec![held(1, 7), held(2, 8)]);
        let mut read = RangeProgressOperation::new("spire");
        assert!(is_child(read.first()));
        let keys = [1, 2].map(|seed| state_key(set_id, Ulid::from_bytes([seed; 16])));
        let found = read.next(child(Ok::<_, UsageError>(Some(bound))));
        assert_eq!(found, Ok(Next::Call(batch_read(SEARCH_INDEX_KEYSPACE, keys))));
        (read, set_id)
    }

    // A range with an index counts its generation; one without has none indexed.
    #[test]
    fn counts_range_progress() {
        let (mut read, set_id) = progress_read();
        let state = IndexState::Ready { generation: 3 };
        let count = index(IndexEffect::Count {
            set_id,
            range_id: Ulid::from_bytes([2; 16]),
            generation: 3,
        });
        let states = read.next(self::read(vec![None, Some(state.to_bytes().unwrap())]));
        assert_eq!(states, Ok(Next::Call(count)));
        let Ok(Next::Done(progress)) = read.next(indexed(IndexEvent::Counted(5))) else {
            panic!("expected the progress");
        };
        let unopened = IndexProgress {
            ready: false,
            indexed: 0,
            keys: 7,
        };
        let ready = IndexProgress {
            ready: true,
            indexed: 5,
            keys: 8,
        };
        assert_eq!(progress[&Ulid::from_bytes([1; 16])], unopened);
        assert_eq!(progress[&Ulid::from_bytes([2; 16])], ready);
    }

    // A failed count fails the read, so no partial progress is shown.
    #[test]
    fn count_failure_fails() {
        let (mut read, _) = progress_read();
        let state = IndexState::Ready { generation: 3 };
        let states = self::read(vec![Some(state.to_bytes().unwrap()), None]);
        read.next(states).unwrap();
        let failed = IndexError::Failed("disk".to_string());
        let answer = read.next(indexed(IndexEvent::Error(failed.clone())));
        assert_eq!(answer, Err(FeedError::Index(failed)));
    }

    // No held ranges, or a set without a search index, have no progress to report.
    #[test]
    fn progress_needs_search() {
        let finished = |held: Option<HeldRanges>| {
            let mut read = RangeProgressOperation::new("spire");
            read.first().unwrap();
            read.next(child(Ok::<_, UsageError>(held)))
        };
        let none = Ok(Next::Done(BTreeMap::new()));
        assert_eq!(finished(None), none);
        let (snapshot, _) = record(true);
        let plain = (binding(&snapshot, 1, 5), snapshot, vec![held(1, 7)]);
        assert_eq!(finished(Some(plain)), none);
    }
}
