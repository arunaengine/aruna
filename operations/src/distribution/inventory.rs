//! Maintains the ordered inventory, version id index, range counters and key sample of a
//! distributed bucket inside the transaction that writes the versions and the head.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CONTENT_INDEX_KEYSPACE, INVENTORY_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, RANGE_INDEX_KEYSPACE, SEARCH_FEED_KEYSPACE, SOURCE_ROOT_KEYSPACE,
    UPLOAD_INVENTORY_KEYSPACE, VERSION_INDEX_KEYSPACE,
};
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, CurrentVersionPointer};
use aruna_core::structs::storage::distribution::accounting::{
    Charge, CounterOverflow, RangeAccount, RangeCounters, RangeSlot, SAMPLE_SIZE, SampleEntry,
    account_key, generation_key, pending_key, sample_count_key, sample_prefix, slot_key, slot_seek,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, HeadEntry, IndexEntry, Position, VersionEntry, content_key, head_position, index_key,
    logical_identity, upload_position, version_position,
};
use aruna_core::structs::storage::distribution::search::feed_value;
use aruna_core::structs::storage::distribution::source_root::{RootEntry, root_key};
use aruna_core::structs::storage::distribution::{DistributionBinding, KeyRange, OriginalEpoch};
use aruna_core::types::{Effects, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum InventoryError {
    #[error("no installed range of set {set_id} contains key {key:?}")]
    RangeMissing { set_id: Ulid, key: String },
    #[error("range {0} was split; retry against its children")]
    RangeRetired(Ulid),
    #[error("version id {0} is already indexed with another key, epoch or content")]
    Collision(Ulid),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Overflow(#[from] CounterOverflow),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("inventory update received unexpected event: {0:?}")]
    UnexpectedEvent(Box<Event>),
}

/// One version written to the key. An indexed id of the same key, epoch and content is the
/// same version: a replay changes nothing and a changed entry is replaced in place.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AddedVersion {
    pub version_id: Ulid,
    pub epoch: OriginalEpoch,
    /// The key's head generation the version was written as.
    pub generation: u64,
    pub entry: VersionEntry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeadChange {
    Keep,
    Set(HeadEntry),
    Remove,
}

/// What one transaction does to one key of a distributed bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventoryChange {
    pub set_id: Ulid,
    pub key: String,
    pub added: Vec<AddedVersion>,
    pub removed: Vec<Ulid>,
    pub head: HeadChange,
    /// A multipart upload opened (`true`) or closed (`false`).
    pub upload: Option<(Ulid, bool)>,
    /// The source root of a received external reference, which a new coordinator checks.
    pub root: Option<RootEntry>,
    /// Whether a changed head feeds the range's search index; off when the set keeps none.
    pub feed: bool,
}

impl InventoryChange {
    pub fn new(set_id: Ulid, key: impl Into<String>) -> Self {
        Self {
            set_id,
            key: key.into(),
            added: Vec::new(),
            removed: Vec::new(),
            head: HeadChange::Keep,
            upload: None,
            root: None,
            feed: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase {
    Pending,
    Locate,
    Read,
    SampleCount,
    SampleProbe,
    SampleRead,
    Write,
    Delete,
    Count,
    Done,
}

/// Which inventory a contribution's position belongs to, so it is compared with that cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Walk {
    Versions,
    Uploads,
}

/// Most sample rows, and bytes, one range's sample read takes in the transaction.
const SAMPLE_READ_ROWS: usize = 2 * SAMPLE_SIZE as usize;
const SAMPLE_READ_BYTES: usize = 1 << 20;

/// A range found in the range index, with its key bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Located {
    set_id: Ulid,
    range_id: Ulid,
    lower: String,
    upper: Option<String>,
}

impl Located {
    fn contains(&self, set_id: Ulid, key: &str) -> bool {
        let below = self.upper.as_deref().is_none_or(|upper| key < upper);
        self.set_id == set_id && self.lower.as_str() <= key && below
    }
}

/// The keys of one range that may enter its sample, in sample order, and the sample's size.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Sampling {
    set_id: Ulid,
    range_id: Ulid,
    candidates: Vec<SampleEntry>,
    size: i128,
}

/// What one key's change does, planned from the key's rows read in the transaction.
#[derive(Clone, Debug, PartialEq)]
struct KeyPlan {
    change: InventoryChange,
    range_id: Ulid,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    counters: Vec<(Key, RangeCounters)>,
    /// A key that gained a head and may enter the sample.
    candidate: Option<SampleEntry>,
    /// Sample entries the change removed with the key's head.
    unsampled: i128,
}

/// Embeddable update of the inventory rows of one or more keys. It works in rounds of distinct
/// keys and version ids: a round reads the range index and each range's account row once in the
/// host transaction, so a split or a rebuild page conflicts it, reads all its keys' rows in one
/// batch, settles each range's sample once and stages its writes and counter deltas together.
#[derive(Clone, Debug, PartialEq)]
pub struct InventoryUpdate {
    queued: VecDeque<InventoryChange>,
    unlocated: Vec<InventoryChange>,
    keys: Vec<KeyPlan>,
    located: Vec<Located>,
    /// The round's ranges in read order; their account rows lead the round's read.
    ranges: Vec<(Ulid, Ulid)>,
    sampling: VecDeque<Sampling>,
    phase: Phase,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    counters: Vec<(Key, RangeCounters)>,
    /// Net entries earlier rounds staged in each range's sample, keyed by every range whose
    /// sample they changed; the committed rows do not show them yet.
    sampled: BTreeMap<Ulid, i128>,
}

fn row(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> (String, Key, Value) {
    (key_space.to_string(), key.into(), value.into())
}

impl InventoryUpdate {
    pub fn new(change: InventoryChange) -> Self {
        Self {
            queued: VecDeque::from([change]),
            unlocated: Vec::new(),
            keys: Vec::new(),
            located: Vec::new(),
            ranges: Vec::new(),
            sampling: VecDeque::new(),
            phase: Phase::Pending,
            writes: Vec::new(),
            deletes: Vec::new(),
            counters: Vec::new(),
            sampled: BTreeMap::new(),
        }
    }

    /// Adds the change of another key before the update starts.
    pub fn push(&mut self, change: InventoryChange) {
        self.queued.push_back(change);
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    #[cfg(test)]
    pub fn change(&self) -> &InventoryChange {
        self.queued.front().expect("a queued change")
    }

    #[cfg(test)]
    pub fn change_mut(&mut self) -> &mut InventoryChange {
        self.queued.front_mut().expect("a queued change")
    }

    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        self.next_round(txn_id).unwrap_or_default()
    }

    /// Returns `Ok(Some(effects))` while storage work remains and `Ok(None)` once every row
    /// and counter delta is staged in the transaction.
    pub fn step(&mut self, event: Event, txn_id: TxnId) -> Result<Option<Effects>, InventoryError> {
        let event = match event {
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            event => event,
        };
        match (&self.phase, event) {
            (Phase::Locate, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.locate(&values)?;
                Ok(Some(self.locate_next(txn_id)))
            }
            (Phase::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.plan_keys(values)?;
                Ok(self.count_samples(txn_id))
            }
            (Phase::SampleCount, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.size_samples(values)?;
                Ok(self.next_sample(txn_id, false))
            }
            (Phase::SampleProbe, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let last = values.first().and_then(|(key, _)| SampleEntry::decode(key));
                let first = self.sampling.front().and_then(|at| at.candidates.first());
                let enters =
                    first.is_some_and(|first| last.is_none_or(|last| ranks_below(first, &last)));
                if !enters {
                    self.sampling.pop_front();
                    return Ok(self.next_sample(txn_id, false));
                }
                Ok(self.next_sample(txn_id, true))
            }
            (
                Phase::SampleRead,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                // A sample far above its size is not read whole; its candidates stay out.
                if let (Some(sampling), None) = (self.sampling.pop_front(), next_start_after) {
                    let existing = values
                        .iter()
                        .filter_map(|(key, _)| SampleEntry::decode(key));
                    self.settle(sampling, existing.collect())?;
                }
                Ok(self.next_sample(txn_id, false))
            }
            (Phase::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
            | (Phase::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. }))
            | (Phase::Count, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                Ok(self.next_effect(txn_id))
            }
            (_, event) => Err(InventoryError::UnexpectedEvent(Box::new(event))),
        }
    }

    /// Takes the next round of distinct keys and version ids, or ends the update.
    fn next_round(&mut self, txn_id: TxnId) -> Option<Effects> {
        let mut keys = BTreeSet::new();
        let mut ids = BTreeSet::new();
        while let Some(change) = self.queued.front() {
            let added = change.added.iter().map(|added| added.version_id);
            let versions: Vec<Ulid> = added.chain(change.removed.iter().copied()).collect();
            let key = (change.set_id, change.key.clone());
            let repeated = keys.contains(&key) || versions.iter().any(|id| ids.contains(id));
            if repeated && !self.unlocated.is_empty() {
                break;
            }
            keys.insert(key);
            ids.extend(versions);
            self.unlocated.extend(self.queued.pop_front());
        }
        if self.unlocated.is_empty() {
            self.phase = Phase::Done;
            return None;
        }
        Some(self.locate_next(txn_id))
    }

    /// Seeks the range of the first key outside every located range, or reads the round's
    /// rows once every key has its range.
    fn locate_next(&mut self, txn_id: TxnId) -> Effects {
        for change in std::mem::take(&mut self.unlocated) {
            let range =
                (self.located.iter()).find(|range| range.contains(change.set_id, &change.key));
            match range {
                Some(range) => self.keys.push(KeyPlan::new(change, range.range_id)),
                None => self.unlocated.push(change),
            }
        }
        let Some(change) = self.unlocated.first() else {
            return self.read_rows(txn_id);
        };
        self.phase = Phase::Locate;
        let set_id = change.set_id;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: RANGE_INDEX_KEYSPACE.to_string(),
            prefix: Some(set_id.to_bytes().to_vec().into()),
            start: Some(IterStart::After(slot_seek(set_id, &change.key).into())),
            limit: 1,
            txn_id: Some(txn_id),
        })]
    }

    /// Takes the range index row found for the first unlocated key.
    fn locate(&mut self, values: &[(Key, Value)]) -> Result<(), InventoryError> {
        let Some(change) = self.unlocated.first() else {
            return Ok(());
        };
        let missing = || InventoryError::RangeMissing {
            set_id: change.set_id,
            key: change.key.clone(),
        };
        let (row_key, value) = values.first().ok_or_else(missing)?;
        let slot = RangeSlot::from_bytes(value)?;
        // The open end's row key is no UTF-8 text.
        let upper = (row_key.get(Ulid::nil().to_bytes().len()..))
            .and_then(|upper| std::str::from_utf8(upper).ok());
        let located = Located {
            set_id: change.set_id,
            range_id: slot.range_id,
            lower: slot.lower,
            upper: upper.map(str::to_string),
        };
        if !located.contains(change.set_id, &change.key) {
            return Err(missing());
        }
        self.located.push(located);
        Ok(())
    }

    fn read_rows(&mut self, txn_id: TxnId) -> Effects {
        self.phase = Phase::Read;
        self.ranges.clear();
        for key in &self.keys {
            let range = (key.change.set_id, key.range_id);
            if !self.ranges.contains(&range) {
                self.ranges.push(range);
            }
        }
        let accounts = (self.ranges.iter()).map(|(set_id, range_id)| {
            let key = account_key(*set_id, *range_id);
            (RANGE_ACCOUNT_KEYSPACE.to_string(), key.into())
        });
        let mut reads: Vec<(String, Key)> = accounts.collect();
        for key in &self.keys {
            reads.extend(key.reads());
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// Plans every key of the round from the read rows and gathers its staged work.
    fn plan_keys(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<(), InventoryError> {
        let mut values = values.into_iter().map(|(_, value)| value);
        let mut accounts = Vec::with_capacity(self.ranges.len());
        for _ in &self.ranges {
            let account = values.next().flatten();
            accounts.push(
                account
                    .map(|value| RangeAccount::from_bytes(&value))
                    .transpose()?,
            );
        }
        let mut next = || values.next().flatten();
        let mut candidates: BTreeMap<(Ulid, Ulid), Vec<SampleEntry>> = BTreeMap::new();
        for mut key in std::mem::take(&mut self.keys) {
            let range = (key.change.set_id, key.range_id);
            let at = self.ranges.iter().position(|known| *known == range);
            let account = at.and_then(|at| accounts.get(at)).and_then(Option::as_ref);
            let account = account.ok_or_else(|| InventoryError::RangeMissing {
                set_id: key.change.set_id,
                key: key.change.key.clone(),
            })?;
            key.plan(account, &mut next)?;
            if let Some(candidate) = key.candidate.take() {
                candidates.entry(range).or_default().push(candidate);
            }
            if key.unsampled != 0 {
                *self.sampled.entry(key.range_id).or_default() -= key.unsampled;
            }
            self.writes.append(&mut key.writes);
            self.deletes.append(&mut key.deletes);
            for (row, delta) in key.counters {
                add_counter(&mut self.counters, row, delta)?;
            }
        }
        self.sampling = (candidates.into_iter())
            .map(|((set_id, range_id), mut candidates)| {
                candidates.sort_by(|left, right| rank(left).cmp(&rank(right)));
                Sampling {
                    set_id,
                    range_id,
                    candidates,
                    size: 0,
                }
            })
            .collect();
        Ok(())
    }

    /// Reads the size of each sample new keys may enter, outside the transaction, so inserts
    /// below the size never conflict.
    fn count_samples(&mut self, txn_id: TxnId) -> Option<Effects> {
        if self.sampling.is_empty() {
            return self.next_effect(txn_id);
        }
        self.phase = Phase::SampleCount;
        let reads = (self.sampling.iter())
            .map(|at| sample_count_key(at.set_id, at.range_id))
            .map(|key| (RANGE_COUNTER_KEYSPACE.to_string(), key.into()));
        Some(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: reads.collect(),
            txn_id: None,
        })])
    }

    /// Settles the samples all candidates fit into; the others are read before.
    fn size_samples(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<(), InventoryError> {
        let queue = std::mem::take(&mut self.sampling);
        for (mut sampling, (_, value)) in queue.into_iter().zip(values) {
            let stored = value
                .map(|value| RangeCounters::from_bytes(&value))
                .transpose()?;
            let staged = self
                .sampled
                .get(&sampling.range_id)
                .copied()
                .unwrap_or_default();
            sampling.size = stored.unwrap_or_default().sampled.saturating_add(staged);
            let wanted = sampling
                .size
                .saturating_add(sampling.candidates.len() as i128);
            match wanted <= i128::from(SAMPLE_SIZE) {
                true => self.settle(sampling, Vec::new())?,
                false => self.sampling.push_back(sampling),
            }
        }
        Ok(())
    }

    /// Reads the next sample its candidates may enter: a full one is first probed outside the
    /// transaction, so keys that cannot enter never conflict on its largest entry. In the
    /// transaction, only the entries from the smallest candidate on are read.
    fn next_sample(&mut self, txn_id: TxnId, probed: bool) -> Option<Effects> {
        let Some(sampling) = self.sampling.front() else {
            return self.next_effect(txn_id);
        };
        let (set_id, range_id) = (sampling.set_id, sampling.range_id);
        let prefix = sample_prefix(set_id, range_id);
        let full = sampling.size >= i128::from(SAMPLE_SIZE);
        // Entries staged earlier in this transaction are visible only inside it.
        if !probed && full && !self.sampled.contains_key(&range_id) {
            self.phase = Phase::SampleProbe;
            return Some(smallvec![Effect::Storage(StorageEffect::Last {
                key_space: KEY_SAMPLE_KEYSPACE.to_string(),
                prefix: Some(prefix.into()),
                txn_id: None,
            })]);
        }
        let Some(first) = sampling.candidates.first() else {
            self.sampling.pop_front();
            return self.next_sample(txn_id, false);
        };
        let start = first.row_key(set_id, range_id);
        // The tag byte that ends the prefix is below 0xFF, so the next tag bounds the sample.
        let mut end = prefix;
        if let Some(tag) = end.last_mut() {
            *tag += 1;
        }
        self.phase = Phase::SampleRead;
        Some(smallvec![Effect::Storage(StorageEffect::IterRange {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            start: IterStart::At(start.into()),
            end: IterEnd::Before(end.into()),
            limit: SAMPLE_READ_ROWS,
            max_bytes: SAMPLE_READ_BYTES,
            txn_id: Some(txn_id),
        })])
    }

    /// Keeps the `SAMPLE_SIZE` smallest of the sample and its candidates: candidates among them
    /// enter, `existing` entries (those from the smallest candidate on) beyond them leave.
    fn settle(
        &mut self,
        sampling: Sampling,
        existing: Vec<SampleEntry>,
    ) -> Result<(), InventoryError> {
        let Sampling {
            set_id,
            range_id,
            candidates,
            size,
        } = sampling;
        let below = size.saturating_sub(existing.len() as i128).max(0);
        let room = (i128::from(SAMPLE_SIZE) - below).max(0);
        let old = existing.into_iter().map(|entry| (entry, false));
        let mut merged: Vec<_> = old
            .chain(candidates.into_iter().map(|entry| (entry, true)))
            .collect();
        merged.sort_by(|(left, _), (right, _)| rank(left).cmp(&rank(right)));
        let (mut entries, mut changed) = (0, false);
        for (index, (entry, new)) in merged.into_iter().enumerate() {
            let key = entry.row_key(set_id, range_id);
            match ((index as i128) < room, new) {
                (true, true) => {
                    self.writes.push(row(KEY_SAMPLE_KEYSPACE, key, vec![]));
                    entries += 1;
                }
                (false, false) => {
                    self.deletes.push((KEY_SAMPLE_KEYSPACE.into(), key.into()));
                    entries -= 1;
                }
                _ => continue,
            }
            changed = true;
        }
        if !changed {
            return Ok(());
        }
        *self.sampled.entry(range_id).or_default() += entries;
        if entries != 0 {
            let delta = RangeCounters {
                sampled: entries,
                ..RangeCounters::default()
            };
            add_counter(
                &mut self.counters,
                sample_count_key(set_id, range_id).into(),
                delta,
            )?;
        }
        Ok(())
    }

    fn next_effect(&mut self, txn_id: TxnId) -> Option<Effects> {
        if !self.writes.is_empty() && self.phase != Phase::Write {
            self.phase = Phase::Write;
            let writes = std::mem::take(&mut self.writes);
            return Some(smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            })]);
        }
        if !self.deletes.is_empty() && !matches!(self.phase, Phase::Delete | Phase::Count) {
            self.phase = Phase::Delete;
            let deletes = std::mem::take(&mut self.deletes);
            return Some(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })]);
        }
        if !self.counters.is_empty() && self.phase != Phase::Count {
            self.phase = Phase::Count;
            let deltas = std::mem::take(&mut self.counters);
            return Some(smallvec![Effect::Storage(StorageEffect::AddCounters {
                key_space: RANGE_COUNTER_KEYSPACE.to_string(),
                deltas,
                txn_id,
            })]);
        }
        self.next_round(txn_id)
    }
}

impl KeyPlan {
    fn new(change: InventoryChange, range_id: Ulid) -> Self {
        Self {
            change,
            range_id,
            writes: Vec::new(),
            deletes: Vec::new(),
            counters: Vec::new(),
            candidate: None,
            unsampled: 0,
        }
    }

    /// The key's own rows, read after the account rows of the round's ranges.
    fn reads(&self) -> Vec<(String, Key)> {
        let (set_id, key) = (self.change.set_id, &self.change.key);
        let mut reads = Vec::new();
        if self.change.head != HeadChange::Keep {
            reads.push((
                INVENTORY_KEYSPACE.to_string(),
                head_position(set_id, key).into(),
            ));
            let sample = SampleEntry::of_key(key).row_key(set_id, self.range_id);
            reads.push((KEY_SAMPLE_KEYSPACE.to_string(), sample.into()));
        }
        if let Some((upload_id, _)) = self.change.upload {
            let position = upload_position(set_id, key, upload_id);
            reads.push((UPLOAD_INVENTORY_KEYSPACE.to_string(), position.into()));
        }
        let ids = self.change.added.iter().map(|added| added.version_id);
        for version_id in ids.chain(self.change.removed.iter().copied()) {
            let index = index_key(set_id, version_id);
            reads.push((VERSION_INDEX_KEYSPACE.to_string(), index.into()));
        }
        reads
    }

    /// Turns the key's read rows, taken from `next` in [`Self::reads`] order, into writes,
    /// deletes and per-row counter deltas.
    fn plan(
        &mut self,
        account: &RangeAccount,
        next: &mut impl FnMut() -> Option<Value>,
    ) -> Result<(), InventoryError> {
        let (head, sample) = match self.change.head {
            HeadChange::Keep => (None, None),
            _ => (next(), next()),
        };
        let had_head = head.is_some();
        let mut contributions = Vec::new();
        if let Some((upload_id, opened)) = self.change.upload {
            // Opening an open upload or closing an absent one changes nothing.
            if next().is_some() != opened {
                let position = upload_position(self.change.set_id, &self.change.key, upload_id);
                let space = UPLOAD_INVENTORY_KEYSPACE.to_string();
                match opened {
                    true => self
                        .writes
                        .push((space, position.clone().into(), Value::from(vec![]))),
                    false => self.deletes.push((space, position.clone().into())),
                }
                let counters = RangeCounters::of_upload(if opened { 1 } else { -1 });
                contributions.push((Walk::Uploads, position, counters));
            }
        }
        self.plan_versions(next, &mut contributions)?;
        if let Some(entry) = &self.change.root {
            // A blind write: replays and other versions of the root rewrite the same row.
            let key = root_key(self.change.set_id, &entry.root.digest()?);
            self.writes
                .push(row(SOURCE_ROOT_KEYSPACE, key, entry.to_bytes()?));
        }
        self.plan_head(head, &mut contributions)?;
        self.plan_sample(had_head, sample.is_some());
        let routed = route(self.change.set_id, self.range_id, account, contributions)?;
        self.counters.extend(routed);
        Ok(())
    }

    fn plan_versions(
        &mut self,
        next: &mut impl FnMut() -> Option<Value>,
        contributions: &mut Vec<(Walk, Vec<u8>, RangeCounters)>,
    ) -> Result<(), InventoryError> {
        let set_id = self.change.set_id;
        for added in self.change.added.clone() {
            let mut position = version_position(
                set_id,
                &self.change.key,
                added.epoch,
                added.generation,
                added.version_id,
            );
            let mut old = None;
            if let Some(value) = next() {
                // The same version again keeps its first position; only its entry may change,
                // and never to other content.
                let existing = IndexEntry::from_bytes(&value)?;
                let decoded = Position::decode(&existing.position)?;
                let same =
                    matches!(decoded.entry, Entry::Version { epoch, .. } if epoch == added.epoch);
                if !same
                    || decoded.key != self.change.key
                    || !existing.version.same_content(&added.entry)
                {
                    return Err(InventoryError::Collision(added.version_id));
                }
                // A replay of pending content after its hash became known changes nothing.
                if added.entry.pending && existing.version.identity.is_some() {
                    continue;
                }
                if existing.version == added.entry {
                    continue;
                }
                // Deletes run after the writes, so a row the update writes again stays.
                if let Some(identity) = existing.version.identity
                    && added.entry.identity != Some(identity)
                {
                    let content = content_key(set_id, &identity, added.version_id);
                    self.deletes
                        .push((CONTENT_INDEX_KEYSPACE.into(), content.into()));
                }
                old = Some(RangeCounters::of_version(added.epoch, &existing.version));
                position = existing.position;
            }
            let mut counters = RangeCounters::of_version(added.epoch, &added.entry);
            if let Some(old) = old {
                counters = counters.merge(old.negate()?)?;
            }
            let entry = IndexEntry {
                position: position.clone(),
                version: added.entry,
            };
            let index = index_key(set_id, added.version_id);
            let value = added.entry.to_bytes()?;
            self.writes
                .push(row(INVENTORY_KEYSPACE, position.clone(), value));
            self.writes
                .push(row(VERSION_INDEX_KEYSPACE, index, entry.to_bytes()?));
            if let Some(identity) = added.entry.identity {
                let content = content_key(set_id, &identity, added.version_id);
                self.writes
                    .push(row(CONTENT_INDEX_KEYSPACE, content, Vec::new()));
            }
            contributions.push((Walk::Versions, position, counters));
        }
        for version_id in self.change.removed.clone() {
            let Some(value) = next() else {
                continue;
            };
            let entry = IndexEntry::from_bytes(&value)?;
            let Entry::Version { epoch, .. } = Position::decode(&entry.position)?.entry else {
                return Err(InventoryError::Collision(version_id));
            };
            let index = index_key(set_id, version_id);
            self.deletes
                .push((INVENTORY_KEYSPACE.into(), entry.position.clone().into()));
            self.deletes
                .push((VERSION_INDEX_KEYSPACE.into(), index.into()));
            if let Some(identity) = entry.version.identity {
                let content = content_key(set_id, &identity, version_id);
                self.deletes
                    .push((CONTENT_INDEX_KEYSPACE.into(), content.into()));
            }
            let counters = RangeCounters::of_version(epoch, &entry.version).negate()?;
            contributions.push((Walk::Versions, entry.position, counters));
        }
        Ok(())
    }

    fn plan_head(
        &mut self,
        stored: Option<Value>,
        contributions: &mut Vec<(Walk, Vec<u8>, RangeCounters)>,
    ) -> Result<(), InventoryError> {
        let old = stored
            .map(|value| HeadEntry::from_bytes(&value))
            .transpose()?;
        let new = match &self.change.head {
            HeadChange::Keep => return Ok(()),
            HeadChange::Set(head) => Some(*head),
            HeadChange::Remove => None,
        };
        if old == new {
            return Ok(());
        }
        let position = head_position(self.change.set_id, &self.change.key);
        // The search feed row carries the new head to the range's index in this transaction.
        if self.change.feed {
            let feed = feed_value(new.as_ref())?;
            self.writes
                .push(row(SEARCH_FEED_KEYSPACE, position.clone(), feed));
        }
        match new {
            Some(head) => {
                self.writes
                    .push(row(INVENTORY_KEYSPACE, position.clone(), head.to_bytes()?))
            }
            None => self
                .deletes
                .push((INVENTORY_KEYSPACE.into(), position.clone().into())),
        }
        let removed = old.map_or_else(RangeCounters::default, |old| RangeCounters::of_head(&old));
        let added = new.map_or_else(RangeCounters::default, |new| RangeCounters::of_head(&new));
        contributions.push((Walk::Versions, position, added.merge(removed.negate()?)?));
        Ok(())
    }

    /// A key that gains a head is a sample candidate; a sampled key whose head goes leaves.
    fn plan_sample(&mut self, had_head: bool, sampled: bool) {
        let entry = SampleEntry::of_key(&self.change.key);
        match (&self.change.head, had_head, sampled) {
            (HeadChange::Set(_), false, false) => self.candidate = Some(entry),
            (HeadChange::Remove, _, true) => {
                let (set_id, range_id) = (self.change.set_id, self.range_id);
                let key = entry.row_key(set_id, range_id);
                self.deletes.push((KEY_SAMPLE_KEYSPACE.into(), key.into()));
                let delta = RangeCounters {
                    sampled: -1,
                    ..RangeCounters::default()
                };
                let count = sample_count_key(set_id, range_id);
                self.counters.push((count.into(), delta));
                self.unsampled += 1;
            }
            _ => {}
        }
    }
}

/// The sample order: priority, then key.
fn rank(entry: &SampleEntry) -> (u64, &str) {
    (entry.priority, entry.key.as_str())
}

/// Whether `entry` sorts before `other` in the sample order.
fn ranks_below(entry: &SampleEntry, other: &SampleEntry) -> bool {
    rank(entry) < rank(other)
}

/// Adds `delta` to the staged delta of `row`.
fn add_counter(
    counters: &mut Vec<(Key, RangeCounters)>,
    row: Key,
    delta: RangeCounters,
) -> Result<(), CounterOverflow> {
    match counters.iter_mut().find(|(key, _)| *key == row) {
        Some((_, total)) => *total = total.merge(delta)?,
        None => counters.push((row, delta)),
    }
    Ok(())
}

/// Routes each contribution to the rows the account names: the active or pending row, and
/// the rebuild row when the contribution is behind that walk's cursor.
fn route(
    set_id: Ulid,
    range_id: Ulid,
    account: &RangeAccount,
    contributions: Vec<(Walk, Vec<u8>, RangeCounters)>,
) -> Result<Vec<(Key, RangeCounters)>, InventoryError> {
    let charged = match account.charge {
        Charge::Active(generation) => generation_key(set_id, range_id, generation),
        Charge::Child { .. } => pending_key(set_id, range_id),
        Charge::Retired { .. } => return Err(InventoryError::RangeRetired(range_id)),
    };
    let mut rows: Vec<(Vec<u8>, RangeCounters)> = Vec::new();
    let mut add = |key: Vec<u8>, delta: RangeCounters| -> Result<(), CounterOverflow> {
        match rows.iter_mut().find(|row| row.0 == key) {
            Some((_, total)) => *total = total.merge(delta)?,
            None => rows.push((key, delta)),
        }
        Ok(())
    };
    for (walk, position, delta) in contributions {
        add(charged.clone(), delta)?;
        if let Some(rebuild) = &account.rebuild {
            let cursor = match walk {
                Walk::Versions => &rebuild.versions,
                Walk::Uploads => &rebuild.uploads,
            };
            if cursor.covers(&position) {
                add(generation_key(set_id, range_id, rebuild.generation), delta)?;
            }
        }
    }
    Ok(rows
        .into_iter()
        .filter(|(_, delta)| !delta.is_zero())
        .map(|(key, delta)| (ByteView::from(key), delta))
        .collect())
}

/// Stamps the head generation and the coordinator's epoch a version is written as, inside a
/// distributed bucket only.
pub fn stamp_version(
    version: BlobVersion,
    binding: Option<&DistributionBinding>,
    generation: u64,
    epoch: OriginalEpoch,
) -> BlobVersion {
    match binding {
        Some(_) => {
            let identity = logical_identity(&version);
            version
                .with_generation(generation)
                .with_epoch(epoch)
                .with_identity(identity)
        }
        None => version,
    }
}

/// The head a coordinator installs in a distributed bucket carries the range's epoch.
pub fn stamp_head(
    pointer: CurrentVersionPointer,
    binding: Option<&DistributionBinding>,
    epoch: OriginalEpoch,
) -> CurrentVersionPointer {
    match binding {
        Some(_) => pointer.with_epoch(epoch),
        None => pointer,
    }
}

/// What a version row of `size` bytes contributes.
pub fn entry_of(version: &BlobVersion, size: u64) -> VersionEntry {
    VersionEntry {
        marker: version.is_deleted(),
        size,
        referenced: matches!(version.state, BlobVersionState::Reference { .. }),
        identity: logical_identity(version),
        pending: matches!(version.state, BlobVersionState::PendingContent { .. }),
    }
}

/// The change of one version written to `key` of a distributed bucket; with `head` the
/// version also becomes the key's head. `None` for an ordinary bucket.
pub fn version_written(
    binding: Option<&DistributionBinding>,
    key: &str,
    added: AddedVersion,
    head: bool,
) -> Option<InventoryChange> {
    let mut change = InventoryChange::new(binding?.set_id, key);
    if head {
        change.head = HeadChange::Set(HeadEntry {
            version_id: added.version_id,
            epoch: added.epoch,
            generation: added.generation,
            version: added.entry,
        });
    }
    change.added.push(added);
    Some(change)
}

/// The range index rows and fresh account rows of ranges this node has not opened yet.
pub fn open_ranges(
    set_id: Ulid,
    ranges: &[KeyRange],
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let mut writes = Vec::with_capacity(ranges.len() * 2);
    for range in ranges {
        let slot = RangeSlot {
            range_id: range.range_id,
            lower: range.lower.clone(),
        };
        let upper = range.upper.as_deref();
        writes.push(row(
            RANGE_INDEX_KEYSPACE,
            slot_key(set_id, upper),
            slot.to_bytes()?,
        ));
        let account = RangeAccount::fresh(range.lower.clone(), range.upper.clone());
        let key = account_key(set_id, range.range_id);
        writes.push(row(RANGE_ACCOUNT_KEYSPACE, key, account.to_bytes()?));
    }
    Ok(writes)
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;
