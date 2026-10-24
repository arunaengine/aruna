//! Maintains the ordered inventory, version id index, range counters and key sample of a
//! distributed bucket inside the transaction that writes the versions and the head.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CONTENT_INDEX_KEYSPACE, INVENTORY_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, RANGE_INDEX_KEYSPACE, SOURCE_ROOT_KEYSPACE, UPLOAD_INVENTORY_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
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
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Phase {
    Pending,
    Locate,
    Read { range_id: Ulid },
    SampleCount { range_id: Ulid },
    SampleProbe { range_id: Ulid },
    SampleLast { range_id: Ulid },
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

/// Embeddable update of one key's inventory rows. It reads the range index and the range's
/// account row in the host transaction, so a split or a rebuild page conflicts it.
#[derive(Clone, Debug, PartialEq)]
pub struct InventoryUpdate {
    change: InventoryChange,
    phase: Phase,
    writes: Vec<(String, Key, Value)>,
    deletes: Vec<(String, Key)>,
    counters: Vec<(Key, RangeCounters)>,
    /// A key that gained a head and may enter the sample once its size is known.
    candidate: Option<SampleEntry>,
    /// Entries earlier updates of the same transaction added to each range's sample, keyed by
    /// every range they changed; the committed rows do not show them yet.
    sampled_before: BTreeMap<Ulid, i128>,
    /// The entries this update adds to or removes from its range's sample.
    sample_change: Option<(Ulid, i128)>,
}

fn row(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> (String, Key, Value) {
    (key_space.to_string(), key.into(), value.into())
}

impl InventoryUpdate {
    pub fn new(change: InventoryChange) -> Self {
        Self {
            change,
            phase: Phase::Pending,
            writes: Vec::new(),
            deletes: Vec::new(),
            counters: Vec::new(),
            candidate: None,
            sampled_before: BTreeMap::new(),
            sample_change: None,
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == Phase::Done
    }

    pub fn change(&self) -> &InventoryChange {
        &self.change
    }

    /// Counts what earlier updates of the same transaction added to each range's sample.
    pub fn set_sampled(&mut self, sampled: BTreeMap<Ulid, i128>) {
        self.sampled_before = sampled;
    }

    /// The range whose sample this update changed and the net entries it added (or removed).
    pub fn sample_change(&self) -> Option<(Ulid, i128)> {
        self.sample_change
    }

    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        self.phase = Phase::Locate;
        let set_id = self.change.set_id;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: RANGE_INDEX_KEYSPACE.to_string(),
            prefix: Some(set_id.to_bytes().to_vec().into()),
            start: Some(IterStart::After(slot_seek(set_id, &self.change.key).into())),
            limit: 1,
            txn_id: Some(txn_id),
        })]
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
                let range_id = self.locate(&values)?;
                self.phase = Phase::Read { range_id };
                Ok(Some(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: self.reads(range_id),
                    txn_id: Some(txn_id),
                })]))
            }
            (
                Phase::Read { range_id },
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let range_id = *range_id;
                self.plan(range_id, values)?;
                if self.candidate.is_none() {
                    return Ok(self.next_effect(txn_id));
                }
                // The size is read outside the transaction, so inserts below it never conflict.
                self.phase = Phase::SampleCount { range_id };
                Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: RANGE_COUNTER_KEYSPACE.to_string(),
                    key: sample_count_key(self.change.set_id, range_id).into(),
                    txn_id: None,
                })]))
            }
            (
                Phase::SampleCount { range_id },
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                let range_id = *range_id;
                let stored = value
                    .map(|value| RangeCounters::from_bytes(&value))
                    .transpose()?;
                let pending = self
                    .sampled_before
                    .get(&range_id)
                    .copied()
                    .unwrap_or_default();
                let size = stored.unwrap_or_default().sampled.saturating_add(pending);
                if size < i128::from(SAMPLE_SIZE) {
                    self.admit(range_id, None)?;
                    return Ok(self.next_effect(txn_id));
                }
                // Entries staged earlier in this transaction are visible only inside it.
                if self.sampled_before.contains_key(&range_id) {
                    self.phase = Phase::SampleLast { range_id };
                    return Ok(Some(smallvec![self.read_largest(range_id, Some(txn_id))]));
                }
                // A full sample is probed outside the transaction first, so keys that cannot
                // enter never conflict on its largest entry.
                self.phase = Phase::SampleProbe { range_id };
                Ok(Some(smallvec![self.read_largest(range_id, None)]))
            }
            (
                Phase::SampleProbe { range_id },
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => {
                let range_id = *range_id;
                let last = values.first().and_then(|(key, _)| SampleEntry::decode(key));
                let enters = (self.candidate.as_ref())
                    .is_some_and(|entry| last.as_ref().is_none_or(|last| ranks_below(entry, last)));
                if !enters {
                    self.candidate = None;
                    return Ok(self.next_effect(txn_id));
                }
                self.phase = Phase::SampleLast { range_id };
                Ok(Some(smallvec![self.read_largest(range_id, Some(txn_id))]))
            }
            (
                Phase::SampleLast { range_id },
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => {
                let range_id = *range_id;
                let last = values.first().and_then(|(key, _)| SampleEntry::decode(key));
                match last {
                    Some(last) => self.admit(range_id, Some(last))?,
                    None => self.admit(range_id, None)?,
                }
                Ok(self.next_effect(txn_id))
            }
            (Phase::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
            | (Phase::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. }))
            | (Phase::Count, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                Ok(self.next_effect(txn_id))
            }
            (_, event) => Err(InventoryError::UnexpectedEvent(Box::new(event))),
        }
    }

    fn locate(&self, values: &[(Key, Value)]) -> Result<Ulid, InventoryError> {
        let missing = || InventoryError::RangeMissing {
            set_id: self.change.set_id,
            key: self.change.key.clone(),
        };
        let (_, value) = values.first().ok_or_else(missing)?;
        let slot = RangeSlot::from_bytes(value)?;
        match slot.lower.as_str() <= self.change.key.as_str() {
            true => Ok(slot.range_id),
            false => Err(missing()),
        }
    }

    fn reads(&self, range_id: Ulid) -> Vec<(String, Key)> {
        let (set_id, key) = (self.change.set_id, &self.change.key);
        let mut reads = vec![(
            RANGE_ACCOUNT_KEYSPACE.to_string(),
            account_key(set_id, range_id).into(),
        )];
        if self.change.head != HeadChange::Keep {
            reads.push((
                INVENTORY_KEYSPACE.to_string(),
                head_position(set_id, key).into(),
            ));
            let sample = SampleEntry::of_key(key).row_key(set_id, range_id);
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

    /// Turns the read rows into writes, deletes and per-row counter deltas.
    fn plan(
        &mut self,
        range_id: Ulid,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<(), InventoryError> {
        let mut values = values.into_iter().map(|(_, value)| value);
        let mut next = || values.next().flatten();
        let account = next().ok_or_else(|| InventoryError::RangeMissing {
            set_id: self.change.set_id,
            key: self.change.key.clone(),
        })?;
        let account = RangeAccount::from_bytes(&account)?;
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
        self.plan_versions(&mut next, &mut contributions)?;
        if let Some(entry) = &self.change.root {
            // A blind write: replays and other versions of the root rewrite the same row.
            let key = root_key(self.change.set_id, &entry.root.digest()?);
            self.writes
                .push(row(SOURCE_ROOT_KEYSPACE, key, entry.to_bytes()?));
        }
        self.plan_head(head, &mut contributions)?;
        self.plan_sample(range_id, had_head, sample.is_some());
        let routed = route(self.change.set_id, range_id, &account, contributions)?;
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
                if let Some(identity) = existing.version.identity {
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
    fn plan_sample(&mut self, range_id: Ulid, had_head: bool, sampled: bool) {
        let entry = SampleEntry::of_key(&self.change.key);
        match (&self.change.head, had_head, sampled) {
            (HeadChange::Set(_), false, false) => self.candidate = Some(entry),
            (HeadChange::Remove, _, true) => {
                let key = entry.row_key(self.change.set_id, range_id);
                self.deletes.push((KEY_SAMPLE_KEYSPACE.into(), key.into()));
                self.count_sample(range_id, -1);
            }
            _ => {}
        }
    }

    /// The sample's largest entry, read in the transaction only to validate an admission.
    fn read_largest(&self, range_id: Ulid, txn_id: Option<TxnId>) -> Effect {
        Effect::Storage(StorageEffect::Last {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: Some(sample_prefix(self.change.set_id, range_id).into()),
            txn_id,
        })
    }

    /// Takes the candidate into a sample below its size, or in place of a larger `last`.
    fn admit(&mut self, range_id: Ulid, last: Option<SampleEntry>) -> Result<(), InventoryError> {
        let Some(entry) = self.candidate.take() else {
            return Ok(());
        };
        let set_id = self.change.set_id;
        match last {
            Some(last) if !ranks_below(&entry, &last) => return Ok(()),
            Some(last) => {
                let evicted = last.row_key(set_id, range_id);
                self.deletes
                    .push((KEY_SAMPLE_KEYSPACE.into(), evicted.into()));
                self.touch_sample(range_id, 0);
            }
            None => self.count_sample(range_id, 1),
        }
        self.writes.push(row(
            KEY_SAMPLE_KEYSPACE,
            entry.row_key(set_id, range_id),
            vec![],
        ));
        Ok(())
    }

    fn count_sample(&mut self, range_id: Ulid, entries: i128) {
        let delta = RangeCounters {
            sampled: entries,
            ..RangeCounters::default()
        };
        let key = sample_count_key(self.change.set_id, range_id);
        self.counters.push((key.into(), delta));
        self.touch_sample(range_id, entries);
    }

    /// Records that this update changed the range's sample, even when its size stays.
    fn touch_sample(&mut self, range_id: Ulid, entries: i128) {
        let total = self.sample_change.map_or(0, |(_, total)| total);
        self.sample_change = Some((range_id, total.saturating_add(entries)));
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
        self.phase = Phase::Done;
        None
    }
}

/// Whether `entry` sorts before `other` in the sample order (priority, then key).
fn ranks_below(entry: &SampleEntry, other: &SampleEntry) -> bool {
    (entry.priority, entry.key.as_str()) < (other.priority, other.key.as_str())
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
