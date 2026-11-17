//! Builds the usage counter changes of storage writes and gates them on quota.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::usage::{
    BucketDelta, DIRTY_GLOBAL_KEY, NodeUsageSnapshot, USAGE_COMPLETE_KEY, USAGE_REBUILD_KEY,
    UsageCounters, UsageDelta, dirty_group_key, dirty_share_key, global_group_key,
    global_shard_key, shard_for_archive, shard_for_hash, usage_backend_key, usage_bucket_key,
    usage_group_key, usage_group_prefix, usage_node_id,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use crate::distribution::inventory::{InventoryChange, InventoryError, InventoryUpdate};

#[derive(Debug, Error, PartialEq)]
pub enum UsageUpdateError {
    #[error("usage counter update received unexpected event: {0:?}")]
    UnexpectedEvent(Event),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum UsageUpdatePhase {
    Pending,
    Adding,
    Writing,
    Inventory,
    Done,
}

/// Embeddable update of the maintained usage counters. Storage adds the deltas
/// when the host's transaction commits, so counters stay atomic with the data
/// and parallel writers of one group do not conflict on the counter rows.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageCounterUpdate {
    entries: Vec<(Vec<u8>, UsageDelta)>,
    /// The row of the bucket the change concerns, if it concerns one.
    bucket: Option<(Vec<u8>, BucketDelta)>,
    dirty_group: Option<GroupId>,
    /// The distributed bucket rows of the changed keys, maintained in the same transaction.
    inventory: Option<InventoryUpdate>,
    phase: UsageUpdatePhase,
}

/// What a write does to the version rows of its bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowChange {
    /// No row is added or removed, as when a row is replaced in place.
    Kept,
    AddedVersion,
    AddedMarker,
    RemovedVersion,
    RemovedMarker,
    /// A marker row replaced in place by a version row of the same id.
    MarkerToVersion,
    VersionToMarker,
}

impl RowChange {
    /// Writing a row, a delete marker or not, where the version id may already have one;
    /// `existing` says whether that row is a delete marker.
    pub fn written(existing: Option<bool>, marker: bool) -> Self {
        match (existing, marker) {
            (None, false) => Self::AddedVersion,
            (None, true) => Self::AddedMarker,
            (Some(true), false) => Self::MarkerToVersion,
            (Some(false), true) => Self::VersionToMarker,
            (Some(_), _) => Self::Kept,
        }
    }

    fn delta(self) -> BucketDelta {
        let (versions, delete_markers) = match self {
            Self::Kept => (0, 0),
            Self::AddedVersion => (1, 0),
            Self::AddedMarker => (0, 1),
            Self::RemovedVersion => (-1, 0),
            Self::RemovedMarker => (0, -1),
            Self::MarkerToVersion => (1, -1),
            Self::VersionToMarker => (-1, 1),
        };
        BucketDelta {
            versions,
            delete_markers,
            ..Default::default()
        }
    }
}

/// One physical copy's contribution to the stored counters, booked against one shard.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredDelta {
    pub shard: usize,
    pub backend: BackendRef,
    pub blobs: i128,
    pub bytes: i128,
}

impl StoredDelta {
    pub fn new(blake3: [u8; 32], backend: BackendRef, blobs: i128, bytes: i128) -> Self {
        Self {
            shard: shard_for_hash(&blake3),
            backend,
            blobs,
            bytes,
        }
    }

    /// A Pithos archive is booked against the shard of its archive id, a plain copy against the
    /// shard of its content hash. `None` for a plain copy without a hash.
    pub fn of_copy(location: &BackendLocation, blobs: i128, bytes: i128) -> Option<Self> {
        let shard = match &location.format.layout {
            StoredLayout::Pithos(_) => shard_for_archive(location.ulid),
            StoredLayout::Raw
            | StoredLayout::Frames(_)
            | StoredLayout::FramedPart(_)
            | StoredLayout::Plain(_) => shard_for_hash(location.get_blake3()?.try_into().ok()?),
        };
        Some(Self {
            shard,
            backend: location.backend.clone(),
            blobs,
            bytes,
        })
    }

    /// Credit for a copy a write newly created, in stored bytes. An adopted copy
    /// adds nothing, so its update never joins the counter rows to the transaction.
    pub fn for_location(location: &BackendLocation, new_blob: bool) -> Option<Self> {
        let bytes = match new_blob {
            true => i128::from(location.stored_size()),
            false => 0,
        };
        Self::of_copy(location, i128::from(new_blob), bytes)
    }

    /// The global hash shard carries both stored counters; the backend row
    /// carries bytes alone, which is all quota admission reads.
    fn entries(&self) -> Vec<(Vec<u8>, UsageDelta)> {
        if self.blobs == 0 && self.bytes == 0 {
            return Vec::new();
        }
        vec![
            (
                global_shard_key(self.shard),
                UsageDelta {
                    stored_blobs: self.blobs,
                    stored_bytes: self.bytes,
                    ..Default::default()
                },
            ),
            (
                usage_backend_key(&self.backend, self.shard),
                UsageDelta {
                    stored_bytes: self.bytes,
                    ..Default::default()
                },
            ),
        ]
    }
}

fn merge_entry(entries: &mut Vec<(Vec<u8>, UsageDelta)>, key: Vec<u8>, delta: UsageDelta) {
    match entries.iter_mut().find(|(existing, _)| *existing == key) {
        Some((_, existing)) => *existing = existing.merge(delta),
        None => entries.push((key, delta)),
    }
}

/// Books one multipart upload record of `bucket` created (`1`) or removed (`-1`) in `txn_id`;
/// storage answers with `BatchWriteResult`.
pub fn count_upload(bucket: &str, uploads: i128, txn_id: TxnId) -> Effect {
    let delta = BucketDelta {
        open_uploads: uploads,
        ..Default::default()
    };
    Effect::Storage(StorageEffect::AddUsage {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        deltas: Vec::new(),
        buckets: vec![(usage_bucket_key(bucket).into(), delta)],
        txn_id,
    })
}

impl UsageCounterUpdate {
    pub fn for_group(group_id: GroupId, delta: UsageDelta) -> Self {
        Self {
            entries: vec![
                (global_group_key(group_id), delta),
                (usage_group_key(group_id), delta),
            ],
            bucket: None,
            dirty_group: Some(group_id),
            inventory: None,
            phase: UsageUpdatePhase::Pending,
        }
    }

    /// Adds one physical copy's `stored_*` change to a group update. The copy is
    /// booked against the shard its hash owns and the row of the backend holding
    /// it, never against the writing group's shard.
    pub fn with_stored(group_id: GroupId, group_delta: UsageDelta, stored: StoredDelta) -> Self {
        let mut entries = vec![
            (global_group_key(group_id), group_delta),
            (usage_group_key(group_id), group_delta),
        ];
        for (key, delta) in stored.entries() {
            merge_entry(&mut entries, key, delta);
        }
        Self {
            entries,
            bucket: None,
            dirty_group: Some(group_id),
            inventory: None,
            phase: UsageUpdatePhase::Pending,
        }
    }

    /// Also books the group delta and the version `row` change on the row of `bucket`.
    pub fn in_bucket(mut self, bucket: &str, row: RowChange) -> Self {
        let group = self.dirty_group.map(usage_group_key);
        let delta = self
            .entries
            .iter()
            .find(|(key, _)| Some(key) == group.as_ref());
        if let Some(&(_, delta)) = delta {
            let delta = BucketDelta::from(delta).merge(row.delta());
            self.bucket = Some((usage_bucket_key(bucket), delta));
        }
        self
    }

    /// Also books `versions` added version rows on the bucket row set by [`Self::in_bucket`].
    pub fn with_versions(mut self, versions: u64) -> Self {
        if let Some((_, delta)) = self.bucket.as_mut() {
            delta.versions = delta.versions.saturating_add(i128::from(versions));
        }
        self
    }

    /// Also maintains the inventory of a distributed bucket's key; `None` for ordinary buckets.
    pub fn with_inventory(self, change: Option<InventoryChange>) -> Self {
        self.with_inventories(change)
    }

    /// Maintains the inventory of several keys of a distributed bucket, batched in rounds.
    pub fn with_inventories(mut self, changes: impl IntoIterator<Item = InventoryChange>) -> Self {
        for change in changes {
            match self.inventory.as_mut() {
                Some(inventory) => inventory.push(change),
                None => self.inventory = Some(InventoryUpdate::new(change)),
            }
        }
        self
    }

    /// The reclaim sweep's debit: physical bytes leave, no group counter moves.
    pub fn for_stored(stored: StoredDelta) -> Self {
        Self {
            entries: stored.entries(),
            bucket: None,
            dirty_group: None,
            inventory: None,
            phase: UsageUpdatePhase::Pending,
        }
    }

    /// Writes, in the counter update's transaction, the dirty markers the publisher
    /// scans. The value is a fresh generation id: the publisher clears a marker only
    /// if its stored generation still matches, so a mid-publish re-dirty keeps its retry.
    fn dirty_marker_writes(&self) -> Vec<(String, Key, Value)> {
        let generation = ByteView::from(ulid::Ulid::generate().to_bytes().to_vec());
        let mut writes = vec![(
            NODE_STATS_KEYSPACE.to_string(),
            ByteView::from(DIRTY_GLOBAL_KEY.to_vec()),
            generation.clone(),
        )];
        if let Some(group_id) = self.dirty_group {
            writes.push((
                NODE_STATS_KEYSPACE.to_string(),
                ByteView::from(dirty_group_key(group_id)),
                generation.clone(),
            ));
            // Only a removal can shrink this node's replica share; additions leave it deductible.
            if self.shrinks(group_id) {
                let key = ByteView::from(dirty_share_key(group_id));
                writes.push((NODE_STATS_KEYSPACE.to_string(), key, generation));
            }
        }
        writes
    }

    /// Whether the update lowers the group's logical bytes or removes a distributed version.
    fn shrinks(&self, group_id: GroupId) -> bool {
        let group = usage_group_key(group_id);
        let lowers =
            (self.entries.iter()).any(|(key, delta)| *key == group && delta.logical_bytes < 0);
        lowers
            || self
                .inventory
                .as_ref()
                .is_some_and(InventoryUpdate::removes)
    }

    pub fn is_noop(&self) -> bool {
        let bucket = self.bucket.as_ref().map(|(_, delta)| *delta);
        self.entries.iter().all(|(_, delta)| delta.is_zero())
            && bucket.is_none_or(|delta| delta == BucketDelta::default())
            && self.inventory.is_none()
    }

    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        self.phase = UsageUpdatePhase::Adding;
        smallvec![Effect::Storage(StorageEffect::AddUsage {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            deltas: self
                .entries
                .iter()
                .map(|(key, delta)| (key.clone().into(), *delta))
                .collect(),
            buckets: (self.bucket.iter())
                .map(|(key, delta)| (key.clone().into(), *delta))
                .collect(),
            txn_id,
        })]
    }

    /// Returns `Ok(Some(effects))` while more storage work is needed and
    /// `Ok(None)` once the update is staged in the transaction.
    pub fn step(
        &mut self,
        event: Event,
        txn_id: TxnId,
    ) -> Result<Option<Effects>, UsageUpdateError> {
        match (&self.phase, event) {
            (UsageUpdatePhase::Adding, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.phase = UsageUpdatePhase::Writing;
                Ok(Some(smallvec![Effect::Storage(
                    StorageEffect::BatchWrite {
                        writes: self.dirty_marker_writes(),
                        txn_id: Some(txn_id),
                    }
                )]))
            }
            (UsageUpdatePhase::Writing, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.phase = UsageUpdatePhase::Inventory;
                Ok(self.next_inventory(txn_id))
            }
            (UsageUpdatePhase::Inventory, event) => {
                let Some(inventory) = self.inventory.as_mut() else {
                    return Err(UsageUpdateError::UnexpectedEvent(event));
                };
                match inventory.step(event, txn_id)? {
                    Some(effects) => Ok(Some(effects)),
                    None => {
                        self.phase = UsageUpdatePhase::Done;
                        Ok(None)
                    }
                }
            }
            (_, received) => Err(UsageUpdateError::UnexpectedEvent(received)),
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == UsageUpdatePhase::Done
    }

    /// Starts the inventory update, or ends the update without one.
    fn next_inventory(&mut self, txn_id: TxnId) -> Option<Effects> {
        match self.inventory.as_mut() {
            Some(inventory) => Some(inventory.start(txn_id)),
            None => {
                self.phase = UsageUpdatePhase::Done;
                None
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QuotaGatePhase {
    Pending,
    ReadMarker,
    /// While a rebuild runs, whether complete counters exist from an earlier one.
    ReadComplete,
    ReadRealmConfig,
    ReadLocal,
    ScanRemote,
    /// Whether this node's own share may have shrunk since its last publish.
    ReadDirty,
    Done,
}

#[derive(Debug, Error, PartialEq)]
pub enum QuotaGateError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("quota gate received unexpected event: {0:?}")]
    UnexpectedEvent(Event),
}

/// Read side of the per-group quota gate: sums realm-wide `logical_bytes` in the caller's write
/// transaction, so same-group writes conflict; `ceiling` folds in grace. Replicas count only when
/// group quota settings say so. A rebuild without complete earlier counters refuses every write.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaGate {
    ceiling: u64,
    delta_logical_bytes: u64,
    group_id: GroupId,
    group_key: Vec<u8>,
    remote_prefix: Vec<u8>,
    local_node_id: NodeId,
    realm_id: Option<RealmId>,
    active_node_ids: Option<HashSet<NodeId>>,
    usage: u64,
    /// The deducted replicas' logical bytes in `usage`, from the other nodes' last snapshots.
    replica_bytes: u64,
    /// This node's own published share, deducted only while nothing shrank it since.
    own_replicas: u64,
    /// The record numbers this node's own share was computed under, by set.
    own_revisions: BTreeMap<ulid::Ulid, u64>,
    /// The other nodes' shares and the record numbers each was computed under, by set.
    remote_shares: Vec<(u64, BTreeMap<ulid::Ulid, u64>)>,
    count_replicas: bool,
    rebuilding: bool,
    phase: QuotaGatePhase,
}

impl QuotaGate {
    const SCAN_LIMIT: usize = 1_000;

    pub fn new(
        ceiling: u64,
        delta_logical_bytes: u64,
        group_id: GroupId,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            ceiling,
            delta_logical_bytes,
            group_id,
            group_key: usage_group_key(group_id),
            remote_prefix: usage_group_prefix(group_id),
            local_node_id,
            realm_id: None,
            active_node_ids: None,
            usage: 0,
            replica_bytes: 0,
            own_replicas: 0,
            own_revisions: BTreeMap::new(),
            remote_shares: Vec::new(),
            count_replicas: false,
            rebuilding: false,
            phase: QuotaGatePhase::Pending,
        }
    }

    pub fn new_for_realm(
        ceiling: u64,
        delta_logical_bytes: u64,
        group_id: GroupId,
        local_node_id: NodeId,
        realm_id: RealmId,
    ) -> Self {
        Self {
            realm_id: Some(realm_id),
            ..Self::new(ceiling, delta_logical_bytes, group_id, local_node_id)
        }
    }

    /// Reads the rebuild marker first, inside the write transaction, so a rebuild that begins
    /// or finishes meanwhile makes the write conflict and retry.
    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        self.phase = QuotaGatePhase::ReadMarker;
        smallvec![rebuild_marker_read(Some(txn_id))]
    }

    fn read_config_effect(&mut self, txn_id: TxnId) -> Effects {
        if let Some(realm_id) = self.realm_id {
            self.phase = QuotaGatePhase::ReadRealmConfig;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: DocumentTarget::RealmConfig { realm_id }.storage_key(),
                txn_id: Some(txn_id),
            })];
        }
        self.read_local_effect(txn_id)
    }

    fn read_local_effect(&mut self, txn_id: TxnId) -> Effects {
        self.phase = QuotaGatePhase::ReadLocal;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: self.group_key.clone().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn scan_effect(&self, start_after: Option<Key>, txn_id: TxnId) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(self.remote_prefix.clone().into()),
            start: start_after.map(IterStart::After),
            limit: Self::SCAN_LIMIT,
            txn_id: Some(txn_id),
        })]
    }

    /// Returns `Ok(Some(effects))` while more storage work is needed and
    /// `Ok(None)` once the group's realm-wide usage has been summed.
    pub fn step(&mut self, event: Event, txn_id: TxnId) -> Result<Option<Effects>, QuotaGateError> {
        match (&self.phase, event) {
            (
                QuotaGatePhase::ReadMarker,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if value.is_some() {
                    // A rebuild runs: the last complete counters plus their deltas still hold.
                    self.phase = QuotaGatePhase::ReadComplete;
                    return Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: USAGE_STATS_KEYSPACE.to_string(),
                        key: Key::from(USAGE_COMPLETE_KEY.to_vec()),
                        txn_id: Some(txn_id),
                    })]));
                }
                Ok(Some(self.read_config_effect(txn_id)))
            }
            (
                QuotaGatePhase::ReadComplete,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if value.is_none() {
                    self.rebuilding = true;
                    self.phase = QuotaGatePhase::Done;
                    return Ok(None);
                }
                Ok(Some(self.read_config_effect(txn_id)))
            }
            (
                QuotaGatePhase::ReadRealmConfig,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if let Some(bytes) = value {
                    let document = RealmConfigDocument::from_bytes(bytes.as_ref())?;
                    self.active_node_ids =
                        Some(document.sync_eligible_nodes()?.into_iter().collect());
                    self.count_replicas = document.replica_quota.counts(&self.group_id);
                }
                Ok(Some(self.read_local_effect(txn_id)))
            }
            (QuotaGatePhase::ReadLocal, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if let Some(bytes) = value {
                    let counters = UsageCounters::from_bytes(bytes.as_ref())?;
                    self.usage = self.usage.saturating_add(counters.logical_bytes);
                }
                self.phase = QuotaGatePhase::ScanRemote;
                Ok(Some(self.scan_effect(None, txn_id)))
            }
            (
                QuotaGatePhase::ScanRemote,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                for (key, value) in values {
                    // Live counters already include our snapshot's counters, not its replica
                    // share. Snapshots must also bind their embedded node id to the storage key.
                    let key_node_id = usage_node_id(key.as_ref());
                    let local = key_node_id == Some(self.local_node_id);
                    if !local && !counted(self.active_node_ids.as_ref(), key_node_id) {
                        continue;
                    }
                    let snapshot = NodeUsageSnapshot::from_bytes(value.as_ref())?;
                    if key_node_id != Some(snapshot.node_id) {
                        continue;
                    }
                    let replicas = snapshot.replicas.logical_bytes;
                    if local {
                        self.own_replicas = replicas;
                        self.own_revisions = snapshot.revisions;
                        continue;
                    }
                    self.usage = self.usage.saturating_add(snapshot.counters.logical_bytes);
                    self.remote_shares.push((replicas, snapshot.revisions));
                }
                if next_start_after.is_none() {
                    self.settle_shares();
                }
                match next_start_after {
                    Some(start) => Ok(Some(self.scan_effect(Some(start), txn_id))),
                    // The own share may exceed the live copies once a removal shrank it.
                    None if !self.count_replicas && self.own_replicas > 0 => {
                        self.phase = QuotaGatePhase::ReadDirty;
                        Ok(Some(smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: NODE_STATS_KEYSPACE.to_string(),
                            key: Key::from(dirty_share_key(self.group_id)),
                            txn_id: Some(txn_id),
                        })]))
                    }
                    None => {
                        self.phase = QuotaGatePhase::Done;
                        Ok(None)
                    }
                }
            }
            (QuotaGatePhase::ReadDirty, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if value.is_some() {
                    self.own_replicas = 0;
                }
                self.phase = QuotaGatePhase::Done;
                Ok(None)
            }
            (_, received) => Err(QuotaGateError::UnexpectedEvent(received)),
        }
    }

    /// Deducts only shares, this node's own included, computed under the newest record known for
    /// every set they cover: a node that has not published under it may still count copies of
    /// ranges another node now counts, so its share stays charged until it publishes again.
    fn settle_shares(&mut self) {
        let mut newest = self.own_revisions.clone();
        for (set_id, number) in self.remote_shares.iter().flat_map(|(_, numbers)| numbers) {
            let known = newest.entry(*set_id).or_default();
            *known = (*known).max(*number);
        }
        let current = |numbers: &BTreeMap<ulid::Ulid, u64>| {
            let newer = |(set_id, number)| newest.get(set_id).is_some_and(|known| known > number);
            !numbers.iter().any(newer)
        };
        if !current(&self.own_revisions) {
            self.own_replicas = 0;
        }
        self.replica_bytes = (self.remote_shares.iter())
            .filter(|(_, numbers)| current(numbers))
            .fold(0, |sum, (bytes, _)| sum.saturating_add(*bytes));
    }

    /// Projected group-wide `logical_bytes` if the pending write commits.
    pub fn projected_usage(&self) -> u64 {
        let replicas = self.replica_bytes.saturating_add(self.own_replicas);
        let usage = match self.count_replicas {
            true => self.usage,
            false => self.usage.saturating_sub(replicas),
        };
        usage.saturating_add(self.delta_logical_bytes)
    }

    /// True when committing the pending write would push the group's realm-wide
    /// `logical_bytes` past the ceiling, or while usage is being rebuilt without complete
    /// counters from an earlier rebuild. At-ceiling passes.
    pub fn is_exceeded(&self) -> bool {
        self.rebuilding || self.projected_usage() > self.ceiling
    }

    /// True when the write is refused because usage counters are being rebuilt.
    pub fn is_rebuilding(&self) -> bool {
        self.rebuilding
    }

    pub fn ceiling(&self) -> u64 {
        self.ceiling
    }
}

/// Reads the usage rebuild marker; a present row means the local counters are incomplete.
pub fn rebuild_marker_read(txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: Key::from(USAGE_REBUILD_KEY.to_vec()),
        txn_id,
    })
}

/// Debounce window for the coalesced node-usage snapshot publisher. `ShortenTimer`
/// fires this long after a burst's first dirty write, so bursts collapse into one
/// publish run with bounded latency.
pub const SNAPSHOT_PUBLISH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Schedules (or shortens toward) the debounced snapshot publish task.
pub fn schedule_snapshot_publish() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishUsageSnapshots,
        after: SNAPSHOT_PUBLISH_DEBOUNCE,
    })
}

/// Whether a snapshot of `node_id` counts: with a realm config, only its sync-eligible nodes do.
pub(crate) fn counted(active_node_ids: Option<&HashSet<NodeId>>, node_id: Option<NodeId>) -> bool {
    match (active_node_ids, node_id) {
        (Some(active_node_ids), Some(node_id)) => active_node_ids.contains(&node_id),
        _ => true,
    }
}
