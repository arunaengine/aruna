//! Tracks per-group and node-wide storage usage counters and gates writes on quota.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, BucketInfo};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::usage::{
    BucketDelta, DIRTY_GLOBAL_KEY, DIRTY_PREFIX, NodeUsageSnapshot, SUMMARY_GLOBAL_KEY,
    SUMMARY_GROUP_PREFIX, USAGE_COMPLETE_KEY, USAGE_GLOBAL_KEY, USAGE_GLOBAL_PREFIX,
    USAGE_GROUP_PREFIX, USAGE_REBUILD_KEY, UsageCounterError, UsageCounters, UsageDelta,
    dirty_group_id, dirty_group_key, dirty_share_id, dirty_share_key, global_group_key,
    global_shard_key, global_shard_keys, shard_for_archive, shard_for_hash, usage_backend_key,
    usage_bucket_key, usage_global_key, usage_group_id, usage_group_key, usage_group_prefix,
    usage_node_id, usage_snapshot_key, usage_summary_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;
use thiserror::Error;
use tracing::warn;

use crate::distribution::inventory::{InventoryChange, InventoryError, InventoryUpdate};
use crate::distribution::usage::replica_shares;
use crate::driver::{DriverContext, drive};
use crate::storage_read::scan_all;
use crate::sync::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

pub use crate::node::usage_rebuild::{
    RebuildStatsError, RebuildStatsOperation, RebuildStatsState, spawn_usage_rebuild,
};

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

/// False while a usage rebuild runs: local usage counters must not count as free capacity
/// and do not yet back quota decisions.
pub async fn usage_complete(context: &DriverContext) -> Result<bool, StorageError> {
    let Effect::Storage(read) = rebuild_marker_read(None) else {
        return Err(StorageError::InvalidEffect);
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_none()),
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        other => Err(StorageError::ReadError(format!(
            "unexpected answer {other:?}"
        ))),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LoadCountersState {
    Init,
    ReadCounter,
    ReadGlobal,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum LoadCountersError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
    #[error(
        "usage counter read received unexpected event in state {state:?}: expected {expected}, got {received:?}"
    )]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("usage counter read did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct LoadCountersOperation {
    key: Vec<u8>,
    state: LoadCountersState,
    output: Option<Result<UsageCounters, LoadCountersError>>,
}

impl LoadCountersOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            state: LoadCountersState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<UsageCounters, LoadCountersError>) -> Effects {
        self.state = if result.is_ok() {
            LoadCountersState::Finish
        } else {
            LoadCountersState::Error
        };
        self.output = Some(result);
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, received: Event) -> Effects {
        self.finish(Err(LoadCountersError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            received,
        }))
    }

    fn handle_counter_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => self.finish(UsageCounters::from_bytes(&bytes).map_err(Into::into)),
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                self.finish(Ok(UsageCounters::default()))
            }
            Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
            other => self.unexpected_event("storage read result", other),
        }
    }

    fn handle_global_read(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                self.finish(sum_global_usage(values))
            }
            Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
            other => self.unexpected_event("storage batch read result", other),
        }
    }
}

impl Operation for LoadCountersOperation {
    type Output = UsageCounters;
    type Error = LoadCountersError;

    fn start(&mut self) -> Effects {
        if self.key.as_slice() == USAGE_GLOBAL_KEY {
            self.state = LoadCountersState::ReadGlobal;
            let reads = global_shard_keys()
                .into_iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
                .collect::<Vec<_>>();
            smallvec![Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: None,
            })]
        } else {
            self.state = LoadCountersState::ReadCounter;
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: self.key.clone().into(),
                txn_id: None,
            })]
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            LoadCountersState::ReadCounter => self.handle_counter_read(event),
            LoadCountersState::ReadGlobal => self.handle_global_read(event),
            LoadCountersState::Init | LoadCountersState::Finish | LoadCountersState::Error => {
                self.unexpected_event("no event", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            LoadCountersState::Finish | LoadCountersState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(LoadCountersError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn sum_global_usage(values: Vec<(Key, Option<Value>)>) -> Result<UsageCounters, LoadCountersError> {
    let mut total = UsageCounters::default();

    for (_, value) in values {
        let Some(bytes) = value else {
            continue;
        };
        let counters = UsageCounters::from_bytes(bytes.as_ref())?;
        total.add(&counters)?;
    }

    Ok(total)
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

/// Groups/global scope touched by a publish run, so callers can refresh exactly
/// the affected realm summary cache entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishedUsage {
    pub global: bool,
    pub groups: Vec<GroupId>,
}

impl PublishedUsage {
    pub fn is_empty(&self) -> bool {
        !self.global && self.groups.is_empty()
    }

    fn targets(&self, realm_id: RealmId, node_id: NodeId) -> Vec<DocumentTarget> {
        let mut targets = Vec::with_capacity(self.groups.len() + usize::from(self.global));
        if self.global {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            });
        }
        for group_id in &self.groups {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: Some(*group_id),
            });
        }
        targets
    }
}

/// Which realm-wide counter a caller wants to read from the summed cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RealmUsageScope {
    Global,
    Group(GroupId),
}

pub(crate) async fn read_local_global(
    storage: &StorageHandle,
    txn_id: Option<TxnId>,
) -> Result<UsageCounters, String> {
    let reads = global_shard_keys()
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchRead { reads, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            let mut total = UsageCounters::default();
            for (_, value) in values {
                if let Some(bytes) = value {
                    let counters =
                        UsageCounters::from_bytes(bytes.as_ref()).map_err(|e| e.to_string())?;
                    total.add(&counters).map_err(|e| e.to_string())?;
                }
            }
            Ok(total)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected global read event: {other:?}")),
    }
}

async fn read_local_group(
    storage: &StorageHandle,
    group_id: GroupId,
    txn_id: Option<TxnId>,
) -> Result<UsageCounters, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: Key::from(usage_group_key(group_id)),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => UsageCounters::from_bytes(bytes.as_ref()).map_err(|e| e.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(UsageCounters::default())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected group read event: {other:?}")),
    }
}

/// Whether a snapshot of `node_id` counts: with a realm config, only its sync-eligible nodes do.
fn counted(active_node_ids: Option<&HashSet<NodeId>>, node_id: Option<NodeId>) -> bool {
    match (active_node_ids, node_id) {
        (Some(active_node_ids), Some(node_id)) => active_node_ids.contains(&node_id),
        _ => true,
    }
}

/// Sums remote nodes' snapshots under `prefix`, and every node's replica share, the local one
/// included. The local counters are skipped so they are never double-counted against the live
/// local counters.
async fn sum_snapshots(
    storage: &StorageHandle,
    prefix: Vec<u8>,
    local_node_id: NodeId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<(UsageCounters, UsageCounters), String> {
    let entries = scan_all(storage, NODE_STATS_KEYSPACE, Some(Key::from(prefix))).await?;
    let (mut total, mut replicas) = (UsageCounters::default(), UsageCounters::default());
    for (key, value) in entries {
        let key_node_id = usage_node_id(key.as_ref());
        let local = key_node_id == Some(local_node_id);
        if !local && !counted(active_node_ids, key_node_id) {
            continue;
        }
        let snapshot = NodeUsageSnapshot::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
        // Reject snapshots whose embedded node differs from the storage key.
        if key_node_id != Some(snapshot.node_id) {
            continue;
        }
        replicas
            .add(&snapshot.replicas)
            .map_err(|e| e.to_string())?;
        if !local {
            total.add(&snapshot.counters).map_err(|e| e.to_string())?;
        }
    }
    Ok((total, replicas))
}

/// Rebuilds this node's snapshot documents from live counters and distributes them
/// over the sync layer: dirty mode refreshes marked groups, full mode all groups plus
/// the global. Markers clear only after durable replication, never if re-dirtied.
pub async fn publish_usage_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_retaining_markers(ctx, node_id, realm_id, full).await?;
    clear_consumed_markers(&ctx.storage_handle, observed_markers).await?;
    Ok(published)
}

async fn publish_retaining_markers(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<(PublishedUsage, Vec<(Key, Value)>), String> {
    let storage = &ctx.storage_handle;

    let observed_markers = scan_all(
        storage,
        NODE_STATS_KEYSPACE,
        Some(Key::from(DIRTY_PREFIX.to_vec())),
    )
    .await?;

    let mut groups: BTreeSet<GroupId> = BTreeSet::new();
    let mut global = false;
    for (key, _) in &observed_markers {
        if key.as_ref() == DIRTY_GLOBAL_KEY {
            global = true;
        } else if let Some(group_id) = dirty_group_id(key.as_ref()) {
            groups.insert(group_id);
        } else if let Some(group_id) = dirty_share_id(key.as_ref()) {
            // A share marker the scan saw without its group's marker still needs a publish.
            groups.insert(group_id);
        }
    }

    if full {
        global = true;
        for (key, _) in scan_all(
            storage,
            USAGE_STATS_KEYSPACE,
            Some(Key::from(b"group/".to_vec())),
        )
        .await?
        {
            if let Some(rest) = key.as_ref().strip_prefix(b"group/")
                && let Ok(bytes) = <[u8; 16]>::try_from(rest)
            {
                groups.insert(GroupId::from_bytes(bytes));
            }
        }
        // Republish snapshots missing a live counter as zero so peers stop summing stale data.
        for (key, _) in scan_all(
            storage,
            NODE_STATS_KEYSPACE,
            Some(Key::from(USAGE_GROUP_PREFIX.to_vec())),
        )
        .await?
        {
            if usage_node_id(key.as_ref()) == Some(node_id)
                && let Some(group_id) = usage_group_id(key.as_ref())
            {
                groups.insert(group_id);
            }
        }
    }

    if !global && groups.is_empty() {
        return Ok((PublishedUsage::default(), Vec::new()));
    }

    // Counters and replica shares come from one snapshot, as a purge debits both together.
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: true })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("unexpected transaction answer {other:?}")),
    };
    let group_list: Vec<GroupId> = groups.into_iter().collect();
    let read = async {
        let shares = replica_shares(ctx, txn_id)
            .await
            .map_err(|e| e.to_string())?;
        let counters = if global {
            read_local_global(storage, Some(txn_id)).await?
        } else {
            UsageCounters::default()
        };
        let mut group_counters = Vec::with_capacity(group_list.len());
        for group_id in &group_list {
            group_counters.push(read_local_group(storage, *group_id, Some(txn_id)).await?);
        }
        Ok::<_, String>((shares, counters, group_counters))
    }
    .await;
    storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    let (shares, counters, group_counters) = read?;
    let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(group_list.len() + 1);
    if global {
        let (mut replicas, mut revisions) = (UsageCounters::default(), BTreeMap::new());
        for share in shares.values() {
            replicas.add(&share.replicas).map_err(|e| e.to_string())?;
            revisions.extend(&share.revisions);
        }
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters,
            replicas,
            revisions,
        };
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(usage_global_key(node_id)),
            Value::from(snapshot.to_bytes().map_err(|e| e.to_string())?),
        ));
    }
    for (group_id, counters) in group_list.iter().zip(group_counters) {
        let share = shares.get(group_id).cloned().unwrap_or_default();
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters,
            replicas: share.replicas,
            revisions: share.revisions,
        };
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(usage_snapshot_key(*group_id, node_id)),
            Value::from(snapshot.to_bytes().map_err(|e| e.to_string())?),
        ));
    }

    // Persist the refreshed snapshots but keep the dirty markers until the
    // documents have been durably handed to replication.
    write_snapshot_documents(storage, writes).await?;

    let published = PublishedUsage {
        global,
        groups: group_list,
    };
    let targets = published.targets(realm_id, node_id);
    if let Err(error) = drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: targets,
            // Steady-state publishers must not mint the shared node-usage
            // genesis; bootstrap/placement-origin paths do that authoritatively.
            allow_genesis: false,
        }),
        ctx,
    )
    .await
    .map_err(|error| format!("node usage snapshot replication failed: {error}"))
    {
        if let Err(retry_error) = signal_snapshot_retry(ctx, &published, &observed_markers).await {
            return Err(format!(
                "{error}; additionally failed to mark usage snapshot retry: {retry_error}"
            ));
        }
        return Err(error);
    }

    Ok((published, observed_markers))
}

pub async fn publish_refresh_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_retaining_markers(ctx, node_id, realm_id, full).await?;
    if !published.is_empty()
        && let Err(error) =
            recompute_usage_summary(ctx, node_id, published.global, published.groups.clone()).await
    {
        if let Err(retry_error) = signal_snapshot_retry(ctx, &published, &observed_markers).await {
            return Err(format!(
                "{error}; additionally failed to mark usage snapshot retry: {retry_error}"
            ));
        }
        return Err(error);
    }
    // Replication and summary refresh both succeeded; only now consume the
    // markers, and only those a concurrent write did not re-dirty meanwhile.
    clear_consumed_markers(&ctx.storage_handle, observed_markers).await?;
    Ok(published)
}

async fn signal_snapshot_retry(
    ctx: &DriverContext,
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
) -> Result<(), String> {
    write_retry_markers(&ctx.storage_handle, published, observed_markers).await?;

    let Some(task_handle) = ctx.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle.send_effect(schedule_snapshot_publish()).await {
        Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(TaskEvent::Error { message, .. }) => {
            warn!(message = %message, "Failed to schedule usage snapshot retry");
            Ok(())
        }
        other => {
            warn!(event = ?other, "Unexpected usage snapshot retry schedule result");
            Ok(())
        }
    }
}

async fn write_retry_markers(
    storage: &StorageHandle,
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
) -> Result<(), String> {
    if published.is_empty() {
        return Ok(());
    }

    let observed_keys: HashSet<Vec<u8>> = observed_markers
        .iter()
        .map(|(key, _)| key.as_ref().to_vec())
        .collect();
    let generation = Value::from(ulid::Ulid::generate().to_bytes().to_vec());
    let mut writes: Vec<(String, Key, Value)> =
        Vec::with_capacity(published.groups.len() + usize::from(published.global));
    if published.global && !observed_keys.contains(DIRTY_GLOBAL_KEY) {
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(DIRTY_GLOBAL_KEY.to_vec()),
            generation.clone(),
        ));
    }
    for group_id in &published.groups {
        let marker_key = dirty_group_key(*group_id);
        if observed_keys.contains(&marker_key) {
            continue;
        }
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(marker_key),
            generation.clone(),
        ));
    }
    if writes.is_empty() {
        return Ok(());
    }

    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("retry marker write failed: {other:?}")),
    }
}

/// Persists the refreshed snapshot documents. Each snapshot key has a single
/// writer (this node), so a plain batch write is enough.
async fn write_snapshot_documents(
    storage: &StorageHandle,
    writes: Vec<(String, Key, Value)>,
) -> Result<(), String> {
    if writes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("snapshot write failed: {other:?}")),
    }
}

/// Deletes each observed dirty marker only if its stored generation still matches
/// the one seen at publish start. Re-reading inside the transaction makes a racing
/// counter update abort the commit, so its retry signal is never lost.
async fn clear_consumed_markers(
    storage: &StorageHandle,
    observed: Vec<(Key, Value)>,
) -> Result<(), String> {
    if observed.is_empty() {
        return Ok(());
    }

    let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    else {
        return Err("failed to start marker cleanup transaction".to_string());
    };

    let reads = observed
        .iter()
        .map(|(key, _)| (NODE_STATS_KEYSPACE.to_string(), key.clone()))
        .collect();
    let current = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("marker re-read failed: {other:?}"));
        }
    };

    let mut deletes: Vec<(String, Key)> = Vec::with_capacity(observed.len());
    for ((key, observed_generation), (_, current_value)) in observed.iter().zip(current) {
        if current_value.as_ref() == Some(observed_generation) {
            deletes.push((NODE_STATS_KEYSPACE.to_string(), key.clone()));
        }
    }

    if deletes.is_empty() {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Ok(());
    }

    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("marker delete failed: {other:?}"));
        }
    }

    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        // A concurrent counter update re-dirtied one of the observed markers; the
        // conflicting commit aborts and every marker survives for the next run.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(()),
        other => Err(format!("marker cleanup commit failed: {other:?}")),
    }
}

async fn active_usage_nodes(ctx: &DriverContext) -> Result<Option<HashSet<NodeId>>, String> {
    let Some(net_handle) = ctx.net_handle.as_ref() else {
        return Ok(None);
    };
    let realm_id = *net_handle.realm_id();
    let key = DocumentTarget::RealmConfig { realm_id }.storage_key();
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => {
            let document = RealmConfigDocument::from_bytes(bytes.as_ref())
                .map_err(|error| error.to_string())?;
            Ok(Some(
                document
                    .sync_eligible_nodes()
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .collect(),
            ))
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected realm config read event: {other:?}")),
    }
}

async fn clear_usage_cache(ctx: &DriverContext) -> Result<(), String> {
    let mut deletes = vec![(
        NODE_STATS_KEYSPACE.to_string(),
        Key::from(SUMMARY_GLOBAL_KEY.to_vec()),
    )];
    for (key, _) in scan_all(
        &ctx.storage_handle,
        NODE_STATS_KEYSPACE,
        Some(Key::from(SUMMARY_GROUP_PREFIX.to_vec())),
    )
    .await?
    {
        deletes.push((NODE_STATS_KEYSPACE.to_string(), key));
    }
    match ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected summary cache delete event: {other:?}")),
    }
}

/// Recomputes the persisted realm-wide summed cache for the requested scopes:
/// realm total = live local counters + every remote node's snapshot.
pub async fn recompute_usage_summary(
    ctx: &DriverContext,
    local_node_id: NodeId,
    include_global: bool,
    groups: Vec<GroupId>,
) -> Result<(), String> {
    let storage = &ctx.storage_handle;
    let active_node_ids = active_usage_nodes(ctx).await?;
    let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(groups.len() + 1);

    if include_global {
        let total = realm_global_usage(storage, local_node_id, active_node_ids.as_ref()).await?;
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(SUMMARY_GLOBAL_KEY.to_vec()),
            Value::from(total.counters.to_bytes().map_err(|e| e.to_string())?),
        ));
    }
    for group_id in groups {
        let total =
            realm_group_usage(storage, local_node_id, group_id, active_node_ids.as_ref()).await?;
        writes.push((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(usage_summary_key(group_id)),
            Value::from(total.counters.to_bytes().map_err(|e| e.to_string())?),
        ));
    }

    if writes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected summary write event: {other:?}")),
    }
}

/// Refreshes the persisted realm usage sum cache for exactly the `NodeUsage` scopes
/// touched by a document-sync reconcile. Shared by all reconcile handlers so remote
/// snapshots update the realm aggregate on every path.
pub async fn refresh_usage_targets(
    ctx: &DriverContext,
    local_node_id: NodeId,
    targets: &[DocumentTarget],
) {
    let mut include_global = false;
    let mut groups = BTreeSet::new();
    let mut realm_config_changed = false;
    for target in targets {
        match target {
            DocumentTarget::NodeUsage { group_id, .. } => match group_id {
                Some(group_id) => {
                    groups.insert(*group_id);
                }
                None => include_global = true,
            },
            DocumentTarget::RealmConfig { .. } => {
                realm_config_changed = true;
            }
            _ => {}
        }
    }
    if realm_config_changed && let Err(error) = clear_usage_cache(ctx).await {
        warn!(error = %error, "Failed to clear realm usage summary after realm config change");
    }
    // Distribution records arrive as realm config targets; a new coordinator moves replica shares.
    if realm_config_changed && let Err(error) = dirty_bound_groups(ctx).await {
        warn!(error = %error, "Failed to mark distributed bucket groups for a usage publish");
    }
    if !include_global && groups.is_empty() {
        return;
    }
    if let Err(error) = recompute_usage_summary(
        ctx,
        local_node_id,
        include_global,
        groups.into_iter().collect(),
    )
    .await
    {
        warn!(error = %error, "Failed to refresh realm usage summary after document sync reconciliation");
    }
}

/// A realm-wide usage total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RealmUsage {
    /// Objects and data counted once, a distributed range at its coordinator; `stored_*` and
    /// `buckets` over every node.
    pub counters: UsageCounters,
    /// The copies of distributed ranges left out of `counters`.
    pub replicas: UsageCounters,
}

impl RealmUsage {
    fn of(
        local: UsageCounters,
        (remote, replicas): (UsageCounters, UsageCounters),
    ) -> Result<Self, String> {
        let mut total = local;
        total.add(&remote).map_err(|e| e.to_string())?;
        Ok(Self {
            counters: total.without_replicas(&replicas),
            replicas,
        })
    }
}

/// Marks the groups of local distributed buckets and the global for the next publish, which
/// recomputes their replica shares, and schedules it.
async fn dirty_bound_groups(ctx: &DriverContext) -> Result<(), String> {
    let mut groups = BTreeSet::new();
    for (_, value) in scan_all(&ctx.storage_handle, S3_BUCKET_KEYSPACE, None).await? {
        let info = BucketInfo::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
        if info.distribution.is_some() {
            groups.insert(info.group_id);
        }
    }
    if groups.is_empty() {
        return Ok(());
    }
    mark_dirty(ctx, groups).await
}

/// Rows marking `groups` and the global for the next publish, for a caller's transaction.
pub(crate) fn dirty_rows(groups: impl IntoIterator<Item = GroupId>) -> Vec<(String, Key, Value)> {
    let generation = Value::from(ulid::Ulid::generate().to_bytes().to_vec());
    let marker = |key: Vec<u8>| {
        (
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(key),
            generation.clone(),
        )
    };
    let mut rows = vec![marker(DIRTY_GLOBAL_KEY.to_vec())];
    for group in groups {
        rows.push(marker(dirty_group_key(group)));
        rows.push(marker(dirty_share_key(group)));
    }
    rows
}

/// Schedules the publish that consumes committed dirty markers.
pub(crate) async fn wake_publish(ctx: &DriverContext) {
    if let Some(task_handle) = ctx.task_handle.as_ref() {
        task_handle.send_effect(schedule_snapshot_publish()).await;
    }
}

/// Marks `groups` and the global for the next publish and schedules it.
pub(crate) async fn mark_dirty(
    ctx: &DriverContext,
    groups: impl IntoIterator<Item = GroupId>,
) -> Result<(), String> {
    let write = StorageEffect::BatchWrite {
        writes: dirty_rows(groups),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("dirty marker write failed: {other:?}")),
    }
    wake_publish(ctx).await;
    Ok(())
}

async fn realm_global_usage(
    storage: &StorageHandle,
    local_node_id: NodeId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<RealmUsage, String> {
    let local = read_local_global(storage, None).await?;
    let prefix = USAGE_GLOBAL_PREFIX.to_vec();
    let remote = sum_snapshots(storage, prefix, local_node_id, active_node_ids).await?;
    RealmUsage::of(local, remote)
}

async fn realm_group_usage(
    storage: &StorageHandle,
    local_node_id: NodeId,
    group_id: GroupId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<RealmUsage, String> {
    let local = read_local_group(storage, group_id, None).await?;
    let prefix = usage_group_prefix(group_id);
    let remote = sum_snapshots(storage, prefix, local_node_id, active_node_ids).await?;
    RealmUsage::of(local, remote)
}

/// Sums the realm-wide total from the live local counters and the remote snapshots, the
/// same rows quota admission counts. The stored sum can be older than a local write.
pub async fn load_realm_usage(
    ctx: &DriverContext,
    local_node_id: NodeId,
    scope: RealmUsageScope,
) -> Result<RealmUsage, String> {
    let storage = &ctx.storage_handle;
    let active_node_ids = active_usage_nodes(ctx).await?;
    match scope {
        RealmUsageScope::Global => {
            realm_global_usage(storage, local_node_id, active_node_ids.as_ref()).await
        }
        RealmUsageScope::Group(group_id) => {
            realm_group_usage(storage, local_node_id, group_id, active_node_ids.as_ref()).await
        }
    }
}

/// Re-arms the debounced publish task when dirty markers survived a restart.
pub async fn restore_usage_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let has_markers = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(DIRTY_PREFIX.to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => !values.is_empty(),
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan node usage dirty markers");
            return;
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning node usage dirty markers");
            return;
        }
    };
    if has_markers
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_snapshot_publish()).await
    {
        warn!(message = %message, "Failed to restore node usage publish timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::bucket::create::CreateBucketOperation;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
        UPLOAD_KEYSPACE,
    };
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::{
        BackendRef, BlobHeadKey, BucketInfo, CurrentVersionPointer,
    };
    use aruna_core::structs::storage::blob::{BlobLocationKey, BlobVersion, VersionKey};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::{FrameLayout, StoredFormat, StoredLayout};
    use aruna_core::structs::storage::multipart::MultipartUpload;
    use aruna_core::structs::storage::multipart::MultipartUploadStatus;
    use aruna_core::structs::storage::usage::global_shard_keys;
    use aruna_core::structs::storage::usage::usage_hash_key;
    use aruna_core::structs::storage::usage::{BucketCounters, global_shard_index};
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    // A rebuild refuses quota writes only while no earlier rebuild completed the counters.
    #[test]
    fn rebuild_keeps_counters() {
        let node_id = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let txn_id = Ulid::from_bytes([1; 16]);
        let marker = |value: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::ReadResult {
                key: Key::from(b"marker".to_vec()),
                value: value.map(Into::into),
            })
        };
        let mut fresh = QuotaGate::new(10, 1, Ulid::from_bytes([2; 16]), node_id);
        fresh.start(txn_id);
        assert!(
            fresh
                .step(marker(Some(Vec::new())), txn_id)
                .unwrap()
                .is_some()
        );
        assert_eq!(fresh.step(marker(None), txn_id).unwrap(), None);
        assert!(fresh.is_rebuilding() && fresh.is_exceeded());

        let mut complete = QuotaGate::new(10, 1, Ulid::from_bytes([2; 16]), node_id);
        complete.start(txn_id);
        complete.step(marker(Some(Vec::new())), txn_id).unwrap();
        let next = complete.step(marker(Some(Vec::new())), txn_id).unwrap();
        assert!(matches!(
            next.as_deref(),
            Some([Effect::Storage(StorageEffect::Read { key, .. })]) if key.as_ref() != USAGE_COMPLETE_KEY
        ));
        assert!(!complete.is_rebuilding());
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn location(blob_size: u64, staging: bool, partial: bool) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging,
            partial,
            blob_size,
            hashes: std::collections::HashMap::new(),
        }
    }

    fn hash_on_shard(shard: usize) -> [u8; 32] {
        let mut blake3 = [0u8; 32];
        blake3[0] = shard as u8;
        blake3
    }

    #[test]
    fn credit_counts_stored() {
        // Backend capacity counts what the backend holds, not the original size.
        let mut framed = location(100, false, false);
        framed.hashes.insert("blake3".to_string(), vec![2u8; 32]);
        framed.format.layout = StoredLayout::Frames(Box::new(FrameLayout {
            level: 3,
            frames: 1,
            stored_size: 40,
            index_hash: [0u8; 32],
        }));

        let delta = StoredDelta::for_location(&framed, true).unwrap();

        assert_eq!((delta.blobs, delta.bytes), (1, 40));
        let adopted = StoredDelta::for_location(&framed, false).unwrap();
        assert_eq!((adopted.blobs, adopted.bytes), (0, 0));
    }

    #[test]
    fn credit_picks_shard() {
        // Stored bytes belong to the hash's shard and the holding backend's row,
        // never to the writing group's shard.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let blake3 = hash_on_shard((global_shard_index(group_id) + 1) % 64);
        let backend = BackendRef::Node("cold".to_string());

        let update = UsageCounterUpdate::with_stored(
            group_id,
            UsageDelta {
                objects: 1,
                logical_bytes: 10,
                ..Default::default()
            },
            StoredDelta::new(blake3, backend.clone(), 1, 10),
        );

        assert_eq!(update.entries.len(), 4);
        let shard = shard_for_hash(&blake3);
        for (key, expected) in [
            (usage_hash_key(&blake3), (1, 10)),
            (usage_backend_key(&backend, shard), (0, 10)),
            (global_group_key(group_id), (0, 0)),
        ] {
            let (_, delta) = update
                .entries
                .iter()
                .find(|(entry, _)| *entry == key)
                .expect("entry present");
            assert_eq!((delta.stored_blobs, delta.stored_bytes), expected);
        }
    }

    #[test]
    fn credit_skips_group() {
        // Reclaim and rebuild account shared copies by hash.
        // The group usage row therefore remains logical-only.
        let group_id = Ulid::from_bytes([2u8; 16]);
        let blake3 = hash_on_shard((global_shard_index(group_id) + 1) % 64);

        let update = UsageCounterUpdate::with_stored(
            group_id,
            UsageDelta {
                objects: 1,
                logical_bytes: 10,
                ..Default::default()
            },
            StoredDelta::new(blake3, BackendRef::node_default(), 1, 10),
        );

        let (_, group) = update
            .entries
            .iter()
            .find(|(key, _)| *key == usage_group_key(group_id))
            .expect("group entry");
        assert_eq!(group.objects, 1);
        assert_eq!((group.stored_blobs, group.stored_bytes), (0, 0));
    }

    #[test]
    fn credit_merges_shard() {
        // One key must carry both halves when the hash shard is the group shard.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let blake3 = hash_on_shard(global_shard_index(group_id));

        let update = UsageCounterUpdate::with_stored(
            group_id,
            UsageDelta {
                objects: 1,
                logical_bytes: 10,
                ..Default::default()
            },
            StoredDelta::new(blake3, BackendRef::node_default(), 1, 10),
        );

        assert_eq!(update.entries.len(), 3);
        let (_, delta) = update
            .entries
            .iter()
            .find(|(key, _)| *key == usage_hash_key(&blake3))
            .expect("global shard entry");
        assert_eq!(delta.objects, 1);
        assert_eq!(delta.logical_bytes, 10);
        assert_eq!(delta.stored_blobs, 1);
        assert_eq!(delta.stored_bytes, 10);
    }

    #[test]
    fn adopted_credits_nothing() {
        let update = UsageCounterUpdate::for_stored(StoredDelta::new(
            [3u8; 32],
            BackendRef::node_default(),
            0,
            0,
        ));

        assert!(update.entries.is_empty());
        assert!(update.is_noop());
    }

    async fn read_optional_counters(ctx: &DriverContext, key: Vec<u8>) -> Option<UsageCounters> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => Some(UsageCounters::from_bytes(&bytes).unwrap()),
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    async fn read_counters(ctx: &DriverContext, key: Vec<u8>) -> UsageCounters {
        read_optional_counters(ctx, key).await.unwrap_or_default()
    }

    async fn read_bucket(ctx: &DriverContext, bucket: &str) -> BucketCounters {
        let read = StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: usage_bucket_key(bucket).into(),
            txn_id: None,
        };
        match ctx.storage_handle.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => BucketCounters::from_bytes(&bytes).unwrap(),
            other => panic!("expected the bucket row, got {other:?}"),
        }
    }

    async fn read_global_counters(ctx: &DriverContext) -> UsageCounters {
        let mut total = UsageCounters::default();
        for key in global_shard_keys() {
            total.add(&read_counters(ctx, key).await).unwrap();
        }
        total
    }

    async fn write_counters(ctx: &DriverContext, key: Vec<u8>, counters: UsageCounters) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: key.into(),
                value: counters.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn update_applies_deltas() {
        let group_id = Ulid::generate();
        let txn_id = Ulid::generate();
        let mut update = UsageCounterUpdate::for_group(
            group_id,
            UsageDelta {
                objects: 1,
                logical_bytes: 42,
                ..Default::default()
            },
        );

        let effects = update.start(txn_id);
        let [Effect::Storage(StorageEffect::AddUsage { deltas, .. })] = effects.as_slice() else {
            panic!("expected the counter deltas, got {effects:?}");
        };
        let expected = UsageDelta {
            objects: 1,
            logical_bytes: 42,
            ..Default::default()
        };
        assert_eq!(
            deltas,
            &vec![
                (global_group_key(group_id).into(), expected),
                (usage_group_key(group_id).into(), expected),
            ]
        );

        let effects = update
            .step(
                Event::Storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                }),
                txn_id,
            )
            .unwrap()
            .unwrap();
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the dirty marker write");
        };
        assert!(
            writes
                .iter()
                .all(|(key_space, ..)| key_space == NODE_STATS_KEYSPACE)
        );

        let done = update
            .step(
                Event::Storage(StorageEvent::BatchWriteResult {
                    entries: Vec::new(),
                }),
                txn_id,
            )
            .unwrap();
        assert!(done.is_none());
        assert!(update.is_done());
    }

    #[test]
    fn bucket_books_delta() {
        let group_id = Ulid::generate();
        let delta = UsageDelta {
            buckets: 1,
            objects: -1,
            logical_bytes: -42,
            ..Default::default()
        };
        let update = UsageCounterUpdate::for_group(group_id, delta);
        let mut update = update.in_bucket("data", RowChange::RemovedVersion);
        let effects = update.start(Ulid::generate());
        let [
            Effect::Storage(StorageEffect::AddUsage {
                deltas, buckets, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the counter deltas, got {effects:?}");
        };
        assert_eq!(deltas.len(), 2);
        let bucket = BucketDelta {
            objects: -1,
            versions: -1,
            logical_bytes: -42,
            ..Default::default()
        };
        assert_eq!(buckets, &vec![(usage_bucket_key("data").into(), bucket)]);

        // A marker alone moves no group counter, yet the update still books it.
        let update = UsageCounterUpdate::for_group(group_id, UsageDelta::default());
        assert!(update.is_noop());
        assert!(!update.in_bucket("data", RowChange::AddedMarker).is_noop());

        // The reclaim sweep's physical debit names no group, so no bucket row moves.
        let stored = StoredDelta::new([1; 32], BackendRef::node_default(), -1, -5);
        let mut update = UsageCounterUpdate::for_stored(stored).in_bucket("data", RowChange::Kept);
        let effects = update.start(Ulid::generate());
        let [Effect::Storage(StorageEffect::AddUsage { buckets, .. })] = effects.as_slice() else {
            panic!("expected the counter deltas, got {effects:?}");
        };
        assert!(buckets.is_empty());
    }

    #[test]
    fn rows_follow_replacement() {
        // A replay replaces a row of its own kind; another kind moves one count to the other.
        let cases = [
            (None, false, RowChange::AddedVersion, (1, 0)),
            (None, true, RowChange::AddedMarker, (0, 1)),
            (Some(true), false, RowChange::MarkerToVersion, (1, -1)),
            (Some(false), true, RowChange::VersionToMarker, (-1, 1)),
            (Some(true), true, RowChange::Kept, (0, 0)),
            (Some(false), false, RowChange::Kept, (0, 0)),
        ];
        let group_id = Ulid::generate();
        for (existing, marker, row, (versions, delete_markers)) in cases {
            assert_eq!(RowChange::written(existing, marker), row);
            let update = UsageCounterUpdate::for_group(group_id, UsageDelta::default());
            let mut update = update.in_bucket("data", row);
            let effects = update.start(Ulid::generate());
            let [Effect::Storage(StorageEffect::AddUsage { buckets, .. })] = effects.as_slice()
            else {
                panic!("expected the counter deltas, got {effects:?}");
            };
            let delta = BucketDelta {
                versions,
                delete_markers,
                ..Default::default()
            };
            assert_eq!(buckets, &vec![(usage_bucket_key("data").into(), delta)]);
        }
    }

    #[tokio::test]
    async fn bucket_maintains_usage() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::generate();

        drive(
            CreateBucketOperation::new(
                "counted".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
            &ctx,
        )
        .await
        .unwrap();

        let global = read_global_counters(&ctx).await;
        assert_eq!(global.buckets, 1);
        let group = read_counters(&ctx, usage_group_key(group_id)).await;
        assert_eq!(group.buckets, 1);
    }

    #[tokio::test]
    async fn load_reads_counters() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::generate();
        let group = UsageCounters {
            buckets: 1,
            objects: 2,
            logical_bytes: 3,
            ..Default::default()
        };
        write_counters(&ctx, usage_group_key(group_id), group).await;

        let loaded = drive(LoadCountersOperation::new(usage_group_key(group_id)), &ctx)
            .await
            .unwrap();
        assert_eq!(loaded, group);

        let shard_zero = UsageCounters {
            buckets: 4,
            stored_bytes: 5,
            ..Default::default()
        };
        let shard_one = UsageCounters {
            objects: 6,
            logical_bytes: 7,
            ..Default::default()
        };
        write_counters(&ctx, global_shard_key(0), shard_zero).await;
        write_counters(&ctx, global_shard_key(1), shard_one).await;

        let loaded = drive(LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()), &ctx)
            .await
            .unwrap();
        let mut expected = UsageCounters::default();
        expected.add(&shard_zero).unwrap();
        expected.add(&shard_one).unwrap();
        assert_eq!(loaded, expected);

        let stale_global_temp = tempdir().unwrap();
        let stale_global_ctx = test_ctx(stale_global_temp.path().to_str().unwrap());
        write_counters(
            &stale_global_ctx,
            USAGE_GLOBAL_KEY.to_vec(),
            UsageCounters {
                buckets: 99,
                ..Default::default()
            },
        )
        .await;
        let loaded = drive(
            LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()),
            &stale_global_ctx,
        )
        .await
        .unwrap();
        assert_eq!(loaded, UsageCounters::default());
    }

    #[tokio::test]
    async fn rebuild_counts_bytes() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_a = Ulid::generate();
        let group_b = Ulid::generate();

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("failed to start transaction");
        };

        for (bucket, group_id) in [("alpha", group_a), ("beta", group_b)] {
            let info = BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            };
            ctx.storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: bucket.as_bytes().to_vec().into(),
                    value: info.to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;
        }

        // Two completed blobs plus one staging blob that must not count.
        let live = location(100, false, false);
        let shared = location(40, false, false);
        let staged = location(7, true, false);
        let mut hashes = Vec::new();
        for (index, loc) in [&live, &shared, &staged].into_iter().enumerate() {
            let hash = [index as u8 + 1; 32];
            hashes.push(hash);
            ctx.storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: BlobLocationKey::new(hash, EncodingClass::Raw, loc.backend.clone())
                        .to_bytes()
                        .into(),
                    value: loc.to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;
        }

        // Fixtures cover a live object, deletion, reference, and two materialized versions.
        let write_version = async |bucket: &str, key: &str, version: BlobVersion| {
            let version_id = Ulid::generate();
            ctx.storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new(bucket, key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;
            version_id
        };
        let write_head = async |bucket: &str, key: &str, version_id: Ulid| {
            ctx.storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: Some(txn_id),
                })
                .await;
        };

        let now = SystemTime::now();
        let user = Default::default();
        let alpha_live_head = write_version(
            "alpha",
            "live.txt",
            BlobVersion::materialized(
                hashes[0],
                BackendRef::node_default(),
                EncodingClass::Raw,
                now,
                user,
                None,
            ),
        )
        .await;
        write_version(
            "alpha",
            "gone.txt",
            BlobVersion::materialized(
                hashes[1],
                BackendRef::node_default(),
                EncodingClass::Raw,
                now,
                user,
                None,
            ),
        )
        .await;
        let alpha_gone_head =
            write_version("alpha", "gone.txt", BlobVersion::deleted(now, user)).await;
        write_version(
            "beta",
            "shared.bin",
            BlobVersion::materialized(
                hashes[1],
                BackendRef::node_default(),
                EncodingClass::Raw,
                now,
                user,
                None,
            ),
        )
        .await;
        let beta_head = write_version(
            "beta",
            "shared.bin",
            BlobVersion::materialized(
                hashes[1],
                BackendRef::node_default(),
                EncodingClass::Raw,
                now,
                user,
                None,
            ),
        )
        .await;
        let alpha_ref_head = write_version(
            "alpha",
            "ref.txt",
            BlobVersion::reference(
                VersionSourceBinding {
                    strategy: StagingStrategy::Reference,
                    descriptor: PortableSourceDescriptor {
                        kind: SourceConnectorKind::Http,
                        public_config: Default::default(),
                        source_path: "ref.txt".to_string(),
                        version_selector: None,
                        capabilities: Vec::new(),
                        origin_node_id: None,
                        namespace: None,
                        root: None,
                    },
                    connector_id: Some(Ulid::generate()),
                    mirrors: Vec::new(),
                },
                SourceMetadata {
                    content_length: 30,
                    content_type: None,
                    etag: None,
                    last_modified: None,
                    source_version: None,
                    digests: Default::default(),
                },
                now,
                user,
                now,
            ),
        )
        .await;

        write_head("alpha", "live.txt", alpha_live_head).await;
        write_head("alpha", "gone.txt", alpha_gone_head).await;
        write_head("alpha", "ref.txt", alpha_ref_head).await;
        write_head("beta", "shared.bin", beta_head).await;
        // One upload record in alpha, being completed, and one of a bucket that is gone.
        for (bucket, status) in [
            ("alpha", MultipartUploadStatus::Completing),
            ("gone", MultipartUploadStatus::Open),
        ] {
            let upload = MultipartUpload {
                upload_id: Ulid::generate(),
                backend: BackendRef::node_default(),
                storage_class: None,
                bucket: bucket.to_string(),
                key: "part.bin".to_string(),
                group_id: group_a,
                created_by: user,
                created_at: now,
                status,
                checksum_hint: None,
                metadata: Default::default(),
                placement_policies: Vec::new(),
                subject_generation: 0,
                completing_since_ms: None,
                backend_upload: None,
                encryption: None,
            };
            ctx.storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: UPLOAD_KEYSPACE.to_string(),
                    key: upload.upload_id.to_bytes().to_vec().into(),
                    value: upload.to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;
        }

        ctx.storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let global = drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

        // live.txt, ref.txt, and shared.bin are live; gone.txt ends on a delete marker.
        assert_eq!(global.buckets, 2);
        assert_eq!(global.objects, 3);
        assert_eq!(global.stored_blobs, 2);
        assert_eq!(global.stored_bytes, 140);
        // Referenced source size is reported separately from local logical bytes.
        assert_eq!(global.logical_bytes, 220);
        assert_eq!(global.referenced_bytes, 30);

        let stored_global = read_global_counters(&ctx).await;
        assert_eq!(stored_global, global);

        let alpha = read_counters(&ctx, usage_group_key(group_a)).await;
        assert_eq!(alpha.buckets, 1);
        assert_eq!(alpha.objects, 2);
        assert_eq!(alpha.logical_bytes, 140);
        assert_eq!(alpha.referenced_bytes, 30);

        let beta = read_counters(&ctx, usage_group_key(group_b)).await;
        assert_eq!(beta.buckets, 1);
        assert_eq!(beta.objects, 1);
        assert_eq!(beta.logical_bytes, 80);
        assert_eq!(beta.referenced_bytes, 0);

        // Each bucket row holds its bucket's share and counts its rows.
        let alpha_row = BucketCounters {
            objects: 2,
            versions: 3,
            delete_markers: 1,
            logical_bytes: 140,
            referenced_bytes: 30,
            open_uploads: 1,
        };
        assert_eq!(read_bucket(&ctx, "alpha").await, alpha_row);
        let beta_row = BucketCounters {
            objects: 1,
            versions: 2,
            logical_bytes: 80,
            ..Default::default()
        };
        assert_eq!(read_bucket(&ctx, "beta").await, beta_row);
    }

    #[tokio::test]
    async fn rebuild_removes_stale() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let stale_group = Ulid::generate();
        let stale_key = usage_group_key(stale_group);

        ctx.storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        USAGE_STATS_KEYSPACE.to_string(),
                        stale_key.clone().into(),
                        UsageCounters {
                            buckets: 9,
                            ..Default::default()
                        }
                        .to_bytes()
                        .unwrap()
                        .into(),
                    ),
                    (
                        USAGE_STATS_KEYSPACE.to_string(),
                        b"global".to_vec().into(),
                        UsageCounters {
                            objects: 9,
                            ..Default::default()
                        }
                        .to_bytes()
                        .unwrap()
                        .into(),
                    ),
                ],
                txn_id: None,
            })
            .await;

        drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

        assert!(read_optional_counters(&ctx, stale_key).await.is_none());
        assert!(
            read_optional_counters(&ctx, b"global".to_vec())
                .await
                .is_none()
        );
        assert!(
            read_optional_counters(&ctx, global_shard_key(0))
                .await
                .is_some()
        );
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    async fn read_node_stat(ctx: &DriverContext, key: Vec<u8>) -> Option<Vec<u8>> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NODE_STATS_KEYSPACE.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|b| b.to_vec()),
            other => panic!("unexpected event: {other:?}"),
        }
    }

    async fn write_node_stat(ctx: &DriverContext, key: Vec<u8>, value: Vec<u8>) {
        write_stat(ctx, NODE_STATS_KEYSPACE, key, value).await;
    }

    async fn write_stat(ctx: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected event: {other:?}"),
        }
    }

    fn logical(logical_bytes: u64) -> UsageCounters {
        UsageCounters {
            logical_bytes,
            ..Default::default()
        }
    }

    /// Drives a gate of a two-node realm: `live` local logical bytes, then each node's snapshot
    /// as (counters, replicas), the local one first, and whether a removal shrank the share.
    fn run_gate(
        count_replicas: bool,
        limits: (u64, u64),
        live: u64,
        shares: [(u64, u64); 2],
        dirty: bool,
    ) -> QuotaGate {
        run_gate_at(count_replicas, limits, live, shares, dirty, [1, 1])
    }

    /// [`run_gate`] with the number of the one set's record each node's share was computed under.
    fn run_gate_at(
        count_replicas: bool,
        (ceiling, delta): (u64, u64),
        live: u64,
        shares: [(u64, u64); 2],
        dirty: bool,
        numbers: [u64; 2],
    ) -> QuotaGate {
        use aruna_core::structs::identity::realm::{RealmNode, RealmNodeKind};
        let nodes = [node(1), node(2)];
        let group_id = Ulid::from_bytes([2; 16]);
        let realm_id = RealmId([4; 32]);
        let txn_id = Ulid::from_bytes([1; 16]);
        let read = |value: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::ReadResult {
                key: Key::from(b"row".to_vec()),
                value: value.map(Into::into),
            })
        };
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.nodes = nodes
            .map(|node_id| RealmNode {
                node_id: node_id.to_string(),
                kind: RealmNodeKind::Management,
            })
            .to_vec();
        config.replica_quota.count = count_replicas;
        let mut gate = QuotaGate::new_for_realm(ceiling, delta, group_id, nodes[0], realm_id);
        gate.start(txn_id);
        gate.step(read(None), txn_id).unwrap();
        gate.step(read(Some(postcard::to_allocvec(&config).unwrap())), txn_id)
            .unwrap();
        gate.step(read(Some(logical(live).to_bytes().unwrap())), txn_id)
            .unwrap();
        let set_id = Ulid::from_bytes([3; 16]);
        let values = (nodes.into_iter().zip(shares).zip(numbers))
            .map(|((node_id, (counters, replicas)), number)| {
                let snapshot = NodeUsageSnapshot {
                    node_id,
                    counters: logical(counters),
                    replicas: logical(replicas),
                    revisions: BTreeMap::from([(set_id, number)]),
                };
                let key = Key::from(usage_snapshot_key(group_id, node_id));
                (key, Value::from(snapshot.to_bytes().unwrap()))
            })
            .collect();
        let scanned = Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        });
        if let Some(effects) = gate.step(scanned, txn_id).unwrap() {
            let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
                panic!("the gate reads one marker: {effects:?}");
            };
            assert_eq!(
                key.as_ref(),
                dirty_share_key(group_id),
                "the share marker, not the group's"
            );
            let marker = dirty.then(|| vec![1]);
            assert_eq!(gate.step(read(marker), txn_id).unwrap(), None);
        }
        gate
    }

    // Replicas count toward a quota only by the group's setting; the own snapshot adds its
    // replica share, never its counters a second time, and no share once a removal shrank it.
    #[test]
    fn gate_counts_replicas() {
        let cases = [(false, false, 61), (false, true, 101), (true, false, 151)];
        for (count_replicas, dirty, projected) in cases {
            let gate = run_gate(
                count_replicas,
                (1_000, 1),
                100,
                [(100, 40), (50, 50)],
                dirty,
            );
            assert_eq!(gate.projected_usage(), projected);
        }
    }

    // Another node owns 100 B and this node just purged its 100 B copy: until the next publish
    // its stale share must not hide the owner's bytes from a 100 B ceiling.
    #[test]
    fn stale_share_refused() {
        let gate = run_gate(false, (100, 100), 0, [(100, 100), (100, 0)], true);
        assert!(gate.is_exceeded());
        let published = run_gate(false, (100, 100), 0, [(0, 0), (100, 0)], false);
        assert!(
            published.is_exceeded(),
            "after the publish the owner's bytes still count"
        );
    }

    // After a promotion the former coordinator publishes its 100 B as replicas; the record marked
    // this node's share in its install, so the stale 100 B share is not deducted as well.
    #[test]
    fn promotion_counts_once() {
        let gate = run_gate(false, (1_000, 1), 120, [(120, 100), (100, 100)], true);
        assert_eq!(gate.projected_usage(), 121);
    }

    // The former coordinator's snapshot under the promoting record can arrive before that record:
    // a share computed under an older record of the set is then not deducted, 1 B counts as 121 B.
    #[test]
    fn newer_record_counts() {
        let shares = [(120, 100), (100, 100)];
        let early = run_gate_at(false, (1_000, 1), 120, shares, false, [1, 2]);
        assert_eq!(early.projected_usage(), 121);
        let applied = run_gate_at(false, (1_000, 1), 120, shares, false, [2, 2]);
        assert_eq!(
            applied.projected_usage(),
            21,
            "both shares under one record are exact"
        );
    }

    // The reverse on the former coordinator A: it writes under record 2 while the promoted node's
    // snapshot is still from record 1, so that stale share stays charged too: 1 B counts as 121 B.
    #[test]
    fn older_record_charged() {
        let shares = [(100, 100), (120, 100)];
        let stale = run_gate_at(false, (1_000, 1), 100, shares, false, [2, 1]);
        assert_eq!(stale.projected_usage(), 121);
        let published = run_gate_at(
            false,
            (1_000, 1),
            100,
            [(100, 100), (120, 0)],
            false,
            [2, 2],
        );
        assert_eq!(
            published.projected_usage(),
            121,
            "the republished share counts the same"
        );
    }

    // Steady additions keep a group dirty but never mark its share, so the gate keeps deducting
    // it: with 100 B owned elsewhere and a 100 B local copy, a 150 B quota admits 1 B more.
    #[test]
    fn additions_keep_share() {
        let group_id = Ulid::from_bytes([2; 16]);
        let txn_id = Ulid::from_bytes([1; 16]);
        let markers = |logical_bytes| {
            let delta = UsageDelta {
                logical_bytes,
                ..Default::default()
            };
            let mut update = UsageCounterUpdate::for_group(group_id, delta);
            update.start(txn_id);
            let added = Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            });
            let effects = update.step(added, txn_id).unwrap().unwrap();
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice()
            else {
                panic!("the update writes its markers: {effects:?}");
            };
            writes
                .iter()
                .map(|(_, key, _)| key.to_vec())
                .collect::<Vec<_>>()
        };
        let share = dirty_share_key(group_id);
        assert!(markers(100).contains(&dirty_group_key(group_id)));
        assert!(
            !markers(100).contains(&share),
            "an addition leaves the share deductible"
        );
        assert!(markers(-100).contains(&share), "a removal may shrink it");

        let gate = run_gate(false, (150, 1), 100, [(100, 100), (100, 0)], false);
        assert_eq!(gate.projected_usage(), 101);
        assert!(!gate.is_exceeded());
    }

    // Realm totals leave out the copies of distributed ranges, the own node's included, while
    // physical stored bytes add up over every node.
    #[tokio::test]
    async fn totals_count_once() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let (local, remote) = (node(1), node(2));
        let group_id = Ulid::from_bytes([5; 16]);
        let counters = |objects, logical_bytes, referenced_bytes, stored_bytes| UsageCounters {
            objects,
            logical_bytes,
            referenced_bytes,
            stored_bytes,
            ..Default::default()
        };
        write_counters(&ctx, global_shard_key(0), counters(3, 30, 9, 50)).await;
        write_counters(&ctx, usage_group_key(group_id), counters(3, 30, 9, 0)).await;
        let snapshot = |node_id, counters, replicas| {
            NodeUsageSnapshot {
                node_id,
                counters,
                replicas,
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap()
        };
        for (global, key) in [
            (true, usage_global_key(local)),
            (false, usage_snapshot_key(group_id, local)),
        ] {
            let own = counters(3, 30, 9, if global { 50 } else { 0 });
            let value = snapshot(local, own, counters(1, 10, 4, 0));
            write_node_stat(&ctx, key, value).await;
        }
        for (global, key) in [
            (true, usage_global_key(remote)),
            (false, usage_snapshot_key(group_id, remote)),
        ] {
            // The remote node holds copies only.
            let held = counters(2, 20, 6, if global { 25 } else { 0 });
            let value = snapshot(remote, held, counters(2, 20, 6, 0));
            write_node_stat(&ctx, key, value).await;
        }

        let global = load_realm_usage(&ctx, local, RealmUsageScope::Global)
            .await
            .unwrap();
        assert_eq!(global.counters, counters(2, 20, 5, 75));
        assert_eq!(global.replicas, counters(3, 30, 10, 0));
        let group = load_realm_usage(&ctx, local, RealmUsageScope::Group(group_id))
            .await
            .unwrap();
        assert_eq!(group.counters, counters(2, 20, 5, 0));
    }

    fn binding(seed: u8) -> aruna_core::structs::storage::distribution::DistributionBinding {
        aruna_core::structs::storage::distribution::DistributionBinding {
            set_id: Ulid::from_bytes([seed; 16]),
            member_id: Ulid::from_bytes([9; 16]),
            incarnation: 1,
            revision: Default::default(),
        }
    }

    async fn write_bucket(
        ctx: &DriverContext,
        bucket: &str,
        group_id: GroupId,
        distribution: Option<aruna_core::structs::storage::distribution::DistributionBinding>,
    ) {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution,
        };
        let write = StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            value: info.to_bytes().unwrap().into(),
            txn_id: None,
        };
        ctx.storage_handle.send_storage_effect(write).await;
    }

    // Distribution records arrive as realm config targets; the groups of local distributed
    // buckets are marked so their replica shares are published again.
    #[tokio::test]
    async fn record_marks_groups() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let (bound, plain) = (Ulid::from_bytes([6; 16]), Ulid::from_bytes([7; 16]));
        write_bucket(&ctx, "spire", bound, Some(binding(8))).await;
        write_bucket(&ctx, "plain", plain, None).await;

        let target = DocumentTarget::RealmConfig {
            realm_id: RealmId([4; 32]),
        };
        refresh_usage_targets(&ctx, node(1), &[target]).await;

        assert!(read_node_stat(&ctx, dirty_group_key(bound)).await.is_some());
        assert!(read_node_stat(&ctx, dirty_group_key(plain)).await.is_none());
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
    }

    // A replica share that cannot be read fails the publish, so its markers stay for the retry.
    #[tokio::test]
    async fn share_failure_keeps() {
        use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::from_bytes([6; 16]);
        write_bucket(&ctx, "spire", group_id, Some(binding(8))).await;
        let record = Ulid::from_bytes([8; 16]).to_bytes().to_vec();
        write_stat(&ctx, DISTRIBUTION_KEYSPACE, record, vec![0xff; 3]).await;
        write_node_stat(&ctx, dirty_group_key(group_id), vec![1]).await;

        let published = publish_usage_snapshots(&ctx, node(1), RealmId([4; 32]), false).await;

        assert!(published.is_err(), "an unreadable record is no zero share");
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn update_writes_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::generate();

        drive(
            CreateBucketOperation::new(
                "counted".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                    storage_routing: Vec::new(),
                    placement_policies: Vec::new(),
                    placement_policy_generation: 0,
                    compression: Compression::Off,
                    distribution: None,
                },
            ),
            &ctx,
        )
        .await
        .unwrap();

        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    // A paged marker scan can pass a group's marker before it is written and then see the
    // group's share marker: the group is published in that run, so clearing the marker is safe.
    #[tokio::test]
    async fn share_marker_publishes() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::from_bytes([5; 16]);
        write_counters(&ctx, usage_group_key(group_id), logical(7)).await;
        write_node_stat(&ctx, dirty_share_key(group_id), vec![1]).await;

        let published = publish_usage_snapshots(&ctx, node(1), RealmId([4; 32]), false)
            .await
            .unwrap();

        assert_eq!(published.groups, vec![group_id]);
        let snapshot = read_node_stat(&ctx, usage_snapshot_key(group_id, node(1))).await;
        let snapshot = NodeUsageSnapshot::from_bytes(&snapshot.unwrap()).unwrap();
        assert_eq!(snapshot.counters.logical_bytes, 7);
        assert!(
            read_node_stat(&ctx, dirty_share_key(group_id))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_consumes_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();

        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), Vec::new()).await;
        write_node_stat(&ctx, dirty_group_key(group_id), Vec::new()).await;

        let published = publish_usage_snapshots(&ctx, node_id, realm_id, false)
            .await
            .unwrap();
        assert!(published.global);
        assert_eq!(published.groups, vec![group_id]);

        // Markers were consumed.
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_none()
        );

        // Snapshot documents were written under this node's keys.
        let global = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, usage_global_key(node_id))
                .await
                .expect("global snapshot written"),
        )
        .unwrap();
        assert_eq!(global.node_id, node_id);
        assert_eq!(global.counters.logical_bytes, 10);
        let group = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
                .await
                .expect("group snapshot written"),
        )
        .unwrap();
        assert_eq!(group.counters.logical_bytes, 10);
    }

    #[tokio::test]
    async fn summary_excludes_own() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);
        let group_id = Ulid::generate();

        let local_counters = UsageCounters {
            logical_bytes: 10,
            objects: 1,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), local_counters).await;
        write_counters(&ctx, usage_group_key(group_id), local_counters).await;

        let remote_snapshot = |counters| NodeUsageSnapshot {
            node_id: remote,
            counters,
            replicas: Default::default(),
            revisions: Default::default(),
        };
        let remote_counters = UsageCounters {
            logical_bytes: 5,
            objects: 1,
            ..Default::default()
        };
        write_node_stat(
            &ctx,
            usage_global_key(remote),
            remote_snapshot(remote_counters).to_bytes().unwrap(),
        )
        .await;
        write_node_stat(
            &ctx,
            usage_snapshot_key(group_id, remote),
            remote_snapshot(remote_counters).to_bytes().unwrap(),
        )
        .await;
        // This node's own stale snapshot must be ignored in favor of live counters.
        write_node_stat(
            &ctx,
            usage_global_key(local),
            NodeUsageSnapshot {
                node_id: local,
                counters: UsageCounters {
                    logical_bytes: 999,
                    ..Default::default()
                },
                replicas: Default::default(),
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        recompute_usage_summary(&ctx, local, true, vec![group_id])
            .await
            .unwrap();

        let global = load_realm_usage(&ctx, local, RealmUsageScope::Global)
            .await
            .unwrap()
            .counters;
        assert_eq!(global.logical_bytes, 15);
        assert_eq!(global.objects, 2);
        let group = load_realm_usage(&ctx, local, RealmUsageScope::Group(group_id))
            .await
            .unwrap()
            .counters;
        assert_eq!(group.logical_bytes, 15);
        assert_eq!(group.objects, 2);
    }

    #[tokio::test]
    async fn load_recomputes_usage() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);

        write_counters(
            &ctx,
            global_shard_key(0),
            UsageCounters {
                logical_bytes: 3,
                ..Default::default()
            },
        )
        .await;
        write_node_stat(
            &ctx,
            usage_global_key(remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: UsageCounters {
                    logical_bytes: 4,
                    ..Default::default()
                },
                replicas: Default::default(),
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        // No summary/global cache entry exists, so the read recomputes on the fly.
        let global = load_realm_usage(&ctx, local, RealmUsageScope::Global)
            .await
            .unwrap()
            .counters;
        assert_eq!(global.logical_bytes, 7);
    }

    #[tokio::test]
    async fn rebuild_preserves_snapshots() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let remote = node(2);
        let snapshot_key = usage_global_key(remote);
        let snapshot = NodeUsageSnapshot {
            node_id: remote,
            counters: UsageCounters {
                logical_bytes: 42,
                ..Default::default()
            },
            replicas: Default::default(),
            revisions: Default::default(),
        };
        write_node_stat(&ctx, snapshot_key.clone(), snapshot.to_bytes().unwrap()).await;
        write_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec(), Vec::new()).await;

        drive(RebuildStatsOperation::new(), &ctx).await.unwrap();

        assert_eq!(
            read_node_stat(&ctx, snapshot_key).await,
            Some(snapshot.to_bytes().unwrap())
        );
        assert!(
            read_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn clear_keeps_regenerated() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let generation_one = Ulid::generate().to_bytes().to_vec();

        write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation_one.clone()).await;
        let observed = vec![(
            Key::from(DIRTY_GLOBAL_KEY.to_vec()),
            Value::from(generation_one),
        )];

        // A concurrent counter update re-dirties the marker with a new generation
        // after it was observed but before it is cleared.
        let generation_two = Ulid::generate().to_bytes().to_vec();
        write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation_two.clone()).await;

        clear_consumed_markers(&ctx.storage_handle, observed)
            .await
            .unwrap();

        // The re-dirtied marker survives, keeping the retry signal for the racing
        // write's counter change.
        assert_eq!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec()).await,
            Some(generation_two.clone())
        );

        // Observing the current generation lets the marker be consumed.
        let observed = vec![(
            Key::from(DIRTY_GLOBAL_KEY.to_vec()),
            Value::from(generation_two),
        )];
        clear_consumed_markers(&ctx.storage_handle, observed)
            .await
            .unwrap();
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn failure_preserves_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();
        let generation = Ulid::generate().to_bytes().to_vec();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation.clone()).await;
        write_node_stat(&ctx, dirty_group_key(group_id), generation).await;

        // Corrupt the realm config document so replication fails hard.
        let config_key = DocumentTarget::RealmConfig { realm_id }.storage_key();
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: config_key,
                value: Value::from(b"corrupt".to_vec()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let result = publish_usage_snapshots(&ctx, node_id, realm_id, false).await;
        assert!(
            result.is_err(),
            "replication failure must surface as an error, got {result:?}"
        );

        // Markers survive so the debounced publisher retries the run.
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn replication_failure_redirties() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_none()
        );

        let config_key = DocumentTarget::RealmConfig { realm_id }.storage_key();
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: config_key,
                value: Value::from(b"corrupt".to_vec()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write event: {other:?}"),
        }

        let result = publish_refresh_snapshots(&ctx, node_id, realm_id, true).await;
        assert!(
            result.is_err(),
            "replication failure must surface as an error, got {result:?}"
        );

        assert!(
            read_node_stat(&ctx, usage_global_key(node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn summary_failure_preserves() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let remote = node(2);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();
        let generation = Ulid::generate().to_bytes().to_vec();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec(), generation.clone()).await;
        write_node_stat(&ctx, dirty_group_key(group_id), generation.clone()).await;
        write_node_stat(&ctx, usage_global_key(remote), b"corrupt".to_vec()).await;

        let result = publish_refresh_snapshots(&ctx, node_id, realm_id, false).await;
        assert!(
            result.is_err(),
            "summary recompute failure must surface as an error, got {result:?}"
        );

        // The snapshots were published, but the markers survive so the debounced
        // publisher retries and refreshes the realm summary cache later.
        assert!(
            read_node_stat(&ctx, usage_global_key(node_id))
                .await
                .is_some()
        );
        assert_eq!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec()).await,
            Some(generation.clone())
        );
        assert_eq!(
            read_node_stat(&ctx, dirty_group_key(group_id)).await,
            Some(generation)
        );
    }

    #[tokio::test]
    async fn summary_failure_redirties() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let remote = node(2);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::generate();

        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, usage_global_key(remote), b"corrupt".to_vec()).await;

        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_none()
        );

        let result = publish_refresh_snapshots(&ctx, node_id, realm_id, true).await;
        assert!(
            result.is_err(),
            "summary recompute failure must surface as an error, got {result:?}"
        );

        // Full startup may publish without markers. A later summary failure must mark every
        // published scope so the debounced publisher retries.
        assert!(
            read_node_stat(&ctx, usage_global_key(node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, usage_snapshot_key(group_id, node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_zeros_stale() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let live_group = Ulid::generate();
        let stale_group = Ulid::generate();

        // A live group still has a counter key.
        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, usage_group_key(live_group), counters).await;

        // Removing a previously published group's counter leaves a stale snapshot.
        write_node_stat(
            &ctx,
            usage_snapshot_key(stale_group, node_id),
            NodeUsageSnapshot {
                node_id,
                counters: UsageCounters {
                    buckets: 5,
                    logical_bytes: 500,
                    ..Default::default()
                },
                replicas: Default::default(),
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        let published = publish_usage_snapshots(&ctx, node_id, realm_id, true)
            .await
            .unwrap();
        assert!(published.groups.contains(&stale_group));
        assert!(published.groups.contains(&live_group));

        // The stale snapshot is overwritten with a zero total.
        let stale = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, usage_snapshot_key(stale_group, node_id))
                .await
                .expect("stale snapshot present"),
        )
        .unwrap();
        assert_eq!(stale.counters, UsageCounters::default());

        // The live group keeps its real total.
        let live = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, usage_snapshot_key(live_group, node_id))
                .await
                .expect("live snapshot present"),
        )
        .unwrap();
        assert_eq!(live.counters.logical_bytes, 10);
    }

    #[tokio::test]
    async fn refresh_recomputes_targets() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);
        let group_id = Ulid::generate();

        let local_counters = UsageCounters {
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, global_group_key(group_id), local_counters).await;
        write_counters(&ctx, usage_group_key(group_id), local_counters).await;

        let remote_counters = UsageCounters {
            logical_bytes: 5,
            ..Default::default()
        };
        write_node_stat(
            &ctx,
            usage_global_key(remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: remote_counters,
                replicas: Default::default(),
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        write_node_stat(
            &ctx,
            usage_snapshot_key(group_id, remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: remote_counters,
                replicas: Default::default(),
                revisions: Default::default(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        refresh_usage_targets(
            &ctx,
            local,
            &[
                DocumentTarget::NodeUsage {
                    realm_id: RealmId::from_bytes([9u8; 32]),
                    node_id: remote,
                    group_id: None,
                },
                DocumentTarget::NodeUsage {
                    realm_id: RealmId::from_bytes([9u8; 32]),
                    node_id: remote,
                    group_id: Some(group_id),
                },
            ],
        )
        .await;

        // The summed cache was materialized for both touched scopes.
        assert_eq!(
            read_node_stat(&ctx, SUMMARY_GLOBAL_KEY.to_vec())
                .await
                .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
            Some(15)
        );
        assert_eq!(
            read_node_stat(&ctx, usage_summary_key(group_id))
                .await
                .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
            Some(15)
        );
    }

    // A state that expects no event must reject one instead of ignoring it.
    #[test]
    fn terminal_rejects_event() {
        let stray = || {
            Event::Storage(StorageEvent::TransactionCommitted {
                txn_id: TxnId::generate(),
            })
        };

        let mut load = LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec());
        load.step(stray());
        assert!(matches!(
            load.finalize(),
            Err(LoadCountersError::UnexpectedEvent { .. })
        ));

        let mut rebuild = RebuildStatsOperation::new();
        rebuild.start();
        rebuild.step(stray());
        rebuild.step(stray());
        assert!(matches!(
            rebuild.finalize(),
            Err(RebuildStatsError::InvalidStateEvent {
                state: RebuildStatsState::Error,
                ..
            })
        ));
    }
}
