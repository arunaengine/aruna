//! Tracks per-group and node-wide storage usage counters and gates writes on quota.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::usage::{
    BucketDelta, DIRTY_GLOBAL_KEY, NodeUsageSnapshot, USAGE_COMPLETE_KEY, USAGE_GLOBAL_KEY,
    USAGE_REBUILD_KEY, UsageCounterError, UsageCounters, UsageDelta, dirty_group_key,
    dirty_share_key, global_group_key, global_shard_key, global_shard_keys, shard_for_archive,
    shard_for_hash, usage_backend_key, usage_bucket_key, usage_group_key, usage_group_prefix,
    usage_node_id,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use byteview::ByteView;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use thiserror::Error;

use crate::distribution::inventory::{InventoryChange, InventoryError, InventoryUpdate};
use aruna_core::rows::StagedCall;

#[derive(Debug, Error, PartialEq)]
pub enum UsageUpdateError {
    #[error("usage counter update received unexpected event: {0}")]
    UnexpectedEvent(&'static str),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
    #[error("usage counter update failed: {0}")]
    Io(IoError),
}

impl From<IoError> for UsageUpdateError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
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
    /// The id the dirty markers carry, given by the host when the update starts.
    generation: ulid::Ulid,
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

/// Counts `uploads` more or fewer open uploads of `bucket` when the transaction commits.
pub fn upload_count(bucket: &str, uploads: i128) -> IoCall {
    let delta = BucketDelta {
        open_uploads: uploads,
        ..Default::default()
    };
    IoCall::AddUsage {
        space: USAGE_STATS_KEYSPACE.to_string(),
        deltas: Vec::new(),
        buckets: vec![(usage_bucket_key(bucket).into(), delta)],
    }
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
            generation: ulid::Ulid::nil(),
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
            generation: ulid::Ulid::nil(),
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
            generation: ulid::Ulid::nil(),
        }
    }

    /// Writes, in the counter update's transaction, the dirty markers the publisher
    /// scans. The value is the fresh generation id: the publisher clears a marker only
    /// if its stored generation still matches, so a mid-publish re-dirty keeps its retry.
    fn dirty_marker_writes(&self) -> Vec<(String, Key, Value)> {
        let generation = ByteView::from(self.generation.to_bytes().to_vec());
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

    /// The first call: the counter additions, which apply when the host's transaction commits.
    /// `generation` is a fresh id of the host's run, which the dirty markers carry.
    pub fn start_calls(&mut self, generation: ulid::Ulid) -> StagedCall {
        self.generation = generation;
        self.phase = UsageUpdatePhase::Adding;
        StagedCall::InTransaction(IoCall::AddUsage {
            space: USAGE_STATS_KEYSPACE.to_string(),
            deltas: (self.entries.iter())
                .map(|(key, delta)| (key.clone().into(), *delta))
                .collect(),
            buckets: (self.bucket.iter())
                .map(|(key, delta)| (key.clone().into(), *delta))
                .collect(),
        })
    }

    /// The next call after `result`, or `None` once the update is staged in the transaction.
    pub fn step_calls(&mut self, result: IoResult) -> Result<Option<StagedCall>, UsageUpdateError> {
        match self.phase {
            UsageUpdatePhase::Adding => {
                result.done()?;
                self.phase = UsageUpdatePhase::Writing;
                let writes = IoCall::Write(self.dirty_marker_writes());
                Ok(Some(StagedCall::InTransaction(writes)))
            }
            UsageUpdatePhase::Writing => {
                result.done()?;
                self.phase = UsageUpdatePhase::Inventory;
                Ok(self.inventory_start())
            }
            UsageUpdatePhase::Inventory => {
                let inventory = self.inventory.as_mut().ok_or(IoError::Unfinished)?;
                match inventory.step_calls(result)? {
                    Some(call) => Ok(Some(call)),
                    None => {
                        self.phase = UsageUpdatePhase::Done;
                        Ok(None)
                    }
                }
            }
            UsageUpdatePhase::Pending | UsageUpdatePhase::Done => {
                Err(UsageUpdateError::UnexpectedEvent(result.label()))
            }
        }
    }

    /// Starts the inventory update, or ends the update without one.
    fn inventory_start(&mut self) -> Option<StagedCall> {
        let call = self
            .inventory
            .as_mut()
            .and_then(InventoryUpdate::start_calls);
        if call.is_none() {
            self.phase = UsageUpdatePhase::Done;
        }
        call
    }

    pub fn is_done(&self) -> bool {
        self.phase == UsageUpdatePhase::Done
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
    #[error("quota gate received unexpected event: {0}")]
    UnexpectedEvent(&'static str),
    #[error("quota gate read failed: {0}")]
    Io(IoError),
}

impl From<IoError> for QuotaGateError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
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
    /// or finishes meanwhile makes the write conflict and retry. Every call of the gate runs in
    /// the host's transaction.
    pub fn start_calls(&mut self) -> IoCall {
        self.phase = QuotaGatePhase::ReadMarker;
        read_row(USAGE_STATS_KEYSPACE, USAGE_REBUILD_KEY.to_vec())
    }

    fn read_config_call(&mut self) -> IoCall {
        if let Some(realm_id) = self.realm_id {
            self.phase = QuotaGatePhase::ReadRealmConfig;
            let key = DocumentTarget::RealmConfig { realm_id }.storage_key();
            return read_row(REALM_CONFIG_KEYSPACE, key);
        }
        self.read_local_call()
    }

    fn read_local_call(&mut self) -> IoCall {
        self.phase = QuotaGatePhase::ReadLocal;
        read_row(USAGE_STATS_KEYSPACE, self.group_key.clone())
    }

    fn scan_call(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: NODE_STATS_KEYSPACE.to_string(),
            prefix: self.remote_prefix.clone().into(),
            after,
            limit: Self::SCAN_LIMIT,
        }
    }

    /// The next call after `result`, or `None` once the group's realm-wide usage is summed.
    pub fn step_calls(&mut self, result: IoResult) -> Result<Option<IoCall>, QuotaGateError> {
        match self.phase {
            QuotaGatePhase::ReadMarker => {
                if result.value()?.is_some() {
                    // A rebuild runs: the last complete counters plus their deltas still hold.
                    self.phase = QuotaGatePhase::ReadComplete;
                    let key = USAGE_COMPLETE_KEY.to_vec();
                    return Ok(Some(read_row(USAGE_STATS_KEYSPACE, key)));
                }
                Ok(Some(self.read_config_call()))
            }
            QuotaGatePhase::ReadComplete => {
                if result.value()?.is_none() {
                    self.rebuilding = true;
                    self.phase = QuotaGatePhase::Done;
                    return Ok(None);
                }
                Ok(Some(self.read_config_call()))
            }
            QuotaGatePhase::ReadRealmConfig => {
                if let Some(bytes) = result.value()? {
                    let document = RealmConfigDocument::from_bytes(bytes.as_ref())?;
                    self.active_node_ids =
                        Some(document.sync_eligible_nodes()?.into_iter().collect());
                    self.count_replicas = document.replica_quota.counts(&self.group_id);
                }
                Ok(Some(self.read_local_call()))
            }
            QuotaGatePhase::ReadLocal => {
                if let Some(bytes) = result.value()? {
                    let counters = UsageCounters::from_bytes(bytes.as_ref())?;
                    self.usage = self.usage.saturating_add(counters.logical_bytes);
                }
                self.phase = QuotaGatePhase::ScanRemote;
                Ok(Some(self.scan_call(None)))
            }
            QuotaGatePhase::ScanRemote => {
                let (values, next_start_after) = result.page()?;
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
                    Some(start) => Ok(Some(self.scan_call(Some(start)))),
                    // The own share may exceed the live copies once a removal shrank it.
                    None if !self.count_replicas && self.own_replicas > 0 => {
                        self.phase = QuotaGatePhase::ReadDirty;
                        let key = dirty_share_key(self.group_id);
                        Ok(Some(read_row(NODE_STATS_KEYSPACE, key)))
                    }
                    None => {
                        self.phase = QuotaGatePhase::Done;
                        Ok(None)
                    }
                }
            }
            QuotaGatePhase::ReadDirty => {
                if result.value()?.is_some() {
                    self.own_replicas = 0;
                }
                self.phase = QuotaGatePhase::Done;
                Ok(None)
            }
            QuotaGatePhase::Pending | QuotaGatePhase::Done => {
                Err(QuotaGateError::UnexpectedEvent(result.label()))
            }
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

/// Reads whether the local usage counters are complete: false while a usage rebuild runs,
/// when they must not count as free capacity and do not yet back quota decisions.
#[derive(Debug, Default, PartialEq)]
pub struct UsageCompleteOperation;

impl UsageCompleteOperation {
    pub fn new() -> Self {
        Self
    }
}

impl Operation for UsageCompleteOperation {
    type Output = bool;
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<bool>, StorageError> {
        // A present row means the local counters are incomplete.
        let read = read_row(USAGE_STATS_KEYSPACE, USAGE_REBUILD_KEY.to_vec());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, StorageError> {
        Ok(Next::Done(result.value()?.is_none()))
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum LoadCountersError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
}

impl From<IoError> for LoadCountersError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

#[derive(Debug, PartialEq)]
pub struct LoadCountersOperation {
    key: Vec<u8>,
}

impl LoadCountersOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }

    fn is_global(&self) -> bool {
        self.key.as_slice() == USAGE_GLOBAL_KEY
    }
}

impl Operation for LoadCountersOperation {
    type Output = UsageCounters;
    type Error = LoadCountersError;

    fn first(&mut self) -> Result<Next<UsageCounters>, LoadCountersError> {
        Ok(Next::Call(match self.is_global() {
            true => IoCall::Read(global_shard_rows()),
            false => read_row(USAGE_STATS_KEYSPACE, self.key.clone()),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UsageCounters>, LoadCountersError> {
        if self.is_global() {
            let total = global_sum::<LoadCountersError>(result.values()?)?;
            return Ok(Next::Done(total));
        }
        let counters = match result.value()? {
            Some(bytes) => UsageCounters::from_bytes(&bytes)?,
            None => UsageCounters::default(),
        };
        Ok(Next::Done(counters))
    }
}

/// Whether a snapshot of `node_id` counts: with a realm config, only its sync-eligible nodes do.
pub fn counted(active_node_ids: Option<&HashSet<NodeId>>, node_id: Option<NodeId>) -> bool {
    match (active_node_ids, node_id) {
        (Some(active_node_ids), Some(node_id)) => active_node_ids.contains(&node_id),
        _ => true,
    }
}

/// The rows of the node-wide counter shards.
pub fn global_shard_rows() -> Vec<(KeySpace, Key)> {
    (global_shard_keys().into_iter())
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
        .collect()
}

/// The node-wide counters: the sum of the rows [`global_shard_rows`] names.
pub fn global_sum<E>(values: impl IntoIterator<Item = Option<Value>>) -> Result<UsageCounters, E>
where
    E: From<ConversionError> + From<UsageCounterError>,
{
    let mut total = UsageCounters::default();
    for bytes in values.into_iter().flatten() {
        total.add(&UsageCounters::from_bytes(bytes.as_ref())?)?;
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

/// Rows marking `groups` and the global for the next publish under `generation`, for a
/// caller's transaction. A publish clears a marker only while it keeps that generation.
pub fn dirty_rows(
    groups: impl IntoIterator<Item = GroupId>,
    generation: ulid::Ulid,
) -> Vec<(String, Key, Value)> {
    let generation = Value::from(generation.to_bytes().to_vec());
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

#[derive(Debug, Error, PartialEq)]
pub enum MarkDirtyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected event while marking usage for a publish: {0}")]
    UnexpectedEvent(&'static str),
}

impl From<IoError> for MarkDirtyError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Marks groups and the global for the next usage publish, then schedules that publish; the
/// wake form only schedules the publish that consumes markers a caller already committed.
/// Scheduling is best effort: a refused timer still ends the run.
#[derive(Debug, PartialEq)]
pub struct MarkDirtyOperation {
    writes: Option<Vec<(String, Key, Value)>>,
    marking: bool,
}

impl MarkDirtyOperation {
    pub fn new(groups: impl IntoIterator<Item = GroupId>, generation: ulid::Ulid) -> Self {
        Self {
            writes: Some(dirty_rows(groups, generation)),
            marking: false,
        }
    }

    pub fn wake() -> Self {
        Self {
            writes: None,
            marking: false,
        }
    }
}

impl Operation for MarkDirtyOperation {
    type Output = ();
    type Error = MarkDirtyError;

    fn first(&mut self) -> Result<Next<()>, MarkDirtyError> {
        let Some(writes) = self.writes.take() else {
            return Ok(Next::Call(schedule_snapshot_publish().into()));
        };
        self.marking = true;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, MarkDirtyError> {
        if std::mem::take(&mut self.marking) {
            result.done()?;
            return Ok(Next::Call(schedule_snapshot_publish().into()));
        }
        match result.event()? {
            Event::Task(_) => Ok(Next::Done(())),
            other => Err(MarkDirtyError::UnexpectedEvent(other.label())),
        }
    }
}

#[cfg(test)]
#[path = "counter_tests.rs"]
mod tests;

#[cfg(test)]
mod call_tests {
    use aruna_core::task::TaskEvent;

    use super::*;

    fn task(event: TaskEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Task(event)))
    }

    fn publish_call() -> Next<()> {
        Next::Call(schedule_snapshot_publish().into())
    }

    #[test]
    fn marker_marks_incomplete() {
        let marker_read = || read_row(USAGE_STATS_KEYSPACE, USAGE_REBUILD_KEY.to_vec());
        for (value, complete) in [(None, true), (Some(Value::from(vec![1])), false)] {
            let mut operation = UsageCompleteOperation::new();
            assert_eq!(operation.first(), Ok(Next::Call(marker_read())));
            let answer = IoResult::Values(vec![value]);
            assert_eq!(operation.next(answer), Ok(Next::Done(complete)));
        }
        let mut wrong = UsageCompleteOperation::new();
        wrong.first().unwrap();
        assert!(wrong.next(IoResult::Done).is_err());
        assert!(wrong.abort().is_empty());
    }

    #[test]
    fn loads_one_counter() {
        let key = usage_group_key(ulid::Ulid::from_parts(6, 6));
        let mut missing = LoadCountersOperation::new(key.clone());
        assert_eq!(
            missing.first(),
            Ok(Next::Call(read_row(USAGE_STATS_KEYSPACE, key.clone())))
        );
        let answer = IoResult::Values(vec![None]);
        assert_eq!(
            missing.next(answer),
            Ok(Next::Done(UsageCounters::default()))
        );
        let mut broken = LoadCountersOperation::new(key);
        broken.first().unwrap();
        let answer = IoResult::Values(vec![Some(Value::from(vec![0xff]))]);
        assert!(broken.next(answer).is_err());
    }

    #[test]
    fn loads_global_shards() {
        let mut operation = LoadCountersOperation::new(USAGE_GLOBAL_KEY.to_vec());
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Read(global_shard_rows())))
        );
        let answer = IoResult::Values(vec![None; global_shard_rows().len()]);
        assert_eq!(
            operation.next(answer),
            Ok(Next::Done(UsageCounters::default()))
        );
    }

    #[test]
    fn marks_then_wakes() {
        let group = ulid::Ulid::from_parts(6, 6);
        let generation = ulid::Ulid::from_parts(1, 1);
        let mut mark = MarkDirtyOperation::new([group], generation);
        assert_eq!(
            mark.first(),
            Ok(Next::Call(IoCall::Write(dirty_rows([group], generation))))
        );
        assert_eq!(mark.next(IoResult::Done), Ok(publish_call()));
        let scheduled = task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishUsageSnapshots,
            after: SNAPSHOT_PUBLISH_DEBOUNCE,
        });
        assert_eq!(mark.next(scheduled), Ok(Next::Done(())));
        // A refused timer still ends the wake; the markers wait for the next publish.
        let mut wake = MarkDirtyOperation::wake();
        assert_eq!(wake.first(), Ok(publish_call()));
        let refused = task(TaskEvent::Error {
            key: None,
            message: "no task handle".to_string(),
        });
        assert_eq!(wake.next(refused), Ok(Next::Done(())));
    }

    #[test]
    fn marking_rejects_stray() {
        let mut marking =
            MarkDirtyOperation::new([ulid::Ulid::from_parts(6, 6)], ulid::Ulid::nil());
        marking.first().unwrap();
        assert!(marking.next(IoResult::Values(vec![None])).is_err());
        let mut waking = MarkDirtyOperation::wake();
        waking.first().unwrap();
        let stray = IoResult::Event(Box::new(Event::Delayed));
        assert!(matches!(
            waking.next(stray),
            Err(MarkDirtyError::UnexpectedEvent(_))
        ));
        assert!(waking.abort().is_empty());
    }
}
