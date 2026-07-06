use aruna_core::NodeId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, REALM_CONFIG_KEYSPACE,
    S3_BUCKET_KEYSPACE, USAGE_NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    NODE_USAGE_DIRTY_GLOBAL_KEY, NODE_USAGE_DIRTY_PREFIX, NODE_USAGE_GLOBAL_PREFIX,
    NODE_USAGE_GROUP_PREFIX, NODE_USAGE_SUMMARY_GLOBAL_KEY, NODE_USAGE_SUMMARY_GROUP_PREFIX,
    NodeUsageSnapshot, RealmConfigDocument, RealmId, USAGE_GLOBAL_KEY, USAGE_GLOBAL_SHARD_COUNT,
    UsageCounterError, UsageCounters, UsageDelta, VersionKey, node_usage_dirty_group_id,
    node_usage_dirty_group_key, node_usage_global_key, node_usage_group_key,
    node_usage_group_key_group_id, node_usage_group_prefix, node_usage_key_node_id,
    node_usage_summary_group_key, usage_global_key_for_group, usage_global_shard_index,
    usage_global_shard_key, usage_global_shard_keys, usage_group_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use smallvec::smallvec;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use thiserror::Error;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::replicate_documents::{ReplicateDocumentsConfig, ReplicateDocumentsOperation};

#[derive(Debug, Error, PartialEq)]
pub enum UsageUpdateError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
    #[error("usage counter update received unexpected event: {0:?}")]
    UnexpectedEvent(Event),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum UsageUpdatePhase {
    Pending,
    Reading,
    Writing,
    Done,
}

/// Embeddable read-modify-write of the maintained usage counters. Hosts run
/// it inside their own transaction right before the commit so counter changes
/// are atomic with the data they account for.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageCounterUpdate {
    entries: Vec<(Vec<u8>, UsageDelta)>,
    dirty_group: GroupId,
    phase: UsageUpdatePhase,
}

impl UsageCounterUpdate {
    pub fn for_group(group_id: GroupId, delta: UsageDelta) -> Self {
        Self {
            entries: vec![
                (usage_global_key_for_group(group_id), delta),
                (usage_group_key(group_id), delta),
            ],
            dirty_group: group_id,
            phase: UsageUpdatePhase::Pending,
        }
    }

    pub fn with_global(
        group_id: GroupId,
        group_delta: UsageDelta,
        global_delta: UsageDelta,
    ) -> Self {
        Self {
            entries: vec![
                (usage_global_key_for_group(group_id), global_delta),
                (usage_group_key(group_id), group_delta),
            ],
            dirty_group: group_id,
            phase: UsageUpdatePhase::Pending,
        }
    }

    /// Writes, in the same transaction as the counter update, the dirty markers
    /// the debounced publisher scans to rebuild and distribute node snapshots.
    /// The marker value is a fresh generation id: the publisher only clears a
    /// marker whose stored generation still matches the one it observed, so a
    /// write that re-dirties a marker mid-publish keeps its retry signal.
    fn dirty_marker_writes(&self) -> Vec<(String, Key, Value)> {
        let generation = ByteView::from(ulid::Ulid::new().to_bytes().to_vec());
        vec![
            (
                USAGE_NODE_STATS_KEYSPACE.to_string(),
                ByteView::from(NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()),
                generation.clone(),
            ),
            (
                USAGE_NODE_STATS_KEYSPACE.to_string(),
                ByteView::from(node_usage_dirty_group_key(self.dirty_group)),
                generation,
            ),
        ]
    }

    pub fn is_noop(&self) -> bool {
        self.entries.iter().all(|(_, delta)| delta.is_zero())
    }

    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        self.phase = UsageUpdatePhase::Reading;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: self
                .entries
                .iter()
                .map(|(key, _)| (USAGE_STATS_KEYSPACE.to_string(), key.clone().into()))
                .collect(),
            txn_id: Some(txn_id),
        })]
    }

    /// Returns `Ok(Some(effects))` while more storage work is needed and
    /// `Ok(None)` once the counters are written.
    pub fn step(
        &mut self,
        event: Event,
        txn_id: TxnId,
    ) -> Result<Option<Effects>, UsageUpdateError> {
        match (&self.phase, event) {
            (
                UsageUpdatePhase::Reading,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => {
                let mut writes = Vec::with_capacity(self.entries.len() + 2);
                for ((key, delta), (_, value)) in self.entries.iter().zip(values) {
                    let mut counters = match value {
                        Some(value) => UsageCounters::from_bytes(value.as_ref())?,
                        None => UsageCounters::default(),
                    };
                    counters.apply(delta)?;
                    writes.push((
                        USAGE_STATS_KEYSPACE.to_string(),
                        key.clone().into(),
                        counters.to_bytes()?.into(),
                    ));
                }
                writes.extend(self.dirty_marker_writes());
                self.phase = UsageUpdatePhase::Writing;
                Ok(Some(smallvec![Effect::Storage(
                    StorageEffect::BatchWrite {
                        writes,
                        txn_id: Some(txn_id),
                    }
                )]))
            }
            (UsageUpdatePhase::Writing, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.phase = UsageUpdatePhase::Done;
                Ok(None)
            }
            (_, received) => Err(UsageUpdateError::UnexpectedEvent(received)),
        }
    }

    pub fn is_done(&self) -> bool {
        self.phase == UsageUpdatePhase::Done
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QuotaGatePhase {
    Pending,
    ReadRealmConfig,
    ReadLocal,
    ScanRemote,
    Done,
}

#[derive(Debug, Error, PartialEq)]
pub enum QuotaGateError {
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("quota gate received unexpected event: {0:?}")]
    UnexpectedEvent(Event),
}

/// Embeddable read side of the hard per-group quota gate. Sums the group's
/// realm-wide `logical_bytes` — the live local group counter plus every remote
/// node's replicated group snapshot — inside the caller's write transaction right
/// before it commits the counters, so a concurrent same-group write conflicts on
/// the group counter key and cannot slip a second write past the ceiling.
///
/// `ceiling` already folds in the grace factor; that headroom is deliberately the
/// budget for remote snapshot staleness, since remote group totals lag the
/// authoritative counters that produced them.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaGate {
    ceiling: u64,
    delta_logical_bytes: u64,
    group_key: Vec<u8>,
    remote_prefix: Vec<u8>,
    local_node_id: NodeId,
    realm_id: Option<RealmId>,
    active_node_ids: Option<HashSet<NodeId>>,
    usage: u64,
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
            group_key: usage_group_key(group_id),
            remote_prefix: node_usage_group_prefix(group_id),
            local_node_id,
            realm_id: None,
            active_node_ids: None,
            usage: 0,
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

    pub fn start(&mut self, txn_id: TxnId) -> Effects {
        if let Some(realm_id) = self.realm_id {
            self.phase = QuotaGatePhase::ReadRealmConfig;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: DocumentSyncTarget::RealmConfig { realm_id }.storage_key(),
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
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
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
                QuotaGatePhase::ReadRealmConfig,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => {
                if let Some(bytes) = value {
                    let document = RealmConfigDocument::from_bytes(bytes.as_ref())?;
                    self.active_node_ids =
                        Some(document.sync_eligible_node_ids()?.into_iter().collect());
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
                    // Skip our own snapshot: the live local counter already accounts
                    // for it. Mirror `sum_remote_snapshots` — never trust a snapshot
                    // whose embedded node id disagrees with its storage key.
                    let key_node_id = node_usage_key_node_id(key.as_ref());
                    if key_node_id == Some(self.local_node_id) {
                        continue;
                    }
                    if let (Some(active_node_ids), Some(node_id)) =
                        (&self.active_node_ids, key_node_id)
                        && !active_node_ids.contains(&node_id)
                    {
                        continue;
                    }
                    let snapshot = NodeUsageSnapshot::from_bytes(value.as_ref())?;
                    if key_node_id != Some(snapshot.node_id) {
                        continue;
                    }
                    self.usage = self.usage.saturating_add(snapshot.counters.logical_bytes);
                }
                match next_start_after {
                    Some(start) => Ok(Some(self.scan_effect(Some(start), txn_id))),
                    None => {
                        self.phase = QuotaGatePhase::Done;
                        Ok(None)
                    }
                }
            }
            (_, received) => Err(QuotaGateError::UnexpectedEvent(received)),
        }
    }

    /// Projected group-wide `logical_bytes` if the pending write commits.
    pub fn projected_usage(&self) -> u64 {
        self.usage.saturating_add(self.delta_logical_bytes)
    }

    /// True when committing the pending write would push the group's realm-wide
    /// `logical_bytes` past the ceiling. At-ceiling passes; one byte over fails.
    pub fn is_exceeded(&self) -> bool {
        self.projected_usage() > self.ceiling
    }

    pub fn ceiling(&self) -> u64 {
        self.ceiling
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LoadUsageCountersState {
    Init,
    ReadCounter,
    ReadGlobal,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum LoadUsageCountersError {
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
pub struct LoadUsageCountersOperation {
    key: Vec<u8>,
    state: LoadUsageCountersState,
    output: Option<Result<UsageCounters, LoadUsageCountersError>>,
}

impl LoadUsageCountersOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self {
            key,
            state: LoadUsageCountersState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<UsageCounters, LoadUsageCountersError>) -> Effects {
        self.state = if result.is_ok() {
            LoadUsageCountersState::Finish
        } else {
            LoadUsageCountersState::Error
        };
        self.output = Some(result);
        smallvec![]
    }

    fn unexpected_event(&mut self, expected: &'static str, received: Event) -> Effects {
        self.finish(Err(LoadUsageCountersError::UnexpectedEvent {
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
                self.finish(sum_global_usage_counters(values))
            }
            Event::Storage(StorageEvent::Error { error }) => self.finish(Err(error.into())),
            other => self.unexpected_event("storage batch read result", other),
        }
    }
}

impl Operation for LoadUsageCountersOperation {
    type Output = UsageCounters;
    type Error = LoadUsageCountersError;

    fn start(&mut self) -> Effects {
        if self.key.as_slice() == USAGE_GLOBAL_KEY {
            self.state = LoadUsageCountersState::ReadGlobal;
            let reads = usage_global_shard_keys()
                .into_iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
                .collect::<Vec<_>>();
            smallvec![Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: None,
            })]
        } else {
            self.state = LoadUsageCountersState::ReadCounter;
            smallvec![Effect::Storage(StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: self.key.clone().into(),
                txn_id: None,
            })]
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            LoadUsageCountersState::ReadCounter => self.handle_counter_read(event),
            LoadUsageCountersState::ReadGlobal => self.handle_global_read(event),
            LoadUsageCountersState::Init
            | LoadUsageCountersState::Finish
            | LoadUsageCountersState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            LoadUsageCountersState::Finish | LoadUsageCountersState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(LoadUsageCountersError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn sum_global_usage_counters(
    values: Vec<(Key, Option<Value>)>,
) -> Result<UsageCounters, LoadUsageCountersError> {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebuildUsageStatsState {
    Init,
    ScanBuckets,
    ScanBlobs,
    ScanHeads,
    ScanVersions,
    ScanCounters,
    StartWriteTransaction,
    WriteCounters,
    DeleteStaleCounters,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RebuildUsageStatsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    CounterError(#[from] UsageCounterError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: RebuildUsageStatsState,
        expected: &'static str,
        received: Event,
    },
    #[error("RebuildUsageStats failed")]
    RebuildFailed,
}

/// Recomputes all usage counters from the underlying keyspaces and writes
/// them in one transaction. Runs at startup when counters are missing and
/// doubles as a repair tool; it is not safe to run concurrently with writes.
/// An object counts as live when its current head points at a non-delete
/// version, matching the pointer transitions the write operations maintain.
#[derive(Debug, PartialEq)]
pub struct RebuildUsageStatsOperation {
    state: RebuildUsageStatsState,
    txn_id: Option<TxnId>,
    bucket_groups: HashMap<String, GroupId>,
    blob_sizes: HashMap<Vec<u8>, u64>,
    current_versions: HashMap<(String, String), ulid::Ulid>,
    global: UsageCounters,
    global_shards: Vec<UsageCounters>,
    groups: HashMap<GroupId, UsageCounters>,
    existing_counter_keys: Vec<Vec<u8>>,
    stale_counter_deletes: Vec<(String, Key)>,
    output: Option<Result<UsageCounters, RebuildUsageStatsError>>,
}

impl Default for RebuildUsageStatsOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl RebuildUsageStatsOperation {
    const SCAN_LIMIT: usize = 1_000;

    pub fn new() -> Self {
        Self {
            state: RebuildUsageStatsState::Init,
            txn_id: None,
            bucket_groups: HashMap::new(),
            blob_sizes: HashMap::new(),
            current_versions: HashMap::new(),
            global: UsageCounters::default(),
            global_shards: vec![UsageCounters::default(); USAGE_GLOBAL_SHARD_COUNT],
            groups: HashMap::new(),
            existing_counter_keys: Vec::new(),
            stale_counter_deletes: Vec::new(),
            output: None,
        }
    }

    fn emit_error(&mut self, error: RebuildUsageStatsError) -> Effects {
        self.state = RebuildUsageStatsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn iter_effect(&self, key_space: &str, start_after: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: Self::SCAN_LIMIT,
            txn_id: None,
        })
    }

    fn scan_keyspace(&self) -> Option<&'static str> {
        match self.state {
            RebuildUsageStatsState::ScanBuckets => Some(S3_BUCKET_KEYSPACE),
            RebuildUsageStatsState::ScanBlobs => Some(BLOB_LOCATIONS_KEYSPACE),
            RebuildUsageStatsState::ScanHeads => Some(BLOB_HEAD_KEYSPACE),
            RebuildUsageStatsState::ScanVersions => Some(BLOB_VERSIONS_KEYSPACE),
            RebuildUsageStatsState::ScanCounters => Some(USAGE_STATS_KEYSPACE),
            _ => None,
        }
    }

    fn group_entry(&mut self, group_id: GroupId) -> &mut UsageCounters {
        self.groups.entry(group_id).or_default()
    }

    fn global_shard_entry(&mut self, group_id: GroupId) -> &mut UsageCounters {
        &mut self.global_shards[usage_global_shard_index(group_id)]
    }

    fn consume_values(&mut self, values: &[(Key, Value)]) -> Result<(), RebuildUsageStatsError> {
        match self.state {
            RebuildUsageStatsState::ScanBuckets => {
                for (key, value) in values {
                    let info = BucketInfo::from_bytes(value.as_ref())?;
                    let bucket = String::from_utf8(key.to_vec()).map_err(ConversionError::from)?;
                    let delta = UsageCounters {
                        buckets: 1,
                        ..Default::default()
                    };
                    self.global.add(&delta)?;
                    self.group_entry(info.group_id).add(&delta)?;
                    self.global_shard_entry(info.group_id).add(&delta)?;
                    self.bucket_groups.insert(bucket, info.group_id);
                }
            }
            RebuildUsageStatsState::ScanBlobs => {
                for (key, value) in values {
                    let location = BackendLocation::from_bytes(value.as_ref())?;
                    if location.staging || location.partial {
                        continue;
                    }
                    let delta = UsageCounters {
                        stored_blobs: 1,
                        stored_bytes: location.blob_size,
                        ..Default::default()
                    };
                    self.global.add(&delta)?;
                    self.global_shards[0].add(&delta)?;
                    self.blob_sizes.insert(key.to_vec(), location.blob_size);
                }
            }
            RebuildUsageStatsState::ScanHeads => {
                for (key, value) in values {
                    let head_key = BlobHeadKey::from_bytes(key.as_ref())?;
                    let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
                    self.current_versions
                        .insert((head_key.bucket, head_key.key), pointer.version_id);
                }
            }
            RebuildUsageStatsState::ScanVersions => {
                for (key, value) in values {
                    let version = BlobVersion::from_bytes(value.as_ref())?;
                    let version_key = VersionKey::from_bytes(key.as_ref())?;
                    let object = (version_key.bucket.clone(), version_key.key.clone());
                    if self
                        .current_versions
                        .get(&object)
                        .is_some_and(|version_id| *version_id == version_key.version_id)
                        && !version.is_deleted()
                    {
                        let delta = UsageCounters {
                            objects: 1,
                            ..Default::default()
                        };
                        self.global.add(&delta)?;
                        if let Some(group_id) = self.bucket_groups.get(&version_key.bucket).copied()
                        {
                            self.group_entry(group_id).add(&delta)?;
                            self.global_shard_entry(group_id).add(&delta)?;
                        } else {
                            self.global_shards[0].add(&delta)?;
                        }
                    }

                    let Some(size) = (match version.state {
                        BlobVersionState::Materialized { blob_hash, .. } => {
                            self.blob_sizes.get(blob_hash.as_slice()).copied()
                        }
                        BlobVersionState::Reference {
                            cached_metadata, ..
                        } => Some(cached_metadata.content_length),
                        BlobVersionState::Deleted => None,
                    }) else {
                        continue;
                    };
                    let delta = UsageCounters {
                        logical_bytes: size,
                        ..Default::default()
                    };
                    self.global.add(&delta)?;
                    if let Some(group_id) = self.bucket_groups.get(&version_key.bucket).copied() {
                        self.group_entry(group_id).add(&delta)?;
                        self.global_shard_entry(group_id).add(&delta)?;
                    } else {
                        self.global_shards[0].add(&delta)?;
                    }
                }
            }
            RebuildUsageStatsState::ScanCounters => {
                self.existing_counter_keys
                    .extend(values.iter().map(|(key, _)| key.to_vec()));
            }
            _ => {}
        }
        Ok(())
    }

    fn next_scan(&mut self) -> Effects {
        let next = match self.state {
            RebuildUsageStatsState::ScanBuckets => RebuildUsageStatsState::ScanBlobs,
            RebuildUsageStatsState::ScanBlobs => RebuildUsageStatsState::ScanHeads,
            RebuildUsageStatsState::ScanHeads => RebuildUsageStatsState::ScanVersions,
            RebuildUsageStatsState::ScanVersions => RebuildUsageStatsState::ScanCounters,
            RebuildUsageStatsState::ScanCounters => {
                self.state = RebuildUsageStatsState::StartWriteTransaction;
                return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })];
            }
            _ => return self.emit_error(RebuildUsageStatsError::RebuildFailed),
        };
        self.state = next;
        let key_space = match self.scan_keyspace() {
            Some(key_space) => key_space,
            None => return self.emit_error(RebuildUsageStatsError::RebuildFailed),
        };
        smallvec![self.iter_effect(key_space, None)]
    }

    fn handle_page(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(RebuildUsageStatsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        if let Err(err) = self.consume_values(&values) {
            return self.emit_error(err);
        }

        if let Some(start_after) = next_start_after {
            let key_space = match self.scan_keyspace() {
                Some(key_space) => key_space,
                None => return self.emit_error(RebuildUsageStatsError::RebuildFailed),
            };
            return smallvec![self.iter_effect(key_space, Some(start_after))];
        }

        self.next_scan()
    }

    fn handle_write_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(RebuildUsageStatsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);

        let mut writes = Vec::with_capacity(USAGE_GLOBAL_SHARD_COUNT + self.groups.len());
        let mut write_keys = HashSet::with_capacity(USAGE_GLOBAL_SHARD_COUNT + self.groups.len());
        for (shard, counters) in self.global_shards.iter().enumerate() {
            let key = usage_global_shard_key(shard);
            let bytes = match counters.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.emit_error(err.into()),
            };
            write_keys.insert(key.clone());
            writes.push((USAGE_STATS_KEYSPACE.to_string(), key.into(), bytes.into()));
        }
        for (group_id, counters) in &self.groups {
            let key = usage_group_key(*group_id);
            let bytes = match counters.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.emit_error(err.into()),
            };
            write_keys.insert(key.clone());
            writes.push((USAGE_STATS_KEYSPACE.to_string(), key.into(), bytes.into()));
        }
        self.stale_counter_deletes = self
            .existing_counter_keys
            .iter()
            .filter(|key| !write_keys.contains(*key))
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.clone().into()))
            .collect();

        self.state = RebuildUsageStatsState::WriteCounters;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    fn handle_counters_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(RebuildUsageStatsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(RebuildUsageStatsError::RebuildFailed);
        };
        if !self.stale_counter_deletes.is_empty() {
            self.state = RebuildUsageStatsState::DeleteStaleCounters;
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes: std::mem::take(&mut self.stale_counter_deletes),
                txn_id: Some(txn_id),
            })];
        }
        self.state = RebuildUsageStatsState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_stale_counters_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(RebuildUsageStatsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(RebuildUsageStatsError::RebuildFailed);
        };
        self.state = RebuildUsageStatsState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(RebuildUsageStatsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };
        self.state = RebuildUsageStatsState::Finish;
        self.output = Some(Ok(self.global));
        smallvec![]
    }
}

impl Operation for RebuildUsageStatsOperation {
    type Output = Option<Result<UsageCounters, RebuildUsageStatsError>>;
    type Error = RebuildUsageStatsError;

    fn start(&mut self) -> Effects {
        self.state = RebuildUsageStatsState::ScanBuckets;
        smallvec![self.iter_effect(S3_BUCKET_KEYSPACE, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(RebuildUsageStatsError::StorageError(error));
        }

        match self.state {
            RebuildUsageStatsState::Init => self.start(),
            RebuildUsageStatsState::ScanBuckets
            | RebuildUsageStatsState::ScanBlobs
            | RebuildUsageStatsState::ScanHeads
            | RebuildUsageStatsState::ScanVersions
            | RebuildUsageStatsState::ScanCounters => self.handle_page(event),
            RebuildUsageStatsState::StartWriteTransaction => {
                self.handle_write_transaction_started(event)
            }
            RebuildUsageStatsState::WriteCounters => self.handle_counters_written(event),
            RebuildUsageStatsState::DeleteStaleCounters => {
                self.handle_stale_counters_deleted(event)
            }
            RebuildUsageStatsState::CommitTransaction => self.handle_transaction_committed(event),
            RebuildUsageStatsState::Finish | RebuildUsageStatsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RebuildUsageStatsState::Finish | RebuildUsageStatsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == RebuildUsageStatsState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(RebuildUsageStatsError::RebuildFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Debounce window for the coalesced node-usage snapshot publisher. `ShortenTimer`
/// makes the timer fire this long after the *first* dirty write of a burst and
/// keeps every later write inside the same window, so bursts collapse into one
/// publish run with bounded latency.
pub const USAGE_SNAPSHOT_PUBLISH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Schedules (or shortens toward) the debounced snapshot publish task.
pub fn schedule_usage_snapshot_publish_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishUsageSnapshots,
        after: USAGE_SNAPSHOT_PUBLISH_DEBOUNCE,
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

    fn targets(&self, realm_id: RealmId, node_id: NodeId) -> Vec<DocumentSyncTarget> {
        let mut targets = Vec::with_capacity(self.groups.len() + usize::from(self.global));
        if self.global {
            targets.push(DocumentSyncTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            });
        }
        for group_id in &self.groups {
            targets.push(DocumentSyncTarget::NodeUsage {
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

async fn iter_all(
    storage: &StorageHandle,
    key_space: &str,
    prefix: Option<Key>,
) -> Result<Vec<(Key, Value)>, String> {
    let mut collected = Vec::new();
    let mut start = None;
    loop {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: prefix.clone(),
                start: start.map(IterStart::After),
                limit: 1_000,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                collected.extend(values);
                match next_start_after {
                    Some(next) => start = Some(next),
                    None => break,
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
            other => return Err(format!("unexpected iter event: {other:?}")),
        }
    }
    Ok(collected)
}

pub(crate) async fn read_local_global(storage: &StorageHandle) -> Result<UsageCounters, String> {
    let reads = usage_global_shard_keys()
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
        .collect();
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
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
) -> Result<UsageCounters, String> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: Key::from(usage_group_key(group_id)),
            txn_id: None,
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

/// Sums remote nodes' snapshots under `prefix`, skipping the local node's own
/// snapshot so it is never double-counted against the live local counters.
async fn sum_remote_snapshots(
    storage: &StorageHandle,
    prefix: Vec<u8>,
    local_node_id: NodeId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<UsageCounters, String> {
    let entries = iter_all(storage, USAGE_NODE_STATS_KEYSPACE, Some(Key::from(prefix))).await?;
    let mut total = UsageCounters::default();
    for (key, value) in entries {
        let key_node_id = node_usage_key_node_id(key.as_ref());
        if key_node_id == Some(local_node_id) {
            continue;
        }
        if let (Some(active_node_ids), Some(node_id)) = (active_node_ids, key_node_id)
            && !active_node_ids.contains(&node_id)
        {
            continue;
        }
        let snapshot = NodeUsageSnapshot::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
        // Defensive: ingest ties a snapshot's node id to its storage key, but
        // never sum one whose embedded node id disagrees with its key so a
        // misattributed snapshot can never inflate the realm aggregate.
        if key_node_id != Some(snapshot.node_id) {
            continue;
        }
        total.add(&snapshot.counters).map_err(|e| e.to_string())?;
    }
    Ok(total)
}

/// Rebuilds this node's snapshot documents from live local counters and
/// distributes them over the sync layer. In dirty mode only groups with a
/// pending marker are refreshed; in full mode every group plus the global total
/// is republished (used at startup and after a counter rebuild), and groups this
/// node published before but no longer counts are re-published as zero snapshots.
///
/// The dirty markers are only cleared after replication has durably accepted the
/// snapshots, and only for markers whose generation was not bumped by a
/// concurrent write, so a failed publish or a racing counter update always
/// leaves a retry signal behind.
pub async fn publish_usage_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_usage_snapshots_retaining_markers(ctx, node_id, realm_id, full).await?;
    clear_consumed_markers(&ctx.storage_handle, observed_markers).await?;
    Ok(published)
}

async fn publish_usage_snapshots_retaining_markers(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<(PublishedUsage, Vec<(Key, Value)>), String> {
    let storage = &ctx.storage_handle;

    let observed_markers = iter_all(
        storage,
        USAGE_NODE_STATS_KEYSPACE,
        Some(Key::from(NODE_USAGE_DIRTY_PREFIX.to_vec())),
    )
    .await?;

    let mut groups: BTreeSet<GroupId> = BTreeSet::new();
    let mut global = false;
    for (key, _) in &observed_markers {
        if key.as_ref() == NODE_USAGE_DIRTY_GLOBAL_KEY {
            global = true;
        } else if let Some(group_id) = node_usage_dirty_group_id(key.as_ref()) {
            groups.insert(group_id);
        }
    }

    if full {
        global = true;
        for (key, _) in iter_all(
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
        // Groups this node has a published snapshot for but no live counter (e.g.
        // their counter key was pruned by a rebuild) are re-published: reading a
        // missing counter yields zero, so the stale snapshot is overwritten with a
        // zero total and peers stop summing it.
        for (key, _) in iter_all(
            storage,
            USAGE_NODE_STATS_KEYSPACE,
            Some(Key::from(NODE_USAGE_GROUP_PREFIX.to_vec())),
        )
        .await?
        {
            if node_usage_key_node_id(key.as_ref()) == Some(node_id)
                && let Some(group_id) = node_usage_group_key_group_id(key.as_ref())
            {
                groups.insert(group_id);
            }
        }
    }

    if !global && groups.is_empty() {
        return Ok((PublishedUsage::default(), Vec::new()));
    }

    let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(groups.len() + 1);
    if global {
        let counters = read_local_global(storage).await?;
        let snapshot = NodeUsageSnapshot { node_id, counters };
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
            Key::from(node_usage_global_key(node_id)),
            Value::from(snapshot.to_bytes().map_err(|e| e.to_string())?),
        ));
    }
    let group_list: Vec<GroupId> = groups.into_iter().collect();
    for group_id in &group_list {
        let counters = read_local_group(storage, *group_id).await?;
        let snapshot = NodeUsageSnapshot { node_id, counters };
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
            Key::from(node_usage_group_key(*group_id, node_id)),
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
        if let Err(retry_error) =
            signal_usage_snapshot_retry(ctx, &published, &observed_markers).await
        {
            return Err(format!(
                "{error}; additionally failed to mark usage snapshot retry: {retry_error}"
            ));
        }
        return Err(error);
    }

    Ok((published, observed_markers))
}

pub async fn publish_and_refresh_usage_snapshots(
    ctx: &DriverContext,
    node_id: NodeId,
    realm_id: RealmId,
    full: bool,
) -> Result<PublishedUsage, String> {
    let (published, observed_markers) =
        publish_usage_snapshots_retaining_markers(ctx, node_id, realm_id, full).await?;
    if !published.is_empty()
        && let Err(error) =
            recompute_realm_usage_summary(ctx, node_id, published.global, published.groups.clone())
                .await
    {
        if let Err(retry_error) =
            signal_usage_snapshot_retry(ctx, &published, &observed_markers).await
        {
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

async fn signal_usage_snapshot_retry(
    ctx: &DriverContext,
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
) -> Result<(), String> {
    write_usage_snapshot_retry_markers(&ctx.storage_handle, published, observed_markers).await?;

    let Some(task_handle) = ctx.task_handle.as_ref() else {
        return Ok(());
    };
    match task_handle
        .send_effect(schedule_usage_snapshot_publish_effect())
        .await
    {
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

async fn write_usage_snapshot_retry_markers(
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
    let generation = Value::from(ulid::Ulid::new().to_bytes().to_vec());
    let mut writes: Vec<(String, Key, Value)> =
        Vec::with_capacity(published.groups.len() + usize::from(published.global));
    if published.global && !observed_keys.contains(NODE_USAGE_DIRTY_GLOBAL_KEY) {
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
            Key::from(NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()),
            generation.clone(),
        ));
    }
    for group_id in &published.groups {
        let marker_key = node_usage_dirty_group_key(*group_id);
        if observed_keys.contains(&marker_key) {
            continue;
        }
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
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

/// Deletes each observed dirty marker, but only if its stored generation still
/// matches the one seen when the publish run started. Re-reading the markers
/// inside the write transaction makes fjall abort the commit if a concurrent
/// counter update re-dirtied any of them after they were observed, so a racing
/// write never loses its retry signal.
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
        .map(|(key, _)| (USAGE_NODE_STATS_KEYSPACE.to_string(), key.clone()))
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
            deletes.push((USAGE_NODE_STATS_KEYSPACE.to_string(), key.clone()));
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

async fn active_usage_node_ids(ctx: &DriverContext) -> Result<Option<HashSet<NodeId>>, String> {
    let Some(net_handle) = ctx.net_handle.as_ref() else {
        return Ok(None);
    };
    let realm_id = *net_handle.realm_id();
    let key = DocumentSyncTarget::RealmConfig { realm_id }.storage_key();
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
                    .sync_eligible_node_ids()
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

async fn clear_realm_usage_summary_cache(ctx: &DriverContext) -> Result<(), String> {
    let mut deletes = vec![(
        USAGE_NODE_STATS_KEYSPACE.to_string(),
        Key::from(NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec()),
    )];
    for (key, _) in iter_all(
        &ctx.storage_handle,
        USAGE_NODE_STATS_KEYSPACE,
        Some(Key::from(NODE_USAGE_SUMMARY_GROUP_PREFIX.to_vec())),
    )
    .await?
    {
        deletes.push((USAGE_NODE_STATS_KEYSPACE.to_string(), key));
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
pub async fn recompute_realm_usage_summary(
    ctx: &DriverContext,
    local_node_id: NodeId,
    include_global: bool,
    groups: Vec<GroupId>,
) -> Result<(), String> {
    let storage = &ctx.storage_handle;
    let active_node_ids = active_usage_node_ids(ctx).await?;
    let mut writes: Vec<(String, Key, Value)> = Vec::with_capacity(groups.len() + 1);

    if include_global {
        let total = realm_global_usage(storage, local_node_id, active_node_ids.as_ref()).await?;
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
            Key::from(NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec()),
            Value::from(total.to_bytes().map_err(|e| e.to_string())?),
        ));
    }
    for group_id in groups {
        let total =
            realm_group_usage(storage, local_node_id, group_id, active_node_ids.as_ref()).await?;
        writes.push((
            USAGE_NODE_STATS_KEYSPACE.to_string(),
            Key::from(node_usage_summary_group_key(group_id)),
            Value::from(total.to_bytes().map_err(|e| e.to_string())?),
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

/// Refreshes the persisted realm usage summed cache for exactly the `NodeUsage`
/// scopes touched by a document-sync reconcile. Shared by every reconcile
/// handler (inbound apply, durable outbox drain, and the `SyncDocument` timer) so
/// remote snapshots that land on any of those paths update the realm aggregate.
pub async fn refresh_realm_usage_summary_for_targets(
    ctx: &DriverContext,
    local_node_id: NodeId,
    targets: &[DocumentSyncTarget],
) {
    let mut include_global = false;
    let mut groups = BTreeSet::new();
    let mut realm_config_changed = false;
    for target in targets {
        match target {
            DocumentSyncTarget::NodeUsage { group_id, .. } => match group_id {
                Some(group_id) => {
                    groups.insert(*group_id);
                }
                None => include_global = true,
            },
            DocumentSyncTarget::RealmConfig { .. } => {
                realm_config_changed = true;
            }
            _ => {}
        }
    }
    if realm_config_changed && let Err(error) = clear_realm_usage_summary_cache(ctx).await {
        warn!(error = %error, "Failed to clear realm usage summary after realm config change");
    }
    if !include_global && groups.is_empty() {
        return;
    }
    if let Err(error) = recompute_realm_usage_summary(
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

async fn realm_global_usage(
    storage: &StorageHandle,
    local_node_id: NodeId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<UsageCounters, String> {
    let mut total = read_local_global(storage).await?;
    let remote = sum_remote_snapshots(
        storage,
        NODE_USAGE_GLOBAL_PREFIX.to_vec(),
        local_node_id,
        active_node_ids,
    )
    .await?;
    total.add(&remote).map_err(|e| e.to_string())?;
    Ok(total)
}

async fn realm_group_usage(
    storage: &StorageHandle,
    local_node_id: NodeId,
    group_id: GroupId,
    active_node_ids: Option<&HashSet<NodeId>>,
) -> Result<UsageCounters, String> {
    let mut total = read_local_group(storage, group_id).await?;
    let remote = sum_remote_snapshots(
        storage,
        node_usage_group_prefix(group_id),
        local_node_id,
        active_node_ids,
    )
    .await?;
    total.add(&remote).map_err(|e| e.to_string())?;
    Ok(total)
}

/// Reads the realm-wide total from the summed cache, falling back to an
/// on-the-fly recompute when the cache entry has not been materialized yet.
pub async fn load_realm_usage(
    ctx: &DriverContext,
    local_node_id: NodeId,
    scope: RealmUsageScope,
) -> Result<UsageCounters, String> {
    let storage = &ctx.storage_handle;
    let cache_key = match scope {
        RealmUsageScope::Global => Key::from(NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec()),
        RealmUsageScope::Group(group_id) => Key::from(node_usage_summary_group_key(group_id)),
    };
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            key: cache_key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => UsageCounters::from_bytes(bytes.as_ref()).map_err(|e| e.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => match scope {
            RealmUsageScope::Global => {
                let active_node_ids = active_usage_node_ids(ctx).await?;
                realm_global_usage(storage, local_node_id, active_node_ids.as_ref()).await
            }
            RealmUsageScope::Group(group_id) => {
                let active_node_ids = active_usage_node_ids(ctx).await?;
                realm_group_usage(storage, local_node_id, group_id, active_node_ids.as_ref()).await
            }
        },
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected realm usage read event: {other:?}")),
    }
}

/// Re-arms the debounced publish task when dirty markers survived a restart.
pub async fn restore_usage_snapshot_publish_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    let has_markers = match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(NODE_USAGE_DIRTY_PREFIX.to_vec())),
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
        && let Event::Task(TaskEvent::Error { message, .. }) = task_handle
            .send_effect(schedule_usage_snapshot_publish_effect())
            .await
    {
        warn!(message = %message, "Failed to restore node usage publish timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::create_bucket::CreateBucketOperation;
    use aruna_core::structs::{
        BlobHeadKey, BucketInfo, CurrentVersionPointer, PortableSourceDescriptor,
        SourceConnectorKind, SourceMetadata, StagingStrategy, VersionSourceBinding,
        usage_global_shard_keys,
    };
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn location(blob_size: u64, staging: bool, partial: bool) -> BackendLocation {
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            staging,
            partial,
            blob_size,
            hashes: std::collections::HashMap::new(),
        }
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

    async fn read_global_counters(ctx: &DriverContext) -> UsageCounters {
        let mut total = UsageCounters::default();
        for key in usage_global_shard_keys() {
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
    fn counter_update_applies_deltas_with_synthetic_events() {
        let group_id = Ulid::new();
        let txn_id = Ulid::new();
        let mut update = UsageCounterUpdate::for_group(
            group_id,
            UsageDelta {
                objects: 1,
                logical_bytes: 42,
                ..Default::default()
            },
        );

        let effects = update.start(txn_id);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == 2
        ));

        let existing = UsageCounters {
            objects: 5,
            logical_bytes: 100,
            ..Default::default()
        };
        let effects = update
            .step(
                Event::Storage(StorageEvent::BatchReadResult {
                    values: vec![
                        (
                            usage_global_key_for_group(group_id).into(),
                            Some(existing.to_bytes().unwrap().into()),
                        ),
                        (usage_group_key(group_id).into(), None),
                    ],
                }),
                txn_id,
            )
            .unwrap()
            .unwrap();

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected counter batch write");
        };
        let global = UsageCounters::from_bytes(writes[0].2.as_ref()).unwrap();
        assert_eq!(global.objects, 6);
        assert_eq!(global.logical_bytes, 142);
        let group = UsageCounters::from_bytes(writes[1].2.as_ref()).unwrap();
        assert_eq!(group.objects, 1);
        assert_eq!(group.logical_bytes, 42);

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

    #[tokio::test]
    async fn create_bucket_maintains_usage_counters() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::new();

        drive(
            CreateBucketOperation::new(
                "counted".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                },
            ),
            &ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let global = read_global_counters(&ctx).await;
        assert_eq!(global.buckets, 1);
        let group = read_counters(&ctx, usage_group_key(group_id)).await;
        assert_eq!(group.buckets, 1);
    }

    #[tokio::test]
    async fn load_usage_counters_reads_group_and_global_counters() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::new();
        let group = UsageCounters {
            buckets: 1,
            objects: 2,
            logical_bytes: 3,
            ..Default::default()
        };
        write_counters(&ctx, usage_group_key(group_id), group).await;

        let loaded = drive(
            LoadUsageCountersOperation::new(usage_group_key(group_id)),
            &ctx,
        )
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
        write_counters(&ctx, usage_global_shard_key(0), shard_zero).await;
        write_counters(&ctx, usage_global_shard_key(1), shard_one).await;

        let loaded = drive(
            LoadUsageCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()),
            &ctx,
        )
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
            LoadUsageCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()),
            &stale_global_ctx,
        )
        .await
        .unwrap();
        assert_eq!(loaded, UsageCounters::default());
    }

    #[tokio::test]
    async fn rebuild_counts_live_objects_logical_and_stored_bytes() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_a = Ulid::new();
        let group_b = Ulid::new();

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
                    key: hash.to_vec().into(),
                    value: loc.to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;
        }

        // alpha/live.txt: one materialized version, live.
        // alpha/gone.txt: materialized version superseded by a delete marker.
        // alpha/ref.txt: one reference version, live.
        // beta/shared.bin: two materialized versions of the shared blob.
        let write_version = async |bucket: &str, key: &str, version: BlobVersion| {
            let version_id = Ulid::new();
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
            BlobVersion::materialized(hashes[0], now, user, None),
        )
        .await;
        write_version(
            "alpha",
            "gone.txt",
            BlobVersion::materialized(hashes[1], now, user, None),
        )
        .await;
        let alpha_gone_head =
            write_version("alpha", "gone.txt", BlobVersion::deleted(now, user)).await;
        write_version(
            "beta",
            "shared.bin",
            BlobVersion::materialized(hashes[1], now, user, None),
        )
        .await;
        let beta_head = write_version(
            "beta",
            "shared.bin",
            BlobVersion::materialized(hashes[1], now, user, None),
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
                    },
                    connector_id: Some(Ulid::new()),
                },
                SourceMetadata {
                    content_length: 30,
                    content_type: None,
                    etag: None,
                    last_modified: None,
                    source_version: None,
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

        ctx.storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let global = drive(RebuildUsageStatsOperation::new(), &ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // live.txt, ref.txt, and shared.bin are live; gone.txt ends on a delete marker.
        assert_eq!(global.buckets, 2);
        assert_eq!(global.objects, 3);
        assert_eq!(global.stored_blobs, 2);
        assert_eq!(global.stored_bytes, 140);
        // 100 + 40 + 30 + 40 + 40: every materialized/reference version counts logically.
        assert_eq!(global.logical_bytes, 250);

        let stored_global = read_global_counters(&ctx).await;
        assert_eq!(stored_global, global);

        let alpha = read_counters(&ctx, usage_group_key(group_a)).await;
        assert_eq!(alpha.buckets, 1);
        assert_eq!(alpha.objects, 2);
        assert_eq!(alpha.logical_bytes, 170);

        let beta = read_counters(&ctx, usage_group_key(group_b)).await;
        assert_eq!(beta.buckets, 1);
        assert_eq!(beta.objects, 1);
        assert_eq!(beta.logical_bytes, 80);
    }

    #[tokio::test]
    async fn rebuild_removes_stale_usage_counter_keys() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let stale_group = Ulid::new();
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

        drive(RebuildUsageStatsOperation::new(), &ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(read_optional_counters(&ctx, stale_key).await.is_none());
        assert!(
            read_optional_counters(&ctx, b"global".to_vec())
                .await
                .is_none()
        );
        assert!(
            read_optional_counters(&ctx, usage_global_shard_key(0))
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
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
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
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USAGE_NODE_STATS_KEYSPACE.to_string(),
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

    #[tokio::test]
    async fn usage_update_writes_dirty_markers_in_transaction() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let group_id = Ulid::new();

        drive(
            CreateBucketOperation::new(
                "counted".to_string(),
                BucketInfo {
                    group_id,
                    created_at: SystemTime::now(),
                    created_by: Default::default(),
                    cors_configuration: None,
                },
            ),
            &ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_consumes_markers_and_writes_snapshots() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::new();

        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec(), Vec::new()).await;
        write_node_stat(&ctx, node_usage_dirty_group_key(group_id), Vec::new()).await;

        let published = publish_usage_snapshots(&ctx, node_id, realm_id, false)
            .await
            .unwrap();
        assert!(published.global);
        assert_eq!(published.groups, vec![group_id]);

        // Markers were consumed.
        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_none()
        );

        // Snapshot documents were written under this node's keys.
        let global = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, node_usage_global_key(node_id))
                .await
                .expect("global snapshot written"),
        )
        .unwrap();
        assert_eq!(global.node_id, node_id);
        assert_eq!(global.counters.logical_bytes, 10);
        let group = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, node_usage_group_key(group_id, node_id))
                .await
                .expect("group snapshot written"),
        )
        .unwrap();
        assert_eq!(group.counters.logical_bytes, 10);
    }

    #[tokio::test]
    async fn realm_summary_sums_local_and_remote_excluding_own_snapshot() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);
        let group_id = Ulid::new();

        let local_counters = UsageCounters {
            logical_bytes: 10,
            objects: 1,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), local_counters).await;
        write_counters(&ctx, usage_group_key(group_id), local_counters).await;

        let remote_snapshot = |counters| NodeUsageSnapshot {
            node_id: remote,
            counters,
        };
        let remote_counters = UsageCounters {
            logical_bytes: 5,
            objects: 1,
            ..Default::default()
        };
        write_node_stat(
            &ctx,
            node_usage_global_key(remote),
            remote_snapshot(remote_counters).to_bytes().unwrap(),
        )
        .await;
        write_node_stat(
            &ctx,
            node_usage_group_key(group_id, remote),
            remote_snapshot(remote_counters).to_bytes().unwrap(),
        )
        .await;
        // This node's own stale snapshot must be ignored in favor of live counters.
        write_node_stat(
            &ctx,
            node_usage_global_key(local),
            NodeUsageSnapshot {
                node_id: local,
                counters: UsageCounters {
                    logical_bytes: 999,
                    ..Default::default()
                },
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        recompute_realm_usage_summary(&ctx, local, true, vec![group_id])
            .await
            .unwrap();

        let global = load_realm_usage(&ctx, local, RealmUsageScope::Global)
            .await
            .unwrap();
        assert_eq!(global.logical_bytes, 15);
        assert_eq!(global.objects, 2);
        let group = load_realm_usage(&ctx, local, RealmUsageScope::Group(group_id))
            .await
            .unwrap();
        assert_eq!(group.logical_bytes, 15);
        assert_eq!(group.objects, 2);
    }

    #[tokio::test]
    async fn load_realm_usage_falls_back_to_on_the_fly_recompute() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);

        write_counters(
            &ctx,
            usage_global_shard_key(0),
            UsageCounters {
                logical_bytes: 3,
                ..Default::default()
            },
        )
        .await;
        write_node_stat(
            &ctx,
            node_usage_global_key(remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: UsageCounters {
                    logical_bytes: 4,
                    ..Default::default()
                },
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        // No summary/global cache entry exists, so the read recomputes on the fly.
        let global = load_realm_usage(&ctx, local, RealmUsageScope::Global)
            .await
            .unwrap();
        assert_eq!(global.logical_bytes, 7);
    }

    #[tokio::test]
    async fn rebuild_leaves_node_usage_keyspace_intact() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let remote = node(2);
        let snapshot_key = node_usage_global_key(remote);
        let snapshot = NodeUsageSnapshot {
            node_id: remote,
            counters: UsageCounters {
                logical_bytes: 42,
                ..Default::default()
            },
        };
        write_node_stat(&ctx, snapshot_key.clone(), snapshot.to_bytes().unwrap()).await;
        write_node_stat(&ctx, NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec(), Vec::new()).await;

        drive(RebuildUsageStatsOperation::new(), &ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(
            read_node_stat(&ctx, snapshot_key).await,
            Some(snapshot.to_bytes().unwrap())
        );
        assert!(
            read_node_stat(&ctx, NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn clear_consumed_markers_keeps_regenerated_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let generation_one = Ulid::new().to_bytes().to_vec();

        write_node_stat(
            &ctx,
            NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec(),
            generation_one.clone(),
        )
        .await;
        let observed = vec![(
            Key::from(NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()),
            Value::from(generation_one),
        )];

        // A concurrent counter update re-dirties the marker with a new generation
        // after it was observed but before it is cleared.
        let generation_two = Ulid::new().to_bytes().to_vec();
        write_node_stat(
            &ctx,
            NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec(),
            generation_two.clone(),
        )
        .await;

        clear_consumed_markers(&ctx.storage_handle, observed)
            .await
            .unwrap();

        // The re-dirtied marker survives, keeping the retry signal for the racing
        // write's counter change.
        assert_eq!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()).await,
            Some(generation_two.clone())
        );

        // Observing the current generation lets the marker be consumed.
        let observed = vec![(
            Key::from(NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()),
            Value::from(generation_two),
        )];
        clear_consumed_markers(&ctx.storage_handle, observed)
            .await
            .unwrap();
        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn publish_failure_preserves_dirty_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::new();
        let generation = Ulid::new().to_bytes().to_vec();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(
            &ctx,
            NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec(),
            generation.clone(),
        )
        .await;
        write_node_stat(&ctx, node_usage_dirty_group_key(group_id), generation).await;

        // Corrupt the realm config document so replication fails hard.
        let config_key = DocumentSyncTarget::RealmConfig { realm_id }.storage_key();
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
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn full_publish_replication_failure_marks_published_scopes_dirty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::new();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_none()
        );

        let config_key = DocumentSyncTarget::RealmConfig { realm_id }.storage_key();
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

        let result = publish_and_refresh_usage_snapshots(&ctx, node_id, realm_id, true).await;
        assert!(
            result.is_err(),
            "replication failure must surface as an error, got {result:?}"
        );

        assert!(
            read_node_stat(&ctx, node_usage_global_key(node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_group_key(group_id, node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn summary_refresh_failure_preserves_dirty_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let remote = node(2);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::new();
        let generation = Ulid::new().to_bytes().to_vec();

        let counters = UsageCounters {
            buckets: 1,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(
            &ctx,
            NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec(),
            generation.clone(),
        )
        .await;
        write_node_stat(
            &ctx,
            node_usage_dirty_group_key(group_id),
            generation.clone(),
        )
        .await;
        write_node_stat(&ctx, node_usage_global_key(remote), b"corrupt".to_vec()).await;

        let result = publish_and_refresh_usage_snapshots(&ctx, node_id, realm_id, false).await;
        assert!(
            result.is_err(),
            "summary recompute failure must surface as an error, got {result:?}"
        );

        // The snapshots were published, but the markers survive so the debounced
        // publisher retries and refreshes the realm summary cache later.
        assert!(
            read_node_stat(&ctx, node_usage_global_key(node_id))
                .await
                .is_some()
        );
        assert_eq!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec()).await,
            Some(generation.clone())
        );
        assert_eq!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id)).await,
            Some(generation)
        );
    }

    #[tokio::test]
    async fn full_publish_summary_refresh_failure_marks_published_scopes_dirty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let remote = node(2);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let group_id = Ulid::new();

        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), counters).await;
        write_counters(&ctx, usage_group_key(group_id), counters).await;
        write_node_stat(&ctx, node_usage_global_key(remote), b"corrupt".to_vec()).await;

        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_none()
        );

        let result = publish_and_refresh_usage_snapshots(&ctx, node_id, realm_id, true).await;
        assert!(
            result.is_err(),
            "summary recompute failure must surface as an error, got {result:?}"
        );

        // Full startup publishes can start with no dirty markers. Once snapshots
        // were published, a summary refresh failure must create retry markers for
        // every published scope so the debounced publisher can refresh later.
        assert!(
            read_node_stat(&ctx, node_usage_global_key(node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_group_key(group_id, node_id))
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, NODE_USAGE_DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_some()
        );
        assert!(
            read_node_stat(&ctx, node_usage_dirty_group_key(group_id))
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn full_publish_zeros_stale_group_snapshots() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(1);
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let live_group = Ulid::new();
        let stale_group = Ulid::new();

        // A live group still has a counter key.
        let counters = UsageCounters {
            buckets: 1,
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, usage_group_key(live_group), counters).await;

        // A group this node published before, whose counter key no longer exists
        // (e.g. pruned by a rebuild), leaves a stale snapshot behind.
        write_node_stat(
            &ctx,
            node_usage_group_key(stale_group, node_id),
            NodeUsageSnapshot {
                node_id,
                counters: UsageCounters {
                    buckets: 5,
                    logical_bytes: 500,
                    ..Default::default()
                },
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
            &read_node_stat(&ctx, node_usage_group_key(stale_group, node_id))
                .await
                .expect("stale snapshot present"),
        )
        .unwrap();
        assert_eq!(stale.counters, UsageCounters::default());

        // The live group keeps its real total.
        let live = NodeUsageSnapshot::from_bytes(
            &read_node_stat(&ctx, node_usage_group_key(live_group, node_id))
                .await
                .expect("live snapshot present"),
        )
        .unwrap();
        assert_eq!(live.counters.logical_bytes, 10);
    }

    #[tokio::test]
    async fn refresh_summary_from_targets_recomputes_touched_scopes() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let local = node(1);
        let remote = node(2);
        let group_id = Ulid::new();

        let local_counters = UsageCounters {
            logical_bytes: 10,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_key_for_group(group_id), local_counters).await;
        write_counters(&ctx, usage_group_key(group_id), local_counters).await;

        let remote_counters = UsageCounters {
            logical_bytes: 5,
            ..Default::default()
        };
        write_node_stat(
            &ctx,
            node_usage_global_key(remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: remote_counters,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
        write_node_stat(
            &ctx,
            node_usage_group_key(group_id, remote),
            NodeUsageSnapshot {
                node_id: remote,
                counters: remote_counters,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        refresh_realm_usage_summary_for_targets(
            &ctx,
            local,
            &[
                DocumentSyncTarget::NodeUsage {
                    realm_id: RealmId::from_bytes([9u8; 32]),
                    node_id: remote,
                    group_id: None,
                },
                DocumentSyncTarget::NodeUsage {
                    realm_id: RealmId::from_bytes([9u8; 32]),
                    node_id: remote,
                    group_id: Some(group_id),
                },
            ],
        )
        .await;

        // The summed cache was materialized for both touched scopes.
        assert_eq!(
            read_node_stat(&ctx, NODE_USAGE_SUMMARY_GLOBAL_KEY.to_vec())
                .await
                .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
            Some(15)
        );
        assert_eq!(
            read_node_stat(&ctx, node_usage_summary_group_key(group_id))
                .await
                .map(|bytes| UsageCounters::from_bytes(&bytes).unwrap().logical_bytes),
            Some(15)
        );
    }
}
