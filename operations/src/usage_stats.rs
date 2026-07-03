use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    USAGE_GLOBAL_KEY, USAGE_GLOBAL_SHARD_COUNT, UsageCounterError, UsageCounters, UsageDelta,
    VersionKey, usage_global_key_for_group, usage_global_shard_index, usage_global_shard_key,
    usage_global_shard_keys, usage_group_key,
};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

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
    phase: UsageUpdatePhase,
}

impl UsageCounterUpdate {
    pub fn for_group(group_id: GroupId, delta: UsageDelta) -> Self {
        Self {
            entries: vec![
                (usage_global_key_for_group(group_id), delta),
                (usage_group_key(group_id), delta),
            ],
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
            phase: UsageUpdatePhase::Pending,
        }
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
                let mut writes = Vec::with_capacity(self.entries.len());
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
            let mut reads = usage_global_shard_keys()
                .into_iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
                .collect::<Vec<_>>();
            reads.push((
                USAGE_STATS_KEYSPACE.to_string(),
                USAGE_GLOBAL_KEY.to_vec().into(),
            ));
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
    let mut saw_shard = false;
    let mut legacy = None;
    let shard_count = values.len().saturating_sub(1);

    for (index, (_, value)) in values.into_iter().enumerate() {
        let Some(bytes) = value else {
            continue;
        };
        let counters = UsageCounters::from_bytes(bytes.as_ref())?;
        if index < shard_count {
            saw_shard = true;
            total.add(&counters)?;
        } else {
            legacy = Some(counters);
        }
    }

    if saw_shard {
        Ok(total)
    } else {
        Ok(legacy.unwrap_or_default())
    }
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

                    let BlobVersionState::Materialized { blob_hash, .. } = version.state else {
                        continue;
                    };
                    let Some(size) = self.blob_sizes.get(blob_hash.as_slice()).copied() else {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::create_bucket::CreateBucketOperation;
    use aruna_core::structs::{
        BlobHeadKey, BucketInfo, CurrentVersionPointer, usage_global_shard_keys,
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
        let legacy = UsageCounters {
            buckets: 99,
            ..Default::default()
        };
        write_counters(&ctx, usage_global_shard_key(0), shard_zero).await;
        write_counters(&ctx, usage_global_shard_key(1), shard_one).await;
        write_counters(&ctx, USAGE_GLOBAL_KEY.to_vec(), legacy).await;

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

        let legacy_temp = tempdir().unwrap();
        let legacy_ctx = test_ctx(legacy_temp.path().to_str().unwrap());
        write_counters(&legacy_ctx, USAGE_GLOBAL_KEY.to_vec(), legacy).await;
        let loaded = drive(
            LoadUsageCountersOperation::new(USAGE_GLOBAL_KEY.to_vec()),
            &legacy_ctx,
        )
        .await
        .unwrap();
        assert_eq!(loaded, legacy);
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

        write_head("alpha", "live.txt", alpha_live_head).await;
        write_head("alpha", "gone.txt", alpha_gone_head).await;
        write_head("beta", "shared.bin", beta_head).await;

        ctx.storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let global = drive(RebuildUsageStatsOperation::new(), &ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // live.txt and shared.bin are live; gone.txt ends on a delete marker.
        assert_eq!(global.buckets, 2);
        assert_eq!(global.objects, 2);
        assert_eq!(global.stored_blobs, 2);
        assert_eq!(global.stored_bytes, 140);
        // 100 + 40 + 40 + 40: every materialized version counts logically.
        assert_eq!(global.logical_bytes, 220);

        let stored_global = read_global_counters(&ctx).await;
        assert_eq!(stored_global, global);

        let alpha = read_counters(&ctx, usage_group_key(group_a)).await;
        assert_eq!(alpha.buckets, 1);
        assert_eq!(alpha.objects, 1);
        assert_eq!(alpha.logical_bytes, 140);

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
}
