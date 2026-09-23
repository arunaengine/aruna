//! Drains the materialization queue: applies pending events, retries and dead-letters.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    DEAD_LETTER_KEYSPACE, DOCUMENT_JOB_KEYSPACE, EVENT_LOG_KEYSPACE, MATERIALIZATION_JOB_KEYSPACE,
    MATERIALIZATION_PRUNE_KEYSPACE, MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    ApplyRoCrateRequest, DeadLetterRecord, MaterializationState, MaterializationStatusRecord,
    MetadataBatch, MetadataCrateRequest, MetadataEffect, MetadataError, MetadataEvent,
    MetadataEventPayload, MetadataEventRecord, MetadataGraphPolicy, MetadataMaterializationRecord,
    MetadataRawRevision, MetadataRequestDurability, deterministic_materialization_actor,
};
use aruna_core::storage_entries::{
    dead_letter_entry, dead_letter_key, document_job_entry, document_job_key, document_job_prefix,
    event_log_key, materialization_job_entry, materialization_job_key, materialization_prune_entry,
    materialization_prune_key, materialization_status_entry, materialization_status_key,
    profile_validation_entry,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;

use crate::tasks::queue_backoff::{due_after, retry_delay_ms};

use super::iri_index::MetadataIriError;
use super::profile::validation::{assess_render, violation_count};
use super::queue_storage::{
    MetadataQueueError, abort_storage_transaction, commit_storage_transaction,
    start_write_transaction,
};
use super::raw_revision::{RawReadError, RawStateCache};
use super::repository::{
    StorageReadError, parse_lifecycle_read, parse_status_read, read_lifecycle_effect,
    read_status_effect,
};

const MATERIALIZATION_PAGE_SIZE: usize = 512;
const MATERIALIZATION_BATCH_SIZE: usize = 512;
// Only application failures count here: infrastructure errors retry forever with
// backoff, so an overloaded node never parks work it could still finish.
const MATERIALIZATION_MAX_FAILURES: u32 = 10;
// Parked jobs return to the queue on this backoff, doubling per park up to the
// cap, so a node recovers on its own once the cause clears.
const REQUEUE_BASE_MS: u64 = 60_000;
const REQUEUE_MAX_MS: u64 = 3_600_000;
const REQUEUE_PAGE_SIZE: usize = 256;
// Jobs per finish transaction. Small enough that a failed commit costs little,
// large enough that a full batch commits in a handful of transactions.
const MATERIALIZATION_FINISH_CHUNK: usize = 256;
const GRAPH_FENCE_SHARDS: usize = 32;

// Craqle already serializes same-graph writes internally. This bounded fence
// extends that ordering over the lifecycle read performed by the Aruna handle.
static METADATA_GRAPH_FENCES: LazyLock<[Semaphore; GRAPH_FENCE_SHARDS]> =
    LazyLock::new(|| std::array::from_fn(|_| Semaphore::new(1)));

pub(crate) fn metadata_graph_fence(graph_iri: &str) -> &'static Semaphore {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    let shard = u64::from_be_bytes(prefix) as usize;
    &METADATA_GRAPH_FENCES[shard % GRAPH_FENCE_SHARDS]
}

pub const MATERIALIZATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const MATERIALIZATION_RETRY_AFTER: Duration = Duration::from_secs(1);
// The bulk storage lane enforces fairness when immediate timer refires bypass this gap.
pub const NEXT_BATCH_AFTER: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub struct MetadataDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug)]
struct CompletedMaterializationJob {
    job_key: Vec<u8>,
    document_job_key: Option<Vec<u8>>,
    status: Option<MaterializationStatusRecord>,
    iri_index_writes: Vec<(String, ByteView, ByteView)>,
    raw_state_write: Option<(String, ByteView, ByteView)>,
    validation_write: Option<(String, ByteView, ByteView)>,
    sync: Option<CompletedMaterializationSync>,
}

#[derive(Debug, Clone)]
struct CompletedMaterializationSync {
    graph_iri: String,
    peers: Vec<NodeId>,
}

/// The outcome of one job attempt, resolved together in the per-batch finish
/// transaction so a batch of failures costs one transaction, not one each.
#[derive(Debug)]
enum FinishedMaterializationJob {
    Completed(CompletedMaterializationJob),
    Rescheduled {
        job_key: Vec<u8>,
        job: MetadataMaterializationRecord,
        status: MaterializationStatusRecord,
    },
    Parked {
        job_key: Vec<u8>,
        job: MetadataMaterializationRecord,
        status: MaterializationStatusRecord,
    },
}

#[derive(Debug, Default)]
struct MaterializationGroupOutcome {
    finished: Vec<FinishedMaterializationJob>,
    processed: usize,
    craqle_elapsed: Duration,
    error: Option<MetadataMaterializationError>,
}

#[derive(Debug, Default)]
struct MaterializationBatchTimings {
    processed: usize,
    groups: usize,
    craqle_elapsed: Duration,
    finish_elapsed: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationJobObsolescence {
    Live,
    Final,
    RetryAdvanced,
}

#[derive(Debug, Error)]
pub enum MetadataMaterializationError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("metadata handle missing")]
    MetadataHandleMissing,
    #[error("metadata create event log record not found for {document_id}/{event_id}")]
    CreateEventMissing { document_id: Ulid, event_id: Ulid },
    #[error("unexpected event while processing metadata materialization queue: {0}")]
    UnexpectedEvent(String),
    #[error("inconsistent metadata raw event log: {0}")]
    InconsistentLog(String),
}

impl From<MetadataQueueError> for MetadataMaterializationError {
    fn from(error: MetadataQueueError) -> Self {
        match error {
            MetadataQueueError::Storage(error) => Self::Storage(error),
            MetadataQueueError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
        }
    }
}

impl From<RawReadError> for MetadataMaterializationError {
    fn from(error: RawReadError) -> Self {
        match error {
            RawReadError::Storage(error) => Self::Storage(error),
            RawReadError::Conversion(error) => Self::Conversion(error),
            RawReadError::Metadata(error) => Self::Metadata(error),
            RawReadError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
            RawReadError::LimitExceeded(message) => Self::UnexpectedEvent(message),
            RawReadError::InconsistentLog(message) => Self::InconsistentLog(message),
        }
    }
}

impl From<MetadataIriError> for MetadataMaterializationError {
    fn from(error: MetadataIriError) -> Self {
        match error {
            MetadataIriError::Storage(error) => Self::Storage(error),
            MetadataIriError::Conversion(error) => Self::Conversion(error),
            MetadataIriError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
        }
    }
}

pub fn schedule_materialization() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainMaterializationQueue,
        after: Duration::ZERO,
    })
}

pub fn new_materialization_job(
    event: &MetadataEventRecord,
    due_at_ms: u64,
) -> MetadataMaterializationRecord {
    MetadataMaterializationRecord::new(event, due_at_ms)
}

pub fn new_pending_status(
    event: &MetadataEventRecord,
    updated_at_ms: u64,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord::pending(event, updated_at_ms)
}

pub async fn restore_materialization_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_timer_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainMaterializationQueue,
                    after,
                }))
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore metadata materialization timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan metadata materialization jobs"),
    }
}

pub async fn next_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, MetadataMaterializationError> {
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_ms) = scan_due_jobs(storage, now_ms, 1).await?;
    if !jobs.is_empty() || has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms)))
}

pub async fn process_materialization_batch(
    context: &DriverContext,
) -> Result<MetadataDrainResult, MetadataMaterializationError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_ms) =
        scan_due_jobs(&context.storage_handle, now_ms, MATERIALIZATION_BATCH_SIZE).await?;
    let scan_elapsed = batch_started.elapsed();
    let job_count = jobs.len();
    let oldest_lag_ms = jobs
        .iter()
        .map(|(_, job)| now_ms.saturating_sub(job.due_at_ms))
        .max()
        .unwrap_or(0);
    let timings = process_job_groups(context, jobs).await?;
    // Finished jobs may queue link pushes, and the link drain stops once its queue is empty.
    if timings.processed > 0
        && let Some(task_handle) = &context.task_handle
    {
        crate::jobs::invenio::link_queue::restore_link_timer(&context.storage_handle, task_handle)
            .await;
    }
    if job_count > 0 {
        info!(
            event = "pipeline.materialization.summary",
            jobs = job_count,
            processed = timings.processed,
            groups = timings.groups,
            scan_ms = duration_ms(scan_elapsed),
            craqle_apply_ms = duration_ms(timings.craqle_elapsed),
            finish_ms = duration_ms(timings.finish_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Metadata materialization batch summary"
        );
    }
    Ok(MetadataDrainResult {
        processed: timings.processed,
        has_more_due,
        next_due_after: if has_more_due {
            Some(Duration::ZERO)
        } else {
            next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

// Half-core concurrency reserves capacity for foreground create and validation traffic.
fn materialization_group_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|cores| cores.get())
        .unwrap_or(4)
        .div_ceil(2)
        .max(1)
}

fn collect_group_outcome(
    result: Result<MaterializationGroupOutcome, tokio::task::JoinError>,
    finished: &mut Vec<FinishedMaterializationJob>,
    timings: &mut MaterializationBatchTimings,
    first_error: &mut Option<MetadataMaterializationError>,
) {
    match result {
        Ok(outcome) => {
            timings.processed = timings.processed.saturating_add(outcome.processed);
            timings.craqle_elapsed = timings
                .craqle_elapsed
                .saturating_add(outcome.craqle_elapsed);
            finished.extend(outcome.finished);
            if first_error.is_none() {
                *first_error = outcome.error;
            }
        }
        Err(error) => {
            if first_error.is_none() {
                *first_error = Some(MetadataMaterializationError::UnexpectedEvent(
                    error.to_string(),
                ));
            }
        }
    }
}

async fn process_job_groups(
    context: &DriverContext,
    jobs: Vec<(Vec<u8>, MetadataMaterializationRecord)>,
) -> Result<MaterializationBatchTimings, MetadataMaterializationError> {
    let mut groups: BTreeMap<Ulid, Vec<(Vec<u8>, MetadataMaterializationRecord)>> = BTreeMap::new();
    for (job_key, job) in jobs {
        groups
            .entry(job.document_id)
            .or_default()
            .push((job_key, job));
    }

    let concurrency = materialization_group_concurrency();
    let mut tasks = JoinSet::new();
    let mut finished = Vec::new();
    let mut timings = MaterializationBatchTimings {
        groups: groups.len(),
        ..MaterializationBatchTimings::default()
    };
    let mut first_error = None;
    for (_, jobs) in groups {
        let mut jobs = jobs;
        jobs.sort_by_key(|(_, job)| job.event_id);
        if tasks.len() >= concurrency
            && let Some(result) = tasks.join_next().await
        {
            collect_group_outcome(result, &mut finished, &mut timings, &mut first_error);
        }
        let context = context.clone();
        tasks.spawn(async move {
            let mut outcome = MaterializationGroupOutcome::default();
            let mut advanced_event_ids = BTreeSet::new();
            let mut raw_state_cache = RawStateCache::default();
            let Some(document_id) = jobs.first().map(|(_, job)| job.document_id) else {
                return outcome;
            };
            let group = match load_group_jobs(&context.storage_handle, document_id).await {
                Ok(group) => group,
                Err(error) => {
                    outcome.error = Some(error);
                    return outcome;
                }
            };
            for (job_key, job) in jobs {
                let event_id = job.event_id;
                match process_materialization_job(
                    &context,
                    job_key,
                    job,
                    &group,
                    &advanced_event_ids,
                    &mut raw_state_cache,
                )
                .await
                {
                    Ok(processed_job) => {
                        outcome.craqle_elapsed = outcome
                            .craqle_elapsed
                            .saturating_add(processed_job.craqle_elapsed);
                        if processed_job.attempted {
                            outcome.processed = outcome.processed.saturating_add(1);
                        }
                        if let Some(finished) = processed_job.finished {
                            if matches!(finished, FinishedMaterializationJob::Completed(_)) {
                                advanced_event_ids.insert(event_id);
                            }
                            outcome.finished.push(finished);
                        }
                        if processed_job.stop_group {
                            break;
                        }
                    }
                    Err(error) => {
                        outcome.error = Some(error);
                        break;
                    }
                }
            }
            outcome
        });
    }

    while let Some(result) = tasks.join_next().await {
        collect_group_outcome(result, &mut finished, &mut timings, &mut first_error);
    }
    let finish_started = Instant::now();
    let syncs = dedupe_graph_syncs(&finished);
    if let Err(error) = finish_completed_jobs(&context.storage_handle, finished).await
        && first_error.is_none()
    {
        first_error = Some(error);
    }
    timings.finish_elapsed = finish_started.elapsed();
    // Schedule syncs for committed chunks because their deleted job rows cannot retry them.
    schedule_completed_syncs(context, syncs).await;
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(timings)
}

// One SyncGraphBestEffort per graph, keeping the last peers seen, so a batch
// carrying many events for one document schedules a single sync.
fn dedupe_graph_syncs(
    finished: &[FinishedMaterializationJob],
) -> Vec<CompletedMaterializationSync> {
    let mut by_graph: BTreeMap<String, CompletedMaterializationSync> = BTreeMap::new();
    for job in finished {
        if let FinishedMaterializationJob::Completed(job) = job
            && let Some(sync) = &job.sync
        {
            by_graph.insert(sync.graph_iri.clone(), sync.clone());
        }
    }
    by_graph.into_values().collect()
}

async fn schedule_completed_syncs(
    context: &DriverContext,
    syncs: Vec<CompletedMaterializationSync>,
) {
    if syncs.is_empty() {
        return;
    }
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return;
    };
    for sync in syncs {
        match metadata_handle
            .send_effect(Effect::Metadata(MetadataEffect::SyncBestEffort {
                graph_iri: sync.graph_iri.clone(),
                peers: sync.peers,
            }))
            .await
        {
            Event::Metadata(MetadataEvent::GraphSyncScheduled { .. }) => {}
            Event::Metadata(MetadataEvent::Error { error, .. }) => {
                warn!(error = ?error, graph_iri = %sync.graph_iri, "Failed to schedule metadata graph sync after materialization");
            }
            other => {
                warn!(event = ?other, graph_iri = %sync.graph_iri, "Unexpected metadata graph sync result after materialization");
            }
        }
    }
}

/// Row changes for one finish chunk, resolved before the transaction opens.
#[derive(Debug, Default)]
struct FinishPlan {
    writes: Vec<(String, ByteView, ByteView)>,
    deletes: Vec<(String, ByteView)>,
    /// Documents whose winning cursor advanced; their prior index rows are
    /// pruned once per batch rather than once per chunk.
    superseding: HashMap<Ulid, Ulid>,
}

// Chunked so a failure costs one chunk instead of the whole batch, and the craqle work
// behind the committed chunks survives.
async fn finish_completed_jobs(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
) -> Result<(), MetadataMaterializationError> {
    let mut superseding = HashMap::new();
    let finish = finish_chunks(storage, finished, &mut superseding).await;
    let prune = prune_superseded_rows(storage, superseding).await;
    finish.and(prune)
}

async fn finish_chunks(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
    superseding: &mut HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationError> {
    let mut chunk = Vec::with_capacity(MATERIALIZATION_FINISH_CHUNK);
    for job in finished {
        chunk.push(job);
        if chunk.len() >= MATERIALIZATION_FINISH_CHUNK {
            let taken = std::mem::take(&mut chunk);
            superseding.extend(finish_chunk(storage, taken).await?);
            chunk.reserve(MATERIALIZATION_FINISH_CHUNK);
        }
    }
    if !chunk.is_empty() {
        superseding.extend(finish_chunk(storage, chunk).await?);
    }
    Ok(())
}

// The IRI index cannot be scanned per document, so this walks the whole keyspace once per
// batch.
async fn prune_superseded_rows(
    storage: &StorageHandle,
    mut superseding: HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationError> {
    let pending = read_pending_prunes(storage).await?;
    for (document_id, cursor) in pending.iter() {
        superseding.entry(*document_id).or_insert(*cursor);
    }
    if superseding.is_empty() {
        return Ok(());
    }
    match prune_index_rows(storage, &superseding).await {
        Ok(()) if pending.is_empty() => Ok(()),
        Ok(()) => delete_pending_prunes(storage, pending.keys().copied().collect()).await,
        Err(error) => {
            warn!(
                error = %error,
                documents = superseding.len(),
                "Deferring metadata materialization index pruning"
            );
            persist_pending_prunes(storage, &superseding).await
        }
    }
}

async fn prune_index_rows(
    storage: &StorageHandle,
    superseding: &HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationError> {
    let stale = super::iri_index::superseded_keys(storage, None, superseding).await?;
    delete_materialization_entries(storage, stale).await
}

// One page per batch: leftovers stay parked for the next drain, so a long
// backlog cannot make a single batch scan unboundedly.
async fn read_pending_prunes(
    storage: &StorageHandle,
) -> Result<HashMap<Ulid, Ulid>, MetadataMaterializationError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: MATERIALIZATION_PRUNE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: MATERIALIZATION_PAGE_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => {
            let mut pending = HashMap::new();
            for (key, value) in values {
                let (Ok(document_id), Ok(cursor)) = (
                    <[u8; 16]>::try_from(key.as_ref()),
                    postcard::from_bytes::<Ulid>(&value),
                ) else {
                    warn!(key = ?key.to_vec(), "Deleting malformed metadata materialization prune entry");
                    delete_materialization_entries(
                        storage,
                        vec![(MATERIALIZATION_PRUNE_KEYSPACE.to_string(), key.clone())],
                    )
                    .await?;
                    continue;
                };
                pending.insert(Ulid::from_bytes(document_id), cursor);
            }
            Ok(pending)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn persist_pending_prunes(
    storage: &StorageHandle,
    superseding: &HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationError> {
    let mut writes = Vec::with_capacity(superseding.len());
    for (document_id, cursor) in superseding {
        writes.push(materialization_prune_entry(*document_id, *cursor)?);
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_pending_prunes(
    storage: &StorageHandle,
    document_ids: Vec<Ulid>,
) -> Result<(), MetadataMaterializationError> {
    let deletes = document_ids
        .into_iter()
        .map(|document_id| {
            (
                MATERIALIZATION_PRUNE_KEYSPACE.to_string(),
                materialization_prune_key(document_id),
            )
        })
        .collect();
    delete_materialization_entries(storage, deletes).await
}

/// Returns the documents whose projection this chunk advanced, so the batch can
/// prune their prior-cursor index rows once.
async fn finish_chunk(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
) -> Result<HashMap<Ulid, Ulid>, MetadataMaterializationError> {
    let plan = plan_finish_chunk(storage, finished).await?;
    if plan.writes.is_empty() && plan.deletes.is_empty() {
        return Ok(plan.superseding);
    }
    let txn_id = start_write_transaction(storage).await?;
    let result = async {
        transactional_batch_write(storage, txn_id, plan.writes).await?;
        transactional_batch_delete(storage, txn_id, plan.deletes).await
    }
    .await;
    match result {
        Ok(()) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(plan.superseding)
        }
        Err(error) => {
            abort_storage_transaction(
                storage,
                txn_id,
                "Failed to abort materialization storage transaction",
                "Unexpected materialization storage transaction abort result",
            )
            .await;
            Err(error)
        }
    }
}

// Guard reads precede the transaction so concurrent changes enter its read set.
async fn plan_finish_chunk(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
) -> Result<FinishPlan, MetadataMaterializationError> {
    let snapshot =
        read_status_map(storage, finished.iter().map(finished_document_id).collect()).await?;
    let parked = read_dead_letters(storage, &finished).await?;
    let mut plan = FinishPlan {
        deletes: Vec::with_capacity(finished.len().saturating_mul(2)),
        ..FinishPlan::default()
    };
    let mut planned: HashMap<Ulid, MaterializationStatusRecord> = HashMap::new();
    let mut superseding: HashMap<Ulid, Ulid> = HashMap::new();
    for finished in finished {
        match finished {
            FinishedMaterializationJob::Completed(job) => {
                if let Some(status) = job.status {
                    let current = guard_status(&snapshot, &planned, status.document_id);
                    if should_write_final(current, &status) {
                        superseding.insert(status.document_id, status.event_id);
                        plan.writes.extend(job.iri_index_writes);
                        if let Some(raw_state_write) = job.raw_state_write {
                            plan.writes.push(raw_state_write);
                        }
                        if let Some(validation_write) = job.validation_write {
                            plan.writes.push(validation_write);
                        }
                        plan.writes.push(materialization_status_entry(&status)?);
                        planned.insert(status.document_id, status);
                    }
                }
                plan.deletes.push((
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job.job_key),
                ));
                if let Some(document_job_key) = job.document_job_key {
                    plan.deletes.push((
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        ByteView::from(document_job_key),
                    ));
                }
            }
            FinishedMaterializationJob::Rescheduled {
                job_key,
                job,
                status,
            } => {
                let old_index_delete = (
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job_key),
                );
                let current = guard_status(&snapshot, &planned, job.document_id);
                if current.is_some_and(|current| retry_already_advanced(current, &job)) {
                    plan.deletes.push(old_index_delete);
                    plan.deletes.push((
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        document_job_key(job.document_id, job.event_id),
                    ));
                    continue;
                }
                let attempts = job.attempts.saturating_add(1);
                let next_job = MetadataMaterializationRecord {
                    document_id: job.document_id,
                    event_id: job.event_id,
                    due_at_ms: unix_timestamp_millis().saturating_add(retry_delay_ms(attempts)),
                    attempts,
                    failures: status.failures,
                    parks: job.parks,
                };
                if should_write_retry(current, &status) {
                    plan.writes.push(materialization_status_entry(&status)?);
                    planned.insert(status.document_id, status);
                }
                plan.writes.push(materialization_job_entry(&next_job)?);
                plan.writes.push(document_job_entry(&next_job)?);
                plan.deletes.push(old_index_delete);
            }
            FinishedMaterializationJob::Parked {
                job_key,
                job,
                status,
            } => {
                let current = guard_status(&snapshot, &planned, job.document_id);
                let job_deletes = [
                    (
                        MATERIALIZATION_JOB_KEYSPACE.to_string(),
                        ByteView::from(job_key),
                    ),
                    (
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        document_job_key(job.document_id, job.event_id),
                    ),
                ];
                // An already-superseded job must not leave a dead letter behind:
                // requeueing it later would resurrect an obsolete event.
                if current.is_some_and(|current| retry_already_advanced(current, &job)) {
                    plan.deletes.extend(job_deletes);
                    continue;
                }
                if should_write_final(current, &status) {
                    plan.writes.push(materialization_status_entry(&status)?);
                }
                let previous = parked.get(&(job.document_id, job.event_id));
                let dead_letter = parked_dead_letter(&job, &status, previous);
                plan.writes.push(dead_letter_entry(&dead_letter)?);
                plan.deletes.extend(job_deletes);
                warn!(
                    event = "materialization.job.parked",
                    document_id = %job.document_id,
                    event_id = %job.event_id,
                    attempts = status.attempts,
                    failures = status.failures,
                    requeue_at_ms = dead_letter.requeue_at_ms,
                    error = status.last_error.as_deref().unwrap_or_default(),
                    "Parked metadata materialization job as dead letter"
                );
                planned.insert(status.document_id, status);
            }
        }
    }

    // Linked repositories learn of the change in the same commit as the new status.
    match crate::jobs::invenio::link_queue::queue_rows(storage, superseding.keys().copied()).await {
        Ok(rows) => plan.writes.extend(rows),
        Err(error) => warn!(%error, "Failed to queue Invenio link pushes after materialization"),
    }
    plan.superseding = superseding;
    Ok(plan)
}

// A status planned earlier in this chunk is newer than the snapshot, so it is
// what later jobs of the same document must be judged against.
fn guard_status<'a>(
    snapshot: &'a HashMap<Ulid, MaterializationStatusRecord>,
    planned: &'a HashMap<Ulid, MaterializationStatusRecord>,
    document_id: Ulid,
) -> Option<&'a MaterializationStatusRecord> {
    planned
        .get(&document_id)
        .or_else(|| snapshot.get(&document_id))
}

fn finished_document_id(finished: &FinishedMaterializationJob) -> Ulid {
    match finished {
        FinishedMaterializationJob::Completed(job) => job
            .status
            .as_ref()
            .map(|status| status.document_id)
            .unwrap_or_else(|| {
                job_key_target(&job.job_key)
                    .map(|(document_id, _)| document_id)
                    .unwrap_or_else(Ulid::nil)
            }),
        FinishedMaterializationJob::Rescheduled { job, .. }
        | FinishedMaterializationJob::Parked { job, .. } => job.document_id,
    }
}

async fn read_status_map(
    storage: &StorageHandle,
    document_ids: BTreeSet<Ulid>,
) -> Result<HashMap<Ulid, MaterializationStatusRecord>, MetadataMaterializationError> {
    if document_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let reads = document_ids
        .iter()
        .map(|document_id| {
            (
                MATERIALIZATION_STATUS_KEYSPACE.to_string(),
                materialization_status_key(*document_id),
            )
        })
        .collect();
    let values = batch_read_values(storage, reads).await?;
    let mut statuses = HashMap::new();
    for (document_id, value) in document_ids.into_iter().zip(values) {
        if let Some(value) = value {
            statuses.insert(
                document_id,
                postcard::from_bytes(&value).map_err(ConversionError::from)?,
            );
        }
    }
    Ok(statuses)
}

async fn read_dead_letters(
    storage: &StorageHandle,
    finished: &[FinishedMaterializationJob],
) -> Result<HashMap<(Ulid, Ulid), DeadLetterRecord>, MetadataMaterializationError> {
    let targets: BTreeSet<(Ulid, Ulid)> = finished
        .iter()
        .filter_map(|finished| match finished {
            FinishedMaterializationJob::Parked { job, .. } => Some((job.document_id, job.event_id)),
            _ => None,
        })
        .collect();
    if targets.is_empty() {
        return Ok(HashMap::new());
    }
    let reads = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                DEAD_LETTER_KEYSPACE.to_string(),
                dead_letter_key(*document_id, *event_id),
            )
        })
        .collect();
    let values = batch_read_values(storage, reads).await?;
    let mut dead_letters = HashMap::new();
    for (target, value) in targets.into_iter().zip(values) {
        if let Some(record) = value.and_then(|value| postcard::from_bytes(&value).ok()) {
            dead_letters.insert(target, record);
        }
    }
    Ok(dead_letters)
}

async fn batch_read_values(
    storage: &StorageHandle,
    reads: Vec<(String, ByteView)>,
) -> Result<Vec<Option<ByteView>>, MetadataMaterializationError> {
    match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

// A re-parked job keeps its park count so its requeue backoff keeps growing instead of
// restarting at the base delay.
fn parked_dead_letter(
    job: &MetadataMaterializationRecord,
    status: &MaterializationStatusRecord,
    previous: Option<&DeadLetterRecord>,
) -> DeadLetterRecord {
    let parks = previous
        .map_or(job.parks, |previous| previous.parks.max(job.parks))
        .saturating_add(1);
    let now_ms = unix_timestamp_millis();
    DeadLetterRecord {
        job: job.clone(),
        last_error: status.last_error.clone().unwrap_or_default(),
        parked_at_ms: now_ms,
        parks,
        requeue_at_ms: now_ms.saturating_add(requeue_after_ms(parks)),
    }
}

fn requeue_after_ms(parks: u32) -> u64 {
    crate::tasks::queue_backoff::retry_after_ms(
        parks.saturating_sub(1),
        REQUEUE_BASE_MS,
        REQUEUE_MAX_MS,
    )
}

#[cfg(test)]
async fn read_dead_letter(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<DeadLetterRecord>, MetadataMaterializationError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: DEAD_LETTER_KEYSPACE.to_string(),
            key: dead_letter_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(postcard::from_bytes(&value).ok()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

/// Returns due dead letters to the queue and clears their terminal status, so a
/// node converges once the cause clears. Dead letters the document has moved
/// past are dropped instead. Returns the number of jobs requeued.
pub async fn requeue_dead_letters(
    storage: &StorageHandle,
) -> Result<usize, MetadataMaterializationError> {
    let now_ms = unix_timestamp_millis();
    let mut start_after = None;
    let mut requeued = 0usize;
    loop {
        let (values, next_start_after) = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: DEAD_LETTER_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REQUEUE_PAGE_SIZE,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            let Ok(dead_letter) = postcard::from_bytes::<DeadLetterRecord>(&value) else {
                warn!(key = ?key.to_vec(), "Deleting malformed metadata materialization dead letter");
                delete_dead_letter(storage, key.to_vec()).await?;
                continue;
            };
            if dead_letter.requeue_at_ms > now_ms {
                continue;
            }
            match requeue_dead_letter(storage, &dead_letter).await {
                Ok(true) => requeued = requeued.saturating_add(1),
                Ok(false) => {}
                // A racing finish aborts this requeue but must not stop the remaining sweep.
                Err(MetadataMaterializationError::Storage(StorageError::TransactionConflict)) => {
                    debug!(
                        event = "materialization.deadletter.contended",
                        document_id = %dead_letter.job.document_id,
                        event_id = %dead_letter.job.event_id,
                        "Deferring contended metadata materialization dead letter"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    if requeued > 0 {
        info!(
            event = "materialization.deadletter.requeued",
            jobs = requeued,
            "Requeued parked metadata materialization jobs"
        );
    }
    Ok(requeued)
}

// The parked job's own status is Failed at its own event, so only a final status
// beyond that means the document moved on and requeueing would regress it.
fn dead_letter_superseded(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status_obsoletes_job(status, job)
        && (status.event_id > job.event_id || status.state == MaterializationState::Materialized)
}

// The parked status is terminal for this event, so it must be cleared with the job rows or
// the requeued job is pruned as obsolete on the next scan.
async fn requeue_dead_letter(
    storage: &StorageHandle,
    dead_letter: &DeadLetterRecord,
) -> Result<bool, MetadataMaterializationError> {
    let job = MetadataMaterializationRecord {
        document_id: dead_letter.job.document_id,
        event_id: dead_letter.job.event_id,
        due_at_ms: unix_timestamp_millis(),
        attempts: 0,
        failures: MATERIALIZATION_MAX_FAILURES.saturating_sub(1),
        parks: dead_letter.parks,
    };
    let event = match read_create_event(storage, job.document_id, job.event_id).await {
        Ok(event) => event,
        Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
            delete_dead_letter(
                storage,
                dead_letter_key(job.document_id, job.event_id).to_vec(),
            )
            .await?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let status = MaterializationStatusRecord {
        failures: job.failures,
        ..new_pending_status(&event, unix_timestamp_millis())
    };
    let txn_id = start_write_transaction(storage).await?;
    let result = requeue_in_txn(storage, txn_id, &job, &status).await;
    match result {
        Ok(requeued) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(requeued)
        }
        Err(error) => {
            abort_storage_transaction(
                storage,
                txn_id,
                "Failed to abort materialization dead letter requeue",
                "Unexpected materialization dead letter requeue abort result",
            )
            .await;
            Err(error)
        }
    }
}

// The transactional status guard conflicts with a newer event instead of restoring stale work.
async fn requeue_in_txn(
    storage: &StorageHandle,
    txn_id: Ulid,
    job: &MetadataMaterializationRecord,
    status: &MaterializationStatusRecord,
) -> Result<bool, MetadataMaterializationError> {
    let dead_letter_delete = vec![(
        DEAD_LETTER_KEYSPACE.to_string(),
        dead_letter_key(job.document_id, job.event_id),
    )];
    if let Some(current) =
        read_materialization_status(storage, job.document_id, Some(txn_id)).await?
        && dead_letter_superseded(&current, job)
    {
        info!(
            event = "materialization.deadletter.superseded",
            document_id = %job.document_id,
            event_id = %job.event_id,
            status_event_id = %current.event_id,
            "Dropping superseded metadata materialization dead letter"
        );
        transactional_batch_delete(storage, txn_id, dead_letter_delete).await?;
        return Ok(false);
    }
    transactional_batch_write(
        storage,
        txn_id,
        vec![
            materialization_status_entry(status)?,
            materialization_job_entry(job)?,
            document_job_entry(job)?,
        ],
    )
    .await?;
    transactional_batch_delete(storage, txn_id, dead_letter_delete).await?;
    Ok(true)
}

async fn delete_dead_letter(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationError> {
    delete_materialization_entries(
        storage,
        vec![(DEAD_LETTER_KEYSPACE.to_string(), ByteView::from(key))],
    )
    .await
}

async fn transactional_batch_write(
    storage: &StorageHandle,
    txn_id: Ulid,
    writes: Vec<(String, ByteView, ByteView)>,
) -> Result<(), MetadataMaterializationError> {
    if writes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn transactional_batch_delete(
    storage: &StorageHandle,
    txn_id: Ulid,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataMaterializationError> {
    if deletes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn enqueue_job(
    context: &DriverContext,
    event: &MetadataEventRecord,
) -> Result<(), MetadataMaterializationError> {
    let now = unix_timestamp_millis();
    let status = new_pending_status(event, now);
    let job = new_materialization_job(event, now);
    write_status_job(&context.storage_handle, &status, &job).await?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        match task_handle.send_effect(schedule_materialization()).await {
            Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
                return Err(MetadataMaterializationError::UnexpectedEvent(message));
            }
            other => {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
    }
    Ok(())
}

async fn read_document_job(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<MetadataMaterializationRecord>, MetadataMaterializationError> {
    let key = document_job_key(document_id, event_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: DOCUMENT_JOB_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match postcard::from_bytes::<MetadataMaterializationRecord>(&value) {
            Ok(job) => Ok(Some(job)),
            Err(error) => {
                warn!(error = %error, document_id = %document_id, event_id = %event_id, "Deleting malformed metadata materialization document job");
                delete_job(storage, key.to_vec()).await?;
                Ok(None)
            }
        },
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

// The due index is due-ordered and a row is valid only when the sidecar row matches its due
// time.
async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<
    (
        Vec<(Vec<u8>, MetadataMaterializationRecord)>,
        bool,
        Option<u64>,
    ),
    MetadataMaterializationError,
> {
    let mut start_after = None;
    let mut jobs = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: MATERIALIZATION_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        let mut stale = Vec::new();
        let mut candidates = Vec::new();
        let mut next_due_ms = None;
        for (key, _value) in values {
            let key = key.to_vec();
            let Some((due_at_ms, document_id, event_id)) = job_key_parts(&key) else {
                warn!(key = ?key, "Deleting malformed metadata materialization index row");
                stale.push((
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(key),
                ));
                continue;
            };
            if due_at_ms > now_ms {
                next_due_ms = Some(due_at_ms);
                break;
            }
            candidates.push((key, due_at_ms, document_id, event_id));
        }

        // Resolved in slices of the outstanding limit so a probe for a single
        // due job does not read a whole page of sidecars, events and statuses.
        let mut cursor = 0usize;
        while cursor < candidates.len() {
            let remaining = limit.saturating_sub(jobs.len()).max(1);
            let end = candidates.len().min(cursor.saturating_add(remaining));
            let (live, dead) = resolve_due_jobs(storage, &candidates[cursor..end]).await?;
            cursor = end;
            stale.extend(dead);
            for (_, job) in live {
                jobs.push((materialization_job_key(&job).to_vec(), job));
                if jobs.len() >= limit {
                    delete_materialization_entries(storage, stale).await?;
                    return Ok((jobs, true, None));
                }
            }
        }
        delete_materialization_entries(storage, stale).await?;

        if let Some(due_at_ms) = next_due_ms {
            return Ok((jobs, false, Some(due_at_ms)));
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok((jobs, false, None)),
        }
    }
}

/// One index row seen by the scan: its key, the due time encoded in it and the
/// sidecar it points at.
type DueCandidate = (Vec<u8>, u64, Ulid, Ulid);

// Resolves a slice of index rows against their sidecars and liveness, returning
// the live jobs and the rows the caller should prune.
async fn resolve_due_jobs(
    storage: &StorageHandle,
    candidates: &[DueCandidate],
) -> Result<(ScannedJobs, Vec<(String, ByteView)>), MetadataMaterializationError> {
    let targets: Vec<(Ulid, Ulid)> = candidates
        .iter()
        .map(|(_, _, document_id, event_id)| (*document_id, *event_id))
        .collect();
    let sidecars = read_document_jobs(storage, &targets).await?;
    let mut due = Vec::with_capacity(candidates.len());
    let mut stale = Vec::new();
    for ((key, due_at_ms, document_id, event_id), sidecar) in candidates.iter().zip(sidecars) {
        match sidecar {
            Some(job) if job.due_at_ms == *due_at_ms => due.push((key.clone(), job)),
            // The sidecar is authoritative: an index row it does not match is
            // stale. A missing or malformed sidecar is dropped with it.
            sidecar => {
                stale.push((
                    MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(key.clone()),
                ));
                if sidecar.is_none() {
                    stale.push((
                        DOCUMENT_JOB_KEYSPACE.to_string(),
                        document_job_key(*document_id, *event_id),
                    ));
                }
            }
        }
    }

    let (live, dead) = filter_live_jobs(storage, due).await?;
    for (key, job) in dead {
        stale.push((
            MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(key),
        ));
        stale.push((
            DOCUMENT_JOB_KEYSPACE.to_string(),
            document_job_key(job.document_id, job.event_id),
        ));
    }
    Ok((live, stale))
}

async fn read_document_jobs(
    storage: &StorageHandle,
    targets: &[(Ulid, Ulid)],
) -> Result<Vec<Option<MetadataMaterializationRecord>>, MetadataMaterializationError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let reads = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                DOCUMENT_JOB_KEYSPACE.to_string(),
                document_job_key(*document_id, *event_id),
            )
        })
        .collect();
    Ok(batch_read_values(storage, reads)
        .await?
        .into_iter()
        .map(|value| value.and_then(|value| postcard::from_bytes(&value).ok()))
        .collect())
}

// Live means the create event still exists and no status has advanced past the
// job. Dead jobs are returned so the caller can prune both of their rows.
type ScannedJobs = Vec<(Vec<u8>, MetadataMaterializationRecord)>;

async fn filter_live_jobs(
    storage: &StorageHandle,
    jobs: ScannedJobs,
) -> Result<(ScannedJobs, ScannedJobs), MetadataMaterializationError> {
    if jobs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let statuses = read_status_map(
        storage,
        jobs.iter().map(|(_, job)| job.document_id).collect(),
    )
    .await?;
    let event_reads = jobs
        .iter()
        .map(|(_, job)| {
            (
                EVENT_LOG_KEYSPACE.to_string(),
                event_log_key(job.document_id, job.event_id),
            )
        })
        .collect();
    let events = batch_read_values(storage, event_reads).await?;
    let mut live = Vec::with_capacity(jobs.len());
    let mut dead = Vec::new();
    for ((key, job), event) in jobs.into_iter().zip(events) {
        let advanced = statuses
            .get(&job.document_id)
            .is_some_and(|status| retry_already_advanced(status, &job));
        if event.is_none() || advanced {
            dead.push((key, job));
        } else {
            live.push((key, job));
        }
    }
    Ok((live, dead))
}

#[derive(Debug, Default)]
struct ProcessedMaterializationJob {
    finished: Option<FinishedMaterializationJob>,
    craqle_elapsed: Duration,
    attempted: bool,
    stop_group: bool,
}

impl ProcessedMaterializationJob {
    fn completed(job: CompletedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            finished: Some(FinishedMaterializationJob::Completed(job)),
            craqle_elapsed,
            attempted: true,
            stop_group: false,
        }
    }

    // Rescheduled and parked jobs both stop the group so later events for the
    // same document do not apply out of order this batch.
    fn deferred(finished: FinishedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            finished: Some(finished),
            craqle_elapsed,
            attempted: true,
            stop_group: true,
        }
    }

    fn blocked() -> Self {
        Self {
            finished: None,
            craqle_elapsed: Duration::ZERO,
            attempted: false,
            stop_group: true,
        }
    }
}

// Jobs that exhaust the failure budget park as a dead letter; others reschedule with backoff.
// The resulting row writes and deletes are folded into the per-batch finish txn.
fn defer_materialization_job(
    job_key: &[u8],
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    error: &MetadataMaterializationError,
) -> FinishedMaterializationJob {
    let application_failure = matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Application
    );
    let failures = job.failures.saturating_add(u32::from(application_failure));
    let message = error.to_string();
    if failures >= MATERIALIZATION_MAX_FAILURES {
        FinishedMaterializationJob::Parked {
            job_key: job_key.to_vec(),
            job: job.clone(),
            status: materialization_failure_status(job, event, message, failures, true),
        }
    } else {
        FinishedMaterializationJob::Rescheduled {
            job_key: job_key.to_vec(),
            job: job.clone(),
            status: materialization_failure_status(job, event, message, failures, false),
        }
    }
}

async fn process_materialization_job(
    context: &DriverContext,
    job_key: Vec<u8>,
    job: MetadataMaterializationRecord,
    group: &GroupJobs,
    advanced_event_ids: &BTreeSet<Ulid>,
    raw_state_cache: &mut RawStateCache,
) -> Result<ProcessedMaterializationJob, MetadataMaterializationError> {
    if older_job_exists(&context.storage_handle, group, &job, advanced_event_ids).await? {
        return Ok(ProcessedMaterializationJob::blocked());
    }
    let document_job_key = document_job_key(job.document_id, job.event_id).to_vec();

    let obsolescence = job_obsolescence(group.status.as_ref(), &job);
    let event = read_create_event(&context.storage_handle, job.document_id, job.event_id).await;
    match obsolescence {
        MaterializationJobObsolescence::Live => {}
        MaterializationJobObsolescence::Final => {
            return Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: None,
                    iri_index_writes: Vec::new(),
                    raw_state_write: None,
                    validation_write: None,
                    sync: None,
                },
                Duration::ZERO,
            ));
        }
        MaterializationJobObsolescence::RetryAdvanced => {
            delete_global_job(&context.storage_handle, job_key).await?;
            return Ok(ProcessedMaterializationJob::default());
        }
    }

    let event = match event {
        Ok(event) => event,
        Err(MetadataMaterializationError::CreateEventMissing { .. }) => {
            return Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: None,
                    iri_index_writes: Vec::new(),
                    raw_state_write: None,
                    validation_write: None,
                    sync: None,
                },
                Duration::ZERO,
            ));
        }
        Err(error) => return Err(error),
    };
    if metadata_graph_deleted(&context.storage_handle, &event.record.graph_iri).await? {
        return Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                document_job_key: Some(document_job_key),
                status: Some(materialization_failure_status(
                    &job,
                    &event,
                    "metadata graph was deleted before materialization".to_string(),
                    job.failures,
                    true,
                )),
                iri_index_writes: Vec::new(),
                raw_state_write: None,
                validation_write: None,
                sync: None,
            },
            Duration::ZERO,
        ));
    }

    let apply_started = Instant::now();
    let apply_result = materialize_create_event(context, &event, raw_state_cache).await;
    let craqle_elapsed = apply_started.elapsed();
    match apply_result {
        Ok(materialized) => {
            let raw_revision = materialized.raw_revision;
            let iri_index_writes = match project_materialized_iris(context, &event).await {
                Ok(writes) => writes,
                Err(error) => {
                    return Ok(ProcessedMaterializationJob::deferred(
                        defer_materialization_job(&job_key, &job, &event, &error),
                        craqle_elapsed,
                    ));
                }
            };
            Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: Some(materialization_success_status(
                        &job,
                        &event,
                        raw_revision.as_ref(),
                    )),
                    iri_index_writes,
                    raw_state_write: Some(materialized.raw_state_write),
                    validation_write: materialized.validation_write,
                    sync: Some(CompletedMaterializationSync {
                        graph_iri: event.record.graph_iri.clone(),
                        peers: event.record.holder_node_ids.clone(),
                    }),
                },
                craqle_elapsed,
            ))
        }
        Err(error) if is_terminal_error(&error) => Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                document_job_key: Some(document_job_key),
                status: Some(materialization_failure_status(
                    &job,
                    &event,
                    error.to_string(),
                    job.failures,
                    true,
                )),
                iri_index_writes: Vec::new(),
                raw_state_write: None,
                validation_write: None,
                sync: None,
            },
            craqle_elapsed,
        )),
        Err(error) => Ok(ProcessedMaterializationJob::deferred(
            defer_materialization_job(&job_key, &job, &event, &error),
            craqle_elapsed,
        )),
    }
}

/// Everything a document's group needs to order its own events: its queued jobs
/// and its status, loaded once instead of once per event.
#[derive(Debug, Default)]
struct GroupJobs {
    pending: Vec<MetadataMaterializationRecord>,
    status: Option<MaterializationStatusRecord>,
}

// The sidecar keyspace is per-document and event-ordered, so this costs one
// prefix scan plus one status read for the whole group.
async fn load_group_jobs(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<GroupJobs, MetadataMaterializationError> {
    let status = read_materialization_status(storage, document_id, None).await?;
    let prefix = document_job_prefix(document_id);
    let mut pending = Vec::new();
    let mut malformed = Vec::new();
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: DOCUMENT_JOB_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.take().map(IterStart::After),
                limit: MATERIALIZATION_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            match postcard::from_bytes::<MetadataMaterializationRecord>(&value) {
                Ok(job) if job.document_id == document_id => pending.push(job),
                Ok(_) => {}
                Err(error) => {
                    warn!(error = %error, key = ?key.to_vec(), "Deleting malformed metadata materialization document job");
                    malformed.push((DOCUMENT_JOB_KEYSPACE.to_string(), key));
                }
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    delete_materialization_entries(storage, malformed).await?;
    pending.sort_by_key(|job| job.event_id);
    Ok(GroupJobs { pending, status })
}

// An unadvanced, live job for an earlier event of the same document must run
// first, so this one waits for the next batch.
async fn older_job_exists(
    storage: &StorageHandle,
    group: &GroupJobs,
    job: &MetadataMaterializationRecord,
    advanced_event_ids: &BTreeSet<Ulid>,
) -> Result<bool, MetadataMaterializationError> {
    for pending in &group.pending {
        if pending.event_id >= job.event_id
            || advanced_event_ids.contains(&pending.event_id)
            || group
                .status
                .as_ref()
                .is_some_and(|status| status_obsoletes_job(status, pending))
        {
            continue;
        }
        if !materialization_event_exists(storage, pending).await? {
            warn!(document_id = %pending.document_id, event_id = %pending.event_id, "Deleting orphan metadata materialization job");
            delete_materialization_job(storage, materialization_job_key(pending).to_vec()).await?;
            continue;
        }
        return Ok(true);
    }
    Ok(false)
}

async fn read_create_event(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<MetadataEventRecord, MetadataMaterializationError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: EVENT_LOG_KEYSPACE.to_string(),
            key: event_log_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let event: MetadataEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            if event.record.document_id != document_id || event.event_id != event_id {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "metadata event log key mismatch for {document_id}/{event_id}"
                )));
            }
            Ok(event)
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(MetadataMaterializationError::CreateEventMissing {
                document_id,
                event_id,
            })
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn job_obsolescence(
    status: Option<&MaterializationStatusRecord>,
    job: &MetadataMaterializationRecord,
) -> MaterializationJobObsolescence {
    let Some(status) = status else {
        return MaterializationJobObsolescence::Live;
    };
    if status_obsoletes_job(status, job) {
        return MaterializationJobObsolescence::Final;
    }
    if status.event_id == job.event_id && status.attempts > job.attempts {
        return MaterializationJobObsolescence::RetryAdvanced;
    }
    MaterializationJobObsolescence::Live
}

async fn read_materialization_status(
    storage: &StorageHandle,
    document_id: Ulid,
    txn_id: Option<Ulid>,
) -> Result<Option<MaterializationStatusRecord>, MetadataMaterializationError> {
    let event = storage
        .send_effect(read_status_effect(document_id, txn_id))
        .await;
    parse_status_read(event).map_err(|error| match error {
        StorageReadError::Storage(error) => error.into(),
        StorageReadError::Conversion(error) => error.into(),
    })
}

fn status_is_final(status: &MaterializationStatusRecord) -> bool {
    matches!(
        status.state,
        MaterializationState::Materialized | MaterializationState::Failed
    )
}

fn status_obsoletes_job(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status.event_id >= job.event_id && status_is_final(status)
}

fn retry_already_advanced(
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> bool {
    status_obsoletes_job(status, job)
        || (status.event_id == job.event_id && status.attempts > job.attempts)
}

fn should_write_final(
    current: Option<&MaterializationStatusRecord>,
    next: &MaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || (current.event_id == next.event_id && current.attempts >= next.attempts)
            || (current.event_id == next.event_id && status_is_final(current))
    })
}

fn should_write_retry(
    current: Option<&MaterializationStatusRecord>,
    next: &MaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || retry_already_advanced(
                current,
                &MetadataMaterializationRecord {
                    document_id: next.document_id,
                    event_id: next.event_id,
                    due_at_ms: 0,
                    attempts: next.attempts,
                    failures: next.failures,
                    parks: 0,
                },
            )
    })
}

pub async fn materialization_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataMaterializationError> {
    let mut start_after = None;
    loop {
        let (values, next_start_after) = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: MATERIALIZATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: 1,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };
        let Some((key, _value)) = values.into_iter().next() else {
            return Ok(false);
        };
        let key = key.to_vec();
        match job_key_parts(&key) {
            Some((due_at_ms, document_id, event_id)) => {
                match read_document_job(storage, document_id, event_id).await? {
                    Some(job) if job.due_at_ms == due_at_ms => {
                        if job_is_live(storage, &job).await? {
                            return Ok(true);
                        }
                        delete_materialization_job(storage, key).await?;
                    }
                    _ => delete_global_job(storage, key).await?,
                }
            }
            None => delete_global_job(storage, key).await?,
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(false),
        }
    }
}

async fn delete_materialization_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationError> {
    let mut deletes = vec![(
        MATERIALIZATION_JOB_KEYSPACE.to_string(),
        ByteView::from(key.clone()),
    )];
    if let Some((document_id, event_id)) = job_key_target(&key) {
        deletes.push((
            DOCUMENT_JOB_KEYSPACE.to_string(),
            document_job_key(document_id, event_id),
        ));
    }
    delete_materialization_entries(storage, deletes).await
}

async fn delete_global_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationError> {
    delete_materialization_entries(
        storage,
        vec![(
            MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(key),
        )],
    )
    .await
}

async fn delete_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationError> {
    delete_materialization_entries(
        storage,
        vec![(DOCUMENT_JOB_KEYSPACE.to_string(), ByteView::from(key))],
    )
    .await
}

async fn delete_materialization_entries(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataMaterializationError> {
    if deletes.is_empty() {
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn job_key_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    job_key_parts(key).map(|(_, document_id, event_id)| (document_id, event_id))
}

fn job_key_parts(key: &[u8]) -> Option<(u64, Ulid, Ulid)> {
    if key.len() != 40 {
        return None;
    }
    let mut due_at_ms = [0u8; 8];
    due_at_ms.copy_from_slice(&key[..8]);
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[8..24]);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&key[24..40]);
    Some((
        u64::from_be_bytes(due_at_ms),
        Ulid::from_bytes(document_id),
        Ulid::from_bytes(event_id),
    ))
}

async fn metadata_graph_deleted(
    storage: &StorageHandle,
    graph_iri: &str,
) -> Result<bool, MetadataMaterializationError> {
    let event = storage
        .send_effect(read_lifecycle_effect(graph_iri, None))
        .await;
    parse_lifecycle_read(event)
        .map(|record| record.is_some_and(|record| record.is_deleted()))
        .map_err(|error| match error {
            StorageReadError::Storage(error) => error.into(),
            StorageReadError::Conversion(error) => error.into(),
        })
}

struct MaterializedCreateEvent {
    raw_revision: Option<MetadataRawRevision>,
    raw_state_write: (String, ByteView, ByteView),
    validation_write: Option<(String, ByteView, ByteView)>,
}

async fn materialize_create_event(
    context: &DriverContext,
    event: &MetadataEventRecord,
    raw_state_cache: &mut RawStateCache,
) -> Result<MaterializedCreateEvent, MetadataMaterializationError> {
    if let MetadataEventPayload::ApplyBatch { batch, .. } = &event.payload {
        return merge_batch_event(context, event, batch, raw_state_cache).await;
    }
    let raw_plan =
        crate::metadata::raw_revision::prepare_raw_event(context, event, raw_state_cache).await?;
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationError::MetadataHandleMissing)?;
    match metadata_handle
        .send_effect(graph_materialization_effect(
            event,
            raw_plan.revision.as_ref(),
            raw_plan.rebuild,
        ))
        .await
    {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. })
        | Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. })
        | Event::Metadata(MetadataEvent::EntityUpsertResult { .. }) => {
            raw_plan.cache(raw_state_cache);
            Ok(MaterializedCreateEvent {
                raw_revision: raw_plan.revision,
                raw_state_write: raw_plan.state_write,
                validation_write: None,
            })
        }
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

/// Merges the origin's batch, then re-renders and re-validates the graph. The
/// merge is order independent and idempotent by dot, so every holder converges
/// whatever order events arrive in.
async fn merge_batch_event(
    context: &DriverContext,
    event: &MetadataEventRecord,
    batch: &MetadataBatch,
    raw_state_cache: &mut RawStateCache,
) -> Result<MaterializedCreateEvent, MetadataMaterializationError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationError::MetadataHandleMissing)?;
    // A batch carries quads only, so the event's visibility is applied here the
    // way a crate replacement used to carry it.
    match metadata_handle
        .send_effect(Effect::Metadata(MetadataEffect::SetGraphPolicy {
            graph_iri: event.record.graph_iri.clone(),
            policy: MetadataGraphPolicy {
                public: event.record.public,
                permission_paths: vec![event.record.permission_path.clone()],
            }
            .normalized(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::GraphPolicySet { .. }) => {}
        Event::Metadata(MetadataEvent::Error { error, .. }) => return Err(error.into()),
        other => {
            return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    }
    match metadata_handle
        .send_effect(Effect::Metadata(MetadataEffect::MergeBatch {
            graph_iri: event.record.graph_iri.clone(),
            batch: batch.clone(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::BatchMerged { .. }) => {}
        Event::Metadata(MetadataEvent::Error { error, .. }) => return Err(error.into()),
        other => {
            return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    }
    let render = match metadata_handle
        .send_effect(Effect::Metadata(MetadataEffect::ExportRoCrate {
            graph_iri: event.record.graph_iri.clone(),
        }))
        .await
    {
        Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => jsonld,
        Event::Metadata(MetadataEvent::Error { error, .. }) => return Err(error.into()),
        other => {
            return Err(MetadataMaterializationError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let mut status = assess_render(
        context,
        event.record.document_id,
        event.record.group_id,
        &render,
    )
    .await;
    let findings = violation_count(&status);
    let raw_plan = crate::metadata::raw_revision::prepare_merged_event(
        context,
        event,
        render,
        findings,
        raw_state_cache,
    )
    .await?;
    raw_plan.cache(raw_state_cache);
    status.dataset_revision = event.event_id;
    status.dataset_digest = raw_plan
        .revision
        .as_ref()
        .and_then(|revision| revision.dataset_digest);
    let validation_write = profile_validation_entry(&status)?;
    Ok(MaterializedCreateEvent {
        raw_revision: raw_plan.revision,
        raw_state_write: raw_plan.state_write,
        validation_write: Some(validation_write),
    })
}

async fn project_materialized_iris(
    context: &DriverContext,
    event: &MetadataEventRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, MetadataMaterializationError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationError::MetadataHandleMissing)?;
    let references = metadata_handle
        .snapshot_iri_references(event.record.graph_iri.clone())
        .await?;
    let records = super::iri_index::project_iri_references(
        event.record.document_id,
        event.event_id,
        references,
    );
    super::iri_index::iri_write_entries(&records).map_err(MetadataMaterializationError::from)
}

fn graph_materialization_effect(
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
    rebuild: bool,
) -> Effect {
    let policy = MetadataGraphPolicy {
        public: event.record.public,
        permission_paths: vec![event.record.permission_path.clone()],
    }
    .normalized();
    let deterministic_actor = Some(deterministic_materialization_actor(event.event_id));
    if rebuild && let Some(raw_revision) = raw_revision {
        return Effect::Metadata(MetadataEffect::ApplyRoCrate {
            request: ApplyRoCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: raw_revision.jsonld.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        });
    }
    match &event.payload {
        MetadataEventPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => Effect::Metadata(MetadataEffect::CreateCrate {
            request: MetadataCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                name: name.clone(),
                description: description.clone(),
                date_published: date_published.clone(),
                license: license.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        }),
        MetadataEventPayload::RoCrate { jsonld }
        | MetadataEventPayload::ReplaceRoCrate { jsonld } => {
            Effect::Metadata(MetadataEffect::ApplyRoCrate {
                request: ApplyRoCrateRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::UpsertDataEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertDataEntity {
                request: aruna_core::metadata::UpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::UpsertContextualEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity {
                request: aruna_core::metadata::UpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataEventPayload::ApplyBatch { batch, .. } => {
            Effect::Metadata(MetadataEffect::MergeBatch {
                graph_iri: event.record.graph_iri.clone(),
                batch: batch.clone(),
            })
        }
    }
}

fn materialization_success_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: raw_revision.map(|revision| revision.context_digest),
        dataset_digest: raw_revision.and_then(|revision| revision.dataset_digest),
        state: MaterializationState::Materialized,
        attempts: job.attempts.saturating_add(1),
        failures: job.failures,
        last_error: None,
        updated_at_ms: unix_timestamp_millis(),
    }
}

fn materialization_failure_status(
    job: &MetadataMaterializationRecord,
    event: &MetadataEventRecord,
    error: String,
    failures: u32,
    terminal: bool,
) -> MaterializationStatusRecord {
    MaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: None,
        dataset_digest: None,
        state: if terminal {
            MaterializationState::Failed
        } else {
            MaterializationState::Pending
        },
        attempts: job.attempts.saturating_add(1),
        failures,
        last_error: Some(error),
        updated_at_ms: unix_timestamp_millis(),
    }
}

/// How a failed materialization attempt is charged. Only [`Application`]
/// failures spend the budget that eventually parks a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MaterializationFailureKind {
    Terminal,
    Transient,
    Application,
}

fn materialization_failure_kind(
    error: &MetadataMaterializationError,
) -> MaterializationFailureKind {
    match error {
        MetadataMaterializationError::Metadata(
            MetadataError::InvalidInput(_) | MetadataError::Validation(_),
        ) => MaterializationFailureKind::Terminal,
        // A storage failure or an off-contract adapter event is never the
        // document's fault, so it must not spend the budget that parks a job.
        MetadataMaterializationError::Storage(_)
        | MetadataMaterializationError::UnexpectedEvent(_)
        | MetadataMaterializationError::Metadata(
            MetadataError::ChannelClosed
            | MetadataError::TaskJoin(_)
            | MetadataError::HandleMissing
            | MetadataError::Persist(_)
            | MetadataError::Storage(_),
        )
        | MetadataMaterializationError::MetadataHandleMissing => {
            MaterializationFailureKind::Transient
        }
        _ => MaterializationFailureKind::Application,
    }
}

fn is_terminal_error(error: &MetadataMaterializationError) -> bool {
    matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Terminal
    )
}

async fn write_status_job(
    storage: &StorageHandle,
    status: &MaterializationStatusRecord,
    job: &MetadataMaterializationRecord,
) -> Result<(), MetadataMaterializationError> {
    let writes = vec![
        materialization_status_entry(status)?,
        materialization_job_entry(job)?,
        document_job_entry(job)?,
    ];
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn materialization_event_exists(
    storage: &StorageHandle,
    job: &MetadataMaterializationRecord,
) -> Result<bool, MetadataMaterializationError> {
    match read_create_event(storage, job.document_id, job.event_id).await {
        Ok(_) => Ok(true),
        Err(MetadataMaterializationError::CreateEventMissing { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn job_is_live(
    storage: &StorageHandle,
    job: &MetadataMaterializationRecord,
) -> Result<bool, MetadataMaterializationError> {
    if !materialization_event_exists(storage, job).await? {
        return Ok(false);
    }
    let status = read_materialization_status(storage, job.document_id, None).await?;
    Ok(!status
        .as_ref()
        .is_some_and(|status| retry_already_advanced(status, job)))
}

#[cfg(test)]
#[path = "materialization_queue_tests.rs"]
mod tests;
