use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE, METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
    METADATA_MATERIALIZATION_JOB_KEYSPACE, METADATA_MATERIALIZATION_PRUNE_KEYSPACE,
    METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataCreateCrateRequest, MetadataCreateEventPayload,
    MetadataCreateEventRecord, MetadataEffect, MetadataError, MetadataEvent,
    MetadataGraphLifecycleRecord, MetadataGraphPolicy, MetadataMaterializationDeadLetterRecord,
    MetadataMaterializationJobRecord, MetadataMaterializationState,
    MetadataMaterializationStatusRecord, MetadataRawRevision, MetadataRequestDurability,
    deterministic_materialization_actor,
};
use aruna_core::storage_entries::{
    dead_letter_entry, dead_letter_key, materialization_prune_entry, materialization_prune_key,
    metadata_event_log_key, metadata_graph_lifecycle_key,
    metadata_materialization_document_job_key, metadata_materialization_document_job_prefix,
    metadata_materialization_document_job_write_entry, metadata_materialization_job_key,
    metadata_materialization_job_write_entry, metadata_materialization_status_key,
    metadata_materialization_status_write_entry,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;

use crate::queue_backoff::queue_retry_after_ms;

use super::iri_index::MetadataIriIndexError;
use super::queue_storage::{
    MetadataQueueStorageError, abort_storage_transaction_best_effort, commit_storage_transaction,
    start_write_transaction,
};
use super::raw::{MetadataRawReadError, RawStateCache};

const MATERIALIZATION_SCAN_PAGE_SIZE: usize = 512;
const MATERIALIZATION_BATCH_SIZE: usize = 512;
// Only application failures count here: infrastructure errors retry forever with
// backoff, so an overloaded node never parks work it could still finish.
const MATERIALIZATION_MAX_FAILURES: u32 = 10;
// Parked jobs return to the queue on this backoff, doubling per park up to the
// cap, so a node recovers on its own once the cause clears.
const DEAD_LETTER_REQUEUE_BASE_MS: u64 = 60_000;
const DEAD_LETTER_REQUEUE_MAX_MS: u64 = 3_600_000;
const DEAD_LETTER_REQUEUE_PAGE_SIZE: usize = 256;
// Jobs per finish transaction. Small enough that a failed commit costs little,
// large enough that a full batch commits in a handful of transactions.
const MATERIALIZATION_FINISH_CHUNK: usize = 256;
const METADATA_GRAPH_FENCE_SHARDS: usize = 32;

// Craqle already serializes same-graph writes internally. This bounded fence
// extends that ordering over the lifecycle read performed by the Aruna handle.
static METADATA_GRAPH_FENCES: LazyLock<[Semaphore; METADATA_GRAPH_FENCE_SHARDS]> =
    LazyLock::new(|| std::array::from_fn(|_| Semaphore::new(1)));

pub(crate) fn metadata_graph_fence(graph_iri: &str) -> &'static Semaphore {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    let shard = u64::from_be_bytes(prefix) as usize;
    &METADATA_GRAPH_FENCES[shard % METADATA_GRAPH_FENCE_SHARDS]
}

pub const METADATA_MATERIALIZATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const METADATA_MATERIALIZATION_RETRY_AFTER: Duration = Duration::from_secs(1);
// Best-effort gap between full batches; enqueue ResetTimer(ZERO) and refire on
// completion can bypass it under continuous refill, so real fairness is enforced
// by the bulk storage lane, not this constant.
pub const METADATA_MATERIALIZATION_NEXT_BATCH_AFTER: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub struct MetadataMaterializationDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug)]
struct CompletedMaterializationJob {
    job_key: Vec<u8>,
    document_job_key: Option<Vec<u8>>,
    status: Option<MetadataMaterializationStatusRecord>,
    iri_index_writes: Vec<(String, ByteView, ByteView)>,
    raw_state_write: Option<(String, ByteView, ByteView)>,
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
        job: MetadataMaterializationJobRecord,
        status: MetadataMaterializationStatusRecord,
    },
    Parked {
        job_key: Vec<u8>,
        job: MetadataMaterializationJobRecord,
        status: MetadataMaterializationStatusRecord,
    },
}

#[derive(Debug, Default)]
struct MaterializationGroupOutcome {
    finished: Vec<FinishedMaterializationJob>,
    processed: usize,
    craqle_elapsed: Duration,
    error: Option<MetadataMaterializationQueueError>,
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
pub enum MetadataMaterializationQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("metadata handle missing")]
    MetadataHandleMissing,
    #[error("metadata create event log record not found for {document_id}/{event_id}")]
    MetadataCreateEventMissing { document_id: Ulid, event_id: Ulid },
    #[error("unexpected event while processing metadata materialization queue: {0}")]
    UnexpectedEvent(String),
    #[error("inconsistent metadata raw event log: {0}")]
    InconsistentLog(String),
}

impl From<MetadataQueueStorageError> for MetadataMaterializationQueueError {
    fn from(error: MetadataQueueStorageError) -> Self {
        match error {
            MetadataQueueStorageError::Storage(error) => Self::Storage(error),
            MetadataQueueStorageError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
        }
    }
}

impl From<MetadataRawReadError> for MetadataMaterializationQueueError {
    fn from(error: MetadataRawReadError) -> Self {
        match error {
            MetadataRawReadError::Storage(error) => Self::Storage(error),
            MetadataRawReadError::Conversion(error) => Self::Conversion(error),
            MetadataRawReadError::Metadata(error) => Self::Metadata(error),
            MetadataRawReadError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
            MetadataRawReadError::LimitExceeded(message) => Self::UnexpectedEvent(message),
            MetadataRawReadError::InconsistentLog(message) => Self::InconsistentLog(message),
        }
    }
}

impl From<MetadataIriIndexError> for MetadataMaterializationQueueError {
    fn from(error: MetadataIriIndexError) -> Self {
        match error {
            MetadataIriIndexError::Storage(error) => Self::Storage(error),
            MetadataIriIndexError::Conversion(error) => Self::Conversion(error),
            MetadataIriIndexError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
        }
    }
}

pub fn schedule_metadata_materialization_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainMetadataMaterializationQueue,
        after: Duration::ZERO,
    })
}

pub fn new_materialization_job(
    event: &MetadataCreateEventRecord,
    due_at_ms: u64,
) -> MetadataMaterializationJobRecord {
    MetadataMaterializationJobRecord::new(event, due_at_ms)
}

pub fn new_pending_materialization_status(
    event: &MetadataCreateEventRecord,
    updated_at_ms: u64,
) -> MetadataMaterializationStatusRecord {
    MetadataMaterializationStatusRecord::pending(event, updated_at_ms)
}

pub async fn restore_metadata_materialization_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    match next_metadata_materialization_timer_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainMetadataMaterializationQueue,
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

pub async fn next_metadata_materialization_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, MetadataMaterializationQueueError> {
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_at_ms) =
        scan_due_materialization_jobs(storage, now_ms, 1).await?;
    if !jobs.is_empty() || has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(next_due_at_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms)))
}

pub async fn process_metadata_materialization_batch(
    context: &DriverContext,
) -> Result<MetadataMaterializationDrainResult, MetadataMaterializationQueueError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_at_ms) =
        scan_due_materialization_jobs(&context.storage_handle, now_ms, MATERIALIZATION_BATCH_SIZE)
            .await?;
    let scan_elapsed = batch_started.elapsed();
    let job_count = jobs.len();
    let oldest_lag_ms = jobs
        .iter()
        .map(|(_, job)| now_ms.saturating_sub(job.due_at_ms))
        .max()
        .unwrap_or(0);
    let timings = process_materialization_job_groups(context, jobs).await?;
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
    Ok(MetadataMaterializationDrainResult {
        processed: timings.processed,
        has_more_due,
        next_due_after: if has_more_due {
            Some(Duration::ZERO)
        } else {
            next_due_at_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

// Materialization shares CPU, the craqle write pool, and the storage actor
// with foreground create/validate traffic; capping drain concurrency at half
// the cores keeps ingest latency flat while the queue still drains steadily.
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
    first_error: &mut Option<MetadataMaterializationQueueError>,
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
                *first_error = Some(MetadataMaterializationQueueError::UnexpectedEvent(
                    error.to_string(),
                ));
            }
        }
    }
}

async fn process_materialization_job_groups(
    context: &DriverContext,
    jobs: Vec<(Vec<u8>, MetadataMaterializationJobRecord)>,
) -> Result<MaterializationBatchTimings, MetadataMaterializationQueueError> {
    let mut groups: BTreeMap<Ulid, Vec<(Vec<u8>, MetadataMaterializationJobRecord)>> =
        BTreeMap::new();
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
    if let Err(error) =
        finish_completed_materialization_jobs(&context.storage_handle, finished).await
        && first_error.is_none()
    {
        first_error = Some(error);
    }
    timings.finish_elapsed = finish_started.elapsed();
    // The applies behind these syncs are already durable, so they are scheduled
    // even when a later finish chunk failed: the committed chunks deleted their
    // job rows, so nothing would retry their only explicit push.
    schedule_completed_materialization_syncs(context, syncs).await;
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

async fn schedule_completed_materialization_syncs(
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
            .send_effect(Effect::Metadata(MetadataEffect::SyncGraphBestEffort {
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

// Chunked so a failure costs one chunk instead of the whole batch, and the
// craqle work behind the committed chunks survives. The chunks that did commit
// are still pruned, since their job rows are gone and nothing else would.
async fn finish_completed_materialization_jobs(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
) -> Result<(), MetadataMaterializationQueueError> {
    let mut superseding = HashMap::new();
    let finish = finish_chunks(storage, finished, &mut superseding).await;
    let prune = prune_superseded_rows(storage, superseding).await;
    finish.and(prune)
}

async fn finish_chunks(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
    superseding: &mut HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationQueueError> {
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

// The IRI reference index cannot be scanned per document, so this walks the
// whole keyspace: once per batch, never once per chunk. Cleanup only, so it
// runs after the chunks commit rather than inside them, and a failure is parked
// durably: the finished jobs are gone, so nothing else would retry it.
async fn prune_superseded_rows(
    storage: &StorageHandle,
    mut superseding: HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationQueueError> {
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
) -> Result<(), MetadataMaterializationQueueError> {
    let stale = super::iri_index::superseded_iri_reference_keys(storage, None, superseding).await?;
    delete_materialization_entries(storage, stale).await
}

// One page per batch: leftovers stay parked for the next drain, so a long
// backlog cannot make a single batch scan unboundedly.
async fn read_pending_prunes(
    storage: &StorageHandle,
) -> Result<HashMap<Ulid, Ulid>, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_MATERIALIZATION_PRUNE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: MATERIALIZATION_SCAN_PAGE_SIZE,
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
                        vec![(
                            METADATA_MATERIALIZATION_PRUNE_KEYSPACE.to_string(),
                            key.clone(),
                        )],
                    )
                    .await?;
                    continue;
                };
                pending.insert(Ulid::from_bytes(document_id), cursor);
            }
            Ok(pending)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn persist_pending_prunes(
    storage: &StorageHandle,
    superseding: &HashMap<Ulid, Ulid>,
) -> Result<(), MetadataMaterializationQueueError> {
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_pending_prunes(
    storage: &StorageHandle,
    document_ids: Vec<Ulid>,
) -> Result<(), MetadataMaterializationQueueError> {
    let deletes = document_ids
        .into_iter()
        .map(|document_id| {
            (
                METADATA_MATERIALIZATION_PRUNE_KEYSPACE.to_string(),
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
) -> Result<HashMap<Ulid, Ulid>, MetadataMaterializationQueueError> {
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
            abort_storage_transaction_best_effort(
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

// Guards read the pre-transaction snapshot instead of reading inside the
// transaction: a write-only transaction has an empty read set and can never
// conflict with a concurrent create, update or inbound sync.
async fn plan_finish_chunk(
    storage: &StorageHandle,
    finished: Vec<FinishedMaterializationJob>,
) -> Result<FinishPlan, MetadataMaterializationQueueError> {
    let snapshot =
        read_status_map(storage, finished.iter().map(finished_document_id).collect()).await?;
    let parked = read_dead_letters(storage, &finished).await?;
    let mut plan = FinishPlan {
        deletes: Vec::with_capacity(finished.len().saturating_mul(2)),
        ..FinishPlan::default()
    };
    let mut planned: HashMap<Ulid, MetadataMaterializationStatusRecord> = HashMap::new();
    let mut superseding: HashMap<Ulid, Ulid> = HashMap::new();
    for finished in finished {
        match finished {
            FinishedMaterializationJob::Completed(job) => {
                if let Some(status) = job.status {
                    let current = guard_status(&snapshot, &planned, status.document_id);
                    if should_write_final_materialization_status(current, &status) {
                        superseding.insert(status.document_id, status.event_id);
                        plan.writes.extend(job.iri_index_writes);
                        if let Some(raw_state_write) = job.raw_state_write {
                            plan.writes.push(raw_state_write);
                        }
                        plan.writes
                            .push(metadata_materialization_status_write_entry(&status)?);
                        planned.insert(status.document_id, status);
                    }
                }
                plan.deletes.push((
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job.job_key),
                ));
                if let Some(document_job_key) = job.document_job_key {
                    plan.deletes.push((
                        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
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
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(job_key),
                );
                let current = guard_status(&snapshot, &planned, job.document_id);
                if current
                    .is_some_and(|current| materialization_retry_already_advanced(current, &job))
                {
                    plan.deletes.push(old_index_delete);
                    plan.deletes.push((
                        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                        metadata_materialization_document_job_key(job.document_id, job.event_id),
                    ));
                    continue;
                }
                let attempts = job.attempts.saturating_add(1);
                let next_job = MetadataMaterializationJobRecord {
                    document_id: job.document_id,
                    event_id: job.event_id,
                    due_at_ms: unix_timestamp_millis()
                        .saturating_add(queue_retry_after_ms(attempts)),
                    attempts,
                    failures: status.failures,
                    parks: job.parks,
                };
                if should_write_pending_retry_status(current, &status) {
                    plan.writes
                        .push(metadata_materialization_status_write_entry(&status)?);
                    planned.insert(status.document_id, status);
                }
                plan.writes
                    .push(metadata_materialization_job_write_entry(&next_job)?);
                plan.writes
                    .push(metadata_materialization_document_job_write_entry(
                        &next_job,
                    )?);
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
                        METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                        ByteView::from(job_key),
                    ),
                    (
                        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                        metadata_materialization_document_job_key(job.document_id, job.event_id),
                    ),
                ];
                // An already-superseded job must not leave a dead letter behind:
                // requeueing it later would resurrect an obsolete event.
                if current
                    .is_some_and(|current| materialization_retry_already_advanced(current, &job))
                {
                    plan.deletes.extend(job_deletes);
                    continue;
                }
                if should_write_final_materialization_status(current, &status) {
                    plan.writes
                        .push(metadata_materialization_status_write_entry(&status)?);
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

    plan.superseding = superseding;
    Ok(plan)
}

// A status planned earlier in this chunk is newer than the snapshot, so it is
// what later jobs of the same document must be judged against.
fn guard_status<'a>(
    snapshot: &'a HashMap<Ulid, MetadataMaterializationStatusRecord>,
    planned: &'a HashMap<Ulid, MetadataMaterializationStatusRecord>,
    document_id: Ulid,
) -> Option<&'a MetadataMaterializationStatusRecord> {
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
                materialization_job_key_target(&job.job_key)
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
) -> Result<HashMap<Ulid, MetadataMaterializationStatusRecord>, MetadataMaterializationQueueError> {
    if document_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let reads = document_ids
        .iter()
        .map(|document_id| {
            (
                METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
                metadata_materialization_status_key(*document_id),
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
) -> Result<
    HashMap<(Ulid, Ulid), MetadataMaterializationDeadLetterRecord>,
    MetadataMaterializationQueueError,
> {
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
                METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE.to_string(),
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
) -> Result<Vec<Option<ByteView>>, MetadataMaterializationQueueError> {
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

// A re-parked job keeps its park count so its requeue backoff keeps growing
// instead of restarting at the base delay. The count survives a requeue on the
// job itself, since the requeue deletes the dead letter that carried it.
fn parked_dead_letter(
    job: &MetadataMaterializationJobRecord,
    status: &MetadataMaterializationStatusRecord,
    previous: Option<&MetadataMaterializationDeadLetterRecord>,
) -> MetadataMaterializationDeadLetterRecord {
    let parks = previous
        .map_or(job.parks, |previous| previous.parks.max(job.parks))
        .saturating_add(1);
    let now_ms = unix_timestamp_millis();
    MetadataMaterializationDeadLetterRecord {
        job: job.clone(),
        last_error: status.last_error.clone().unwrap_or_default(),
        parked_at_ms: now_ms,
        parks,
        requeue_at_ms: now_ms.saturating_add(requeue_after_ms(parks)),
    }
}

fn requeue_after_ms(parks: u32) -> u64 {
    crate::queue_backoff::retry_after_ms(
        parks.saturating_sub(1),
        DEAD_LETTER_REQUEUE_BASE_MS,
        DEAD_LETTER_REQUEUE_MAX_MS,
    )
}

#[cfg(test)]
async fn read_dead_letter(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<MetadataMaterializationDeadLetterRecord>, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE.to_string(),
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

/// Returns due dead letters to the queue and clears their terminal status, so a
/// node converges once the cause clears. Dead letters the document has moved
/// past are dropped instead. Returns the number of jobs requeued.
pub async fn requeue_dead_letters(
    storage: &StorageHandle,
) -> Result<usize, MetadataMaterializationQueueError> {
    let now_ms = unix_timestamp_millis();
    let mut start_after = None;
    let mut requeued = 0usize;
    loop {
        let (values, next_start_after) = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: DEAD_LETTER_REQUEUE_PAGE_SIZE,
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
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            let Ok(dead_letter) =
                postcard::from_bytes::<MetadataMaterializationDeadLetterRecord>(&value)
            else {
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
                // A racing finish for the same document aborts this requeue; the
                // dead letter stays, so one contended document must not stop the
                // sweep from freeing the rest.
                Err(MetadataMaterializationQueueError::Storage(
                    StorageError::TransactionConflict,
                )) => {
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
    status: &MetadataMaterializationStatusRecord,
    job: &MetadataMaterializationJobRecord,
) -> bool {
    materialization_status_obsoletes_job(status, job)
        && (status.event_id > job.event_id
            || status.state == MetadataMaterializationState::Materialized)
}

// The parked status is terminal for this event, so it must be cleared with the
// job rows or the requeued job is pruned as obsolete on the next scan. The
// requeued job keeps one failure of budget so a poison document re-parks fast.
async fn requeue_dead_letter(
    storage: &StorageHandle,
    dead_letter: &MetadataMaterializationDeadLetterRecord,
) -> Result<bool, MetadataMaterializationQueueError> {
    let job = MetadataMaterializationJobRecord {
        document_id: dead_letter.job.document_id,
        event_id: dead_letter.job.event_id,
        due_at_ms: unix_timestamp_millis(),
        attempts: 0,
        failures: MATERIALIZATION_MAX_FAILURES.saturating_sub(1),
        parks: dead_letter.parks,
    };
    let event = match read_create_event(storage, job.document_id, job.event_id).await {
        Ok(event) => event,
        Err(MetadataMaterializationQueueError::MetadataCreateEventMissing { .. }) => {
            delete_dead_letter(
                storage,
                dead_letter_key(job.document_id, job.event_id).to_vec(),
            )
            .await?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    let status = MetadataMaterializationStatusRecord {
        failures: job.failures,
        ..new_pending_materialization_status(&event, unix_timestamp_millis())
    };
    let txn_id = start_write_transaction(storage).await?;
    let result = requeue_in_txn(storage, txn_id, &job, &status).await;
    match result {
        Ok(requeued) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(requeued)
        }
        Err(error) => {
            abort_storage_transaction_best_effort(
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

// The status guard is read inside the transaction: a newer event finishing
// between the read and the commit writes that same key, so the conflict manager
// aborts this requeue instead of letting it restore an obsolete event.
async fn requeue_in_txn(
    storage: &StorageHandle,
    txn_id: Ulid,
    job: &MetadataMaterializationJobRecord,
    status: &MetadataMaterializationStatusRecord,
) -> Result<bool, MetadataMaterializationQueueError> {
    let dead_letter_delete = vec![(
        METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE.to_string(),
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
            metadata_materialization_status_write_entry(status)?,
            metadata_materialization_job_write_entry(job)?,
            metadata_materialization_document_job_write_entry(job)?,
        ],
    )
    .await?;
    transactional_batch_delete(storage, txn_id, dead_letter_delete).await?;
    Ok(true)
}

async fn delete_dead_letter(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationQueueError> {
    delete_materialization_entries(
        storage,
        vec![(
            METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE.to_string(),
            ByteView::from(key),
        )],
    )
    .await
}

async fn transactional_batch_write(
    storage: &StorageHandle,
    txn_id: Ulid,
    writes: Vec<(String, ByteView, ByteView)>,
) -> Result<(), MetadataMaterializationQueueError> {
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn transactional_batch_delete(
    storage: &StorageHandle,
    txn_id: Ulid,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataMaterializationQueueError> {
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn enqueue_metadata_materialization_job(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let now = unix_timestamp_millis();
    let status = new_pending_materialization_status(event, now);
    let job = new_materialization_job(event, now);
    write_materialization_status_and_job(&context.storage_handle, &status, &job).await?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        match task_handle
            .send_effect(schedule_metadata_materialization_drain_effect())
            .await
        {
            Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(message));
            }
            other => {
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
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
) -> Result<Option<MetadataMaterializationJobRecord>, MetadataMaterializationQueueError> {
    let key = metadata_materialization_document_job_key(document_id, event_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            key: key.clone(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
            Ok(job) => Ok(Some(job)),
            Err(error) => {
                warn!(error = %error, document_id = %document_id, event_id = %event_id, "Deleting malformed metadata materialization document job");
                delete_materialization_document_job(storage, key.to_vec()).await?;
                Ok(None)
            }
        },
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

// The due index is due-ordered and its rows are valid only when the sidecar row
// exists with a matching due time. Rows resolve in slices of the outstanding
// limit, each slice costing three batch reads, and a page prunes its dead rows
// in one delete: a full batch costs O(slices) requests, not O(due jobs).
async fn scan_due_materialization_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<
    (
        Vec<(Vec<u8>, MetadataMaterializationJobRecord)>,
        bool,
        Option<u64>,
    ),
    MetadataMaterializationQueueError,
> {
    let mut start_after = None;
    let mut jobs = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: MATERIALIZATION_SCAN_PAGE_SIZE,
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
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        let mut stale = Vec::new();
        let mut candidates = Vec::new();
        let mut next_due_at_ms = None;
        for (key, _value) in values {
            let key = key.to_vec();
            let Some((due_at_ms, document_id, event_id)) = materialization_job_key_parts(&key)
            else {
                warn!(key = ?key, "Deleting malformed metadata materialization index row");
                stale.push((
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(key),
                ));
                continue;
            };
            if due_at_ms > now_ms {
                next_due_at_ms = Some(due_at_ms);
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
                jobs.push((metadata_materialization_job_key(&job).to_vec(), job));
                if jobs.len() >= limit {
                    delete_materialization_entries(storage, stale).await?;
                    return Ok((jobs, true, None));
                }
            }
        }
        delete_materialization_entries(storage, stale).await?;

        if let Some(due_at_ms) = next_due_at_ms {
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
) -> Result<(ScannedJobs, Vec<(String, ByteView)>), MetadataMaterializationQueueError> {
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
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(key.clone()),
                ));
                if sidecar.is_none() {
                    stale.push((
                        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                        metadata_materialization_document_job_key(*document_id, *event_id),
                    ));
                }
            }
        }
    }

    let (live, dead) = filter_live_jobs(storage, due).await?;
    for (key, job) in dead {
        stale.push((
            METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(key),
        ));
        stale.push((
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            metadata_materialization_document_job_key(job.document_id, job.event_id),
        ));
    }
    Ok((live, stale))
}

async fn read_document_jobs(
    storage: &StorageHandle,
    targets: &[(Ulid, Ulid)],
) -> Result<Vec<Option<MetadataMaterializationJobRecord>>, MetadataMaterializationQueueError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let reads = targets
        .iter()
        .map(|(document_id, event_id)| {
            (
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                metadata_materialization_document_job_key(*document_id, *event_id),
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
type ScannedJobs = Vec<(Vec<u8>, MetadataMaterializationJobRecord)>;

async fn filter_live_jobs(
    storage: &StorageHandle,
    jobs: ScannedJobs,
) -> Result<(ScannedJobs, ScannedJobs), MetadataMaterializationQueueError> {
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
                METADATA_EVENT_LOG_KEYSPACE.to_string(),
                metadata_event_log_key(job.document_id, job.event_id),
            )
        })
        .collect();
    let events = batch_read_values(storage, event_reads).await?;
    let mut live = Vec::with_capacity(jobs.len());
    let mut dead = Vec::new();
    for ((key, job), event) in jobs.into_iter().zip(events) {
        let advanced = statuses
            .get(&job.document_id)
            .is_some_and(|status| materialization_retry_already_advanced(status, &job));
        if event.is_none() || advanced {
            dead.push((key, job));
        } else {
            live.push((key, job));
        }
    }
    Ok((live, dead))
}

fn due_after(now_ms: u64, due_at_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
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

// Jobs that exhaust the failure budget park as a dead letter; others reschedule
// with backoff. The resulting row writes and deletes are folded into the
// per-batch finish txn.
fn defer_materialization_job(
    job_key: &[u8],
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    error: &MetadataMaterializationQueueError,
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
    job: MetadataMaterializationJobRecord,
    group: &GroupJobs,
    advanced_event_ids: &BTreeSet<Ulid>,
    raw_state_cache: &mut RawStateCache,
) -> Result<ProcessedMaterializationJob, MetadataMaterializationQueueError> {
    if older_job_exists(&context.storage_handle, group, &job, advanced_event_ids).await? {
        return Ok(ProcessedMaterializationJob::blocked());
    }
    let document_job_key =
        metadata_materialization_document_job_key(job.document_id, job.event_id).to_vec();

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
                    sync: None,
                },
                Duration::ZERO,
            ));
        }
        MaterializationJobObsolescence::RetryAdvanced => {
            delete_materialization_global_job(&context.storage_handle, job_key).await?;
            return Ok(ProcessedMaterializationJob::default());
        }
    }

    let event = match event {
        Ok(event) => event,
        Err(MetadataMaterializationQueueError::MetadataCreateEventMissing { .. }) => {
            return Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: None,
                    iri_index_writes: Vec::new(),
                    raw_state_write: None,
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
            let iri_index_writes = match project_materialized_iri_references(context, &event).await
            {
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
                    sync: Some(CompletedMaterializationSync {
                        graph_iri: event.record.graph_iri.clone(),
                        peers: event.record.holder_node_ids.clone(),
                    }),
                },
                craqle_elapsed,
            ))
        }
        Err(error) if is_terminal_materialization_error(&error) => {
            Ok(ProcessedMaterializationJob::completed(
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
                    sync: None,
                },
                craqle_elapsed,
            ))
        }
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
    pending: Vec<MetadataMaterializationJobRecord>,
    status: Option<MetadataMaterializationStatusRecord>,
}

// The sidecar keyspace is per-document and event-ordered, so this costs one
// prefix scan plus one status read for the whole group.
async fn load_group_jobs(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<GroupJobs, MetadataMaterializationQueueError> {
    let status = read_materialization_status(storage, document_id, None).await?;
    let prefix = metadata_materialization_document_job_prefix(document_id);
    let mut pending = Vec::new();
    let mut malformed = Vec::new();
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                prefix: Some(prefix.clone()),
                start: start_after.take().map(IterStart::After),
                limit: MATERIALIZATION_SCAN_PAGE_SIZE,
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
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) if job.document_id == document_id => pending.push(job),
                Ok(_) => {}
                Err(error) => {
                    warn!(error = %error, key = ?key.to_vec(), "Deleting malformed metadata materialization document job");
                    malformed.push((
                        METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                        key,
                    ));
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
    job: &MetadataMaterializationJobRecord,
    advanced_event_ids: &BTreeSet<Ulid>,
) -> Result<bool, MetadataMaterializationQueueError> {
    for pending in &group.pending {
        if pending.event_id >= job.event_id
            || advanced_event_ids.contains(&pending.event_id)
            || group
                .status
                .as_ref()
                .is_some_and(|status| materialization_status_obsoletes_job(status, pending))
        {
            continue;
        }
        if !materialization_event_exists(storage, pending).await? {
            warn!(document_id = %pending.document_id, event_id = %pending.event_id, "Deleting orphan metadata materialization job");
            delete_materialization_job(storage, metadata_materialization_job_key(pending).to_vec())
                .await?;
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
) -> Result<MetadataCreateEventRecord, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_EVENT_LOG_KEYSPACE.to_string(),
            key: metadata_event_log_key(document_id, event_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let event: MetadataCreateEventRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            if event.record.document_id != document_id || event.event_id != event_id {
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "metadata event log key mismatch for {document_id}/{event_id}"
                )));
            }
            Ok(event)
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Err(
            MetadataMaterializationQueueError::MetadataCreateEventMissing {
                document_id,
                event_id,
            },
        ),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn job_obsolescence(
    status: Option<&MetadataMaterializationStatusRecord>,
    job: &MetadataMaterializationJobRecord,
) -> MaterializationJobObsolescence {
    let Some(status) = status else {
        return MaterializationJobObsolescence::Live;
    };
    if materialization_status_obsoletes_job(status, job) {
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
) -> Result<Option<MetadataMaterializationStatusRecord>, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: metadata_materialization_status_key(document_id),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(
            postcard::from_bytes(&value).map_err(ConversionError::from)?,
        )),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn materialization_status_is_final(status: &MetadataMaterializationStatusRecord) -> bool {
    matches!(
        status.state,
        MetadataMaterializationState::Materialized | MetadataMaterializationState::Failed
    )
}

fn materialization_status_obsoletes_job(
    status: &MetadataMaterializationStatusRecord,
    job: &MetadataMaterializationJobRecord,
) -> bool {
    status.event_id >= job.event_id && materialization_status_is_final(status)
}

fn materialization_retry_already_advanced(
    status: &MetadataMaterializationStatusRecord,
    job: &MetadataMaterializationJobRecord,
) -> bool {
    materialization_status_obsoletes_job(status, job)
        || (status.event_id == job.event_id && status.attempts > job.attempts)
}

fn should_write_final_materialization_status(
    current: Option<&MetadataMaterializationStatusRecord>,
    next: &MetadataMaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || (current.event_id == next.event_id && current.attempts >= next.attempts)
            || (current.event_id == next.event_id && materialization_status_is_final(current))
    })
}

fn should_write_pending_retry_status(
    current: Option<&MetadataMaterializationStatusRecord>,
    next: &MetadataMaterializationStatusRecord,
) -> bool {
    !current.is_some_and(|current| {
        current.event_id > next.event_id
            || materialization_retry_already_advanced(
                current,
                &MetadataMaterializationJobRecord {
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

pub async fn metadata_materialization_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataMaterializationQueueError> {
    let mut start_after = None;
    loop {
        let (values, next_start_after) = match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
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
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };
        let Some((key, _value)) = values.into_iter().next() else {
            return Ok(false);
        };
        let key = key.to_vec();
        match materialization_job_key_parts(&key) {
            Some((due_at_ms, document_id, event_id)) => {
                match read_document_job(storage, document_id, event_id).await? {
                    Some(job) if job.due_at_ms == due_at_ms => {
                        if materialization_job_is_live(storage, &job).await? {
                            return Ok(true);
                        }
                        delete_materialization_job(storage, key).await?;
                    }
                    _ => delete_materialization_global_job(storage, key).await?,
                }
            }
            None => delete_materialization_global_job(storage, key).await?,
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
) -> Result<(), MetadataMaterializationQueueError> {
    let mut deletes = vec![(
        METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
        ByteView::from(key.clone()),
    )];
    if let Some((document_id, event_id)) = materialization_job_key_target(&key) {
        deletes.push((
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            metadata_materialization_document_job_key(document_id, event_id),
        ));
    }
    delete_materialization_entries(storage, deletes).await
}

async fn delete_materialization_global_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationQueueError> {
    delete_materialization_entries(
        storage,
        vec![(
            METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(key),
        )],
    )
    .await
}

async fn delete_materialization_document_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), MetadataMaterializationQueueError> {
    delete_materialization_entries(
        storage,
        vec![(
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            ByteView::from(key),
        )],
    )
    .await
}

async fn delete_materialization_entries(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataMaterializationQueueError> {
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn materialization_job_key_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    materialization_job_key_parts(key).map(|(_, document_id, event_id)| (document_id, event_id))
}

fn materialization_job_key_parts(key: &[u8]) -> Option<(u64, Ulid, Ulid)> {
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
) -> Result<bool, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: metadata_graph_lifecycle_key(graph_iri),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let record: MetadataGraphLifecycleRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            Ok(record.is_deleted())
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

struct MaterializedCreateEvent {
    raw_revision: Option<MetadataRawRevision>,
    raw_state_write: (String, ByteView, ByteView),
}

async fn materialize_create_event(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
    raw_state_cache: &mut RawStateCache,
) -> Result<MaterializedCreateEvent, MetadataMaterializationQueueError> {
    let raw_plan = crate::metadata::raw::prepare_raw_event(context, event, raw_state_cache).await?;
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationQueueError::MetadataHandleMissing)?;
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
            })
        }
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn project_materialized_iri_references(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, MetadataMaterializationQueueError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationQueueError::MetadataHandleMissing)?;
    let references = metadata_handle
        .snapshot_iri_references(event.record.graph_iri.clone())
        .await?;
    let records = super::iri_index::project_metadata_iri_references(
        event.record.document_id,
        event.event_id,
        references,
    );
    super::iri_index::metadata_iri_reference_write_entries(&records)
        .map_err(MetadataMaterializationQueueError::from)
}

fn graph_materialization_effect(
    event: &MetadataCreateEventRecord,
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
            request: MetadataApplyRoCrateRequest {
                graph_iri: event.record.graph_iri.clone(),
                jsonld: raw_revision.jsonld.clone(),
                policy,
                durability: MetadataRequestDurability::WalAlreadyDurable,
                deterministic_actor,
            },
        });
    }
    match &event.payload {
        MetadataCreateEventPayload::Scaffold {
            name,
            description,
            date_published,
            license,
        } => Effect::Metadata(MetadataEffect::CreateCrate {
            request: MetadataCreateCrateRequest {
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
        MetadataCreateEventPayload::RoCrate { jsonld }
        | MetadataCreateEventPayload::ReplaceRoCrate { jsonld } => {
            Effect::Metadata(MetadataEffect::ApplyRoCrate {
                request: MetadataApplyRoCrateRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    policy,
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataCreateEventPayload::UpsertDataEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertDataEntity {
                request: aruna_core::metadata::MetadataUpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
        MetadataCreateEventPayload::UpsertContextualEntity { jsonld } => {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity {
                request: aruna_core::metadata::MetadataUpsertEntityRequest {
                    graph_iri: event.record.graph_iri.clone(),
                    jsonld: jsonld.clone(),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor,
                },
            })
        }
    }
}

fn materialization_success_status(
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    raw_revision: Option<&MetadataRawRevision>,
) -> MetadataMaterializationStatusRecord {
    MetadataMaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: raw_revision.map(|revision| revision.context_digest),
        dataset_digest: raw_revision.and_then(|revision| revision.dataset_digest),
        state: MetadataMaterializationState::Materialized,
        attempts: job.attempts.saturating_add(1),
        failures: job.failures,
        last_error: None,
        updated_at_ms: unix_timestamp_millis(),
    }
}

fn materialization_failure_status(
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    error: String,
    failures: u32,
    terminal: bool,
) -> MetadataMaterializationStatusRecord {
    MetadataMaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        context_digest: None,
        dataset_digest: None,
        state: if terminal {
            MetadataMaterializationState::Failed
        } else {
            MetadataMaterializationState::Pending
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
    error: &MetadataMaterializationQueueError,
) -> MaterializationFailureKind {
    match error {
        MetadataMaterializationQueueError::Metadata(
            MetadataError::InvalidInput(_) | MetadataError::Validation(_),
        ) => MaterializationFailureKind::Terminal,
        // A storage failure or an off-contract adapter event is never the
        // document's fault, so it must not spend the budget that parks a job.
        MetadataMaterializationQueueError::Storage(_)
        | MetadataMaterializationQueueError::UnexpectedEvent(_)
        | MetadataMaterializationQueueError::Metadata(
            MetadataError::ChannelClosed
            | MetadataError::TaskJoin(_)
            | MetadataError::HandleMissing
            | MetadataError::Persist(_)
            | MetadataError::Storage(_),
        )
        | MetadataMaterializationQueueError::MetadataHandleMissing => {
            MaterializationFailureKind::Transient
        }
        _ => MaterializationFailureKind::Application,
    }
}

fn is_terminal_materialization_error(error: &MetadataMaterializationQueueError) -> bool {
    matches!(
        materialization_failure_kind(error),
        MaterializationFailureKind::Terminal
    )
}

async fn write_materialization_status_and_job(
    storage: &StorageHandle,
    status: &MetadataMaterializationStatusRecord,
    job: &MetadataMaterializationJobRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let writes = vec![
        metadata_materialization_status_write_entry(status)?,
        metadata_materialization_job_write_entry(job)?,
        metadata_materialization_document_job_write_entry(job)?,
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
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn materialization_event_exists(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<bool, MetadataMaterializationQueueError> {
    match read_create_event(storage, job.document_id, job.event_id).await {
        Ok(_) => Ok(true),
        Err(MetadataMaterializationQueueError::MetadataCreateEventMissing { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

async fn materialization_job_is_live(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<bool, MetadataMaterializationQueueError> {
    if !materialization_event_exists(storage, job).await? {
        return Ok(false);
    }
    let status = read_materialization_status(storage, job.document_id, None).await?;
    Ok(!status
        .as_ref()
        .is_some_and(|status| materialization_retry_already_advanced(status, job)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::keyspaces::{
        METADATA_IRI_REFERENCE_INDEX_KEYSPACE, METADATA_RAW_REVISION_KEYSPACE,
    };
    use aruna_core::storage_entries::{metadata_create_event_write_entry, raw_revision_key};
    use aruna_core::structs::{MetadataRegistryRecord, PlacementRef, RealmId};
    use aruna_storage::{FjallStorage, StorageHandle};
    use std::collections::BTreeSet;
    use std::thread;
    use tempfile::tempdir;

    #[tokio::test]
    async fn delete_waits_fence() {
        let graph_iri = "urn:test:graph-fence";
        let held = metadata_graph_fence(graph_iri)
            .acquire()
            .await
            .expect("graph fence remains open");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = ready_tx.send(());
            let _permit = metadata_graph_fence(graph_iri)
                .acquire()
                .await
                .expect("graph fence remains open");
            let _ = done_tx.send(());
        });

        ready_rx.await.expect("delete task started");
        assert!(matches!(
            done_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        drop(held);
        done_rx.await.expect("delete task completed");
        task.await.expect("delete task joined");
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn create_event(document_id: Ulid, event_id: Ulid, name: &str) -> MetadataCreateEventRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let group_id = Ulid::from_parts(7, 1);
        let document_path = format!("datasets/{name}");
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: document_path.clone(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                &document_path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: event_id,
            last_event_id: event_id,
        };
        MetadataCreateEventRecord {
            event_id,
            record,
            user_id: aruna_core::UserId::local(Ulid::from_parts(7, 2), realm_id),
            node_id: node(1),
            payload: MetadataCreateEventPayload::Scaffold {
                name: name.to_string(),
                description: "Materialization test".to_string(),
                date_published: "2026-01-01".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
            occurred_at_ms: 1,
        }
    }

    fn with_payload(
        mut event: MetadataCreateEventRecord,
        payload: MetadataCreateEventPayload,
    ) -> MetadataCreateEventRecord {
        event.payload = payload;
        event
    }

    async fn write_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn storage_key_exists(storage: &StorageHandle, key_space: &str, key: Vec<u8>) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    // Loads the document's group first, exactly as the drain does before it
    // checks a job's predecessors.
    async fn older_exists(
        storage: &StorageHandle,
        document_id: Ulid,
        event_id: Ulid,
        advanced: &BTreeSet<Ulid>,
    ) -> Result<bool, MetadataMaterializationQueueError> {
        let group = load_group_jobs(storage, document_id).await?;
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 0,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        older_job_exists(storage, &group, &job, advanced).await
    }

    async fn index_job_keys(storage: &StorageHandle) -> Vec<(u64, Ulid, Ulid)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 4096,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .filter_map(|(key, _)| materialization_job_key_parts(&key))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn corrupt_materialization_job_only_is_deleted() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let corrupt_key = vec![0];
        write_entries(
            &storage,
            vec![(
                METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(vec![1, 2, 3]),
            )],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_metadata_materialization_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(
            !storage_key_exists(&storage, METADATA_MATERIALIZATION_JOB_KEYSPACE, corrupt_key).await
        );
    }

    #[tokio::test]
    async fn materialization_jobs_exist_deletes_corrupt_before_valid() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let corrupt_key = vec![0];
        let event = create_event(
            Ulid::from_bytes([12u8; 16]),
            Ulid::from_parts(12, 1),
            "valid",
        );
        let valid_job = new_materialization_job(&event, 1);
        write_entries(
            &storage,
            vec![
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(corrupt_key.clone()),
                    ByteView::from(vec![1, 2, 3]),
                ),
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&valid_job).expect("job entry"),
                metadata_materialization_document_job_write_entry(&valid_job).expect("sidecar"),
            ],
        )
        .await;

        assert!(metadata_materialization_jobs_exist(&storage).await.unwrap());
        assert!(
            !storage_key_exists(&storage, METADATA_MATERIALIZATION_JOB_KEYSPACE, corrupt_key).await
        );
    }

    #[tokio::test]
    async fn corrupt_global_materialization_job_deletes_document_sidecar() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([14u8; 16]);
        let old_event_id = Ulid::from_parts(14, 1);
        let newer_event_id = Ulid::from_parts(14, 2);
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let (_, global_key, _) = metadata_materialization_job_write_entry(&old_job).unwrap();
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    global_key.clone(),
                    ByteView::from(vec![1, 2, 3]),
                ),
                metadata_materialization_document_job_write_entry(&old_job).unwrap(),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_metadata_materialization_batch(&context)
            .await
            .expect("corrupt global job drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                global_key.to_vec()
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                document_key.to_vec()
            )
            .await
        );
        assert!(
            !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn orphan_malformed_document_sidecar_is_deleted_while_checking_predecessors() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([16u8; 16]);
        let old_event_id = Ulid::from_parts(16, 1);
        let newer_event_id = Ulid::from_parts(16, 2);
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![(
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                document_key.clone(),
                ByteView::from(vec![1, 2, 3]),
            )],
        )
        .await;

        assert!(
            !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                document_key.to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn orphan_valid_document_sidecar_is_deleted_while_checking_predecessors() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([17u8; 16]);
        let old_event_id = Ulid::from_parts(17, 1);
        let newer_event_id = Ulid::from_parts(17, 2);
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![metadata_materialization_document_job_write_entry(&old_job).unwrap()],
        )
        .await;

        assert!(
            !older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                document_key.to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn orphan_global_materialization_job_is_deleted() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([22u8; 16]);
        let event_id = Ulid::from_parts(22, 1);
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let global_key = metadata_materialization_job_key(&job);
        let document_key = metadata_materialization_document_job_key(document_id, event_id);
        write_entries(
            &storage,
            vec![
                metadata_materialization_job_write_entry(&job).unwrap(),
                metadata_materialization_document_job_write_entry(&job).unwrap(),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_metadata_materialization_batch(&context)
            .await
            .expect("orphan global job drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                global_key.to_vec()
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                document_key.to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn corrupt_materialization_document_job_is_deleted_while_checking_predecessors() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([13u8; 16]);
        let event_id = Ulid::from_bytes([255u8; 16]);
        let mut corrupt_key = document_id.to_bytes().to_vec();
        corrupt_key.push(0);
        write_entries(
            &storage,
            vec![(
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                ByteView::from(corrupt_key.clone()),
                ByteView::from(vec![1, 2, 3]),
            )],
        )
        .await;

        assert!(
            !older_exists(&storage, document_id, event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                corrupt_key
            )
            .await
        );
    }

    #[tokio::test]
    async fn scan_stops_early() {
        // Due jobs sort before future jobs, so the scan returns at the first
        // future key without paging the whole keyspace.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let document_id = Ulid::from_bytes([40u8; 16]);
        let due_event = Ulid::from_parts(1, 1);
        let event = create_event(document_id, due_event, "due");
        let due_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: due_event,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let mut writes = vec![
            metadata_create_event_write_entry(&event).unwrap(),
            metadata_materialization_job_write_entry(&due_job).unwrap(),
            metadata_materialization_document_job_write_entry(&due_job).unwrap(),
        ];
        for index in 0..600u64 {
            let future_job = MetadataMaterializationJobRecord {
                document_id: Ulid::from_bytes([41u8; 16]),
                event_id: Ulid::from_parts(2, u128::from(index)),
                due_at_ms: now_ms.saturating_add(60_000).saturating_add(index),
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            writes.push(metadata_materialization_job_write_entry(&future_job).unwrap());
        }
        write_entries(&storage, writes).await;

        let before = storage.snapshot_metrics().requests_total;
        let (jobs, has_more_due, next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
                .await
                .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        assert_eq!(
            jobs,
            vec![(metadata_materialization_job_key(&due_job).to_vec(), due_job)]
        );
        assert!(!has_more_due);
        assert!(next_due_at_ms.is_some());
        assert!(delta <= 10, "scan issued {delta} storage requests");
    }

    #[tokio::test]
    async fn scan_batches_reads() {
        // A page of due jobs resolves in a handful of batch reads instead of a
        // sidecar, event and status read per row.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let mut writes = Vec::new();
        for index in 0..64u8 {
            let document_id = Ulid::from_bytes([index; 16]);
            let event_id = Ulid::from_parts(1, u128::from(index));
            let event = create_event(document_id, event_id, "batched");
            let job = MetadataMaterializationJobRecord {
                document_id,
                event_id,
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            writes.push(metadata_create_event_write_entry(&event).unwrap());
            writes.push(metadata_materialization_job_write_entry(&job).unwrap());
            writes.push(metadata_materialization_document_job_write_entry(&job).unwrap());
        }
        write_entries(&storage, writes).await;

        let before = storage.snapshot_metrics().requests_total;
        let (jobs, _, _) =
            scan_due_materialization_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
                .await
                .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        assert_eq!(jobs.len(), 64);
        assert!(delta <= 8, "scan issued {delta} storage requests");
    }

    #[tokio::test]
    async fn probe_reads_one() {
        // The timer probe asks for a single due job, so it must not resolve the
        // whole page of index rows behind it.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut writes = Vec::new();
        for index in 0..64u8 {
            let document_id = Ulid::from_bytes([100 + index; 16]);
            let event_id = Ulid::from_parts(1, u128::from(index));
            let event = create_event(document_id, event_id, "probe");
            let job = MetadataMaterializationJobRecord {
                document_id,
                event_id,
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            writes.push(metadata_create_event_write_entry(&event).unwrap());
            writes.push(metadata_materialization_job_write_entry(&job).unwrap());
            writes.push(metadata_materialization_document_job_write_entry(&job).unwrap());
        }
        write_entries(&storage, writes).await;

        let before = storage.snapshot_metrics().requests_total;
        let after = next_metadata_materialization_timer_after(&storage)
            .await
            .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        assert_eq!(after, Some(Duration::ZERO));
        assert!(delta <= 6, "probe issued {delta} storage requests");
    }

    #[tokio::test]
    async fn stale_index_pruned() {
        // An index row with no sidecar, and one whose sidecar due time differs,
        // are both deleted and yield no job.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let orphan = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([42u8; 16]),
            event_id: Ulid::from_parts(1, 1),
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let mismatched = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([43u8; 16]),
            event_id: Ulid::from_parts(1, 2),
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let mismatched_sidecar = MetadataMaterializationJobRecord {
            due_at_ms: 999,
            ..mismatched.clone()
        };
        let orphan_key = metadata_materialization_job_key(&orphan);
        let mismatched_key = metadata_materialization_job_key(&mismatched);
        write_entries(
            &storage,
            vec![
                metadata_materialization_job_write_entry(&orphan).unwrap(),
                metadata_materialization_job_write_entry(&mismatched).unwrap(),
                metadata_materialization_document_job_write_entry(&mismatched_sidecar).unwrap(),
            ],
        )
        .await;

        let (jobs, has_more_due, _next) =
            scan_due_materialization_jobs(&storage, now_ms, MATERIALIZATION_BATCH_SIZE)
                .await
                .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                orphan_key.to_vec()
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                mismatched_key.to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn older_check_bounded() {
        // The predecessor check for one document must not scan the sidecar rows
        // of unrelated documents.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([44u8; 16]);
        let first = Ulid::from_parts(1, 1);
        let middle = Ulid::from_parts(2, 1);
        let last = Ulid::from_parts(3, 1);
        let first_event = create_event(document_id, first, "first");
        let job_for = |event_id: Ulid| MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let mut writes = vec![
            metadata_create_event_write_entry(&first_event).unwrap(),
            metadata_materialization_document_job_write_entry(&job_for(first)).unwrap(),
            metadata_materialization_document_job_write_entry(&job_for(middle)).unwrap(),
            metadata_materialization_document_job_write_entry(&job_for(last)).unwrap(),
        ];
        for index in 0..500u64 {
            let other = MetadataMaterializationJobRecord {
                document_id: Ulid::from_parts(9, u128::from(index)),
                event_id: Ulid::from_parts(9, u128::from(index)),
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            writes.push(metadata_materialization_document_job_write_entry(&other).unwrap());
        }
        write_entries(&storage, writes).await;

        let before = storage.snapshot_metrics().requests_total;
        let older = older_exists(&storage, document_id, middle, &BTreeSet::new())
            .await
            .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        assert!(older);
        assert!(delta <= 10, "older check issued {delta} storage requests");
    }

    fn application_failure() -> MetadataMaterializationQueueError {
        MetadataMaterializationQueueError::Metadata(MetadataError::Backend("boom".to_string()))
    }

    #[tokio::test]
    async fn failure_cap_parks() {
        // The apply that spends the last of the failure budget parks as a dead
        // letter: both job rows go, the record that can requeue them stays.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([45u8; 16]);
        let event_id = Ulid::from_parts(1, 1);
        let event = create_event(document_id, event_id, "capped");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES - 1,
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
        };
        let index_key = metadata_materialization_job_key(&job);
        let sidecar_key = metadata_materialization_document_job_key(document_id, event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&job).unwrap(),
                metadata_materialization_document_job_write_entry(&job).unwrap(),
            ],
        )
        .await;

        let parked =
            defer_materialization_job(index_key.as_ref(), &job, &event, &application_failure());
        assert!(matches!(parked, FinishedMaterializationJob::Parked { .. }));
        finish_completed_materialization_jobs(&storage, vec![parked])
            .await
            .unwrap();

        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                index_key.to_vec()
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE,
                sidecar_key.to_vec()
            )
            .await
        );
        let status = read_materialization_status(&storage, document_id, None)
            .await
            .unwrap()
            .expect("failed status is written");
        assert_eq!(status.state, MetadataMaterializationState::Failed);
        assert_eq!(status.failures, MATERIALIZATION_MAX_FAILURES);
        assert!(!metadata_materialization_jobs_exist(&storage).await.unwrap());
        let dead_letter = read_dead_letter(&storage, document_id, event_id)
            .await
            .unwrap()
            .expect("dead letter is written");
        assert_eq!(dead_letter.parks, 1);
        assert!(dead_letter.requeue_at_ms > unix_timestamp_millis());
    }

    #[test]
    fn cap_boundary_reschedules() {
        // Below the cap reschedules with failures advanced; at the cap parks.
        let document_id = Ulid::from_bytes([46u8; 16]);
        let event_id = Ulid::from_parts(2, 1);
        let event = create_event(document_id, event_id, "boundary");
        let reschedule = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES - 2,
            failures: MATERIALIZATION_MAX_FAILURES - 2,
            parks: 0,
        };
        let key = metadata_materialization_job_key(&reschedule);
        match defer_materialization_job(key.as_ref(), &reschedule, &event, &application_failure()) {
            FinishedMaterializationJob::Rescheduled { status, .. } => {
                assert_eq!(status.failures, MATERIALIZATION_MAX_FAILURES - 1);
            }
            other => panic!("expected reschedule, got {other:?}"),
        }
        let park = MetadataMaterializationJobRecord {
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
            ..reschedule
        };
        assert!(matches!(
            defer_materialization_job(key.as_ref(), &park, &event, &application_failure()),
            FinishedMaterializationJob::Parked { .. }
        ));
    }

    #[test]
    fn transient_skips_cap() {
        // A storage timeout is infrastructure: it advances the retry backoff but
        // must never spend the failure budget that parks a job.
        let document_id = Ulid::from_bytes([47u8; 16]);
        let event_id = Ulid::from_parts(3, 1);
        let event = create_event(document_id, event_id, "transient");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES + 5,
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
        };
        let key = metadata_materialization_job_key(&job);
        let errors = [
            MetadataMaterializationQueueError::Storage(StorageError::Timeout),
            MetadataMaterializationQueueError::Storage(StorageError::TransactionConflict),
            // A failed durability flush runs after every apply; charging it would
            // park documents whenever the disk is the bottleneck.
            MetadataMaterializationQueueError::Metadata(MetadataError::Persist(
                "journal".to_string(),
            )),
        ];
        for error in errors {
            match defer_materialization_job(key.as_ref(), &job, &event, &error) {
                FinishedMaterializationJob::Rescheduled { status, .. } => {
                    assert_eq!(status.failures, job.failures);
                    assert_eq!(status.attempts, job.attempts + 1);
                }
                other => panic!("expected reschedule, got {other:?}"),
            }
        }
    }

    #[test]
    fn iri_failure_transient() {
        // A bulk-lane QueueFull surfaced through the IRI index keeps its storage
        // cause, so it defers without spending the failure budget.
        let document_id = Ulid::from_bytes([51u8; 16]);
        let event_id = Ulid::from_parts(5, 1);
        let event = create_event(document_id, event_id, "indexed");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
        };
        let key = metadata_materialization_job_key(&job);
        let error = MetadataMaterializationQueueError::from(MetadataIriIndexError::Storage(
            StorageError::QueueFull,
        ));
        match defer_materialization_job(key.as_ref(), &job, &event, &error) {
            FinishedMaterializationJob::Rescheduled { status, .. } => {
                assert_eq!(status.failures, job.failures);
            }
            other => panic!("expected reschedule, got {other:?}"),
        }
    }

    #[test]
    fn handle_failure_transient() {
        // The lifecycle read and the craqle apply reach the queue through the
        // metadata handle; an overloaded lane or a failed disk write there must
        // not spend the budget reserved for failures the document caused.
        let document_id = Ulid::from_bytes([53u8; 16]);
        let event_id = Ulid::from_parts(7, 1);
        let event = create_event(document_id, event_id, "handle");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
        };
        let key = metadata_materialization_job_key(&job);
        let errors = [
            MetadataMaterializationQueueError::Metadata(MetadataError::Storage(
                StorageError::QueueFull,
            )),
            MetadataMaterializationQueueError::Metadata(MetadataError::Storage(
                StorageError::Timeout,
            )),
        ];
        for error in errors {
            match defer_materialization_job(key.as_ref(), &job, &event, &error) {
                FinishedMaterializationJob::Rescheduled { status, .. } => {
                    assert_eq!(status.failures, job.failures);
                }
                other => panic!("expected reschedule, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn deadletters_requeue_due() {
        // A due dead letter returns to the queue with one failure of budget left
        // and a pending status; one that is not due yet stays parked.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([48u8; 16]);
        let event_id = Ulid::from_parts(4, 1);
        let event = create_event(document_id, event_id, "requeue");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES,
            failures: MATERIALIZATION_MAX_FAILURES,
            parks: 0,
        };
        let pending = MetadataMaterializationDeadLetterRecord {
            job: job.clone(),
            last_error: "boom".to_string(),
            parked_at_ms: 1,
            parks: 1,
            requeue_at_ms: unix_timestamp_millis().saturating_add(600_000),
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                dead_letter_entry(&pending).unwrap(),
            ],
        )
        .await;
        assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 0);

        let due = MetadataMaterializationDeadLetterRecord {
            requeue_at_ms: 1,
            ..pending
        };
        write_entries(&storage, vec![dead_letter_entry(&due).unwrap()]).await;
        assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 1);

        assert!(
            read_dead_letter(&storage, document_id, event_id)
                .await
                .unwrap()
                .is_none()
        );
        let requeued = read_document_job(&storage, document_id, event_id)
            .await
            .unwrap()
            .expect("job is requeued");
        assert_eq!(requeued.failures, MATERIALIZATION_MAX_FAILURES - 1);
        assert_eq!(requeued.attempts, 0);
        let status = read_materialization_status(&storage, document_id, None)
            .await
            .unwrap()
            .expect("status is reset");
        assert_eq!(status.state, MetadataMaterializationState::Pending);
        assert!(metadata_materialization_jobs_exist(&storage).await.unwrap());
    }

    #[tokio::test]
    async fn requeued_parks_fast() {
        // A requeued dead letter carries one failure of budget, so a document
        // that is still poisonous re-parks after a single apply.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([53u8; 16]);
        let event_id = Ulid::from_parts(6, 1);
        let event = create_event(document_id, event_id, "poison");
        let parked = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES,
            failures: MATERIALIZATION_MAX_FAILURES,
            parks: 0,
        };
        let due = MetadataMaterializationDeadLetterRecord {
            job: parked,
            last_error: "boom".to_string(),
            parked_at_ms: 1,
            parks: 1,
            requeue_at_ms: 1,
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                dead_letter_entry(&due).unwrap(),
            ],
        )
        .await;
        assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 1);

        let requeued = read_document_job(&storage, document_id, event_id)
            .await
            .unwrap()
            .expect("job is requeued");
        let key = metadata_materialization_job_key(&requeued);
        let FinishedMaterializationJob::Parked { job, status, .. } =
            defer_materialization_job(key.as_ref(), &requeued, &event, &application_failure())
        else {
            panic!("expected park");
        };

        // The requeue deleted the dead letter that held the park count, so the
        // job carries it: the second park must wait longer than the first.
        let reparked = parked_dead_letter(&job, &status, None);
        assert_eq!(reparked.parks, due.parks + 1);
        assert!(
            reparked.requeue_at_ms.saturating_sub(reparked.parked_at_ms)
                > requeue_after_ms(due.parks)
        );
    }

    // A status that already materialized `event_id`, as a newer event would.
    fn materialized_status(
        document_id: Ulid,
        event: &MetadataCreateEventRecord,
    ) -> MetadataMaterializationStatusRecord {
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id: event.event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        materialization_success_status(&job, event, None)
    }

    #[tokio::test]
    async fn deadletter_drops_superseded() {
        // Requeueing a dead letter whose document has since materialized a newer
        // event would regress the projection, so the dead letter is dropped.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([54u8; 16]);
        let old_event_id = Ulid::from_parts(1, 1);
        let newer_event_id = Ulid::from_parts(2, 1);
        let old_event = create_event(document_id, old_event_id, "old");
        let newer_event = create_event(document_id, newer_event_id, "newer");
        let newer_status = materialized_status(document_id, &newer_event);
        let due = MetadataMaterializationDeadLetterRecord {
            job: MetadataMaterializationJobRecord {
                document_id,
                event_id: old_event_id,
                due_at_ms: 1,
                attempts: MATERIALIZATION_MAX_FAILURES,
                failures: MATERIALIZATION_MAX_FAILURES,
                parks: 0,
            },
            last_error: "boom".to_string(),
            parked_at_ms: 1,
            parks: 1,
            requeue_at_ms: 1,
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&old_event).unwrap(),
                metadata_create_event_write_entry(&newer_event).unwrap(),
                metadata_materialization_status_write_entry(&newer_status).unwrap(),
                dead_letter_entry(&due).unwrap(),
            ],
        )
        .await;

        assert_eq!(requeue_dead_letters(&storage).await.unwrap(), 0);

        assert!(
            read_dead_letter(&storage, document_id, old_event_id)
                .await
                .unwrap()
                .is_none()
        );
        let status = read_materialization_status(&storage, document_id, None)
            .await
            .unwrap()
            .expect("status survives");
        assert_eq!(status.event_id, newer_event_id);
        assert_eq!(status.state, MetadataMaterializationState::Materialized);
        assert!(
            read_document_job(&storage, document_id, old_event_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn requeue_aborts_raced() {
        // A newer event finishing while the requeue is open must beat it: the
        // requeue would otherwise reinstate an obsolete event as pending and
        // delete nothing, leaving the document projected from stale state.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([56u8; 16]);
        let old_event_id = Ulid::from_parts(1, 1);
        let newer_event_id = Ulid::from_parts(2, 1);
        let old_event = create_event(document_id, old_event_id, "old");
        let newer_event = create_event(document_id, newer_event_id, "newer");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES,
            failures: MATERIALIZATION_MAX_FAILURES,
            parks: 1,
        };
        let status = MetadataMaterializationStatusRecord {
            failures: job.failures,
            ..new_pending_materialization_status(&old_event, 1)
        };
        write_entries(
            &storage,
            vec![metadata_create_event_write_entry(&old_event).unwrap()],
        )
        .await;

        let txn_id = start_write_transaction(&storage).await.unwrap();
        assert!(
            requeue_in_txn(&storage, txn_id, &job, &status)
                .await
                .unwrap()
        );

        let racing = start_write_transaction(&storage).await.unwrap();
        transactional_batch_write(
            &storage,
            racing,
            vec![
                metadata_materialization_status_write_entry(&materialized_status(
                    document_id,
                    &newer_event,
                ))
                .unwrap(),
            ],
        )
        .await
        .unwrap();
        commit_storage_transaction(&storage, racing).await.unwrap();

        assert!(commit_storage_transaction(&storage, txn_id).await.is_err());
        let current = read_materialization_status(&storage, document_id, None)
            .await
            .unwrap()
            .expect("racing status survives");
        assert_eq!(current.event_id, newer_event_id);
        assert!(
            read_document_job(&storage, document_id, old_event_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn park_skips_superseded() {
        // Parking a job the document has already moved past must not leave a
        // dead letter that a later sweep could resurrect.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([55u8; 16]);
        let old_event_id = Ulid::from_parts(1, 1);
        let newer_event_id = Ulid::from_parts(2, 1);
        let old_event = create_event(document_id, old_event_id, "old");
        let newer_event = create_event(document_id, newer_event_id, "newer");
        let newer_status = materialized_status(document_id, &newer_event);
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: MATERIALIZATION_MAX_FAILURES - 1,
            failures: MATERIALIZATION_MAX_FAILURES - 1,
            parks: 0,
        };
        let job_key = metadata_materialization_job_key(&job);
        write_entries(
            &storage,
            vec![
                metadata_materialization_status_write_entry(&newer_status).unwrap(),
                metadata_materialization_job_write_entry(&job).unwrap(),
                metadata_materialization_document_job_write_entry(&job).unwrap(),
            ],
        )
        .await;

        let parked =
            defer_materialization_job(job_key.as_ref(), &job, &old_event, &application_failure());
        assert!(matches!(parked, FinishedMaterializationJob::Parked { .. }));
        let plan = plan_finish_chunk(&storage, vec![parked]).await.unwrap();

        assert!(
            !plan
                .writes
                .iter()
                .any(|(key_space, _, _)| key_space == METADATA_MATERIALIZATION_DEAD_LETTER_KEYSPACE)
        );
        assert!(plan.deletes.contains(&(
            METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            job_key.clone()
        )));
        assert!(plan.deletes.contains(&(
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            metadata_materialization_document_job_key(document_id, old_event_id)
        )));
    }

    #[test]
    fn graph_materialization_effect_uses_event_id_actor_and_wal_durability() {
        let document_id = Ulid::from_bytes([1u8; 16]);
        let event_id = Ulid::from_parts(1, 1);
        let event = create_event(document_id, event_id, "deterministic");
        let deterministic_actor = Some(deterministic_materialization_actor(event_id));

        match graph_materialization_effect(&event, None, false) {
            Effect::Metadata(MetadataEffect::CreateCrate { request }) => {
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }

        let rocrate = with_payload(
            event.clone(),
            MetadataCreateEventPayload::RoCrate {
                jsonld: "{}".to_string(),
            },
        );
        match graph_materialization_effect(&rocrate, None, false) {
            Effect::Metadata(MetadataEffect::ApplyRoCrate { request }) => {
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }

        let data = with_payload(
            event.clone(),
            MetadataCreateEventPayload::UpsertDataEntity {
                jsonld: r#"{"@id":"./file.txt","@type":"File","name":"file"}"#.to_string(),
            },
        );
        match graph_materialization_effect(&data, None, false) {
            Effect::Metadata(MetadataEffect::UpsertDataEntity { request }) => {
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }

        let contextual = with_payload(
            event,
            MetadataCreateEventPayload::UpsertContextualEntity {
                jsonld: r##"{"@id":"#lab","@type":"Organization","name":"lab"}"##.to_string(),
            },
        );
        match graph_materialization_effect(&contextual, None, false) {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity { request }) => {
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }

        let raw_revision = MetadataRawRevision {
            jsonld: r#"{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[]}"#
                .to_string(),
            winning_event_id: event_id,
            context_digest: [1; 32],
            dataset_digest: Some([2; 32]),
        };
        match graph_materialization_effect(&contextual, Some(&raw_revision), true) {
            Effect::Metadata(MetadataEffect::ApplyRoCrate { request }) => {
                assert_eq!(request.jsonld, raw_revision.jsonld);
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }
        assert!(matches!(
            graph_materialization_effect(&contextual, Some(&raw_revision), false),
            Effect::Metadata(MetadataEffect::UpsertContextualEntity { .. })
        ));
    }

    #[test]
    fn replaying_same_materialization_event_is_graph_idempotent() {
        let document_id = Ulid::from_bytes([2u8; 16]);
        let event_id = Ulid::from_parts(2, 1);
        let event = create_event(document_id, event_id, "replay");
        let data = with_payload(
            event.clone(),
            MetadataCreateEventPayload::UpsertDataEntity {
                jsonld: r#"{"@id":"./file.txt","@type":"File","name":"file"}"#.to_string(),
            },
        );

        for event in [event, data] {
            assert_eq!(
                graph_materialization_effect(&event, None, false),
                graph_materialization_effect(&event, None, false)
            );
        }
    }

    #[test]
    fn newer_pending_status_does_not_obsolete_older_job() {
        let document_id = Ulid::from_bytes([8u8; 16]);
        let older_event_id = Ulid::from_parts(8, 1);
        let newer_event_id = Ulid::from_parts(8, 2);
        let older_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: older_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        let newer_final = MetadataMaterializationStatusRecord {
            state: MetadataMaterializationState::Materialized,
            ..newer_pending.clone()
        };

        assert!(!materialization_status_obsoletes_job(
            &newer_pending,
            &older_job
        ));
        assert!(materialization_status_obsoletes_job(
            &newer_final,
            &older_job
        ));
    }

    #[test]
    fn older_retry_status_does_not_regress_newer_pending_status() {
        let document_id = Ulid::from_bytes([9u8; 16]);
        let older_event_id = Ulid::from_parts(9, 1);
        let newer_event_id = Ulid::from_parts(9, 2);
        let older_retry = MetadataMaterializationStatusRecord {
            document_id,
            event_id: older_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 1,
            failures: 0,
            last_error: Some("transient".to_string()),
            updated_at_ms: 1,
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 2,
        };

        assert!(!should_write_pending_retry_status(
            Some(&newer_pending),
            &older_retry
        ));
        assert!(should_write_pending_retry_status(None, &older_retry));
    }

    #[test]
    fn stale_final_status_does_not_overwrite_same_event_retry_status() {
        let document_id = Ulid::from_bytes([29u8; 16]);
        let event_id = Ulid::from_parts(29, 1);
        let retry_status = MetadataMaterializationStatusRecord {
            document_id,
            event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 1,
            failures: 0,
            last_error: Some("transient".to_string()),
            updated_at_ms: 1,
        };
        let stale_final = MetadataMaterializationStatusRecord {
            state: MetadataMaterializationState::Materialized,
            last_error: None,
            updated_at_ms: 2,
            ..retry_status.clone()
        };
        let fresh_final = MetadataMaterializationStatusRecord {
            attempts: 2,
            ..stale_final.clone()
        };

        assert!(!should_write_final_materialization_status(
            Some(&retry_status),
            &stale_final
        ));
        assert!(should_write_final_materialization_status(
            Some(&retry_status),
            &fresh_final
        ));
    }

    // Rescheduled jobs for `count` distinct documents, rows already persisted.
    async fn reschedule_batch(
        storage: &StorageHandle,
        count: usize,
    ) -> Vec<FinishedMaterializationJob> {
        let mut finished = Vec::new();
        for seed in 0..count {
            let document_id = Ulid::from_parts(900, seed as u128);
            let event_id = Ulid::from_parts(1, seed as u128);
            let event = create_event(document_id, event_id, "chunk");
            let job = MetadataMaterializationJobRecord {
                document_id,
                event_id,
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            write_entries(
                storage,
                vec![
                    metadata_materialization_job_write_entry(&job).unwrap(),
                    metadata_materialization_document_job_write_entry(&job).unwrap(),
                ],
            )
            .await;
            let key = metadata_materialization_job_key(&job);
            finished.push(defer_materialization_job(
                key.as_ref(),
                &job,
                &event,
                &application_failure(),
            ));
        }
        finished
    }

    #[tokio::test]
    async fn finish_commits_chunks() {
        // More jobs than one chunk holds still resolve completely, and they do
        // so in several transactions so a late failure cannot undo early work.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let count = MATERIALIZATION_FINISH_CHUNK + 3;
        let finished = reschedule_batch(&storage, count).await;

        let before = storage.snapshot_metrics().requests_total;
        finish_completed_materialization_jobs(&storage, finished)
            .await
            .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        // Two chunks, each a handful of requests: far below one transaction per
        // job, and each chunk commits on its own.
        assert!(delta <= 16, "finish issued {delta} storage requests");
        assert_eq!(index_job_keys(&storage).await.len(), count);
        assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    }

    #[tokio::test]
    async fn finish_avoids_conflicts() {
        // A status write landing between the guard snapshot and the commit must
        // not conflict: the finish transaction only writes.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let finished = reschedule_batch(&storage, 4).await;
        let plan = plan_finish_chunk(&storage, finished).await.unwrap();

        let document_id = Ulid::from_bytes([120u8; 16]);
        let racing = MetadataMaterializationStatusRecord {
            document_id,
            event_id: Ulid::from_parts(9, 9),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 9,
        };
        write_entries(
            &storage,
            vec![metadata_materialization_status_write_entry(&racing).unwrap()],
        )
        .await;

        let txn_id = start_write_transaction(&storage).await.unwrap();
        transactional_batch_write(&storage, txn_id, plan.writes)
            .await
            .unwrap();
        transactional_batch_delete(&storage, txn_id, plan.deletes)
            .await
            .unwrap();
        commit_storage_transaction(&storage, txn_id).await.unwrap();
        assert_eq!(storage.snapshot_metrics().conflicts_total, 0);
    }

    #[tokio::test]
    async fn finish_keeps_newer() {
        // A newer job enqueued after the guard snapshot has its status
        // overwritten by the older completion; it must still be scanned as due
        // rather than pruned, so the overwrite is self-healing.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([77u8; 16]);
        let old_event_id = Ulid::from_parts(1, 1);
        let newer_event_id = Ulid::from_parts(2, 1);
        let old_event = create_event(document_id, old_event_id, "old");
        let newer_event = create_event(document_id, newer_event_id, "newer");
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let (_, old_job_key, _) = metadata_materialization_job_write_entry(&old_job).unwrap();
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&old_event).unwrap(),
                metadata_materialization_job_write_entry(&old_job).unwrap(),
                metadata_materialization_document_job_write_entry(&old_job).unwrap(),
            ],
        )
        .await;
        let finished = vec![FinishedMaterializationJob::Completed(
            CompletedMaterializationJob {
                job_key: old_job_key.to_vec(),
                document_job_key: Some(
                    metadata_materialization_document_job_key(document_id, old_event_id).to_vec(),
                ),
                status: Some(materialization_success_status(&old_job, &old_event, None)),
                iri_index_writes: Vec::new(),
                raw_state_write: None,
                sync: None,
            },
        )];
        let plan = plan_finish_chunk(&storage, finished).await.unwrap();

        // The newer event lands after the snapshot was taken.
        let newer_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: newer_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&newer_event).unwrap(),
                metadata_materialization_status_write_entry(&new_pending_materialization_status(
                    &newer_event,
                    2,
                ))
                .unwrap(),
                metadata_materialization_job_write_entry(&newer_job).unwrap(),
                metadata_materialization_document_job_write_entry(&newer_job).unwrap(),
            ],
        )
        .await;

        let txn_id = start_write_transaction(&storage).await.unwrap();
        transactional_batch_write(&storage, txn_id, plan.writes)
            .await
            .unwrap();
        transactional_batch_delete(&storage, txn_id, plan.deletes)
            .await
            .unwrap();
        commit_storage_transaction(&storage, txn_id).await.unwrap();

        let (jobs, _, _) = scan_due_materialization_jobs(
            &storage,
            unix_timestamp_millis(),
            MATERIALIZATION_BATCH_SIZE,
        )
        .await
        .unwrap();
        assert_eq!(
            jobs.iter().map(|(_, job)| job.event_id).collect::<Vec<_>>(),
            vec![newer_event_id],
            "the newer job survives the older completion"
        );
    }

    #[tokio::test]
    async fn finish_does_not_regress_newer_status() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([3u8; 16]);
        let old_event_id = Ulid::from_parts(3, 1);
        let newer_event_id = Ulid::from_parts(4, 1);
        let old_event = create_event(document_id, old_event_id, "old");
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let newer_status = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 7,
            failures: 0,
            last_error: Some("newer pending".to_string()),
            updated_at_ms: 7,
        };
        let (_, old_job_key, _) = metadata_materialization_job_write_entry(&old_job).unwrap();
        let stale_index_key = vec![9u8; 16];
        let raw_state_key = raw_revision_key(document_id);
        write_entries(
            &storage,
            vec![
                metadata_materialization_status_write_entry(&newer_status).unwrap(),
                metadata_materialization_job_write_entry(&old_job).unwrap(),
                metadata_materialization_document_job_write_entry(&old_job).unwrap(),
            ],
        )
        .await;

        finish_completed_materialization_jobs(
            &storage,
            vec![FinishedMaterializationJob::Completed(
                CompletedMaterializationJob {
                    job_key: old_job_key.to_vec(),
                    document_job_key: Some(
                        metadata_materialization_document_job_key(
                            old_job.document_id,
                            old_job.event_id,
                        )
                        .to_vec(),
                    ),
                    status: Some(materialization_success_status(&old_job, &old_event, None)),
                    iri_index_writes: vec![(
                        METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                        ByteView::from(stale_index_key.clone()),
                        ByteView::from(vec![1]),
                    )],
                    raw_state_write: Some((
                        METADATA_RAW_REVISION_KEYSPACE.to_string(),
                        raw_state_key.clone(),
                        ByteView::from(vec![1]),
                    )),
                    sync: None,
                },
            )],
        )
        .await
        .unwrap();

        assert_eq!(
            read_materialization_status(&storage, document_id, None)
                .await
                .unwrap(),
            Some(newer_status)
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_IRI_REFERENCE_INDEX_KEYSPACE,
                stale_index_key,
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_RAW_REVISION_KEYSPACE,
                raw_state_key.to_vec(),
            )
            .await
        );
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                key: old_job_key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                key: metadata_materialization_document_job_key(
                    old_job.document_id,
                    old_job.event_id,
                ),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {}
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn supersedes_prior_rows() {
        // A second revision must leave only its own cursor's IRI index rows.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([5u8; 16]);

        let build = |event_id: Ulid| {
            let event = create_event(document_id, event_id, "rev");
            let job = MetadataMaterializationJobRecord {
                document_id,
                event_id,
                due_at_ms: 1,
                attempts: 0,
                failures: 0,
                parks: 0,
            };
            CompletedMaterializationJob {
                job_key: metadata_materialization_job_write_entry(&job)
                    .unwrap()
                    .1
                    .to_vec(),
                document_job_key: Some(
                    metadata_materialization_document_job_key(document_id, event_id).to_vec(),
                ),
                status: Some(materialization_success_status(&job, &event, None)),
                iri_index_writes: vec![(
                    METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                    aruna_core::storage_entries::metadata_iri_reference_key(
                        "p",
                        "o",
                        document_id,
                        event_id,
                    ),
                    ByteView::from(vec![1u8]),
                )],
                raw_state_write: Some((
                    METADATA_RAW_REVISION_KEYSPACE.to_string(),
                    raw_revision_key(document_id),
                    ByteView::from(event_id.to_bytes().to_vec()),
                )),
                sync: None,
            }
        };

        let first = Ulid::from_parts(1, 1);
        let second = Ulid::from_parts(2, 1);
        finish_completed_materialization_jobs(
            &storage,
            vec![FinishedMaterializationJob::Completed(build(first))],
        )
        .await
        .unwrap();
        finish_completed_materialization_jobs(
            &storage,
            vec![FinishedMaterializationJob::Completed(build(second))],
        )
        .await
        .unwrap();

        let key_of = |cursor: Ulid| {
            aruna_core::storage_entries::metadata_iri_reference_key("p", "o", document_id, cursor)
                .as_ref()
                .to_vec()
        };
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_IRI_REFERENCE_INDEX_KEYSPACE,
                key_of(first)
            )
            .await,
            "prior cursor rows must be removed"
        );
        assert!(
            storage_key_exists(
                &storage,
                METADATA_IRI_REFERENCE_INDEX_KEYSPACE,
                key_of(second)
            )
            .await,
            "current cursor rows must remain"
        );
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_RAW_REVISION_KEYSPACE.to_string(),
                key: raw_revision_key(document_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) => assert_eq!(value.as_ref(), second.to_bytes()),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn prune_resumes_parked() {
        // A prune that failed after its jobs were deleted leaves nothing to retry
        // from, so the batch parks the cursor: a later batch must finish the work
        // and clear the parked entry.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([57u8; 16]);
        let stale = Ulid::from_parts(1, 1);
        let current = Ulid::from_parts(2, 1);
        let stale_key =
            aruna_core::storage_entries::metadata_iri_reference_key("p", "o", document_id, stale);
        write_entries(
            &storage,
            vec![
                (
                    METADATA_IRI_REFERENCE_INDEX_KEYSPACE.to_string(),
                    stale_key.clone(),
                    ByteView::from(vec![1u8]),
                ),
                materialization_prune_entry(document_id, current).unwrap(),
            ],
        )
        .await;

        prune_superseded_rows(&storage, HashMap::new())
            .await
            .unwrap();

        assert!(
            !storage_key_exists(
                &storage,
                METADATA_IRI_REFERENCE_INDEX_KEYSPACE,
                stale_key.to_vec()
            )
            .await
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_PRUNE_KEYSPACE,
                materialization_prune_key(document_id).to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn older_check_local() {
        // Predecessor lookups read the status then one document-local sidecar
        // scan; they never fall back to a full global keyspace scan.
        let (storage, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let document_id = Ulid::from_bytes([11u8; 16]);
        let event_id = Ulid::from_parts(11, 2);
        let scripted = thread::spawn(move || {
            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("status read request");
            let status_key = match effect {
                StorageEffect::Read {
                    key_space,
                    key,
                    txn_id: None,
                } => {
                    assert_eq!(key_space, METADATA_MATERIALIZATION_STATUS_KEYSPACE);
                    assert_eq!(key, metadata_materialization_status_key(document_id));
                    key
                }
                other => panic!("unexpected storage effect: {other:?}"),
            };
            response_tx.send(StorageEvent::ReadResult {
                key: status_key,
                value: None,
            });

            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("document-local predecessor scan");
            match effect {
                StorageEffect::Iter {
                    key_space,
                    prefix,
                    start,
                    limit,
                    txn_id: None,
                } => {
                    assert_eq!(key_space, METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE);
                    assert_eq!(
                        prefix,
                        Some(metadata_materialization_document_job_prefix(document_id))
                    );
                    assert_eq!(start, None);
                    assert_eq!(limit, MATERIALIZATION_SCAN_PAGE_SIZE);
                }
                other => panic!("unexpected storage effect: {other:?}"),
            }
            response_tx.send(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            });

            assert!(
                receiver.recv().is_err(),
                "no global predecessor scan is issued"
            );
        });

        assert!(
            !older_exists(&storage, document_id, event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
        drop(storage);
        scripted.join().expect("scripted storage actor finished");
    }

    #[tokio::test]
    async fn older_queued_job_blocks_later_materialization_until_advanced() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([10u8; 16]);
        let older_event_id = Ulid::from_parts(10, 1);
        let newer_event_id = Ulid::from_parts(10, 2);
        let older_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: older_event_id,
            due_at_ms: 30_000,
            attempts: 1,
            failures: 0,
            parks: 0,
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            context_digest: None,
            dataset_digest: None,
            state: MetadataMaterializationState::Pending,
            attempts: 0,
            failures: 0,
            last_error: None,
            updated_at_ms: 1,
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&create_event(
                    document_id,
                    older_event_id,
                    "older-queued",
                ))
                .unwrap(),
                metadata_materialization_status_write_entry(&newer_pending).unwrap(),
                metadata_materialization_job_write_entry(&older_job).unwrap(),
                metadata_materialization_document_job_write_entry(&older_job).unwrap(),
            ],
        )
        .await;

        assert!(
            older_exists(&storage, document_id, newer_event_id, &BTreeSet::new())
                .await
                .unwrap()
        );

        let mut advanced = BTreeSet::new();
        advanced.insert(older_event_id);
        assert!(
            !older_exists(&storage, document_id, newer_event_id, &advanced)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn reschedules_batched() {
        // Three failing jobs across distinct documents resolve in one finish
        // transaction: the request delta stays at single-transaction scale.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let jobs: Vec<_> = (0..3u8)
            .map(|seed| {
                let document_id = Ulid::from_bytes([70 + seed; 16]);
                let event_id = Ulid::from_parts(1, u128::from(seed));
                let event = create_event(document_id, event_id, "retry");
                let job = MetadataMaterializationJobRecord {
                    document_id,
                    event_id,
                    due_at_ms: 1,
                    attempts: 0,
                    failures: 0,
                    parks: 0,
                };
                let index_key = metadata_materialization_job_key(&job);
                (job, event, index_key)
            })
            .collect();
        for (job, _, _) in &jobs {
            write_entries(
                &storage,
                vec![
                    metadata_materialization_job_write_entry(job).unwrap(),
                    metadata_materialization_document_job_write_entry(job).unwrap(),
                ],
            )
            .await;
        }
        let finished: Vec<_> = jobs
            .iter()
            .map(|(job, event, index_key)| {
                defer_materialization_job(index_key.as_ref(), job, event, &application_failure())
            })
            .collect();
        assert!(
            finished
                .iter()
                .all(|finished| matches!(finished, FinishedMaterializationJob::Rescheduled { .. }))
        );

        let before = storage.snapshot_metrics().requests_total;
        finish_completed_materialization_jobs(&storage, finished)
            .await
            .unwrap();
        let delta = storage.snapshot_metrics().requests_total - before;

        // A single finish transaction issues far fewer requests than one
        // transaction per job would.
        assert!(delta <= 10, "finish issued {delta} storage requests");
        for (job, _, _) in &jobs {
            let requeued = read_document_job(&storage, job.document_id, job.event_id)
                .await
                .unwrap()
                .expect("job requeued");
            assert_eq!(requeued.attempts, 1);
        }

        // Exactly the rescheduled index rows survive: the old due_at=1 keys are
        // gone and each document keeps one new-due row.
        let index_keys = index_job_keys(&storage).await;
        assert_eq!(index_keys.len(), jobs.len());
        for (job, _, old_key) in &jobs {
            assert!(
                !storage_key_exists(
                    &storage,
                    METADATA_MATERIALIZATION_JOB_KEYSPACE,
                    old_key.to_vec()
                )
                .await
            );
            let row = index_keys
                .iter()
                .find(|(_, doc, event)| *doc == job.document_id && *event == job.event_id)
                .expect("rescheduled index row present");
            assert_ne!(row.0, job.due_at_ms);
        }
    }

    #[tokio::test]
    async fn failing_apply_reschedules() {
        // A non-terminal apply failure (missing metadata handle) reschedules the
        // job with attempts advanced and both rows kept at a new due time.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([90u8; 16]);
        let event_id = Ulid::from_parts(3, 1);
        let event = create_event(document_id, event_id, "reschedule");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        let old_index_key = metadata_materialization_job_key(&job);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&job).unwrap(),
                metadata_materialization_document_job_write_entry(&job).unwrap(),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_metadata_materialization_batch(&context)
            .await
            .expect("batch drains");
        assert_eq!(result.processed, 1);

        let requeued = read_document_job(&storage, document_id, event_id)
            .await
            .unwrap()
            .expect("job rescheduled");
        assert_eq!(requeued.attempts, 1);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                old_index_key.to_vec()
            )
            .await
        );
        let index_keys = index_job_keys(&storage).await;
        assert_eq!(index_keys.len(), 1);
        assert_eq!(index_keys[0].1, document_id);
        assert_eq!(index_keys[0].2, event_id);
        assert_ne!(index_keys[0].0, job.due_at_ms);
    }

    #[test]
    fn sync_dedupes_graphs() {
        let graph_iri = MetadataRegistryRecord::graph_iri_for(Ulid::from_bytes([50u8; 16]));
        let completed = |peers: Vec<NodeId>| {
            FinishedMaterializationJob::Completed(CompletedMaterializationJob {
                job_key: vec![1],
                document_job_key: None,
                status: None,
                iri_index_writes: Vec::new(),
                raw_state_write: None,
                sync: Some(CompletedMaterializationSync {
                    graph_iri: graph_iri.clone(),
                    peers,
                }),
            })
        };
        let finished = vec![completed(vec![node(1)]), completed(vec![node(2)])];

        let syncs = dedupe_graph_syncs(&finished);

        assert_eq!(syncs.len(), 1);
        assert_eq!(syncs[0].graph_iri, graph_iri);
        assert_eq!(syncs[0].peers, vec![node(2)]);
    }
}
