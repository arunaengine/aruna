use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE, METADATA_MATERIALIZATION_JOB_KEYSPACE,
    METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataCreateCrateRequest, MetadataCreateEventPayload,
    MetadataCreateEventRecord, MetadataEffect, MetadataError, MetadataEvent,
    MetadataGraphLifecycleRecord, MetadataGraphPolicy, MetadataMaterializationJobRecord,
    MetadataMaterializationState, MetadataMaterializationStatusRecord, MetadataRequestDurability,
    deterministic_materialization_actor,
};
use aruna_core::storage_entries::{
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
use tokio::task::JoinSet;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;

use crate::queue_backoff::queue_retry_after_ms;

use super::queue_storage::{
    MetadataQueueStorageError, abort_storage_transaction_best_effort, commit_storage_transaction,
    start_write_transaction,
};

const MATERIALIZATION_SCAN_PAGE_SIZE: usize = 512;
const MATERIALIZATION_BATCH_SIZE: usize = 128;

pub const METADATA_MATERIALIZATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const METADATA_MATERIALIZATION_RETRY_AFTER: Duration = Duration::from_secs(1);

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
    sync: Option<CompletedMaterializationSync>,
}

#[derive(Debug, Clone)]
struct CompletedMaterializationSync {
    graph_iri: String,
    peers: Vec<NodeId>,
}

#[derive(Debug, Default)]
struct MaterializationGroupOutcome {
    completed: Vec<CompletedMaterializationJob>,
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
}

impl From<MetadataQueueStorageError> for MetadataMaterializationQueueError {
    fn from(error: MetadataQueueStorageError) -> Self {
        match error {
            MetadataQueueStorageError::Storage(error) => Self::Storage(error),
            MetadataQueueStorageError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
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
    completed: &mut Vec<CompletedMaterializationJob>,
    timings: &mut MaterializationBatchTimings,
    first_error: &mut Option<MetadataMaterializationQueueError>,
) {
    match result {
        Ok(outcome) => {
            timings.processed = timings.processed.saturating_add(outcome.processed);
            timings.craqle_elapsed = timings
                .craqle_elapsed
                .saturating_add(outcome.craqle_elapsed);
            completed.extend(outcome.completed);
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

fn materialization_job_preferred(
    candidate: &MetadataMaterializationJobRecord,
    current: &MetadataMaterializationJobRecord,
) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

fn completed_stale_materialization_job(job_key: Vec<u8>) -> CompletedMaterializationJob {
    CompletedMaterializationJob {
        job_key,
        document_job_key: None,
        status: None,
        sync: None,
    }
}

fn deduplicate_materialization_jobs(
    jobs: Vec<(Vec<u8>, MetadataMaterializationJobRecord)>,
) -> (
    Vec<(Vec<u8>, MetadataMaterializationJobRecord)>,
    Vec<CompletedMaterializationJob>,
) {
    let mut selected: BTreeMap<Ulid, (Vec<u8>, MetadataMaterializationJobRecord)> = BTreeMap::new();
    let mut stale = Vec::new();
    for (job_key, job) in jobs {
        match selected.get_mut(&job.event_id) {
            Some((selected_key, selected_job))
                if materialization_job_preferred(&job, selected_job) =>
            {
                let previous_key = std::mem::replace(selected_key, job_key);
                *selected_job = job;
                if previous_key.as_slice() != selected_key.as_slice() {
                    stale.push(completed_stale_materialization_job(previous_key));
                }
            }
            Some((selected_key, _)) => {
                if selected_key.as_slice() != job_key.as_slice() {
                    stale.push(completed_stale_materialization_job(job_key));
                }
            }
            None => {
                selected.insert(job.event_id, (job_key, job));
            }
        }
    }
    (selected.into_values().collect(), stale)
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
    let mut completed = Vec::new();
    let mut timings = MaterializationBatchTimings {
        groups: groups.len(),
        ..MaterializationBatchTimings::default()
    };
    let mut first_error = None;
    for (_, jobs) in groups {
        let (mut jobs, stale_jobs) = deduplicate_materialization_jobs(jobs);
        completed.extend(stale_jobs);
        jobs.sort_by_key(|(_, job)| job.event_id);
        if tasks.len() >= concurrency
            && let Some(result) = tasks.join_next().await
        {
            collect_group_outcome(result, &mut completed, &mut timings, &mut first_error);
        }
        let context = context.clone();
        tasks.spawn(async move {
            let mut outcome = MaterializationGroupOutcome::default();
            let mut advanced_event_ids = BTreeSet::new();
            for (job_key, job) in jobs {
                let event_id = job.event_id;
                match process_materialization_job(&context, job_key, job, &advanced_event_ids).await
                {
                    Ok(processed_job) => {
                        outcome.craqle_elapsed = outcome
                            .craqle_elapsed
                            .saturating_add(processed_job.craqle_elapsed);
                        if processed_job.attempted {
                            outcome.processed = outcome.processed.saturating_add(1);
                        }
                        if let Some(completed) = processed_job.completed {
                            advanced_event_ids.insert(event_id);
                            outcome.completed.push(completed);
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
        collect_group_outcome(result, &mut completed, &mut timings, &mut first_error);
    }
    let finish_started = Instant::now();
    let syncs = completed
        .iter()
        .filter_map(|job| job.sync.clone())
        .collect::<Vec<_>>();
    if let Err(error) =
        finish_completed_materialization_jobs(&context.storage_handle, completed).await
        && first_error.is_none()
    {
        first_error = Some(error);
    }
    timings.finish_elapsed = finish_started.elapsed();
    if let Some(error) = first_error {
        return Err(error);
    }
    schedule_completed_materialization_syncs(context, syncs).await;
    Ok(timings)
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

async fn finish_completed_materialization_jobs(
    storage: &StorageHandle,
    completed: Vec<CompletedMaterializationJob>,
) -> Result<(), MetadataMaterializationQueueError> {
    if completed.is_empty() {
        return Ok(());
    }
    let txn_id = start_write_transaction(storage).await?;
    let result = finish_completed_materialization_jobs_in_txn(storage, txn_id, completed).await;
    match result {
        Ok(()) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(())
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

async fn finish_completed_materialization_jobs_in_txn(
    storage: &StorageHandle,
    txn_id: Ulid,
    completed: Vec<CompletedMaterializationJob>,
) -> Result<(), MetadataMaterializationQueueError> {
    let mut writes = Vec::new();
    let mut deletes = Vec::with_capacity(completed.len().saturating_mul(2));
    for job in completed {
        if let Some(status) = job.status {
            let current =
                read_materialization_status(storage, status.document_id, Some(txn_id)).await?;
            if should_write_final_materialization_status(current.as_ref(), &status) {
                writes.push(metadata_materialization_status_write_entry(&status)?);
            }
        }
        deletes.push((
            METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            ByteView::from(job.job_key),
        ));
        if let Some(document_job_key) = job.document_job_key {
            deletes.push((
                METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                ByteView::from(document_job_key),
            ));
        }
    }

    transactional_batch_write(storage, txn_id, writes).await?;
    transactional_batch_delete(storage, txn_id, deletes).await
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
    let mut next_due_at_ms = None;
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

        for (key, value) in values {
            let key = key.to_vec();
            let job = match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Repairing or deleting malformed metadata materialization job");
                    if let Some(job) =
                        repair_or_delete_malformed_materialization_job(storage, key).await?
                    {
                        if job.due_at_ms > now_ms {
                            next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
                            continue;
                        }
                        jobs.push((metadata_materialization_job_key(&job).to_vec(), job));
                        if jobs.len() >= limit {
                            return Ok((jobs, true, next_due_at_ms));
                        }
                    }
                    continue;
                }
            };
            if !materialization_job_key_matches(&key, &job) {
                warn!(key = ?key, "Repairing metadata materialization job stored under non-canonical key");
                if let Some(existing_job) = find_decoded_global_materialization_job(
                    storage,
                    job.document_id,
                    job.event_id,
                    Some(key.as_slice()),
                )
                .await?
                {
                    delete_materialization_global_job(storage, key).await?;
                    if existing_job.due_at_ms > now_ms {
                        next_due_at_ms = min_due_at(next_due_at_ms, existing_job.due_at_ms);
                        continue;
                    }
                    jobs.push((
                        metadata_materialization_job_key(&existing_job).to_vec(),
                        existing_job,
                    ));
                    if jobs.len() >= limit {
                        return Ok((jobs, true, next_due_at_ms));
                    }
                    continue;
                }
                if !materialization_job_is_live(storage, &job).await? {
                    delete_materialization_global_job(storage, key).await?;
                    continue;
                }
                repair_materialization_job_key(storage, key, &job).await?;
                if job.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
                    continue;
                }
                jobs.push((metadata_materialization_job_key(&job).to_vec(), job));
                if jobs.len() >= limit {
                    return Ok((jobs, true, next_due_at_ms));
                }
                continue;
            }
            if let Some(existing_job) = find_decoded_global_materialization_job(
                storage,
                job.document_id,
                job.event_id,
                Some(key.as_slice()),
            )
            .await?
                && materialization_job_preferred(&existing_job, &job)
            {
                write_materialization_document_job(storage, &existing_job).await?;
                delete_materialization_global_job(storage, key).await?;
                if existing_job.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, existing_job.due_at_ms);
                    continue;
                }
                jobs.push((
                    metadata_materialization_job_key(&existing_job).to_vec(),
                    existing_job,
                ));
                if jobs.len() >= limit {
                    return Ok((jobs, true, next_due_at_ms));
                }
                continue;
            }
            if !materialization_job_is_live(storage, &job).await? {
                match materialization_job_obsolescence(storage, &job).await? {
                    MaterializationJobObsolescence::Final if job.due_at_ms <= now_ms => {
                        jobs.push((key, job));
                        if jobs.len() >= limit {
                            return Ok((jobs, true, next_due_at_ms));
                        }
                    }
                    MaterializationJobObsolescence::Final => {
                        delete_materialization_job(storage, key).await?;
                    }
                    MaterializationJobObsolescence::RetryAdvanced => {
                        if let Some(preferred) = find_decoded_global_materialization_job(
                            storage,
                            job.document_id,
                            job.event_id,
                            Some(key.as_slice()),
                        )
                        .await?
                        {
                            write_materialization_document_job(storage, &preferred).await?;
                        }
                        delete_materialization_global_job(storage, key).await?;
                    }
                    MaterializationJobObsolescence::Live => {
                        delete_materialization_job(storage, key).await?;
                    }
                }
                continue;
            }
            if job.due_at_ms > now_ms {
                next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
                continue;
            }
            jobs.push((key, job));
            if jobs.len() >= limit {
                return Ok((jobs, true, next_due_at_ms));
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok((jobs, false, next_due_at_ms)),
        }
    }
}

fn min_due_at(current: Option<u64>, due_at_ms: u64) -> Option<u64> {
    Some(current.map_or(due_at_ms, |current| current.min(due_at_ms)))
}

fn due_after(now_ms: u64, due_at_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
}

#[derive(Debug, Default)]
struct ProcessedMaterializationJob {
    completed: Option<CompletedMaterializationJob>,
    craqle_elapsed: Duration,
    attempted: bool,
    stop_group: bool,
}

impl ProcessedMaterializationJob {
    fn completed(job: CompletedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            completed: Some(job),
            craqle_elapsed,
            attempted: true,
            stop_group: false,
        }
    }

    fn rescheduled(craqle_elapsed: Duration) -> Self {
        Self {
            completed: None,
            craqle_elapsed,
            attempted: true,
            stop_group: true,
        }
    }

    fn blocked() -> Self {
        Self {
            completed: None,
            craqle_elapsed: Duration::ZERO,
            attempted: false,
            stop_group: true,
        }
    }
}

async fn process_materialization_job(
    context: &DriverContext,
    job_key: Vec<u8>,
    job: MetadataMaterializationJobRecord,
    advanced_event_ids: &BTreeSet<Ulid>,
) -> Result<ProcessedMaterializationJob, MetadataMaterializationQueueError> {
    if older_materialization_job_exists(
        &context.storage_handle,
        job.document_id,
        job.event_id,
        advanced_event_ids,
    )
    .await?
    {
        return Ok(ProcessedMaterializationJob::blocked());
    }
    let document_job_key =
        metadata_materialization_document_job_key(job.document_id, job.event_id).to_vec();

    let (obsolescence, event) = tokio::join!(
        materialization_job_obsolescence(&context.storage_handle, &job),
        read_create_event(&context.storage_handle, job.document_id, job.event_id),
    );
    match obsolescence? {
        MaterializationJobObsolescence::Live => {}
        MaterializationJobObsolescence::Final => {
            return Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: None,
                    sync: None,
                },
                Duration::ZERO,
            ));
        }
        MaterializationJobObsolescence::RetryAdvanced => {
            if let Some(preferred) = find_decoded_global_materialization_job(
                &context.storage_handle,
                job.document_id,
                job.event_id,
                Some(job_key.as_slice()),
            )
            .await?
            {
                write_materialization_document_job(&context.storage_handle, &preferred).await?;
            }
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
                    true,
                )),
                sync: None,
            },
            Duration::ZERO,
        ));
    }

    let apply_started = Instant::now();
    let apply_result = materialize_create_event(context, &event).await;
    let craqle_elapsed = apply_started.elapsed();
    match apply_result {
        Ok(()) => Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                document_job_key: Some(document_job_key),
                status: Some(materialization_success_status(&job, &event)),
                sync: Some(CompletedMaterializationSync {
                    graph_iri: event.record.graph_iri.clone(),
                    peers: event.record.holder_node_ids.clone(),
                }),
            },
            craqle_elapsed,
        )),
        Err(error) if is_terminal_materialization_error(&error) => {
            Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    document_job_key: Some(document_job_key),
                    status: Some(materialization_failure_status(
                        &job,
                        &event,
                        error.to_string(),
                        true,
                    )),
                    sync: None,
                },
                craqle_elapsed,
            ))
        }
        Err(error) => {
            reschedule_materialization_job(
                &context.storage_handle,
                &job_key,
                &job,
                &event,
                error.to_string(),
            )
            .await?;
            Ok(ProcessedMaterializationJob::rescheduled(craqle_elapsed))
        }
    }
}

async fn older_materialization_job_exists(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
    advanced_event_ids: &BTreeSet<Ulid>,
) -> Result<bool, MetadataMaterializationQueueError> {
    let status = read_materialization_status(storage, document_id, None).await?;
    let prefix = metadata_materialization_document_job_prefix(document_id);
    let stop_key = metadata_materialization_document_job_key(document_id, event_id);
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
            let key = key.to_vec();
            if key.as_slice() >= stop_key.as_ref() {
                return older_global_materialization_job_exists(
                    storage,
                    document_id,
                    event_id,
                    advanced_event_ids,
                    status.as_ref(),
                )
                .await;
            }
            let job = match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Repairing or deleting malformed metadata materialization document job while checking predecessors");
                    if let Some((job_document_id, job_event_id)) =
                        materialization_document_job_key_target(&key)
                        && job_document_id == document_id
                        && job_event_id < event_id
                        && !advanced_event_ids.contains(&job_event_id)
                    {
                        match read_global_materialization_job(
                            storage,
                            job_document_id,
                            job_event_id,
                        )
                        .await?
                        {
                            Some(job)
                                if !status.as_ref().is_some_and(|status| {
                                    materialization_status_obsoletes_job(status, &job)
                                }) =>
                            {
                                write_materialization_document_job(storage, &job).await?;
                                return Ok(true);
                            }
                            _ => {}
                        }
                    }
                    delete_materialization_document_job(storage, key).await?;
                    continue;
                }
            };
            if !materialization_document_job_key_matches(&key, &job) {
                warn!(key = ?key, "Repairing or deleting metadata materialization document job stored under non-canonical key");
                if let Some((job_document_id, job_event_id)) =
                    materialization_document_job_key_target(&key)
                    && job_document_id == document_id
                    && job_event_id < event_id
                    && !advanced_event_ids.contains(&job_event_id)
                {
                    match read_global_materialization_job(storage, job_document_id, job_event_id)
                        .await?
                    {
                        Some(global_job)
                            if !status.as_ref().is_some_and(|status| {
                                materialization_status_obsoletes_job(status, &global_job)
                            }) =>
                        {
                            write_materialization_document_job(storage, &global_job).await?;
                            return Ok(true);
                        }
                        _ => {}
                    }
                }
                delete_materialization_document_job(storage, key).await?;
                continue;
            }
            if job.document_id == document_id
                && job.event_id < event_id
                && !advanced_event_ids.contains(&job.event_id)
                && !status
                    .as_ref()
                    .is_some_and(|status| materialization_status_obsoletes_job(status, &job))
            {
                match read_global_materialization_job(storage, job.document_id, job.event_id)
                    .await?
                {
                    Some(global_job) => {
                        if global_job != job {
                            write_materialization_document_job(storage, &global_job).await?;
                        }
                        return Ok(true);
                    }
                    None => {
                        warn!(key = ?key, "Deleting orphan metadata materialization document job");
                        delete_materialization_document_job(storage, key).await?;
                    }
                }
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => {
                return older_global_materialization_job_exists(
                    storage,
                    document_id,
                    event_id,
                    advanced_event_ids,
                    status.as_ref(),
                )
                .await;
            }
        }
    }
}

async fn older_global_materialization_job_exists(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
    advanced_event_ids: &BTreeSet<Ulid>,
    status: Option<&MetadataMaterializationStatusRecord>,
) -> Result<bool, MetadataMaterializationQueueError> {
    let mut start_after = None;
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

        for (key, value) in values {
            let key = key.to_vec();
            let mut job = match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Repairing or deleting malformed metadata materialization job while checking global predecessors");
                    if let Some(job) =
                        repair_or_delete_malformed_materialization_job(storage, key).await?
                        && job.document_id == document_id
                        && job.event_id < event_id
                        && !advanced_event_ids.contains(&job.event_id)
                        && !status.is_some_and(|status| {
                            materialization_status_obsoletes_job(status, &job)
                        })
                    {
                        write_materialization_document_job(storage, &job).await?;
                        return Ok(true);
                    }
                    continue;
                }
            };
            if !materialization_job_key_matches(&key, &job) {
                warn!(key = ?key, "Repairing metadata materialization job stored under non-canonical key while checking global predecessors");
                let Some(global_job) =
                    read_global_materialization_job(storage, job.document_id, job.event_id).await?
                else {
                    continue;
                };
                job = global_job;
            }
            if job.document_id != document_id
                || job.event_id >= event_id
                || advanced_event_ids.contains(&job.event_id)
                || status.is_some_and(|status| materialization_status_obsoletes_job(status, &job))
            {
                continue;
            }
            if !materialization_event_exists(storage, &job).await? {
                warn!(document_id = %job.document_id, event_id = %job.event_id, "Deleting orphan metadata materialization job while checking global predecessors");
                delete_materialization_job(
                    storage,
                    metadata_materialization_job_key(&job).to_vec(),
                )
                .await?;
                continue;
            }
            write_materialization_document_job(storage, &job).await?;
            return Ok(true);
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(false),
        }
    }
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

async fn materialization_job_obsolescence(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<MaterializationJobObsolescence, MetadataMaterializationQueueError> {
    let Some(status) = read_materialization_status(storage, job.document_id, None).await? else {
        return Ok(MaterializationJobObsolescence::Live);
    };
    if materialization_status_obsoletes_job(&status, job) {
        return Ok(MaterializationJobObsolescence::Final);
    }
    if status.event_id == job.event_id && status.attempts > job.attempts {
        return Ok(MaterializationJobObsolescence::RetryAdvanced);
    }
    Ok(MaterializationJobObsolescence::Live)
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
                },
            )
    })
}

pub async fn metadata_materialization_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataMaterializationQueueError> {
    let mut start_after = None;
    loop {
        match storage
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
            }) => {
                let Some((key, value)) = values.into_iter().next() else {
                    return Ok(false);
                };
                match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                    Ok(job) if materialization_job_key_matches(key.as_ref(), &job) => {
                        if materialization_job_is_live(storage, &job).await? {
                            return Ok(true);
                        }
                        delete_materialization_global_job(storage, key.to_vec()).await?;
                    }
                    Ok(job) => {
                        let key = key.to_vec();
                        warn!(key = ?key, "Repairing metadata materialization job stored under non-canonical key while probing queue");
                        if let Some(existing_job) = find_decoded_global_materialization_job(
                            storage,
                            job.document_id,
                            job.event_id,
                            Some(key.as_slice()),
                        )
                        .await?
                        {
                            delete_materialization_global_job(storage, key).await?;
                            if materialization_job_is_live(storage, &existing_job).await? {
                                return Ok(true);
                            }
                            delete_materialization_global_job(
                                storage,
                                metadata_materialization_job_key(&existing_job).to_vec(),
                            )
                            .await?;
                            continue;
                        }
                        if materialization_job_is_live(storage, &job).await? {
                            repair_materialization_job_key(storage, key, &job).await?;
                            return Ok(true);
                        }
                        delete_materialization_global_job(storage, key).await?;
                    }
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Repairing or deleting malformed metadata materialization job while probing queue");
                        if repair_or_delete_malformed_materialization_job(storage, key)
                            .await?
                            .is_some()
                        {
                            return Ok(true);
                        }
                    }
                }
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => return Ok(false),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
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

async fn repair_or_delete_malformed_materialization_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<Option<MetadataMaterializationJobRecord>, MetadataMaterializationQueueError> {
    let Some((due_at_ms, document_id, event_id)) = materialization_job_key_parts(&key) else {
        delete_materialization_job(storage, key).await?;
        return Ok(None);
    };
    let probe_job = MetadataMaterializationJobRecord {
        document_id,
        event_id,
        due_at_ms: 0,
        attempts: 0,
    };
    if !materialization_event_exists(storage, &probe_job).await? {
        delete_materialization_job(storage, key).await?;
        return Ok(None);
    }
    let current = read_materialization_status(storage, document_id, None).await?;
    if current
        .as_ref()
        .is_some_and(|status| materialization_status_obsoletes_job(status, &probe_job))
    {
        delete_materialization_job(storage, key).await?;
        return Ok(None);
    }
    if let Some(existing_job) = find_decoded_global_materialization_job(
        storage,
        document_id,
        event_id,
        Some(key.as_slice()),
    )
    .await?
    {
        delete_materialization_global_job(storage, key).await?;
        return Ok(Some(existing_job));
    }
    let job = MetadataMaterializationJobRecord {
        document_id,
        event_id,
        due_at_ms,
        attempts: current
            .filter(|status| status.event_id == event_id)
            .map(|status| status.attempts)
            .unwrap_or(0),
    };
    repair_materialization_job_key(storage, key, &job).await?;
    Ok(Some(job))
}

async fn find_decoded_global_materialization_job(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
    skip_key: Option<&[u8]>,
) -> Result<Option<MetadataMaterializationJobRecord>, MetadataMaterializationQueueError> {
    let mut selected: Option<(Vec<u8>, MetadataMaterializationJobRecord)> = None;
    let mut stale_keys = Vec::new();
    let mut start_after = None;
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

        for (key, value) in values {
            if skip_key.is_some_and(|skip_key| key.as_ref() == skip_key) {
                continue;
            }
            let Ok(job) = postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) else {
                continue;
            };
            if job.document_id != document_id || job.event_id != event_id {
                continue;
            }
            let key = key.to_vec();
            if !materialization_job_is_live(storage, &job).await? {
                stale_keys.push(key);
                continue;
            }
            match selected.as_mut() {
                Some((selected_key, selected_job))
                    if materialization_job_preferred(&job, selected_job) =>
                {
                    stale_keys.push(std::mem::replace(selected_key, key));
                    *selected_job = job;
                }
                Some(_) => stale_keys.push(key),
                None => selected = Some((key, job)),
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    let Some((key, job)) = selected else {
        for key in stale_keys {
            delete_materialization_global_job(storage, key).await?;
        }
        return Ok(None);
    };
    if !materialization_job_key_matches(&key, &job) {
        repair_materialization_job_key(storage, key.clone(), &job).await?;
    }
    let canonical_key = metadata_materialization_job_key(&job);
    for stale_key in stale_keys {
        if stale_key.as_slice() != key.as_slice() && stale_key.as_slice() != canonical_key.as_ref()
        {
            delete_materialization_global_job(storage, stale_key).await?;
        }
    }
    Ok(Some(job))
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

async fn repair_materialization_job_key(
    storage: &StorageHandle,
    old_key: Vec<u8>,
    job: &MetadataMaterializationJobRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let canonical_key = metadata_materialization_job_key(job);
    let txn_id = start_write_transaction(storage).await?;
    let result = async {
        transactional_batch_write(
            storage,
            txn_id,
            vec![
                metadata_materialization_job_write_entry(job)?,
                metadata_materialization_document_job_write_entry(job)?,
            ],
        )
        .await?;
        if old_key.as_slice() != canonical_key.as_ref() {
            transactional_batch_delete(
                storage,
                txn_id,
                vec![(
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(old_key),
                )],
            )
            .await?;
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(())
        }
        Err(error) => {
            abort_storage_transaction_best_effort(
                storage,
                txn_id,
                "Failed to abort materialization repair transaction",
                "Unexpected materialization repair transaction abort result",
            )
            .await;
            Err(error)
        }
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

fn materialization_job_key_matches(key: &[u8], job: &MetadataMaterializationJobRecord) -> bool {
    metadata_materialization_job_key(job).as_ref() == key
}

fn materialization_document_job_key_target(key: &[u8]) -> Option<(Ulid, Ulid)> {
    if key.len() != 32 {
        return None;
    }
    let mut document_id = [0u8; 16];
    document_id.copy_from_slice(&key[..16]);
    let mut event_id = [0u8; 16];
    event_id.copy_from_slice(&key[16..32]);
    Some((Ulid::from_bytes(document_id), Ulid::from_bytes(event_id)))
}

fn materialization_document_job_key_matches(
    key: &[u8],
    job: &MetadataMaterializationJobRecord,
) -> bool {
    metadata_materialization_document_job_key(job.document_id, job.event_id).as_ref() == key
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

async fn materialize_create_event(
    context: &DriverContext,
    event: &MetadataCreateEventRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let metadata_handle = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataMaterializationQueueError::MetadataHandleMissing)?;
    match metadata_handle
        .send_effect(graph_materialization_effect(event))
        .await
    {
        Event::Metadata(MetadataEvent::CreateCrateResult { .. })
        | Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. })
        | Event::Metadata(MetadataEvent::EntityUpsertResult { .. }) => Ok(()),
        Event::Metadata(MetadataEvent::Error { error, .. }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn graph_materialization_effect(event: &MetadataCreateEventRecord) -> Effect {
    let policy = MetadataGraphPolicy {
        public: event.record.public,
        permission_paths: vec![event.record.permission_path.clone()],
    }
    .normalized();
    let deterministic_actor = Some(deterministic_materialization_actor(event.event_id));
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
) -> MetadataMaterializationStatusRecord {
    MetadataMaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        state: MetadataMaterializationState::Materialized,
        attempts: job.attempts.saturating_add(1),
        last_error: None,
        updated_at_ms: unix_timestamp_millis(),
    }
}

fn materialization_failure_status(
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    error: String,
    terminal: bool,
) -> MetadataMaterializationStatusRecord {
    MetadataMaterializationStatusRecord {
        document_id: event.record.document_id,
        event_id: event.event_id,
        graph_iri: event.record.graph_iri.clone(),
        state: if terminal {
            MetadataMaterializationState::Failed
        } else {
            MetadataMaterializationState::Pending
        },
        attempts: job.attempts.saturating_add(1),
        last_error: Some(error),
        updated_at_ms: unix_timestamp_millis(),
    }
}

async fn reschedule_materialization_job(
    storage: &StorageHandle,
    job_key: &[u8],
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    error: String,
) -> Result<(), MetadataMaterializationQueueError> {
    let txn_id = start_write_transaction(storage).await?;
    let result =
        reschedule_materialization_job_in_txn(storage, txn_id, job_key, job, event, error).await;
    match result {
        Ok(()) => {
            commit_storage_transaction(storage, txn_id).await?;
            Ok(())
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

async fn reschedule_materialization_job_in_txn(
    storage: &StorageHandle,
    txn_id: Ulid,
    job_key: &[u8],
    job: &MetadataMaterializationJobRecord,
    event: &MetadataCreateEventRecord,
    error: String,
) -> Result<(), MetadataMaterializationQueueError> {
    let old_global_job_delete = (
        METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
        ByteView::from(job_key.to_vec()),
    );
    let current = read_materialization_status(storage, job.document_id, Some(txn_id)).await?;
    if current
        .as_ref()
        .is_some_and(|status| materialization_retry_already_advanced(status, job))
    {
        return transactional_batch_delete(
            storage,
            txn_id,
            vec![
                old_global_job_delete,
                (
                    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                    metadata_materialization_document_job_key(job.document_id, job.event_id),
                ),
            ],
        )
        .await;
    }

    let attempts = job.attempts.saturating_add(1);
    let status = materialization_failure_status(job, event, error, false);
    let retry_delay_ms = queue_retry_after_ms(attempts);
    let next_job = MetadataMaterializationJobRecord {
        document_id: job.document_id,
        event_id: job.event_id,
        due_at_ms: unix_timestamp_millis().saturating_add(retry_delay_ms),
        attempts,
    };
    let mut writes = Vec::with_capacity(3);
    if should_write_pending_retry_status(current.as_ref(), &status) {
        writes.push(metadata_materialization_status_write_entry(&status)?);
    }
    writes.push(metadata_materialization_job_write_entry(&next_job)?);
    writes.push(metadata_materialization_document_job_write_entry(
        &next_job,
    )?);
    transactional_batch_write(storage, txn_id, writes).await?;
    transactional_batch_delete(storage, txn_id, vec![old_global_job_delete]).await
}

fn is_terminal_materialization_error(error: &MetadataMaterializationQueueError) -> bool {
    matches!(
        error,
        MetadataMaterializationQueueError::Metadata(MetadataError::InvalidInput(_))
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

async fn write_materialization_document_job(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let (key_space, key, value) = metadata_materialization_document_job_write_entry(job)?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_global_materialization_job(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<MetadataMaterializationJobRecord>, MetadataMaterializationQueueError> {
    if let Some(job) =
        find_decoded_global_materialization_job(storage, document_id, event_id, None).await?
    {
        return Ok(Some(job));
    }
    find_repaired_malformed_global_materialization_job(storage, document_id, event_id).await
}

async fn find_repaired_malformed_global_materialization_job(
    storage: &StorageHandle,
    document_id: Ulid,
    event_id: Ulid,
) -> Result<Option<MetadataMaterializationJobRecord>, MetadataMaterializationQueueError> {
    let mut start_after = None;
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

        for (key, value) in values {
            let Err(error) = postcard::from_bytes::<MetadataMaterializationJobRecord>(&value)
            else {
                continue;
            };
            let key = key.to_vec();
            warn!(error = %error, key = ?key, "Repairing or deleting malformed metadata materialization job while repairing document sidecar");
            if let Some(job) = repair_or_delete_malformed_materialization_job(storage, key).await?
                && job.document_id == document_id
                && job.event_id == event_id
            {
                return Ok(Some(job));
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(None),
        }
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
    use aruna_core::storage_entries::metadata_create_event_write_entry;
    use aruna_core::structs::{MetadataRegistryRecord, RealmId};
    use aruna_storage::{FjallStorage, StorageHandle};
    use std::collections::BTreeSet;
    use std::thread;
    use tempfile::tempdir;

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
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms: 1,
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
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
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

    async fn read_materialization_document_job(
        storage: &StorageHandle,
        key: Vec<u8>,
    ) -> Option<MetadataMaterializationJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|value| {
                postcard::from_bytes(&value).expect("materialization document job decodes")
            }),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn read_materialization_global_job_at_key(
        storage: &StorageHandle,
        key: Vec<u8>,
    ) -> Option<MetadataMaterializationJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|value| postcard::from_bytes(&value).expect("materialization job decodes")),
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
            !older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn malformed_document_sidecar_repairs_from_valid_global_job_and_blocks_newer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([15u8; 16]);
        let old_event_id = Ulid::from_parts(15, 1);
        let newer_event_id = Ulid::from_parts(15, 2);
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 0,
        };
        let old_event = create_event(document_id, old_event_id, "malformed-sidecar");
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&old_event).unwrap(),
                metadata_materialization_job_write_entry(&old_job).unwrap(),
                (
                    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                    document_key.clone(),
                    ByteView::from(vec![1, 2, 3]),
                ),
            ],
        )
        .await;

        assert!(
            older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            read_materialization_document_job(&storage, document_key.to_vec()).await,
            Some(old_job)
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
            !older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
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
        };
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![metadata_materialization_document_job_write_entry(&old_job).unwrap()],
        )
        .await;

        assert!(
            !older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
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
    async fn noncanonical_future_materialization_job_does_not_hide_due_job() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([18u8; 16]),
            event_id: Ulid::from_parts(18, 1),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 0,
        };
        let due_job = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([19u8; 16]),
            event_id: Ulid::from_parts(19, 1),
            due_at_ms: 1,
            attempts: 0,
        };
        let future_event = create_event(future_job.document_id, future_job.event_id, "future");
        let due_event = create_event(due_job.document_id, due_job.event_id, "due");
        let misplaced_key = vec![0];
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&future_event).unwrap(),
                metadata_create_event_write_entry(&due_event).unwrap(),
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&future_job).unwrap()),
                ),
                metadata_materialization_job_write_entry(&due_job).unwrap(),
            ],
        )
        .await;

        let (jobs, has_more_due, _next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, 8)
                .await
                .unwrap();

        assert_eq!(
            jobs,
            vec![(metadata_materialization_job_key(&due_job).to_vec(), due_job)]
        );
        assert!(!has_more_due);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                misplaced_key
            )
            .await
        );
        assert!(
            storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                metadata_materialization_job_key(&future_job).to_vec()
            )
            .await
        );
    }

    #[tokio::test]
    async fn due_noncanonical_materialization_job_after_future_job_is_repaired() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([20u8; 16]),
            event_id: Ulid::from_parts(20, 1),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 0,
        };
        let due_job = MetadataMaterializationJobRecord {
            document_id: Ulid::from_bytes([21u8; 16]),
            event_id: Ulid::from_parts(21, 1),
            due_at_ms: 1,
            attempts: 0,
        };
        let future_event = create_event(future_job.document_id, future_job.event_id, "future");
        let due_event = create_event(due_job.document_id, due_job.event_id, "due");
        let misplaced_key = vec![255];
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&future_event).unwrap(),
                metadata_create_event_write_entry(&due_event).unwrap(),
                metadata_materialization_job_write_entry(&future_job).unwrap(),
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&due_job).unwrap()),
                ),
            ],
        )
        .await;

        let (jobs, has_more_due, _next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, 8)
                .await
                .unwrap();

        assert_eq!(
            jobs,
            vec![(metadata_materialization_job_key(&due_job).to_vec(), due_job)]
        );
        assert!(!has_more_due);
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                misplaced_key
            )
            .await
        );
    }

    #[tokio::test]
    async fn malformed_future_materialization_retry_preserves_encoded_due_at() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let document_id = Ulid::from_bytes([26u8; 16]);
        let event_id = Ulid::from_parts(26, 1);
        let event = create_event(document_id, event_id, "malformed-future-retry");
        let future_job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 1,
        };
        let future_key = metadata_materialization_job_key(&future_job);
        let pending_retry = MetadataMaterializationStatusRecord {
            document_id,
            event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            state: MetadataMaterializationState::Pending,
            attempts: 1,
            last_error: Some("transient".to_string()),
            updated_at_ms: now_ms,
        };
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_status_write_entry(&pending_retry).unwrap(),
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    future_key.clone(),
                    ByteView::from(vec![1, 2, 3]),
                ),
            ],
        )
        .await;

        let (jobs, has_more_due, next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, 8)
                .await
                .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert_eq!(next_due_at_ms, Some(future_job.due_at_ms));
        assert_eq!(
            read_materialization_global_job_at_key(&storage, future_key.to_vec()).await,
            Some(future_job)
        );
    }

    #[tokio::test]
    async fn noncanonical_materialization_duplicate_preserves_future_retry() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let document_id = Ulid::from_bytes([27u8; 16]);
        let event_id = Ulid::from_parts(27, 1);
        let event = create_event(document_id, event_id, "noncanonical-future-retry");
        let future_job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 1,
        };
        let stale_job = MetadataMaterializationJobRecord {
            due_at_ms: 1,
            attempts: 0,
            ..future_job.clone()
        };
        let misplaced_key = vec![0];
        let future_key = metadata_materialization_job_key(&future_job);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&future_job).unwrap(),
                (
                    METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&stale_job).unwrap()),
                ),
            ],
        )
        .await;

        let (jobs, has_more_due, next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, 8)
                .await
                .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert_eq!(next_due_at_ms, Some(future_job.due_at_ms));
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                misplaced_key
            )
            .await
        );
        assert_eq!(
            read_materialization_global_job_at_key(&storage, future_key.to_vec()).await,
            Some(future_job)
        );
    }

    #[tokio::test]
    async fn canonical_materialization_duplicate_preserves_future_retry() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let now_ms = unix_timestamp_millis();
        let document_id = Ulid::from_bytes([28u8; 16]);
        let event_id = Ulid::from_parts(28, 1);
        let event = create_event(document_id, event_id, "canonical-future-retry");
        let future_job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 1,
        };
        let stale_job = MetadataMaterializationJobRecord {
            due_at_ms: 1,
            attempts: 0,
            ..future_job.clone()
        };
        let stale_key = metadata_materialization_job_key(&stale_job);
        let future_key = metadata_materialization_job_key(&future_job);
        let document_key = metadata_materialization_document_job_key(document_id, event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&stale_job).unwrap(),
                metadata_materialization_job_write_entry(&future_job).unwrap(),
            ],
        )
        .await;

        let (jobs, has_more_due, next_due_at_ms) =
            scan_due_materialization_jobs(&storage, now_ms, 8)
                .await
                .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert_eq!(next_due_at_ms, Some(future_job.due_at_ms));
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                stale_key.to_vec()
            )
            .await
        );
        assert_eq!(
            read_materialization_global_job_at_key(&storage, future_key.to_vec()).await,
            Some(future_job.clone())
        );
        assert_eq!(
            read_materialization_document_job(&storage, document_key.to_vec()).await,
            Some(future_job)
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
    async fn noncanonical_decodable_document_sidecar_repairs_from_global_job() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([23u8; 16]);
        let old_event_id = Ulid::from_parts(23, 1);
        let newer_event_id = Ulid::from_parts(23, 2);
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 0,
        };
        let wrong_job = MetadataMaterializationJobRecord {
            event_id: Ulid::from_parts(23, 99),
            ..old_job.clone()
        };
        let old_event = create_event(document_id, old_event_id, "noncanonical-sidecar");
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&old_event).unwrap(),
                metadata_materialization_job_write_entry(&old_job).unwrap(),
                (
                    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
                    document_key.clone(),
                    ByteView::from(postcard::to_allocvec(&wrong_job).unwrap()),
                ),
            ],
        )
        .await;

        assert!(
            older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            read_materialization_document_job(&storage, document_key.to_vec()).await,
            Some(old_job)
        );
    }

    #[tokio::test]
    async fn missing_document_sidecar_repairs_from_global_job_and_blocks_newer() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([24u8; 16]);
        let old_event_id = Ulid::from_parts(24, 1);
        let newer_event_id = Ulid::from_parts(24, 2);
        let event = create_event(document_id, old_event_id, "missing-sidecar");
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 0,
        };
        let document_key = metadata_materialization_document_job_key(document_id, old_event_id);
        write_entries(
            &storage,
            vec![
                metadata_create_event_write_entry(&event).unwrap(),
                metadata_materialization_job_write_entry(&old_job).unwrap(),
            ],
        )
        .await;

        assert!(
            older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            read_materialization_document_job(&storage, document_key.to_vec()).await,
            Some(old_job)
        );
    }

    #[tokio::test]
    async fn future_orphan_global_materialization_job_is_deleted_during_predecessor_check() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let document_id = Ulid::from_bytes([25u8; 16]);
        let old_event_id = Ulid::from_parts(25, 1);
        let newer_event_id = Ulid::from_parts(25, 2);
        let old_job = MetadataMaterializationJobRecord {
            document_id,
            event_id: old_event_id,
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 0,
        };
        let global_key = metadata_materialization_job_key(&old_job);
        write_entries(
            &storage,
            vec![metadata_materialization_job_write_entry(&old_job).unwrap()],
        )
        .await;

        assert!(
            !older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );
        assert!(
            !storage_key_exists(
                &storage,
                METADATA_MATERIALIZATION_JOB_KEYSPACE,
                global_key.to_vec()
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
            !older_materialization_job_exists(&storage, document_id, event_id, &BTreeSet::new())
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

    #[test]
    fn graph_materialization_effect_uses_event_id_actor_and_wal_durability() {
        let document_id = Ulid::from_bytes([1u8; 16]);
        let event_id = Ulid::from_parts(1, 1);
        let event = create_event(document_id, event_id, "deterministic");
        let deterministic_actor = Some(deterministic_materialization_actor(event_id));

        match graph_materialization_effect(&event) {
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
        match graph_materialization_effect(&rocrate) {
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
        match graph_materialization_effect(&data) {
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
        match graph_materialization_effect(&contextual) {
            Effect::Metadata(MetadataEffect::UpsertContextualEntity { request }) => {
                assert_eq!(
                    request.durability,
                    MetadataRequestDurability::WalAlreadyDurable
                );
                assert_eq!(request.deterministic_actor, deterministic_actor);
            }
            other => panic!("unexpected materialization effect: {other:?}"),
        }
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
                graph_materialization_effect(&event),
                graph_materialization_effect(&event)
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
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            state: MetadataMaterializationState::Pending,
            attempts: 0,
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
            state: MetadataMaterializationState::Pending,
            attempts: 1,
            last_error: Some("transient".to_string()),
            updated_at_ms: 1,
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            state: MetadataMaterializationState::Pending,
            attempts: 0,
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
            state: MetadataMaterializationState::Pending,
            attempts: 1,
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
        };
        let newer_status = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            state: MetadataMaterializationState::Pending,
            attempts: 7,
            last_error: Some("newer pending".to_string()),
            updated_at_ms: 7,
        };
        let (_, old_job_key, _) = metadata_materialization_job_write_entry(&old_job).unwrap();
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
            vec![CompletedMaterializationJob {
                job_key: old_job_key.to_vec(),
                document_job_key: Some(
                    metadata_materialization_document_job_key(
                        old_job.document_id,
                        old_job.event_id,
                    )
                    .to_vec(),
                ),
                status: Some(materialization_success_status(&old_job, &old_event)),
                sync: None,
            }],
        )
        .await
        .unwrap();

        assert_eq!(
            read_materialization_status(&storage, document_id, None)
                .await
                .unwrap(),
            Some(newer_status)
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
    async fn unrelated_jobs_do_not_force_global_predecessor_scan() {
        let (storage, receiver) = StorageHandle::new();
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

            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("global predecessor scan");
            match effect {
                StorageEffect::Iter {
                    key_space,
                    prefix,
                    start,
                    limit,
                    txn_id: None,
                } => {
                    assert_eq!(key_space, METADATA_MATERIALIZATION_JOB_KEYSPACE);
                    assert_eq!(prefix, None);
                    assert_eq!(start, None);
                    assert_eq!(limit, MATERIALIZATION_SCAN_PAGE_SIZE);
                }
                other => panic!("unexpected storage effect: {other:?}"),
            }
            response_tx.send(StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            });
        });

        assert!(
            !older_materialization_job_exists(&storage, document_id, event_id, &BTreeSet::new())
                .await
                .unwrap()
        );
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
        };
        let newer_pending = MetadataMaterializationStatusRecord {
            document_id,
            event_id: newer_event_id,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            state: MetadataMaterializationState::Pending,
            attempts: 0,
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
            older_materialization_job_exists(
                &storage,
                document_id,
                newer_event_id,
                &BTreeSet::new()
            )
            .await
            .unwrap()
        );

        let mut advanced = BTreeSet::new();
        advanced.insert(older_event_id);
        assert!(
            !older_materialization_job_exists(&storage, document_id, newer_event_id, &advanced)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn retry_reschedule_is_atomic() {
        let (storage, receiver) = StorageHandle::new();
        let txn_id = Ulid::from_parts(5, 1);
        let document_id = Ulid::from_bytes([4u8; 16]);
        let event_id = Ulid::from_parts(5, 2);
        let event = create_event(document_id, event_id, "retry");
        let job = MetadataMaterializationJobRecord {
            document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
        };
        let old_job_key = b"old-job".to_vec();
        let scripted = thread::spawn({
            let old_job_key = old_job_key.clone();
            move || {
                let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                    receiver.recv().expect("start transaction request");
                assert_eq!(effect, StorageEffect::StartTransaction { read: false });
                response_tx.send(StorageEvent::TransactionStarted { txn_id });

                let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                    receiver.recv().expect("status read request");
                let status_key = match effect {
                    StorageEffect::Read {
                        key_space,
                        key,
                        txn_id: Some(read_txn_id),
                    } => {
                        assert_eq!(read_txn_id, txn_id);
                        assert_eq!(key_space, METADATA_MATERIALIZATION_STATUS_KEYSPACE);
                        key
                    }
                    other => panic!("unexpected storage effect: {other:?}"),
                };
                response_tx.send(StorageEvent::ReadResult {
                    key: status_key,
                    value: None,
                });

                let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                    receiver.recv().expect("retry writes request");
                let write_entries = match effect {
                    StorageEffect::BatchWrite {
                        writes,
                        txn_id: Some(write_txn_id),
                    } => {
                        assert_eq!(write_txn_id, txn_id);
                        assert_eq!(writes.len(), 3);
                        writes
                    }
                    other => panic!("unexpected storage effect: {other:?}"),
                };
                assert_eq!(write_entries[0].0, METADATA_MATERIALIZATION_STATUS_KEYSPACE);
                assert_eq!(write_entries[1].0, METADATA_MATERIALIZATION_JOB_KEYSPACE);
                assert_eq!(
                    write_entries[2].0,
                    METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE
                );
                response_tx.send(StorageEvent::BatchWriteResult {
                    entries: write_entries
                        .iter()
                        .map(|(key_space, key, _)| (key_space.clone(), key.clone()))
                        .collect(),
                });

                let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                    receiver.recv().expect("old job delete request");
                let deletes = match effect {
                    StorageEffect::BatchDelete {
                        deletes,
                        txn_id: Some(delete_txn_id),
                    } => {
                        assert_eq!(delete_txn_id, txn_id);
                        deletes
                    }
                    other => panic!("unexpected storage effect: {other:?}"),
                };
                assert_eq!(
                    deletes,
                    vec![(
                        METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                        ByteView::from(old_job_key)
                    )]
                );
                response_tx.send(StorageEvent::BatchDeleteResult { entries: deletes });

                let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                    receiver.recv().expect("commit request");
                assert_eq!(effect, StorageEffect::CommitTransaction { txn_id });
                response_tx.send(StorageEvent::TransactionCommitted { txn_id });
            }
        });

        reschedule_materialization_job(&storage, &old_job_key, &job, &event, "transient".into())
            .await
            .unwrap();
        scripted.join().expect("scripted storage actor finished");
    }
}
