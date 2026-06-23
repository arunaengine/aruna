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
    metadata_materialization_document_job_write_entry, metadata_materialization_job_write_entry,
    metadata_materialization_status_key, metadata_materialization_status_write_entry,
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

const MATERIALIZATION_SCAN_PAGE_SIZE: usize = 512;
const MATERIALIZATION_BATCH_SIZE: usize = 128;
const MATERIALIZATION_RETRY_BASE_MS: u64 = 250;
const MATERIALIZATION_RETRY_MAX_MS: u64 = 30_000;

pub const METADATA_MATERIALIZATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const METADATA_MATERIALIZATION_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct MetadataMaterializationDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
}

#[derive(Debug)]
struct CompletedMaterializationJob {
    job_key: Vec<u8>,
    document_job_key: Vec<u8>,
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
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty() => {}
        Event::Storage(StorageEvent::IterResult { .. }) => {
            let event = task_handle
                .send_effect(schedule_metadata_materialization_drain_effect())
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore metadata materialization timer");
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, "Failed to scan metadata materialization jobs");
        }
        other => {
            warn!(event = ?other, "Unexpected event while scanning metadata materialization jobs")
        }
    }
}

pub async fn process_metadata_materialization_batch(
    context: &DriverContext,
) -> Result<MetadataMaterializationDrainResult, MetadataMaterializationQueueError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due) =
        read_due_materialization_jobs(&context.storage_handle, now_ms, MATERIALIZATION_BATCH_SIZE)
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
    for (_, mut jobs) in groups {
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
        Ok(()) => commit_storage_transaction(storage, txn_id).await,
        Err(error) => {
            abort_storage_transaction_best_effort(storage, txn_id).await;
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
        deletes.push((
            METADATA_MATERIALIZATION_DOCUMENT_JOB_KEYSPACE.to_string(),
            ByteView::from(job.document_job_key),
        ));
    }

    transactional_batch_write(storage, txn_id, writes).await?;
    transactional_batch_delete(storage, txn_id, deletes).await
}

async fn start_write_transaction(
    storage: &StorageHandle,
) -> Result<Ulid, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn commit_storage_transaction(
    storage: &StorageHandle,
    txn_id: Ulid,
) -> Result<(), MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn abort_storage_transaction_best_effort(storage: &StorageHandle, txn_id: Ulid) {
    match storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionAborted { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            warn!(error = %error, txn_id = %txn_id, "Failed to abort materialization storage transaction");
        }
        other => {
            warn!(event = ?other, txn_id = %txn_id, "Unexpected materialization storage transaction abort result");
        }
    }
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

async fn read_due_materialization_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<
    (Vec<(Vec<u8>, MetadataMaterializationJobRecord)>, bool),
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

        for (key, value) in values {
            let job = match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Failed to decode metadata materialization job");
                    continue;
                }
            };
            if job.due_at_ms > now_ms {
                return Ok((jobs, false));
            }
            jobs.push((key.to_vec(), job));
            if jobs.len() >= limit {
                return Ok((jobs, true));
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok((jobs, false)),
        }
    }
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

    let (obsolete, event) = tokio::join!(
        materialization_job_obsolete(&context.storage_handle, &job),
        read_create_event(&context.storage_handle, job.document_id, job.event_id),
    );
    if obsolete? {
        return Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                document_job_key,
                status: None,
                sync: None,
            },
            Duration::ZERO,
        ));
    }

    let event = event?;
    if metadata_graph_deleted(&context.storage_handle, &event.record.graph_iri).await? {
        return Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                document_job_key,
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
                document_job_key,
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
                    document_job_key,
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
            if key.as_ref() >= stop_key.as_ref() {
                return Ok(false);
            }
            let job = match postcard::from_bytes::<MetadataMaterializationJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, "Failed to decode metadata materialization job while checking predecessors");
                    continue;
                }
            };
            if job.document_id == document_id
                && job.event_id < event_id
                && !advanced_event_ids.contains(&job.event_id)
                && !status
                    .as_ref()
                    .is_some_and(|status| materialization_status_obsoletes_job(status, &job))
            {
                return Ok(true);
            }
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
        }) => Ok(postcard::from_bytes(&value).map_err(ConversionError::from)?),
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

async fn materialization_job_obsolete(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<bool, MetadataMaterializationQueueError> {
    let Some(status) = read_materialization_status(storage, job.document_id, None).await? else {
        return Ok(false);
    };
    Ok(materialization_status_obsoletes_job(&status, job))
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
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
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
        Ok(()) => commit_storage_transaction(storage, txn_id).await,
        Err(error) => {
            abort_storage_transaction_best_effort(storage, txn_id).await;
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
    let retry_after_ms = retry_after_ms(attempts);
    let next_job = MetadataMaterializationJobRecord {
        document_id: job.document_id,
        event_id: job.event_id,
        due_at_ms: unix_timestamp_millis().saturating_add(retry_after_ms),
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

fn retry_after_ms(attempts: u32) -> u64 {
    let shift = attempts.min(7);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    MATERIALIZATION_RETRY_BASE_MS
        .saturating_mul(multiplier)
        .min(MATERIALIZATION_RETRY_MAX_MS)
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
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
}
