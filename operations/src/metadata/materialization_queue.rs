use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_EVENT_LOG_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_MATERIALIZATION_JOB_KEYSPACE, METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::{
    MetadataApplyRoCrateRequest, MetadataCreateCrateRequest, MetadataCreateEventPayload,
    MetadataCreateEventRecord, MetadataEffect, MetadataError, MetadataEvent,
    MetadataGraphLifecycleRecord, MetadataGraphPolicy, MetadataMaterializationJobRecord,
    MetadataMaterializationState, MetadataMaterializationStatusRecord, MetadataRequestDurability,
    deterministic_materialization_actor,
};
use aruna_core::storage_entries::{
    metadata_event_log_key, metadata_graph_lifecycle_key, metadata_materialization_job_write_entry,
    metadata_materialization_status_key, metadata_materialization_status_write_entry,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use aruna_core::telemetry::duration_ms;
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
    status: Option<MetadataMaterializationStatusRecord>,
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
            start_after: None,
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
            timings.craqle_elapsed = timings.craqle_elapsed.saturating_add(outcome.craqle_elapsed);
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
    for (_, jobs) in groups {
        if tasks.len() >= concurrency
            && let Some(result) = tasks.join_next().await
        {
            collect_group_outcome(result, &mut completed, &mut timings, &mut first_error);
        }
        let context = context.clone();
        tasks.spawn(async move {
            let mut outcome = MaterializationGroupOutcome::default();
            for (job_key, job) in jobs {
                match process_materialization_job(&context, job_key, job).await {
                    Ok(processed_job) => {
                        outcome.craqle_elapsed = outcome
                            .craqle_elapsed
                            .saturating_add(processed_job.craqle_elapsed);
                        if let Some(completed) = processed_job.completed {
                            outcome.completed.push(completed);
                        }
                        outcome.processed = outcome.processed.saturating_add(1);
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
    Ok(timings)
}

async fn finish_completed_materialization_jobs(
    storage: &StorageHandle,
    completed: Vec<CompletedMaterializationJob>,
) -> Result<(), MetadataMaterializationQueueError> {
    if completed.is_empty() {
        return Ok(());
    }
    let mut writes = Vec::new();
    for job in &completed {
        if let Some(status) = &job.status {
            writes.push(metadata_materialization_status_write_entry(status)?);
        }
    }
    if !writes.is_empty() {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
    }
    let deletes = completed
        .into_iter()
        .map(|job| {
            (
                METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
                ByteView::from(job.job_key),
            )
        })
        .collect();
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
                start_after: start_after.take(),
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
}

impl ProcessedMaterializationJob {
    fn completed(job: CompletedMaterializationJob, craqle_elapsed: Duration) -> Self {
        Self {
            completed: Some(job),
            craqle_elapsed,
        }
    }
}

async fn process_materialization_job(
    context: &DriverContext,
    job_key: Vec<u8>,
    job: MetadataMaterializationJobRecord,
) -> Result<ProcessedMaterializationJob, MetadataMaterializationQueueError> {
    let (obsolete, event) = tokio::join!(
        materialization_job_obsolete(&context.storage_handle, &job),
        read_create_event(&context.storage_handle, job.document_id, job.event_id),
    );
    if obsolete? {
        return Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                status: None,
            },
            Duration::ZERO,
        ));
    }

    let event = event?;
    if metadata_graph_deleted(&context.storage_handle, &event.record.graph_iri).await? {
        return Ok(ProcessedMaterializationJob::completed(
            CompletedMaterializationJob {
                job_key,
                status: Some(materialization_failure_status(
                    &job,
                    &event,
                    "metadata graph was deleted before materialization".to_string(),
                    true,
                )),
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
                status: Some(materialization_success_status(&job, &event)),
            },
            craqle_elapsed,
        )),
        Err(error) if is_terminal_materialization_error(&error) => {
            Ok(ProcessedMaterializationJob::completed(
                CompletedMaterializationJob {
                    job_key,
                    status: Some(materialization_failure_status(
                        &job,
                        &event,
                        error.to_string(),
                        true,
                    )),
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
            Ok(ProcessedMaterializationJob {
                completed: None,
                craqle_elapsed,
            })
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
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
            key: metadata_materialization_status_key(job.document_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let status: MetadataMaterializationStatusRecord =
                postcard::from_bytes(&value).map_err(ConversionError::from)?;
            if status.event_id > job.event_id {
                return Ok(true);
            }
            Ok(status.event_id == job.event_id
                && matches!(
                    status.state,
                    MetadataMaterializationState::Materialized
                        | MetadataMaterializationState::Failed
                ))
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(false),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn metadata_materialization_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start_after: None,
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
        | Event::Metadata(MetadataEvent::ApplyRoCrateResult { .. }) => Ok(()),
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
        MetadataCreateEventPayload::RoCrate { jsonld } => {
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
    let status = materialization_failure_status(job, event, error, false);
    write_materialization_status(storage, &status).await?;
    let attempts = job.attempts.saturating_add(1);
    let retry_after_ms = retry_after_ms(attempts);
    let next_job = MetadataMaterializationJobRecord {
        document_id: job.document_id,
        event_id: job.event_id,
        due_at_ms: unix_timestamp_millis().saturating_add(retry_after_ms),
        attempts,
    };
    write_materialization_job(storage, &next_job).await?;
    delete_materialization_job(storage, job_key).await
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

async fn write_materialization_status(
    storage: &StorageHandle,
    status: &MetadataMaterializationStatusRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let (key_space, key, value) = metadata_materialization_status_write_entry(status)?;
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

async fn write_materialization_job(
    storage: &StorageHandle,
    job: &MetadataMaterializationJobRecord,
) -> Result<(), MetadataMaterializationQueueError> {
    let (key_space, key, value) = metadata_materialization_job_write_entry(job)?;
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

async fn delete_materialization_job(
    storage: &StorageHandle,
    job_key: &[u8],
) -> Result<(), MetadataMaterializationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: METADATA_MATERIALIZATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(job_key.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataMaterializationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}
