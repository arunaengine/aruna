use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{METADATA_GRAPH_LIFECYCLE_KEYSPACE, METADATA_GRAPH_PRUNE_JOB_KEYSPACE};
use aruna_core::metadata::{
    MetadataError, MetadataGraphLifecycleRecord, MetadataGraphPruneJobRecord,
};
use aruna_core::storage_entries::{
    metadata_graph_lifecycle_key, metadata_graph_prune_job_key,
    metadata_graph_prune_job_write_entry,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;

use crate::queue_backoff::queue_retry_after_ms;

use super::queue_storage::{
    MetadataQueueStorageError, abort_storage_transaction_best_effort, commit_storage_transaction,
    start_write_transaction,
};

const PRUNE_SCAN_PAGE_SIZE: usize = 512;
const PRUNE_BATCH_SIZE: usize = 128;

pub const METADATA_GRAPH_PRUNE_POLL_AFTER: Duration = Duration::from_secs(5);
pub const METADATA_GRAPH_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct MetadataGraphPruneDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct MetadataGraphTombstoneProcessingResult {
    pub enqueued: usize,
    pub pruned: usize,
}

#[derive(Debug, Default)]
struct ProcessedPruneJobGroup {
    completed_keys: Vec<Vec<u8>>,
    processed: usize,
}

#[derive(Debug, Error)]
pub enum MetadataGraphPruneQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected event while processing metadata graph prune queue: {0}")]
    UnexpectedEvent(String),
}

impl From<MetadataQueueStorageError> for MetadataGraphPruneQueueError {
    fn from(error: MetadataQueueStorageError) -> Self {
        match error {
            MetadataQueueStorageError::Storage(error) => Self::Storage(error),
            MetadataQueueStorageError::UnexpectedEvent(event) => Self::UnexpectedEvent(event),
        }
    }
}

pub fn new_graph_prune_job(graph_iri: String, due_at_ms: u64) -> MetadataGraphPruneJobRecord {
    MetadataGraphPruneJobRecord::new(graph_iri, due_at_ms)
}

pub fn write_graph_prune_job_effect(
    record: &MetadataGraphPruneJobRecord,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = metadata_graph_prune_job_write_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn schedule_metadata_graph_prune_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainMetadataGraphPruneQueue,
        after: Duration::ZERO,
    })
}

pub async fn restore_metadata_graph_prune_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_metadata_graph_prune_timer_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainMetadataGraphPruneQueue,
                    after,
                }))
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore metadata graph prune timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan metadata graph prune jobs"),
    }
}

pub async fn next_metadata_graph_prune_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, MetadataGraphPruneQueueError> {
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_at_ms) =
        scan_due_graph_prune_jobs(storage, now_ms, 1).await?;
    if !jobs.is_empty() || has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(next_due_at_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms)))
}

pub async fn metadata_graph_prune_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataGraphPruneQueueError> {
    let mut start_after = None;
    loop {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
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
                match postcard::from_bytes::<MetadataGraphPruneJobRecord>(&value) {
                    Ok(job) if graph_prune_job_key_matches(key.as_ref(), &job) => {
                        return Ok(true);
                    }
                    Ok(job) => {
                        let key = key.to_vec();
                        warn!(key = ?key, "Repairing metadata graph prune job stored under non-canonical key while probing queue");
                        if find_decoded_graph_prune_job(
                            storage,
                            &job.graph_iri,
                            Some(key.as_slice()),
                        )
                        .await?
                        .is_some()
                        {
                            delete_graph_prune_jobs(storage, vec![key]).await?;
                            return Ok(true);
                        }
                        repair_graph_prune_job_key(storage, key, &job).await?;
                        return Ok(true);
                    }
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed metadata graph prune job while probing queue");
                        delete_graph_prune_jobs(storage, vec![key]).await?;
                    }
                }
                match next_start_after {
                    Some(next) => start_after = Some(next),
                    None => return Ok(false),
                }
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
    }
}

pub async fn process_metadata_graph_prune_batch(
    context: &DriverContext,
) -> Result<MetadataGraphPruneDrainResult, MetadataGraphPruneQueueError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due, next_due_at_ms) =
        scan_due_graph_prune_jobs(&context.storage_handle, now_ms, PRUNE_BATCH_SIZE).await?;
    let scan_elapsed = batch_started.elapsed();
    let job_count = jobs.len();
    let oldest_lag_ms = jobs
        .iter()
        .map(|(_, job)| now_ms.saturating_sub(job.due_at_ms))
        .max()
        .unwrap_or(0);

    let mut completed_keys = Vec::new();
    let mut processed = 0usize;
    let groups = group_prune_jobs(jobs);
    let group_count = groups.len();
    for (graph_iri, jobs) in groups {
        let outcome = process_prune_job_group(context, graph_iri, jobs).await?;
        processed = processed.saturating_add(outcome.processed);
        completed_keys.extend(outcome.completed_keys);
    }
    let finish_started = Instant::now();
    delete_graph_prune_jobs(&context.storage_handle, completed_keys).await?;
    let finish_elapsed = finish_started.elapsed();

    if job_count > 0 {
        info!(
            event = "pipeline.metadata_prune.summary",
            jobs = job_count,
            groups = group_count,
            processed,
            scan_ms = duration_ms(scan_elapsed),
            finish_ms = duration_ms(finish_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Metadata graph prune batch summary"
        );
    }
    Ok(MetadataGraphPruneDrainResult {
        processed,
        has_more_due,
        next_due_after: if has_more_due {
            Some(Duration::ZERO)
        } else {
            next_due_at_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

pub async fn enqueue_metadata_graph_prune_job(
    context: &DriverContext,
    graph_iri: String,
) -> Result<(), MetadataGraphPruneQueueError> {
    let job = new_graph_prune_job(graph_iri, unix_timestamp_millis());
    write_graph_prune_job(&context.storage_handle, &job).await?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        match task_handle
            .send_effect(schedule_metadata_graph_prune_drain_effect())
            .await
        {
            Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
                return Err(MetadataGraphPruneQueueError::UnexpectedEvent(message));
            }
            other => {
                return Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
    }
    Ok(())
}

pub async fn process_metadata_graph_tombstones(
    context: &DriverContext,
    tombstones: impl IntoIterator<Item = MetadataGraphLifecycleRecord>,
) -> MetadataGraphTombstoneProcessingResult {
    let mut result = MetadataGraphTombstoneProcessingResult::default();
    let mut seen_graphs = BTreeSet::new();
    let metadata_handle = context.metadata_handle.clone();
    for tombstone in tombstones {
        if !tombstone.is_deleted() || !seen_graphs.insert(tombstone.graph_iri.clone()) {
            continue;
        }

        if let Some(metadata_handle) = metadata_handle.as_ref() {
            metadata_handle.remove_cached_registry_record(tombstone.document_id);
        }

        if let Err(error) =
            enqueue_metadata_graph_prune_job(context, tombstone.graph_iri.clone()).await
        {
            warn!(graph_iri = %tombstone.graph_iri, error = ?error, "Failed to enqueue metadata graph prune job");
            continue;
        }
        result.enqueued = result.enqueued.saturating_add(1);

        let Some(metadata_handle) = metadata_handle.as_ref() else {
            continue;
        };
        match metadata_handle
            .prune_graph_if_deleted(tombstone.graph_iri.clone())
            .await
        {
            Ok(true) => result.pruned = result.pruned.saturating_add(1),
            Ok(false) => {}
            Err(error) => {
                warn!(graph_iri = %tombstone.graph_iri, error = ?error, "Failed to prune deleted metadata graph");
            }
        }
    }
    result
}

fn group_prune_jobs(
    jobs: Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>,
) -> BTreeMap<String, Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>> {
    let mut groups: BTreeMap<String, Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>> = BTreeMap::new();
    for (key, job) in jobs {
        groups
            .entry(job.graph_iri.clone())
            .or_default()
            .push((key, job));
    }
    groups
}

async fn process_prune_job_group(
    context: &DriverContext,
    graph_iri: String,
    jobs: Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>,
) -> Result<ProcessedPruneJobGroup, MetadataGraphPruneQueueError> {
    let job_keys = jobs.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    let job = representative_job(&graph_iri, &jobs);
    if !metadata_graph_deleted(&context.storage_handle, &graph_iri).await? {
        return Ok(ProcessedPruneJobGroup {
            completed_keys: job_keys,
            processed: 0,
        });
    }

    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        reschedule_graph_prune_job(
            &context.storage_handle,
            &job_keys,
            &job,
            "metadata handle missing".to_string(),
        )
        .await?;
        return Ok(ProcessedPruneJobGroup {
            completed_keys: Vec::new(),
            processed: 1,
        });
    };

    match metadata_handle
        .prune_graph_if_deleted(graph_iri.clone())
        .await
    {
        Ok(_) => Ok(ProcessedPruneJobGroup {
            completed_keys: job_keys,
            processed: 1,
        }),
        Err(error) => {
            reschedule_graph_prune_job(&context.storage_handle, &job_keys, &job, error.to_string())
                .await?;
            Ok(ProcessedPruneJobGroup {
                completed_keys: Vec::new(),
                processed: 1,
            })
        }
    }
}

fn representative_job(
    graph_iri: &str,
    jobs: &[(Vec<u8>, MetadataGraphPruneJobRecord)],
) -> MetadataGraphPruneJobRecord {
    jobs.iter()
        .map(|(_, job)| job)
        .max_by_key(|job| (job.attempts, job.due_at_ms))
        .cloned()
        .unwrap_or_else(|| new_graph_prune_job(graph_iri.to_string(), unix_timestamp_millis()))
}

async fn scan_due_graph_prune_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<
    (
        Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>,
        bool,
        Option<u64>,
    ),
    MetadataGraphPruneQueueError,
> {
    let mut start_after = None;
    let mut jobs = Vec::new();
    let mut next_due_at_ms = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: PRUNE_SCAN_PAGE_SIZE,
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
                return Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            let key = key.to_vec();
            let job = match postcard::from_bytes::<MetadataGraphPruneJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Deleting malformed metadata graph prune job");
                    delete_graph_prune_jobs(storage, vec![key]).await?;
                    continue;
                }
            };
            if !graph_prune_job_key_matches(&key, &job) {
                warn!(key = ?key, "Repairing metadata graph prune job stored under non-canonical key");
                if let Some(existing_job) =
                    find_decoded_graph_prune_job(storage, &job.graph_iri, Some(key.as_slice()))
                        .await?
                {
                    delete_graph_prune_jobs(storage, vec![key]).await?;
                    if existing_job.due_at_ms > now_ms {
                        next_due_at_ms = min_due_at(next_due_at_ms, existing_job.due_at_ms);
                        continue;
                    }
                    jobs.push((
                        metadata_graph_prune_job_key(&existing_job).to_vec(),
                        existing_job,
                    ));
                    if jobs.len() >= limit {
                        return Ok((jobs, true, next_due_at_ms));
                    }
                    continue;
                }
                repair_graph_prune_job_key(storage, key, &job).await?;
                if job.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
                    continue;
                }
                jobs.push((metadata_graph_prune_job_key(&job).to_vec(), job));
                if jobs.len() >= limit {
                    return Ok((jobs, true, next_due_at_ms));
                }
                continue;
            }
            if let Some(existing_job) =
                find_decoded_graph_prune_job(storage, &job.graph_iri, Some(key.as_slice())).await?
                && graph_prune_job_preferred(&existing_job, &job)
            {
                delete_graph_prune_jobs(storage, vec![key]).await?;
                if existing_job.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, existing_job.due_at_ms);
                    continue;
                }
                jobs.push((
                    metadata_graph_prune_job_key(&existing_job).to_vec(),
                    existing_job,
                ));
                if jobs.len() >= limit {
                    return Ok((jobs, true, next_due_at_ms));
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

async fn metadata_graph_deleted(
    storage: &StorageHandle,
    graph_iri: &str,
) -> Result<bool, MetadataGraphPruneQueueError> {
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
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn write_graph_prune_job(
    storage: &StorageHandle,
    job: &MetadataGraphPruneJobRecord,
) -> Result<(), MetadataGraphPruneQueueError> {
    let (key_space, key, value) = metadata_graph_prune_job_write_entry(job)?;
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
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn repair_graph_prune_job_key(
    storage: &StorageHandle,
    old_key: Vec<u8>,
    job: &MetadataGraphPruneJobRecord,
) -> Result<(), MetadataGraphPruneQueueError> {
    let canonical_key = metadata_graph_prune_job_key(job);
    let txn_id = start_write_transaction(storage).await?;
    let result = async {
        transactional_batch_write(
            storage,
            txn_id,
            vec![metadata_graph_prune_job_write_entry(job)?],
        )
        .await?;
        if old_key.as_slice() != canonical_key.as_ref() {
            transactional_batch_delete(
                storage,
                txn_id,
                vec![(
                    METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
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
                "Failed to abort metadata graph prune repair transaction",
                "Unexpected metadata graph prune repair transaction abort result",
            )
            .await;
            Err(error)
        }
    }
}

fn graph_prune_job_key_matches(key: &[u8], job: &MetadataGraphPruneJobRecord) -> bool {
    metadata_graph_prune_job_key(job).as_ref() == key
}

fn graph_prune_job_preferred(
    candidate: &MetadataGraphPruneJobRecord,
    current: &MetadataGraphPruneJobRecord,
) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

async fn find_decoded_graph_prune_job(
    storage: &StorageHandle,
    graph_iri: &str,
    skip_key: Option<&[u8]>,
) -> Result<Option<MetadataGraphPruneJobRecord>, MetadataGraphPruneQueueError> {
    let mut selected: Option<(Vec<u8>, MetadataGraphPruneJobRecord)> = None;
    let mut stale_keys = Vec::new();
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: PRUNE_SCAN_PAGE_SIZE,
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
                return Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            if skip_key.is_some_and(|skip_key| key.as_ref() == skip_key) {
                continue;
            }
            let Ok(job) = postcard::from_bytes::<MetadataGraphPruneJobRecord>(&value) else {
                continue;
            };
            if job.graph_iri != graph_iri {
                continue;
            }
            let key = key.to_vec();
            match selected.as_mut() {
                Some((selected_key, selected_job))
                    if graph_prune_job_preferred(&job, selected_job) =>
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
        return Ok(None);
    };
    if !graph_prune_job_key_matches(&key, &job) {
        repair_graph_prune_job_key(storage, key.clone(), &job).await?;
    }
    let canonical_key = metadata_graph_prune_job_key(&job);
    let stale_keys = stale_keys
        .into_iter()
        .filter(|stale_key| {
            stale_key.as_slice() != key.as_slice() && stale_key.as_slice() != canonical_key.as_ref()
        })
        .collect::<Vec<_>>();
    delete_graph_prune_jobs(storage, stale_keys).await?;
    Ok(Some(job))
}

async fn reschedule_graph_prune_job(
    storage: &StorageHandle,
    old_keys: &[Vec<u8>],
    job: &MetadataGraphPruneJobRecord,
    error: String,
) -> Result<(), MetadataGraphPruneQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let next_job = MetadataGraphPruneJobRecord {
        graph_iri: job.graph_iri.clone(),
        due_at_ms: unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts)),
        attempts,
        last_error: Some(error),
    };
    let write = metadata_graph_prune_job_write_entry(&next_job)?;
    let next_key = write.1.to_vec();
    let deletes = old_keys
        .iter()
        .filter(|key| key.as_slice() != next_key.as_slice())
        .map(|key| {
            (
                METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                ByteView::from(key.clone()),
            )
        })
        .collect::<Vec<_>>();

    let txn_id = start_write_transaction(storage).await?;
    let result = async {
        transactional_batch_write(storage, txn_id, vec![write]).await?;
        transactional_batch_delete(storage, txn_id, deletes).await
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
                "Failed to abort metadata graph prune transaction",
                "Unexpected metadata graph prune transaction abort result",
            )
            .await;
            Err(error)
        }
    }
}

async fn delete_graph_prune_jobs(
    storage: &StorageHandle,
    keys: Vec<Vec<u8>>,
) -> Result<(), MetadataGraphPruneQueueError> {
    let deletes = keys
        .into_iter()
        .map(|key| {
            (
                METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                ByteView::from(key),
            )
        })
        .collect::<Vec<_>>();
    transactional_batch_delete_no_txn(storage, deletes).await
}

async fn transactional_batch_write(
    storage: &StorageHandle,
    txn_id: Ulid,
    writes: Vec<(String, ByteView, ByteView)>,
) -> Result<(), MetadataGraphPruneQueueError> {
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
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn transactional_batch_delete(
    storage: &StorageHandle,
    txn_id: Ulid,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataGraphPruneQueueError> {
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
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn transactional_batch_delete_no_txn(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataGraphPruneQueueError> {
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
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::MetaResourceId;

    fn doc_id(seed: u64) -> MetaResourceId {
        MetaResourceId::try_from((1u128 << 60) | u128::from(seed)).unwrap()
    }
    use aruna_core::storage_entries::{
        metadata_graph_lifecycle_write_entry, metadata_registry_write_entries,
    };
    use aruna_core::structs::MetadataRegistryRecord;
    use aruna_core::structs::PlacementRef;
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    use crate::metadata::MetadataHandle;

    fn lifecycle(graph_iri: &str) -> MetadataGraphLifecycleRecord {
        MetadataGraphLifecycleRecord::deleted(
            graph_iri.to_string(),
            RealmId::from_bytes([7u8; 32]),
            Ulid::from_parts(7, 1),
            doc_id(7),
            1,
        )
    }

    fn registry_record(group_id: Ulid, path: &str) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let document_id = doc_id(1);
        MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: path.to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                path,
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            last_event_id: Ulid::nil(),
        }
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

    async fn storage_key_exists(storage: &StorageHandle, key: Vec<u8>) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<MetadataGraphPruneJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).expect("job decodes"))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn read_job_at_key(
        storage: &StorageHandle,
        key: Vec<u8>,
    ) -> Option<MetadataGraphPruneJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                key: ByteView::from(key),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| postcard::from_bytes(&value).expect("prune job decodes"))
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn document_lifecycle_tombstone_processing_enqueues_prune_and_hides_stale_registry_cache()
    {
        let dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage.clone(),
            None,
            None,
            None,
        )
        .expect("metadata handle opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle.clone()),
            task_handle: Some(TaskHandle::new()),
        };
        let record = registry_record(Ulid::from_parts(8, 1), "docs/tombstoned");
        write_entries(
            &storage,
            metadata_registry_write_entries(&record).expect("registry entries"),
        )
        .await;
        metadata_handle.expire_visibility_caches();
        let stale = metadata_handle
            .list_cached_registry_records_for_group(record.group_id)
            .await
            .expect("registry cache fills");
        assert_eq!(stale.as_ref(), &vec![record.clone()]);
        let tombstone = MetadataGraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );
        write_entries(
            &storage,
            vec![metadata_graph_lifecycle_write_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        let processed = process_metadata_graph_tombstones(&context, vec![tombstone.clone()]).await;

        assert_eq!(processed.enqueued, 1);
        let listed = metadata_handle
            .list_cached_registry_records_for_group(record.group_id)
            .await
            .expect("registry cache reads");
        assert!(listed.is_empty());
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, record.graph_iri);
    }

    #[tokio::test]
    async fn job_is_dropped_when_lifecycle_is_not_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let job = new_graph_prune_job("urn:graph:not-deleted".to_string(), 1);
        write_entries(
            &storage,
            vec![metadata_graph_prune_job_write_entry(&job).expect("job entry")],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = process_metadata_graph_prune_batch(&context)
            .await
            .expect("queue drains");

        assert_eq!(result.processed, 0);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn corrupt_graph_prune_job_only_is_deleted() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        write_entries(
            &storage,
            vec![(
                METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
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

        let result = process_metadata_graph_prune_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(!storage_key_exists(&storage, corrupt_key).await);
    }

    #[tokio::test]
    async fn graph_prune_jobs_exist_deletes_corrupt_before_valid() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        let valid_job = new_graph_prune_job("urn:graph:valid".to_string(), 1);
        write_entries(
            &storage,
            vec![
                (
                    METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(corrupt_key.clone()),
                    ByteView::from(vec![1, 2, 3]),
                ),
                metadata_graph_prune_job_write_entry(&valid_job).expect("job entry"),
            ],
        )
        .await;

        assert!(metadata_graph_prune_jobs_exist(&storage).await.unwrap());
        assert!(!storage_key_exists(&storage, corrupt_key).await);
    }

    #[tokio::test]
    async fn noncanonical_future_graph_prune_job_does_not_hide_due_job() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:future".to_string(),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 0,
            last_error: None,
        };
        let due_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:due".to_string(),
            due_at_ms: 1,
            attempts: 0,
            last_error: None,
        };
        let misplaced_key = vec![0];
        write_entries(
            &storage,
            vec![
                (
                    METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&future_job).unwrap()),
                ),
                metadata_graph_prune_job_write_entry(&due_job).expect("job entry"),
            ],
        )
        .await;

        let (jobs, has_more_due, _next_due_at_ms) = scan_due_graph_prune_jobs(&storage, now_ms, 8)
            .await
            .unwrap();

        assert_eq!(
            jobs,
            vec![(metadata_graph_prune_job_key(&due_job).to_vec(), due_job)]
        );
        assert!(!has_more_due);
        assert!(!storage_key_exists(&storage, misplaced_key).await);
        assert!(
            storage_key_exists(&storage, metadata_graph_prune_job_key(&future_job).to_vec()).await
        );
    }

    #[tokio::test]
    async fn due_noncanonical_graph_prune_job_after_future_job_is_repaired() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:future-canonical".to_string(),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 0,
            last_error: None,
        };
        let due_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:due-misplaced".to_string(),
            due_at_ms: 1,
            attempts: 0,
            last_error: None,
        };
        let misplaced_key = vec![255];
        write_entries(
            &storage,
            vec![
                metadata_graph_prune_job_write_entry(&future_job).expect("future job entry"),
                (
                    METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&due_job).unwrap()),
                ),
            ],
        )
        .await;

        let (jobs, has_more_due, _next_due_at_ms) = scan_due_graph_prune_jobs(&storage, now_ms, 8)
            .await
            .unwrap();

        assert_eq!(
            jobs,
            vec![(metadata_graph_prune_job_key(&due_job).to_vec(), due_job)]
        );
        assert!(!has_more_due);
        assert!(!storage_key_exists(&storage, misplaced_key).await);
    }

    #[tokio::test]
    async fn noncanonical_graph_prune_duplicate_preserves_future_retry() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:future-retry".to_string(),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
        };
        let stale_job = MetadataGraphPruneJobRecord {
            due_at_ms: 1,
            attempts: 0,
            last_error: None,
            ..future_job.clone()
        };
        let misplaced_key = vec![0];
        let future_key = metadata_graph_prune_job_key(&future_job);
        write_entries(
            &storage,
            vec![
                metadata_graph_prune_job_write_entry(&future_job).expect("future job entry"),
                (
                    METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(misplaced_key.clone()),
                    ByteView::from(postcard::to_allocvec(&stale_job).unwrap()),
                ),
            ],
        )
        .await;

        let (jobs, has_more_due, next_due_at_ms) = scan_due_graph_prune_jobs(&storage, now_ms, 8)
            .await
            .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert_eq!(next_due_at_ms, Some(future_job.due_at_ms));
        assert!(!storage_key_exists(&storage, misplaced_key).await);
        assert_eq!(
            read_job_at_key(&storage, future_key.to_vec()).await,
            Some(future_job)
        );
    }

    #[tokio::test]
    async fn canonical_graph_prune_duplicate_preserves_future_retry() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let now_ms = unix_timestamp_millis();
        let future_job = MetadataGraphPruneJobRecord {
            graph_iri: "urn:graph:canonical-future-retry".to_string(),
            due_at_ms: now_ms.saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
        };
        let stale_job = MetadataGraphPruneJobRecord {
            due_at_ms: 1,
            attempts: 0,
            last_error: None,
            ..future_job.clone()
        };
        let stale_key = metadata_graph_prune_job_key(&stale_job);
        let future_key = metadata_graph_prune_job_key(&future_job);
        write_entries(
            &storage,
            vec![
                metadata_graph_prune_job_write_entry(&stale_job).expect("stale job entry"),
                metadata_graph_prune_job_write_entry(&future_job).expect("future job entry"),
            ],
        )
        .await;

        let (jobs, has_more_due, next_due_at_ms) = scan_due_graph_prune_jobs(&storage, now_ms, 8)
            .await
            .unwrap();

        assert!(jobs.is_empty());
        assert!(!has_more_due);
        assert_eq!(next_due_at_ms, Some(future_job.due_at_ms));
        assert!(!storage_key_exists(&storage, stale_key.to_vec()).await);
        assert_eq!(
            read_job_at_key(&storage, future_key.to_vec()).await,
            Some(future_job)
        );
    }

    #[tokio::test]
    async fn missing_metadata_handle_reschedules_job_with_backoff() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let graph_iri = "urn:graph:deleted";
        let job = new_graph_prune_job(graph_iri.to_string(), 1);
        write_entries(
            &storage,
            vec![
                metadata_graph_lifecycle_write_entry(&lifecycle(graph_iri))
                    .expect("lifecycle entry"),
                metadata_graph_prune_job_write_entry(&job).expect("job entry"),
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

        let result = process_metadata_graph_prune_batch(&context)
            .await
            .expect("queue reschedules");

        assert_eq!(result.processed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, graph_iri);
        assert_eq!(jobs[0].attempts, 1);
        assert_eq!(
            jobs[0].last_error.as_deref(),
            Some("metadata handle missing")
        );
        assert!(jobs[0].due_at_ms > job.due_at_ms);
    }

    #[tokio::test]
    async fn metadata_graph_prune_jobs_exist_reflects_queue_state() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");

        assert!(!metadata_graph_prune_jobs_exist(&storage).await.unwrap());
        let job = new_graph_prune_job("urn:graph:queued".to_string(), 1);
        write_entries(
            &storage,
            vec![metadata_graph_prune_job_write_entry(&job).expect("job entry")],
        )
        .await;

        assert!(metadata_graph_prune_jobs_exist(&storage).await.unwrap());
    }
}
