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
    metadata_graph_lifecycle_key, metadata_graph_prune_job_write_entry,
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

const PRUNE_SCAN_PAGE_SIZE: usize = 512;
const PRUNE_BATCH_SIZE: usize = 128;
const PRUNE_RETRY_BASE_MS: u64 = 250;
const PRUNE_RETRY_MAX_MS: u64 = 30_000;

pub const METADATA_GRAPH_PRUNE_POLL_AFTER: Duration = Duration::from_secs(5);
pub const METADATA_GRAPH_PRUNE_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct MetadataGraphPruneDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
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
    match metadata_graph_prune_jobs_exist(storage).await {
        Ok(false) => {}
        Ok(true) => {
            let event = task_handle
                .send_effect(schedule_metadata_graph_prune_drain_effect())
                .await;
            if let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore metadata graph prune timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan metadata graph prune jobs"),
    }
}

pub async fn metadata_graph_prune_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, MetadataGraphPruneQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_GRAPH_PRUNE_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn process_metadata_graph_prune_batch(
    context: &DriverContext,
) -> Result<MetadataGraphPruneDrainResult, MetadataGraphPruneQueueError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let (jobs, has_more_due) =
        read_due_graph_prune_jobs(&context.storage_handle, now_ms, PRUNE_BATCH_SIZE).await?;
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
            metadata_handle.remove_visible_registry_record(tombstone.document_id);
            metadata_handle.remove_cached_accepted_create(tombstone.document_id);
        }
        crate::metadata::visible_registry::remove_visible_registry_record(
            context,
            tombstone.group_id,
            tombstone.document_id,
        );

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

async fn read_due_graph_prune_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, MetadataGraphPruneJobRecord)>, bool), MetadataGraphPruneQueueError> {
    let mut start_after = None;
    let mut jobs = Vec::new();
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
            let job = match postcard::from_bytes::<MetadataGraphPruneJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Failed to decode metadata graph prune job");
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

async fn reschedule_graph_prune_job(
    storage: &StorageHandle,
    old_keys: &[Vec<u8>],
    job: &MetadataGraphPruneJobRecord,
    error: String,
) -> Result<(), MetadataGraphPruneQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let next_job = MetadataGraphPruneJobRecord {
        graph_iri: job.graph_iri.clone(),
        due_at_ms: unix_timestamp_millis().saturating_add(retry_after_ms(attempts)),
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
        Ok(()) => commit_storage_transaction(storage, txn_id).await,
        Err(error) => {
            abort_storage_transaction_best_effort(storage, txn_id).await;
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

fn retry_after_ms(attempts: u32) -> u64 {
    let shift = attempts.min(7);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    PRUNE_RETRY_BASE_MS
        .saturating_mul(multiplier)
        .min(PRUNE_RETRY_MAX_MS)
}

async fn start_write_transaction(
    storage: &StorageHandle,
) -> Result<Ulid, MetadataGraphPruneQueueError> {
    match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => Ok(txn_id),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn commit_storage_transaction(
    storage: &StorageHandle,
    txn_id: Ulid,
) -> Result<(), MetadataGraphPruneQueueError> {
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataGraphPruneQueueError::UnexpectedEvent(format!(
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
            warn!(error = %error, txn_id = %txn_id, "Failed to abort metadata graph prune transaction");
        }
        other => {
            warn!(event = ?other, txn_id = %txn_id, "Unexpected metadata graph prune transaction abort result");
        }
    }
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
    use aruna_core::storage_entries::{
        metadata_graph_lifecycle_write_entry, metadata_registry_write_entries,
    };
    use aruna_core::structs::MetadataRegistryRecord;
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
            Ulid::from_parts(7, 2),
            1,
        )
    }

    fn registry_record(group_id: Ulid, path: &str) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let document_id = Ulid::new();
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
        crate::metadata::visible_registry::invalidate_visible_registry(&context);
        let stale = crate::metadata::visible_registry::list_visible_registry_records_for_group(
            &context,
            record.group_id,
        )
        .await
        .expect("visible registry fills");
        assert_eq!(stale.as_ref(), &vec![record.clone()]);
        metadata_handle.cache_accepted_create(record.clone());

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
        assert!(
            metadata_handle
                .cached_accepted_create(record.document_id)
                .is_none()
        );
        let listed = crate::metadata::visible_registry::list_visible_registry_records_for_group(
            &context,
            record.group_id,
        )
        .await
        .expect("visible registry reads");
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
