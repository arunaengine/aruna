//! Queues and drains prune jobs that remove deleted metadata graphs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::graph_deleted::{GraphDeletedError, GraphDeletedOperation};
use crate::metadata::repository::schedule_prune_drain;
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use aruna_core::calls::IoError;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::PRUNE_JOB_KEYSPACE;
use aruna_core::metadata::{GraphLifecycleRecord, GraphPruneRecord, MetadataError};
use aruna_core::storage_entries::graph_prune_entry;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::repository::link_queue::queue_deleted;

use crate::tasks::queue_backoff::due_after;

use super::prune_jobs::{DuePrune, DuePruneOperation, RescheduleOperation};

const PRUNE_BATCH_SIZE: usize = 128;

pub const GRAPH_POLL_AFTER: Duration = Duration::from_secs(5);
pub const GRAPH_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub struct GraphDrainResult {
    pub processed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TombstoneProcessingResult {
    pub enqueued: usize,
    pub pruned: usize,
}

#[derive(Debug, Default)]
struct ProcessedJobGroup {
    completed_keys: Vec<Vec<u8>>,
    processed: usize,
}

#[derive(Debug, Error)]
pub enum MetadataGraphError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("unexpected event while processing metadata graph prune queue: {0}")]
    UnexpectedEvent(String),
}

// The runner's conflict keeps its storage error, as before the runner owned the transaction.
impl From<IoError> for MetadataGraphError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::UnexpectedEvent(other.to_string()),
        }
    }
}

pub(crate) async fn restore_prune_timer(context: &DriverContext, task_handle: &TaskHandle) {
    match next_prune_timer(context).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainPruneQueue,
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

pub(crate) async fn next_prune_timer(
    context: &DriverContext,
) -> Result<Option<Duration>, MetadataGraphError> {
    let now_ms = unix_timestamp_millis();
    let due = drive(DuePruneOperation::new(now_ms, 1), context).await?;
    if !due.jobs.is_empty() || due.has_more_due {
        return Ok(Some(Duration::ZERO));
    }
    Ok(due
        .next_due_ms
        .map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms)))
}

/// Whether any prune job is queued, due or not; repairs the queue on the way.
pub async fn prune_jobs_exist(context: &DriverContext) -> Result<bool, MetadataGraphError> {
    let queued = drive(DuePruneOperation::new(u64::MAX, 1), context).await?;
    Ok(!queued.jobs.is_empty())
}

pub async fn process_prune_batch(
    context: &DriverContext,
) -> Result<GraphDrainResult, MetadataGraphError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let DuePrune {
        jobs,
        has_more_due,
        next_due_ms,
    } = drive(DuePruneOperation::new(now_ms, PRUNE_BATCH_SIZE), context).await?;
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
        let outcome = process_prune_job(context, graph_iri, jobs).await?;
        processed = processed.saturating_add(outcome.processed);
        completed_keys.extend(outcome.completed_keys);
    }
    let finish_started = Instant::now();
    delete_prune_jobs(&context.storage_handle, completed_keys).await?;
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
    Ok(GraphDrainResult {
        processed,
        has_more_due,
        next_due_after: if has_more_due {
            Some(Duration::ZERO)
        } else {
            next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

pub(crate) async fn enqueue_prune_job(
    context: &DriverContext,
    graph_iri: String,
) -> Result<(), MetadataGraphError> {
    let job = GraphPruneRecord::new(graph_iri, unix_timestamp_millis());
    write_prune_job(&context.storage_handle, &job).await?;
    if let Some(task_handle) = context.task_handle.as_ref() {
        match task_handle.send_effect(schedule_prune_drain()).await {
            Event::Task(aruna_core::task::TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) => {
                return Err(MetadataGraphError::UnexpectedEvent(message));
            }
            other => {
                return Err(MetadataGraphError::UnexpectedEvent(
                    other.label().to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub async fn process_graph_tombstones(
    context: &DriverContext,
    tombstones: impl IntoIterator<Item = GraphLifecycleRecord>,
) -> TombstoneProcessingResult {
    let mut result = TombstoneProcessingResult::default();
    let mut seen_graphs = BTreeSet::new();
    let mut deleted_documents = BTreeSet::new();
    let metadata_handle = context.metadata_handle.clone();
    for tombstone in tombstones {
        if !tombstone.is_deleted() || !seen_graphs.insert(tombstone.graph_iri.clone()) {
            continue;
        }

        deleted_documents.insert(tombstone.document_id);
        if let Some(metadata_handle) = metadata_handle.as_ref() {
            metadata_handle.remove_cached_record(tombstone.document_id);
        }

        if let Err(error) = enqueue_prune_job(context, tombstone.graph_iri.clone()).await {
            warn!(graph_iri = %tombstone.graph_iri, error = ?error, "Failed to enqueue metadata graph prune job");
            continue;
        }
        result.enqueued = result.enqueued.saturating_add(1);

        if metadata_handle.is_none() {
            continue;
        }
        match crate::metadata::prune_if_deleted(context, tombstone.graph_iri.clone()).await {
            Ok(true) => result.pruned = result.pruned.saturating_add(1),
            Ok(false) => {}
            Err(error) => {
                warn!(graph_iri = %tombstone.graph_iri, error = ?error, "Failed to prune deleted metadata graph");
            }
        }
    }

    // A deleted document is absent from the registry, so its IRI index rows are pruned here,
    // read through the document's own rows.
    if let Err(error) = prune_deleted_index(context, &deleted_documents).await {
        warn!(error = ?error, "Failed to prune IRI index rows for deleted documents");
    }
    result
}

async fn prune_deleted_index(
    context: &DriverContext,
    document_ids: &BTreeSet<Ulid>,
) -> Result<(), MetadataError> {
    if document_ids.is_empty() {
        return Ok(());
    }
    let keys = super::iri_index::document_iri_keys(&context.storage_handle, document_ids).await?;
    super::iri_index::delete_keys(&context.storage_handle, keys).await?;
    Ok(super::data_links::delete_documents(&context.storage_handle, document_ids).await?)
}

fn group_prune_jobs(
    jobs: Vec<(Vec<u8>, GraphPruneRecord)>,
) -> BTreeMap<String, Vec<(Vec<u8>, GraphPruneRecord)>> {
    let mut groups: BTreeMap<String, Vec<(Vec<u8>, GraphPruneRecord)>> = BTreeMap::new();
    for (key, job) in jobs {
        groups
            .entry(job.graph_iri.clone())
            .or_default()
            .push((key, job));
    }
    groups
}

async fn process_prune_job(
    context: &DriverContext,
    graph_iri: String,
    jobs: Vec<(Vec<u8>, GraphPruneRecord)>,
) -> Result<ProcessedJobGroup, MetadataGraphError> {
    let job_keys = jobs.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    let job = representative_job(&graph_iri, &jobs);
    let deleted = drive(GraphDeletedOperation::new(&graph_iri), context)
        .await
        .map_err(GraphDeletedError::into_error::<MetadataGraphError>)?;
    if !deleted {
        return Ok(ProcessedJobGroup {
            completed_keys: job_keys,
            processed: 0,
        });
    }

    if context.metadata_handle.is_none() {
        reschedule_prune_job(
            context,
            &job_keys,
            &job,
            "metadata handle missing".to_string(),
        )
        .await?;
        return Ok(ProcessedJobGroup {
            completed_keys: Vec::new(),
            processed: 1,
        });
    }

    // A retry after a failed link queueing finds the graph already pruned, so it queues again.
    let pruned = match crate::metadata::prune_if_deleted(context, graph_iri.clone()).await {
        Ok(_) => queue_deleted(context, &graph_iri)
            .await
            .map_err(|error| format!("queueing the repository links failed: {error}")),
        Err(error) => Err(error.to_string()),
    };
    match pruned {
        Ok(()) => Ok(ProcessedJobGroup {
            completed_keys: job_keys,
            processed: 1,
        }),
        Err(error) => {
            reschedule_prune_job(context, &job_keys, &job, error).await?;
            Ok(ProcessedJobGroup {
                completed_keys: Vec::new(),
                processed: 1,
            })
        }
    }
}

fn representative_job(graph_iri: &str, jobs: &[(Vec<u8>, GraphPruneRecord)]) -> GraphPruneRecord {
    jobs.iter()
        .map(|(_, job)| job)
        .max_by_key(|job| (job.attempts, job.due_at_ms))
        .cloned()
        .unwrap_or_else(|| GraphPruneRecord::new(graph_iri.to_string(), unix_timestamp_millis()))
}

async fn write_prune_job(
    storage: &StorageHandle,
    job: &GraphPruneRecord,
) -> Result<(), MetadataGraphError> {
    let (key_space, key, value) = graph_prune_entry(job)?;
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
        other => Err(MetadataGraphError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

async fn reschedule_prune_job(
    context: &DriverContext,
    old_keys: &[Vec<u8>],
    job: &GraphPruneRecord,
    error: String,
) -> Result<(), MetadataGraphError> {
    let now_ms = unix_timestamp_millis();
    let operation = RescheduleOperation::new(old_keys, job, error, now_ms)?;
    Ok(drive(operation, context).await?)
}

async fn delete_prune_jobs(
    storage: &StorageHandle,
    keys: Vec<Vec<u8>>,
) -> Result<(), MetadataGraphError> {
    let deletes = keys
        .into_iter()
        .map(|key| (PRUNE_JOB_KEYSPACE.to_string(), ByteView::from(key)))
        .collect::<Vec<_>>();
    delete_batch_entries(storage, deletes).await
}

async fn delete_batch_entries(
    storage: &StorageHandle,
    deletes: Vec<(String, ByteView)>,
) -> Result<(), MetadataGraphError> {
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
        other => Err(MetadataGraphError::UnexpectedEvent(
            other.label().to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::storage_entries::{graph_lifecycle_entry, registry_write_entries};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    use crate::metadata::MetadataHandle;
    use crate::testing::write_entries;
    use crate::tests::metadata::storage_key_exists;

    fn lifecycle(graph_iri: &str) -> GraphLifecycleRecord {
        GraphLifecycleRecord::deleted(
            graph_iri.to_string(),
            RealmId::from_bytes([7u8; 32]),
            Ulid::from_parts(7, 1),
            Ulid::from_parts(7, 2),
            1,
        )
    }

    fn registry_record(group_id: Ulid, path: &str) -> MetadataRegistryRecord {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let document_id = Ulid::generate();
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
            establishing_event_id: Ulid::nil(),
            last_event_id: Ulid::nil(),
        }
    }

    async fn index_row_exists(storage: &StorageHandle, key: ByteView) -> bool {
        match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::keyspaces::IRI_INDEX_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<GraphPruneRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: PRUNE_JOB_KEYSPACE.to_string(),
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

    fn storage_context(storage: &StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    #[tokio::test]
    async fn lifecycle_tombstone_processing() {
        let dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let graph_handle = aruna_graph::GraphHandle::open(
            metadata_dir.path(),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .expect("metadata handle opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle.clone()),
            graph_handle: Some(graph_handle.clone()),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let record = registry_record(Ulid::from_parts(8, 1), "docs/tombstoned");
        write_entries(
            &storage,
            registry_write_entries(&record).expect("registry entries"),
        )
        .await;
        metadata_handle.expire_visibility_caches();
        let stale = crate::metadata::list_cached_group(&context, record.group_id)
            .await
            .expect("registry cache fills");
        assert_eq!(stale.as_ref(), &vec![record.clone()]);
        let tombstone = GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );
        write_entries(
            &storage,
            vec![graph_lifecycle_entry(&tombstone).expect("lifecycle entry")],
        )
        .await;

        let processed = process_graph_tombstones(&context, vec![tombstone.clone()]).await;

        assert_eq!(processed.enqueued, 1);
        let listed = crate::metadata::list_cached_group(&context, record.group_id)
            .await
            .expect("registry cache reads");
        assert!(listed.is_empty());
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].graph_iri, record.graph_iri);
    }

    #[tokio::test]
    async fn prunes_index_rows() {
        // Deleting a document drops its IRI reference index rows.
        let dir = tempdir().expect("temp dir");
        let metadata_dir = tempdir().expect("metadata dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let node_id = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
        let graph_handle = aruna_graph::GraphHandle::open(
            metadata_dir.path(),
            node_id,
            None,
            None,
            aruna_graph::GraphOptions::default(),
        )
        .expect("metadata handle opens");
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            storage.clone(),
            None,
            Default::default(),
        );
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            graph_handle: Some(graph_handle),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let record = registry_record(Ulid::from_parts(9, 1), "docs/deleted");
        let index_key = aruna_core::storage_entries::iri_reference_key(
            "https://schema.org/conformsTo",
            "https://example.test/profile",
            record.document_id,
        );
        let document_row =
            aruna_core::structs::storage::data::link::document_key(record.document_id, &index_key);
        write_entries(
            &storage,
            vec![
                (
                    aruna_core::keyspaces::IRI_INDEX_KEYSPACE.to_string(),
                    index_key.clone(),
                    ByteView::from(vec![1u8]),
                ),
                (
                    aruna_core::keyspaces::DOCUMENT_IRI_KEYSPACE.to_string(),
                    ByteView::from(document_row),
                    ByteView::from(Vec::new()),
                ),
            ],
        )
        .await;
        let tombstone = GraphLifecycleRecord::deleted(
            record.graph_iri.clone(),
            record.realm_id,
            record.group_id,
            record.document_id,
            2,
        );

        process_graph_tombstones(&context, vec![tombstone]).await;

        assert!(!index_row_exists(&storage, index_key).await);
    }

    #[tokio::test]
    async fn job_dropped_when() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let job = GraphPruneRecord::new("urn:graph:not-deleted".to_string(), 1);
        write_entries(&storage, vec![graph_prune_entry(&job).expect("job entry")]).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context).await.expect("queue drains");

        assert_eq!(result.processed, 0);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn corrupt_prune_job() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        write_entries(
            &storage,
            vec![(
                PRUNE_JOB_KEYSPACE.to_string(),
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(!storage_key_exists(&storage, PRUNE_JOB_KEYSPACE, corrupt_key).await);
    }

    #[tokio::test]
    async fn existence_deletes_corrupt() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let corrupt_key = vec![0];
        let valid_job = GraphPruneRecord::new("urn:graph:valid".to_string(), 1);
        write_entries(
            &storage,
            vec![
                (
                    PRUNE_JOB_KEYSPACE.to_string(),
                    ByteView::from(corrupt_key.clone()),
                    ByteView::from(vec![1, 2, 3]),
                ),
                graph_prune_entry(&valid_job).expect("job entry"),
            ],
        )
        .await;

        assert!(prune_jobs_exist(&storage_context(&storage)).await.unwrap());
        assert!(!storage_key_exists(&storage, PRUNE_JOB_KEYSPACE, corrupt_key).await);
    }

    #[tokio::test]
    async fn missing_handle_reschedules() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let graph_iri = "urn:graph:deleted";
        let job = GraphPruneRecord::new(graph_iri.to_string(), 1);
        write_entries(
            &storage,
            vec![
                graph_lifecycle_entry(&lifecycle(graph_iri)).expect("lifecycle entry"),
                graph_prune_entry(&job).expect("job entry"),
            ],
        )
        .await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = process_prune_batch(&context)
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
    async fn existence_reflects_queue() {
        let dir = tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");

        assert!(!prune_jobs_exist(&storage_context(&storage)).await.unwrap());
        let job = GraphPruneRecord::new("urn:graph:queued".to_string(), 1);
        write_entries(&storage, vec![graph_prune_entry(&job).expect("job entry")]).await;

        assert!(prune_jobs_exist(&storage_context(&storage)).await.unwrap());
    }
}
