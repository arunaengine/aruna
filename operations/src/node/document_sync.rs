//! Runs the metadata work after document sync: projection, graph prunes and maintenance.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::document::{DocumentReconcileResult, DocumentTarget};
use aruna_core::metadata::MetadataEventRecord;
use aruna_core::task::TaskKey;
use tracing::{debug, error, warn};

use crate::driver::DriverContext;
use crate::metadata::projector::{
    MetadataProjectionError, PROJECTION_RETRY_AFTER, project_create_events, project_logged_events,
    schedule_projection_drain,
};
use crate::metadata::prune_queue::process_graph_tombstones;

/// Time spent on each part of [`apply_metadata`].
pub(crate) struct MetadataTimings {
    pub project: Duration,
    pub prune: Duration,
}

/// Projects the create events a reconcile applied, then queues prunes for deleted graphs.
/// With `retry`, a failed projection arms the projection drain; otherwise it is only logged.
pub(crate) async fn apply_metadata(
    context: &DriverContext,
    node_id: Option<NodeId>,
    result: DocumentReconcileResult,
    retry: bool,
) -> MetadataTimings {
    let DocumentReconcileResult {
        targets,
        metadata_create_events,
        metadata_graph_tombstones,
    } = result;
    let project_started = Instant::now();
    if let Err(projection) =
        project_reconciled(context, node_id, targets, metadata_create_events).await
    {
        if retry {
            error!(error = ?projection, "Failed to project metadata create events after inbound document sync reconciliation");
            if let Err(error) = schedule_projection_drain(context, PROJECTION_RETRY_AFTER).await {
                warn!(task_id = ?TaskKey::DrainProjectionQueue, error = ?error, "Failed to schedule metadata projection retry");
            }
        } else {
            warn!(error = ?projection, "Failed to project restored metadata create events");
        }
    }
    let project = project_started.elapsed();
    let prune_started = Instant::now();
    process_graph_tombstones(context, metadata_graph_tombstones).await;
    MetadataTimings {
        project,
        prune: prune_started.elapsed(),
    }
}

// Carried create events are projected directly; without them the targets name events to
// read back from the log.
async fn project_reconciled(
    context: &DriverContext,
    node_id: Option<NodeId>,
    targets: Vec<DocumentTarget>,
    events: Vec<MetadataEventRecord>,
) -> Result<usize, MetadataProjectionError> {
    if !events.is_empty() {
        return project_create_events(context, events, node_id).await;
    }
    let pairs: Vec<_> = targets
        .into_iter()
        .filter_map(|target| match target {
            DocumentTarget::MetadataCreateEvent {
                document_id,
                event_id,
            } => Some((document_id, event_id)),
            _ => None,
        })
        .collect();
    if pairs.is_empty() {
        return Ok(0);
    }
    project_logged_events(context, pairs).await
}

/// Reconciles the metadata store with document sync and prunes deleted graphs.
pub(crate) async fn run_maintenance(context: &DriverContext, source: &'static str, attempt: usize) {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return;
    };
    if let Err(error) = metadata_handle.reconcile_document_sync(context).await {
        warn!(
            source,
            attempt,
            error = ?error,
            "Metadata document sync reconciliation failed"
        );
    }
    match metadata_handle.prune_deleted_graphs(context).await {
        Ok(pruned) if pruned > 0 => {
            debug!(source, attempt, pruned, "Metadata graph prune completed")
        }
        Ok(_) => {}
        Err(error) => warn!(
            source,
            attempt,
            error = ?error,
            "Metadata graph prune failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
    use aruna_core::task::PersistedTaskTimer;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;

    async fn read_timer(
        storage: &aruna_storage::StorageHandle,
        key: &TaskKey,
    ) -> Option<PersistedTaskTimer> {
        let event = storage
            .send_storage_effect(aruna_core::effects::StorageEffect::Read {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                key: postcard::to_allocvec(key).unwrap().into(),
                txn_id: None,
            })
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| postcard::from_bytes(&value).expect("timer decodes"))
            }
            other => panic!("unexpected task timer read event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn projection_failure_retries() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let result = DocumentReconcileResult {
            targets: vec![DocumentTarget::MetadataCreateEvent {
                document_id: ulid::Ulid::generate(),
                event_id: ulid::Ulid::generate(),
            }],
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        };

        apply_metadata(&context, None, result.clone(), false).await;
        assert!(
            read_timer(&storage, &TaskKey::DrainProjectionQueue)
                .await
                .is_none()
        );
        apply_metadata(&context, None, result, true).await;
        let timer = read_timer(&storage, &TaskKey::DrainProjectionQueue)
            .await
            .expect("projection retry timer persisted");
        assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
    }
}
