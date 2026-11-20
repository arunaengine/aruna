//! Runs the metadata work after document sync: projection, graph prunes and maintenance.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentReconcileResult, DocumentTarget};
use aruna_core::id::NodeId;
use aruna_core::metadata::{GraphLifecycleRecord, MetadataEventRecord};
use aruna_core::operation::Operation;
use thiserror::Error;
use tracing::error;

use crate::metadata::projection::{
    LoggedProjection, MetadataProjectionError, PROJECTION_RETRY_AFTER, ProjectEventsOperation,
    ProjectLoggedOperation, ProjectionWakeOperation,
};
use crate::metadata::prune_queue::{
    MetadataGraphError, TombstoneProcessingResult, TombstonesOperation,
};

/// Time spent on each part of [`AfterSyncOperation`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct MetadataTimings {
    pub project: Duration,
    pub prune: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SyncStage {
    Project,
    Wake,
    Prune,
}

/// Why the metadata work after a sync must run again.
#[derive(Debug, Error, PartialEq)]
pub enum AfterSyncError {
    /// The create events were not projected; the next batches of their documents wait for them.
    #[error("create events were not projected: {0:?}")]
    Project(MetadataProjectionError),
    #[error("the projection retry was not scheduled: {0:?}")]
    Wake(MetadataProjectionError),
    #[error("deleted metadata graphs were not queued for pruning: {0:?}")]
    Prune(MetadataGraphError),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Projects the create events a reconcile applied, then queues prunes for deleted graphs. A failed
/// projection also arms the projection drain; it, a failed handover and a failed prune are errors
/// for a caller that retries, reported after the prunes. Timings use the runner clock.
#[derive(Debug, PartialEq)]
pub struct AfterSyncOperation {
    node_id: Option<NodeId>,
    targets: Vec<DocumentTarget>,
    events: Vec<MetadataEventRecord>,
    tombstones: Vec<GraphLifecycleRecord>,
    stage: SyncStage,
    /// Whether the projection reads its events back from the log.
    logged: bool,
    started_ms: u64,
    pruning_ms: u64,
    now_ms: u64,
    timings: MetadataTimings,
    /// A projection that failed, and its retry that was not scheduled, reported once the
    /// prunes ran.
    unprojected: Option<MetadataProjectionError>,
    unscheduled: Option<MetadataProjectionError>,
}

impl AfterSyncOperation {
    pub fn new(node_id: Option<NodeId>, result: DocumentReconcileResult) -> Self {
        let DocumentReconcileResult {
            targets,
            metadata_create_events,
            metadata_graph_tombstones,
        } = result;
        Self {
            node_id,
            targets,
            events: metadata_create_events,
            tombstones: metadata_graph_tombstones,
            stage: SyncStage::Project,
            logged: false,
            started_ms: 0,
            pruning_ms: 0,
            now_ms: 0,
            timings: MetadataTimings::default(),
            unprojected: None,
            unscheduled: None,
        }
    }

    /// Carried create events are projected directly; without them the targets name events to
    /// read back from the log.
    fn project(&mut self) -> Option<IoCall> {
        let events = std::mem::take(&mut self.events);
        if !events.is_empty() {
            let project = ProjectEventsOperation::new(events, self.node_id);
            return Some(IoCall::child(project));
        }
        let pairs: Vec<_> = std::mem::take(&mut self.targets)
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
            return None;
        }
        let project = ProjectLoggedOperation::new(pairs, false, self.node_id);
        self.logged = true;
        Some(IoCall::child(project))
    }

    fn projected(&mut self, result: IoResult) -> Result<Option<MetadataProjectionError>, IoError> {
        Ok(match self.logged {
            true => result
                .child::<LoggedProjection, MetadataProjectionError>()?
                .err(),
            false => result.child::<usize, MetadataProjectionError>()?.err(),
        })
    }

    fn prune(&mut self) -> Next<MetadataTimings> {
        self.timings.project = elapsed(self.started_ms, self.now_ms);
        self.pruning_ms = self.now_ms;
        self.stage = SyncStage::Prune;
        let tombstones = TombstonesOperation::new(std::mem::take(&mut self.tombstones));
        Next::Call(IoCall::child(tombstones))
    }
}

fn elapsed(from_ms: u64, to_ms: u64) -> Duration {
    Duration::from_millis(to_ms.saturating_sub(from_ms))
}

type SyncTurn = Result<Next<MetadataTimings>, AfterSyncError>;

impl Operation for AfterSyncOperation {
    type Output = MetadataTimings;
    type Error = AfterSyncError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SyncTurn {
        self.started_ms = self.now_ms;
        match self.project() {
            Some(call) => Ok(Next::Call(call)),
            None => Ok(self.prune()),
        }
    }

    fn next(&mut self, result: IoResult) -> SyncTurn {
        match self.stage {
            SyncStage::Project => {
                let Some(projection) = self.projected(result)? else {
                    return Ok(self.prune());
                };
                error!(error = ?projection, "Create events not projected; handed to the drain");
                self.unprojected = Some(projection);
                self.stage = SyncStage::Wake;
                let wake = ProjectionWakeOperation::new(PROJECTION_RETRY_AFTER);
                Ok(Next::Call(IoCall::child(wake)))
            }
            SyncStage::Wake => {
                // The prunes do not depend on the projection, so they run before the error.
                self.unscheduled = result.child::<(), MetadataProjectionError>()?.err();
                Ok(self.prune())
            }
            SyncStage::Prune => {
                let pruned = result.child::<TombstoneProcessingResult, MetadataGraphError>()?;
                pruned.map_err(AfterSyncError::Prune)?;
                if let Some(error) = self.unscheduled.take() {
                    return Err(AfterSyncError::Wake(error));
                }
                if let Some(error) = self.unprojected.take() {
                    return Err(AfterSyncError::Project(error));
                }
                self.timings.prune = elapsed(self.pruning_ms, self.now_ms);
                Ok(Next::Done(self.timings))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
    use aruna_core::task::{PersistedTaskTimer, TaskKey};
    use aruna_drivers::driver::DriverContext;
    use aruna_storage::storage::worker::FjallStorage;
    use aruna_tasks::TaskHandle;
    use aruna_test_support::metadata::refusing;
    use tempfile::tempdir;
    use ulid::Ulid;

    async fn read_timer(
        storage: &aruna_storage::storage::handle::StorageHandle,
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
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let result = DocumentReconcileResult {
            targets: vec![DocumentTarget::MetadataCreateEvent {
                document_id: ulid::Ulid::generate(),
                event_id: ulid::Ulid::generate(),
            }],
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        };

        let after = AfterSyncOperation::new(None, result);
        let failed = aruna_drivers::driver::drive(after, &context).await;
        assert!(matches!(failed, Err(AfterSyncError::Project(_))));
        let timer = read_timer(&storage, &TaskKey::DrainProjectionQueue)
            .await
            .expect("projection retry timer persisted");
        assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn empty() -> DocumentReconcileResult {
        DocumentReconcileResult {
            targets: Vec::new(),
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        }
    }

    #[test]
    fn nothing_projected_prunes() {
        let mut after = AfterSyncOperation::new(None, empty());
        after.stamp(10);
        assert!(matches!(after.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(after.stage, SyncStage::Prune);
        after.stamp(25);
        let pruned = child(Ok::<_, MetadataGraphError>(
            TombstoneProcessingResult::default(),
        ));
        assert_eq!(
            after.next(pruned),
            Ok(Next::Done(MetadataTimings {
                project: Duration::ZERO,
                prune: Duration::from_millis(15),
            }))
        );
    }

    fn unprojected() -> IoResult {
        child(Err::<LoggedProjection, _>(
            MetadataProjectionError::PlacementFenced,
        ))
    }

    // A failed projection is handed to the projection drain and still ends in an error, as do a
    // failed handover and prune, so the applied drain keeps the follow-up and its document's
    // later batches wait for it instead of projecting first.
    #[test]
    fn failed_projection_retries() {
        let mut result = empty();
        result.targets = vec![DocumentTarget::MetadataCreateEvent {
            document_id: ulid::Ulid::from_parts(1, 1),
            event_id: ulid::Ulid::from_parts(2, 2),
        }];
        let mut handed = AfterSyncOperation::new(None, result.clone());
        handed.first().unwrap();
        handed.next(unprojected()).unwrap();
        handed.next(child(Ok::<(), MetadataProjectionError>(()))).unwrap();
        let pruned = child(Ok::<_, MetadataGraphError>(
            TombstoneProcessingResult::default(),
        ));
        let unprojected = AfterSyncError::Project(MetadataProjectionError::PlacementFenced);
        assert_eq!(handed.next(pruned), Err(unprojected));

        let mut after = AfterSyncOperation::new(None, result);
        after.first().unwrap();
        after.next(unprojected()).unwrap();
        assert_eq!(after.stage, SyncStage::Wake);
        let unscheduled = child(Err::<(), _>(MetadataProjectionError::PlacementFenced));
        assert!(matches!(
            after.next(unscheduled),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(after.stage, SyncStage::Prune);
        let pruned = child(Ok::<_, MetadataGraphError>(
            TombstoneProcessingResult::default(),
        ));
        let failed = AfterSyncError::Wake(MetadataProjectionError::PlacementFenced);
        assert_eq!(after.next(pruned), Err(failed));
    }

    // A projection retry that could not be scheduled still lets the tombstones of the batch
    // queue their prune jobs; the error comes after them.
    #[tokio::test]
    async fn failed_wake_prunes() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let refused = refusing(storage.clone(), TASK_TIMER_KEYSPACE);
        let context = aruna_drivers::testing::test_context(refused);
        let (realm_id, group_id) = (RealmId::from_bytes([7u8; 32]), Ulid::from_parts(3, 1));
        let document_id = Ulid::from_parts(3, 3);
        let graph_iri = MetadataRegistryRecord::graph_iri_for(document_id);
        let tombstone =
            GraphLifecycleRecord::deleted(graph_iri, realm_id, group_id, document_id, 2);
        let result = DocumentReconcileResult {
            targets: vec![DocumentTarget::MetadataCreateEvent {
                document_id: ulid::Ulid::from_parts(4, 1),
                event_id: ulid::Ulid::from_parts(4, 2),
            }],
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: vec![tombstone],
        };

        let after = aruna_drivers::driver::drive(AfterSyncOperation::new(None, result), &context);

        assert!(matches!(after.await, Err(AfterSyncError::Wake(_))));
        let queued = aruna_test_support::metadata::prune_jobs_exist(&context).await;
        assert!(queued.expect("prune jobs read"));
    }

    #[test]
    fn failed_prune_errs() {
        let mut after = AfterSyncOperation::new(None, empty());
        after.first().unwrap();
        let error = || MetadataGraphError::UnexpectedEvent("down".to_string());
        let pruned = after.next(child(Err::<TombstoneProcessingResult, _>(error())));
        assert_eq!(pruned, Err(AfterSyncError::Prune(error())));
    }
}
