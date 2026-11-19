//! Runs the metadata work after document sync: projection, graph prunes and maintenance.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentReconcileResult, DocumentTarget};
use aruna_core::metadata::{GraphLifecycleRecord, MetadataEventRecord};
use aruna_core::operation::Operation;
use aruna_core::task::TaskKey;
use tracing::{error, warn};

use crate::metadata::projection::{
    LoggedProjection, MetadataProjectionError, PROJECTION_RETRY_AFTER, ProjectEventsOperation,
    ProjectLoggedOperation, ProjectionWakeOperation, max_clock_skew,
};
use crate::metadata::prune_queue::{
    MetadataGraphError, TombstoneProcessingResult, TombstonesOperation,
};

/// Time spent on each part of [`AfterSyncOperation`].
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct MetadataTimings {
    pub project: Duration,
    pub prune: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SyncStage {
    Project,
    Wake,
    Prune,
}

/// Projects the create events a reconcile applied, then queues prunes for deleted graphs. With
/// `retry`, a failed projection arms the projection drain; otherwise it is only logged. Its
/// timings come from the runner's clock.
#[derive(Debug, PartialEq)]
pub struct AfterSyncOperation {
    node_id: Option<NodeId>,
    retry: bool,
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
}

impl AfterSyncOperation {
    pub fn new(node_id: Option<NodeId>, result: DocumentReconcileResult, retry: bool) -> Self {
        let DocumentReconcileResult {
            targets,
            metadata_create_events,
            metadata_graph_tombstones,
        } = result;
        Self {
            node_id,
            retry,
            targets,
            events: metadata_create_events,
            tombstones: metadata_graph_tombstones,
            stage: SyncStage::Project,
            logged: false,
            started_ms: 0,
            pruning_ms: 0,
            now_ms: 0,
            timings: MetadataTimings::default(),
        }
    }

    /// Carried create events are projected directly; without them the targets name events to
    /// read back from the log.
    fn project(&mut self) -> Option<IoCall> {
        let events = std::mem::take(&mut self.events);
        if !events.is_empty() {
            let project = ProjectEventsOperation::new(events, self.node_id, max_clock_skew());
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
        let project = ProjectLoggedOperation::new(pairs, false, self.node_id, max_clock_skew());
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

impl Operation for AfterSyncOperation {
    type Output = MetadataTimings;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<MetadataTimings>, IoError> {
        self.started_ms = self.now_ms;
        match self.project() {
            Some(call) => Ok(Next::Call(call)),
            None => Ok(self.prune()),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataTimings>, IoError> {
        match self.stage {
            SyncStage::Project => {
                let Some(projection) = self.projected(result)? else {
                    return Ok(self.prune());
                };
                if !self.retry {
                    warn!(error = ?projection, "Failed to project restored metadata create events");
                    return Ok(self.prune());
                }
                error!(error = ?projection, "Failed to project metadata create events after inbound document sync reconciliation");
                self.stage = SyncStage::Wake;
                let wake = ProjectionWakeOperation::new(PROJECTION_RETRY_AFTER);
                Ok(Next::Call(IoCall::child(wake)))
            }
            SyncStage::Wake => {
                if let Err(error) = result.child::<(), MetadataProjectionError>()? {
                    warn!(task_id = ?TaskKey::DrainProjectionQueue, error = ?error, "Failed to schedule metadata projection retry");
                }
                Ok(self.prune())
            }
            SyncStage::Prune => {
                if let Err(error) =
                    result.child::<TombstoneProcessingResult, MetadataGraphError>()?
                {
                    warn!(error = ?error, "Failed to prune deleted metadata graphs");
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
    use crate::driver::DriverContext;
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

        crate::driver::drive(
            AfterSyncOperation::new(None, result.clone(), false),
            &context,
        )
        .await
        .unwrap();
        assert!(
            read_timer(&storage, &TaskKey::DrainProjectionQueue)
                .await
                .is_none()
        );
        crate::driver::drive(AfterSyncOperation::new(None, result, true), &context)
            .await
            .unwrap();
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
        let mut after = AfterSyncOperation::new(None, empty(), true);
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

    #[test]
    fn failed_projection_retries() {
        let mut result = empty();
        result.targets = vec![DocumentTarget::MetadataCreateEvent {
            document_id: ulid::Ulid::from_parts(1, 1),
            event_id: ulid::Ulid::from_parts(2, 2),
        }];
        let failed = || {
            child(Err::<LoggedProjection, _>(
                MetadataProjectionError::PlacementFenced,
            ))
        };
        let mut retried = AfterSyncOperation::new(None, result.clone(), true);
        retried.first().unwrap();
        retried.next(failed()).unwrap();
        assert_eq!(retried.stage, SyncStage::Wake);
        let mut logged = AfterSyncOperation::new(None, result, false);
        logged.first().unwrap();
        logged.next(failed()).unwrap();
        assert_eq!(logged.stage, SyncStage::Prune);
    }
}
