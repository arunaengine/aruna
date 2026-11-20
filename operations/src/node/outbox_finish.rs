//! Finishes one synced outbox sub-batch: refreshes what the synced documents feed, settles their
//! metadata follow-ups and deletes the records once nothing waits on them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentNetEvent, DocumentTarget};
use aruna_core::document_apply::ApplyError;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::TaskKey;
use tracing::warn;
use ulid::Ulid;

use crate::node::applied_drain::SettleAppliedOperation;
use crate::node::summary_refresh::{RefreshUsageError, RefreshUsageOperation};
use crate::notifications::email::RefreshMailOperation;
use crate::notifications::watch::interest::RefreshInterestOperation;
use crate::sync::document_outbox::DeleteOutboxPlan;
use crate::sync::outbox::batches::DrainSyncOutcome;

/// What finishing a sub-batch left, and whether its records were deleted, so the drain forgets
/// that it published them.
#[derive(Debug, Default, PartialEq)]
pub struct FinishedBatch {
    pub outcome: DrainSyncOutcome,
    pub deleted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FinishStep {
    Usage,
    Interest,
    Mail,
    Settle,
    Delete,
}

type FinishTurn = Result<Next<FinishedBatch>, IoError>;

/// Finishes one sub-batch from the answer its sync got. A failed sync, unsettled follow-ups or a
/// failed delete leave the records for a retry; a failed refresh is only logged.
#[derive(Debug, PartialEq)]
pub struct FinishBatchOperation {
    serving: Option<(NodeId, RealmId)>,
    generation: Ulid,
    record_keys: Vec<Vec<u8>>,
    requested: Vec<DocumentTarget>,
    synced: Option<Event>,
    applied: Vec<DocumentTarget>,
    refresh: Vec<DocumentTarget>,
    finished: FinishedBatch,
    step: FinishStep,
    started_ms: u64,
    now_ms: u64,
}

impl FinishBatchOperation {
    /// Finishes the sub-batch of `record_keys` that asked for `requested` targets, from the event
    /// its sync answered. The usage refresh records under `generation`.
    pub fn new(
        serving: Option<(NodeId, RealmId)>,
        generation: Ulid,
        (record_keys, requested): (Vec<Vec<u8>>, Vec<DocumentTarget>),
        synced: Event,
        outcome: DrainSyncOutcome,
    ) -> Self {
        Self {
            serving,
            generation,
            record_keys,
            requested,
            synced: Some(synced),
            applied: Vec::new(),
            refresh: Vec::new(),
            finished: FinishedBatch {
                outcome,
                deleted: false,
            },
            step: FinishStep::Usage,
            started_ms: 0,
            now_ms: 0,
        }
    }

    fn ask<O>(&mut self, step: FinishStep, operation: O) -> FinishTurn
    where
        O: Operation + 'static,
        O::Output: PartialEq + 'static,
        O::Error: PartialEq + 'static,
    {
        self.step = step;
        self.started_ms = self.now_ms;
        Ok(Next::Call(IoCall::child(operation)))
    }

    /// The time since the current step started.
    fn elapsed(&self) -> Duration {
        Duration::from_millis(self.now_ms.saturating_sub(self.started_ms))
    }

    fn done(&mut self) -> FinishTurn {
        Ok(Next::Done(std::mem::take(&mut self.finished)))
    }

    fn retry(&mut self) -> FinishTurn {
        self.finished.outcome.retry_needed = true;
        self.done()
    }

    fn interest(&mut self) -> FinishTurn {
        match self.serving {
            Some((_, realm_id)) => {
                let refresh = RefreshInterestOperation::new(&self.refresh, realm_id);
                self.ask(FinishStep::Interest, refresh)
            }
            None => self.mail(),
        }
    }

    fn mail(&mut self) -> FinishTurn {
        let refresh = RefreshMailOperation::applied(&self.applied);
        self.ask(FinishStep::Mail, refresh)
    }

    /// Deletes the records, and tells the dashboards when a deleted record changes them.
    fn deleted(&mut self, result: Result<(), ApplyError>) -> FinishTurn {
        self.finished.outcome.delete_elapsed = self.elapsed();
        if let Err(error) = result {
            let task_id = TaskKey::DrainSyncOutbox;
            warn!(
                task_id = ?task_id,
                error = %error,
                "Failed to delete document sync outbox records"
            );
            return self.retry();
        }
        self.finished.outcome.deleted += self.record_keys.len();
        self.finished.deleted = true;
        if !self.refresh.iter().any(DocumentTarget::changes_dashboard) {
            return self.done();
        }
        let changed = IoCall::Effect(Box::new(Effect::Net(NetEffect::DashboardChanged)));
        Ok(Next::Then {
            output: std::mem::take(&mut self.finished),
            calls: vec![changed],
        })
    }
}

impl Operation for FinishBatchOperation {
    type Output = FinishedBatch;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> FinishTurn {
        let task_id = TaskKey::DrainSyncOutbox;
        let targets = match self.synced.take() {
            Some(Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsReconciled {
                targets,
                ..
            }))) => targets,
            Some(Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. }))) => {
                warn!(task_id = ?task_id, error = %error, "Failed to sync document batch");
                return self.retry();
            }
            Some(Event::Net(NetEvent::Error(error))) => {
                warn!(task_id = ?task_id, error = ?error, "Failed to sync document batch");
                return self.retry();
            }
            other => {
                warn!(task_id = ?task_id, event = ?other, "Unexpected document sync batch result");
                return self.retry();
            }
        };
        self.refresh = targets.clone();
        self.refresh.append(&mut self.requested);
        self.applied = targets;
        match self.serving {
            Some((node_id, realm_id)) => {
                let (refresh, generation) = (&self.refresh, self.generation);
                let usage =
                    RefreshUsageOperation::new(node_id, Some(realm_id), refresh, generation);
                self.ask(FinishStep::Usage, usage)
            }
            None => self.mail(),
        }
    }

    fn next(&mut self, result: IoResult) -> FinishTurn {
        match self.step {
            FinishStep::Usage => {
                if let Err(error) = result.child::<(), RefreshUsageError>()? {
                    warn!(%error, "Realm usage refresh stopped after an outbox drain");
                }
                self.interest()
            }
            FinishStep::Interest => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Watch interest refresh stopped after an outbox drain");
                }
                self.mail()
            }
            FinishStep::Mail => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Mail settings refresh stopped after an outbox drain");
                }
                // Each apply stored its metadata follow-up with it, and the applied drain owns it;
                // the records stay for a rerun while its leftover work has no armed timer.
                self.ask(FinishStep::Settle, SettleAppliedOperation::new())
            }
            FinishStep::Settle => {
                self.finished.outcome.project_elapsed = self.elapsed();
                let settled = result.child::<bool, IoError>()?.unwrap_or_else(|error| {
                    warn!(%error, "Queued follow-ups of a document sync were not settled");
                    false
                });
                if !settled {
                    let task_id = TaskKey::DrainSyncOutbox;
                    warn!(task_id = ?task_id, "Metadata follow-ups of an outbox drain wait");
                    return self.retry();
                }
                let keys = self.record_keys.clone();
                let delete = PlanOperation::new(DeleteOutboxPlan { keys });
                self.ask(FinishStep::Delete, delete)
            }
            FinishStep::Delete => {
                let deleted = result.child::<(), ApplyError>()?;
                self.deleted(deleted)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn reconciled(targets: Vec<DocumentTarget>) -> Event {
        let reconciled = DocumentNetEvent::DocumentsReconciled {
            applied: targets.len(),
            targets,
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        };
        Event::Net(NetEvent::DocumentSync(reconciled))
    }

    fn finish(synced: Event) -> FinishBatchOperation {
        let keys = vec![b"record".to_vec()];
        let generation = Ulid::from_parts(1, 1);
        let outcome = DrainSyncOutcome::default();
        FinishBatchOperation::new(None, generation, (keys, Vec::new()), synced, outcome)
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    // A failed sync keeps the records for a retry without refreshing anything.
    #[test]
    fn failed_sync_retries() {
        let error = DocumentNetEvent::Error {
            target: None,
            error: "broken".to_string(),
        };
        let mut run = finish(Event::Net(NetEvent::DocumentSync(error)));
        let Ok(Next::Done(finished)) = run.first() else {
            panic!("a failed sync finishes at once");
        };
        assert!(finished.outcome.retry_needed && !finished.deleted);
    }

    // Unsettled follow-ups keep the records; settled ones let the records go.
    #[test]
    fn settled_records_deleted() {
        let mut run = finish(reconciled(Vec::new()));
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let mailed = run.next(child::<(), IoError>(Ok(())));
        assert!(matches!(mailed, Ok(Next::Call(IoCall::Child(_)))));
        let Ok(Next::Done(waiting)) = run.next(child::<bool, IoError>(Ok(false))) else {
            panic!("unsettled follow-ups finish the batch");
        };
        assert!(waiting.outcome.retry_needed && !waiting.deleted);

        let mut run = finish(reconciled(Vec::new()));
        run.first().unwrap();
        run.next(child::<(), IoError>(Ok(()))).unwrap();
        run.next(child::<bool, IoError>(Ok(true))).unwrap();
        let Ok(Next::Done(finished)) = run.next(child::<(), ApplyError>(Ok(()))) else {
            panic!("a deleted batch finishes");
        };
        assert!(finished.deleted && !finished.outcome.retry_needed);
        assert_eq!(finished.outcome.deleted, 1);
    }
}
