//! Reconciles sync topics: reads causal batches from net, applies them, and moves the cursors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tracing::warn;

use super::batch::{ApplyBatchOperation, BatchOutcome};
use super::create::{CreateBatchOutcome, CreateBatchPlan, PendingCreateApply};
use super::distribution::RetryParkedPlan;
use super::overflow::{ReplayOverflowOperation, overflow_key};
use super::quarantine::CursorEvidencePlan;
use super::reads::dependency_available;
use super::registry::Row;
use super::{ApplyError, Step, deferred_topics_key, invalid, plan_call, topic_cursor_key};
use crate::calls::{IoCall, IoError, IoResult, Next};
use crate::deferred_topics::{
    DeferredRegistrationOutcome, DocumentSyncDependency, register_deferred_topic,
    remove_deferred_topic,
};
use crate::document::{
    DocumentEffect, DocumentNetEvent, DocumentReconcileResult, DocumentTarget, ReconcileBatch,
};
use crate::effects::{Effect, NetEffect};
use crate::events::{Event, NetEvent};
use crate::keyspaces::APPLIED_OPS_KEYSPACE;
use crate::operation::{HoldScope, Operation, TransactionMode};
use crate::row_plan::{NeedsRows, PlanRows, PlanStop, Planned, RowPlan};
use crate::rows::{done, event, read_row, value};
use crate::structs::SyncRejection;
use crate::structs::identity::realm::RealmId;
use crate::types::{Key, Value};

type DeferredTopics = BTreeMap<DocumentSyncDependency, BTreeSet<irokle::TopicId>>;

/// Topics still to reconcile plus the stored registry of topics waiting for a dependency.
#[derive(Debug, Default, PartialEq)]
pub struct ReconcileWork {
    deferred: DeferredTopics,
    queued: BTreeSet<irokle::TopicId>,
    queue: VecDeque<irokle::TopicId>,
}

impl ReconcileWork {
    fn push(&mut self, topic_id: irokle::TopicId) {
        if self.queued.insert(topic_id) {
            self.queue.push_back(topic_id);
        }
    }
}

/// Starts a pass: dependencies that became available release their topics once per pass, so a
/// restart does not wait for a new event to seed its queue again.
#[derive(Clone, Debug, PartialEq)]
struct PendingWorkPlan {
    topics: Vec<irokle::TopicId>,
}

impl RowPlan for PendingWorkPlan {
    type Output = ReconcileWork;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<ReconcileWork>> {
        let deferred: DeferredTopics = rows
            .read(APPLIED_OPS_KEYSPACE, deferred_topics_key())?
            .map(|bytes| postcard::from_bytes(&bytes))
            .transpose()
            .map_err(invalid)?
            .unwrap_or_default();
        let mut work = ReconcileWork {
            deferred,
            ..ReconcileWork::default()
        };
        for topic_id in &self.topics {
            work.push(*topic_id);
        }
        let mut satisfied = Vec::new();
        let mut waiting = false;
        for dependency in work.deferred.keys().copied() {
            match dependency_available(rows, dependency) {
                Ok(true) => satisfied.push(dependency),
                Ok(false) => {}
                Err(PlanStop::NeedsRows) => waiting = true,
                Err(stop) => return Err(stop),
            }
        }
        if waiting {
            return Err(NeedsRows.into());
        }
        for dependency in satisfied {
            for topic_id in work.deferred.remove(&dependency).unwrap_or_default() {
                work.push(topic_id);
            }
        }
        Ok(Planned::output(work))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::None
    }
}

/// Where the current topic's batch moves its cursor.
#[derive(Debug, PartialEq)]
struct BatchCursor {
    topic_id: irokle::TopicId,
    key: Key,
    value: Value,
    start: Value,
}

#[derive(Debug, PartialEq)]
enum Stage {
    Planning,
    ReadingCursor(irokle::TopicId),
    LoadingBatch(irokle::TopicId),
    Applying(BatchCursor),
    CheckingOverflow(BatchCursor, Box<BatchOutcome>),
    RecordingOverflow(BatchCursor, Box<BatchOutcome>),
    WritingCursor(irokle::TopicId, BTreeSet<DocumentSyncDependency>),
    WritingEvidence(irokle::TopicId, BTreeSet<DocumentSyncDependency>),
    SavingDeferred,
    ApplyingCreates,
    SavingUpdated,
    RetryingParked,
    ReplayingOverflow,
    ReloadingPeers,
}

/// One reconcile pass over the requested topics. It holds the reconcile scope, so cursor repair,
/// admin settlement and the deferred-topic registry see a consistent view.
#[derive(Debug, PartialEq)]
pub struct ReconcileTopicsOperation {
    realm_id: RealmId,
    topics: Vec<irokle::TopicId>,
    reload_peers: bool,
    work: ReconcileWork,
    result: DocumentReconcileResult,
    pending_creates: Vec<PendingCreateApply>,
    cursor_writes: Vec<(irokle::TopicId, Row)>,
    rejections: Vec<SyncRejection>,
    saved_deferred: Vec<u8>,
    stage: Stage,
}

type PassNext = Result<Next<DocumentReconcileResult>, ApplyError>;

impl ReconcileTopicsOperation {
    pub fn new(realm_id: RealmId, topics: Vec<irokle::TopicId>) -> Self {
        Self {
            realm_id,
            topics,
            reload_peers: false,
            work: ReconcileWork::default(),
            result: DocumentReconcileResult::default(),
            pending_creates: Vec::new(),
            cursor_writes: Vec::new(),
            rejections: Vec::new(),
            saved_deferred: Vec::new(),
            stage: Stage::Planning,
        }
    }

    /// Also reloads the realm's peers when the pass applied a realm config.
    pub fn reloading_peers(mut self) -> Self {
        self.reload_peers = true;
        self
    }

    fn call(&mut self, stage: Stage, call: IoCall) -> PassNext {
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn net_call(&mut self, stage: Stage, effect: DocumentEffect) -> PassNext {
        let effect = Effect::Net(NetEffect::DocumentSync(effect));
        self.call(stage, IoCall::Effect(Box::new(effect)))
    }

    fn next_topic(&mut self) -> PassNext {
        let Some(topic_id) = self.work.queue.pop_front() else {
            return self.save_deferred();
        };
        self.work.queued.remove(&topic_id);
        self.pending_creates
            .retain(|pending| pending.identity.topic != topic_id);
        self.cursor_writes.retain(|(topic, _)| *topic != topic_id);
        self.rejections
            .retain(|rejection| rejection.topic_id() != topic_id);
        let read = read_row(APPLIED_OPS_KEYSPACE, topic_cursor_key(topic_id));
        self.call(Stage::ReadingCursor(topic_id), read)
    }

    fn on_batch(&mut self, topic_id: irokle::TopicId, batch: Option<ReconcileBatch>) -> PassNext {
        let Some(batch) = batch else {
            return self.next_topic();
        };
        let ReconcileBatch {
            start_value,
            cursor_key,
            cursor_value,
            rejections,
            events,
        } = batch;
        let apply = ApplyBatchOperation::new(topic_id, self.realm_id, events, rejections);
        let cursor = BatchCursor {
            topic_id,
            key: cursor_key,
            value: cursor_value,
            start: start_value,
        };
        self.call(Stage::Applying(cursor), IoCall::child(apply))
    }

    fn settle(&mut self, cursor: BatchCursor, mut outcome: BatchOutcome) -> PassNext {
        let BatchCursor {
            topic_id,
            key,
            value,
            ..
        } = cursor;
        self.result.targets.append(&mut outcome.applied_targets);
        self.result
            .metadata_graph_tombstones
            .append(&mut outcome.graph_tombstones);
        self.pending_creates.append(&mut outcome.pending_creates);
        if !outcome.cross_topic.is_empty() {
            remove_deferred_topic(&mut self.work.deferred, topic_id);
            for dependency in outcome.cross_topic {
                self.defer(dependency, topic_id);
            }
            // Registry capacity limits retry discovery, not whether an unresolved topic is safe
            // to mark as applied.
            return self.next_topic();
        }
        let write = (APPLIED_OPS_KEYSPACE.to_string(), key, value);
        if outcome.deferred_creates {
            self.rejections.append(&mut outcome.rejections);
            self.cursor_writes.push((topic_id, write));
            return self.topic_done(topic_id, outcome.satisfied);
        }
        if outcome.rejections.is_empty() {
            let stage = Stage::WritingCursor(topic_id, outcome.satisfied);
            return self.call(stage, IoCall::Write(vec![write]));
        }
        let plan = CursorEvidencePlan {
            rejections: outcome.rejections,
            cursor: write,
        };
        self.call(
            Stage::WritingEvidence(topic_id, outcome.satisfied),
            plan_call(plan),
        )
    }

    fn defer(&mut self, dependency: DocumentSyncDependency, topic_id: irokle::TopicId) {
        if matches!(
            register_deferred_topic(&mut self.work.deferred, dependency, topic_id),
            DeferredRegistrationOutcome::CapacityExceeded
        ) {
            warn!(%topic_id, ?dependency, "Dropping document dependency registration because the deferred-topic registry is full");
        }
    }

    fn topic_done(
        &mut self,
        topic_id: irokle::TopicId,
        satisfied: BTreeSet<DocumentSyncDependency>,
    ) -> PassNext {
        remove_deferred_topic(&mut self.work.deferred, topic_id);
        for dependency in satisfied {
            for topic in self.work.deferred.remove(&dependency).unwrap_or_default() {
                self.work.push(topic);
            }
        }
        self.next_topic()
    }

    fn deferred_row(&self) -> Result<(Vec<u8>, IoCall), ApplyError> {
        let bytes = postcard::to_allocvec(&self.work.deferred).map_err(invalid)?;
        let row = (
            APPLIED_OPS_KEYSPACE.to_string(),
            deferred_topics_key(),
            Value::from(bytes.clone()),
        );
        Ok((bytes, IoCall::Write(vec![row])))
    }

    fn save_deferred(&mut self) -> PassNext {
        let (bytes, call) = self.deferred_row()?;
        self.saved_deferred = bytes;
        self.call(Stage::SavingDeferred, call)
    }

    fn apply_creates(&mut self) -> PassNext {
        let plan = CreateBatchPlan::new(
            std::mem::take(&mut self.pending_creates),
            std::mem::take(&mut self.cursor_writes),
            std::mem::take(&mut self.rejections),
        )?;
        self.call(Stage::ApplyingCreates, plan_call(plan))
    }

    fn on_creates(&mut self, outcome: CreateBatchOutcome) -> PassNext {
        for note in outcome.notes {
            warn!(topic_id = %note.topic, document_id = %note.document_id, "{}", note.message);
        }
        for (dependency, topic_id) in outcome.deferred {
            self.defer(dependency, topic_id);
        }
        for (target, record) in outcome.accepted {
            self.result.targets.push(target);
            self.result.metadata_create_events.push(record);
        }
        let (bytes, call) = self.deferred_row()?;
        if bytes != self.saved_deferred {
            return self.call(Stage::SavingUpdated, call);
        }
        self.retry_parked()
    }

    /// Every pass gives parked snapshots another chance: a local membership change never passes
    /// through the replicated apply path.
    fn retry_parked(&mut self) -> PassNext {
        let plan = RetryParkedPlan {
            realm_id: self.realm_id,
        };
        self.call(Stage::RetryingParked, plan_call(plan))
    }

    fn finish(&mut self) -> PassNext {
        let realm_config_applied = self
            .result
            .targets
            .iter()
            .any(|target| matches!(target, DocumentTarget::RealmConfig { .. }));
        if self.reload_peers && realm_config_applied {
            return self.net_call(Stage::ReloadingPeers, DocumentEffect::ReloadRealmPeers);
        }
        Ok(Next::Done(std::mem::take(&mut self.result)))
    }
}

fn net_answer(result: IoResult, expected: &'static str) -> Result<DocumentNetEvent, ApplyError> {
    match event(result)? {
        Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
            Err(ApplyError::Failed(error))
        }
        Event::Net(NetEvent::DocumentSync(answer)) => Ok(answer),
        Event::Net(NetEvent::Error(error)) => Err(ApplyError::Failed(format!("{error:?}"))),
        other => Err(IoError::Unexpected {
            expected,
            got: other.label(),
        }
        .into()),
    }
}

impl Operation for ReconcileTopicsOperation {
    type Output = DocumentReconcileResult;
    type Error = ApplyError;

    fn first(&mut self) -> PassNext {
        let plan = PendingWorkPlan {
            topics: std::mem::take(&mut self.topics),
        };
        self.call(Stage::Planning, plan_call(plan))
    }

    fn next(&mut self, result: IoResult) -> PassNext {
        match std::mem::replace(&mut self.stage, Stage::Planning) {
            Stage::Planning => {
                self.work = result.child::<ReconcileWork, ApplyError>()??;
                self.next_topic()
            }
            Stage::ReadingCursor(topic) => {
                let stored_cursor = value(result)?;
                self.net_call(
                    Stage::LoadingBatch(topic),
                    DocumentEffect::ReconcileBatch {
                        topic,
                        stored_cursor,
                    },
                )
            }
            Stage::LoadingBatch(topic) => {
                match net_answer(result, "DocumentNetEvent::ReconcileBatch")? {
                    DocumentNetEvent::ReconcileBatch(batch) => {
                        self.on_batch(topic, batch.map(|batch| *batch))
                    }
                    other => Err(ApplyError::Failed(format!(
                        "unexpected reconcile batch answer: {other:?}"
                    ))),
                }
            }
            Stage::Applying(cursor) => {
                let outcome = result.child::<BatchOutcome, ApplyError>()??;
                if outcome.overflowed {
                    let read = read_row(APPLIED_OPS_KEYSPACE, overflow_key(cursor.topic_id));
                    return self.call(Stage::CheckingOverflow(cursor, Box::new(outcome)), read);
                }
                self.settle(cursor, outcome)
            }
            Stage::CheckingOverflow(cursor, outcome) => {
                // An earlier checkpoint is kept.
                if value(result)?.is_some() {
                    return self.settle(cursor, *outcome);
                }
                let row = (
                    APPLIED_OPS_KEYSPACE.to_string(),
                    overflow_key(cursor.topic_id),
                    cursor.start.clone(),
                );
                self.call(
                    Stage::RecordingOverflow(cursor, outcome),
                    IoCall::Write(vec![row]),
                )
            }
            Stage::RecordingOverflow(cursor, outcome) => {
                done(result)?;
                self.settle(cursor, *outcome)
            }
            Stage::WritingCursor(topic, satisfied) => {
                done(result)?;
                self.topic_done(topic, satisfied)
            }
            Stage::WritingEvidence(topic, satisfied) => {
                // Fail closed: without room for the evidence the events redeliver.
                if !result.child::<bool, ApplyError>()?? {
                    return self.next_topic();
                }
                self.topic_done(topic, satisfied)
            }
            Stage::SavingDeferred => {
                done(result)?;
                self.apply_creates()
            }
            Stage::ApplyingCreates => {
                let outcome = result.child::<CreateBatchOutcome, ApplyError>()??;
                self.on_creates(outcome)
            }
            Stage::SavingUpdated => {
                done(result)?;
                self.retry_parked()
            }
            Stage::RetryingParked => {
                result.child::<(), ApplyError>()??;
                let replay = ReplayOverflowOperation::new(self.realm_id);
                self.call(Stage::ReplayingOverflow, IoCall::child(replay))
            }
            Stage::ReplayingOverflow => {
                result.child::<(), ApplyError>()??;
                self.finish()
            }
            Stage::ReloadingPeers => {
                net_answer(result, "DocumentNetEvent::RealmPeersReloaded")?;
                Ok(Next::Done(std::mem::take(&mut self.result)))
            }
        }
    }

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::DocumentReconcile)
    }
}

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;
