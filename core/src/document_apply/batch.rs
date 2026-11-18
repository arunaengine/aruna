//! Applies one causal batch of a sync topic: screens each event, buffers realm config runs, and
//! retries admin events that waited for state of the same batch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use tracing::warn;
use ulid::Ulid;

use super::admin::{AdminFollowUp, AdminOperationPlan, ConfigRunPlan};
use super::create::PendingCreateApply;
use super::distribution::{DistributionPlan, Handled, RetryParkedPlan};
use super::events::{ApplyEventPlan, ApplyOutcome, family_target};
use super::reconcile::net_answer;
use super::validate::{ConfigSnapshot, ValidateAdminPlan};
use super::{ApplyError, plan_call};
use crate::admin_checks::coalescible_config_op;
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentTarget};
use crate::calls::{IoCall, IoResult, Next};
use crate::config_authority::AdminEventValidation;
use crate::deferred_topics::{DocumentSyncDependency, satisfied_dependencies};
use crate::document::{
    BatchEvent, DocumentEffect, DocumentEvent, DocumentNetEvent, DocumentTarget,
};
use crate::effects::{Effect, NetEffect};
use crate::metadata::GraphLifecycleRecord;
use crate::operation::Operation;
use crate::structs::identity::realm::RealmId;
use crate::structs::placement::record::PlacementRef;
use crate::structs::{SyncQuarantineIdentity, SyncRejection};
use crate::sync_checks::{EventScreen, screen_event};

/// What applying one batch left for the reconcile pass.
#[derive(Debug, Default, PartialEq)]
pub struct BatchOutcome {
    pub applied_targets: Vec<DocumentTarget>,
    pub rejections: Vec<SyncRejection>,
    pub pending_creates: Vec<PendingCreateApply>,
    pub graph_tombstones: Vec<GraphLifecycleRecord>,
    pub cross_topic: BTreeSet<DocumentSyncDependency>,
    pub satisfied: BTreeSet<DocumentSyncDependency>,
    pub deferred_creates: bool,
    /// A distribution snapshot could not be parked because the bound was reached.
    pub overflowed: bool,
}

/// An admin event that passed the shared checks, or waits for them to pass.
#[derive(Debug, Clone, PartialEq)]
struct AdminItem {
    target: DocumentTarget,
    event: AdminDocumentEvent,
    placement: PlacementRef,
    identity: SyncQuarantineIdentity,
    signature: iroh::Signature,
}

#[derive(Debug, PartialEq)]
struct DeferredAdmin {
    item: AdminItem,
    dependency: Option<DocumentSyncDependency>,
    reason: String,
}

impl AdminItem {
    fn rejection(self, reason: impl Into<String>) -> SyncRejection {
        SyncRejection::new(
            self.identity,
            DocumentEvent::AdminOperation {
                target: self.target,
                event: Box::new(self.event),
                placement: self.placement,
                origin_signature: self.signature,
            },
            reason.into(),
        )
    }
}

#[derive(Debug, PartialEq)]
enum Wait {
    /// The buffered run was flushed before the current event applies.
    Flushed,
    /// The buffered run of another target was flushed; this event starts the next run.
    FlushedForRun(Box<AdminItem>),
    /// The last run was flushed; the deferred admin events come next.
    FlushedLast,
    Checked(Box<AdminItem>),
    Applied,
    /// The stored Git record of an applied event is being counted.
    GitNoted,
    AdminApplied(Box<AdminItem>),
    Synced(Option<RealmId>, Box<AdminItem>),
    Parked(Box<AdminItem>),
    Distributed(Box<AdminItem>),
    RetryChecked(Box<AdminItem>),
    RetryApplied(Box<AdminItem>),
    RetrySynced(Option<RealmId>, Box<AdminItem>),
    RetryParked(Box<AdminItem>),
}

/// Applies the events of one batch in order and reports what the pass does next.
#[derive(Debug, PartialEq)]
pub struct ApplyBatchOperation {
    topic_id: irokle::TopicId,
    realm_id: RealmId,
    events: VecDeque<BatchEvent>,
    current: Option<BatchEvent>,
    outcome: BatchOutcome,
    run: Option<(DocumentTarget, Vec<AdminDocumentEvent>)>,
    snapshot: Option<Arc<ConfigSnapshot>>,
    deferred: Vec<DeferredAdmin>,
    retrying: VecDeque<DeferredAdmin>,
    still_waiting: Vec<DeferredAdmin>,
    progressed: bool,
    wait: Option<Wait>,
}

type BatchNext = Result<Next<BatchOutcome>, ApplyError>;

impl ApplyBatchOperation {
    pub fn new(
        topic_id: irokle::TopicId,
        realm_id: RealmId,
        events: Vec<BatchEvent>,
        rejections: Vec<SyncRejection>,
    ) -> Self {
        Self {
            topic_id,
            realm_id,
            events: events.into(),
            current: None,
            outcome: BatchOutcome {
                rejections,
                ..BatchOutcome::default()
            },
            run: None,
            snapshot: None,
            deferred: Vec::new(),
            retrying: VecDeque::new(),
            still_waiting: Vec::new(),
            progressed: false,
            wait: None,
        }
    }

    fn call(&mut self, wait: Wait, call: IoCall) -> BatchNext {
        self.wait = Some(wait);
        Ok(Next::Call(call))
    }

    fn flush(&mut self, wait: Wait) -> Option<BatchNext> {
        let (target, events) = self.run.take()?;
        Some(self.call(wait, plan_call(ConfigRunPlan { target, events })))
    }

    /// Moves to the next event, or to the end of the batch.
    fn advance(&mut self) -> BatchNext {
        self.current = self.events.pop_front();
        self.handle_current()
    }

    fn handle_current(&mut self) -> BatchNext {
        let Some(batch_event) = self.current.as_ref() else {
            return match self.flush(Wait::FlushedLast) {
                Some(next) => next,
                None => self.start_retry(),
            };
        };
        // Any event outside the run must observe the run's state.
        if !is_config_candidate(&batch_event.event)
            && let Some(next) = self.flush(Wait::Flushed)
        {
            return next;
        }
        let Some(BatchEvent {
            event,
            actor,
            actor_seq,
            publisher_allowed,
        }) = self.current.take()
        else {
            return self.advance();
        };
        let identity = SyncQuarantineIdentity {
            topic: self.topic_id,
            actor,
            actor_seq,
        };
        let target_topic = event
            .target()
            .sync_topic_id(self.realm_id, &event.placement());
        match screen_event(
            self.topic_id,
            target_topic,
            event.target(),
            publisher_allowed,
        ) {
            EventScreen::Apply => {}
            EventScreen::Drop => return self.advance(),
            EventScreen::Reject(reason) => {
                warn!(topic_id = %self.topic_id, actor_id = %actor, %target_topic, reason, "Rejecting a document sync event");
                self.outcome
                    .rejections
                    .push(SyncRejection::new(identity, event, reason));
                return self.advance();
            }
        }
        match event {
            DocumentEvent::AdminOperation {
                target,
                event,
                placement,
                origin_signature,
            } if !family_target(&target) => {
                let item = AdminItem {
                    target,
                    event: *event,
                    placement,
                    identity,
                    signature: origin_signature,
                };
                let check = self.check_call(&item);
                self.call(Wait::Checked(Box::new(item)), check)
            }
            event => {
                let plan = ApplyEventPlan {
                    topic_id: self.topic_id,
                    actor_id: actor,
                    realm_id: self.realm_id,
                    identity,
                    event,
                };
                self.call(Wait::Applied, plan_call(plan))
            }
        }
    }

    fn check_call(&self, item: &AdminItem) -> IoCall {
        plan_call(ValidateAdminPlan {
            topic_id: self.topic_id,
            actor_id: item.identity.actor,
            target: item.target.clone(),
            event: item.event.clone(),
            realm_id: self.realm_id,
            placement: item.placement,
            signature: item.signature,
            cache: self.snapshot.clone(),
        })
    }

    fn checked(&mut self, result: IoResult) -> Result<AdminEventValidation, ApplyError> {
        let (validation, snapshot) =
            result.child::<(AdminEventValidation, Option<Arc<ConfigSnapshot>>), ApplyError>()??;
        self.snapshot = snapshot;
        Ok(validation)
    }

    fn on_checked(&mut self, item: AdminItem, validation: AdminEventValidation) -> BatchNext {
        match validation {
            AdminEventValidation::Accepted => self.apply_admin(item),
            AdminEventValidation::Rejected(reason) => {
                warn!(topic_id = %self.topic_id, event_id = %item.event.event_id, %reason, "Rejecting invalid or unauthorized admin operation");
                self.outcome.rejections.push(item.rejection(reason));
                self.advance()
            }
            AdminEventValidation::Deferred { dependency, reason } => {
                warn!(topic_id = %self.topic_id, event_id = %item.event.event_id, %reason, "Deferring admin operation until prerequisite state is available");
                self.deferred.push(DeferredAdmin {
                    item,
                    dependency,
                    reason,
                });
                self.advance()
            }
        }
    }

    fn apply_admin(&mut self, item: AdminItem) -> BatchNext {
        if matches!(item.target, DocumentTarget::RealmConfig { .. })
            && coalescible_config_op(&item.event.op)
        {
            return match &mut self.run {
                Some((target, events)) if *target == item.target => {
                    events.push(item.event.clone());
                    self.finish_admin(&item);
                    self.advance()
                }
                _ => match self.flush(Wait::FlushedForRun(Box::new(item.clone()))) {
                    Some(next) => next,
                    None => self.start_run(item),
                },
            };
        }
        if matches!(item.event.target, AdminDocumentTarget::Distribution { .. }) {
            let plan = DistributionPlan {
                target: item.target.clone(),
                event: item.event.clone(),
            };
            return self.call(Wait::Distributed(Box::new(item)), plan_call(plan));
        }
        let plan = AdminOperationPlan {
            target: item.target.clone(),
            event: item.event.clone(),
        };
        self.call(Wait::AdminApplied(Box::new(item)), plan_call(plan))
    }

    fn start_run(&mut self, item: AdminItem) -> BatchNext {
        self.run = Some((item.target.clone(), vec![item.event.clone()]));
        self.finish_admin(&item);
        self.advance()
    }

    fn finish_admin(&mut self, item: &AdminItem) {
        self.outcome
            .satisfied
            .extend(satisfied_dependencies(&item.target, &item.event));
        self.outcome.applied_targets.push(item.target.clone());
    }

    fn admin_done(&mut self, item: AdminItem) -> BatchNext {
        self.snapshot = None;
        self.finish_admin(&item);
        self.advance()
    }

    fn follow_up(&mut self, follow: AdminFollowUp, item: AdminItem) -> BatchNext {
        if follow.sync {
            return self.call(
                Wait::Synced(follow.retry_parked, Box::new(item)),
                IoCall::Sync,
            );
        }
        self.after_sync(follow.retry_parked, item)
    }

    fn after_sync(&mut self, parked: Option<RealmId>, item: AdminItem) -> BatchNext {
        match parked {
            Some(realm_id) => self.call(
                Wait::Parked(Box::new(item)),
                plan_call(RetryParkedPlan { realm_id }),
            ),
            None => self.admin_done(item),
        }
    }

    /// Folds one event outcome in; a stored Git record names its document.
    fn on_event(&mut self, outcome: ApplyOutcome) -> Option<Ulid> {
        match outcome {
            ApplyOutcome::Applied {
                target,
                tombstone,
                git_document,
            } => {
                self.outcome
                    .graph_tombstones
                    .extend(tombstone.map(|record| *record));
                self.outcome.applied_targets.push(target);
                return git_document;
            }
            ApplyOutcome::Deferred(dependency) => {
                warn!(topic_id = %self.topic_id, ?dependency, "Deferring a document until its dependency replicates");
                self.outcome.cross_topic.insert(dependency);
            }
            ApplyOutcome::Pending(pending) => {
                self.outcome.pending_creates.push(*pending);
                self.outcome.deferred_creates = true;
            }
            ApplyOutcome::Rejected(rejection) => {
                warn!(topic_id = %self.topic_id, reason = %rejection.reason, "Rejecting a replicated document event");
                self.outcome.rejections.push(*rejection);
            }
            ApplyOutcome::Skipped => {}
        }
        None
    }

    fn note_git_record(&mut self, document_id: Ulid) -> BatchNext {
        let effect = DocumentEffect::NoteGitRecord { document_id };
        let effect = Effect::Net(NetEffect::DocumentSync(effect));
        self.call(Wait::GitNoted, IoCall::Effect(Box::new(effect)))
    }

    fn start_retry(&mut self) -> BatchNext {
        self.retrying = std::mem::take(&mut self.deferred).into();
        self.progressed = false;
        self.next_retry()
    }

    fn next_retry(&mut self) -> BatchNext {
        let Some(deferred) = self.retrying.pop_front() else {
            return self.end_retry_pass();
        };
        let check = self.check_call(&deferred.item);
        // The check gives the new reason, so only the item stays.
        self.call(Wait::RetryChecked(Box::new(deferred.item)), check)
    }

    fn end_retry_pass(&mut self) -> BatchNext {
        let waiting = std::mem::take(&mut self.still_waiting);
        if self.progressed && !waiting.is_empty() {
            self.retrying = waiting.into();
            self.progressed = false;
            return self.next_retry();
        }
        if !self.progressed {
            self.reject_unresolved(waiting);
        }
        Ok(Next::Done(std::mem::take(&mut self.outcome)))
    }

    fn reject_unresolved(&mut self, waiting: Vec<DeferredAdmin>) {
        for deferred in waiting {
            if let Some(dependency) = deferred.dependency {
                self.outcome.cross_topic.insert(dependency);
                continue;
            }
            warn!(topic_id = %self.topic_id, event_id = %deferred.item.event.event_id, reason = %deferred.reason, "Rejecting admin operation whose same-topic prerequisite is absent");
            self.outcome
                .rejections
                .push(deferred.item.rejection(deferred.reason));
        }
    }

    fn on_retry_checked(&mut self, item: AdminItem, validation: AdminEventValidation) -> BatchNext {
        match validation {
            AdminEventValidation::Accepted => {
                let plan = AdminOperationPlan {
                    target: item.target.clone(),
                    event: item.event.clone(),
                };
                self.call(Wait::RetryApplied(Box::new(item)), plan_call(plan))
            }
            AdminEventValidation::Rejected(reason) => {
                warn!(topic_id = %self.topic_id, event_id = %item.event.event_id, %reason, "Rejecting deferred admin operation after prerequisite replay");
                self.outcome.rejections.push(item.rejection(reason));
                self.next_retry()
            }
            AdminEventValidation::Deferred { dependency, reason } => {
                self.still_waiting.push(DeferredAdmin {
                    item,
                    dependency,
                    reason,
                });
                self.next_retry()
            }
        }
    }

    fn retry_done(&mut self, item: AdminItem) -> BatchNext {
        self.snapshot = None;
        self.finish_admin(&item);
        self.progressed = true;
        self.next_retry()
    }

    fn retry_sync(&mut self, parked: Option<RealmId>, item: AdminItem) -> BatchNext {
        match parked {
            Some(realm_id) => self.call(
                Wait::RetryParked(Box::new(item)),
                plan_call(RetryParkedPlan { realm_id }),
            ),
            None => self.retry_done(item),
        }
    }
}

/// Whether an event may join a buffered run of realm config events.
fn is_config_candidate(event: &DocumentEvent) -> bool {
    matches!(
        event,
        DocumentEvent::AdminOperation { target, event, .. }
            if matches!(target, DocumentTarget::RealmConfig { .. })
                && coalescible_config_op(&event.op)
    )
}

impl Operation for ApplyBatchOperation {
    type Output = BatchOutcome;
    type Error = ApplyError;

    fn first(&mut self) -> BatchNext {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> BatchNext {
        let Some(wait) = self.wait.take() else {
            return Err(crate::calls::IoError::Unfinished.into());
        };
        match wait {
            Wait::Flushed => {
                result.child::<(), ApplyError>()??;
                self.snapshot = None;
                self.handle_current()
            }
            Wait::FlushedForRun(item) => {
                result.child::<(), ApplyError>()??;
                self.snapshot = None;
                self.start_run(*item)
            }
            Wait::FlushedLast => {
                result.child::<(), ApplyError>()??;
                self.snapshot = None;
                self.start_retry()
            }
            Wait::Checked(item) => {
                let validation = self.checked(result)?;
                self.on_checked(*item, validation)
            }
            Wait::Applied => {
                let outcome = result.child::<ApplyOutcome, ApplyError>()??;
                match self.on_event(outcome) {
                    Some(document_id) => self.note_git_record(document_id),
                    None => self.advance(),
                }
            }
            Wait::GitNoted => match net_answer(result, "DocumentNetEvent::GitRecordNoted")? {
                DocumentNetEvent::GitRecordNoted => self.advance(),
                other => Err(ApplyError::Failed(format!(
                    "unexpected git record answer: {other:?}"
                ))),
            },
            Wait::AdminApplied(item) => {
                let follow = result.child::<AdminFollowUp, ApplyError>()??;
                self.follow_up(follow, *item)
            }
            Wait::Synced(parked, item) => {
                crate::rows::done(result)?;
                self.after_sync(parked, *item)
            }
            Wait::Parked(item) => {
                result.child::<(), ApplyError>()??;
                self.admin_done(*item)
            }
            Wait::Distributed(item) => {
                // The cursor still moves on; an overflow replay revisits this batch later.
                let handled = result.child::<Handled, ApplyError>()??;
                self.outcome.overflowed |= handled == Handled::Full;
                self.admin_done(*item)
            }
            Wait::RetryChecked(item) => {
                let validation = self.checked(result)?;
                self.on_retry_checked(*item, validation)
            }
            Wait::RetryApplied(item) => {
                let follow = result.child::<AdminFollowUp, ApplyError>()??;
                if follow.sync {
                    return self.call(Wait::RetrySynced(follow.retry_parked, item), IoCall::Sync);
                }
                self.retry_sync(follow.retry_parked, *item)
            }
            Wait::RetrySynced(parked, item) => {
                crate::rows::done(result)?;
                self.retry_sync(parked, *item)
            }
            Wait::RetryParked(item) => {
                result.child::<(), ApplyError>()??;
                self.retry_done(*item)
            }
        }
    }
}

#[cfg(test)]
#[path = "batch_tests.rs"]
mod tests;
