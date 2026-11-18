//! Applies a batch of sync events and collects applied targets, rejections and deferrals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::admin_checks::coalescible_config_op;
use aruna_core::admin_documents::{AdminDocumentEvent, AdminDocumentTarget};
use aruna_core::document::{DocumentEvent, DocumentTarget};
use aruna_core::document_apply::create::PendingCreateApply;
use aruna_core::document_apply::distribution::Handled;
use aruna_core::document_apply::events::{ApplyEventPlan, ApplyOutcome};
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_storage::StorageHandle;
use tracing::warn;

use crate::document_sync::plan_run::retry_plan;
use crate::document_sync::{DocumentSyncDependency, DocumentSyncService, SyncRejection};
use crate::error::Result;

use super::admin::{apply_admin_operation, apply_distribution_operation, flush_config_run};
use super::satisfied_dependencies;
use super::validate::{ConfigValidationCache, validate_admin_event};
use aruna_core::config_authority::AdminEventValidation;

#[derive(Default)]
pub(super) struct BatchOutcome {
    pub(super) applied_targets: Vec<DocumentTarget>,
    pub(super) rejections: Vec<SyncRejection>,
    pub(super) pending_creates: Vec<PendingCreateApply>,
    pub(super) graph_tombstones: Vec<GraphLifecycleRecord>,
    pub(super) cross_topic: BTreeSet<DocumentSyncDependency>,
    pub(super) satisfied: BTreeSet<DocumentSyncDependency>,
    pub(super) deferred_creates: bool,
    /// A distribution snapshot could not be parked because the bound was reached.
    pub(super) overflowed: bool,
}

struct DeferredAdmin {
    target: DocumentTarget,
    event: AdminDocumentEvent,
    placement: PlacementRef,
    identity: SyncQuarantineIdentity,
    signature: iroh::Signature,
    dependency: Option<DocumentSyncDependency>,
    reason: String,
}

pub(super) struct BatchState {
    outcome: BatchOutcome,
    deferred_admin: Vec<DeferredAdmin>,
    config_run: Option<(DocumentTarget, Vec<AdminDocumentEvent>)>,
    validation_cache: ConfigValidationCache,
}

impl BatchState {
    pub(super) fn new(rejections: Vec<SyncRejection>) -> Self {
        Self {
            outcome: BatchOutcome {
                rejections,
                ..BatchOutcome::default()
            },
            deferred_admin: Vec::new(),
            config_run: None,
            validation_cache: ConfigValidationCache::default(),
        }
    }

    pub(super) fn add_target(&mut self, target: DocumentTarget) {
        self.outcome.applied_targets.push(target);
    }

    pub(super) fn add_rejection(&mut self, rejection: SyncRejection) {
        self.outcome.rejections.push(rejection);
    }

    pub(super) fn add_pending(&mut self, pending: PendingCreateApply) {
        self.outcome.pending_creates.push(pending);
        self.outcome.deferred_creates = true;
    }

    pub(super) fn add_tombstone(&mut self, record: GraphLifecycleRecord) {
        self.outcome.graph_tombstones.push(record);
    }

    pub(super) fn add_dependency(&mut self, dependency: DocumentSyncDependency) {
        self.outcome.cross_topic.insert(dependency);
    }

    pub(super) async fn flush_config(&mut self, storage: &StorageHandle) -> Result<()> {
        flush_config_run(storage, &mut self.config_run, &mut self.validation_cache).await
    }

    pub(super) fn finish(self) -> BatchOutcome {
        self.outcome
    }
}

pub(super) fn is_config_candidate(event: &DocumentEvent) -> bool {
    matches!(
        event,
        DocumentEvent::AdminOperation { target, event, .. }
            if matches!(target, DocumentTarget::RealmConfig { .. })
                && coalescible_config_op(&event.op)
    )
}

pub(super) async fn apply_batch_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
    state: &mut BatchState,
) -> Result<()> {
    if let DocumentEvent::AdminOperation {
        target,
        event,
        placement,
        origin_signature,
    } = event
    {
        return apply_admin_event(
            service,
            topic_id,
            actor_id,
            identity,
            target,
            event,
            placement,
            origin_signature,
            state,
        )
        .await;
    }
    let build = || ApplyEventPlan {
        topic_id,
        actor_id,
        realm_id: service.realm_id,
        identity,
        event: event.clone(),
    };
    let conflicted = "document apply conflict retries exhausted";
    match retry_plan(&service.storage, build, conflicted).await? {
        ApplyOutcome::Applied {
            target,
            tombstone,
            git_document,
        } => {
            if let Some(document_id) = git_document {
                aruna_core::git::record_written(document_id);
            }
            if let Some(tombstone) = tombstone {
                state.add_tombstone(tombstone);
            }
            state.add_target(target);
        }
        ApplyOutcome::Deferred(dependency) => {
            warn!(%topic_id, target = ?event.target(), ?dependency, "Deferring a document until its dependency replicates");
            state.add_dependency(dependency);
        }
        ApplyOutcome::Pending(pending) => state.add_pending(*pending),
        ApplyOutcome::Rejected(rejection) => {
            warn!(%topic_id, target = ?event.target(), reason = %rejection.reason, "Rejecting a replicated document event");
            state.add_rejection(*rejection);
        }
        ApplyOutcome::Skipped => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_admin_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    target: DocumentTarget,
    event: Box<AdminDocumentEvent>,
    placement: PlacementRef,
    origin_signature: iroh::Signature,
    state: &mut BatchState,
) -> Result<()> {
    match validate_admin_event(
        &service.storage,
        topic_id,
        actor_id,
        &target,
        &event,
        service.realm_id,
        &placement,
        &origin_signature,
        &mut state.validation_cache,
    )
    .await?
    {
        AdminEventValidation::Accepted => {}
        AdminEventValidation::Rejected(reason) => {
            warn!(
                %topic_id,
                event_id = %event.event_id,
                origin_node_id = %event.origin_node_id,
                %reason,
                "Rejecting invalid or unauthorized admin operation"
            );
            state.add_rejection(SyncRejection::new(
                identity,
                DocumentEvent::AdminOperation {
                    target,
                    event,
                    placement,
                    origin_signature,
                },
                reason,
            ));
            return Ok(());
        }
        AdminEventValidation::Deferred { dependency, reason } => {
            warn!(
                %topic_id,
                event_id = %event.event_id,
                origin_node_id = %event.origin_node_id,
                %reason,
                "Deferring admin operation until prerequisite state is available"
            );
            state.deferred_admin.push(DeferredAdmin {
                target,
                event: *event,
                placement,
                identity,
                signature: origin_signature,
                dependency,
                reason,
            });
            return Ok(());
        }
    }

    let dependencies = satisfied_dependencies(&target, event.as_ref());
    if matches!(target, DocumentTarget::RealmConfig { .. }) && coalescible_config_op(&event.op) {
        match &mut state.config_run {
            Some((run_target, events)) if *run_target == target => events.push(*event),
            run => {
                flush_config_run(&service.storage, run, &mut state.validation_cache).await?;
                *run = Some((target.clone(), vec![*event]));
            }
        }
    } else if matches!(event.target, AdminDocumentTarget::Distribution { .. }) {
        let handled =
            apply_distribution_operation(&service.storage, target.clone(), *event).await?;
        // The cursor still moves on; an overflow replay revisits this batch later.
        state.outcome.overflowed |= handled == Handled::Full;
        state.validation_cache.invalidate();
    } else {
        apply_admin_operation(&service.storage, target.clone(), *event).await?;
        state.validation_cache.invalidate();
    }
    state.outcome.satisfied.extend(dependencies);
    state.add_target(target);
    Ok(())
}

pub(super) async fn retry_admin(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    state: &mut BatchState,
) -> Result<()> {
    let mut pending = std::mem::take(&mut state.deferred_admin);
    loop {
        let mut progressed = false;
        let mut retry = Vec::new();
        for deferred in pending {
            match validate_admin_event(
                &service.storage,
                topic_id,
                deferred.identity.actor,
                &deferred.target,
                &deferred.event,
                service.realm_id,
                &deferred.placement,
                &deferred.signature,
                &mut state.validation_cache,
            )
            .await?
            {
                AdminEventValidation::Accepted => {
                    let dependencies = satisfied_dependencies(&deferred.target, &deferred.event);
                    apply_admin_operation(
                        &service.storage,
                        deferred.target.clone(),
                        deferred.event,
                    )
                    .await?;
                    state.validation_cache.invalidate();
                    state.outcome.satisfied.extend(dependencies);
                    state.add_target(deferred.target);
                    progressed = true;
                }
                AdminEventValidation::Rejected(reason) => {
                    warn!(
                        %topic_id,
                        event_id = %deferred.event.event_id,
                        %reason,
                        "Rejecting deferred admin operation after prerequisite replay"
                    );
                    state.add_rejection(SyncRejection::new(
                        deferred.identity,
                        DocumentEvent::AdminOperation {
                            target: deferred.target,
                            event: Box::new(deferred.event),
                            placement: deferred.placement,
                            origin_signature: deferred.signature,
                        },
                        reason,
                    ));
                }
                AdminEventValidation::Deferred { dependency, reason } => {
                    retry.push(DeferredAdmin {
                        dependency,
                        reason,
                        ..deferred
                    })
                }
            }
        }
        if !progressed {
            reject_unresolved(topic_id, retry, state);
            break;
        }
        pending = retry;
        if pending.is_empty() {
            break;
        }
    }
    Ok(())
}

fn reject_unresolved(
    topic_id: ::irokle::TopicId,
    retry: Vec<DeferredAdmin>,
    state: &mut BatchState,
) {
    for deferred in retry {
        if let Some(dependency) = deferred.dependency {
            state.add_dependency(dependency);
        } else {
            warn!(
                %topic_id,
                event_id = %deferred.event.event_id,
                reason = %deferred.reason,
                "Rejecting admin operation whose same-topic prerequisite is absent"
            );
            state.add_rejection(SyncRejection::new(
                deferred.identity,
                DocumentEvent::AdminOperation {
                    target: deferred.target,
                    event: Box::new(deferred.event),
                    placement: deferred.placement,
                    origin_signature: deferred.signature,
                },
                deferred.reason,
            ));
        }
    }
}
