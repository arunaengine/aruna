use std::collections::BTreeSet;

use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::document::{DocumentSyncEvent, DocumentSyncTarget};
use aruna_core::metadata::MetadataGraphLifecycleRecord;
use aruna_core::structs::{PlacementRef, SyncQuarantineIdentity};
use aruna_storage::StorageHandle;
use tracing::warn;

use crate::document_sync::{
    DocumentSyncDependency, DocumentSyncService, PendingMetadataCreateApply, SyncRejection,
};
use crate::error::{NetError, Result};

use super::admin::{apply_admin_operation, coalescible_config_op, flush_config_run};
use super::materialize::reduced_admin_target;
use super::metadata::{MetadataOutcome, apply_metadata_event, apply_policy_event};
use super::satisfied_dependencies;
use super::shared::{SharedOutcome, apply_shared_event, apply_watch_event};
use super::validate::{AdminEventValidation, ConfigValidationCache, validate_admin_event};

#[derive(Default)]
pub(super) struct BatchOutcome {
    pub(super) applied_targets: Vec<DocumentSyncTarget>,
    pub(super) rejections: Vec<SyncRejection>,
    pub(super) pending_creates: Vec<PendingMetadataCreateApply>,
    pub(super) graph_tombstones: Vec<MetadataGraphLifecycleRecord>,
    pub(super) cross_topic: BTreeSet<DocumentSyncDependency>,
    pub(super) satisfied: BTreeSet<DocumentSyncDependency>,
    pub(super) deferred_creates: bool,
}

struct DeferredAdmin {
    target: DocumentSyncTarget,
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
    config_run: Option<(DocumentSyncTarget, Vec<AdminDocumentEvent>)>,
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

    pub(super) fn add_target(&mut self, target: DocumentSyncTarget) {
        self.outcome.applied_targets.push(target);
    }

    pub(super) fn add_rejection(&mut self, rejection: SyncRejection) {
        self.outcome.rejections.push(rejection);
    }

    pub(super) fn add_pending(&mut self, pending: PendingMetadataCreateApply) {
        self.outcome.pending_creates.push(pending);
        self.outcome.deferred_creates = true;
    }

    pub(super) fn add_tombstone(&mut self, record: MetadataGraphLifecycleRecord) {
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

pub(super) fn is_config_candidate(event: &DocumentSyncEvent) -> bool {
    matches!(
        event,
        DocumentSyncEvent::AdminOperation { target, event, .. }
            if matches!(target, DocumentSyncTarget::RealmConfig { .. })
                && coalescible_config_op(&event.op)
    )
}

pub(super) async fn apply_batch_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
    state: &mut BatchState,
) -> Result<()> {
    if matches!(event.target(), DocumentSyncTarget::WatchSubscription { .. })
        && matches!(
            event,
            DocumentSyncEvent::Upsert { .. } | DocumentSyncEvent::Delete { .. }
        )
    {
        let outcome = apply_watch_event(service, topic_id, actor_id, identity, event).await?;
        record_shared(outcome, state);
        return Ok(());
    }
    if matches!(
        event.target(),
        DocumentSyncTarget::NodeUsage { .. }
            | DocumentSyncTarget::WatchInterest { .. }
            | DocumentSyncTarget::NodeInfo { .. }
    ) {
        let outcome = apply_shared_event(service, topic_id, actor_id, identity, event).await?;
        record_shared(outcome, state);
        return Ok(());
    }

    if matches!(
        &event,
        DocumentSyncEvent::Upsert {
            target: DocumentSyncTarget::MetadataRegistry { .. }
                | DocumentSyncTarget::MetadataCreateEvent { .. }
                | DocumentSyncTarget::MetadataDocumentLifecycle { .. }
                | DocumentSyncTarget::MetadataGraphLifecycle { .. },
            ..
        }
    ) {
        let outcome = apply_metadata_event(service, topic_id, identity, event).await?;
        record_metadata(outcome, state);
        return Ok(());
    }
    if matches!(
        event.target(),
        DocumentSyncTarget::PersistentIdMapping { .. } | DocumentSyncTarget::PlacementPolicy { .. }
    ) {
        let outcome = apply_policy_event(service, topic_id, actor_id, identity, event).await?;
        record_metadata(outcome, state);
        return Ok(());
    }

    match event {
        event @ (DocumentSyncEvent::Upsert { .. } | DocumentSyncEvent::Delete { .. })
            if reduced_admin_target(event.target()).is_some() =>
        {
            // Whole-document admin sync is unsupported, but must not wedge replay.
            warn!(
                %topic_id,
                target = ?event.target(),
                "Skipping unsupported whole-document admin sync event"
            );
            state.add_rejection(SyncRejection::new(
                identity,
                event,
                "unsupported whole-document admin sync event",
            ));
        }
        DocumentSyncEvent::AdminOperation {
            target,
            event,
            placement,
            origin_signature,
        } => {
            apply_admin_event(
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
            .await?;
        }
        event => {
            let target = event.target().clone();
            match service.apply_document_event(event.clone()).await {
                Ok(()) => state.add_target(target),
                Err(NetError::Bootstrap(reason)) => {
                    warn!(
                        %topic_id,
                        ?target,
                        %reason,
                        "Quarantining a malformed or unsupported sync event"
                    );
                    state.add_rejection(SyncRejection::new(identity, event, reason));
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

fn record_shared(outcome: SharedOutcome, state: &mut BatchState) {
    match outcome {
        SharedOutcome::Applied(target) => state.add_target(target),
        SharedOutcome::Skipped => {}
        SharedOutcome::Rejected(rejection) => state.add_rejection(rejection),
    }
}

fn record_metadata(outcome: MetadataOutcome, state: &mut BatchState) {
    match outcome {
        MetadataOutcome::Applied { target, tombstone } => {
            if let Some(tombstone) = tombstone {
                state.add_tombstone(tombstone);
            }
            state.add_target(target);
        }
        MetadataOutcome::Deferred(dependency) => state.add_dependency(dependency),
        MetadataOutcome::Pending(pending) => state.add_pending(*pending),
        MetadataOutcome::Rejected(rejection) => state.add_rejection(rejection),
        MetadataOutcome::Skipped => {}
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_admin_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    target: DocumentSyncTarget,
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
                DocumentSyncEvent::AdminOperation {
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
    if matches!(target, DocumentSyncTarget::RealmConfig { .. }) && coalescible_config_op(&event.op)
    {
        match &mut state.config_run {
            Some((run_target, events)) if *run_target == target => events.push(*event),
            run => {
                flush_config_run(&service.storage, run, &mut state.validation_cache).await?;
                *run = Some((target.clone(), vec![*event]));
            }
        }
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
                        DocumentSyncEvent::AdminOperation {
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
                DocumentSyncEvent::AdminOperation {
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
