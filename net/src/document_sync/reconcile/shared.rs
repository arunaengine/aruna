use aruna_core::document::{DocumentSyncEvent, DocumentSyncTarget};
use aruna_core::structs::SyncQuarantineIdentity;
use tracing::warn;

use crate::document_sync::{DocumentSyncService, SyncRejection, node_to_peer};
use crate::error::{NetError, Result};

use super::validate::{
    validate_node_upsert, validate_usage_upsert, validate_watch_delete, validate_watch_interest,
    validate_watch_upsert,
};

pub(super) enum SharedOutcome {
    Applied(DocumentSyncTarget),
    Skipped,
    Rejected(SyncRejection),
}

pub(super) async fn apply_watch_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentSyncEvent::Upsert {
            event_id,
            target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
            bytes,
            change,
        } => {
            let expected_actor =
                ::irokle::actor_id_for(topic_id, node_to_peer(&change.current.actor));
            if actor_id != expected_actor {
                warn!(
                    %topic_id,
                    %owner,
                    %watch_id,
                    "Rejecting watch subscription whose revision actor is not its publisher"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    "watch subscription revision actor is not its publisher",
                )));
            }
            if let Err(reason) = validate_watch_upsert(&target, &bytes, &change) {
                warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription");
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    format!("invalid watch subscription: {reason}"),
                )));
            }
            if service
                .apply_watch_change(target.clone(), Some(bytes), change)
                .await?
            {
                Ok(SharedOutcome::Applied(target))
            } else {
                Ok(SharedOutcome::Skipped)
            }
        }
        DocumentSyncEvent::Delete {
            event_id,
            target: target @ DocumentSyncTarget::WatchSubscription { owner, watch_id },
            change,
        } => {
            let expected_actor =
                ::irokle::actor_id_for(topic_id, node_to_peer(&change.current.actor));
            if actor_id != expected_actor {
                warn!(
                    %topic_id,
                    %owner,
                    %watch_id,
                    "Rejecting watch subscription delete whose revision actor is not its publisher"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Delete {
                        event_id,
                        target,
                        change,
                    },
                    "watch subscription delete actor is not its publisher",
                )));
            }
            if let Err(reason) = validate_watch_delete(&target, &change) {
                warn!(%topic_id, %owner, %watch_id, %reason, "Rejecting invalid watch subscription delete");
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Delete {
                        event_id,
                        target,
                        change,
                    },
                    format!("invalid watch subscription delete: {reason}"),
                )));
            }
            if service
                .apply_watch_change(target.clone(), None, change)
                .await?
            {
                Ok(SharedOutcome::Applied(target))
            } else {
                Ok(SharedOutcome::Skipped)
            }
        }
        _ => Err(NetError::Bootstrap(
            "watch handler received a non-watch event".to_string(),
        )),
    }
}

pub(super) async fn apply_shared_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    if matches!(event.target(), DocumentSyncTarget::NodeUsage { .. }) {
        return apply_usage_event(service, topic_id, actor_id, identity, event).await;
    }
    if matches!(event.target(), DocumentSyncTarget::WatchInterest { .. }) {
        return apply_interest_event(service, topic_id, actor_id, identity, event).await;
    }
    if matches!(event.target(), DocumentSyncTarget::NodeInfo { .. }) {
        return apply_node_event(service, topic_id, actor_id, identity, event).await;
    }
    Err(NetError::Bootstrap(
        "shared handler received a non-shared event".to_string(),
    ))
}

async fn apply_usage_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentSyncEvent::Upsert {
            event_id,
            target:
                target @ DocumentSyncTarget::NodeUsage {
                    node_id: snapshot_node,
                    ..
                },
            bytes,
            change,
        } => {
            // Shared realm topic: the signed publisher must own the claimed node.
            let expected_actor = ::irokle::actor_id_for(topic_id, node_to_peer(&snapshot_node));
            if actor_id != expected_actor {
                warn!(
                    %topic_id,
                    node_id = %snapshot_node,
                    "Rejecting node usage snapshot: publisher is not the owning node"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    "node usage publisher is not the owning node",
                )));
            }
            if let Err(reason) = validate_usage_upsert(&target, &bytes) {
                warn!(
                    %topic_id,
                    node_id = %snapshot_node,
                    %reason,
                    "Rejecting invalid node usage snapshot"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    format!("invalid node usage snapshot: {reason}"),
                )));
            }
            service.apply_upsert(target.clone(), bytes, change).await?;
            Ok(SharedOutcome::Applied(target))
        }
        event => reject_shared_event(topic_id, identity, event),
    }
}

async fn apply_interest_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentSyncEvent::Upsert {
            event_id,
            target:
                target @ DocumentSyncTarget::WatchInterest {
                    realm_id,
                    node_id: interest_node,
                },
            bytes,
            change,
        } => {
            // Shared realm topic: the signed publisher must own the claimed node.
            let expected_actor = ::irokle::actor_id_for(topic_id, node_to_peer(&interest_node));
            if actor_id != expected_actor {
                warn!(
                    %topic_id,
                    realm_id = %realm_id,
                    node_id = %interest_node,
                    "Rejecting watch interest digest: publisher is not the owning node"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    "watch interest publisher is not the owning node",
                )));
            }
            if let Err(reason) = validate_watch_interest(&target, &bytes) {
                warn!(
                    %topic_id,
                    realm_id = %realm_id,
                    node_id = %interest_node,
                    %reason,
                    "Rejecting invalid watch interest digest"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    format!("invalid watch interest digest: {reason}"),
                )));
            }
            service.apply_upsert(target.clone(), bytes, change).await?;
            Ok(SharedOutcome::Applied(target))
        }
        event => reject_shared_event(topic_id, identity, event),
    }
}

async fn apply_node_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentSyncEvent::Upsert {
            event_id,
            target:
                target @ DocumentSyncTarget::NodeInfo {
                    node_id: info_node, ..
                },
            bytes,
            change,
        } => {
            // Shared realm topic: the signed publisher must own the claimed node.
            let expected_actor = ::irokle::actor_id_for(topic_id, node_to_peer(&info_node));
            if actor_id != expected_actor {
                warn!(
                    %topic_id,
                    node_id = %info_node,
                    "Rejecting node info document: publisher is not the owning node"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    "node info publisher is not the owning node",
                )));
            }
            if let Err(reason) = validate_node_upsert(&target, &bytes) {
                warn!(
                    %topic_id,
                    node_id = %info_node,
                    %reason,
                    "Rejecting invalid node info document"
                );
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity,
                    DocumentSyncEvent::Upsert {
                        event_id,
                        target,
                        bytes,
                        change,
                    },
                    format!("invalid node info document: {reason}"),
                )));
            }
            service.apply_upsert(target.clone(), bytes, change).await?;
            Ok(SharedOutcome::Applied(target))
        }
        event => {
            // Node info documents sync only as owner-validated upserts.
            warn!(
                %topic_id,
                target = ?event.target(),
                "Skipping unsupported non-upsert node info document event"
            );
            Ok(SharedOutcome::Rejected(SyncRejection::new(
                identity,
                event,
                "unsupported non-upsert node info document event",
            )))
        }
    }
}

fn reject_shared_event(
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentSyncEvent,
) -> Result<SharedOutcome> {
    // Shared realm snapshots sync only as owner-validated upserts.
    warn!(
        %topic_id,
        target = ?event.target(),
        "Skipping unsupported non-upsert shared document event"
    );
    Ok(SharedOutcome::Rejected(SyncRejection::new(
        identity,
        event,
        "unsupported non-upsert shared realm document event",
    )))
}
