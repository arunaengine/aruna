//! Applies shared topic events: watch subscriptions, notification settings, watch interest,
//! usage and node rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentEvent, DocumentTarget};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::register::{Admission, admit_write};
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::structs::execution::setting::SettingChannel;
use tracing::warn;

use crate::document_sync::{DocumentSyncService, SyncRejection, node_to_peer};
use crate::error::{NetError, Result};

use super::validate::{
    validate_node_upsert, validate_register_upsert, validate_usage_upsert, validate_watch_delete,
    validate_watch_interest, validate_watch_upsert,
};

pub(super) enum SharedOutcome {
    Applied(DocumentTarget),
    Skipped,
    Rejected(SyncRejection),
}

pub(super) async fn apply_watch_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentEvent::Upsert {
            event_id,
            target: target @ DocumentTarget::WatchSubscription { owner, watch_id },
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
                    DocumentEvent::Upsert {
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
                    DocumentEvent::Upsert {
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
        DocumentEvent::Delete {
            event_id,
            target: target @ DocumentTarget::WatchSubscription { owner, watch_id },
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
                    DocumentEvent::Delete {
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
                    DocumentEvent::Delete {
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
        DocumentEvent::Upsert {
            event_id,
            target,
            bytes,
            change,
        } if register_target(&target) => {
            let expected_actor =
                ::irokle::actor_id_for(topic_id, node_to_peer(&change.current.actor));
            let mut reason = match actor_id == expected_actor {
                true => validate_register_upsert(&target, &bytes, &change)
                    .err()
                    .map(|reason| format!("invalid notification setting: {reason}")),
                false => {
                    Some("notification setting revision actor is not its publisher".to_string())
                }
            };
            // Register writes are immutable: a replay changes nothing, other bytes are refused.
            let mut replay = false;
            if reason.is_none() {
                let stored = stored_row(service, &target).await?;
                match admit_write(stored.as_deref(), &bytes) {
                    Admission::Store => {}
                    Admission::Replay => replay = true,
                    Admission::Conflict => {
                        reason = Some("notification setting conflicts with a stored write".into())
                    }
                }
            }
            if let Some(reason) = reason {
                warn!(%topic_id, %reason, "Rejecting a notification setting");
                let event = DocumentEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                };
                return Ok(SharedOutcome::Rejected(SyncRejection::new(
                    identity, event, reason,
                )));
            }
            if !replay
                && service
                    .apply_watch_change(target.clone(), Some(bytes), change)
                    .await?
            {
                Ok(SharedOutcome::Applied(target))
            } else {
                Ok(SharedOutcome::Skipped)
            }
        }
        // Settings are immutable register writes; a newer write turns a setting off.
        event @ DocumentEvent::Delete { .. } if register_target(event.target()) => {
            warn!(%topic_id, "Rejecting a notification setting delete");
            Ok(SharedOutcome::Rejected(SyncRejection::new(
                identity,
                event,
                "notification settings are never deleted",
            )))
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
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    if matches!(event.target(), DocumentTarget::NodeUsage { .. }) {
        return apply_usage_event(service, topic_id, actor_id, identity, event).await;
    }
    if matches!(event.target(), DocumentTarget::WatchInterest { .. }) {
        return apply_interest_event(service, topic_id, actor_id, identity, event).await;
    }
    if matches!(event.target(), DocumentTarget::NodeInfo { .. }) {
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
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentEvent::Upsert {
            event_id,
            target:
                target @ DocumentTarget::NodeUsage {
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
                    DocumentEvent::Upsert {
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
                    DocumentEvent::Upsert {
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
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentEvent::Upsert {
            event_id,
            target:
                target @ DocumentTarget::WatchInterest {
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
                    DocumentEvent::Upsert {
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
                    DocumentEvent::Upsert {
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
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    match event {
        DocumentEvent::Upsert {
            event_id,
            target:
                target @ DocumentTarget::NodeInfo {
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
                    DocumentEvent::Upsert {
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
                    DocumentEvent::Upsert {
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

/// Notification settings and owner secrets: immutable register writes.
fn register_target(target: &DocumentTarget) -> bool {
    SettingChannel::of_target(target).is_some()
        || matches!(target, DocumentTarget::OwnerSecret { .. })
}

/// The stored bytes of a register write, if this node holds the target.
async fn stored_row(
    service: &DocumentSyncService,
    target: &DocumentTarget,
) -> Result<Option<Vec<u8>>> {
    let read = StorageEffect::Read {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        txn_id: None,
    };
    match service.storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|row| row.to_vec())),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(NetError::Dht(format!("unexpected setting read: {other:?}"))),
    }
}

fn reject_shared_event(
    topic_id: ::irokle::TopicId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
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
