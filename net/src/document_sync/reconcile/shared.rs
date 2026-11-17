//! Applies shared topic events: watch subscriptions, notification settings, watch interest,
//! usage and node rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentEvent, DocumentTarget};
use aruna_core::register::{Admission, admit_write};
use aruna_core::structs::SyncQuarantineIdentity;
use aruna_core::structs::execution::setting::SettingChannel;
use tracing::warn;

use crate::document_sync::storage::storage_read_from;
use crate::document_sync::{DocumentSyncService, SyncRejection};
use crate::error::{NetError, Result};

use aruna_core::sync_checks::{
    published_by, shared_verdict, validate_register_upsert, validate_watch_delete,
    validate_watch_upsert,
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
            if !published_by(topic_id, actor_id, &change.current.actor) {
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
            if !published_by(topic_id, actor_id, &change.current.actor) {
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
            let mut reason = match published_by(topic_id, actor_id, &change.current.actor) {
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

/// Stores a usage snapshot, watch interest digest or node info document that its own node
/// published; anything else on the shared realm topic is quarantined.
pub(super) async fn apply_shared_event(
    service: &DocumentSyncService,
    topic_id: ::irokle::TopicId,
    actor_id: ::irokle::ActorId,
    identity: SyncQuarantineIdentity,
    event: DocumentEvent,
) -> Result<SharedOutcome> {
    if !matches!(
        event.target(),
        DocumentTarget::NodeUsage { .. }
            | DocumentTarget::WatchInterest { .. }
            | DocumentTarget::NodeInfo { .. }
    ) {
        return Err(NetError::Bootstrap(
            "shared handler received a non-shared event".to_string(),
        ));
    }
    if let Err(reason) = shared_verdict(topic_id, actor_id, &event) {
        warn!(%topic_id, target = ?event.target(), %reason, "Rejecting a shared realm document");
        return Ok(SharedOutcome::Rejected(SyncRejection::new(
            identity, event, reason,
        )));
    }
    let DocumentEvent::Upsert {
        target,
        bytes,
        change,
        ..
    } = event
    else {
        return Err(NetError::Bootstrap(
            "shared realm document verdict admitted a non-upsert event".to_string(),
        ));
    };
    service.apply_upsert(target.clone(), bytes, change).await?;
    Ok(SharedOutcome::Applied(target))
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
    let key_space = target.storage_keyspace().to_string();
    let row = storage_read_from(&service.storage, key_space, target.storage_key()).await?;
    Ok(row.map(|row| row.to_vec()))
}
