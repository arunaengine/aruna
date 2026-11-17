//! Checks the notification records and watch events a peer delivers before any is stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::execution::notification::NotificationTransportMessage;
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, parse_watch_path, watch_resource_path,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use crate::notifications::outbox::OUTBOX_BATCH_SIZE;
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::protocol::EVENT_BATCH_SIZE;

pub(crate) const MAX_FUTURE_SKEW: u64 = 5 * 60 * 1000;

/// A restart or recovery notice speaks for the node that holds the bucket, so only that node
/// may deliver it.
pub(crate) fn verify_node_origin(
    records: &[NotificationRecord],
    peer: NodeId,
) -> Result<(), String> {
    let foreign = records.iter().any(|record| match &record.kind {
        NotificationKind::BucketRestartLocked { node_id, .. }
        | NotificationKind::BucketRecoveryDegraded { node_id, .. }
        | NotificationKind::BucketKeyPending { node_id, .. }
        | NotificationKind::ShareUpload { node_id, .. }
        | NotificationKind::BucketDeleted { node_id, .. } => *node_id != peer,
        _ => false,
    });
    match foreign {
        true => Err("restart lock notification from another node".to_string()),
        false => Ok(()),
    }
}

pub(crate) fn validate_inbound_batch(
    records: &[NotificationRecord],
    now_ms: u64,
) -> Result<(), String> {
    if records.len() > OUTBOX_BATCH_SIZE {
        return Err(format!(
            "notification batch count {} exceeds cap {}",
            records.len(),
            OUTBOX_BATCH_SIZE
        ));
    }
    for record in records {
        validate_inbound_record(record, now_ms)?;
    }
    Ok(())
}

pub(crate) fn validate_watch_events(
    events: &[WatchEvent],
    realm_id: RealmId,
    now_ms: u64,
) -> Result<(), String> {
    if events.len() > EVENT_BATCH_SIZE {
        return Err(format!(
            "watch event batch count {} exceeds cap {}",
            events.len(),
            EVENT_BATCH_SIZE
        ));
    }
    for event in events {
        validate_watch_event(event, realm_id, now_ms)?;
    }
    Ok(())
}

pub(crate) fn validate_watch_event(
    event: &WatchEvent,
    realm_id: RealmId,
    now_ms: u64,
) -> Result<(), String> {
    if event.realm_id != realm_id {
        return Err("watch event realm mismatch".to_string());
    }
    if event.event_id.is_nil() {
        return Err("watch event has empty event_id".to_string());
    }
    if event.occurred_at_ms > now_ms.saturating_add(MAX_FUTURE_SKEW) {
        return Err(format!(
            "watch event occurred_at_ms {} is too far in the future",
            event.occurred_at_ms
        ));
    }
    // A federated user of another realm may act on this realm's resources.
    if event.actor.is_nil() {
        return Err("watch event has empty actor".to_string());
    }
    if event.path.is_empty() {
        return Err("watch event has empty path".to_string());
    }

    match (&event.kind, &event.detail) {
        (
            WatchEventKind::MetadataCreated,
            WatchEventDetail::MetadataCreated {
                group_id,
                document_id,
            },
        ) => {
            if group_id.is_nil() {
                return Err("watch event has empty group_id".to_string());
            }
            if document_id.is_nil() {
                return Err("watch event has empty document_id".to_string());
            }
            let Some(document_path) = event.path.strip_prefix(&format!("meta/{group_id}/")) else {
                return Err("watch event metadata path does not match detail".to_string());
            };
            if document_path.is_empty()
                || MetadataRegistryRecord::normalize_document_path(document_path) != document_path
            {
                return Err("watch event metadata path is not canonical".to_string());
            }
        }
        (
            WatchEventKind::DataUploaded,
            WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket,
                key,
                ..
            },
        ) => {
            if group_id.is_nil() {
                return Err("watch event has empty group_id".to_string());
            }
            if bucket.is_empty() {
                return Err("watch event has empty bucket".to_string());
            }
            if key.is_empty() {
                return Err("watch event has empty key".to_string());
            }
            let expected_path = watch_resource_path(*group_id, *node_id, bucket, key);
            if event.path != expected_path {
                return Err("watch event data path does not match detail".to_string());
            }
        }
        (
            WatchEventKind::SyncCompleted,
            WatchEventDetail::SyncCompleted {
                group_id,
                node_id,
                bucket,
                relationship_id,
                ..
            },
        )
        | (
            WatchEventKind::SyncFailed,
            WatchEventDetail::SyncFailed {
                group_id,
                node_id,
                bucket,
                relationship_id,
                ..
            },
        ) => {
            validate_sync_detail(event, *group_id, *node_id, bucket, *relationship_id)?;
            if let WatchEventDetail::SyncFailed { error, .. } = &event.detail
                && error.is_empty()
            {
                return Err("watch event has empty sync error".to_string());
            }
        }
        _ => return Err("watch event kind does not match detail".to_string()),
    }

    Ok(())
}

pub(crate) fn validate_inbound_record(
    record: &NotificationRecord,
    now_ms: u64,
) -> Result<(), String> {
    if record.watch_authorization.is_some()
        || matches!(
            record.kind,
            NotificationKind::MetadataCreated { .. }
                | NotificationKind::DataUploaded { .. }
                | NotificationKind::SyncCompleted { .. }
                | NotificationKind::SyncFailed { .. }
        )
    {
        return Err("resource watch records must use watch event delivery".to_string());
    }
    if record.read_at_ms.is_some() {
        return Err("delivered notification records must be unread".to_string());
    }
    if record.created_at_ms > now_ms.saturating_add(MAX_FUTURE_SKEW) {
        return Err(format!(
            "notification created_at_ms {} is too far in the future",
            record.created_at_ms
        ));
    }
    if record.notification_id.is_nil() {
        return Err("notification record has empty notification_id".to_string());
    }
    if record.recipient.is_nil() {
        return Err("notification record has empty recipient".to_string());
    }
    validate_inbound_kind(&record.kind, record.recipient.realm_id)
}

pub(crate) fn validate_inbound_kind(
    kind: &NotificationKind,
    recipient_realm: RealmId,
) -> Result<(), String> {
    match kind {
        NotificationKind::GroupJoinRequested {
            group_id,
            request_id,
            actor_user_id,
        } => {
            if group_id.is_nil() || request_id.is_nil() {
                return Err("join notification has empty group or request id".into());
            }
            validate_group_user("actor_user_id", actor_user_id)?;
        }
        NotificationKind::AddedToGroup {
            group_id,
            actor_user_id,
        }
        | NotificationKind::RemovedFromGroup {
            group_id,
            actor_user_id,
        } => {
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            validate_group_user("actor_user_id", actor_user_id)?;
        }
        NotificationKind::GroupMemberAdded {
            group_id,
            member_user_id,
            actor_user_id,
            ..
        } => {
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            validate_group_user("member_user_id", member_user_id)?;
            validate_group_user("actor_user_id", actor_user_id)?;
        }
        NotificationKind::NodeOnboarded { realm_id, .. } => {
            if *realm_id != recipient_realm {
                return Err(
                    "node onboarding notification realm must match recipient realm".to_string(),
                );
            }
        }
        NotificationKind::MetadataCreated {
            path,
            group_id,
            document_id,
            actor_user_id,
        } => {
            if path.is_empty() {
                return Err("notification record has empty path".to_string());
            }
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            if document_id.is_nil() {
                return Err("notification record has empty document_id".to_string());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
        NotificationKind::DataUploaded {
            path,
            group_id,
            node_id,
            bucket,
            key,
            actor_user_id,
            ..
        } => {
            if group_id.is_nil() {
                return Err("notification record has empty group_id".to_string());
            }
            if bucket.is_empty() {
                return Err("notification record has empty bucket".to_string());
            }
            if key.is_empty() {
                return Err("notification record has empty key".to_string());
            }
            if path != &watch_resource_path(*group_id, *node_id, bucket, key) {
                return Err("notification record data path does not match detail".to_string());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
        NotificationKind::SyncCompleted {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            actor_user_id,
            ..
        }
        | NotificationKind::SyncFailed {
            path,
            group_id,
            node_id,
            bucket,
            relationship_id,
            actor_user_id,
            ..
        } => {
            validate_sync_path(path, *group_id, *node_id, bucket, *relationship_id)?;
            if let NotificationKind::SyncFailed { error, .. } = kind
                && error.is_empty()
            {
                return Err("notification record has empty sync error".to_string());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
        NotificationKind::BucketRestartLocked {
            bucket, group_id, ..
        }
        | NotificationKind::BucketRecoveryDegraded {
            bucket, group_id, ..
        }
        | NotificationKind::BucketDeleted {
            bucket, group_id, ..
        } => {
            if bucket.is_empty() || group_id.is_nil() {
                return Err("bucket key notification has empty bucket or group".to_string());
            }
        }
        NotificationKind::BucketKeyPending {
            bucket,
            group_id,
            member_user_id,
            ..
        } => {
            if bucket.is_empty() || group_id.is_nil() {
                return Err("bucket key notification has empty bucket or group".to_string());
            }
            // The waiting member may be a user of another realm; the recipient stays local.
            if member_user_id.is_nil() {
                return Err("notification record has empty member_user_id".to_string());
            }
        }
        NotificationKind::ShareUpload {
            bucket,
            group_id,
            key,
            ..
        } => {
            if bucket.is_empty() || group_id.is_nil() || key.is_empty() {
                return Err("share upload notification has empty bucket, group or key".to_string());
            }
        }
        NotificationKind::SecretUseWidened {
            name,
            group_id,
            node_id,
            actor_user_id,
        } => {
            let named = aruna_core::secrets::SecretName::try_from(name.clone()).is_ok();
            if !named
                || group_id.is_some_and(|id| id.is_nil())
                || (group_id.is_some() && node_id.is_some())
            {
                return Err("secret notice has an invalid name or scope".to_string());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_sync_detail(
    event: &WatchEvent,
    group_id: ulid::Ulid,
    node_id: NodeId,
    bucket: &str,
    relationship_id: ulid::Ulid,
) -> Result<(), String> {
    validate_sync_path(&event.path, group_id, node_id, bucket, relationship_id)
        .map_err(|error| error.replace("notification record", "watch event"))
}

pub(crate) fn validate_sync_path(
    path: &str,
    group_id: ulid::Ulid,
    node_id: NodeId,
    bucket: &str,
    relationship_id: ulid::Ulid,
) -> Result<(), String> {
    if group_id.is_nil() {
        return Err("notification record has empty group_id".to_string());
    }
    if bucket.is_empty() {
        return Err("notification record has empty bucket".to_string());
    }
    if relationship_id.is_nil() {
        return Err("notification record has empty relationship_id".to_string());
    }
    let resource = parse_watch_path(path)
        .ok_or_else(|| "notification record sync path is not canonical".to_string())?;
    if resource.group_id != group_id || resource.node_id != node_id || resource.bucket != bucket {
        return Err("notification record sync path does not match detail".to_string());
    }
    Ok(())
}

/// Group records may name a federated user of another realm as actor or member.
pub(crate) fn validate_group_user(field: &str, user_id: &UserId) -> Result<(), String> {
    if user_id.is_nil() {
        return Err(format!("notification record has empty {field}"));
    }
    Ok(())
}

pub(crate) fn validate_kind_user(
    field: &str,
    user_id: &UserId,
    recipient_realm: RealmId,
) -> Result<(), String> {
    if user_id.is_nil() {
        return Err(format!("notification record has empty {field}"));
    }
    if user_id.realm_id != recipient_realm {
        return Err(format!(
            "notification record {field} realm must match recipient realm"
        ));
    }
    Ok(())
}

pub(crate) fn verify_batch_holder(
    records: &[NotificationRecord],
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    for record in records {
        verify_recipient_holder(&record.recipient, realm_config, local_node_id)?;
    }
    Ok(())
}

pub(crate) fn verify_recipient_holder(
    recipient: &UserId,
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    match resolve_inbox_holder(recipient, realm_config).map_err(|error| error.to_string())? {
        Some(holder) if holder == local_node_id => Ok(()),
        Some(holder) => Err(format!(
            "notification inbox recipient `{recipient}` is held by `{holder}`, not local node `{local_node_id}`"
        )),
        None => Err(format!(
            "no eligible notification inbox holder for recipient `{recipient}`"
        )),
    }
}

/// The one recipient realm of a delivered batch.
pub(crate) fn batch_realm(records: &[NotificationRecord]) -> Result<RealmId, String> {
    let Some(first) = records.first() else {
        return Err("empty batch".to_string());
    };
    let realm_id = first.recipient.realm_id;
    if records
        .iter()
        .any(|record| record.recipient.realm_id != realm_id)
    {
        return Err("mixed-realm batch".to_string());
    }
    Ok(realm_id)
}

// The recipient realm is peer-asserted; an empty DeliverBatch has no record to
// derive it from and must be rejected before any indexing.
pub(crate) fn message_realm(message: &NotificationTransportMessage) -> Result<RealmId, String> {
    match message {
        NotificationTransportMessage::DeliverBatch { records } => batch_realm(records),
        NotificationTransportMessage::List { recipient, .. }
        | NotificationTransportMessage::UnreadCount { recipient }
        | NotificationTransportMessage::MarkRead { recipient, .. } => Ok(recipient.realm_id),
        NotificationTransportMessage::CreateWatch { owner, .. }
        | NotificationTransportMessage::DeleteWatch { owner, .. }
        | NotificationTransportMessage::ListWatches { owner } => Ok(owner.realm_id),
        NotificationTransportMessage::DeliverWatchEvents { events } => {
            let Some(first) = events.first() else {
                return Err("empty batch".to_string());
            };
            if events.len() > EVENT_BATCH_SIZE {
                return Err(format!(
                    "watch event batch count {} exceeds cap {}",
                    events.len(),
                    EVENT_BATCH_SIZE
                ));
            }
            let realm_id = first.realm_id;
            if events.iter().any(|event| event.realm_id != realm_id) {
                return Err("mixed-realm batch".to_string());
            }
            Ok(realm_id)
        }
        NotificationTransportMessage::DeliverAck { .. }
        | NotificationTransportMessage::ListResult { .. }
        | NotificationTransportMessage::UnreadCountResult { .. }
        | NotificationTransportMessage::MarkReadResult { .. }
        | NotificationTransportMessage::Reject(_)
        | NotificationTransportMessage::WatchCreated { .. }
        | NotificationTransportMessage::WatchDeleted
        | NotificationTransportMessage::WatchList { .. }
        | NotificationTransportMessage::WatchEventsAck { .. } => {
            Err("unexpected notification control message".to_string())
        }
    }
}

/// The stored realm config when `peer` may speak for the realm: only sync-eligible
/// (server-class) nodes, stricter than the metadata `has_node` gate.
pub(crate) fn peer_config(
    peer: NodeId,
    realm_id: RealmId,
    value: Option<&[u8]>,
) -> Result<RealmConfigDocument, String> {
    let bytes = value.ok_or_else(|| format!("realm `{realm_id}` config unavailable"))?;
    let config = RealmConfigDocument::from_bytes(bytes).map_err(|error| error.to_string())?;
    let eligible = config
        .sync_eligible_nodes()
        .map_err(|error| error.to_string())?;
    if !eligible.contains(&peer) {
        return Err(format!(
            "notification peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
        ));
    }
    Ok(config)
}
