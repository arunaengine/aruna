use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::execution::notification::{NotificationKind, NotificationRecord};
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, parse_watch_path, watch_resource_path,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::time::unix_timestamp_millis;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use byteview::ByteView;
use tokio::time::{Instant, timeout_at};
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::notifications::client::{
    close_stream, drain_request_stream, read_message, write_message,
};
use crate::notifications::dispatch::{list_on_holder, mark_on_holder, unread_on_holder};
use crate::notifications::inbox::upsert_with_report;
use crate::notifications::mark_read::MARK_MAX_IDS;
use crate::notifications::outbox::OUTBOX_BATCH_SIZE;
use crate::notifications::placement::resolve_inbox_holder;
use crate::notifications::protocol::{
    EVENT_BATCH_SIZE, NotificationTransportMessage, notification_message_kind,
};
use crate::notifications::watch::authorization::{
    WatchAuthorization, authorize_forwarded_watch, list_authorized_subscriptions,
};
use crate::notifications::watch::expand::expand_watch_events;
use crate::notifications::watch::interest::{mark_interest_dirty, schedule_interest_publish};
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_UNAUTHORIZED, WATCH_SUBSCRIPTION_UNAVAILABLE, create_holder_watch,
    delete_holder_watch,
};

const MAX_FUTURE_SKEW: u64 = 5 * 60 * 1000;
const NOTIFICATION_AUTH_TIMEOUT: Duration = Duration::from_secs(30);

#[tracing::instrument(
    name = "notifications.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub async fn handle_notification_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(peer = %peer, "Dropping inbound notification stream without net handle");
        return;
    };

    let message = match read_message(&mut stream).await {
        Ok(message) => message,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read notification message");
            return;
        }
    };
    debug!(peer = %peer, message = notification_message_kind(&message), "Received notification message");

    if let Err(error) = drain_request_stream(&mut stream).await {
        warn!(peer = %peer, error = %error, "Failed to drain notification request stream");
        close_stream(&mut stream).await;
        return;
    }
    let response = build_response(context, net_handle, peer, message).await;

    if let Err(error) = write_message(&mut stream, &response).await {
        warn!(peer = %peer, error = %error, "Failed to write notification response");
    }
    close_stream(&mut stream).await;
}

async fn build_response(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    message: NotificationTransportMessage,
) -> NotificationTransportMessage {
    let realm_id = match message_realm(&message) {
        Ok(realm_id) => realm_id,
        Err(reason) => return NotificationTransportMessage::Reject(reason),
    };
    let realm_config = match authorize_peer(context, net_handle, peer, realm_id).await {
        Ok(config) => config,
        Err(reason) => return NotificationTransportMessage::Reject(reason),
    };
    let local_node_id = net_handle.node_id();

    match message {
        NotificationTransportMessage::DeliverBatch { records } => {
            if let Err(reason) = validate_inbound_batch(&records, unix_timestamp_millis()) {
                return NotificationTransportMessage::Reject(reason);
            }
            if let Err(reason) = verify_batch_holder(&records, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }
            match upsert_with_report(&context.storage_handle, &records).await {
                Ok(outcome) => {
                    wake_recipients(net_handle, &outcome.recipients);
                    NotificationTransportMessage::DeliverAck {
                        written: outcome.written as u32,
                    }
                }
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::List {
            recipient,
            cursor,
            limit,
        } => {
            if let Err(reason) = verify_recipient_holder(&recipient, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }
            match list_on_holder(context, recipient, cursor, limit as usize).await {
                Ok((records, next_cursor)) => NotificationTransportMessage::ListResult {
                    records,
                    next_cursor,
                },
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::UnreadCount { recipient } => {
            if let Err(reason) = verify_recipient_holder(&recipient, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }
            match unread_on_holder(context, recipient).await {
                Ok((count, capped)) => {
                    NotificationTransportMessage::UnreadCountResult { count, capped }
                }
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::MarkRead {
            recipient,
            ids,
            up_to_ms,
        } => {
            if ids.len() > MARK_MAX_IDS {
                return NotificationTransportMessage::Reject(format!(
                    "mark read id count {} exceeds cap {MARK_MAX_IDS}",
                    ids.len()
                ));
            }
            if let Err(reason) = verify_recipient_holder(&recipient, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }
            match mark_on_holder(context, recipient, ids, up_to_ms).await {
                Ok(marked) => {
                    if marked > 0 {
                        net_handle.notify_inbox_activity(recipient);
                    }
                    NotificationTransportMessage::MarkReadResult { marked }
                }
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::CreateWatch {
            owner,
            path_prefix,
            event_mask,
            authorization,
        } => {
            if let Err(reason) = verify_recipient_holder(&owner, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }

            match authorize_forwarded_watch(
                context,
                realm_id,
                owner,
                &path_prefix,
                event_mask,
                &authorization,
            )
            .await
            {
                Ok(WatchAuthorization::Authorized) => {}
                Ok(WatchAuthorization::Denied(reason)) => {
                    return NotificationTransportMessage::Reject(format!(
                        "{WATCH_SUBSCRIPTION_UNAUTHORIZED}: {}",
                        reason.metric_reason().as_str()
                    ));
                }
                Ok(WatchAuthorization::Unavailable(_)) => {
                    return NotificationTransportMessage::Reject(
                        WATCH_SUBSCRIPTION_UNAVAILABLE.to_string(),
                    );
                }
                Err(error) => return NotificationTransportMessage::Reject(error),
            }

            match create_holder_watch(
                context,
                local_node_id,
                owner,
                path_prefix,
                event_mask,
                authorization,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(subscription) => {
                    schedule_interest_publish(context).await;
                    NotificationTransportMessage::WatchCreated { subscription }
                }
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::DeleteWatch { owner, watch_id } => {
            if let Err(reason) = verify_recipient_holder(&owner, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }

            match delete_holder_watch(
                context,
                local_node_id,
                owner,
                watch_id,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(()) => {
                    schedule_interest_publish(context).await;
                    NotificationTransportMessage::WatchDeleted
                }
                Err(error) => NotificationTransportMessage::Reject(error.to_string()),
            }
        }
        NotificationTransportMessage::ListWatches { owner } => {
            if let Err(reason) = verify_recipient_holder(&owner, &realm_config, local_node_id) {
                return NotificationTransportMessage::Reject(reason);
            }

            match list_authorized_subscriptions(context, owner).await {
                Ok(subscriptions) => NotificationTransportMessage::WatchList { subscriptions },
                Err(error) => NotificationTransportMessage::Reject(error),
            }
        }
        NotificationTransportMessage::DeliverWatchEvents { events } => {
            if let Err(reason) = validate_watch_events(&events, realm_id, unix_timestamp_millis()) {
                return NotificationTransportMessage::Reject(reason);
            }
            match expand_watch_events(context, realm_id, &realm_config, local_node_id, &events)
                .await
            {
                Ok((outcome, found_stale)) => {
                    if found_stale && let Err(error) = mark_interest_dirty(context, realm_id).await
                    {
                        warn!(%error, "Failed to retract dropped watch interest after delivery");
                    }
                    wake_recipients(net_handle, &outcome.recipients);
                    NotificationTransportMessage::WatchEventsAck {
                        written: outcome.written as u32,
                    }
                }
                Err(error) => NotificationTransportMessage::Reject(error),
            }
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
            NotificationTransportMessage::Reject(
                "unexpected notification control message".to_string(),
            )
        }
    }
}

fn validate_inbound_batch(records: &[NotificationRecord], now_ms: u64) -> Result<(), String> {
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

fn validate_watch_events(
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

fn validate_watch_event(event: &WatchEvent, realm_id: RealmId, now_ms: u64) -> Result<(), String> {
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
    if event.actor.is_nil() {
        return Err("watch event has empty actor".to_string());
    }
    if event.actor.realm_id != realm_id {
        return Err("watch event actor realm must match event realm".to_string());
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

fn validate_inbound_record(record: &NotificationRecord, now_ms: u64) -> Result<(), String> {
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

fn validate_inbound_kind(kind: &NotificationKind, recipient_realm: RealmId) -> Result<(), String> {
    match kind {
        NotificationKind::GroupJoinRequested {
            group_id,
            request_id,
            actor_user_id,
        } => {
            if group_id.is_nil() || request_id.is_nil() {
                return Err("join notification has empty group or request id".into());
            }
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
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
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
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
            validate_kind_user("member_user_id", member_user_id, recipient_realm)?;
            validate_kind_user("actor_user_id", actor_user_id, recipient_realm)?;
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
    }
    Ok(())
}

fn validate_sync_detail(
    event: &WatchEvent,
    group_id: ulid::Ulid,
    node_id: NodeId,
    bucket: &str,
    relationship_id: ulid::Ulid,
) -> Result<(), String> {
    validate_sync_path(&event.path, group_id, node_id, bucket, relationship_id)
        .map_err(|error| error.replace("notification record", "watch event"))
}

fn validate_sync_path(
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

fn validate_kind_user(
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

fn verify_batch_holder(
    records: &[NotificationRecord],
    realm_config: &RealmConfigDocument,
    local_node_id: NodeId,
) -> Result<(), String> {
    for record in records {
        verify_recipient_holder(&record.recipient, realm_config, local_node_id)?;
    }
    Ok(())
}

fn verify_recipient_holder(
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

// Best-effort per-recipient wake so a holder's live streams refetch the unread
// count after inbox writes. A send with no subscribers is a no-op.
fn wake_recipients(net_handle: &NetHandle, recipients: &[UserId]) {
    for recipient in recipients {
        net_handle.notify_inbox_activity(*recipient);
    }
}

// The recipient realm is peer-asserted; an empty DeliverBatch has no record to
// derive it from and must be rejected before any indexing.
fn message_realm(message: &NotificationTransportMessage) -> Result<RealmId, String> {
    match message {
        NotificationTransportMessage::DeliverBatch { records } => {
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

// Deliberately stricter than the metadata `has_node` gate: only sync-eligible
// (server-class) realm nodes are trusted to assert recipient identity.
async fn authorize_peer(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    realm_id: RealmId,
) -> Result<RealmConfigDocument, String> {
    if realm_id != *net_handle.realm_id() {
        return Err(format!(
            "notification peer `{peer}` addressed foreign realm `{realm_id}`"
        ));
    }
    let Some(config) = read_realm_config(context, realm_id).await? else {
        return Err(format!("realm `{realm_id}` config unavailable"));
    };
    let eligible = config
        .sync_eligible_nodes()
        .map_err(|error| error.to_string())?;
    if eligible.contains(&peer) {
        Ok(config)
    } else {
        Err(format!(
            "notification peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
        ))
    }
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>, String> {
    let deadline = Instant::now() + NOTIFICATION_AUTH_TIMEOUT;
    let event = timeout_at(
        deadline,
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                txn_id: None,
            }),
    )
    .await
    .map_err(|_| "timed out reading notification realm config".to_string())?;
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}

#[cfg(test)]
#[path = "incoming_tests.rs"]
mod tests;
