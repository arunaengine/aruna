//! Serves inbound notification streams and validates each record before storing it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::time::unix_timestamp_millis;
use aruna_net::NetHandle;
use aruna_core::alpn::Alpn;
use aruna_core::structs::execution::notification_transport::NotificationTransportMessage;
use aruna_net::notification::{
    close_stream, drain_request, read_frame, write_frame, write_message,
};
use aruna_net::streams::BiStream;
use byteview::ByteView;
use tokio::time::{Instant, timeout_at};
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive};
use crate::notifications::checks::{message_realm, validate_watch_events, verify_recipient_holder};
use crate::triggers::{PeerInput, peer_operation};
use crate::notifications::dispatch::{list_on_holder, mark_on_holder, unread_on_holder};
use crate::notifications::mark_read::MARK_MAX_IDS;

use crate::notifications::watch::authorization::{
    WatchAuthorization, authorize_forwarded_watch, list_authorized_subscriptions,
};
use crate::notifications::watch::expand::expand_watch_events;
use crate::notifications::watch::interest::{mark_interest_dirty, schedule_interest_publish};
use crate::notifications::watch::subscriptions::{
    WATCH_SUBSCRIPTION_UNAUTHORIZED, WATCH_SUBSCRIPTION_UNAVAILABLE, create_holder_watch,
    delete_holder_watch,
};

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
    let frame = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read notification message");
            return;
        }
    };
    if let Err(error) = drain_request(&mut stream).await {
        warn!(peer = %peer, error = %error, "Failed to drain notification request stream");
        close_stream(&mut stream).await;
        return;
    }
    let input = PeerInput {
        alpn: Alpn::Notification,
        peer,
        local_node: net_handle.node_id(),
        local_realm: *net_handle.realm_id(),
        now_ms: unix_timestamp_millis(),
        frame: &frame,
    };
    let written = match peer_operation(input) {
        Some(operation) => match drive(operation, context).await {
            Ok(answer) => write_frame(&mut stream, &answer).await,
            Err(error) => Err(error.to_string()),
        },
        None => match postcard::from_bytes::<NotificationTransportMessage>(&frame) {
            Ok(message) => {
                debug!(peer = %peer, message = message.label(), "Received notification message");
                let response = build_response(context, net_handle, peer, message).await;
                write_message(&mut stream, &response).await
            }
            Err(error) => Err(error.to_string()),
        },
    };
    if let Err(error) = written {
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
        // The trigger table answers a delivery.
        NotificationTransportMessage::DeliverBatch { .. }
        | NotificationTransportMessage::DeliverAck { .. }
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

// Best-effort per-recipient wake so a holder's live streams refetch the unread
// count after inbox writes. A send with no subscribers is a no-op.
fn wake_recipients(net_handle: &NetHandle, recipients: &[UserId]) {
    for recipient in recipients {
        net_handle.notify_inbox_activity(*recipient);
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
