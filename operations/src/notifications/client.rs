//! Sends notification requests to a remote holder node and maps their answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::execution::notification::NotificationRecord;
use aruna_core::structs::execution::notification_transport::NotificationTransportMessage;
use aruna_core::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEvent, WatchEventMask, WatchSubscription,
};
use aruna_net::NetHandle;
use ulid::Ulid;

pub async fn list_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    recipient: UserId,
    cursor: Option<Vec<u8>>,
    limit: u32,
) -> Result<(Vec<NotificationRecord>, Option<Vec<u8>>), String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::List {
            recipient,
            cursor,
            limit,
        },
        )
        .await?
    {
        NotificationTransportMessage::ListResult {
            records,
            next_cursor,
        } => Ok((records, next_cursor)),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn unread_count_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    recipient: UserId,
) -> Result<(u32, bool), String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::UnreadCount { recipient },
        )
        .await?
    {
        NotificationTransportMessage::UnreadCountResult { count, capped } => Ok((count, capped)),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn mark_read_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    recipient: UserId,
    ids: Vec<Ulid>,
    up_to_ms: Option<u64>,
) -> Result<u32, String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::MarkRead {
            recipient,
            ids,
            up_to_ms,
        },
        )
        .await?
    {
        NotificationTransportMessage::MarkReadResult { marked } => Ok(marked),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn create_watch_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    owner: UserId,
    path_prefix: String,
    event_mask: WatchEventMask,
    authorization: WatchAuthorizationBinding,
) -> Result<WatchSubscription, String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::CreateWatch {
            owner,
            path_prefix,
            event_mask,
            authorization,
        },
        )
        .await?
    {
        NotificationTransportMessage::WatchCreated { subscription } => Ok(subscription),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn delete_watch_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    owner: UserId,
    watch_id: Ulid,
) -> Result<(), String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::DeleteWatch { owner, watch_id },
        )
        .await?
    {
        NotificationTransportMessage::WatchDeleted => Ok(()),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn deliver_events_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    events: Vec<WatchEvent>,
) -> Result<u32, String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::DeliverWatchEvents { events },
        )
        .await?
    {
        NotificationTransportMessage::WatchEventsAck { written } => Ok(written),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn list_watches_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    owner: UserId,
) -> Result<Vec<WatchSubscription>, String> {
    match net_handle
        .notification_request(
            holder,
            &NotificationTransportMessage::ListWatches { owner },
        )
        .await?
    {
        NotificationTransportMessage::WatchList { subscriptions } => Ok(subscriptions),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}
