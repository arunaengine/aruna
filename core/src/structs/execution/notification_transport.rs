//! Defines the notification transport messages two nodes exchange, one request and one answer
//! frame per stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::UserId;
use crate::id::NodeId;
use crate::structs::execution::notification::NotificationRecord;
use crate::structs::execution::notification_watch::{
    WatchAuthorizationBinding, WatchEvent, WatchEventMask, WatchSubscription,
};

/// One request to the notification holder `node`, answered by one frame.
#[derive(Debug, Clone, PartialEq)]
pub struct NotificationRequest {
    pub node: NodeId,
    pub message: NotificationTransportMessage,
}

/// Bytes of one request or answer frame.
pub const MAX_NOTIFICATION_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotificationTransportMessage {
    DeliverBatch {
        records: Vec<NotificationRecord>,
    },
    DeliverAck {
        written: u32,
    },
    List {
        recipient: UserId,
        cursor: Option<Vec<u8>>,
        limit: u32,
    },
    ListResult {
        records: Vec<NotificationRecord>,
        next_cursor: Option<Vec<u8>>,
    },
    UnreadCount {
        recipient: UserId,
    },
    UnreadCountResult {
        count: u32,
        capped: bool,
    },
    MarkRead {
        recipient: UserId,
        ids: Vec<Ulid>,
        up_to_ms: Option<u64>,
    },
    MarkReadResult {
        marked: u32,
    },
    Reject(String),
    CreateWatch {
        owner: UserId,
        path_prefix: String,
        event_mask: WatchEventMask,
        authorization: WatchAuthorizationBinding,
    },
    WatchCreated {
        subscription: WatchSubscription,
    },
    DeleteWatch {
        owner: UserId,
        watch_id: Ulid,
    },
    WatchDeleted,
    ListWatches {
        owner: UserId,
    },
    WatchList {
        subscriptions: Vec<WatchSubscription>,
    },
    DeliverWatchEvents {
        events: Vec<WatchEvent>,
    },
    WatchEventsAck {
        written: u32,
    },
}

impl NotificationTransportMessage {
    pub fn label(&self) -> &'static str {
        match self {
            NotificationTransportMessage::DeliverBatch { .. } => "deliver_batch",
            NotificationTransportMessage::DeliverAck { .. } => "deliver_ack",
            NotificationTransportMessage::List { .. } => "list",
            NotificationTransportMessage::ListResult { .. } => "list_result",
            NotificationTransportMessage::UnreadCount { .. } => "unread_count",
            NotificationTransportMessage::UnreadCountResult { .. } => "unread_count_result",
            NotificationTransportMessage::MarkRead { .. } => "mark_read",
            NotificationTransportMessage::MarkReadResult { .. } => "mark_read_result",
            NotificationTransportMessage::Reject(_) => "reject",
            NotificationTransportMessage::CreateWatch { .. } => "create_watch",
            NotificationTransportMessage::WatchCreated { .. } => "watch_created",
            NotificationTransportMessage::DeleteWatch { .. } => "delete_watch",
            NotificationTransportMessage::WatchDeleted => "watch_deleted",
            NotificationTransportMessage::ListWatches { .. } => "list_watches",
            NotificationTransportMessage::WatchList { .. } => "watch_list",
            NotificationTransportMessage::DeliverWatchEvents { .. } => "deliver_watch_events",
            NotificationTransportMessage::WatchEventsAck { .. } => "watch_events_ack",
        }
    }
}
