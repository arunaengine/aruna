use aruna_core::structs::{
    NotificationRecord, WatchAuthorizationBinding, WatchEvent, WatchEventMask, WatchSubscription,
};
use aruna_core::types::UserId;
use aruna_net::streams::BiStream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

pub const NOTIFICATION_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

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

pub(crate) fn notification_message_kind(message: &NotificationTransportMessage) -> &'static str {
    match message {
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

pub async fn write_notification_message(
    stream: &mut BiStream,
    message: &NotificationTransportMessage,
) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > NOTIFICATION_MAX_MESSAGE_SIZE {
        return Err("notification message exceeds maximum size".to_string());
    }

    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
}

pub async fn read_notification_message(
    stream: &mut BiStream,
) -> Result<NotificationTransportMessage, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > NOTIFICATION_MAX_MESSAGE_SIZE {
        return Err("notification frame exceeds maximum size".to_string());
    }

    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| err.to_string())?;
    postcard::from_bytes(&bytes).map_err(|err| err.to_string())
}
