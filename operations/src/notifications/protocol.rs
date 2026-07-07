use aruna_core::structs::{NotificationRecord, WatchEvent};
use aruna_net::streams::BiStream;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

pub const NOTIFICATION_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Node-to-node inbox and watch delivery. User-facing inbox reads and watch CRUD
/// migrated to the holder proxy (`Alpn::HolderProxy`), which forwards the
/// caller's bearer; this transport now carries only trusted node-to-node
/// delivery, whose recipient is authoritative because it is server-fanned.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotificationTransportMessage {
    DeliverBatch { records: Vec<NotificationRecord> },
    DeliverAck { written: u32 },
    Reject(String),
    DeliverWatchEvents { events: Vec<WatchEvent> },
    WatchEventsAck { written: u32 },
}

pub(crate) fn notification_message_kind(message: &NotificationTransportMessage) -> &'static str {
    match message {
        NotificationTransportMessage::DeliverBatch { .. } => "deliver_batch",
        NotificationTransportMessage::DeliverAck { .. } => "deliver_ack",
        NotificationTransportMessage::Reject(_) => "reject",
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
