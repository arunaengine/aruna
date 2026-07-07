use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::structs::{NotificationRecord, WatchEvent};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tokio::time::timeout;

use crate::notifications::protocol::{
    NotificationTransportMessage, read_notification_message, write_notification_message,
};

pub const NOTIFICATION_IO_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn send_notification_request(
    net_handle: &NetHandle,
    node_id: NodeId,
    message: NotificationTransportMessage,
) -> Result<NotificationTransportMessage, String> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Notification)
        .await
        .map_err(|error| error.to_string())?;
    write_message(&mut stream, &message).await?;
    stream.0.finish().map_err(|error| error.to_string())?;
    let response = read_message(&mut stream).await?;
    close_stream(&mut stream).await;
    Ok(response)
}

pub async fn deliver_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    records: Vec<NotificationRecord>,
) -> Result<u32, String> {
    match send_notification_request(
        net_handle,
        holder,
        NotificationTransportMessage::DeliverBatch { records },
    )
    .await?
    {
        NotificationTransportMessage::DeliverAck { written } => Ok(written),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub async fn deliver_watch_events_remote(
    net_handle: &NetHandle,
    holder: NodeId,
    events: Vec<WatchEvent>,
) -> Result<u32, String> {
    match send_notification_request(
        net_handle,
        holder,
        NotificationTransportMessage::DeliverWatchEvents { events },
    )
    .await?
    {
        NotificationTransportMessage::WatchEventsAck { written } => Ok(written),
        NotificationTransportMessage::Reject(reason) => Err(reason),
        _ => Err("unexpected notification response".to_string()),
    }
}

pub(crate) async fn write_message(
    stream: &mut BiStream,
    message: &NotificationTransportMessage,
) -> Result<(), String> {
    timeout(
        NOTIFICATION_IO_TIMEOUT,
        write_notification_message(stream, message),
    )
    .await
    .map_err(|_| "timed out writing notification message".to_string())?
}

pub(crate) async fn read_message(
    stream: &mut BiStream,
) -> Result<NotificationTransportMessage, String> {
    timeout(NOTIFICATION_IO_TIMEOUT, read_notification_message(stream))
        .await
        .map_err(|_| "timed out waiting for notification message".to_string())?
}

pub(crate) async fn drain_request_stream(stream: &mut BiStream) -> Result<(), String> {
    timeout(NOTIFICATION_IO_TIMEOUT, stream.1.read_to_end(1))
        .await
        .map_err(|_| "timed out draining notification request stream".to_string())?
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub(crate) async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}
