use super::{BlobEvent, BlobHandler, ControlPlaneTimeoutKind};
use crate::messages::{MessageType, ReplicationMessage};
use aruna_core::errors::BlobError;
use aruna_core::structs::BackendLocation;
use aruna_net::streams::{RecvStream, SendStream};
use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt as TokioAsyncReadExt, AsyncWriteExt as TokioAsyncWriteExt};
use tokio::time::timeout;
use ulid::Ulid;

pub(super) fn control_plane_timeout_event(
    kind: ControlPlaneTimeoutKind,
    action: &'static str,
    timeout: Duration,
) -> BlobEvent {
    let message = format!("control-plane timeout after {timeout:?} while {action}");
    let error = match kind {
        ControlPlaneTimeoutKind::Connection => BlobError::ConnectionFailed(message),
        ControlPlaneTimeoutKind::Read => BlobError::ReadError(message),
        ControlPlaneTimeoutKind::Write => BlobError::WriteError(message),
    };
    BlobEvent::Error(error)
}

pub(super) async fn with_control_plane_timeout<F, T>(
    future: F,
    timeout_duration: Duration,
    kind: ControlPlaneTimeoutKind,
    action: &'static str,
) -> Result<T, BlobEvent>
where
    F: Future<Output = T>,
{
    timeout(timeout_duration, future)
        .await
        .map_err(|_| control_plane_timeout_event(kind, action, timeout_duration))
}

pub(super) async fn send_replication_message_with_timeout(
    sender: &mut SendStream,
    message: ReplicationMessage,
    timeout_duration: Duration,
    action: &'static str,
) -> Result<(), BlobEvent> {
    match with_control_plane_timeout(
        message.send(sender),
        timeout_duration,
        ControlPlaneTimeoutKind::Write,
        action,
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(BlobEvent::Error(BlobError::WriteError(err.to_string()))),
        Err(event) => Err(event),
    }
}

pub(super) async fn read_replication_message_with_timeout(
    receiver: &mut RecvStream,
    timeout_duration: Duration,
    action: &'static str,
) -> Result<ReplicationMessage, BlobEvent> {
    match with_control_plane_timeout(
        ReplicationMessage::read(receiver),
        timeout_duration,
        ControlPlaneTimeoutKind::Read,
        action,
    )
    .await
    {
        Ok(Ok(message)) => Ok(message),
        Ok(Err(event)) => Err(event),
        Err(event) => Err(event),
    }
}

pub(super) async fn send_framed_message_with_timeout(
    sender: &mut SendStream,
    payload: &[u8],
    timeout_duration: Duration,
    action: &'static str,
) -> Result<(), BlobEvent> {
    match with_control_plane_timeout(
        async {
            TokioAsyncWriteExt::write_u32(sender, payload.len() as u32).await?;
            TokioAsyncWriteExt::write_all(sender, payload).await?;
            TokioAsyncWriteExt::flush(sender).await
        },
        timeout_duration,
        ControlPlaneTimeoutKind::Write,
        action,
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(BlobEvent::Error(BlobError::WriteError(err.to_string()))),
        Err(event) => Err(event),
    }
}

pub(super) async fn read_framed_message_with_timeout(
    receiver: &mut RecvStream,
    timeout_duration: Duration,
    action: &'static str,
) -> Result<Vec<u8>, BlobEvent> {
    match with_control_plane_timeout(
        async {
            let msg_len = TokioAsyncReadExt::read_u32(receiver).await?;
            let mut buf = vec![0; msg_len as usize];
            TokioAsyncReadExt::read_exact(receiver, &mut buf).await?;
            Ok::<Vec<u8>, std::io::Error>(buf)
        },
        timeout_duration,
        ControlPlaneTimeoutKind::Read,
        action,
    )
    .await
    {
        Ok(Ok(payload)) => Ok(payload),
        Ok(Err(err)) => Err(BlobEvent::Error(BlobError::ReadError(err.to_string()))),
        Err(event) => Err(event),
    }
}

pub(super) fn validate_replication_init_ack(
    message: ReplicationMessage,
    replication_id: Ulid,
) -> Result<(), BlobError> {
    if message.id != replication_id {
        return Err(BlobError::ReplicationRejected(format!(
            "received replication init ack for unexpected replication id: expected {replication_id}, got {}",
            message.id
        )));
    }

    match message.msg_type {
        MessageType::BaoTreeInfoReceived => Ok(()),
        other => Err(BlobError::ReplicationRejected(format!(
            "unexpected replication init response: {other:?}"
        ))),
    }
}

pub(super) fn parse_replication_init(
    message: ReplicationMessage,
    replication_id: Option<Ulid>,
) -> Result<(Ulid, blake3::Hash, BackendLocation), BlobError> {
    let effective_replication_id = replication_id.unwrap_or(message.id);
    if message.id != effective_replication_id {
        return Err(BlobError::ReplicationRejected(format!(
            "received replication init for unexpected replication id: expected {effective_replication_id}, got {}",
            message.id
        )));
    }

    match message.msg_type {
        MessageType::BaoTreeInfo { root, location } => {
            Ok((effective_replication_id, root, location))
        }
        _ => Err(BlobError::ReplicationRejected(
            "Invalid BaoTreeInfo message".to_string(),
        )),
    }
}

impl BlobHandler {
    pub(super) fn control_plane_connect_timeout(&self) -> Duration {
        self.backend_config.timeouts.control_plane_connect_timeout
    }

    pub(super) fn control_plane_io_timeout(&self) -> Duration {
        self.backend_config.timeouts.control_plane_io_timeout
    }

    pub(super) fn transfer_idle_timeout(&self) -> Duration {
        self.backend_config.timeouts.transfer_idle_timeout
    }
}
