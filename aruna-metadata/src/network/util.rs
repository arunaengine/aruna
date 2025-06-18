use iroh::endpoint::{RecvStream, SendStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use super::network_trait::MetadataMessage;
use crate::error::ArunaMetadataError;

pub(super) async fn read_message(
    recv_stream: &mut RecvStream,
) -> Result<MetadataMessage, ArunaMetadataError> {
    let len = recv_stream.read_u32().await?;
    let mut buf = vec![0; len as usize];
    recv_stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
    let message = postcard::from_bytes::<MetadataMessage>(&buf).map_err(|e| {
        ArunaMetadataError::NetworkError(format!("Failed to deserialize message: {e:#}"))
    })?;
    Ok(message)
}

pub(super) async fn send_message(
    message: MetadataMessage,
    send_stream: &mut SendStream,
) -> Result<(), ArunaMetadataError> {
    let response = postcard::to_allocvec(&message)?;
    send_stream.write_u32(response.len() as u32).await?;
    send_stream
        .write_all(&response)
        .await
        .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
    Ok(())
}
