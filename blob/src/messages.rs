use crate::error::BlobLibError;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::BackendLocation;
use aruna_net::streams::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ulid::Ulid;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum MessageType {
    BaoTreeInfo {
        root: blake3::Hash,
        location: BackendLocation,
    },
    BaoTreeInfoReceived,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ReplicationMessage {
    pub id: Ulid,              // Replication ID (for requests and responses)
    pub msg_type: MessageType, // Type of message with explicit request/response variants
}

impl ReplicationMessage {
    pub fn new(id: Ulid, msg: MessageType) -> Self {
        Self { id, msg_type: msg }
    }

    pub async fn send(self, sender: &mut SendStream) -> Result<(), BlobLibError> {
        let request_buf = postcard::to_allocvec(&self)?;
        AsyncWriteExt::write_u32(sender, request_buf.len() as u32).await?;
        AsyncWriteExt::write_all(sender, &request_buf).await?;
        AsyncWriteExt::flush(sender).await?;
        Ok(())
    }

    pub async fn read(receiver: &mut RecvStream) -> Result<Self, BlobEvent> {
        let msg_len = match receiver.read_u32().await {
            Ok(len) => len,
            Err(err) => return Err(BlobEvent::Error(BlobError::ReadError(err.to_string()))),
        };
        let mut buf = vec![0; msg_len as usize];
        match receiver.read_exact(&mut buf).await {
            Ok(len) => len,
            Err(err) => return Err(BlobEvent::Error(BlobError::ReadError(err.to_string()))),
        };

        let message = match postcard::from_bytes::<ReplicationMessage>(&buf) {
            Ok(len) => len,
            Err(err) => return Err(BlobEvent::Error(BlobError::ConversionError(err.into()))),
        };
        Ok(message)
    }
}
