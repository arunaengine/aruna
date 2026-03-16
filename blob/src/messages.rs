use crate::error::BlobLibError;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::{BlobInfo, NegotiationResult, UserIdentity};
use iroh_quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum MessageType {
    NegotiationRequest {
        user_id: UserIdentity, // Necessary?
        group_id: Ulid,
        size: u64,
    },
    NegotiationResponse(NegotiationResult),
    SomeTest(NegotiationResult),
    BaoTreeInfo {
        root: blake3::Hash,
        blob_info: BlobInfo,
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
        sender.write_u32(request_buf.len() as u32).await?;
        sender.write_all(&request_buf).await?;
        sender.flush().await?;
        Ok(())
    }

    pub async fn read(receiver: &mut RecvStream) -> Result<Self, BlobLibError> {
        let msg_len = receiver.read_u32().await?;
        let mut buf = vec![0; msg_len as usize];
        receiver.read_exact(&mut buf).await?;

        let message = postcard::from_bytes::<ReplicationMessage>(&buf)?;
        Ok(message)
    }

    pub async fn read2(receiver: &mut RecvStream) -> Result<Self, BlobEvent> {
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
