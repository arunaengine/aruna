use aruna_permission::{Path, UserIdentity};
use iroh::NodeAddr;
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ulid::Ulid;

use crate::error::ArunaDataError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MessageType {
    InitReplicationRequest {
        user_id: UserIdentity,
        group_id: Ulid,
        bucket: String,
        permission_path: Path,
        key: Option<String>,
        size: u64,
        root: blake3::Hash,
    },
    InitReplicationResponse {
        ack: bool,
        reason: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicationMessage {
    pub id: Ulid,              // Message ID (for requests and responses)
    pub sender: NodeAddr,      // Sender of this message
    pub msg_type: MessageType, // Type of message with explicit request/response variants
}

impl ReplicationMessage {
    pub async fn send(self, sender: &mut SendStream) -> Result<(), ArunaDataError> {
        let request_buf = postcard::to_allocvec(&self)?;
        sender.write_u32(request_buf.len() as u32).await?;
        sender.write_all(&request_buf).await?;
        sender.flush().await?;

        Ok(())
    }

    pub async fn read(receiver: &mut RecvStream) -> Result<Self, ArunaDataError> {
        let msg_len = receiver.read_u32().await?;
        let mut buf = vec![0; msg_len as usize];
        receiver.read_exact(&mut buf).await?;

        let message = postcard::from_bytes::<ReplicationMessage>(&buf)?;
        Ok(message)
    }
}
