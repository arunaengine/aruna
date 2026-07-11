use crate::error::BlobLibError;
use crate::framing::{MAX_CONTROL_PLANE_FRAME, read_frame, write_frame};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::BackendLocation;
use aruna_net::streams::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
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
        write_frame(sender, &request_buf, MAX_CONTROL_PLANE_FRAME).await?;
        Ok(())
    }

    pub async fn read(receiver: &mut RecvStream) -> Result<Self, BlobEvent> {
        let buf = read_frame(receiver, MAX_CONTROL_PLANE_FRAME)
            .await
            .map_err(|err| BlobEvent::Error(BlobError::ReadError(err.to_string())))?;
        postcard::from_bytes::<ReplicationMessage>(&buf)
            .map_err(|err| BlobEvent::Error(BlobError::ConversionError(err.into())))
    }
}
