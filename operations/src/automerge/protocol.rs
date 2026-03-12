use aruna_core::automerge::{AutomergeInit, AutomergeRejectReason, AutomergeSyncError};
use aruna_net::streams::BiStream;
use serde::{Deserialize, Serialize};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutomergeTransportMessage {
    Init(AutomergeInit),
    Sync(Vec<u8>),
    Done,
    Reject(AutomergeRejectReason),
}

pub async fn write_message(
    stream: &mut BiStream,
    message: &AutomergeTransportMessage,
) -> Result<(), AutomergeSyncError> {
    let bytes = postcard::to_allocvec(message)
        .map_err(|err| AutomergeSyncError::Protocol(err.to_string()))?;
    if bytes.len() > MAX_FRAME_SIZE {
        return Err(AutomergeSyncError::Protocol(
            "automerge frame exceeds maximum size".to_string(),
        ));
    }

    let len = (bytes.len() as u32).to_be_bytes();
    stream
        .0
        .write_all(&len)
        .await
        .map_err(|err| AutomergeSyncError::Network(err.to_string()))?;
    stream
        .0
        .write_all(&bytes)
        .await
        .map_err(|err| AutomergeSyncError::Network(err.to_string()))?;
    Ok(())
}

pub async fn read_message(
    stream: &mut BiStream,
) -> Result<AutomergeTransportMessage, AutomergeSyncError> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| AutomergeSyncError::Network(err.to_string()))?;

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(AutomergeSyncError::InvalidFrame);
    }

    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| AutomergeSyncError::Network(err.to_string()))?;

    postcard::from_bytes(&bytes).map_err(|_| AutomergeSyncError::InvalidFrame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::automerge::AutomergeDocumentVariant;
    use ulid::Ulid;

    #[test]
    fn transport_frame_roundtrip() {
        let message = AutomergeTransportMessage::Init(AutomergeInit::new(
            AutomergeDocumentVariant::Metadata {
                group_id: Ulid::new(),
                document_id: Ulid::new(),
            },
            Vec::new(),
        ));

        let encoded = postcard::to_allocvec(&message).expect("message encodes");
        let decoded: AutomergeTransportMessage = postcard::from_bytes(&encoded).expect("message decodes");
        assert_eq!(message, decoded);
    }
}
