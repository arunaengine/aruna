use aruna_core::document::ShardManifest;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_net::streams::BiStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncWriteExt;

/// A shard manifest can carry one entry per document held in the shard; 16 MiB
/// bounds a large shard while still refusing a hostile oversized frame.
pub const SHARD_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// New-holder request: give me your manifest for this shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ShardTransportMessage {
    ManifestRequest {
        realm_id: RealmId,
        placement: PlacementRef,
    },
}

/// Co-holder reply: the assembled manifest, or a rejection (foreign realm,
/// untrusted peer, or a shard the responder does not hold).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub enum ShardTransportResponse {
    Manifest(Box<ShardManifest>),
    Reject(String),
}

pub async fn write_shard_request(
    stream: &mut BiStream,
    message: &ShardTransportMessage,
) -> Result<(), String> {
    write_frame(stream, message).await
}

pub async fn read_shard_request(stream: &mut BiStream) -> Result<ShardTransportMessage, String> {
    read_frame(stream).await
}

pub async fn write_shard_response(
    stream: &mut BiStream,
    message: &ShardTransportResponse,
) -> Result<(), String> {
    write_frame(stream, message).await
}

pub async fn read_shard_response(stream: &mut BiStream) -> Result<ShardTransportResponse, String> {
    read_frame(stream).await
}

async fn write_frame<T: Serialize>(stream: &mut BiStream, message: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > SHARD_MAX_MESSAGE_SIZE {
        return Err("shard message exceeds maximum size".to_string());
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

async fn read_frame<T: DeserializeOwned>(stream: &mut BiStream) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > SHARD_MAX_MESSAGE_SIZE {
        return Err("shard frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| err.to_string())?;
    postcard::from_bytes(&bytes).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentSyncRevision, DocumentSyncTarget, ShardManifest, ShardManifestEntry,
    };
    use ulid::Ulid;

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard: 7,
        }
    }

    #[test]
    fn manifest_request_round_trips() {
        let message = ShardTransportMessage::ManifestRequest {
            realm_id: RealmId::from_bytes([2; 32]),
            placement: placement(),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert_eq!(
            postcard::from_bytes::<ShardTransportMessage>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn manifest_response_round_trips() {
        let holder = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let manifest = ShardManifest {
            placement: placement(),
            holder,
            entries: vec![ShardManifestEntry {
                target: DocumentSyncTarget::MetadataDocumentLifecycle {
                    document_id: Ulid::from_bytes([4; 16]),
                },
                revision: DocumentSyncRevision {
                    generation: 3,
                    event_id: Ulid::from_bytes([5; 16]),
                    actor: holder,
                    updated_at_ms: 42,
                },
            }],
            cursor: vec![1, 2, 3],
            digest: [7u8; 32],
            updated_at_ms: 99,
        };
        let response = ShardTransportResponse::Manifest(Box::new(manifest));
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert_eq!(
            postcard::from_bytes::<ShardTransportResponse>(&bytes).unwrap(),
            response
        );

        let reject = ShardTransportResponse::Reject("nope".to_string());
        let bytes = postcard::to_allocvec(&reject).unwrap();
        assert_eq!(
            postcard::from_bytes::<ShardTransportResponse>(&bytes).unwrap(),
            reject
        );
    }
}
