use aruna_core::document::ShardManifest;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_net::streams::BiStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncWriteExt;

/// A manifest request has only fixed-size identifiers and integer fields. This
/// small envelope leaves encoding headroom without permitting a large body to
/// be allocated before peer authorization.
pub const SHARD_MAX_REQUEST_SIZE: usize = 128;

/// A shard manifest can carry one entry per document held in the shard; 16 MiB
/// bounds a large response while still refusing a hostile oversized frame.
pub const SHARD_MAX_RESPONSE_SIZE: usize = 16 * 1024 * 1024;

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
    write_frame(stream, message, SHARD_MAX_REQUEST_SIZE).await
}

pub async fn read_shard_request(stream: &mut BiStream) -> Result<ShardTransportMessage, String> {
    read_frame(stream, SHARD_MAX_REQUEST_SIZE).await
}

pub async fn write_shard_response(
    stream: &mut BiStream,
    message: &ShardTransportResponse,
) -> Result<(), String> {
    write_frame(stream, message, SHARD_MAX_RESPONSE_SIZE).await
}

pub async fn read_shard_response(stream: &mut BiStream) -> Result<ShardTransportResponse, String> {
    read_frame(stream, SHARD_MAX_RESPONSE_SIZE).await
}

async fn write_frame<T: Serialize>(
    stream: &mut BiStream,
    message: &T,
    max_size: usize,
) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > max_size {
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

async fn read_frame<T: DeserializeOwned>(
    stream: &mut BiStream,
    max_size: usize,
) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
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
    use aruna_core::{NodeId, alpn::Alpn};
    use aruna_net::{DiscoveryMethod, InboundEventHandler, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use async_trait::async_trait;
    use std::{sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use ulid::Ulid;

    #[derive(Debug)]
    struct StreamCapture(mpsc::UnboundedSender<BiStream>);

    #[async_trait]
    impl InboundEventHandler for StreamCapture {
        async fn handle_incoming_stream(&self, _alpn: Alpn, stream: BiStream, _node_id: NodeId) {
            self.0.send(stream).expect("capture shard stream");
        }
    }

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard: 7,
        }
    }

    #[test]
    fn manifest_request_round_trips() {
        let mut placement = placement();
        placement.epoch = u64::MAX;
        placement.shard = u32::MAX;
        let message = ShardTransportMessage::ManifestRequest {
            realm_id: RealmId::from_bytes([2; 32]),
            placement,
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert!(bytes.len() <= SHARD_MAX_REQUEST_SIZE);
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

    #[tokio::test]
    async fn oversized_request_rejected() {
        let dir_a = tempdir().expect("client temp dir");
        let dir_b = tempdir().expect("server temp dir");
        let storage_a = FjallStorage::open(dir_a.path().to_str().expect("client temp path"))
            .expect("client storage");
        let storage_b = FjallStorage::open(dir_b.path().to_str().expect("server temp path"))
            .expect("server storage");
        let config = || NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind address"),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        };
        let client = NetHandle::new(config(), storage_a)
            .await
            .expect("client net handle");
        let server = NetHandle::new(config(), storage_b)
            .await
            .expect("server net handle");
        client.add_peer_addr(server.endpoint_addr()).await;
        server.add_peer_addr(client.endpoint_addr()).await;
        let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
        server.set_inbound_handler(Arc::new(StreamCapture(stream_tx)));

        let mut outbound = client
            .open_stream(server.node_id(), Alpn::Shard)
            .await
            .expect("open shard stream");
        outbound
            .0
            .write_all(&1024u32.to_be_bytes())
            .await
            .expect("write oversized request header");
        outbound.0.flush().await.expect("flush request header");
        let mut inbound = tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
            .await
            .expect("receive shard stream promptly")
            .expect("capture remains open");

        let error =
            tokio::time::timeout(Duration::from_millis(250), read_shard_request(&mut inbound))
                .await
                .expect("oversized header must be rejected without reading its body")
                .expect_err("oversized request must be rejected");
        assert_eq!(error, "shard frame exceeds maximum size");

        client.shutdown().await;
        server.shutdown().await;
    }
}
