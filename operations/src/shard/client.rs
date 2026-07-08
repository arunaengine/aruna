use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::document::ShardManifest;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tokio::time::timeout;

use crate::shard::protocol::{
    ShardTransportMessage, ShardTransportResponse, read_shard_response, write_shard_request,
};

pub const SHARD_IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Fetches a co-holder's assembled manifest for one shard over the shard ALPN.
/// A `Reject` from the co-holder (foreign realm, untrusted peer, or a shard it
/// does not hold) surfaces as `Err`.
pub async fn fetch_shard_manifest(
    net_handle: &NetHandle,
    node_id: NodeId,
    realm_id: RealmId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Shard)
        .await
        .map_err(|error| error.to_string())?;
    let request = ShardTransportMessage::ManifestRequest {
        realm_id,
        placement,
    };
    write_message(&mut stream, &request).await?;
    stream.0.finish().map_err(|error| error.to_string())?;
    let response = read_response(&mut stream).await?;
    close_stream(&mut stream).await;
    match response {
        ShardTransportResponse::Manifest(manifest) => {
            validate_manifest_response(*manifest, node_id, placement)
        }
        ShardTransportResponse::Reject(reason) => Err(reason),
    }
}

fn validate_manifest_response(
    manifest: ShardManifest,
    node_id: NodeId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    if manifest.placement != placement {
        return Err(format!(
            "shard manifest placement mismatch: requested {placement:?}, received {:?}",
            manifest.placement
        ));
    }
    if manifest.holder != node_id {
        return Err(format!(
            "shard manifest holder mismatch: requested {node_id}, received {}",
            manifest.holder
        ));
    }
    Ok(manifest)
}

async fn write_message(
    stream: &mut BiStream,
    message: &ShardTransportMessage,
) -> Result<(), String> {
    timeout(SHARD_IO_TIMEOUT, write_shard_request(stream, message))
        .await
        .map_err(|_| "timed out writing shard message".to_string())?
}

async fn read_response(stream: &mut BiStream) -> Result<ShardTransportResponse, String> {
    timeout(SHARD_IO_TIMEOUT, read_shard_response(stream))
        .await
        .map_err(|_| "timed out waiting for shard response".to_string())?
}

pub(crate) async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn placement(shard: u32) -> PlacementRef {
        PlacementRef {
            strategy_id: ulid::Ulid::from_bytes([9; 16]),
            epoch: 0,
            shard,
        }
    }

    fn manifest(holder: NodeId, placement: PlacementRef) -> ShardManifest {
        ShardManifest {
            placement,
            holder,
            entries: Vec::new(),
            cursor: Vec::new(),
            digest: [0; 32],
            updated_at_ms: 0,
        }
    }

    #[test]
    fn manifest_response_rejects_wrong_placement() {
        let holder = node_id(1);
        let requested = placement(7);
        let received = manifest(holder, placement(8));

        let error = validate_manifest_response(received, holder, requested)
            .expect_err("wrong placement must be rejected");

        assert!(
            error.contains("placement mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn manifest_response_rejects_wrong_holder() {
        let requested_holder = node_id(1);
        let placement = placement(7);
        let received = manifest(node_id(2), placement);

        let error = validate_manifest_response(received, requested_holder, placement)
            .expect_err("wrong holder must be rejected");

        assert!(
            error.contains("holder mismatch"),
            "unexpected error: {error}"
        );
    }
}
