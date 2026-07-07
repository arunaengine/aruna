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
        ShardTransportResponse::Manifest(manifest) => Ok(*manifest),
        ShardTransportResponse::Reject(reason) => Err(reason),
    }
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
