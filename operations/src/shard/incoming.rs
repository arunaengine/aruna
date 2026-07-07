use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use byteview::ByteView;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_shard_holders;
use crate::shard::assemble_shard_manifest;
use crate::shard::client::close_stream;
use crate::shard::protocol::{
    ShardTransportMessage, ShardTransportResponse, read_shard_request, write_shard_response,
};

#[tracing::instrument(
    name = "shard.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub async fn handle_shard_stream(context: &DriverContext, mut stream: BiStream, peer: NodeId) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(peer = %peer, "Dropping inbound shard stream without net handle");
        return;
    };

    let message = match read_shard_request(&mut stream).await {
        Ok(message) => message,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read shard manifest request");
            return;
        }
    };

    let response = build_response(context, net_handle, peer, message).await;
    if let Err(error) = write_shard_response(&mut stream, &response).await {
        warn!(peer = %peer, error = %error, "Failed to write shard manifest response");
    }
    close_stream(&mut stream).await;
}

async fn build_response(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    message: ShardTransportMessage,
) -> ShardTransportResponse {
    let ShardTransportMessage::ManifestRequest {
        realm_id,
        placement,
    } = message;

    if realm_id != *net_handle.realm_id() {
        return ShardTransportResponse::Reject(format!(
            "shard peer `{peer}` addressed foreign realm `{realm_id}`"
        ));
    }
    let config = match read_realm_config(context, realm_id).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            return ShardTransportResponse::Reject(format!(
                "realm `{realm_id}` config unavailable"
            ));
        }
        Err(error) => return ShardTransportResponse::Reject(error),
    };

    // Trust gate: only sync-eligible (server-class) realm nodes may fetch a
    // manifest, mirroring the notification/metadata peer checks.
    match config.sync_eligible_node_ids() {
        Ok(eligible) if eligible.contains(&peer) => {}
        Ok(_) => {
            return ShardTransportResponse::Reject(format!(
                "shard peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
            ));
        }
        Err(error) => return ShardTransportResponse::Reject(error.to_string()),
    }

    // Only a current holder can answer authoritatively for a shard.
    if !resolve_shard_holders(&config, &placement).contains(&net_handle.node_id()) {
        return ShardTransportResponse::Reject(format!(
            "node does not hold shard {}/{} in realm `{realm_id}`",
            placement.strategy_id, placement.shard
        ));
    }

    match assemble_shard_manifest(context, realm_id, placement).await {
        Ok(manifest) => {
            debug!(
                strategy = %placement.strategy_id,
                shard = placement.shard,
                entries = manifest.entries.len(),
                "Served shard manifest"
            );
            ShardTransportResponse::Manifest(Box::new(manifest))
        }
        Err(error) => ShardTransportResponse::Reject(error),
    }
}

async fn read_realm_config(
    context: &DriverContext,
    realm_id: RealmId,
) -> Result<Option<RealmConfigDocument>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => RealmConfigDocument::from_bytes(&bytes)
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {other:?}")),
    }
}
