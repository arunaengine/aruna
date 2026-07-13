use std::future::Future;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::document::ShardManifest;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use byteview::ByteView;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::placement::resolve_shard_holders;
use crate::shard::assemble_shard_manifest;
use crate::shard::client::{SHARD_IO_TIMEOUT, close_stream};
use crate::shard::protocol::{
    ManifestPagePlan, SHARD_MAX_RESPONSE_SIZE, ShardTransportMessage, ShardTransportResponse,
    plan_manifest_pages, read_shard_request, write_manifest_pages, write_shard_response,
};

enum PreparedShardResponse {
    Message(ShardTransportResponse),
    ManifestPages {
        manifest: Box<ShardManifest>,
        plan: ManifestPagePlan,
    },
}

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

    let message = match with_shard_io_timeout(
        "reading shard manifest request",
        read_shard_request(&mut stream),
    )
    .await
    {
        Ok(message) => message,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read shard manifest request");
            return;
        }
    };

    let response = build_response(context, net_handle, peer, message).await;
    if let Err(error) = with_shard_io_timeout(
        "writing shard manifest response",
        write_response(&mut stream, &response),
    )
    .await
    {
        warn!(peer = %peer, error = %error, "Failed to write shard manifest response");
    }
    close_stream(&mut stream).await;
}

async fn write_response(
    stream: &mut BiStream,
    response: &PreparedShardResponse,
) -> Result<(), String> {
    match response {
        PreparedShardResponse::Message(message) => write_shard_response(stream, message).await,
        PreparedShardResponse::ManifestPages { manifest, plan } => {
            write_manifest_pages(stream, manifest, plan).await
        }
    }
}

async fn with_shard_io_timeout<T>(
    operation: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    with_shard_io_timeout_after(SHARD_IO_TIMEOUT, operation, future).await
}

async fn with_shard_io_timeout_after<T>(
    duration: Duration,
    operation: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    timeout(duration, future)
        .await
        .unwrap_or_else(|_| Err(format!("timed out {operation}")))
}

async fn build_response(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    message: ShardTransportMessage,
) -> PreparedShardResponse {
    let ShardTransportMessage::ManifestRequest {
        realm_id,
        placement,
    } = message;

    if realm_id != *net_handle.realm_id() {
        return PreparedShardResponse::Message(ShardTransportResponse::Reject(format!(
            "shard peer `{peer}` addressed foreign realm `{realm_id}`"
        )));
    }
    let config = match read_realm_config(context, realm_id).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            return PreparedShardResponse::Message(ShardTransportResponse::Reject(format!(
                "realm `{realm_id}` config unavailable"
            )));
        }
        Err(error) => {
            return PreparedShardResponse::Message(ShardTransportResponse::Reject(error));
        }
    };

    // Trust gate: only sync-eligible (server-class) realm nodes may fetch a
    // manifest, mirroring the notification/metadata peer checks.
    match config.sync_eligible_node_ids() {
        Ok(eligible) if eligible.contains(&peer) => {}
        Ok(_) => {
            return PreparedShardResponse::Message(ShardTransportResponse::Reject(format!(
                "shard peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
            )));
        }
        Err(error) => {
            return PreparedShardResponse::Message(ShardTransportResponse::Reject(
                error.to_string(),
            ));
        }
    }

    let holders = resolve_shard_holders(&config, &placement);
    // Only a current holder can answer authoritatively for a shard.
    if !holders.contains(&net_handle.node_id()) {
        return PreparedShardResponse::Message(ShardTransportResponse::Reject(format!(
            "node does not hold shard {}/{} in realm `{realm_id}`",
            placement.strategy_id, placement.shard
        )));
    }
    if !holders.contains(&peer) {
        return PreparedShardResponse::Message(ShardTransportResponse::Reject(format!(
            "shard peer `{peer}` is not a holder for shard {}/{} in realm `{realm_id}`",
            placement.strategy_id, placement.shard
        )));
    }

    match assemble_shard_manifest(context, realm_id, placement).await {
        Ok(manifest) => {
            let entries = manifest.entries.len();
            let response = match plan_manifest_pages(&manifest, SHARD_MAX_RESPONSE_SIZE) {
                Ok(plan) => PreparedShardResponse::ManifestPages {
                    manifest: Box::new(manifest),
                    plan,
                },
                Err(error) => PreparedShardResponse::Message(ShardTransportResponse::Reject(error)),
            };
            debug!(
                strategy = %placement.strategy_id,
                shard = placement.shard,
                entries,
                "Served shard manifest"
            );
            response
        }
        Err(error) => PreparedShardResponse::Message(ShardTransportResponse::Reject(error)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shard_io_timeout_reports_timed_out_operation() {
        let error = with_shard_io_timeout_after(
            Duration::from_millis(1),
            "reading shard manifest request",
            std::future::pending::<Result<(), String>>(),
        )
        .await
        .expect_err("pending shard IO must time out");

        assert_eq!(error, "timed out reading shard manifest request");
    }
}
