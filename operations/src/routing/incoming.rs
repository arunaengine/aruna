use aruna_core::NodeId;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive};
use crate::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use crate::routing::protocol::{
    HolderProxyRequest, HolderProxyResponse, read_holder_proxy_request, write_holder_proxy_response,
};
use crate::routing::{
    MetadataStrategyKeyError, load_metadata_strategy_key, resolve_call_holders, serve_local,
    validate_proxy_bearer,
};

#[tracing::instrument(
    name = "routing.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub async fn handle_holder_proxy_stream(
    context: &DriverContext,
    mut stream: BiStream,
    peer: NodeId,
) {
    let Some(net_handle) = context.net_handle.as_ref() else {
        warn!(peer = %peer, "Dropping inbound holder proxy stream without net handle");
        return;
    };

    let request = match read_holder_proxy_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read holder proxy request");
            return;
        }
    };

    let response = build_response(context, net_handle, peer, request).await;
    if let Err(error) = write_holder_proxy_response(&mut stream, &response).await {
        warn!(peer = %peer, error = %error, "Failed to write holder proxy response");
    }
    close_stream(&mut stream).await;
}

async fn build_response(
    context: &DriverContext,
    net_handle: &NetHandle,
    peer: NodeId,
    request: HolderProxyRequest,
) -> HolderProxyResponse {
    let realm_id = request.realm_id;
    if realm_id != *net_handle.realm_id() {
        return HolderProxyResponse::forbidden(format!(
            "holder proxy peer `{peer}` addressed foreign realm `{realm_id}`"
        ));
    }

    let config = match drive(GetRealmConfigOperation::new(realm_id), context).await {
        Ok(config) => config,
        Err(GetRealmConfigError::DocumentNotFound) => {
            return HolderProxyResponse::unavailable(format!(
                "realm `{realm_id}` config unavailable"
            ));
        }
        Err(error) => return HolderProxyResponse::internal(error.to_string()),
    };

    // Trust gate: only sync-eligible (server-class) realm nodes may proxy.
    match config.sync_eligible_node_ids() {
        Ok(eligible) if eligible.contains(&peer) => {}
        Ok(_) => {
            return HolderProxyResponse::forbidden(format!(
                "holder proxy peer `{peer}` is not a sync-eligible node in realm `{realm_id}`"
            ));
        }
        Err(error) => return HolderProxyResponse::internal(error.to_string()),
    }

    // Loop guard: a target serves only its own hop and never re-forwards.
    if request.hop != 0 {
        return HolderProxyResponse::NotHolder;
    }

    // Resolve holders against the same strategy key the origin used: a by-id
    // metadata mutation loads `(group, path)` from the local registry record. An
    // absent record means this node cannot confirm it holds the shard → NotHolder.
    let metadata_key = match load_metadata_strategy_key(context, &request.call).await {
        Ok(key) => key,
        Err(MetadataStrategyKeyError::NotFound) => return HolderProxyResponse::NotHolder,
        Err(MetadataStrategyKeyError::Storage(reason)) => {
            return HolderProxyResponse::internal(reason);
        }
    };

    let Some((placement, holders)) =
        resolve_call_holders(&config, &request.call, metadata_key.as_ref())
    else {
        return HolderProxyResponse::internal("proxied call domain not yet supported");
    };
    if !holders.contains(&net_handle.node_id()) {
        return HolderProxyResponse::NotHolder;
    }

    // Target validates the forwarded bearer itself and rebuilds the auth context.
    let auth =
        match validate_proxy_bearer(context, request.bearer.as_ref().map(|b| b.as_str())).await {
            Ok(auth) => auth,
            Err(reason) => return HolderProxyResponse::forbidden(reason),
        };

    let response = serve_local(context, request.call, auth).await;
    debug!(
        strategy = %placement.strategy_id,
        shard = placement.shard,
        served = matches!(response, HolderProxyResponse::Ok(_)),
        "Served holder proxy call"
    );
    response
}

pub(crate) async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::incoming::initialize_net_incoming;
    use crate::routing::protocol::{
        GroupCall, HolderProxyResponse, ProxiedCall, ProxyBearerToken, RejectKind,
        read_holder_proxy_response, write_holder_proxy_request,
    };
    use aruna_core::alpn::Alpn;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::{Actor, RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, RelayMethod};
    use aruna_storage::FjallStorage;
    use byteview::ByteView;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct Node {
        _dir: TempDir,
        net: NetHandle,
        context: Arc<DriverContext>,
    }

    async fn spawn(realm_id: RealmId, secret: [u8; 32]) -> Node {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        initialize_net_incoming(context.clone());
        Node {
            _dir: dir,
            net,
            context,
        }
    }

    async fn connect(from: &Node, to: &Node) {
        from.net.add_peer_addr(to.net.endpoint_addr()).await;
    }

    async fn install_config(node: &Node, realm_id: RealmId, members: &[(NodeId, RealmNodeKind)]) {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        for (node_id, kind) in members {
            config.ensure_node(*node_id, kind.clone());
        }
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        match node
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(bytes),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write event: {other:?}"),
        }
    }

    async fn send(from: &Node, to: &Node, request: HolderProxyRequest) -> HolderProxyResponse {
        let mut stream = from
            .net
            .open_stream(to.net.node_id(), Alpn::HolderProxy)
            .await
            .expect("open stream");
        write_holder_proxy_request(&mut stream, &request)
            .await
            .expect("write request");
        stream.0.finish().expect("finish");
        let response = read_holder_proxy_response(&mut stream)
            .await
            .expect("read response");
        close_stream(&mut stream).await;
        response
    }

    fn get_group_request(realm_id: RealmId, group_id: Ulid, hop: u8) -> HolderProxyRequest {
        HolderProxyRequest {
            realm_id,
            bearer: Some(ProxyBearerToken::new("token").unwrap()),
            hop,
            call: ProxiedCall::Group(GroupCall::Get { group_id }),
        }
    }

    #[tokio::test]
    async fn hop_nonzero_is_not_holder_without_forwarding() {
        let realm_id = RealmId::from_bytes([90u8; 32]);
        let a = spawn(realm_id, [90u8; 32]).await;
        let b = spawn(realm_id, [91u8; 32]).await;
        connect(&a, &b).await;
        let members = [
            (a.net.node_id(), RealmNodeKind::Server),
            (b.net.node_id(), RealmNodeKind::Server),
        ];
        install_config(&b, realm_id, &members).await;

        let response = send(&a, &b, get_group_request(realm_id, Ulid::new(), 1)).await;
        assert_eq!(response, HolderProxyResponse::NotHolder);
    }

    #[tokio::test]
    async fn non_realm_peer_is_rejected() {
        let realm_id = RealmId::from_bytes([92u8; 32]);
        let b = spawn(realm_id, [93u8; 32]).await;
        let c = spawn(realm_id, [94u8; 32]).await;
        connect(&c, &b).await;
        // c is not configured in b's realm.
        install_config(&b, realm_id, &[(b.net.node_id(), RealmNodeKind::Server)]).await;

        let response = send(&c, &b, get_group_request(realm_id, Ulid::new(), 0)).await;
        assert!(
            matches!(&response, HolderProxyResponse::Rejected { kind, reason } if *kind == RejectKind::Forbidden && reason.contains("not a sync-eligible node")),
            "unexpected response: {response:?}"
        );
    }

    // With the default replica factor (3) and two nodes, b holds every group's
    // shard, so any group id reaches the bearer check on b.
    #[tokio::test]
    async fn missing_bearer_on_holder_is_rejected() {
        let realm_id = RealmId::from_bytes([95u8; 32]);
        let a = spawn(realm_id, [95u8; 32]).await;
        let b = spawn(realm_id, [96u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        let request = HolderProxyRequest {
            realm_id,
            bearer: None,
            hop: 0,
            call: ProxiedCall::Group(GroupCall::Get {
                group_id: Ulid::new(),
            }),
        };
        let response = send(&a, &b, request).await;
        assert!(
            matches!(&response, HolderProxyResponse::Rejected { kind, reason } if *kind == RejectKind::Forbidden && reason.contains("requires a bearer token")),
            "unexpected response: {response:?}"
        );
    }

    #[tokio::test]
    async fn invalid_bearer_on_holder_is_rejected() {
        let realm_id = RealmId::from_bytes([97u8; 32]);
        let a = spawn(realm_id, [97u8; 32]).await;
        let b = spawn(realm_id, [98u8; 32]).await;
        connect(&a, &b).await;
        install_config(
            &b,
            realm_id,
            &[
                (a.net.node_id(), RealmNodeKind::Server),
                (b.net.node_id(), RealmNodeKind::Server),
            ],
        )
        .await;

        // A syntactically invalid (non-JWT) bearer must be refused by the target.
        let response = send(&a, &b, get_group_request(realm_id, Ulid::new(), 0)).await;
        assert!(
            matches!(&response, HolderProxyResponse::Rejected { kind, reason } if *kind == RejectKind::Forbidden && reason.contains("invalid bearer token")),
            "unexpected response: {response:?}"
        );
    }
}
