//! Accept-time ALPN x node-kind boundary between realm nodes and user devices.

use std::sync::Arc;
use std::time::Duration;

use aruna_core::alpn::Alpn;
use aruna_core::id::NodeId;
use aruna_core::structs::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::types::UserId;
use aruna_net::streams::BiStream;
use aruna_net::{DiscoveryMethod, InboundEventHandler, NetConfig, NetHandle, RelayMethod};
use aruna_storage::FjallStorage;
use async_trait::async_trait;
use tempfile::tempdir;
use tokio::sync::mpsc;

const NETWORK_HANG_CAP: Duration = Duration::from_secs(45);

#[derive(Clone, Default)]
struct TestInboundHandler {
    stream_tx: Option<mpsc::UnboundedSender<(Alpn, BiStream, NodeId)>>,
}

#[async_trait]
impl InboundEventHandler for TestInboundHandler {
    async fn handle_incoming_stream(&self, alpn: Alpn, stream: BiStream, node_id: NodeId) {
        if let Some(tx) = &self.stream_tx {
            let _ = tx.send((alpn, stream, node_id));
        }
    }
}

fn user_kind(realm_id: RealmId) -> RealmNodeKind {
    RealmNodeKind::User {
        owner: UserId::nil(realm_id),
    }
}

fn config(realm_id: RealmId, secret_key: iroh::SecretKey) -> NetConfig {
    NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        secret_key: Some(secret_key),
        realm_id,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    }
}

#[tokio::test]
async fn refuses_user_sync() -> Result<(), Box<dyn std::error::Error>> {
    // The realm node knows the dialer as a User device, so document sync is
    // refused while the metadata read surface stays open. The device's own
    // config calls it Management, so only the accept side can refuse.
    let realm_id = RealmId::from_bytes([91u8; 32]);
    let temp_device = tempdir()?;
    let temp_realm = tempdir()?;
    let storage_device =
        FjallStorage::open(temp_device.path().to_str().ok_or("invalid temp path")?)?;
    let storage_realm = FjallStorage::open(temp_realm.path().to_str().ok_or("invalid temp path")?)?;

    let device = NetHandle::new(
        config(realm_id, iroh::SecretKey::from_bytes(&[91u8; 32])),
        storage_device,
    )
    .await?;
    let realm = NetHandle::new(
        config(realm_id, iroh::SecretKey::from_bytes(&[92u8; 32])),
        storage_realm,
    )
    .await?;

    let mut realm_view = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    realm_view.ensure_node(realm.node_id(), RealmNodeKind::Management);
    realm_view.ensure_node(device.node_id(), user_kind(realm_id));
    realm.refresh_realm_peers_from_document(&realm_view).await?;

    let mut device_view = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    device_view.ensure_node(realm.node_id(), RealmNodeKind::Management);
    device_view.ensure_node(device.node_id(), RealmNodeKind::Management);
    device
        .refresh_realm_peers_from_document(&device_view)
        .await?;

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    realm.set_inbound_handler(Arc::new(TestInboundHandler {
        stream_tx: Some(stream_tx),
    }));
    device.add_peer_addr(realm.endpoint_addr()).await;
    realm.add_peer_addr(device.endpoint_addr()).await;

    let mut allowed = device.open_stream(realm.node_id(), Alpn::Metadata).await?;
    allowed.0.write_all(b"metadata probe").await?;
    let (accepted_alpn, _stream, dialer) = tokio::time::timeout(NETWORK_HANG_CAP, stream_rx.recv())
        .await?
        .ok_or("expected an inbound metadata stream")?;
    assert_eq!(accepted_alpn, Alpn::Metadata);
    assert_eq!(dialer, device.node_id());

    let refused = tokio::time::timeout(NETWORK_HANG_CAP, async {
        let Ok(mut stream) = device
            .open_stream(realm.node_id(), Alpn::DocumentSync)
            .await
        else {
            return true;
        };
        if stream.0.write_all(b"sync probe").await.is_err() {
            return true;
        }
        let _ = stream.0.finish();
        stream.1.read_to_end(1024).await.is_err()
    })
    .await?;
    assert!(refused, "a user-kind key must not reach document sync");

    device.shutdown().await;
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn device_refuses_sync() -> Result<(), Box<dyn std::error::Error>> {
    // Direction matters: a device never dials sync or shard exchange, dials job
    // control for its owner's submissions, and never serves job control.
    let realm_id = RealmId::from_bytes([93u8; 32]);
    let temp_device = tempdir()?;
    let temp_realm = tempdir()?;
    let storage_device =
        FjallStorage::open(temp_device.path().to_str().ok_or("invalid temp path")?)?;
    let storage_realm = FjallStorage::open(temp_realm.path().to_str().ok_or("invalid temp path")?)?;

    let device = NetHandle::new(
        config(realm_id, iroh::SecretKey::from_bytes(&[93u8; 32])),
        storage_device,
    )
    .await?;
    let realm = NetHandle::new(
        config(realm_id, iroh::SecretKey::from_bytes(&[94u8; 32])),
        storage_realm,
    )
    .await?;

    let mut view = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    view.ensure_node(realm.node_id(), RealmNodeKind::Management);
    view.ensure_node(device.node_id(), user_kind(realm_id));
    device.refresh_realm_peers_from_document(&view).await?;
    realm.refresh_realm_peers_from_document(&view).await?;

    realm.set_inbound_handler(Arc::new(TestInboundHandler::default()));
    device.add_peer_addr(realm.endpoint_addr()).await;
    realm.add_peer_addr(device.endpoint_addr()).await;

    for alpn in [Alpn::DocumentSync, Alpn::Shard] {
        device
            .open_stream(realm.node_id(), alpn)
            .await
            .expect_err("a user device must not dial realm infrastructure protocols");
    }
    device.open_stream(realm.node_id(), Alpn::Metadata).await?;
    device
        .open_stream(realm.node_id(), Alpn::JobControl)
        .await?;

    let refused = tokio::time::timeout(NETWORK_HANG_CAP, async {
        let Ok(mut stream) = realm.open_stream(device.node_id(), Alpn::JobControl).await else {
            return true;
        };
        if stream.0.write_all(b"job probe").await.is_err() {
            return true;
        }
        let _ = stream.0.finish();
        stream.1.read_to_end(1024).await.is_err()
    })
    .await?;
    assert!(refused, "a user device must not serve job control");

    device.shutdown().await;
    realm.shutdown().await;
    Ok(())
}
