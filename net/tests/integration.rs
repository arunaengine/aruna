use std::sync::Arc;
use std::time::Duration;

use aruna_core::TopicId;
use aruna_core::alpn::Alpn;
use aruna_core::effects::{DhtEffect, Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::events::{DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::{
    ConnectionAddressStatus, PeerConnectionStatus, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_net::streams::BiStream;
use aruna_net::{
    DiscoveryMethod, InboundEventHandler, NetConfig, NetError, NetHandle, RelayMethod,
};
use aruna_storage::FjallStorage;
use async_trait::async_trait;
use byteview::ByteView;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

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

#[tokio::test]
async fn test_storage_iter_pagination() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;

    for (k, v) in [
        (b"a1", b"v1"),
        (b"a2", b"v2"),
        (b"a3", b"v3"),
        (b"b1", b"v4"),
    ] {
        let _ = storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: "iter_test".to_string(),
                key: ByteView::from(*k),
                value: ByteView::from(*v),
                txn_id: None,
            }))
            .await;
    }

    let first = storage
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: "iter_test".to_string(),
            prefix: Some(ByteView::from(*b"a")),
            start: None,
            limit: 2,
            txn_id: None,
        }))
        .await;

    let (values, next_start_after) = match first {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => (values, next_start_after),
        other => panic!("unexpected first iter result: {other:?}"),
    };

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].0, ByteView::from(*b"a1"));
    assert_eq!(values[1].0, ByteView::from(*b"a2"));
    assert_eq!(next_start_after, Some(ByteView::from(*b"a2")));

    let second = storage
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: "iter_test".to_string(),
            prefix: Some(ByteView::from(*b"a")),
            start: next_start_after.map(IterStart::After),
            limit: 2,
            txn_id: None,
        }))
        .await;

    match second {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].0, ByteView::from(*b"a3"));
            assert!(next_start_after.is_none());
        }
        other => panic!("unexpected second iter result: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_multi_node_dht_put_get() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let key = aruna_core::id::DhtKeyId::from_bytes([42u8; 32]);
    let value = b"replicated-value".to_vec();

    let mut put_succeeded = false;
    let mut last_put_event = None;
    for _ in 0..10 {
        let put = handle_a
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Put {
                key,
                realm_id: RealmId::from_bytes([1u8; 32]),
                value: value.clone(),
                ttl: Duration::from_secs(3600),
            })))
            .await;
        last_put_event = Some(format!("{put:?}"));

        if matches!(put, Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))) {
            put_succeeded = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(
        put_succeeded,
        "expected strict PUT to receive at least one remote acknowledgement, last event: {}",
        last_put_event.unwrap_or_else(|| "<none>".to_string())
    );

    let mut found = false;
    for _ in 0..10 {
        let get = handle_b
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Get {
                key,
                realm_filter: None,
            })))
            .await;

        if let Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) = get
            && values.iter().any(|entry| entry.value == value)
        {
            found = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(found, "expected replicated DHT value on second node");

    handle_a.shutdown().await;
    handle_b.shutdown().await;

    Ok(())
}

#[tokio::test]
async fn dht_fallback() -> Result<(), Box<dyn std::error::Error>> {
    tokio::time::timeout(Duration::from_secs(45), dht_fallback_inner())
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "DHT fallback test exceeded 45 seconds",
            )
        })?
}

async fn dht_fallback_inner() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let temp_c = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;
    let storage_c = FjallStorage::open(temp_c.path().to_str().ok_or("invalid temp path")?)?;

    let secret_a = iroh::SecretKey::from_bytes(&[61u8; 32]);
    let secret_b = iroh::SecretKey::from_bytes(&[62u8; 32]);
    let secret_c = iroh::SecretKey::from_bytes(&[63u8; 32]);
    let node_a = secret_a.public();
    let node_b = secret_b.public();
    let node_c = secret_c.public();
    let realm_id = RealmId::from_bytes([31u8; 32]);
    let discovery_method = DiscoveryMethod::ordered(vec![DiscoveryMethod::DhtSigned {
        ttl: Duration::from_secs(30),
        refresh_after: Duration::from_secs(1),
    }]);

    let cfg = |secret_key: iroh::SecretKey, peer_nodes: Vec<NodeId>| NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        secret_key: Some(secret_key),
        realm_id,
        peer_nodes,
        discovery_method: discovery_method.clone(),
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(secret_a, vec![node_b, node_c]), storage_a).await?;
    let handle_b = NetHandle::new(cfg(secret_b, vec![node_a, node_c]), storage_b).await?;
    let handle_c = NetHandle::new(cfg(secret_c, vec![node_a, node_b]), storage_c).await?;

    let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    realm_config.ensure_node(node_a, RealmNodeKind::Management);
    realm_config.ensure_node(node_b, RealmNodeKind::Management);
    realm_config.ensure_node(node_c, RealmNodeKind::Management);
    handle_a
        .refresh_realm_peers_from_document(&realm_config)
        .await?;
    handle_b
        .refresh_realm_peers_from_document(&realm_config)
        .await?;
    handle_c
        .refresh_realm_peers_from_document(&realm_config)
        .await?;

    let (stream_tx, stream_rx) = mpsc::unbounded_channel();
    handle_b.set_inbound_handler(Arc::new(TestInboundHandler {
        stream_tx: Some(stream_tx),
    }));

    handle_a.add_peer_addr(handle_c.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_c.endpoint_addr()).await;
    handle_c.add_peer_addr(handle_b.endpoint_addr()).await;

    let mut opened_stream = None;
    for _ in 0..20 {
        if let Ok(stream) = handle_a.open_stream(node_b, Alpn::Bao).await {
            opened_stream = Some(stream);
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    assert!(
        opened_stream.is_some(),
        "expected DHT-signed fallback to resolve node_b"
    );
    let mut status = handle_a.get_status().await;
    for _ in 0..50 {
        if status.connections.iter().any(|peer| {
            peer.node_id == node_b
                && peer.status == PeerConnectionStatus::Connected
                && peer.active_addresses.iter().any(|address| {
                    address.status == ConnectionAddressStatus::Active
                        && !address.address.is_empty()
                        && !address.protocol_connections.is_empty()
                })
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        status = handle_a.get_status().await;
    }
    assert!(status.discovery_methods.contains(&"dht_signed".to_string()));
    assert!(status.requests.total > 0);
    assert!(status.connections.iter().any(|peer| {
        peer.node_id == node_b
            && peer.status == PeerConnectionStatus::Connected
            && peer.active_addresses.iter().any(|address| {
                address.status == ConnectionAddressStatus::Active
                    && !address.address.is_empty()
                    && !address.protocol_connections.is_empty()
            })
    }));

    drop(opened_stream);
    drop(stream_rx);
    tokio::join!(
        handle_a.shutdown(),
        handle_b.shutdown(),
        handle_c.shutdown()
    );

    Ok(())
}

#[tokio::test]
async fn test_multi_node_stream_send_recv() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    handle_b.set_inbound_handler(Arc::new(TestInboundHandler {
        stream_tx: Some(stream_tx),
    }));

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let mut stream = handle_a.open_stream(handle_b.node_id(), Alpn::Bao).await?;
    stream
        .0
        .write_all(b"hello stream")
        .await
        .map_err(|e| std::io::Error::other(format!("failed to write outbound stream data: {e}")))?;

    let (incoming_alpn, mut incoming_stream, _node_id) =
        tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
            .await?
            .ok_or("expected inbound stream")?;
    assert_eq!(incoming_alpn, Alpn::Bao);

    let chunk = incoming_stream
        .1
        .read_chunk(1024)
        .await
        .map_err(|e| std::io::Error::other(format!("failed to read inbound stream data: {e}")))?
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "inbound stream closed without data",
            )
        })?;
    assert_eq!(chunk.to_vec(), b"hello stream".to_vec());

    handle_a.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_open_stream_rejects_internal_alpn() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let error = handle_a
        .open_stream(handle_b.node_id(), Alpn::Dht)
        .await
        .expect_err("DHT streams are internal to the DHT service");
    assert!(matches!(error, NetError::Stream(_)));

    handle_a.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}

#[test]
fn test_dht_key_id_hashing() {
    let key1 = DhtKeyId::from_data(b"test-key");
    let key2 = DhtKeyId::from_data(b"test-key");
    let key3 = DhtKeyId::from_data(b"different-key");

    assert_eq!(key1, key2);
    assert_eq!(key1.as_bytes(), key2.as_bytes());
    assert_ne!(key1, key3);
    assert!(key1.as_bytes().iter().any(|&b| b != 0));

    let bytes = *key1.as_bytes();
    let key4 = DhtKeyId::from_bytes(bytes);
    assert_eq!(key1, key4);
}

#[test]
fn test_topic_id_creation() {
    let realm_id = RealmId::from_bytes([3u8; 32]);
    let topic1 = TopicId::realm(realm_id);

    let bytes = topic1.to_bytes();
    let topic2 = TopicId::from_bytes(&bytes).expect("topic roundtrip");
    assert_eq!(topic1, topic2);

    let group_id = Ulid::new();
    let topic3 = TopicId::group(group_id);
    let topic4 = TopicId::group(group_id);
    let topic5 = TopicId::group(Ulid::new());

    assert_eq!(topic3, topic4);
    assert_ne!(topic3, topic5);
}
