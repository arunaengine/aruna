use std::sync::Arc;
use std::time::Duration;

use aruna_core::TopicId;
use aruna_core::alpn::Alpn;
use aruna_core::automerge::AutomergeDocumentVariant;
use aruna_core::effects::{DhtEffect, Effect, GossipEffect, NetEffect, StorageEffect};
use aruna_core::events::{DhtEvent, Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::keys::automerge_document_holder_key;
use aruna_net::dht::rpc::{DhtRequest, DhtResponse, decode_response, encode_request};
use aruna_net::streams::BiStream;
use aruna_net::{InboundEventHandler, NetConfig, NetHandle};
use aruna_storage::FjallStorage;
use async_trait::async_trait;
use byteview::ByteView;
use tempfile::tempdir;
use tokio::sync::mpsc;
use ulid::Ulid;

#[derive(Clone, Default)]
struct TestInboundHandler {
    gossip_tx: Option<mpsc::UnboundedSender<(TopicId, NodeId, Vec<u8>)>>,
    stream_tx: Option<mpsc::UnboundedSender<(Alpn, BiStream, NodeId)>>,
}

#[async_trait]
impl InboundEventHandler for TestInboundHandler {
    async fn handle_gossip_message(&self, topic: TopicId, sender: NodeId, data: Vec<u8>) {
        if let Some(tx) = &self.gossip_tx {
            let _ = tx.send((topic, sender, data));
        }
    }

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
            start_after: None,
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
            start_after: next_start_after,
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
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let key = [42u8; 32];
    let value = b"replicated-value".to_vec();

    let mut put_succeeded = false;
    let mut last_put_event = None;
    for _ in 0..10 {
        let put = handle_a
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Put {
                key,
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
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Get { key })))
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
async fn test_multi_node_gossip_message_delivery() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    let (gossip_tx, mut gossip_rx) = mpsc::unbounded_channel();
    handle_b.set_inbound_handler(Arc::new(TestInboundHandler {
        gossip_tx: Some(gossip_tx),
        stream_tx: None,
    }));

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let topic = TopicId::realm(Ulid::new());

    let subscribe_a = handle_a
        .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
            topic: topic.clone(),
        })))
        .await;
    let subscribe_b = handle_b
        .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
            topic: topic.clone(),
        })))
        .await;

    assert!(
        matches!(
            subscribe_a,
            Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
        ),
        "unexpected subscribe_a event: {subscribe_a:?}"
    );
    assert!(
        matches!(
            subscribe_b,
            Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
        ),
        "unexpected subscribe_b event: {subscribe_b:?}"
    );

    let payload = b"hello gossip".to_vec();
    let mut got_message = false;

    tokio::time::sleep(Duration::from_millis(300)).await;
    for _ in 0..8 {
        let broadcast = handle_a
            .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Broadcast {
                topic: topic.clone(),
                message: payload.clone(),
            })))
            .await;
        assert!(matches!(
            broadcast,
            Event::Net(NetEvent::Gossip(GossipEvent::BroadcastComplete { .. }))
        ));

        let maybe = tokio::time::timeout(Duration::from_millis(1200), gossip_rx.recv()).await;
        if let Ok(Some((recv_topic, _, data))) = maybe
            && recv_topic == topic
            && data == payload
        {
            got_message = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    assert!(got_message, "expected gossip message on second node");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_automerge_topic_subscription_announces_document_holders() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let document = AutomergeDocumentVariant::Metadata {
        group_id: Ulid::new(),
        document_id: Ulid::new(),
    };
    let topic = document.topic_id();
    let holder_key = *automerge_document_holder_key(&document).as_bytes();

    let subscribe = handle_a
        .send_effect(Effect::Net(NetEffect::Gossip(GossipEffect::Subscribe {
            topic: topic.clone(),
        })))
        .await;
    assert!(matches!(
        subscribe,
        Event::Net(NetEvent::Gossip(GossipEvent::Subscribed { .. }))
    ));

    let mut found = false;
    for _ in 0..10 {
        let get = handle_b
            .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Get { key: holder_key })))
            .await;

        if let Event::Net(NetEvent::Dht(DhtEvent::GetResult { values, .. })) = get
            && values.iter().any(|entry| entry.node_id == handle_a.node_id())
        {
            found = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(found, "expected automerge holder DHT entry on second node");

    handle_a.shutdown().await;
    handle_b.shutdown().await;
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
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    let (stream_tx, mut stream_rx) = mpsc::unbounded_channel();
    handle_b.set_inbound_handler(Arc::new(TestInboundHandler {
        gossip_tx: None,
        stream_tx: Some(stream_tx),
    }));

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let (mut send_a, _recv_a) = handle_a.open_stream(handle_b.node_id(), Alpn::Bao).await?;
    send_a
        .write_all(b"hello stream")
        .await
        .map_err(|e| std::io::Error::other(format!("failed to write outbound stream data: {e}")))?;

    let (incoming_alpn, (_send_b, mut recv_b), _node_id) =
        tokio::time::timeout(Duration::from_secs(5), stream_rx.recv())
            .await?
            .ok_or("expected inbound stream")?;
    assert_eq!(incoming_alpn, Alpn::Bao);

    let chunk = recv_b
        .read_chunk(1024)
        .await
        .map_err(|e| std::io::Error::other(format!("failed to read inbound stream data: {e}")))?
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "inbound stream closed without data",
            )
        })?;
    assert_eq!(chunk.bytes.to_vec(), b"hello stream".to_vec());

    handle_a.shutdown().await;
    handle_b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_dht_rpc_ping_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let temp_a = tempdir()?;
    let temp_b = tempdir()?;
    let storage_a = FjallStorage::open(temp_a.path().to_str().ok_or("invalid temp path")?)?;
    let storage_b = FjallStorage::open(temp_b.path().to_str().ok_or("invalid temp path")?)?;

    let cfg = || NetConfig {
        bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
        use_dns_discovery: false,
        ..NetConfig::default()
    };

    let handle_a = NetHandle::new(cfg(), storage_a).await?;
    let handle_b = NetHandle::new(cfg(), storage_b).await?;

    handle_a.add_peer_addr(handle_b.endpoint_addr()).await;
    handle_b.add_peer_addr(handle_a.endpoint_addr()).await;

    let (mut send, mut recv) = handle_a.open_stream(handle_b.node_id(), Alpn::Dht).await?;

    let request = encode_request(&DhtRequest::Ping)?;
    let len = (request.len() as u32).to_be_bytes();
    send.write_all(&len).await?;
    send.write_all(&request).await?;
    send.finish()?;

    let mut response_len = [0u8; 4];
    recv.read_exact(&mut response_len).await?;
    let response_len = u32::from_be_bytes(response_len) as usize;

    let mut response = vec![0u8; response_len];
    recv.read_exact(&mut response).await?;
    let response = decode_response(&response)?;
    assert!(matches!(response, DhtResponse::Pong));

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
    let realm_id = Ulid::new();
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

    let iroh_topic = topic1.to_iroh_topic();
    assert!(iroh_topic.as_bytes().iter().any(|&b| b != 0));
}
