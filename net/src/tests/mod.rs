use super::*;

pub(crate) mod fixtures;
use crate::discovery::{
    DHT_SIGNED_MAX_CLOCK_SKEW_SECS, select_signed_endpoint, validate_endpoint_announcement,
};
use crate::eviction::flush_evicted_documents;
use crate::test_support::make_secret;
use aruna_core::events::DhtEntry;
use aruna_core::structs::{
    ConnectionAddressStatus, PeerConnectionStatus, RealmEndpointAnnouncement,
    endpoint_signing_bytes,
};
use iroh::endpoint::presets;
use iroh::{Endpoint, EndpointAddr};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::{Duration, sleep};

fn make_announcement(
    secret: &iroh::SecretKey,
    realm_id: RealmId,
    endpoint_addr: EndpointAddr,
    issued_at: u64,
    expires_at: u64,
    sequence: u64,
) -> RealmEndpointAnnouncement {
    let node_id = secret.public();
    let signing_bytes = endpoint_signing_bytes(
        &realm_id,
        &node_id,
        &endpoint_addr,
        issued_at,
        expires_at,
        sequence,
    )
    .expect("signing bytes should encode");
    let signature = secret.sign(&signing_bytes);

    RealmEndpointAnnouncement {
        realm_id,
        node_id,
        endpoint_addr,
        issued_at,
        expires_at,
        sequence,
        signature,
    }
}

fn test_evicted_document(seed: u8) -> DocumentSyncEvictedDocument {
    let event_id = ulid::Ulid::from_parts(seed as u64, seed as u128);
    let change = aruna_core::document::DocumentSyncChange {
        base: None,
        current: aruna_core::document::DocumentSyncRevision {
            generation: 1,
            event_id,
            actor: make_secret(seed).public(),
            updated_at_ms: seed as u64,
        },
        kind: aruna_core::document::DocumentSyncChangeKind::Delete,
        placement: aruna_core::structs::PlacementRef::NIL,
    };

    DocumentSyncEvictedDocument {
        event_id,
        target: DocumentSyncTarget::RealmConfig {
            realm_id: RealmId::from_bytes([seed; 32]),
        },
        event: aruna_core::document::DocumentSyncOutboxEvent::Delete { change },
        placement: aruna_core::structs::PlacementRef::NIL,
        allow_genesis: false,
    }
}

#[derive(Debug)]
struct RecordingEvictedHandler {
    documents: tokio::sync::Mutex<Vec<DocumentSyncEvictedDocument>>,
    accepts: bool,
}

impl RecordingEvictedHandler {
    fn new(accepts: bool) -> Self {
        Self {
            documents: tokio::sync::Mutex::new(Vec::new()),
            accepts,
        }
    }
}

#[async_trait]
impl InboundEventHandler for RecordingEvictedHandler {
    async fn handle_incoming_stream(
        &self,
        _alpn: Alpn,
        _stream: streams::BiStream,
        _node_id: NodeId,
    ) {
    }

    async fn handle_evicted_documents(&self, documents: Vec<DocumentSyncEvictedDocument>) -> bool {
        self.documents.lock().await.extend(documents);
        self.accepts
    }
}

async fn eviction_test_service() -> (TempDir, Arc<DocumentSyncService>) {
    let dir = tempfile::tempdir().expect("eviction service tempdir");
    let storage =
        aruna_storage::FjallStorage::open(dir.path().join("storage").to_str().expect("utf-8 path"))
            .expect("storage opens");
    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(iroh::SecretKey::from_bytes(&[123u8; 32]))
        .relay_mode(iroh::RelayMode::Disabled)
        .alpns(vec![Alpn::DocumentSync.as_bytes().to_vec()])
        .bind_addr(
            "127.0.0.1:0"
                .parse::<std::net::SocketAddr>()
                .expect("valid bind addr"),
        )
        .expect("bind addr configures")
        .bind()
        .await
        .expect("endpoint binds");
    let service = DocumentSyncService::open_with_policy(
        endpoint,
        storage,
        dir.path().join("document-sync"),
        &[],
        vec![Alpn::DocumentSync.as_bytes().to_vec()],
        ::irokle::net::IrohRuntimeConfig::default(),
        aruna_storage::FjallPersistPolicy::Buffer,
        RealmId::from_bytes([123u8; 32]),
    )
    .expect("document sync service opens");
    (dir, Arc::new(service))
}

#[tokio::test]
async fn eviction_waits_handler() {
    // A journal entry survives a missing handler and a handler that cannot
    // commit; only a durable conversion releases it.
    let (_dir, service) = eviction_test_service().await;
    let inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>> =
        Arc::new(RwLock::new(None));
    let entry = PendingEviction {
        key: ::irokle::EvictionKey::from_bytes([17; 32]),
        documents: vec![test_evicted_document(11), test_evicted_document(12)],
    };
    let mut pending = vec![entry.clone()];

    assert!(
        !flush_evicted_documents(&inbound_handler, &service, &mut pending).await,
        "payloads must stay journalled without a handler"
    );
    assert_eq!(pending, vec![entry.clone()]);

    let failing = Arc::new(RecordingEvictedHandler::new(false));
    *inbound_handler.write() = Some(failing.clone() as Arc<dyn InboundEventHandler>);
    assert!(
        !flush_evicted_documents(&inbound_handler, &service, &mut pending).await,
        "a handler that cannot commit must not release the entry"
    );
    assert_eq!(pending, vec![entry.clone()]);
    assert_eq!(*failing.documents.lock().await, entry.documents);

    let handler = Arc::new(RecordingEvictedHandler::new(true));
    *inbound_handler.write() = Some(handler.clone() as Arc<dyn InboundEventHandler>);
    assert!(flush_evicted_documents(&inbound_handler, &service, &mut pending).await);
    assert!(pending.is_empty());
    assert_eq!(*handler.documents.lock().await, entry.documents);

    service.shutdown().await;
}

/// A tie-break eviction of one locally authored delete on `placement`.
fn local_eviction(
    service: &DocumentSyncService,
    placement: aruna_core::structs::PlacementRef,
    evicted: bool,
) -> ::irokle::TopicEviction {
    let event_id = ulid::Ulid::from_bytes([31; 16]);
    let event = aruna_core::document::DocumentSyncEvent::Delete {
        event_id,
        target: DocumentSyncTarget::MetadataDocumentLifecycle {
            document_id: event_id,
        },
        change: aruna_core::document::DocumentSyncChange {
            base: None,
            current: aruna_core::document::DocumentSyncRevision {
                generation: 0,
                event_id,
                actor: make_secret(31).public(),
                updated_at_ms: 1,
            },
            kind: aruna_core::document::DocumentSyncChangeKind::Delete,
            placement,
        },
    };
    let topic_id = ::irokle::TopicId::from_bytes([31; 32]);
    let author = service.node().peer_id();
    ::irokle::TopicEviction {
        topic_id,
        losing_genesis: ::irokle::OpId::from_bytes([32; 32]),
        winning_genesis: ::irokle::OpId::from_bytes([33; 32]),
        evicted: evicted
            .then(|| ::irokle::EvictedOp {
                op_id: ::irokle::OpId::from_bytes([34; 32]),
                actor_id: ::irokle::actor_id_for(topic_id, author),
                author,
                actor_seq: 2,
                payload: ::irokle::TopicPayload::Event(
                    ::irokle::EventEnvelope::encode_event(&event).expect("the event encodes"),
                ),
            })
            .into_iter()
            .collect(),
    }
}

#[tokio::test]
async fn eviction_registers_buckets() {
    // The entry outlives the hand-off: it is released only once the
    // replacement rows are durable, and it never stalls another bucket.
    let (_dir, service) = eviction_test_service().await;
    let placement = aruna_core::structs::PlacementRef {
        strategy_id: ulid::Ulid::from_bytes([31; 16]),
        shard: 1,
    };
    let elsewhere = aruna_core::structs::PlacementRef {
        strategy_id: placement.strategy_id,
        shard: 2,
    };
    let entry = service
        .consume_eviction(local_eviction(&service, placement, true))
        .await
        .expect("a journalled eviction is pending");
    assert_eq!(entry.documents.len(), 1);
    assert!(service.eviction_pending(&placement));
    assert!(!service.eviction_pending(&elsewhere));

    let inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>> =
        Arc::new(RwLock::new(None));
    let failing = Arc::new(RecordingEvictedHandler::new(false));
    *inbound_handler.write() = Some(failing as Arc<dyn InboundEventHandler>);
    let mut pending = vec![entry];
    assert!(!flush_evicted_documents(&inbound_handler, &service, &mut pending).await);
    assert!(
        service.eviction_pending(&placement),
        "an unconverted entry must keep blocking its bucket"
    );

    let handler = Arc::new(RecordingEvictedHandler::new(true));
    *inbound_handler.write() = Some(handler as Arc<dyn InboundEventHandler>);
    assert!(flush_evicted_documents(&inbound_handler, &service, &mut pending).await);
    assert!(!service.eviction_pending(&placement));

    service.shutdown().await;
}

#[tokio::test]
async fn empty_eviction_unpending() {
    // Irokle writes no record for an eviction with no payloads, so treating
    // one as pending would arm the retry timer against a phantom forever.
    let (_dir, service) = eviction_test_service().await;
    let placement = aruna_core::structs::PlacementRef::NIL;
    assert!(
        service
            .consume_eviction(local_eviction(&service, placement, false))
            .await
            .is_none()
    );
    assert!(!service.eviction_pending(&placement));

    service.shutdown().await;
}

#[test]
fn ordered_discovery() {
    let method = DiscoveryMethod::ordered(vec![
        DiscoveryMethod::N0Dns,
        DiscoveryMethod::DhtSigned {
            ttl: Duration::from_secs(300),
            refresh_after: Duration::from_secs(60),
        },
    ]);

    assert_eq!(
        method.enabled_methods(),
        vec!["n0_dns".to_string(), "dht_signed".to_string()]
    );
    assert_eq!(
        method.dht_signed_config(),
        Some((Duration::from_secs(300), Duration::from_secs(60)))
    );
}

#[test]
fn relay_additions() {
    let relays = vec![
        "https://relay-a.example".to_string(),
        "https://relay-a.example".to_string(),
        "https://relay-b.example".to_string(),
    ];

    let relay_method = RelayMethod::N0.with_additional_relays(relays);

    assert_eq!(relay_method.method_name(), "n0+custom");
    assert_eq!(
        relay_method.relay_urls(),
        vec![
            "https://relay-a.example".to_string(),
            "https://relay-b.example".to_string(),
        ]
    );
}

#[test]
fn signed_announcement() {
    let secret = make_secret(51);
    let node_id = secret.public();
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let endpoint_addr = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:12345".parse().expect("valid socket addr"));
    let now = 1_000;
    let announcement = make_announcement(&secret, realm_id, endpoint_addr, now, now + 300, 1);

    assert!(
        validate_endpoint_announcement(
            &announcement,
            node_id,
            node_id,
            realm_id,
            &[node_id],
            now,
            300,
        )
        .is_ok()
    );
    assert!(
        validate_endpoint_announcement(&announcement, node_id, node_id, realm_id, &[], now, 300,)
            .is_err()
    );

    let forged_secret = make_secret(52);
    let signing_bytes = endpoint_signing_bytes(
        &announcement.realm_id,
        &announcement.node_id,
        &announcement.endpoint_addr,
        announcement.issued_at,
        announcement.expires_at,
        announcement.sequence,
    )
    .expect("signing bytes should encode");
    let mut forged = announcement.clone();
    forged.signature = forged_secret.sign(&signing_bytes);

    assert!(
        validate_endpoint_announcement(&forged, node_id, node_id, realm_id, &[node_id], now, 300,)
            .is_err()
    );
}

#[test]
fn latest_announcement() {
    let secret = make_secret(53);
    let node_id = secret.public();
    let realm_id = RealmId::from_bytes([10u8; 32]);
    let older = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10001".parse().expect("valid socket addr"));
    let newer = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10002".parse().expect("valid socket addr"));
    let now = 1_000;
    let older = make_announcement(&secret, realm_id, older, now, now + 300, 1);
    let newer = make_announcement(&secret, realm_id, newer, now, now + 300, 2);
    let entries = vec![
        DhtEntry {
            node_id,
            realm_id,
            value: postcard::to_allocvec(&older).expect("encode announcement"),
            expires_at: now + 300,
        },
        DhtEntry {
            node_id,
            realm_id,
            value: postcard::to_allocvec(&newer).expect("encode announcement"),
            expires_at: now + 300,
        },
    ];

    let endpoint = select_signed_endpoint(entries, node_id, realm_id, &[node_id], now, 300)
        .expect("valid announcement");

    assert_eq!(endpoint.addrs, newer.endpoint_addr.addrs);
}

#[test]
fn new_announcement_wins() {
    let secret = make_secret(55);
    let node_id = secret.public();
    let realm_id = RealmId::from_bytes([12u8; 32]);
    let stale = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10010".parse().expect("valid socket addr"));
    let fresh = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10011".parse().expect("valid socket addr"));
    let now = 1_000;
    let stale = make_announcement(&secret, realm_id, stale, now, now + 300, 10);
    let fresh = make_announcement(&secret, realm_id, fresh, now + 10, now + 310, 1);
    let entries = vec![
        DhtEntry {
            node_id,
            realm_id,
            value: postcard::to_allocvec(&stale).expect("encode announcement"),
            expires_at: now + 300,
        },
        DhtEntry {
            node_id,
            realm_id,
            value: postcard::to_allocvec(&fresh).expect("encode announcement"),
            expires_at: now + 310,
        },
    ];

    let endpoint = select_signed_endpoint(entries, node_id, realm_id, &[node_id], now + 10, 300)
        .expect("valid announcement");

    assert_eq!(endpoint.addrs, fresh.endpoint_addr.addrs);
}

#[test]
fn expired_announcement() {
    let secret = make_secret(54);
    let node_id = secret.public();
    let realm_id = RealmId::from_bytes([11u8; 32]);
    let endpoint_addr = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10003".parse().expect("valid socket addr"));
    let announcement = make_announcement(&secret, realm_id, endpoint_addr, 500, 900, 1);
    let entries = vec![DhtEntry {
        node_id,
        realm_id,
        value: postcard::to_allocvec(&announcement).expect("encode announcement"),
        expires_at: 900,
    }];

    assert!(select_signed_endpoint(entries, node_id, realm_id, &[node_id], 1_000, 300).is_none());
}

#[test]
fn invalid_announcement_timing() {
    let secret = make_secret(56);
    let node_id = secret.public();
    let realm_id = RealmId::from_bytes([13u8; 32]);
    let endpoint_addr = EndpointAddr::new(node_id)
        .with_ip_addr("127.0.0.1:10012".parse().expect("valid socket addr"));
    let now = 1_000;
    let future = make_announcement(
        &secret,
        realm_id,
        endpoint_addr.clone(),
        now + DHT_SIGNED_MAX_CLOCK_SKEW_SECS + 1,
        now + DHT_SIGNED_MAX_CLOCK_SKEW_SECS + 301,
        1,
    );
    let excessive_ttl = make_announcement(&secret, realm_id, endpoint_addr, now, now + 301, 1);

    assert!(
        validate_endpoint_announcement(&future, node_id, node_id, realm_id, &[node_id], now, 300,)
            .is_err()
    );
    assert!(
        validate_endpoint_announcement(
            &excessive_ttl,
            node_id,
            node_id,
            realm_id,
            &[node_id],
            now,
            300,
        )
        .is_err()
    );
}

#[tokio::test]
async fn net_handle_creates() -> Result<()> {
    let temp_dir = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let storage = aruna_storage::FjallStorage::open(
        temp_dir
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;

    let config = NetConfig {
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };

    let handle = NetHandle::new(config, storage).await?;
    assert_ne!(handle.node_id().as_bytes(), &[0u8; 32]);
    let status = handle.get_status().await;
    assert!(status.discovery_methods.is_empty());
    assert_eq!(status.relay_method, "none");
    assert!(!status.endpoint_addr.addrs.is_empty());
    assert!(
        status
            .endpoint_addr
            .ip_addrs()
            .all(|socket| !socket.ip().is_unspecified())
    );
    handle.shutdown().await;
    Ok(())
}

async fn notification_wake_handle() -> Result<(TempDir, NetHandle)> {
    let temp_dir = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let storage = aruna_storage::FjallStorage::open(
        temp_dir
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;
    let config = NetConfig {
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };
    let handle = NetHandle::new(config, storage).await?;
    Ok((temp_dir, handle))
}

fn wake_user(seed: u8) -> UserId {
    UserId::new(
        ulid::Ulid::from_bytes([seed; 16]),
        RealmId::from_bytes([seed; 32]),
    )
}

#[tokio::test]
async fn wake_without_subscriber() -> Result<()> {
    let (_dir, handle) = notification_wake_handle().await?;
    // No subscribers: the send must not panic or error.
    handle.notify_inbox_activity(wake_user(1));
    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn wake_reaches_subscriber() -> Result<()> {
    let (_dir, handle) = notification_wake_handle().await?;
    let mut rx = handle.subscribe_notification_wakes();
    let recipient = wake_user(7);
    handle.notify_inbox_activity(recipient);
    let woken = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("wake arrives")
        .expect("channel open");
    assert_eq!(woken, recipient);
    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn dashboard_wake_arrives() -> Result<()> {
    let (_dir, handle) = notification_wake_handle().await?;
    let mut changes = handle.subscribe_dashboard_changes();
    handle.notify_dashboard_change();
    changes.changed().await.expect("channel open");
    assert_eq!(*changes.borrow_and_update(), 1);
    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn endpoint_only_unauthorized() -> Result<()> {
    let temp_a = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let temp_b = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let storage_a = aruna_storage::FjallStorage::open(
        temp_a
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;
    let storage_b = aruna_storage::FjallStorage::open(
        temp_b
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;
    let peer = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_b,
    )
    .await?;
    let peer_id = peer.node_id();
    let handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peer_endpoints: vec![peer.endpoint_addr()],
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_a,
    )
    .await?;

    assert!(
        !handle
            .inner
            .dht_signed_authorized_nodes
            .read()
            .contains(&peer_id)
    );

    handle.shutdown().await;
    peer.shutdown().await;
    Ok(())
}

struct HoldingInboundHandler {
    streams: tokio::sync::Mutex<Vec<streams::BiStream>>,
}

#[async_trait]
impl InboundEventHandler for HoldingInboundHandler {
    async fn handle_incoming_stream(
        &self,
        _alpn: Alpn,
        stream: streams::BiStream,
        _node_id: NodeId,
    ) {
        self.streams.lock().await.push(stream);
    }
}

async fn test_net_handle() -> Result<(NetHandle, TempDir)> {
    let temp_dir = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let storage = aruna_storage::FjallStorage::open(
        temp_dir
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;
    let handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage,
    )
    .await?;
    handle.set_inbound_handler(Arc::new(HoldingInboundHandler {
        streams: tokio::sync::Mutex::new(Vec::new()),
    }));

    Ok((handle, temp_dir))
}

const WAIT_STATUS_TIMEOUT: Duration = Duration::from_secs(30);

async fn wait_for_status(handle: &NetHandle, mut done: impl FnMut(&NetState) -> bool) -> NetState {
    let deadline = tokio::time::Instant::now() + WAIT_STATUS_TIMEOUT;
    loop {
        let status = handle.get_status().await;
        if done(&status) || tokio::time::Instant::now() >= deadline {
            return status;
        }
        sleep(Duration::from_millis(20)).await;
    }
}

async fn await_open_connections(handle: &NetHandle, expected: usize) -> NetState {
    wait_for_status(handle, |status| {
        protocol_connection_count(status) >= expected
    })
    .await
}

async fn await_bootstrap_failure(handle: &NetHandle) -> NetState {
    wait_for_status(handle, |status| status.requests.failures > 0).await
}

async fn await_peer_failure(handle: &NetHandle, peer: NodeId) -> NetState {
    wait_for_status(handle, |status| {
        status
            .connections
            .iter()
            .any(|state| state.node_id == peer && state.status == PeerConnectionStatus::Unreachable)
    })
    .await
}

fn protocol_connection_count(status: &NetState) -> usize {
    status
        .connections
        .iter()
        .flat_map(|peer| &peer.active_addresses)
        .map(|address| address.protocol_connections.len())
        .sum()
}

fn connection_count_for(
    status: &NetState,
    node_id: NodeId,
    alpn: Alpn,
    side: iroh::endpoint::Side,
) -> usize {
    status
        .connections
        .iter()
        .filter(|peer| peer.node_id == node_id)
        .flat_map(|peer| &peer.active_addresses)
        .flat_map(|address| &address.protocol_connections)
        .filter(|connection| connection.alpn == Some(alpn) && connection.side == side)
        .count()
}

async fn await_protocol_count(
    handle: &NetHandle,
    node_id: NodeId,
    alpn: Alpn,
    side: iroh::endpoint::Side,
    expected: usize,
) -> NetState {
    wait_for_status(handle, |status| {
        connection_count_for(status, node_id, alpn, side) == expected
    })
    .await
}

#[tokio::test]
async fn config_replaces_peers() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;
    let peer_a = make_secret(11).public();
    let peer_b = make_secret(12).public();
    let mut document = RealmConfigDocument::default_for_realm(*handle.realm_id(), Vec::new());
    document.ensure_node(
        handle.node_id(),
        aruna_core::structs::RealmNodeKind::Management,
    );
    document.ensure_node(peer_b, aruna_core::structs::RealmNodeKind::Server);
    document.ensure_node(peer_a, aruna_core::structs::RealmNodeKind::Server);
    // A User-kind node must never enter the sync fan-out set.
    let user_node = make_secret(13).public();
    document.ensure_node(
        user_node,
        aruna_core::structs::RealmNodeKind::User {
            owner: aruna_core::UserId::nil(*handle.realm_id()),
        },
    );
    let expected = unique_peer_nodes(vec![peer_a, peer_b], handle.node_id());

    let peers = handle.refresh_document_peers(&document).await?;
    assert_eq!(peers, expected);
    assert_eq!(handle.realm_peers().await, expected);
    assert_eq!(*handle.inner.dht_signed_authorized_nodes.read(), expected);
    // The same refresh that admits a node publishes its kind, so an
    // admitted peer is never briefly ungated.
    assert!(
        handle
            .inner
            .inbound_admission
            .peer_kinds()
            .read()
            .contains_key(&user_node)
    );

    let mut replacement = RealmConfigDocument::default_for_realm(*handle.realm_id(), Vec::new());
    replacement.ensure_node(peer_b, aruna_core::structs::RealmNodeKind::Server);

    let peers = handle.refresh_document_peers(&replacement).await?;
    assert_eq!(peers, vec![peer_b]);
    assert_eq!(handle.realm_peers().await, vec![peer_b]);
    assert_eq!(
        *handle.inner.dht_signed_authorized_nodes.read(),
        vec![peer_b]
    );

    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn refuses_predecessor_alpn() -> Result<()> {
    // An old peer must fail negotiation. It may never reach a handler and
    // decode current frames as if they were its own.
    let (accepting, _dir) = test_net_handle().await?;
    let dialer = Endpoint::builder(presets::Minimal)
        .secret_key(iroh::SecretKey::from_bytes(&[77u8; 32]))
        .relay_mode(iroh::RelayMode::Disabled)
        .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
        .expect("bind addr configures")
        .bind()
        .await
        .expect("dialer endpoint binds");

    assert!(
        dialer
            .connect(accepting.endpoint_addr(), b"aruna/bao/1")
            .await
            .is_err()
    );
    assert!(
        dialer
            .connect(accepting.endpoint_addr(), Alpn::Bao.as_bytes())
            .await
            .is_ok()
    );

    accepting.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn outbound_reuses_connection() -> Result<()> {
    let (a, _a_dir) = test_net_handle().await?;
    let (b, _b_dir) = test_net_handle().await?;
    a.add_peer_addr(b.endpoint_addr()).await;
    b.add_peer_addr(a.endpoint_addr()).await;

    let _first = a.open_stream(b.node_id(), Alpn::Bao).await?;
    let _second = a.open_stream(b.node_id(), Alpn::Bao).await?;

    let status =
        await_protocol_count(&a, b.node_id(), Alpn::Bao, iroh::endpoint::Side::Client, 1).await;

    assert_eq!(
        connection_count_for(
            &status,
            b.node_id(),
            Alpn::Bao,
            iroh::endpoint::Side::Client,
        ),
        1
    );

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn monitor_tracks_connections() -> Result<()> {
    let (a, _a_dir) = test_net_handle().await?;
    let (b, _b_dir) = test_net_handle().await?;
    a.add_peer_addr(b.endpoint_addr()).await;
    b.add_peer_addr(a.endpoint_addr()).await;

    let _a_to_b = a.open_stream(b.node_id(), Alpn::Bao).await?;
    let _b_to_a = b.open_stream(a.node_id(), Alpn::Bao).await?;

    let status = await_open_connections(&a, 2).await;
    assert!(protocol_connection_count(&status) >= 2);

    let mut connection_ids = status
        .connections
        .iter()
        .flat_map(|peer| &peer.active_addresses)
        .flat_map(|address| &address.protocol_connections)
        .map(|connection| connection.connection_id)
        .collect::<Vec<_>>();
    connection_ids.sort_unstable();
    connection_ids.dedup();
    assert_eq!(connection_ids.len(), protocol_connection_count(&status));

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn peer_addresses() -> Result<()> {
    let (a, _a_dir) = test_net_handle().await?;
    let (b, _b_dir) = test_net_handle().await?;

    a.add_peer_addr(b.endpoint_addr()).await;
    let _stream = a.open_stream(b.node_id(), Alpn::Bao).await?;

    let status = await_open_connections(&a, 1).await;
    assert!(status.connections.iter().any(|peer| {
        peer.node_id == b.node_id()
            && peer.status == PeerConnectionStatus::Connected
            && peer.active_addresses.iter().any(|address| {
                address.status == ConnectionAddressStatus::Active
                    && !address.address.is_empty()
                    && !address.protocol_connections.is_empty()
            })
    }));

    a.shutdown().await;
    b.shutdown().await;
    Ok(())
}

// An inbound handler that outlives the drain deadline must not stop the
// teardown behind it: the endpoint closes and the children are joined.
#[tokio::test]
async fn teardown_after_drain() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;
    handle
        .inner
        .inbound_tasks
        .spawn(std::future::pending::<()>());

    let complete = handle.shutdown_with_drain(Duration::from_millis(100)).await;

    assert!(!complete.complete());
    assert_eq!(complete.inbound_pending_at_deadline, 1);
    assert_eq!(complete.inbound_pending, 1);
    assert!(handle.inner.shutdown.is_cancelled());
    assert!(handle.inner.accept_shutdown.is_cancelled());
    assert!(handle.inner.tasks.lock().await.is_empty());
    Ok(())
}

// Admission closes on its own, so a net phase left with no drain budget
// still stops accepting inbound streams.
#[tokio::test]
async fn close_stops_accept() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;

    handle.close_admission();

    assert!(handle.inner.accept_shutdown.is_cancelled());
    assert!(handle.inner.inbound_tasks.is_closed());
    assert!(!handle.inner.shutdown.is_cancelled());
    handle.shutdown().await;
    Ok(())
}

// An interrupted shutdown must not detach the child it was joining: the owner
// keeps the handle, a resumed call joins it, and its completion runs once.
#[tokio::test]
async fn interrupted_shutdown_resumes_and_releases_children_once() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    let (handle, _dir) = test_net_handle().await?;
    let release = Arc::new(Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let child_release = release.clone();
    let child_completed = completed.clone();
    handle
        .inner
        .tasks
        .lock()
        .await
        .push(tokio::spawn(async move {
            let _ = child_release.acquire().await;
            child_completed.fetch_add(1, Ordering::SeqCst);
        }));

    let interrupted = tokio::time::timeout(
        Duration::from_millis(50),
        handle.shutdown_with_drain(Duration::from_millis(10)),
    )
    .await;
    assert!(
        interrupted.is_err(),
        "the controlled child must keep the first shutdown active"
    );
    assert_eq!(completed.load(Ordering::SeqCst), 0);

    release.add_permits(1);
    let complete = handle.shutdown_with_drain(Duration::from_millis(10)).await;

    assert!(
        complete.complete(),
        "the resumed shutdown joins the released child"
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert!(handle.inner.tasks.lock().await.is_empty());
    Ok(())
}

// Repeated shutdown after the first returned incomplete still makes progress:
// the retained owners are joined by the later call instead of being abandoned.
#[tokio::test]
async fn repeated_shutdown_joins_retained_children() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::oneshot;

    let (handle, _dir) = test_net_handle().await?;
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let stopped = Arc::new(AtomicBool::new(false));
    let child_stopped = stopped.clone();
    handle.inner.inbound_tasks.spawn(async move {
        let _ = release_rx.await;
        child_stopped.store(true, Ordering::SeqCst);
    });

    let first = handle.shutdown_with_drain(Duration::from_millis(10)).await;
    assert!(
        !first.complete(),
        "the blocked child outlives the first forced drain"
    );
    assert!(!stopped.load(Ordering::SeqCst));

    release_tx.send(()).expect("the child still waits");
    let second = handle.shutdown_with_drain(Duration::from_millis(100)).await;
    assert!(second.complete(), "the later call joins the released child");
    assert!(stopped.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn peer_warnings() -> Result<()> {
    let temp_dir = tempfile::tempdir().map_err(|e| NetError::Io(e.to_string()))?;
    let storage = aruna_storage::FjallStorage::open(
        temp_dir
            .path()
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?,
    )
    .map_err(|e| NetError::Io(e.to_string()))?;
    let missing_peer = iroh::SecretKey::from_bytes(&[99u8; 32]).public();
    let handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peer_nodes: vec![missing_peer],
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage,
    )
    .await?;

    let status = await_bootstrap_failure(&handle).await;
    assert!(status.requests.failures > 0);
    assert!(
        status
            .connections
            .iter()
            .any(|peer| peer.node_id == missing_peer && peer.active_addresses.is_empty())
    );
    assert!(
        status
            .warnings
            .iter()
            .any(|warning| warning.contains("managed peer") && warning.contains("unreachable"))
    );
    assert!(status.connections.iter().any(
        |peer| peer.node_id == missing_peer && peer.status == PeerConnectionStatus::Unreachable
    ));

    handle.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn failed_target_backoff() -> Result<()> {
    let (handle, _dir) = test_net_handle().await?;
    let missing_peer = iroh::SecretKey::from_bytes(&[77u8; 32]).public();

    let err = handle
        .open_stream(missing_peer, Alpn::Bao)
        .await
        .expect_err("missing peer should fail to connect");
    let status = await_peer_failure(&handle, missing_peer).await;

    assert!(matches!(err, NetError::Connection(_)));
    assert!(status.connections.iter().any(|peer| {
        peer.node_id == missing_peer
            && peer.status == PeerConnectionStatus::Unreachable
            && peer.last_error.is_some()
            && peer.next_retry_in_secs.is_some()
    }));
    assert!(
        status
            .warnings
            .iter()
            .any(|warning| { warning.contains("managed peer") && warning.contains("unreachable") })
    );

    handle.shutdown().await;
    Ok(())
}
