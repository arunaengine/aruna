#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unsafe_code)]

pub mod dht;
mod effect_handlers;
pub mod error;
pub mod gossip;
mod monitoring;
pub mod streams;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{DhtEntry, Event, NetError as CoreNetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::keys::realm_endpoint_key;
use aruna_core::structs::{
    ConnectionAddressState, ConnectionAddressStatus, ConnectionMonitorState, NetState,
    NetworkDiagnosticsState, PeerConnectionState, PeerConnectionStatus, ProtocolConnectionState,
    RealmEndpointAnnouncement, RealmId, realm_endpoint_announcement_signing_bytes,
};
use aruna_core::util::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use crossfire::TrySendError;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};
use iroh::endpoint::{QuicTransportConfig, TransportAddrUsage, VarInt, presets};
use iroh::{Endpoint, EndpointAddr, RelayMap, RelayMode, TransportAddr};
use parking_lot::RwLock;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, warn};

pub use dht::DhtHandle;
pub use error::{NetError, Result};
pub use gossip::GossipService;
pub use monitoring::Monitor;

const DHT_SIGNED_MAX_CLOCK_SKEW_SECS: u64 = 300;
pub use streams::StreamsService;

#[derive(Clone)]
pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub realm_id: RealmId,
    pub peer_nodes: Vec<NodeId>,
    pub peer_endpoints: Vec<EndpointAddr>,
    pub temporary_bootstrap_active: bool,
    pub discovery_method: DiscoveryMethod,
    pub relay_method: RelayMethod,
    pub max_concurrent_uni_streams: Option<u64>,
    pub max_concurrent_bidi_streams: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryMethod {
    None,
    N0Dns,
    CustomDns(Vec<String>),
    DhtSigned {
        ttl: Duration,
        refresh_after: Duration,
    },
    Ordered(Vec<DiscoveryMethod>),
}

impl DiscoveryMethod {
    pub fn ordered(methods: Vec<Self>) -> Self {
        let mut flattened = Vec::new();
        for method in methods {
            match method {
                Self::None => {}
                Self::Ordered(methods) => flattened.extend(methods),
                other => flattened.push(other),
            }
        }

        match flattened.len() {
            0 => Self::None,
            1 => flattened.remove(0),
            _ => Self::Ordered(flattened),
        }
    }

    pub fn enabled_methods(&self) -> Vec<String> {
        self.leaf_methods()
            .into_iter()
            .filter_map(|method| match method {
                Self::None | Self::Ordered(_) => None,
                Self::N0Dns => Some("n0_dns".to_string()),
                Self::CustomDns(_) => Some("custom_dns".to_string()),
                Self::DhtSigned { .. } => Some("dht_signed".to_string()),
            })
            .collect()
    }

    pub fn dns_origins(&self) -> Vec<String> {
        let mut origins = Vec::new();
        for method in self.leaf_methods() {
            if let Self::CustomDns(method_origins) = method {
                origins.extend(method_origins.clone());
            }
        }
        origins
    }

    fn leaf_methods(&self) -> Vec<&DiscoveryMethod> {
        let mut methods = Vec::new();
        self.append_leaf_methods(&mut methods);
        methods
    }

    fn append_leaf_methods<'a>(&'a self, methods: &mut Vec<&'a DiscoveryMethod>) {
        match self {
            Self::Ordered(ordered) => {
                for method in ordered {
                    method.append_leaf_methods(methods);
                }
            }
            method => methods.push(method),
        }
    }

    fn dht_signed_config(&self) -> Option<(Duration, Duration)> {
        self.leaf_methods()
            .into_iter()
            .find_map(|method| match method {
                Self::DhtSigned { ttl, refresh_after } => Some((*ttl, *refresh_after)),
                _ => None,
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayMethod {
    None,
    N0,
    Custom(Vec<String>),
    N0WithCustom(Vec<String>),
}

impl RelayMethod {
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::N0 => "n0",
            Self::Custom(_) => "custom",
            Self::N0WithCustom(_) => "n0+custom",
        }
    }

    pub fn relay_urls(&self) -> Vec<String> {
        match self {
            Self::Custom(relays) | Self::N0WithCustom(relays) => relays.clone(),
            _ => Vec::new(),
        }
    }

    pub fn with_additional_relays(self, additional: Vec<String>) -> Self {
        if additional.is_empty() {
            return self;
        }

        match self {
            Self::None => Self::Custom(unique_relay_urls(additional)),
            Self::N0 => Self::N0WithCustom(unique_relay_urls(additional)),
            Self::Custom(relays) => Self::Custom(merge_relay_urls(relays, additional)),
            Self::N0WithCustom(relays) => Self::N0WithCustom(merge_relay_urls(relays, additional)),
        }
    }
}

fn merge_relay_urls(mut relays: Vec<String>, additional: Vec<String>) -> Vec<String> {
    relays.extend(additional);
    unique_relay_urls(relays)
}

fn unique_relay_urls(relays: Vec<String>) -> Vec<String> {
    let mut unique = Vec::new();
    for relay in relays {
        let relay = relay.trim();
        if relay.is_empty() || unique.iter().any(|existing| existing == relay) {
            continue;
        }
        unique.push(relay.to_string());
    }
    unique
}

pub fn endpoint_addr_to_config_string(endpoint_addr: &EndpointAddr) -> String {
    let mut parts = Vec::with_capacity(endpoint_addr.addrs.len() + 1);
    parts.push(endpoint_addr.id.to_string());
    parts.extend(endpoint_addr.addrs.iter().map(|addr| match addr {
        TransportAddr::Relay(url) => format!("relay:{url}"),
        TransportAddr::Ip(addr) => format!("ip:{addr}"),
        _ => format!("{addr}"),
    }));
    parts.join(";")
}

pub fn endpoint_addr_from_config_string(value: &str) -> std::result::Result<EndpointAddr, String> {
    let mut parts = value
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty());
    let node_id = parts
        .next()
        .ok_or_else(|| "missing endpoint id".to_string())?
        .parse::<iroh::PublicKey>()
        .map_err(|error| error.to_string())?;
    let mut addrs = Vec::new();
    for part in parts {
        if let Some(value) = part.strip_prefix("relay:") {
            addrs.push(TransportAddr::Relay(
                value
                    .parse::<iroh::RelayUrl>()
                    .map_err(|error| error.to_string())?,
            ));
        } else if let Some(value) = part.strip_prefix("ip:") {
            addrs.push(TransportAddr::Ip(
                SocketAddr::from_str(value).map_err(|error| error.to_string())?,
            ));
        } else if part.starts_with("http://") || part.starts_with("https://") {
            addrs.push(TransportAddr::Relay(
                part.parse::<iroh::RelayUrl>()
                    .map_err(|error| error.to_string())?,
            ));
        } else {
            addrs.push(TransportAddr::Ip(
                SocketAddr::from_str(part).map_err(|error| error.to_string())?,
            ));
        }
    }
    Ok(EndpointAddr::from_parts(node_id, addrs))
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            secret_key: None,
            realm_id: RealmId::from_bytes([0u8; 32]),
            peer_nodes: vec![],
            peer_endpoints: vec![],
            temporary_bootstrap_active: false,
            discovery_method: DiscoveryMethod::N0Dns,
            relay_method: RelayMethod::N0,
            max_concurrent_bidi_streams: None,
            max_concurrent_uni_streams: None,
        }
    }
}

impl std::fmt::Debug for NetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetConfig")
            .field("bind_addr", &self.bind_addr)
            .field("has_secret_key", &self.secret_key.is_some())
            .field("realm_id", &self.realm_id)
            .field("peer_nodes", &self.peer_nodes.len())
            .field("peer_endpoints", &self.peer_endpoints.len())
            .field(
                "temporary_bootstrap_active",
                &self.temporary_bootstrap_active,
            )
            .field("discovery_method", &self.discovery_method)
            .field("relay_method", &self.relay_method)
            .finish()
    }
}

type EffectHandle = (NetEffect, oneshot::Sender<NetEvent>, Span);

const PEER_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(5);
const PEER_MAX_RETRY_DELAY: Duration = Duration::from_secs(300);
const PEER_SUCCESS_REFRESH_DELAY: Duration = Duration::from_secs(300);
const PEER_MANAGER_IDLE_DELAY: Duration = Duration::from_secs(300);

#[derive(Debug)]
struct PeerConnectivityManagerState {
    peers: Vec<ManagedPeer>,
}

#[derive(Debug, Clone)]
struct ManagedPeer {
    node_id: NodeId,
    source: String,
    consecutive_failures: u64,
    last_error: Option<String>,
    next_attempt: Instant,
}

#[derive(Debug, Clone)]
struct PeerConnectivityState {
    node_id: NodeId,
    consecutive_failures: u64,
    last_error: Option<String>,
    next_retry_in_secs: Option<u64>,
}

#[derive(Debug)]
enum PeerConnectivityEvent {
    ManagePeer {
        node_id: NodeId,
        source: String,
        immediate: bool,
    },
    ConnectionSuccess {
        node_id: NodeId,
        source: String,
    },
    ConnectionFailure {
        node_id: NodeId,
        source: String,
        error: String,
    },
}

#[async_trait]
pub trait InboundEventHandler: Send + Sync {
    async fn handle_gossip_message(&self, topic: TopicId, sender: NodeId, data: Vec<u8>);
    async fn handle_incoming_stream(&self, alpn: Alpn, stream: streams::BiStream, node_id: NodeId);
}

#[derive(Clone)]
pub struct NetHandle {
    inner: Arc<NetInner>,
    monitor: Monitor,
}

struct NetInner {
    effect_tx: mpsc::Sender<EffectHandle>,
    node_id: NodeId,
    realm_id: RealmId,
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    discovery_method: DiscoveryMethod,
    relay_method: RelayMethod,
    dht_signed_authorized_nodes: Arc<RwLock<Vec<NodeId>>>,
    dht: Arc<DhtHandle>,
    gossip: Arc<GossipService>,
    streams: Arc<StreamsService>,
    peer_connectivity: Arc<Mutex<PeerConnectivityManagerState>>,
    network_diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    peer_connectivity_tx: mpsc::Sender<PeerConnectivityEvent>,
    inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>>,
    shutdown: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl std::fmt::Debug for NetHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetHandle")
            .field("node_id", &self.inner.node_id)
            .finish()
    }
}

impl NetHandle {
    pub async fn new(config: NetConfig, storage: StorageHandle) -> Result<Self> {
        let secret_key = config.secret_key.unwrap_or_else(iroh::SecretKey::generate);

        let address_lookup = MemoryLookup::new();
        let discovery_method = config.discovery_method.clone();
        let relay_method = config.relay_method.clone();
        let configured_relay_urls = relay_method.relay_urls();

        let mut transport_config = QuicTransportConfig::builder();
        if let Some(max_uni) = config.max_concurrent_uni_streams {
            transport_config =
                transport_config.max_concurrent_uni_streams(VarInt::from_u64(max_uni)?);
        }
        if let Some(max_bidi) = config.max_concurrent_bidi_streams {
            transport_config =
                transport_config.max_concurrent_bidi_streams(VarInt::from_u64(max_bidi)?);
        }

        let monitor = Monitor::new();

        let mut endpoint_builder = Endpoint::builder(presets::Minimal)
            .hooks(monitor.clone())
            .transport_config(transport_config.build())
            .secret_key(secret_key)
            .address_lookup(address_lookup.clone())
            .alpns(vec![
                Alpn::Dht.as_bytes().to_vec(),
                Alpn::Gossip.as_bytes().to_vec(),
                Alpn::Bao.as_bytes().to_vec(),
                Alpn::Automerge.as_bytes().to_vec(),
                Alpn::Metadata.as_bytes().to_vec(),
            ]);

        match &config.relay_method {
            RelayMethod::None => {
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Disabled);
            }
            RelayMethod::N0 => {
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Default);
            }
            RelayMethod::Custom(relays) => {
                let relays = RelayMap::try_from_iter(relays.iter().map(|s| s.as_ref()))
                    .map_err(|e| NetError::Bootstrap(format!("Invalid relay URL: {}", e)))?;
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Custom(relays));
            }
            RelayMethod::N0WithCustom(relays) => {
                let relay_map = iroh::defaults::prod::default_relay_map();
                let custom = RelayMap::try_from_iter(relays.iter().map(|s| s.as_ref()))
                    .map_err(|e| NetError::Bootstrap(format!("Invalid relay URL: {}", e)))?;
                relay_map.extend(&custom);
                endpoint_builder = endpoint_builder.relay_mode(RelayMode::Custom(relay_map));
            }
        }

        for method in config.discovery_method.leaf_methods() {
            match method {
                DiscoveryMethod::None | DiscoveryMethod::DhtSigned { .. } => {
                    // No endpoint builder setup needed for these methods.
                }
                DiscoveryMethod::N0Dns => {
                    endpoint_builder = endpoint_builder.address_lookup(PkarrPublisher::n0_dns());
                    endpoint_builder = endpoint_builder.address_lookup(DnsAddressLookup::n0_dns());
                }
                DiscoveryMethod::CustomDns(servers) => {
                    for server in servers {
                        endpoint_builder = endpoint_builder
                            .address_lookup(DnsAddressLookup::builder(server.clone()));
                    }
                }
                DiscoveryMethod::Ordered(_) => {}
            }
        }

        let endpoint_builder = endpoint_builder
            .bind_addr(config.bind_addr)
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;

        let endpoint = endpoint_builder
            .bind()
            .await
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;
        address_lookup.set_endpoint_info(local_endpoint_addr(&endpoint, &configured_relay_urls));

        let node_id = endpoint.id();
        let peer_endpoints = unique_endpoint_addrs(config.peer_endpoints.clone(), node_id);
        for endpoint_addr in &peer_endpoints {
            address_lookup.set_endpoint_info(endpoint_addr.clone());
        }
        let mut peer_nodes = config.peer_nodes.clone();
        peer_nodes.extend(peer_endpoints.iter().map(|endpoint| endpoint.id));
        let peer_nodes = unique_peer_nodes(peer_nodes, node_id);
        let dht_signed_authorized_nodes = Arc::new(RwLock::new(peer_nodes.clone()));
        let peer_connectivity = Arc::new(Mutex::new(PeerConnectivityManagerState::new(
            &peer_nodes,
            "realm_config",
        )));
        let network_diagnostics = Arc::new(Mutex::new(NetworkDiagnosticsState::default()));
        let (peer_connectivity_tx, peer_connectivity_rx) = mpsc::channel(256);
        let shutdown = CancellationToken::new();

        let (gossip_msg_tx, mut gossip_msg_rx) = mpsc::channel::<(TopicId, NodeId, Vec<u8>)>(1024);
        let inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>> =
            Arc::new(RwLock::new(None));

        let (dht_handle, dht_resources) =
            DhtHandle::spawn(endpoint.clone(), storage.clone(), shutdown.child_token())?;
        let dht = Arc::new(dht_handle);

        let gossip = Arc::new(
            GossipService::new(
                endpoint.clone(),
                storage.clone(),
                dht.clone(),
                config.realm_id,
                peer_nodes.clone(),
                shutdown.child_token(),
                gossip_msg_tx.clone(),
            )
            .await?,
        );
        gossip.restore_subscriptions().await?;

        let streams = Arc::new(StreamsService::new(
            endpoint.clone(),
            shutdown.child_token(),
        ));

        let (effect_tx, mut effect_rx) = mpsc::channel::<EffectHandle>(256);

        let shutdown_for_effects = shutdown.clone();
        let dht_for_effects = dht.clone();
        let gossip_for_effects = gossip.clone();
        let effect_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_for_effects.cancelled() => break,
                    maybe_effect = effect_rx.recv() => {
                        let Some((effect, response_tx, span)) = maybe_effect else { break };
                        let dht = dht_for_effects.clone();
                        let gossip = gossip_for_effects.clone();
                        tokio::spawn(async move {
                            let event = effect_handlers::handle_net_effect(
                                &dht,
                                &gossip,
                                effect,
                            )
                            .await;
                            let _ = response_tx.send(event);
                        }.instrument(span));
                    }
                }
            }
        });

        let (dht_tx, mut dht_rx) = mpsc::channel(64);
        let (gossip_conn_tx, mut gossip_conn_rx) = mpsc::channel(64);
        let (stream_tx, mut stream_rx) = mpsc::channel(64);

        let dht_inbound_tx = dht_resources.inbound_stream_tx.clone();
        let dht_for_inbound = dht.clone();
        let gossip_for_inbound = gossip.clone();
        let dht_task = tokio::spawn(async move {
            while let Some((conn, send, recv, peer_id)) = dht_rx.recv().await {
                if let Err(err) = dht_for_inbound.add_peer(peer_id) {
                    warn!(
                        node_id = %peer_id,
                        error = %err,
                        "Failed to add inbound DHT peer to routing queue"
                    );
                }
                gossip_for_inbound.add_bootstrap_node(peer_id);
                match dht_inbound_tx.try_send((conn, send, recv, peer_id)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) => {
                        warn!(node_id = %peer_id, "Dropping inbound DHT stream: queue full");
                    }
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
        });

        let dht_for_gossip = dht.clone();
        let gossip_for_gossip = gossip.clone();
        let gossip_task = tokio::spawn(async move {
            while let Some((conn, peer_id)) = gossip_conn_rx.recv().await {
                if let Err(err) = dht_for_gossip.add_peer(peer_id) {
                    warn!(
                        node_id = %peer_id,
                        error = %err,
                        "Failed to add gossip peer to routing queue"
                    );
                }
                gossip_for_gossip.add_bootstrap_node(peer_id);
                if let Err(err) = gossip_for_gossip.gossip().handle_connection(conn).await {
                    warn!(error = %err, "Failed to hand connection to gossip service");
                }
            }
        });

        let inbound_handler_for_gossip = inbound_handler.clone();
        let shutdown_for_gossip_events = shutdown.clone();
        let gossip_event_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_for_gossip_events.cancelled() => break,
                    maybe_msg = gossip_msg_rx.recv() => {
                        let Some((topic, sender, data)) = maybe_msg else { break };
                        let handler = inbound_handler_for_gossip.read().clone();
                        if let Some(handler) = handler {
                            tokio::spawn(async move {
                                handler.handle_gossip_message(topic, sender, data).await;
                            });
                        } else {
                            warn!(topic = %topic, sender = %sender, "Dropping inbound gossip message without registered handler");
                        }
                    }
                }
            }
        });

        let dht_for_streams = dht.clone();
        let gossip_for_streams = gossip.clone();
        let inbound_handler_for_streams = inbound_handler.clone();
        let stream_task = tokio::spawn(async move {
            while let Some((alpn, send, recv, peer_id)) = stream_rx.recv().await {
                if let Err(err) = dht_for_streams.add_peer(peer_id) {
                    warn!(
                        node_id = %peer_id,
                        error = %err,
                        "Failed to add inbound stream peer to routing queue"
                    );
                }
                gossip_for_streams.add_bootstrap_node(peer_id);

                let handler = inbound_handler_for_streams.read().clone();
                if let Some(handler) = handler {
                    tokio::spawn(async move {
                        handler
                            .handle_incoming_stream(alpn, (send, recv), peer_id)
                            .await;
                    });
                } else {
                    warn!(node_id = %peer_id, "Dropping inbound stream without registered handler");
                }
            }
        });

        let endpoint_for_accept = endpoint.clone();
        let shutdown_for_accept = shutdown.child_token();
        let accept_task = tokio::spawn(async move {
            streams::run_accept_loop(
                endpoint_for_accept,
                dht_tx,
                gossip_conn_tx,
                stream_tx,
                shutdown_for_accept,
            )
            .await;
        });

        let peer_connectivity_task = tokio::spawn(run_peer_connectivity_manager(
            dht.clone(),
            address_lookup.clone(),
            discovery_method.clone(),
            config.realm_id,
            dht_signed_authorized_nodes.clone(),
            peer_connectivity.clone(),
            network_diagnostics.clone(),
            peer_connectivity_rx,
            shutdown.child_token(),
        ));

        let mut tasks = dht_resources.tasks;
        if let Some((ttl, refresh_after)) = discovery_method.dht_signed_config() {
            tasks.push(spawn_dht_signed_publisher(
                dht.clone(),
                endpoint.clone(),
                config.realm_id,
                configured_relay_urls.clone(),
                ttl,
                refresh_after,
                network_diagnostics.clone(),
                shutdown.child_token(),
            ));
        }
        tasks.extend(vec![
            effect_task,
            dht_task,
            gossip_task,
            gossip_event_task,
            stream_task,
            accept_task,
            peer_connectivity_task,
        ]);

        let inner = Arc::new(NetInner {
            effect_tx,
            node_id,
            realm_id: config.realm_id,
            endpoint,
            address_lookup,
            discovery_method,
            relay_method,
            dht_signed_authorized_nodes,
            dht,
            gossip,
            streams,
            peer_connectivity,
            network_diagnostics,
            peer_connectivity_tx,
            inbound_handler,
            shutdown,
            tasks: Mutex::new(tasks),
        });

        Ok(Self { inner, monitor })
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn realm_id(&self) -> &RealmId {
        &self.inner.realm_id
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        local_endpoint_addr(&self.inner.endpoint, &self.inner.relay_method.relay_urls())
    }

    pub async fn add_peer_addr(&self, endpoint_addr: EndpointAddr) {
        if endpoint_addr.id == self.inner.node_id {
            return;
        }

        authorize_dht_signed_node(
            &self.inner.dht_signed_authorized_nodes,
            endpoint_addr.id,
            self.inner.node_id,
        );
        self.inner
            .address_lookup
            .set_endpoint_info(endpoint_addr.clone());
        send_peer_connectivity_event(
            &self.inner.peer_connectivity_tx,
            PeerConnectivityEvent::ManagePeer {
                node_id: endpoint_addr.id,
                source: "endpoint_addr".to_string(),
                immediate: true,
            },
        );
        self.inner.gossip.add_bootstrap_node(endpoint_addr.id);
        if let Err(err) = self.inner.dht.add_peer(endpoint_addr.id) {
            warn!(
                node_id = %endpoint_addr.id,
                error = %err,
                "Failed to add endpoint address peer to DHT"
            );
        }
    }

    pub async fn add_peer_node(&self, node_id: NodeId) {
        if node_id == self.inner.node_id {
            return;
        }

        authorize_dht_signed_node(
            &self.inner.dht_signed_authorized_nodes,
            node_id,
            self.inner.node_id,
        );
        send_peer_connectivity_event(
            &self.inner.peer_connectivity_tx,
            PeerConnectivityEvent::ManagePeer {
                node_id,
                source: "peer_node".to_string(),
                immediate: true,
            },
        );
        self.inner.gossip.add_bootstrap_node(node_id);
        if let Err(err) = self.inner.dht.add_peer(node_id) {
            warn!(
                node_id = %node_id,
                error = %err,
                "Failed to add peer node to DHT"
            );
        }
    }

    pub async fn open_stream(&self, node_id: NodeId, alpn: Alpn) -> Result<streams::BiStream> {
        if node_id != self.inner.node_id {
            if let Err(err) = self.inner.dht.add_peer(node_id) {
                warn!(
                    node_id = %node_id,
                    error = %err,
                    "Failed to add stream target peer to DHT"
                );
            }
            self.inner.gossip.add_bootstrap_node(node_id);
            send_peer_connectivity_event(
                &self.inner.peer_connectivity_tx,
                PeerConnectivityEvent::ManagePeer {
                    node_id,
                    source: "stream_target".to_string(),
                    immediate: false,
                },
            );
        }

        match self.inner.streams.open(node_id, alpn).await {
            Ok(stream) => {
                if node_id != self.inner.node_id {
                    send_peer_connectivity_event(
                        &self.inner.peer_connectivity_tx,
                        PeerConnectivityEvent::ConnectionSuccess {
                            node_id,
                            source: "stream_open".to_string(),
                        },
                    );
                }
                Ok(stream)
            }
            Err(mut err) => {
                if node_id != self.inner.node_id {
                    let authorized_nodes = self.inner.dht_signed_authorized_nodes.read().clone();
                    match resolve_dht_signed_endpoint(
                        &self.inner.dht,
                        self.inner.realm_id,
                        &authorized_nodes,
                        node_id,
                        self.inner.discovery_method.dht_signed_config(),
                        &self.inner.network_diagnostics,
                    )
                    .await
                    {
                        Ok(Some(endpoint_addr)) => {
                            install_dht_signed_endpoint(
                                &self.inner.address_lookup,
                                &self.inner.dht,
                                &self.inner.gossip,
                                endpoint_addr,
                            );
                            debug!(
                                node_id = %node_id,
                                "Retrying stream after DHT-signed endpoint resolution"
                            );
                            match self.inner.streams.open(node_id, alpn).await {
                                Ok(stream) => {
                                    send_peer_connectivity_event(
                                        &self.inner.peer_connectivity_tx,
                                        PeerConnectivityEvent::ConnectionSuccess {
                                            node_id,
                                            source: "stream_open_dht_signed".to_string(),
                                        },
                                    );
                                    return Ok(stream);
                                }
                                Err(retry_err) => {
                                    err = retry_err;
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(resolve_err) => {
                            debug!(
                                node_id = %node_id,
                                error = %resolve_err,
                                "DHT-signed endpoint resolution failed"
                            );
                        }
                    }
                }

                if node_id != self.inner.node_id {
                    send_peer_connectivity_event(
                        &self.inner.peer_connectivity_tx,
                        PeerConnectivityEvent::ConnectionFailure {
                            node_id,
                            source: "stream_open".to_string(),
                            error: err.to_string(),
                        },
                    );
                }
                Err(err)
            }
        }
    }

    pub fn set_inbound_handler(&self, handler: Arc<dyn InboundEventHandler>) {
        *self.inner.inbound_handler.write() = Some(handler);
    }

    pub fn clear_inbound_handler(&self) {
        self.inner.inbound_handler.write().take();
    }

    pub async fn shutdown(&self) {
        if self.inner.shutdown.is_cancelled() {
            return;
        }

        if let Err(err) = self.inner.dht.shutdown().await {
            warn!(error = %err, "DHT shutdown returned error");
        }
        self.inner.shutdown.cancel();
        if let Err(err) = self.inner.gossip.gossip().shutdown().await {
            warn!(error = %err, "Gossip shutdown returned error");
        }
        self.inner.endpoint.close().await;

        let mut tasks = self.inner.tasks.lock().await;
        while let Some(handle) = tasks.pop() {
            let _ = handle.await;
        }
    }

    pub async fn get_status(&self) -> NetState {
        let peer_nodes = self.inner.gossip.get_bootstrap_nodes();
        let configured_relay_urls = self.inner.relay_method.relay_urls();
        let monitor = self.monitor.get_status().await;
        let mut diagnostics = self.inner.network_diagnostics.lock().await.clone();
        if let Ok(size) = self.inner.dht.routing_table_size().await {
            diagnostics.routing_table_size = Some(size);
        }
        let peer_connectivity = peer_connectivity_status(&self.inner.peer_connectivity).await;
        let connections = peer_connection_states(
            &self.inner.endpoint,
            &monitor,
            &peer_connectivity,
            &peer_nodes,
            self.inner.node_id,
        )
        .await;
        let warnings = net_warnings(&peer_nodes, &connections, diagnostics.routing_table_size);

        NetState {
            endpoint_addr: local_endpoint_addr(&self.inner.endpoint, &configured_relay_urls),
            realm_id: *self.realm_id(),
            node_id: self.node_id(),
            discovery_methods: self.inner.discovery_method.enabled_methods(),
            relay_method: self.inner.relay_method.method_name().to_string(),
            relay_urls: configured_relay_urls,
            connections,
            requests: diagnostics.requests,
            routing_table_size: diagnostics.routing_table_size,
            warnings,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_dht_signed_publisher(
    dht: Arc<DhtHandle>,
    endpoint: Endpoint,
    realm_id: RealmId,
    configured_relay_urls: Vec<String>,
    ttl: Duration,
    refresh_after: Duration,
    diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (ttl, refresh_after) = normalized_dht_signed_timing(ttl, refresh_after);
        let mut sequence = 0u64;

        loop {
            sequence = sequence.saturating_add(1);
            match publish_realm_endpoint_announcement(
                &dht,
                &endpoint,
                realm_id,
                &configured_relay_urls,
                ttl,
                sequence,
            )
            .await
            {
                Ok(()) => {
                    diagnostics.lock().await.requests.record_success();
                    debug!(
                        realm_id = %realm_id,
                        node_id = %endpoint.id(),
                        ttl_secs = ttl.as_secs(),
                        sequence,
                        "Published DHT-signed realm endpoint announcement"
                    );
                }
                Err(err) => {
                    diagnostics
                        .lock()
                        .await
                        .requests
                        .record_failure(format!("publish DHT-signed endpoint: {err}"));
                    warn!(
                        realm_id = %realm_id,
                        node_id = %endpoint.id(),
                        error = %err,
                        "Failed to publish DHT-signed realm endpoint announcement"
                    );
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(refresh_after) => {}
            }
        }
    })
}

fn normalized_dht_signed_timing(ttl: Duration, refresh_after: Duration) -> (Duration, Duration) {
    let ttl_secs = ttl.as_secs().max(1);
    let mut refresh_secs = refresh_after.as_secs().max(1);
    if refresh_secs >= ttl_secs && ttl_secs > 1 {
        refresh_secs = (ttl_secs / 2).max(1);
    }

    (
        Duration::from_secs(ttl_secs),
        Duration::from_secs(refresh_secs),
    )
}

async fn publish_realm_endpoint_announcement(
    dht: &DhtHandle,
    endpoint: &Endpoint,
    realm_id: RealmId,
    configured_relay_urls: &[String],
    ttl: Duration,
    sequence: u64,
) -> Result<()> {
    let ttl_secs = ttl.as_secs().max(1);
    let issued_at = unix_timestamp_secs();
    let expires_at = issued_at.saturating_add(ttl_secs);
    let node_id = endpoint.id();
    let endpoint_addr = local_endpoint_addr(endpoint, configured_relay_urls);
    let signing_bytes = realm_endpoint_announcement_signing_bytes(
        &realm_id,
        &node_id,
        &endpoint_addr,
        issued_at,
        expires_at,
        sequence,
    )
    .map_err(|err| NetError::Dht(format!("encode endpoint announcement signing bytes: {err}")))?;
    let signature = endpoint.secret_key().sign(&signing_bytes);
    let announcement = RealmEndpointAnnouncement {
        realm_id,
        node_id,
        endpoint_addr,
        issued_at,
        expires_at,
        sequence,
        signature,
    };
    let value = postcard::to_allocvec(&announcement)
        .map_err(|err| NetError::Dht(format!("encode endpoint announcement: {err}")))?;
    let key = realm_endpoint_key(&realm_id, &node_id);

    dht.put(&key, realm_id, value, Duration::from_secs(ttl_secs))
        .await
}

async fn lookup_dht_signed_endpoint(
    dht: &DhtHandle,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    peer: NodeId,
    config: Option<(Duration, Duration)>,
) -> Result<Option<EndpointAddr>> {
    let Some((ttl, _)) = config else {
        return Ok(None);
    };
    if !authorized_nodes.contains(&peer) {
        return Ok(None);
    }

    let key = realm_endpoint_key(&realm_id, &peer);
    let entries = dht.get(&key, Some(realm_id)).await?;
    let now = unix_timestamp_secs();

    Ok(select_dht_signed_endpoint(
        entries,
        peer,
        realm_id,
        authorized_nodes,
        now,
        ttl.as_secs().max(1),
    ))
}

fn select_dht_signed_endpoint(
    entries: Vec<DhtEntry>,
    peer: NodeId,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    now: u64,
    max_ttl_secs: u64,
) -> Option<EndpointAddr> {
    let mut best = None::<RealmEndpointAnnouncement>;

    for entry in entries {
        let Ok(announcement) = postcard::from_bytes::<RealmEndpointAnnouncement>(&entry.value)
        else {
            continue;
        };
        if validate_realm_endpoint_announcement(
            &announcement,
            entry.node_id,
            peer,
            realm_id,
            authorized_nodes,
            now,
            max_ttl_secs,
        )
        .is_err()
        {
            continue;
        }

        let replace = best.as_ref().is_none_or(|current| {
            announcement.issued_at > current.issued_at
                || (announcement.issued_at == current.issued_at
                    && announcement.sequence > current.sequence)
                || (announcement.issued_at == current.issued_at
                    && announcement.sequence == current.sequence
                    && announcement.expires_at > current.expires_at)
        });
        if replace {
            best = Some(announcement);
        }
    }

    best.map(|announcement| announcement.endpoint_addr)
}

async fn resolve_dht_signed_endpoint(
    dht: &DhtHandle,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    peer: NodeId,
    config: Option<(Duration, Duration)>,
    diagnostics: &Arc<Mutex<NetworkDiagnosticsState>>,
) -> Result<Option<EndpointAddr>> {
    if !dht_signed_lookup_enabled(config, authorized_nodes, peer) {
        return Ok(None);
    }

    let result = lookup_dht_signed_endpoint(dht, realm_id, authorized_nodes, peer, config).await;
    match &result {
        Ok(_) => diagnostics.lock().await.requests.record_success(),
        Err(err) => {
            diagnostics
                .lock()
                .await
                .requests
                .record_failure(format!("resolve DHT-signed endpoint for {peer}: {err}"));
        }
    }

    result
}

fn dht_signed_lookup_enabled(
    config: Option<(Duration, Duration)>,
    authorized_nodes: &[NodeId],
    peer: NodeId,
) -> bool {
    config.is_some() && authorized_nodes.contains(&peer)
}

fn validate_realm_endpoint_announcement(
    announcement: &RealmEndpointAnnouncement,
    entry_publisher: NodeId,
    requested_peer: NodeId,
    realm_id: RealmId,
    authorized_nodes: &[NodeId],
    now: u64,
    max_ttl_secs: u64,
) -> std::result::Result<(), String> {
    if !authorized_nodes.contains(&requested_peer) {
        return Err("node is not authorized for realm discovery".to_string());
    }
    if announcement.realm_id != realm_id {
        return Err("announcement realm does not match lookup realm".to_string());
    }
    if announcement.node_id != requested_peer {
        return Err("announcement node does not match requested peer".to_string());
    }
    if entry_publisher != requested_peer {
        return Err("DHT entry publisher does not match announcement node".to_string());
    }
    if announcement.endpoint_addr.id != requested_peer {
        return Err("announcement endpoint id does not match requested peer".to_string());
    }
    if announcement.issued_at > announcement.expires_at || announcement.expires_at <= now {
        return Err("announcement is expired or has invalid timestamps".to_string());
    }
    if announcement.issued_at > now.saturating_add(DHT_SIGNED_MAX_CLOCK_SKEW_SECS) {
        return Err("announcement is issued too far in the future".to_string());
    }
    if announcement
        .expires_at
        .saturating_sub(announcement.issued_at)
        > max_ttl_secs
    {
        return Err("announcement ttl exceeds configured maximum".to_string());
    }

    let signing_bytes = realm_endpoint_announcement_signing_bytes(
        &announcement.realm_id,
        &announcement.node_id,
        &announcement.endpoint_addr,
        announcement.issued_at,
        announcement.expires_at,
        announcement.sequence,
    )
    .map_err(|err| err.to_string())?;
    announcement
        .node_id
        .verify(&signing_bytes, &announcement.signature)
        .map_err(|err| err.to_string())
}

fn install_dht_signed_endpoint(
    address_lookup: &MemoryLookup,
    dht: &DhtHandle,
    gossip: &GossipService,
    endpoint_addr: EndpointAddr,
) {
    let node_id = endpoint_addr.id;
    address_lookup.set_endpoint_info(endpoint_addr);
    gossip.add_bootstrap_node(node_id);
    if let Err(err) = dht.add_peer(node_id) {
        debug!(
            node_id = %node_id,
            error = %err,
            "Failed to add DHT-signed endpoint peer"
        );
    }
}

fn local_endpoint_addr(endpoint: &Endpoint, configured_relay_urls: &[String]) -> EndpointAddr {
    let observed = endpoint.addr();
    let mut addr = EndpointAddr::new(endpoint.id());

    for relay in observed.relay_urls().cloned() {
        addr = addr.with_relay_url(relay);
    }
    for relay in configured_relay_urls {
        if let Ok(relay) = relay.parse::<iroh::RelayUrl>() {
            addr = addr.with_relay_url(relay);
        }
    }
    for socket in endpoint.bound_sockets() {
        if !socket.ip().is_unspecified() {
            addr = addr.with_ip_addr(socket);
        }
    }

    addr
}

fn authorize_dht_signed_node(
    authorized_nodes: &Arc<RwLock<Vec<NodeId>>>,
    node_id: NodeId,
    local_id: NodeId,
) {
    if node_id == local_id {
        return;
    }

    let mut nodes = authorized_nodes.write();
    if nodes.contains(&node_id) {
        return;
    }
    nodes.push(node_id);
    nodes.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
}

fn unique_endpoint_addrs(
    mut endpoint_addrs: Vec<EndpointAddr>,
    local_id: NodeId,
) -> Vec<EndpointAddr> {
    let mut unique = Vec::<EndpointAddr>::new();
    for endpoint_addr in endpoint_addrs.drain(..) {
        if endpoint_addr.id == local_id {
            continue;
        }
        if let Some(existing) = unique
            .iter_mut()
            .find(|existing| existing.id == endpoint_addr.id)
        {
            *existing = endpoint_addr;
        } else {
            unique.push(endpoint_addr);
        }
    }
    unique.sort_unstable_by(|a, b| a.id.as_bytes().cmp(b.id.as_bytes()));
    unique
}

fn unique_peer_nodes(mut nodes: Vec<NodeId>, local_id: NodeId) -> Vec<NodeId> {
    nodes.retain(|node| *node != local_id);
    nodes.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    nodes.dedup();
    nodes
}

impl PeerConnectivityManagerState {
    fn new(nodes: &[NodeId], source: &str) -> Self {
        let now = Instant::now();
        let mut peers = nodes
            .iter()
            .copied()
            .map(|node_id| ManagedPeer::new(node_id, source, now))
            .collect::<Vec<_>>();
        peers.sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
        peers.dedup_by(|a, b| a.node_id == b.node_id);
        Self { peers }
    }

    fn manage_peer(&mut self, node_id: NodeId, source: &str, now: Instant, immediate: bool) {
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            if immediate && peer.next_attempt > now {
                peer.next_attempt = now;
            }
            return;
        }

        let next_attempt = if immediate {
            now
        } else {
            now + PEER_SUCCESS_REFRESH_DELAY
        };
        self.peers
            .push(ManagedPeer::new(node_id, source, next_attempt));
        self.peers
            .sort_unstable_by(|a, b| a.node_id.as_bytes().cmp(b.node_id.as_bytes()));
    }

    fn record_success(&mut self, node_id: NodeId, source: &str, now: Instant) {
        if self.peer_mut(node_id).is_none() {
            self.manage_peer(node_id, source, now, false);
        }
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            peer.consecutive_failures = 0;
            peer.last_error = None;
            peer.next_attempt = now + PEER_SUCCESS_REFRESH_DELAY;
        }
    }

    fn record_failure(&mut self, node_id: NodeId, source: &str, error: String, now: Instant) {
        if self.peer_mut(node_id).is_none() {
            self.manage_peer(node_id, source, now, false);
        }
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            peer.consecutive_failures = peer.consecutive_failures.saturating_add(1);
            peer.last_error = Some(error);
            peer.next_attempt = now + peer_retry_delay(node_id, peer.consecutive_failures);
        }
    }

    fn due_peers(&self, now: Instant) -> Vec<NodeId> {
        self.peers
            .iter()
            .filter(|peer| peer.next_attempt <= now)
            .map(|peer| peer.node_id)
            .collect()
    }

    fn next_wait(&self, now: Instant) -> Duration {
        self.peers
            .iter()
            .map(|peer| peer.next_attempt.saturating_duration_since(now))
            .min()
            .unwrap_or(PEER_MANAGER_IDLE_DELAY)
    }

    fn peer_source(&self, node_id: NodeId) -> String {
        self.peers
            .iter()
            .find(|peer| peer.node_id == node_id)
            .map(|peer| peer.source.clone())
            .unwrap_or_else(|| "managed_peer".to_string())
    }

    fn status(&self, now: Instant) -> Vec<PeerConnectivityState> {
        self.peers
            .iter()
            .map(|peer| PeerConnectivityState {
                node_id: peer.node_id,
                consecutive_failures: peer.consecutive_failures,
                last_error: peer.last_error.clone(),
                next_retry_in_secs: Some(
                    peer.next_attempt.saturating_duration_since(now).as_secs(),
                ),
            })
            .collect()
    }

    fn peer_mut(&mut self, node_id: NodeId) -> Option<&mut ManagedPeer> {
        self.peers.iter_mut().find(|peer| peer.node_id == node_id)
    }
}

impl ManagedPeer {
    fn new(node_id: NodeId, source: &str, next_attempt: Instant) -> Self {
        Self {
            node_id,
            source: source.to_string(),
            consecutive_failures: 0,
            last_error: None,
            next_attempt,
        }
    }
}

fn merge_source(existing: &mut String, source: &str) {
    if existing.split(',').any(|part| part == source) {
        return;
    }
    if existing.is_empty() {
        existing.push_str(source);
    } else {
        existing.push(',');
        existing.push_str(source);
    }
}

fn peer_retry_delay(node_id: NodeId, consecutive_failures: u64) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(8) as u32;
    let base = PEER_INITIAL_RETRY_DELAY
        .saturating_mul(2u32.saturating_pow(exponent))
        .min(PEER_MAX_RETRY_DELAY);
    let base_ms = base.as_millis() as i128;
    let jitter_window = (base_ms / 5).max(1);
    let jitter_range = (jitter_window * 2 + 1) as u64;
    let jitter_seed = peer_jitter_seed(node_id, consecutive_failures);
    let jitter = (jitter_seed % jitter_range) as i128 - jitter_window;
    let delayed_ms = (base_ms + jitter).max(1_000) as u64;
    Duration::from_millis(delayed_ms)
}

fn peer_jitter_seed(node_id: NodeId, attempt: u64) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&node_id.as_bytes()[..8]);
    u64::from_le_bytes(bytes) ^ attempt.rotate_left(17)
}

fn send_peer_connectivity_event(
    tx: &mpsc::Sender<PeerConnectivityEvent>,
    event: PeerConnectivityEvent,
) {
    if let Err(err) = tx.try_send(event) {
        match err {
            mpsc::error::TrySendError::Full(_) => debug!("peer connectivity event queue full"),
            mpsc::error::TrySendError::Closed(_) => debug!("peer connectivity task stopped"),
        }
    }
}

async fn peer_connectivity_status(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
) -> Vec<PeerConnectivityState> {
    state.lock().await.status(Instant::now())
}

#[allow(clippy::too_many_arguments)]
async fn run_peer_connectivity_manager(
    dht: Arc<DhtHandle>,
    address_lookup: MemoryLookup,
    discovery_method: DiscoveryMethod,
    realm_id: RealmId,
    dht_signed_authorized_nodes: Arc<RwLock<Vec<NodeId>>>,
    state: Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: Arc<Mutex<NetworkDiagnosticsState>>,
    mut event_rx: mpsc::Receiver<PeerConnectivityEvent>,
    shutdown: CancellationToken,
) {
    loop {
        drain_peer_connectivity_events(&state, &mut event_rx).await;

        let now = Instant::now();
        let due_peers = state.lock().await.due_peers(now);
        if !due_peers.is_empty() {
            for peer in due_peers {
                if shutdown.is_cancelled() {
                    return;
                }
                let authorized_nodes = dht_signed_authorized_nodes.read().clone();
                run_peer_connectivity_attempt(
                    &dht,
                    &address_lookup,
                    &discovery_method,
                    realm_id,
                    &authorized_nodes,
                    &state,
                    &diagnostics,
                    peer,
                )
                .await;
            }
            continue;
        }

        let wait = state.lock().await.next_wait(now);

        tokio::select! {
            _ = shutdown.cancelled() => break,
            maybe_event = event_rx.recv() => {
                let Some(event) = maybe_event else { break };
                apply_peer_connectivity_event(&state, event).await;
                while let Ok(event) = event_rx.try_recv() {
                    apply_peer_connectivity_event(&state, event).await;
                }
            }
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

async fn drain_peer_connectivity_events(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    event_rx: &mut mpsc::Receiver<PeerConnectivityEvent>,
) {
    while let Ok(event) = event_rx.try_recv() {
        apply_peer_connectivity_event(state, event).await;
    }
}

async fn apply_peer_connectivity_event(
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    event: PeerConnectivityEvent,
) {
    let now = Instant::now();
    let mut guard = state.lock().await;
    match event {
        PeerConnectivityEvent::ManagePeer {
            node_id,
            source,
            immediate,
        } => {
            guard.manage_peer(node_id, &source, now, immediate);
        }
        PeerConnectivityEvent::ConnectionSuccess { node_id, source } => {
            guard.record_success(node_id, &source, now);
        }
        PeerConnectivityEvent::ConnectionFailure {
            node_id,
            source,
            error,
        } => {
            guard.record_failure(node_id, &source, error, now);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_peer_connectivity_attempt(
    dht: &DhtHandle,
    address_lookup: &MemoryLookup,
    discovery_method: &DiscoveryMethod,
    realm_id: RealmId,
    dht_signed_authorized_nodes: &[NodeId],
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: &Arc<Mutex<NetworkDiagnosticsState>>,
    peer: NodeId,
) {
    let source = state.lock().await.peer_source(peer);

    let mut result = dht.bootstrap_nodes(&[peer]).await;
    if result.is_err() {
        match resolve_dht_signed_endpoint(
            dht,
            realm_id,
            dht_signed_authorized_nodes,
            peer,
            discovery_method.dht_signed_config(),
            diagnostics,
        )
        .await
        {
            Ok(Some(endpoint_addr)) => {
                address_lookup.set_endpoint_info(endpoint_addr.clone());
                if let Err(err) = dht.add_peer(endpoint_addr.id) {
                    debug!(
                        node_id = %endpoint_addr.id,
                        error = %err,
                        "Failed to add DHT-signed endpoint peer before retry"
                    );
                }
                result = dht.bootstrap_nodes(&[peer]).await;
            }
            Ok(None) => {}
            Err(err) => {
                debug!(
                    peer = %peer,
                    error = %err,
                    "DHT-signed endpoint resolution failed during connectivity check"
                );
            }
        }
    }
    let routing_table_size = dht.routing_table_size().await.ok();
    match result {
        Ok(()) => {
            state
                .lock()
                .await
                .record_success(peer, "connectivity_probe", Instant::now());
            let mut diagnostics = diagnostics.lock().await;
            diagnostics.requests.record_success();
            diagnostics.routing_table_size = routing_table_size;
            debug!(
                peer = %peer,
                source = %source,
                routing_table_size = ?routing_table_size,
                "Managed peer connectivity check succeeded"
            );
        }
        Err(err) => {
            state.lock().await.record_failure(
                peer,
                "connectivity_probe",
                err.to_string(),
                Instant::now(),
            );
            let mut diagnostics = diagnostics.lock().await;
            diagnostics
                .requests
                .record_failure(format!("peer {peer}: {err}"));
            diagnostics.routing_table_size = routing_table_size;
            warn!(
                error = %err,
                peer = %peer,
                source = %source,
                routing_table_size = ?routing_table_size,
                "Managed peer connectivity check failed; will retry with backoff"
            );
        }
    }
}

async fn peer_connection_states(
    endpoint: &Endpoint,
    monitor: &ConnectionMonitorState,
    peer_connectivity: &[PeerConnectivityState],
    peer_nodes: &[NodeId],
    local_id: NodeId,
) -> Vec<PeerConnectionState> {
    let mut peers = peer_nodes.to_vec();
    peers.extend(peer_connectivity.iter().map(|peer| peer.node_id));
    peers.extend(
        monitor
            .open_connections
            .iter()
            .map(|connection| connection.remote_id),
    );
    let mut peers = unique_peer_nodes(peers, local_id);
    let mut states = Vec::with_capacity(peers.len());

    for peer in peers.drain(..) {
        let health = peer_connectivity.iter().find(|state| state.node_id == peer);
        let open_connections = monitor
            .open_connections
            .iter()
            .filter(|connection| connection.remote_id == peer)
            .collect::<Vec<_>>();
        let mut active_addresses = active_address_rows(endpoint, peer).await;

        for connection in &open_connections {
            let (address, status) = match &connection.selected_address {
                Some(address) => (address.clone(), ConnectionAddressStatus::Active),
                None => (
                    "not assigned".to_string(),
                    ConnectionAddressStatus::NotAssigned,
                ),
            };
            let row = upsert_address_row(&mut active_addresses, address, status);
            merge_rtt(&mut row.rtt_ms, connection.rtt_ms);
            row.protocol_connections.push(ProtocolConnectionState {
                connection_id: connection.connection_id,
                alpn: connection.alpn,
                side: connection.side,
            });
        }

        for address in &mut active_addresses {
            address
                .protocol_connections
                .sort_unstable_by_key(|connection| connection.connection_id);
        }
        active_addresses.sort_unstable_by(|a, b| a.address.cmp(&b.address));

        let status = if !open_connections.is_empty() {
            PeerConnectionStatus::Connected
        } else if health.is_some_and(|state| state.consecutive_failures > 0) {
            PeerConnectionStatus::Unreachable
        } else {
            PeerConnectionStatus::Known
        };

        states.push(PeerConnectionState {
            node_id: peer,
            status,
            active_addresses,
            last_error: health.and_then(|state| state.last_error.clone()),
            next_retry_in_secs: health.and_then(|state| state.next_retry_in_secs),
        });
    }

    states
}

async fn active_address_rows(endpoint: &Endpoint, peer: NodeId) -> Vec<ConnectionAddressState> {
    let mut rows = Vec::new();
    if let Some(remote_info) = endpoint.remote_info(peer).await {
        for addr in remote_info.addrs() {
            if matches!(addr.usage(), TransportAddrUsage::Active) {
                upsert_address_row(
                    &mut rows,
                    transport_addr_to_string(addr.addr()),
                    ConnectionAddressStatus::Active,
                );
            }
        }
    }
    rows
}

fn upsert_address_row(
    rows: &mut Vec<ConnectionAddressState>,
    address: String,
    status: ConnectionAddressStatus,
) -> &mut ConnectionAddressState {
    if let Some(index) = rows
        .iter()
        .position(|row| row.address == address && row.status == status)
    {
        return &mut rows[index];
    }

    rows.push(ConnectionAddressState {
        status,
        address,
        rtt_ms: None,
        protocol_connections: Vec::new(),
    });
    rows.last_mut().expect("inserted address row")
}

fn merge_rtt(existing: &mut Option<u64>, candidate: Option<u64>) {
    let Some(candidate) = candidate else {
        return;
    };
    *existing = Some(existing.map_or(candidate, |current| current.min(candidate)));
}

fn transport_addr_to_string(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

fn net_warnings(
    peer_nodes: &[NodeId],
    connections: &[PeerConnectionState],
    routing_table_size: Option<usize>,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if connections
        .iter()
        .any(|connection| connection.status == PeerConnectionStatus::Unreachable)
        && !connections
            .iter()
            .any(|connection| connection.status == PeerConnectionStatus::Connected)
    {
        warnings
            .push("no open p2p connections after one or more managed peer failures".to_string());
    }

    if routing_table_size == Some(0) && !peer_nodes.is_empty() {
        warnings.push("DHT routing table is empty despite configured realm peers".to_string());
    }

    for node_id in peer_nodes {
        let has_active_address = connections
            .iter()
            .any(|state| state.node_id == *node_id && !state.active_addresses.is_empty());
        if !has_active_address {
            warnings.push(format!(
                "realm peer {node_id} has no active addresses; add peer endpoints or enable realm discovery"
            ));
        }
    }

    for peer in connections {
        if peer.status == PeerConnectionStatus::Unreachable {
            let error = peer
                .last_error
                .as_deref()
                .unwrap_or("unknown connection error");
            warnings.push(format!(
                "managed peer {} unreachable: {}",
                peer.node_id, error
            ));
        } else if peer.active_addresses.is_empty() {
            warnings.push(format!(
                "managed peer {} has no active addresses; add peer endpoints or enable realm discovery",
                peer.node_id
            ));
        }
    }

    warnings.sort();
    warnings.dedup();
    warnings
}

#[async_trait]
impl Handle for NetHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Net(net_effect) => {
                let (tx, rx) = oneshot::channel();
                if self
                    .inner
                    .effect_tx
                    .send((net_effect, tx, Span::current()))
                    .await
                    .is_err()
                {
                    return Event::Net(NetEvent::Error(CoreNetError::ChannelClosed));
                }

                match rx.await {
                    Ok(event) => Event::Net(event),
                    Err(_) => Event::Net(NetEvent::Error(CoreNetError::ChannelClosed)),
                }
            }
            _ => Event::Net(NetEvent::Error(CoreNetError::InvalidEffect)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::time::{Duration, sleep};

    fn make_secret(seed: u8) -> iroh::SecretKey {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[0] = seed;
        iroh::SecretKey::from_bytes(&seed_bytes)
    }

    fn make_announcement(
        secret: &iroh::SecretKey,
        realm_id: RealmId,
        endpoint_addr: EndpointAddr,
        issued_at: u64,
        expires_at: u64,
        sequence: u64,
    ) -> RealmEndpointAnnouncement {
        let node_id = secret.public();
        let signing_bytes = realm_endpoint_announcement_signing_bytes(
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
            validate_realm_endpoint_announcement(
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
            validate_realm_endpoint_announcement(
                &announcement,
                node_id,
                node_id,
                realm_id,
                &[],
                now,
                300,
            )
            .is_err()
        );

        let forged_secret = make_secret(52);
        let signing_bytes = realm_endpoint_announcement_signing_bytes(
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
            validate_realm_endpoint_announcement(
                &forged,
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

        let endpoint = select_dht_signed_endpoint(entries, node_id, realm_id, &[node_id], now, 300)
            .expect("valid announcement");

        assert_eq!(endpoint.addrs, newer.endpoint_addr.addrs);
    }

    #[test]
    fn newer_issued_announcement_wins_after_sequence_reset() {
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

        let endpoint =
            select_dht_signed_endpoint(entries, node_id, realm_id, &[node_id], now + 10, 300)
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

        assert!(
            select_dht_signed_endpoint(entries, node_id, realm_id, &[node_id], 1_000, 300)
                .is_none()
        );
    }

    #[test]
    fn rejects_future_and_excessive_ttl_announcements() {
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
            validate_realm_endpoint_announcement(
                &future,
                node_id,
                node_id,
                realm_id,
                &[node_id],
                now,
                300,
            )
            .is_err()
        );
        assert!(
            validate_realm_endpoint_announcement(
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
    async fn test_net_handle_creates() -> Result<()> {
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
        assert!(
            status
                .endpoint_addr
                .ip_addrs()
                .all(|socket| !socket.ip().is_unspecified())
        );
        handle.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn peer_endpoint_only_nodes_are_dht_signed_authorized() -> Result<()> {
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
            handle
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
        async fn handle_gossip_message(&self, _topic: TopicId, _sender: NodeId, _data: Vec<u8>) {}

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

    async fn wait_for_open_connections(handle: &NetHandle, expected: usize) -> NetState {
        for _ in 0..50 {
            let status = handle.get_status().await;
            if protocol_connection_count(&status) >= expected {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }

        handle.get_status().await
    }

    async fn wait_for_bootstrap_failure(handle: &NetHandle) -> NetState {
        for _ in 0..50 {
            let status = handle.get_status().await;
            if status.requests.failures > 0 {
                return status;
            }
            sleep(Duration::from_millis(100)).await;
        }

        handle.get_status().await
    }

    async fn wait_for_peer_failure(handle: &NetHandle, peer: NodeId) -> NetState {
        for _ in 0..50 {
            let status = handle.get_status().await;
            if status.connections.iter().any(|state| {
                state.node_id == peer && state.status == PeerConnectionStatus::Unreachable
            }) {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }

        handle.get_status().await
    }

    fn protocol_connection_count(status: &NetState) -> usize {
        status
            .connections
            .iter()
            .flat_map(|peer| &peer.active_addresses)
            .map(|address| address.protocol_connections.len())
            .sum()
    }

    #[tokio::test]
    async fn monitor_tracks_multiple_open_connections() -> Result<()> {
        let (a, _a_dir) = test_net_handle().await?;
        let (b, _b_dir) = test_net_handle().await?;
        a.add_peer_addr(b.endpoint_addr()).await;
        b.add_peer_addr(a.endpoint_addr()).await;

        let _a_to_b = a.open_stream(b.node_id(), Alpn::Bao).await?;
        let _b_to_a = b.open_stream(a.node_id(), Alpn::Bao).await?;

        let status = wait_for_open_connections(&a, 2).await;
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

        let status = wait_for_open_connections(&a, 1).await;
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

        let status = wait_for_bootstrap_failure(&handle).await;
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
                .any(|warning| warning.contains("no active addresses"))
        );
        assert!(
            status
                .warnings
                .iter()
                .any(|warning| warning.contains("managed peer") && warning.contains("unreachable"))
        );
        assert!(
            status
                .connections
                .iter()
                .any(|peer| peer.node_id == missing_peer
                    && peer.status == PeerConnectionStatus::Unreachable)
        );

        handle.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn failed_stream_target_is_managed_with_backoff() -> Result<()> {
        let (handle, _dir) = test_net_handle().await?;
        let missing_peer = iroh::SecretKey::from_bytes(&[77u8; 32]).public();

        let err = handle
            .open_stream(missing_peer, Alpn::Bao)
            .await
            .expect_err("missing peer should fail to connect");
        let status = wait_for_peer_failure(&handle, missing_peer).await;

        assert!(matches!(err, NetError::Connection(_)));
        assert!(status.connections.iter().any(|peer| {
            peer.node_id == missing_peer
                && peer.status == PeerConnectionStatus::Unreachable
                && peer.last_error.is_some()
                && peer.next_retry_in_secs.is_some()
        }));
        assert!(status.warnings.iter().any(|warning| {
            warning.contains("managed peer") && warning.contains("unreachable")
        }));

        handle.shutdown().await;
        Ok(())
    }
}
