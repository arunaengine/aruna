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
use aruna_core::events::{Event, NetError as CoreNetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::structs::{
    BootstrapDiagnosticsState, ConnectionMonitorState, KnownPeerAddressState, NetState,
    PeerConnectivityState, RealmId,
};
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use crossfire::TrySendError;
use iroh::address_lookup::DnsAddressLookup;
use iroh::address_lookup::memory::MemoryLookup;
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
pub use streams::StreamsService;

#[derive(Clone)]
pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub realm_id: RealmId,
    pub bootstrap_nodes: Vec<NodeId>,
    pub bootstrap_endpoints: Vec<EndpointAddr>,
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
}

impl DiscoveryMethod {
    pub fn enabled_methods(&self) -> Vec<String> {
        match self {
            Self::None => Vec::new(),
            Self::N0Dns => vec!["n0_dns".to_string()],
            Self::CustomDns(_) => vec!["custom_dns".to_string()],
        }
    }

    pub fn dns_origins(&self) -> Vec<String> {
        match self {
            Self::CustomDns(origins) => origins.clone(),
            _ => Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayMethod {
    None,
    N0,
    Custom(Vec<String>),
}

impl RelayMethod {
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::N0 => "n0",
            Self::Custom(_) => "custom",
        }
    }

    pub fn relay_urls(&self) -> Vec<String> {
        match self {
            Self::Custom(relays) => relays.clone(),
            _ => Vec::new(),
        }
    }
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
            bootstrap_nodes: vec![],
            bootstrap_endpoints: vec![],
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
            .field("bootstrap_nodes", &self.bootstrap_nodes.len())
            .field("bootstrap_endpoints", &self.bootstrap_endpoints.len())
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
    attempts_total: u64,
    successes_total: u64,
    failures_total: u64,
    consecutive_failures: u64,
    last_error: Option<String>,
    last_successful: bool,
    next_attempt: Instant,
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
    bootstrap_endpoints: Vec<EndpointAddr>,
    dht: Arc<DhtHandle>,
    gossip: Arc<GossipService>,
    streams: Arc<StreamsService>,
    peer_connectivity: Arc<Mutex<PeerConnectivityManagerState>>,
    bootstrap_diagnostics: Arc<Mutex<BootstrapDiagnosticsState>>,
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
        }

        match &config.discovery_method {
            DiscoveryMethod::None => {
                // No additional configuration needed for no discovery
            }
            DiscoveryMethod::N0Dns => {
                endpoint_builder = endpoint_builder.address_lookup(DnsAddressLookup::n0_dns());
            }
            DiscoveryMethod::CustomDns(servers) => {
                for server in servers {
                    endpoint_builder =
                        endpoint_builder.address_lookup(DnsAddressLookup::builder(server.clone()));
                }
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
        let bootstrap_endpoints =
            unique_endpoint_addrs(config.bootstrap_endpoints.clone(), node_id);
        for endpoint_addr in &bootstrap_endpoints {
            address_lookup.set_endpoint_info(endpoint_addr.clone());
        }
        let mut bootstrap_nodes = config.bootstrap_nodes.clone();
        bootstrap_nodes.extend(bootstrap_endpoints.iter().map(|endpoint| endpoint.id));
        let bootstrap_nodes = unique_peer_nodes(bootstrap_nodes, node_id);
        let peer_connectivity = Arc::new(Mutex::new(PeerConnectivityManagerState::new(
            &bootstrap_nodes,
            "bootstrap_config",
        )));
        let bootstrap_diagnostics = Arc::new(Mutex::new(BootstrapDiagnosticsState::default()));
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
                bootstrap_nodes.clone(),
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
            endpoint.clone(),
            address_lookup.clone(),
            peer_connectivity.clone(),
            bootstrap_diagnostics.clone(),
            peer_connectivity_rx,
            shutdown.child_token(),
        ));

        let mut tasks = dht_resources.tasks;
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
            bootstrap_endpoints,
            dht,
            gossip,
            streams,
            peer_connectivity,
            bootstrap_diagnostics,
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
            Err(err) => {
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
        let bootstrap_nodes = self.inner.gossip.get_bootstrap_nodes();
        let configured_relay_urls = self.inner.relay_method.relay_urls();
        let monitor = self.monitor.get_status().await;
        let mut bootstrap = self.inner.bootstrap_diagnostics.lock().await.clone();
        if let Ok(size) = self.inner.dht.routing_table_size().await {
            bootstrap.routing_table_size = Some(size);
        }
        let peer_connectivity = peer_connectivity_status(&self.inner.peer_connectivity).await;
        let mut address_nodes = bootstrap_nodes.clone();
        address_nodes.extend(peer_connectivity.iter().map(|peer| peer.node_id));
        address_nodes = unique_peer_nodes(address_nodes, self.inner.node_id);
        let known_peer_addresses = known_peer_addresses(
            &self.inner.endpoint,
            &self.inner.address_lookup,
            &address_nodes,
        )
        .await;
        let warnings = net_warnings(
            &bootstrap_nodes,
            &peer_connectivity,
            &known_peer_addresses,
            &monitor,
            &bootstrap,
        );

        NetState {
            endpoint_addr: local_endpoint_addr(&self.inner.endpoint, &configured_relay_urls),
            realm_id: *self.realm_id(),
            node_id: self.node_id(),
            discovery_methods: self.inner.discovery_method.enabled_methods(),
            discovery_dns_origins: self.inner.discovery_method.dns_origins(),
            relay_method: self.inner.relay_method.method_name().to_string(),
            relay_urls: configured_relay_urls,
            bootstrap_nodes,
            bootstrap_endpoints: self
                .inner
                .bootstrap_endpoints
                .iter()
                .map(endpoint_addr_to_config_string)
                .collect(),
            monitor,
            bootstrap,
            peer_connectivity,
            known_peer_addresses,
            warnings,
        }
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
        addr = addr.with_ip_addr(socket);
    }

    addr
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
            peer.attempts_total = peer.attempts_total.saturating_add(1);
            peer.successes_total = peer.successes_total.saturating_add(1);
            peer.consecutive_failures = 0;
            peer.last_error = None;
            peer.last_successful = true;
            peer.next_attempt = now + PEER_SUCCESS_REFRESH_DELAY;
        }
    }

    fn record_failure(&mut self, node_id: NodeId, source: &str, error: String, now: Instant) {
        if self.peer_mut(node_id).is_none() {
            self.manage_peer(node_id, source, now, false);
        }
        if let Some(peer) = self.peer_mut(node_id) {
            merge_source(&mut peer.source, source);
            peer.attempts_total = peer.attempts_total.saturating_add(1);
            peer.failures_total = peer.failures_total.saturating_add(1);
            peer.consecutive_failures = peer.consecutive_failures.saturating_add(1);
            peer.last_error = Some(error);
            peer.last_successful = false;
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
                source: peer.source.clone(),
                attempts_total: peer.attempts_total,
                successes_total: peer.successes_total,
                failures_total: peer.failures_total,
                consecutive_failures: peer.consecutive_failures,
                last_error: peer.last_error.clone(),
                last_successful: peer.last_successful,
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
            attempts_total: 0,
            successes_total: 0,
            failures_total: 0,
            consecutive_failures: 0,
            last_error: None,
            last_successful: false,
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

async fn run_peer_connectivity_manager(
    dht: Arc<DhtHandle>,
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    state: Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: Arc<Mutex<BootstrapDiagnosticsState>>,
    mut event_rx: mpsc::Receiver<PeerConnectivityEvent>,
    shutdown: CancellationToken,
) {
    loop {
        drain_peer_connectivity_events(&state, &mut event_rx).await;

        let now = Instant::now();
        let due_peers = state.lock().await.due_peers(now);
        if !due_peers.is_empty() {
            diagnostics.lock().await.last_attempted_peer_count = due_peers.len();
            for peer in due_peers {
                if shutdown.is_cancelled() {
                    return;
                }
                run_peer_connectivity_attempt(
                    &dht,
                    &endpoint,
                    &address_lookup,
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

async fn run_peer_connectivity_attempt(
    dht: &DhtHandle,
    endpoint: &Endpoint,
    address_lookup: &MemoryLookup,
    state: &Arc<Mutex<PeerConnectivityManagerState>>,
    diagnostics: &Arc<Mutex<BootstrapDiagnosticsState>>,
    peer: NodeId,
) {
    let source = state.lock().await.peer_source(peer);
    {
        let mut status = diagnostics.lock().await;
        status.attempts_total = status.attempts_total.saturating_add(1);
    }

    let result = dht.bootstrap_nodes(&[peer]).await;
    let routing_table_size = dht.routing_table_size().await.ok();
    match result {
        Ok(()) => {
            state
                .lock()
                .await
                .record_success(peer, "connectivity_probe", Instant::now());
            let mut status = diagnostics.lock().await;
            status.successes_total = status.successes_total.saturating_add(1);
            status.last_error = None;
            status.last_successful = true;
            status.routing_table_size = routing_table_size;
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
            let known = known_peer_addresses(endpoint, address_lookup, &[peer]).await;
            let mut status = diagnostics.lock().await;
            status.failures_total = status.failures_total.saturating_add(1);
            status.last_error = Some(format!("peer {peer}: {err}"));
            status.last_successful = false;
            status.routing_table_size = routing_table_size;
            warn!(
                error = %err,
                peer = %peer,
                source = %source,
                routing_table_size = ?routing_table_size,
                known_peer_addresses = ?known,
                "Managed peer connectivity check failed; will retry with backoff"
            );
        }
    }
}

async fn known_peer_addresses(
    endpoint: &Endpoint,
    address_lookup: &MemoryLookup,
    nodes: &[NodeId],
) -> Vec<KnownPeerAddressState> {
    let mut peers = unique_peer_nodes(nodes.to_vec(), endpoint.id());
    let mut states = Vec::new();

    for peer in peers.drain(..) {
        if let Some(info) = address_lookup.get_endpoint_info(peer) {
            let endpoint_addr = info.into_endpoint_addr();
            states.push(peer_address_state(
                peer,
                "memory_lookup",
                endpoint_addr.addrs.into_iter().collect(),
                0,
                0,
            ));
        }

        if let Some(remote_info) = endpoint.remote_info(peer).await {
            let mut addrs = Vec::new();
            let mut active = 0;
            let mut inactive = 0;
            for addr in remote_info.addrs() {
                match addr.usage() {
                    TransportAddrUsage::Active => active += 1,
                    TransportAddrUsage::Inactive => inactive += 1,
                    _ => inactive += 1,
                }
                addrs.push(addr.addr().clone());
            }
            states.push(peer_address_state(
                peer,
                "remote_info",
                addrs,
                active,
                inactive,
            ));
        }

        if !states.iter().any(|state| state.node_id == peer) {
            states.push(peer_address_state(peer, "none", Vec::new(), 0, 0));
        }
    }

    states
}

fn peer_address_state(
    node_id: NodeId,
    source: &str,
    addrs: Vec<TransportAddr>,
    active_addresses: usize,
    inactive_addresses: usize,
) -> KnownPeerAddressState {
    let has_direct_ip = addrs.iter().any(TransportAddr::is_ip);
    let has_relay = addrs.iter().any(TransportAddr::is_relay);
    let endpoint_addr =
        (!addrs.is_empty()).then(|| EndpointAddr::from_parts(node_id, addrs.clone()));
    KnownPeerAddressState {
        node_id,
        source: source.to_string(),
        endpoint_addr,
        addresses: addrs.iter().map(transport_addr_to_string).collect(),
        has_direct_ip,
        has_relay,
        active_addresses,
        inactive_addresses,
    }
}

fn transport_addr_to_string(addr: &TransportAddr) -> String {
    match addr {
        TransportAddr::Ip(addr) => addr.to_string(),
        TransportAddr::Relay(url) => url.to_string(),
        _ => format!("{addr:?}"),
    }
}

fn net_warnings(
    bootstrap_nodes: &[NodeId],
    peer_connectivity: &[PeerConnectivityState],
    known_peer_addresses: &[KnownPeerAddressState],
    monitor: &ConnectionMonitorState,
    bootstrap: &BootstrapDiagnosticsState,
) -> Vec<String> {
    let mut warnings = Vec::new();

    if let Some(error) = &bootstrap.last_error {
        warnings.push(format!(
            "last managed peer connectivity attempt failed: {error}"
        ));
    }

    if bootstrap.failures_total > 0
        && monitor.open_connections.is_empty()
        && !peer_connectivity.is_empty()
    {
        warnings
            .push("no open p2p connections after one or more managed peer failures".to_string());
    }

    if bootstrap.routing_table_size == Some(0) && !bootstrap_nodes.is_empty() {
        warnings.push("DHT routing table is empty despite configured bootstrap peers".to_string());
    }

    for node_id in bootstrap_nodes {
        let has_known_address = known_peer_addresses
            .iter()
            .any(|state| state.node_id == *node_id && !state.addresses.is_empty());
        if !has_known_address {
            warnings.push(format!(
                "bootstrap peer {node_id} has no known addresses; add BOOTSTRAP_ENDPOINTS or enable discovery"
            ));
        }
    }

    for peer in peer_connectivity {
        if peer.consecutive_failures > 0 {
            let error = peer
                .last_error
                .as_deref()
                .unwrap_or("unknown connection error");
            warnings.push(format!(
                "managed peer {} unreachable after {} consecutive failures: {}",
                peer.node_id, peer.consecutive_failures, error
            ));
        }

        let has_known_address = known_peer_addresses
            .iter()
            .any(|state| state.node_id == peer.node_id && !state.addresses.is_empty());
        if !has_known_address {
            warnings.push(format!(
                "managed peer {} has no known addresses; add BOOTSTRAP_ENDPOINTS or enable discovery",
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
        handle.shutdown().await;
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
            if status.monitor.open_connections.len() >= expected {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }

        handle.get_status().await
    }

    async fn wait_for_bootstrap_failure(handle: &NetHandle) -> NetState {
        for _ in 0..50 {
            let status = handle.get_status().await;
            if status.bootstrap.failures_total > 0 {
                return status;
            }
            sleep(Duration::from_millis(100)).await;
        }

        handle.get_status().await
    }

    async fn wait_for_peer_failure(handle: &NetHandle, peer: NodeId) -> NetState {
        for _ in 0..50 {
            let status = handle.get_status().await;
            if status
                .peer_connectivity
                .iter()
                .any(|state| state.node_id == peer && state.consecutive_failures > 0)
            {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }

        handle.get_status().await
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
        assert!(status.monitor.open_connections.len() >= 2);
        assert!(status.monitor.observed_connections_total >= 2);
        assert_eq!(status.monitor.dropped_observations_total, 0);

        let mut connection_ids = status
            .monitor
            .open_connections
            .iter()
            .map(|connection| connection.connection_id)
            .collect::<Vec<_>>();
        connection_ids.sort_unstable();
        connection_ids.dedup();
        assert_eq!(connection_ids.len(), status.monitor.open_connections.len());

        a.shutdown().await;
        b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_known_bootstrap_peer_addresses() -> Result<()> {
        let (a, _a_dir) = test_net_handle().await?;
        let (b, _b_dir) = test_net_handle().await?;

        a.add_peer_addr(b.endpoint_addr()).await;

        let status = a.get_status().await;
        assert!(status.bootstrap_nodes.contains(&b.node_id()));
        assert!(status.known_peer_addresses.iter().any(|peer| {
            peer.node_id == b.node_id()
                && peer.source == "memory_lookup"
                && peer.has_direct_ip
                && peer
                    .endpoint_addr
                    .as_ref()
                    .is_some_and(|addr| addr.id == b.node_id())
        }));

        a.shutdown().await;
        b.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn status_reports_bootstrap_failure_warnings() -> Result<()> {
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
                bootstrap_nodes: vec![missing_peer],
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage,
        )
        .await?;

        let status = wait_for_bootstrap_failure(&handle).await;
        assert!(status.bootstrap.failures_total > 0);
        assert!(
            status
                .known_peer_addresses
                .iter()
                .any(|peer| peer.node_id == missing_peer && peer.addresses.is_empty())
        );
        assert!(
            status
                .warnings
                .iter()
                .any(|warning| warning.contains("no known addresses"))
        );
        assert!(
            status
                .warnings
                .iter()
                .any(|warning| warning.contains("managed peer connectivity attempt failed"))
        );
        assert!(
            status
                .peer_connectivity
                .iter()
                .any(|peer| peer.node_id == missing_peer && peer.consecutive_failures > 0)
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
        assert!(status.peer_connectivity.iter().any(|peer| {
            peer.node_id == missing_peer
                && peer.source.contains("stream_target")
                && peer.source.contains("stream_open")
                && peer.consecutive_failures > 0
                && peer.next_retry_in_secs.is_some()
        }));
        assert!(status.warnings.iter().any(|warning| {
            warning.contains("managed peer") && warning.contains("unreachable")
        }));

        handle.shutdown().await;
        Ok(())
    }
}
