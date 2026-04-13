#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unsafe_code)]

pub mod dht;
mod effect_handlers;
pub mod error;
pub mod gossip;
pub mod streams;

use std::net::SocketAddr;
use std::sync::Arc;

use aruna_core::alpn::Alpn;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetError as CoreNetError, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::structs::RealmId;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use crossfire::TrySendError;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use parking_lot::RwLock;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, warn};

pub use dht::DhtHandle;
pub use error::{NetError, Result};
pub use gossip::GossipService;
pub use streams::StreamsService;

pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub realm_id: RealmId,
    pub bootstrap_nodes: Vec<NodeId>,
    pub use_dns_discovery: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            secret_key: None,
            realm_id: RealmId::from_bytes([0u8; 32]),
            bootstrap_nodes: vec![],
            use_dns_discovery: true,
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
            .field("use_dns_discovery", &self.use_dns_discovery)
            .finish()
    }
}

type EffectHandle = (NetEffect, oneshot::Sender<NetEvent>, Span);

#[async_trait]
pub trait InboundEventHandler: Send + Sync {
    async fn handle_gossip_message(&self, topic: TopicId, sender: NodeId, data: Vec<u8>);
    async fn handle_incoming_stream(&self, alpn: Alpn, stream: streams::BiStream, node_id: NodeId);
}

#[derive(Clone)]
pub struct NetHandle {
    inner: Arc<NetInner>,
}

struct NetInner {
    effect_tx: mpsc::Sender<EffectHandle>,
    node_id: NodeId,
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    dht: Arc<DhtHandle>,
    gossip: Arc<GossipService>,
    streams: Arc<StreamsService>,
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
        let secret_key = config
            .secret_key
            .unwrap_or_else(|| iroh::SecretKey::generate(&mut rand::rng()));

        let address_lookup = MemoryLookup::new();
        let mut endpoint_builder = Endpoint::empty_builder(RelayMode::Disabled)
            .secret_key(secret_key)
            .address_lookup(address_lookup.clone())
            .alpns(vec![
                Alpn::Dht.as_bytes().to_vec(),
                Alpn::Gossip.as_bytes().to_vec(),
                Alpn::Bao.as_bytes().to_vec(),
                Alpn::Automerge.as_bytes().to_vec(),
                Alpn::Metadata.as_bytes().to_vec(),
            ]);

        if config.use_dns_discovery {
            endpoint_builder = endpoint_builder
                .address_lookup(PkarrPublisher::n0_dns())
                .address_lookup(DnsAddressLookup::n0_dns());
        }

        let endpoint_builder = endpoint_builder
            .bind_addr(config.bind_addr)
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;

        let endpoint = endpoint_builder
            .bind()
            .await
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;
        address_lookup.add_endpoint_info(local_endpoint_addr(&endpoint));

        let node_id = endpoint.id();
        let shutdown = CancellationToken::new();

        let (gossip_msg_tx, mut gossip_msg_rx) = mpsc::channel::<(TopicId, NodeId, Vec<u8>)>(1024);
        let inbound_handler: Arc<RwLock<Option<Arc<dyn InboundEventHandler>>>> =
            Arc::new(RwLock::new(None));

        let (dht_handle, dht_resources) =
            DhtHandle::spawn(endpoint.clone(), storage.clone(), shutdown.child_token())?;
        let dht = Arc::new(dht_handle);

        if !config.bootstrap_nodes.is_empty()
            && let Err(err) = dht.bootstrap_nodes(&config.bootstrap_nodes).await
        {
            warn!(error = %err, "DHT bootstrap failed; continuing without bootstrap peers");
        }

        let gossip = Arc::new(
            GossipService::new(
                endpoint.clone(),
                storage.clone(),
                dht.clone(),
                config.realm_id.clone(),
                config.bootstrap_nodes.clone(),
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

        let mut tasks = dht_resources.tasks;
        tasks.extend(vec![
            effect_task,
            dht_task,
            gossip_task,
            gossip_event_task,
            stream_task,
            accept_task,
        ]);

        let inner = Arc::new(NetInner {
            effect_tx,
            node_id,
            endpoint,
            address_lookup,
            dht,
            gossip,
            streams,
            inbound_handler,
            shutdown,
            tasks: Mutex::new(tasks),
        });

        Ok(Self { inner })
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        local_endpoint_addr(&self.inner.endpoint)
    }

    pub async fn add_peer_addr(&self, endpoint_addr: EndpointAddr) {
        if endpoint_addr.id == self.inner.node_id {
            return;
        }

        self.inner
            .address_lookup
            .add_endpoint_info(endpoint_addr.clone());
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
        }

        self.inner.streams.open(node_id, alpn).await
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
}

fn local_endpoint_addr(endpoint: &Endpoint) -> EndpointAddr {
    let observed = endpoint.addr();
    let mut addr = EndpointAddr::new(endpoint.id());

    for relay in observed.relay_urls().cloned() {
        addr = addr.with_relay_url(relay);
    }
    for socket in endpoint.bound_sockets() {
        addr = addr.with_ip_addr(socket);
    }

    addr
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
            use_dns_discovery: false,
            ..NetConfig::default()
        };

        let handle = NetHandle::new(config, storage).await?;
        assert_ne!(handle.node_id().as_bytes(), &[0u8; 32]);
        handle.shutdown().await;
        Ok(())
    }
}
