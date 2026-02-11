#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unsafe_code)]

pub mod dht;
pub mod error;
pub mod gossip;
pub mod streams;

use std::net::SocketAddr;
use std::sync::Arc;

use aruna_core::effects::{DhtEffect, Effect, GossipEffect, NetEffect, StreamEffect};
use aruna_core::events::{DhtEvent, Event, GossipEvent, NetEvent, StreamEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::address_lookup::{DnsAddressLookup, PkarrPublisher};
use iroh::{Endpoint, EndpointAddr, RelayMode};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub use dht::DhtService;
pub use error::{NetError, Result};
pub use gossip::GossipService;
pub use streams::StreamsService;

pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub bootstrap_nodes: Vec<NodeId>,
    pub use_dns_discovery: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            secret_key: None,
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
            .field("bootstrap_nodes", &self.bootstrap_nodes.len())
            .field("use_dns_discovery", &self.use_dns_discovery)
            .finish()
    }
}

type EffectHandle = (NetEffect, oneshot::Sender<NetEvent>);

#[derive(Clone)]
pub struct NetHandle {
    inner: Arc<NetInner>,
}

struct NetInner {
    effect_tx: mpsc::Sender<EffectHandle>,
    node_id: NodeId,
    endpoint: Endpoint,
    address_lookup: MemoryLookup,
    dht: Arc<DhtService>,
    gossip: Arc<GossipService>,
    streams: Arc<StreamsService>,
    incoming_event_rx: Mutex<mpsc::Receiver<NetEvent>>,
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
                dht::rpc::DHT_ALPN.to_vec(),
                iroh_gossip::net::GOSSIP_ALPN.to_vec(),
                aruna_core::alpn::Alpn::Bao.as_bytes().to_vec(),
                aruna_core::alpn::Alpn::Automerge.as_bytes().to_vec(),
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

        let (incoming_event_tx, incoming_event_rx) = mpsc::channel::<NetEvent>(1024);

        let dht = Arc::new(
            DhtService::new(endpoint.clone(), storage.clone(), shutdown.child_token()).await?,
        );

        if !config.bootstrap_nodes.is_empty()
            && let Err(err) = dht.bootstrap_nodes(&config.bootstrap_nodes).await
        {
            warn!(error = %err, "DHT bootstrap failed; continuing without bootstrap peers");
        }

        let gossip = Arc::new(
            GossipService::new(
                endpoint.clone(),
                storage.clone(),
                config.bootstrap_nodes.clone(),
                shutdown.child_token(),
                incoming_event_tx.clone(),
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
        let streams_for_effects = streams.clone();
        let effect_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_for_effects.cancelled() => break,
                    maybe_effect = effect_rx.recv() => {
                        let Some((effect, response_tx)) = maybe_effect else { break };
                        let dht = dht_for_effects.clone();
                        let gossip = gossip_for_effects.clone();
                        let streams = streams_for_effects.clone();
                        tokio::spawn(async move {
                            let event = match effect {
                                NetEffect::Dht(dht_effect) => handle_dht_effect(&dht, dht_effect).await,
                                NetEffect::Gossip(gossip_effect) => {
                                    handle_gossip_effect(&gossip, gossip_effect).await
                                }
                                NetEffect::Stream(stream_effect) => {
                                    handle_stream_effect(
                                        &streams,
                                        &dht,
                                        &gossip,
                                        stream_effect,
                                    )
                                    .await
                                }
                            };
                            let _ = response_tx.send(event);
                        });
                    }
                }
            }
        });

        let (dht_tx, mut dht_rx) = mpsc::channel(64);
        let (gossip_conn_tx, mut gossip_conn_rx) = mpsc::channel(64);
        let (stream_tx, mut stream_rx) = mpsc::channel(64);

        let dht_server = dht.server();
        let dht_for_inbound = dht.clone();
        let gossip_for_inbound = gossip.clone();
        let dht_task = tokio::spawn(async move {
            while let Some((send, recv, peer_id)) = dht_rx.recv().await {
                let server = dht_server.clone();
                let dht = dht_for_inbound.clone();
                let gossip = gossip_for_inbound.clone();
                tokio::spawn(async move {
                    dht.add_peer(peer_id).await;
                    gossip.add_bootstrap_node(peer_id);
                    let _ = server.handle_stream(send, recv, peer_id).await;
                });
            }
        });

        let dht_for_gossip = dht.clone();
        let gossip_for_gossip = gossip.clone();
        let gossip_task = tokio::spawn(async move {
            while let Some((conn, peer_id)) = gossip_conn_rx.recv().await {
                dht_for_gossip.add_peer(peer_id).await;
                gossip_for_gossip.add_bootstrap_node(peer_id);
                if let Err(err) = gossip_for_gossip.gossip().handle_connection(conn).await {
                    warn!(error = %err, "Failed to hand connection to gossip service");
                }
            }
        });

        let streams_registry = streams.registry();
        let event_tx = incoming_event_tx.clone();
        let dht_for_streams = dht.clone();
        let gossip_for_streams = gossip.clone();
        let stream_task = tokio::spawn(async move {
            while let Some((_alpn, send, recv, peer_id)) = stream_rx.recv().await {
                dht_for_streams.add_peer(peer_id).await;
                gossip_for_streams.add_bootstrap_node(peer_id);

                let stream_id = streams_registry.register(send, recv);
                let opened = NetEvent::Stream(StreamEvent::Opened {
                    stream_id,
                    node_id: peer_id,
                });
                if event_tx.try_send(opened).is_err() {
                    warn!("Incoming event channel full; dropping inbound stream event");
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

        let inner = Arc::new(NetInner {
            effect_tx,
            node_id,
            endpoint,
            address_lookup,
            dht,
            gossip,
            streams,
            incoming_event_rx: Mutex::new(incoming_event_rx),
            shutdown,
            tasks: Mutex::new(vec![
                effect_task,
                dht_task,
                gossip_task,
                stream_task,
                accept_task,
            ]),
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
        self.inner.dht.add_peer(endpoint_addr.id).await;
    }

    pub fn take_owned_stream(&self, stream_id: u64) -> Option<streams::BiStream> {
        self.inner.streams.take_owned_stream(stream_id)
    }

    pub async fn recv_event(&self) -> Option<NetEvent> {
        let mut rx = self.inner.incoming_event_rx.lock().await;
        rx.recv().await
    }

    pub async fn shutdown(&self) {
        if self.inner.shutdown.is_cancelled() {
            return;
        }

        self.inner.shutdown.cancel();
        let _ = self.inner.gossip.gossip().shutdown().await;
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
                if self.inner.effect_tx.send((net_effect, tx)).await.is_err() {
                    return Event::Net(NetEvent::Error(
                        aruna_core::events::NetError::ChannelClosed,
                    ));
                }

                match rx.await {
                    Ok(event) => Event::Net(event),
                    Err(_) => {
                        Event::Net(NetEvent::Error(aruna_core::events::NetError::ChannelClosed))
                    }
                }
            }
            _ => Event::Net(NetEvent::Error(aruna_core::events::NetError::InvalidEffect)),
        }
    }
}

async fn handle_dht_effect(dht: &DhtService, effect: DhtEffect) -> NetEvent {
    match effect {
        DhtEffect::Put { key, value, ttl } => {
            let key_id = aruna_core::id::DhtKeyId::from_bytes(key);
            match dht.put(&key_id, value, ttl).await {
                Ok(()) => NetEvent::Dht(DhtEvent::PutComplete { key }),
                Err(e) => NetEvent::Dht(DhtEvent::Error {
                    error: aruna_core::errors::DhtError::StoreFailed(e.to_string()),
                }),
            }
        }
        DhtEffect::Get { key } => {
            let key_id = aruna_core::id::DhtKeyId::from_bytes(key);
            match dht.get(&key_id).await {
                Ok(values) => NetEvent::Dht(DhtEvent::GetResult { key, values }),
                Err(e) => NetEvent::Dht(DhtEvent::Error {
                    error: aruna_core::errors::DhtError::Other(e.to_string()),
                }),
            }
        }
    }
}

async fn handle_gossip_effect(gossip: &GossipService, effect: GossipEffect) -> NetEvent {
    match effect {
        GossipEffect::Subscribe { topic } => match gossip.subscribe(topic.clone()).await {
            Ok(()) => NetEvent::Gossip(GossipEvent::Subscribed { topic }),
            Err(e) => NetEvent::Gossip(GossipEvent::Error {
                error: aruna_core::errors::GossipError::Other(e.to_string()),
            }),
        },
        GossipEffect::Broadcast { topic, message } => {
            match gossip.broadcast(topic.clone(), message).await {
                Ok(()) => NetEvent::Gossip(GossipEvent::BroadcastComplete { topic }),
                Err(e) => NetEvent::Gossip(GossipEvent::Error {
                    error: aruna_core::errors::GossipError::BroadcastFailed(e.to_string()),
                }),
            }
        }
        GossipEffect::Unsubscribe { topic } => match gossip.unsubscribe(topic.clone()).await {
            Ok(()) => NetEvent::Gossip(GossipEvent::Unsubscribed { topic }),
            Err(e) => NetEvent::Gossip(GossipEvent::Error {
                error: aruna_core::errors::GossipError::Other(e.to_string()),
            }),
        },
    }
}

async fn handle_stream_effect(
    streams: &StreamsService,
    dht: &DhtService,
    gossip: &GossipService,
    effect: StreamEffect,
) -> NetEvent {
    match effect {
        StreamEffect::Open { node_id, alpn } => {
            dht.add_peer(node_id).await;
            gossip.add_bootstrap_node(node_id);

            match streams.open(node_id, alpn).await {
                Ok(stream_id) => NetEvent::Stream(StreamEvent::Opened { stream_id, node_id }),
                Err(e) => NetEvent::Stream(StreamEvent::Error {
                    stream_id: 0,
                    error: aruna_core::errors::StreamError::ConnectionFailed(e.to_string()),
                }),
            }
        }
        StreamEffect::Send { stream_id, data } => match streams.send(stream_id, data).await {
            Ok(bytes_sent) => NetEvent::Stream(StreamEvent::Sent {
                stream_id,
                bytes_sent,
            }),
            Err(e) => NetEvent::Stream(StreamEvent::Error {
                stream_id,
                error: aruna_core::errors::StreamError::Other(e.to_string()),
            }),
        },
        StreamEffect::Recv {
            stream_id,
            max_bytes,
        } => match streams.recv(stream_id, max_bytes).await {
            Ok(data) => NetEvent::Stream(StreamEvent::Received { stream_id, data }),
            Err(e) => NetEvent::Stream(StreamEvent::Error {
                stream_id,
                error: aruna_core::errors::StreamError::Other(e.to_string()),
            }),
        },
        StreamEffect::Close { stream_id } => {
            streams.close(stream_id);
            NetEvent::Stream(StreamEvent::Closed { stream_id })
        }
        StreamEffect::RequestOwned { stream_id } => {
            if streams.request_owned(stream_id) {
                NetEvent::Stream(StreamEvent::OwnershipReady { stream_id })
            } else {
                NetEvent::Stream(StreamEvent::Error {
                    stream_id,
                    error: aruna_core::errors::StreamError::Other(
                        "Stream not found or busy".to_string(),
                    ),
                })
            }
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
