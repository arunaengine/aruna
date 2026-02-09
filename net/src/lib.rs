#![warn(missing_debug_implementations, rust_2018_idioms)]
#![deny(unsafe_code)]

pub mod dht;
pub mod error;
pub mod gossip;
pub mod streams;

use aruna_core::effects::{DhtEffect, Effect, GossipEffect, NetEffect, StreamEffect};
use aruna_core::events::{DhtEvent, Event, GossipEvent, NetEvent, StreamEvent};
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use iroh::Endpoint;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub use dht::DhtService;
pub use error::{NetError, Result};
pub use gossip::GossipService;
pub use streams::StreamsService;

pub struct NetConfig {
    pub bind_addr: SocketAddr,
    pub secret_key: Option<iroh::SecretKey>,
    pub bootstrap_nodes: Vec<NodeId>,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            // SAFETY: This is a valid socket address literal
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            secret_key: None,
            bootstrap_nodes: vec![],
        }
    }
}

impl std::fmt::Debug for NetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetConfig")
            .field("bind_addr", &self.bind_addr)
            .field("has_secret_key", &self.secret_key.is_some())
            .field("bootstrap_nodes", &self.bootstrap_nodes.len())
            .finish()
    }
}

type EffectHandle = (NetEffect, oneshot::Sender<NetEvent>);

#[derive(Clone)]
pub struct NetHandle {
    effect_tx: mpsc::Sender<EffectHandle>,
    node_id: NodeId,
    /// Receiver for incoming network events (e.g., gossip messages)
    /// Wrapped in Arc<Mutex> for Clone and async-safe access
    incoming_event_rx: Arc<Mutex<mpsc::Receiver<NetEvent>>>,
}

impl std::fmt::Debug for NetHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetHandle")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl NetHandle {
    pub async fn new(config: NetConfig, storage: StorageHandle) -> Result<Self> {
        // 1. Create endpoint
        let secret_key = config
            .secret_key
            .unwrap_or_else(|| iroh::SecretKey::generate(&mut rand::rng()));
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|e| NetError::Bootstrap(e.to_string()))?;

        // 2. Verify endpoint is online
        let _ = endpoint.online().await;
        // NodeId is now iroh::PublicKey directly
        let node_id = endpoint.id();

        let shutdown = CancellationToken::new();

        // 3. Initialize DHT
        let dht = Arc::new(
            DhtService::new(endpoint.clone(), storage.clone(), shutdown.child_token()).await?,
        );
        dht.bootstrap_nodes(&config.bootstrap_nodes).await?;

        // 4. Initialize Gossip with event forwarder channel
        let (gossip_event_tx, gossip_event_rx) = mpsc::channel::<NetEvent>(256);
        let gossip = Arc::new(
            GossipService::new(
                endpoint.clone(),
                storage.clone(),
                shutdown.child_token(),
                gossip_event_tx,
            )
            .await?,
        );
        gossip.restore_subscriptions().await?;

        // 5. Initialize Streams
        let streams = Arc::new(StreamsService::new(
            endpoint.clone(),
            shutdown.child_token(),
        ));

        // 6. Create effect channel and spawn handler
        let (effect_tx, mut effect_rx) = mpsc::channel::<EffectHandle>(256);

        let dht_clone = dht.clone();
        let gossip_clone = gossip.clone();
        let streams_clone = streams.clone();

        tokio::spawn(async move {
            while let Some((effect, response_tx)) = effect_rx.recv().await {
                let event = match effect {
                    NetEffect::Dht(dht_effect) => handle_dht_effect(&dht_clone, dht_effect).await,
                    NetEffect::Gossip(gossip_effect) => {
                        handle_gossip_effect(&gossip_clone, gossip_effect).await
                    }
                    NetEffect::Stream(stream_effect) => {
                        handle_stream_effect(&streams_clone, stream_effect).await
                    }
                };
                let _ = response_tx.send(event);
            }
        });

        // 7. Start accept loop with wired handlers
        let (dht_tx, mut dht_rx) = mpsc::channel(64);
        let (stream_tx, mut stream_rx) = mpsc::channel(64);

        // DHT handler task
        let dht_server = dht.server();
        tokio::spawn(async move {
            while let Some((send, recv, peer_id)) = dht_rx.recv().await {
                let server = dht_server.clone();
                tokio::spawn(async move {
                    let _ = server.handle_stream(send, recv, peer_id).await;
                });
            }
        });

        // Stream handler task - register incoming streams
        let streams_registry = streams.registry();
        tokio::spawn(async move {
            while let Some((_alpn, send, recv, _peer_id)) = stream_rx.recv().await {
                // Register the stream so it's not dropped
                let _stream_id = streams_registry.register(send, recv);
            }
        });

        // Accept loop
        let endpoint_clone = endpoint.clone();
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            streams::run_accept_loop(endpoint_clone, dht_tx, stream_tx, shutdown_clone).await;
        });

        Ok(Self {
            effect_tx,
            node_id,
            incoming_event_rx: Arc::new(Mutex::new(gossip_event_rx)),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Receive the next incoming network event, waiting if necessary.
    /// Returns `None` if the channel is closed.
    ///
    /// # Single-Consumer Design
    ///
    /// This method is designed for **single-consumer usage only**. It holds an
    /// async mutex guard while awaiting the next event, which means:
    /// - Only one task can receive events at a time
    /// - Concurrent calls will serialize, with one blocking until the other completes
    /// - For multi-consumer scenarios, use a separate fan-out mechanism
    ///
    /// Incoming events include:
    /// - `GossipEvent::Message` when messages arrive on subscribed topics
    /// - `GossipEvent::Error` when a subscription terminates unexpectedly
    pub async fn recv_event(&self) -> Option<NetEvent> {
        let mut rx_guard = self.incoming_event_rx.lock().await;
        rx_guard.recv().await
    }

    pub async fn shutdown(&self) {
        // Close effect channel to trigger cleanup
        // The shutdown token will propagate to all services
    }
}

#[async_trait]
impl Handle for NetHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Net(net_effect) => {
                let (tx, rx) = oneshot::channel();
                if self.effect_tx.send((net_effect, tx)).await.is_err() {
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
                Ok(values) => {
                    // get() now returns Vec<DhtEntry> directly
                    NetEvent::Dht(DhtEvent::GetResult { key, values })
                }
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

async fn handle_stream_effect(streams: &StreamsService, effect: StreamEffect) -> NetEvent {
    match effect {
        StreamEffect::Open { node_id, alpn } => match streams.open(node_id, alpn).await {
            Ok(stream_id) => NetEvent::Stream(StreamEvent::Opened { stream_id, node_id }),
            Err(e) => NetEvent::Stream(StreamEvent::Error {
                stream_id: 0,
                error: aruna_core::errors::StreamError::ConnectionFailed(e.to_string()),
            }),
        },
        StreamEffect::Send { stream_id, data } => match streams.send(stream_id, data).await {
            Ok(bytes_sent) => NetEvent::Stream(StreamEvent::Sent {
                stream_id,
                bytes_sent,
            }),
            Err(e) => NetEvent::Stream(StreamEvent::Error {
                stream_id,
                error: aruna_core::errors::StreamError::Other(e.to_string()),
            }),
        }
        StreamEffect::Close { stream_id } => {
            streams.close(stream_id);
            NetEvent::Stream(StreamEvent::Closed { stream_id })
        }
        StreamEffect::RequestOwned { stream_id } => {
            match streams.take_owned(stream_id) {
                Some(_) => NetEvent::Stream(StreamEvent::OwnershipReady { stream_id }),
                None => NetEvent::Stream(StreamEvent::Error {
                    stream_id,
                    error: aruna_core::errors::StreamError::Other(
                        "Stream not found or busy".to_string(),
                    ),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_net_handle_creates() -> Result<()> {
        let temp_dir = std::env::temp_dir().join(format!("aruna-net-test-{}", std::process::id()));
        let path = temp_dir
            .to_str()
            .ok_or_else(|| NetError::Io("Invalid temp path".to_string()))?;
        let storage =
            aruna_storage::FjallStorage::open(path).map_err(|e| NetError::Io(e.to_string()))?;
        let config = NetConfig::default();
        let _handle = NetHandle::new(config, storage).await?;
        Ok(())
    }
}
