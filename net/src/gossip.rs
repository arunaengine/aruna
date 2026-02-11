use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::GossipError;
use aruna_core::events::{Event, GossipEvent, NetEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{NodeId, TopicId};
use aruna_storage::StorageHandle;
use bytes::Bytes;
use byteview::ByteView;
use iroh::Endpoint;
use iroh_gossip::api::GossipSender;
use iroh_gossip::net::Gossip;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::error::{NetError, Result};

const GOSSIP_SUBSCRIPTIONS_KEYSPACE: &str = "gossip_subscriptions";

pub struct GossipService {
    gossip: Gossip,
    storage: StorageHandle,
    subscriptions: Arc<RwLock<HashMap<TopicId, (CancellationToken, GossipSender)>>>,
    bootstrap_nodes: Arc<RwLock<Vec<NodeId>>>,
    shutdown: CancellationToken,
    /// Channel to forward incoming gossip messages as events to the core event stream
    event_tx: mpsc::Sender<NetEvent>,
}

impl GossipService {
    pub async fn new(
        endpoint: Endpoint,
        storage: StorageHandle,
        bootstrap_nodes: Vec<NodeId>,
        shutdown: CancellationToken,
        event_tx: mpsc::Sender<NetEvent>,
    ) -> Result<Self> {
        let gossip = Gossip::builder().spawn(endpoint);

        Ok(Self {
            gossip,
            storage,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            bootstrap_nodes: Arc::new(RwLock::new(bootstrap_nodes)),
            shutdown,
            event_tx,
        })
    }

    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    pub async fn restore_subscriptions(&self) -> Result<()> {
        let effect = Effect::Storage(StorageEffect::Read {
            key_space: GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
            key: ByteView::from(b"topics".as_slice()),
            txn_id: None,
        });

        if let Event::Storage(StorageEvent::ReadResult {
            value: Some(data), ..
        }) = self.storage.send_effect(effect).await
        {
            let topics: Vec<TopicId> = postcard::from_bytes(&data).unwrap_or_default();
            for topic in topics {
                let _ = self.subscribe(topic).await;
            }
        }

        Ok(())
    }

    pub async fn subscribe(&self, topic: TopicId) -> Result<()> {
        if self.subscriptions.read().contains_key(&topic) {
            return Err(NetError::Gossip("Already subscribed".to_string()));
        }

        let cancel = self.shutdown.child_token();

        let bootstrap_nodes = self.bootstrap_nodes.read().clone();
        let gossip_topic = self
            .gossip
            .subscribe(topic.to_iroh_topic(), bootstrap_nodes)
            .await
            .map_err(|e| NetError::Gossip(e.to_string()))?;

        let (sender, mut stream) = gossip_topic.split();

        self.subscriptions
            .write()
            .insert(topic.clone(), (cancel.clone(), sender));
        self.persist_subscriptions().await;

        let subscriptions = self.subscriptions.clone();
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            use futures::stream::StreamExt;
            let mut unexpected_termination = false;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    event = stream.next() => {
                        match event {
                            Some(Ok(event)) => {
                                if let iroh_gossip::api::Event::Received(msg) = event {
                                    // Forward incoming message as a GossipEvent::Message
                                    let gossip_event = GossipEvent::Message {
                                        topic: topic.clone(),
                                        sender: msg.delivered_from,
                                        data: msg.content.to_vec(),
                                    };
                                    // Use try_send to avoid blocking on backpressure
                                    match event_tx.try_send(NetEvent::Gossip(gossip_event)) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            warn!(
                                                topic = %topic,
                                                "Gossip event channel full, dropping message"
                                            );
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            // Event channel closed, stop processing
                                            break;
                                        }
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                // Stream error - emit error event and terminate
                                warn!(topic = %topic, error = %e, "Gossip subscription stream error");
                                unexpected_termination = true;
                                break;
                            }
                            None => {
                                // Stream closed unexpectedly
                                warn!(topic = %topic, "Gossip subscription stream closed unexpectedly");
                                unexpected_termination = true;
                                break;
                            }
                        }
                    }
                }
            }
            // Emit error event if subscription terminated unexpectedly
            if unexpected_termination {
                let error_event = GossipEvent::Error {
                    error: GossipError::Other(format!(
                        "Subscription for topic {} terminated unexpectedly",
                        topic
                    )),
                };
                // Best-effort send - if channel is full or closed, we can't do much
                let _ = event_tx.try_send(NetEvent::Gossip(error_event));
            }
            subscriptions.write().remove(&topic);
        });

        Ok(())
    }

    pub fn add_bootstrap_node(&self, node_id: NodeId) {
        let mut nodes = self.bootstrap_nodes.write();
        if !nodes.contains(&node_id) {
            nodes.push(node_id);
        }
    }

    pub async fn broadcast(&self, topic: TopicId, message: Vec<u8>) -> Result<()> {
        let sender = {
            let guard = self.subscriptions.read();
            match guard.get(&topic) {
                Some((_, sender)) => sender.clone(),
                None => return Err(NetError::Gossip("Not subscribed".to_string())),
            }
        };

        sender
            .broadcast(Bytes::from(message))
            .await
            .map_err(|e| NetError::Gossip(e.to_string()))?;

        Ok(())
    }

    pub async fn unsubscribe(&self, topic: TopicId) -> Result<()> {
        let removed = self.subscriptions.write().remove(&topic);
        if let Some((cancel, _sender)) = removed {
            cancel.cancel();
            self.persist_subscriptions().await;
            Ok(())
        } else {
            Err(NetError::Gossip("Not subscribed".to_string()))
        }
    }

    async fn persist_subscriptions(&self) {
        let topics: Vec<TopicId> = self.subscriptions.read().keys().cloned().collect();
        let Ok(data) = postcard::to_allocvec(&topics) else {
            // Serialization failed - skip persisting
            return;
        };

        let effect = Effect::Storage(StorageEffect::Write {
            key_space: GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
            key: ByteView::from(b"topics".as_slice()),
            value: ByteView::from(data),
            txn_id: None,
        });

        let _ = self.storage.send_effect(effect).await;
    }
}

impl std::fmt::Debug for GossipService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipService")
            .field("subscriptions", &self.subscriptions.read().len())
            .finish()
    }
}
