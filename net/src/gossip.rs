use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::consts::GOSSIP_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
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

use crate::DhtHandle;
use crate::error::{NetError, Result};

const GOSSIP_TOPIC_ANNOUNCE_TTL: Duration = Duration::from_secs(60 * 60);
const GOSSIP_TOPIC_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(30 * 60);

#[derive(Debug)]
struct TopicSubscription {
    cancel: CancellationToken,
    sender: GossipSender,
}

pub struct GossipService {
    gossip: Gossip,
    storage: StorageHandle,
    dht: Arc<DhtHandle>,
    local_node_id: NodeId,
    subscriptions: Arc<RwLock<HashMap<TopicId, TopicSubscription>>>,
    bootstrap_nodes: Arc<RwLock<Vec<NodeId>>>,
    shutdown: CancellationToken,
    /// Channel to forward incoming gossip messages.
    event_tx: mpsc::Sender<(TopicId, NodeId, Vec<u8>)>,
}

impl GossipService {
    pub async fn new(
        endpoint: Endpoint,
        storage: StorageHandle,
        dht: Arc<DhtHandle>,
        bootstrap_nodes: Vec<NodeId>,
        shutdown: CancellationToken,
        event_tx: mpsc::Sender<(TopicId, NodeId, Vec<u8>)>,
    ) -> Result<Self> {
        let gossip = Gossip::builder().spawn(endpoint);
        let local_node_id = dht.local_id();

        Ok(Self {
            gossip,
            storage,
            dht,
            local_node_id,
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
            for topic in decode_persisted_subscriptions(&data) {
                let _ = self.subscribe(topic).await;
            }
        }

        Ok(())
    }

    pub async fn subscribe(&self, topic: TopicId) -> Result<()> {
        if self.subscriptions.read().contains_key(&topic) {
            return Ok(());
        }

        self.announce_topic_subscription(&topic).await?;
        let bootstrap_nodes = self.lookup_topic_bootstrap_nodes(&topic).await?;

        let cancel = self.shutdown.child_token();

        let gossip_topic = self
            .gossip
            .subscribe(topic.to_iroh_topic(), bootstrap_nodes)
            .await
            .map_err(|e| NetError::Gossip(e.to_string()))?;

        let (sender, mut stream) = gossip_topic.split();

        self.subscriptions.write().insert(
            topic.clone(),
            TopicSubscription {
                cancel: cancel.clone(),
                sender,
            },
        );
        self.persist_subscriptions().await;

        let subscriptions = self.subscriptions.clone();
        let event_tx = self.event_tx.clone();
        let reannounce_cancel = cancel.clone();
        let reannounce_topic = topic.clone();
        let reannounce_dht = self.dht.clone();
        let reannounce_local_node_id = self.local_node_id;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = reannounce_cancel.cancelled() => break,
                    _ = tokio::time::sleep(GOSSIP_TOPIC_REANNOUNCE_INTERVAL) => {
                        if let Err(error) = announce_topic_subscription(
                            &reannounce_dht,
                            reannounce_local_node_id,
                            &reannounce_topic,
                        ).await {
                            warn!(topic = %reannounce_topic, error = %error, "Failed to refresh gossip topic announcement");
                        }
                    }
                }
            }
        });
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
                                    match event_tx.try_send((
                                        topic.clone(),
                                        msg.delivered_from,
                                        msg.content.to_vec(),
                                    )) {
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
                                warn!(topic = %topic, error = %e, "Gossip subscription stream error");
                                unexpected_termination = true;
                                break;
                            }
                            None => {
                                warn!(topic = %topic, "Gossip subscription stream closed unexpectedly");
                                unexpected_termination = true;
                                break;
                            }
                        }
                    }
                }
            }
            if unexpected_termination {
                warn!(topic = %topic, "Subscription terminated unexpectedly");
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

    async fn announce_topic_subscription(&self, topic: &TopicId) -> Result<()> {
        announce_topic_subscription(&self.dht, self.local_node_id, topic).await
    }

    async fn lookup_topic_bootstrap_nodes(&self, topic: &TopicId) -> Result<Vec<NodeId>> {
        let configured_nodes = self.bootstrap_nodes.read().clone();
        let mut seen = HashSet::new();
        let mut bootstrap_nodes = Vec::new();

        for node_id in self.lookup_bootstrap_candidates(topic).await?.into_iter().chain(configured_nodes.into_iter()) {
            if node_id == self.local_node_id {
                continue;
            }
            if seen.insert(node_id) {
                bootstrap_nodes.push(node_id);
            }
        }

        Ok(bootstrap_nodes)
    }

    async fn lookup_bootstrap_candidates(&self, topic: &TopicId) -> Result<Vec<NodeId>> {
        let topic_key = aruna_core::keys::gossip_peer_key(topic);
        let mut candidates = self.lookup_nodes_for_key(&topic_key).await?;
        if let TopicId::AutomergeDocument(document_key) = topic {
            candidates.extend(self.lookup_nodes_for_key(document_key).await?);
        }
        Ok(candidates)
    }

    async fn lookup_nodes_for_key(&self, dht_key: &aruna_core::DhtKeyId) -> Result<Vec<NodeId>> {
        let entries = self.dht.get(dht_key).await.map_err(|e| {
            NetError::Gossip(format!(
                "Failed to lookup gossip topic bootstrap nodes: {e}"
            ))
        })?;
        Ok(entries.into_iter().map(|entry| entry.node_id).collect())
    }

    pub async fn broadcast(&self, topic: TopicId, message: Vec<u8>) -> Result<()> {
        let sender = {
            let guard = self.subscriptions.read();
            match guard.get(&topic) {
                Some(subscription) => subscription.sender.clone(),
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
        if let Some(subscription) = removed {
            subscription.cancel.cancel();
            self.persist_subscriptions().await;
            Ok(())
        } else {
            Err(NetError::Gossip("Not subscribed".to_string()))
        }
    }

    async fn persist_subscriptions(&self) {
        let persisted: Vec<TopicId> = self
            .subscriptions
            .read()
            .iter()
            .map(|(topic, _)| topic.clone())
            .collect();

        let Ok(data) = postcard::to_allocvec(&persisted) else {
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

async fn announce_topic_subscription(
    dht: &DhtHandle,
    local_node_id: NodeId,
    topic: &TopicId,
) -> Result<()> {
    let topic_key = aruna_core::keys::gossip_peer_key(topic);
    dht.put(
        &topic_key,
        local_node_id.as_bytes().to_vec(),
        GOSSIP_TOPIC_ANNOUNCE_TTL,
    )
    .await
    .map_err(|e| NetError::Gossip(format!("Failed to announce gossip topic in DHT: {e}")))?;

    if let TopicId::AutomergeDocument(document_key) = topic {
        dht.put(
            document_key,
            local_node_id.as_bytes().to_vec(),
            GOSSIP_TOPIC_ANNOUNCE_TTL,
        )
        .await
        .map_err(|e| NetError::Gossip(format!("Failed to announce automerge document in DHT: {e}")))?;
    }

    Ok(())
}

fn decode_persisted_subscriptions(bytes: &[u8]) -> Vec<TopicId> {
    postcard::from_bytes::<Vec<TopicId>>(bytes).unwrap_or_default()
}

impl std::fmt::Debug for GossipService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipService")
            .field("subscriptions", &self.subscriptions.read().len())
            .finish()
    }
}
