use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::id::{NodeId, TopicId};
use aruna_core::keyspaces::GOSSIP_SUBSCRIPTIONS_KEYSPACE;
use aruna_core::structs::RealmId;
use aruna_storage::StorageHandle;
use bytes::Bytes;
use byteview::ByteView;
use iroh::{Endpoint, PublicKey};
use iroh_gossip::api::GossipSender;
use iroh_gossip::net::Gossip;
use parking_lot::RwLock;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};

use crate::DhtHandle;
use crate::error::{NetError, Result};
use aruna_core::DhtKeyId;
use aruna_core::keys::gossip_peer_key;

const GOSSIP_TOPIC_ANNOUNCE_TTL: Duration = Duration::from_secs(60 * 60);
const GOSSIP_TOPIC_REANNOUNCE_INTERVAL: Duration = Duration::from_secs(30 * 60);
const GOSSIP_RESUBSCRIBE_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct TopicSubscription {
    cancel: CancellationToken,
    sender: GossipSender,
}

#[derive(Clone)]
pub struct GossipService {
    gossip: Gossip,
    storage: StorageHandle,
    dht: Arc<DhtHandle>,
    local_node_id: NodeId,
    local_realm_id: RealmId,
    subscriptions: Arc<RwLock<HashMap<TopicId, TopicSubscription>>>,
    pending_subscriptions: Arc<Mutex<HashMap<TopicId, Arc<Notify>>>>,
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
        local_realm_id: RealmId,
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
            local_realm_id,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pending_subscriptions: Arc::new(Mutex::new(HashMap::new())),
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
                if let Err(error) = self.subscribe(topic.clone()).await {
                    warn!(topic = %topic, error = %error, "Failed to restore persisted gossip subscription");
                }
            }
        }

        Ok(())
    }

    pub async fn subscribe(&self, topic: TopicId) -> Result<()> {
        subscribe_owned(
            self.gossip.clone(),
            self.storage.clone(),
            self.dht.clone(),
            self.local_node_id,
            self.local_realm_id,
            self.subscriptions.clone(),
            self.pending_subscriptions.clone(),
            self.bootstrap_nodes.clone(),
            self.shutdown.clone(),
            self.event_tx.clone(),
            topic,
        )
        .await
    }

    pub fn add_bootstrap_node(&self, node_id: NodeId) {
        let mut nodes = self.bootstrap_nodes.write();
        if !nodes.contains(&node_id) {
            nodes.push(node_id);
        }
    }

    pub fn get_bootstrap_nodes(&self) -> Vec<PublicKey> {
        self.bootstrap_nodes.read().clone()
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
        let persisted: Vec<TopicId> = self.subscriptions.read().keys().cloned().collect();

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

#[allow(clippy::too_many_arguments)]
async fn subscribe_owned(
    gossip: Gossip,
    storage: StorageHandle,
    dht: Arc<DhtHandle>,
    local_node_id: NodeId,
    local_realm_id: RealmId,
    subscriptions: Arc<RwLock<HashMap<TopicId, TopicSubscription>>>,
    pending_subscriptions: Arc<Mutex<HashMap<TopicId, Arc<Notify>>>>,
    bootstrap_nodes_state: Arc<RwLock<Vec<NodeId>>>,
    shutdown: CancellationToken,
    event_tx: mpsc::Sender<(TopicId, NodeId, Vec<u8>)>,
    topic: TopicId,
) -> Result<()> {
    let pending_topic = topic.clone();

    loop {
        let waiter = {
            let mut pending = pending_subscriptions.lock().await;
            let already_subscribed = {
                let guard = subscriptions.read();
                guard.contains_key(&topic)
            };
            if already_subscribed {
                return Ok(());
            }

            if let Some(waiter) = pending.get(&topic) {
                Some(waiter.clone())
            } else {
                pending.insert(topic.clone(), Arc::new(Notify::new()));
                None
            }
        };

        match waiter {
            Some(waiter) => waiter.notified().await,
            None => break,
        }
    }

    let result = async {
        if let Err(error) = announce_topic_subscription(&dht, local_node_id, &local_realm_id, &topic).await {
            warn!(topic = %topic, error = %error, "Failed to announce gossip subscription in DHT");
        }
        let bootstrap_nodes = lookup_topic_bootstrap_nodes_owned(
            &dht,
            local_node_id,
            &local_realm_id,
            &bootstrap_nodes_state,
            &topic,
        )
        .await?;
        let bootstrap_node_count = bootstrap_nodes.len();

        let cancel = shutdown.child_token();
        let gossip_topic = gossip
            .subscribe(topic.to_iroh_topic(), bootstrap_nodes)
            .await
            .map_err(|e| NetError::Gossip(e.to_string()))?;

        let (sender, mut stream) = gossip_topic.split();

        {
            let mut guard = subscriptions.write();
            guard.insert(
                topic.clone(),
                TopicSubscription {
                    cancel: cancel.clone(),
                    sender,
                },
            );
        }
        persist_subscriptions(&storage, &subscriptions).await;
        trace!(
            event = "gossip.subscribed",
            topic = %topic,
            bootstrap_nodes = bootstrap_node_count,
            "Subscribed to gossip topic"
        );

        let reannounce_cancel = cancel.clone();
        let reannounce_topic = topic.clone();
        let reannounce_dht = dht.clone();
        let reannounce_realm_id = local_realm_id;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = reannounce_cancel.cancelled() => break,
                    _ = tokio::time::sleep(GOSSIP_TOPIC_REANNOUNCE_INTERVAL) => {
                        if let Err(error) = announce_topic_subscription(
                            &reannounce_dht,
                            local_node_id,
                            &reannounce_realm_id,
                            &reannounce_topic,
                        ).await {
                            warn!(topic = %reannounce_topic, error = %error, "Failed to refresh gossip topic announcement");
                        }
                    }
                }
            }
        });

        let subscriptions_for_stream = subscriptions.clone();
        let pending_for_stream = pending_subscriptions.clone();
        let storage_for_stream = storage.clone();
        let gossip_for_stream = gossip.clone();
        let dht_for_stream = dht.clone();
        let bootstrap_nodes_for_stream = bootstrap_nodes_state.clone();
        let shutdown_for_stream = shutdown.clone();
        let stream_realm_id = local_realm_id;
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
                                    trace!(
                                        event = "gossip.received",
                                        topic = %topic,
                                        sender = %msg.delivered_from,
                                        message_len = msg.content.len(),
                                        "Received gossip message"
                                    );
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
                                        Err(mpsc::error::TrySendError::Closed(_)) => break,
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

            {
                let mut guard = subscriptions_for_stream.write();
                if let Some(subscription) = guard.remove(&topic) {
                    subscription.cancel.cancel();
                }
            }

            if unexpected_termination {
                tokio::task::spawn_blocking(move || {
                    std::thread::sleep(GOSSIP_RESUBSCRIBE_DELAY);
                    if shutdown_for_stream.is_cancelled() {
                        return;
                    }
                    let runtime = tokio::runtime::Handle::current();
                    if let Err(error) = runtime.block_on(subscribe_owned(
                        gossip_for_stream,
                        storage_for_stream,
                        dht_for_stream,
                        local_node_id,
                        stream_realm_id,
                        subscriptions_for_stream,
                        pending_for_stream,
                        bootstrap_nodes_for_stream,
                        shutdown_for_stream,
                        event_tx,
                        topic.clone(),
                    )) {
                        warn!(topic = %topic, error = %error, "Failed to restore gossip subscription");
                    }
                });
            }
        });

        Ok(())
    }
    .await;

    let notify = {
        let mut pending = pending_subscriptions.lock().await;
        pending.remove(&pending_topic)
    };
    if let Some(notify) = notify {
        notify.notify_waiters();
    }

    result
}

async fn lookup_topic_bootstrap_nodes_owned(
    dht: &Arc<DhtHandle>,
    local_node_id: NodeId,
    local_realm_id: &RealmId,
    bootstrap_nodes_state: &Arc<RwLock<Vec<NodeId>>>,
    topic: &TopicId,
) -> Result<Vec<NodeId>> {
    let configured_nodes = {
        let guard = bootstrap_nodes_state.read();
        guard.clone()
    };
    let mut seen = HashSet::new();
    let mut bootstrap_nodes = Vec::new();

    for node_id in lookup_bootstrap_candidates_owned(dht, local_realm_id, topic)
        .await?
        .into_iter()
        .chain(configured_nodes)
    {
        if node_id == local_node_id {
            continue;
        }
        if seen.insert(node_id) {
            bootstrap_nodes.push(node_id);
        }
    }

    Ok(bootstrap_nodes)
}

async fn lookup_bootstrap_candidates_owned(
    dht: &Arc<DhtHandle>,
    local_realm_id: &RealmId,
    topic: &TopicId,
) -> Result<Vec<NodeId>> {
    let topic_key = gossip_peer_key(topic);
    lookup_nodes_for_key_owned(dht, &topic_key, local_realm_id).await
}

async fn lookup_nodes_for_key_owned(
    dht: &Arc<DhtHandle>,
    dht_key: &DhtKeyId,
    local_realm_id: &RealmId,
) -> Result<Vec<NodeId>> {
    let entries = dht.get(dht_key, Some(*local_realm_id)).await.map_err(|e| {
        NetError::Gossip(format!(
            "Failed to lookup gossip topic bootstrap nodes: {e}"
        ))
    })?;
    Ok(entries.into_iter().map(|entry| entry.node_id).collect())
}

async fn persist_subscriptions(
    storage: &StorageHandle,
    subscriptions: &Arc<RwLock<HashMap<TopicId, TopicSubscription>>>,
) {
    let persisted: Vec<TopicId> = {
        let guard = subscriptions.read();
        guard.keys().cloned().collect()
    };

    let Ok(data) = postcard::to_allocvec(&persisted) else {
        return;
    };

    let effect = Effect::Storage(StorageEffect::Write {
        key_space: GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
        key: ByteView::from(b"topics".as_slice()),
        value: ByteView::from(data),
        txn_id: None,
    });

    let _ = storage.send_effect(effect).await;
}

async fn announce_topic_subscription(
    dht: &DhtHandle,
    local_node_id: NodeId,
    local_realm_id: &RealmId,
    topic: &TopicId,
) -> Result<()> {
    let topic_key = gossip_peer_key(topic);
    dht.put(
        &topic_key,
        *local_realm_id,
        local_node_id.as_bytes().to_vec(),
        GOSSIP_TOPIC_ANNOUNCE_TTL,
    )
    .await
    .map_err(|e| NetError::Gossip(format!("Failed to announce gossip topic in DHT: {e}")))?;

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
