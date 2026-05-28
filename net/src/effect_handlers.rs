use aruna_core::effects::{DhtEffect, GossipEffect, NetEffect, StreamEffect};
use aruna_core::events::{DhtEvent, GossipEvent, NetEvent, StreamEvent};

use crate::{DhtHandle, GossipService};
use aruna_core::errors::DhtError;
use aruna_core::errors::GossipError;
use aruna_core::errors::StreamError;
use aruna_core::id::DhtKeyId;
use tracing::{trace, warn};

#[tracing::instrument(
    name = "net.effect",
    level = "debug",
    skip(dht, gossip, effect),
    fields(effect = net_effect_kind(&effect))
)]
pub async fn handle_net_effect(
    dht: &DhtHandle,
    gossip: &GossipService,
    effect: NetEffect,
) -> NetEvent {
    match effect {
        NetEffect::Dht(dht_effect) => handle_dht_effect(dht, dht_effect).await,
        NetEffect::Gossip(gossip_effect) => handle_gossip_effect(gossip, gossip_effect).await,
        NetEffect::Stream(stream_effect) => handle_stream_effect(stream_effect).await,
    }
}

#[tracing::instrument(
    name = "net.effect.dht",
    level = "debug",
    skip(dht, effect),
    fields(effect = dht_effect_kind(&effect))
)]
async fn handle_dht_effect(dht: &DhtHandle, effect: DhtEffect) -> NetEvent {
    match effect {
        DhtEffect::Put {
            key,
            realm_id,
            value,
            ttl,
        } => {
            trace!(
                event = "dht.put.started",
                key = %hex::encode(&key[..8]),
                realm_id = %realm_id,
                value_len = value.len(),
                ttl_secs = ttl.as_secs(),
                "Starting DHT put"
            );
            let key_id = DhtKeyId::from_bytes(key);
            match dht.put(&key_id, realm_id, value, ttl).await {
                Ok(()) => {
                    trace!(
                        event = "dht.put.completed",
                        key = %hex::encode(&key[..8]),
                        "Completed DHT put"
                    );
                    NetEvent::Dht(DhtEvent::PutComplete { key })
                }
                Err(e) => {
                    warn!(
                        event = "dht.put.failed",
                        key = %hex::encode(&key[..8]),
                        error = %e,
                        "DHT put failed"
                    );
                    NetEvent::Dht(DhtEvent::Error {
                        error: DhtError::StoreFailed(e.to_string()),
                    })
                }
            }
        }
        DhtEffect::Get { key, realm_filter } => {
            trace!(
                event = "dht.get.started",
                key = %hex::encode(&key[..8]),
                realm_id = ?realm_filter,
                "Starting DHT get"
            );
            let key_id = DhtKeyId::from_bytes(key);
            match dht.get(&key_id, realm_filter).await {
                Ok(values) => {
                    trace!(
                        event = "dht.get.completed",
                        key = %hex::encode(&key[..8]),
                        result_count = values.len(),
                        "Completed DHT get"
                    );
                    NetEvent::Dht(DhtEvent::GetResult { key, values })
                }
                Err(e) => {
                    warn!(
                        event = "dht.get.failed",
                        key = %hex::encode(&key[..8]),
                        error = %e,
                        "DHT get failed"
                    );
                    NetEvent::Dht(DhtEvent::Error {
                        error: DhtError::Other(e.to_string()),
                    })
                }
            }
        }
    }
}

#[tracing::instrument(
    name = "net.effect.gossip",
    level = "debug",
    skip(gossip, effect),
    fields(effect = gossip_effect_kind(&effect))
)]
async fn handle_gossip_effect(gossip: &GossipService, effect: GossipEffect) -> NetEvent {
    match effect {
        GossipEffect::Subscribe { topic } => {
            trace!(event = "gossip.subscribe", topic = %topic, "Subscribing to gossip topic");
            match gossip.subscribe(topic.clone()).await {
                Ok(()) => NetEvent::Gossip(GossipEvent::Subscribed { topic }),
                Err(e) => {
                    warn!(
                        event = "gossip.subscribe.failed",
                        topic = %topic,
                        error = %e,
                        "Failed to subscribe to gossip topic"
                    );
                    NetEvent::Gossip(GossipEvent::Error {
                        error: match e.to_string().as_str() {
                            "Already subscribed" => GossipError::AlreadySubscribed,
                            other => GossipError::Other(other.to_string()),
                        },
                    })
                }
            }
        }
        GossipEffect::Broadcast { topic, message } => {
            trace!(
                event = "gossip.broadcast.dispatch",
                topic = %topic,
                message_len = message.len(),
                "Dispatching gossip broadcast"
            );
            match gossip.broadcast(topic.clone(), message).await {
                Ok(()) => NetEvent::Gossip(GossipEvent::BroadcastComplete { topic }),
                Err(e) => {
                    warn!(
                        event = "gossip.broadcast.failed",
                        topic = %topic,
                        error = %e,
                        "Failed to broadcast gossip message"
                    );
                    NetEvent::Gossip(GossipEvent::Error {
                        error: GossipError::BroadcastFailed(e.to_string()),
                    })
                }
            }
        }
        GossipEffect::Unsubscribe { topic } => {
            trace!(event = "gossip.unsubscribe", topic = %topic, "Unsubscribing from gossip topic");
            match gossip.unsubscribe(topic.clone()).await {
                Ok(()) => NetEvent::Gossip(GossipEvent::Unsubscribed { topic }),
                Err(e) => {
                    warn!(
                        event = "gossip.unsubscribe.failed",
                        topic = %topic,
                        error = %e,
                        "Failed to unsubscribe from gossip topic"
                    );
                    NetEvent::Gossip(GossipEvent::Error {
                        error: match e.to_string().as_str() {
                            "Not subscribed" => GossipError::NotSubscribed,
                            other => GossipError::Other(other.to_string()),
                        },
                    })
                }
            }
        }
    }
}

#[tracing::instrument(
    name = "net.effect.stream",
    level = "debug",
    skip(effect),
    fields(effect = stream_effect_kind(&effect))
)]
async fn handle_stream_effect(effect: StreamEffect) -> NetEvent {
    match effect {
        StreamEffect::Open { node_id, .. } => NetEvent::Stream(StreamEvent::Error {
            stream_id: 0,
            error: StreamError::Other(format!(
                "Stream effects are unsupported; call NetHandle::open_stream for node {node_id}"
            )),
        }),
        StreamEffect::Close { stream_id } => NetEvent::Stream(StreamEvent::Error {
            stream_id,
            error: StreamError::Other(
                "Stream effects are unsupported without stream registry".to_string(),
            ),
        }),
    }
}

fn net_effect_kind(effect: &NetEffect) -> &'static str {
    match effect {
        NetEffect::Dht(_) => "dht",
        NetEffect::Gossip(_) => "gossip",
        NetEffect::Stream(_) => "stream",
    }
}

fn dht_effect_kind(effect: &DhtEffect) -> &'static str {
    match effect {
        DhtEffect::Put { .. } => "put",
        DhtEffect::Get { .. } => "get",
    }
}

fn gossip_effect_kind(effect: &GossipEffect) -> &'static str {
    match effect {
        GossipEffect::Subscribe { .. } => "subscribe",
        GossipEffect::Broadcast { .. } => "broadcast",
        GossipEffect::Unsubscribe { .. } => "unsubscribe",
    }
}

fn stream_effect_kind(effect: &StreamEffect) -> &'static str {
    match effect {
        StreamEffect::Open { .. } => "open",
        StreamEffect::Close { .. } => "close",
    }
}
