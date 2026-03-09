use aruna_core::effects::{DhtEffect, GossipEffect, NetEffect, StreamEffect};
use aruna_core::events::{DhtEvent, GossipEvent, NetEvent, StreamEvent};

use crate::{DhtHandle, GossipService};

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

async fn handle_dht_effect(dht: &DhtHandle, effect: DhtEffect) -> NetEvent {
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

async fn handle_stream_effect(effect: StreamEffect) -> NetEvent {
    match effect {
        StreamEffect::Open { node_id, .. } => NetEvent::Stream(StreamEvent::Error {
            stream_id: 0,
            error: aruna_core::errors::StreamError::Other(format!(
                "Stream effects are unsupported; call NetHandle::open_stream for node {node_id}"
            )),
        }),
        StreamEffect::Close { stream_id } => NetEvent::Stream(StreamEvent::Error {
            stream_id,
            error: aruna_core::errors::StreamError::Other(
                "Stream effects are unsupported without stream registry".to_string(),
            ),
        }),
    }
}
