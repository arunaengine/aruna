use aruna_core::effects::{DhtEffect, GossipEffect, NetEffect, StreamEffect};
use aruna_core::events::{DhtEvent, GossipEvent, NetEvent, StreamEvent};

use crate::{DhtService, GossipService, StreamsService};

pub async fn handle_net_effect(
    dht: &DhtService,
    gossip: &GossipService,
    streams: &StreamsService,
    effect: NetEffect,
) -> NetEvent {
    match effect {
        NetEffect::Dht(dht_effect) => handle_dht_effect(dht, dht_effect).await,
        NetEffect::Gossip(gossip_effect) => handle_gossip_effect(gossip, gossip_effect).await,
        NetEffect::Stream(stream_effect) => {
            handle_stream_effect(streams, dht, gossip, stream_effect).await
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
        GossipEffect::Subscribe {
            topic,
            state_machine,
        } => match gossip.subscribe(topic.clone(), state_machine).await {
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
        StreamEffect::Close { stream_id } => {
            streams.close(stream_id);
            NetEvent::Stream(StreamEvent::Closed { stream_id })
        }
    }
}
