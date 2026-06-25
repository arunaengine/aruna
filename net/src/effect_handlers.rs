use aruna_core::effects::{DhtEffect, NetEffect, StreamEffect};
use aruna_core::errors::{DhtError, StreamError};
use aruna_core::events::{DhtEvent, NetEvent, StreamEvent};
use aruna_core::id::{DhtKeyId, hex_prefix};
use tracing::{trace, warn};

use crate::{DhtHandle, IrokleService};

#[tracing::instrument(
    name = "net.effect",
    level = "debug",
    skip(dht, irokle, effect),
    fields(effect = net_effect_kind(&effect))
)]
pub async fn handle_net_effect(
    dht: &DhtHandle,
    irokle: &IrokleService,
    effect: NetEffect,
) -> NetEvent {
    match effect {
        NetEffect::Dht(dht_effect) => handle_dht_effect(dht, dht_effect).await,
        NetEffect::Irokle(irokle_effect) => match irokle_effect {
            aruna_core::IrokleEffect::PublishDocuments { documents, peers } => {
                NetEvent::Irokle(irokle.publish_documents(documents, peers).await)
            }
            aruna_core::IrokleEffect::SyncDocument { target, peers } => {
                NetEvent::Irokle(irokle.sync_document_event(target, peers).await)
            }
            aruna_core::IrokleEffect::SyncDocuments { targets, peers } => {
                NetEvent::Irokle(irokle.sync_documents_event(targets, peers).await)
            }
        },
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
                key = %hex_prefix(&key),
                realm_id = %realm_id,
                value_len = value.len(),
                ttl_secs = ttl.as_secs(),
                "Starting DHT put"
            );
            let key_id = DhtKeyId::from_bytes(key);
            match dht.put(&key_id, realm_id, value, ttl).await {
                Ok(()) => NetEvent::Dht(DhtEvent::PutComplete { key }),
                Err(error) => {
                    warn!(key = %hex_prefix(&key), error = %error, "DHT put failed");
                    NetEvent::Dht(DhtEvent::Error {
                        error: DhtError::StoreFailed(error.to_string()),
                    })
                }
            }
        }
        DhtEffect::Get { key, realm_filter } => {
            trace!(
                event = "dht.get.started",
                key = %hex_prefix(&key),
                realm_id = ?realm_filter,
                "Starting DHT get"
            );
            let key_id = DhtKeyId::from_bytes(key);
            match dht.get(&key_id, realm_filter).await {
                Ok(values) => NetEvent::Dht(DhtEvent::GetResult { key, values }),
                Err(error) => {
                    warn!(key = %hex_prefix(&key), error = %error, "DHT get failed");
                    NetEvent::Dht(DhtEvent::Error {
                        error: DhtError::Other(error.to_string()),
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
        NetEffect::Irokle(_) => "irokle",
        NetEffect::Stream(_) => "stream",
    }
}

fn dht_effect_kind(effect: &DhtEffect) -> &'static str {
    match effect {
        DhtEffect::Put { .. } => "put",
        DhtEffect::Get { .. } => "get",
    }
}

fn stream_effect_kind(effect: &StreamEffect) -> &'static str {
    match effect {
        StreamEffect::Open { .. } => "open",
        StreamEffect::Close { .. } => "close",
    }
}
