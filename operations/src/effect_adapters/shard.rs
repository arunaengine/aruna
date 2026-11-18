//! Carries out shard topic effects with the net handle and moves shard manifests over streams.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::document::ShardManifest;
use aruna_core::effects::{ShardMembership, ShardNetEffect};
use aruna_core::events::{Event, GenesisProbe, NetEvent, ShardNetEvent};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_net::NetHandle;
use aruna_net::streams::BiStream;
use irokle::Storage as _;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, timeout};
use tracing::{debug, warn};

use crate::driver::{DriverContext, drive};
use crate::node::startup::apply_restored_reconcile;
use crate::shard::client::{SHARD_IO_TIMEOUT, validate_manifest_response};
use crate::shard::incoming::{ManifestAnswer, ServeManifestOperation};
use crate::shard::protocol::{
    MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, ManifestAssembler, ShardTransportMessage,
    ShardTransportResponse, encode_frame, page_frames, plan_manifest_pages,
};

pub(super) async fn dispatch_shard(effect: ShardNetEffect, context: &DriverContext) -> Event {
    let answer = match &context.net_handle {
        None => ShardNetEvent::NoNetwork,
        Some(net) => run_effect(effect, net, context).await,
    };
    Event::Net(NetEvent::Shard(Box::new(answer)))
}

fn outcome<E: std::fmt::Display>(result: Result<(), E>) -> ShardNetEvent {
    ShardNetEvent::Outcome(result.map_err(|error| error.to_string()))
}

async fn run_effect(
    effect: ShardNetEffect,
    net: &NetHandle,
    context: &DriverContext,
) -> ShardNetEvent {
    match effect {
        ShardNetEffect::Existing(topics) => ShardNetEvent::Existing(
            (topics.into_iter())
                .filter(|topic| net.sync_topic_exists(*topic).unwrap_or(false))
                .collect(),
        ),
        ShardNetEffect::Reopen(topics) => {
            let mut failed = 0;
            for topic in topics {
                if let Err(error) = net.reopen_sync_topic(topic) {
                    debug!(error = %error, "Failed to reopen a current shard holder topic");
                    failed += 1;
                }
            }
            ShardNetEvent::Reopened { failed }
        }
        ShardNetEffect::Close(topic) => ShardNetEvent::Closed(
            net.close_sync_topic(topic)
                .map_err(|error| error.to_string()),
        ),
        ShardNetEffect::Membership(ShardMembership {
            topics,
            members,
            publishers,
            retained,
            verified,
        }) => outcome(
            net.reconcile_shard_membership(&topics, members, publishers, &retained, &verified)
                .await,
        ),
        ShardNetEffect::Probe { topics, peers } => {
            let probe = net.probe_shard_geneses(topics, peers).await;
            ShardNetEvent::Probed(GenesisProbe {
                known_co_holder: probe.known_co_holder,
                unconfirmed: probe.unconfirmed,
                unreachable: probe.unreachable,
            })
        }
        ShardNetEffect::Pull { topics, peers } => {
            let event = net.sync_document_topics(topics, peers).await;
            let context = Arc::new(context.clone());
            let applied = apply_restored_reconcile(&context, net.node_id(), event).await;
            ShardNetEvent::Pulled { applied }
        }
        ShardNetEffect::EnsureLocal(topics) => outcome(net.ensure_local_topics(&topics).await),
        ShardNetEffect::EnsureShared { topics, peers } => {
            outcome(net.ensure_sync_topics(&topics, peers).await)
        }
        ShardNetEffect::EvictionPending(placement) => {
            ShardNetEvent::Flag(net.eviction_pending(&placement))
        }
        ShardNetEffect::TopicFacts(topic) => {
            let (digest, cursor) = topic_digest_cursor(net, topic);
            ShardNetEvent::Facts {
                holder: net.node_id(),
                digest,
                cursor,
            }
        }
        ShardNetEffect::FrontierRoot { topic, frontier } => {
            ShardNetEvent::Root(frontier_root(net, topic, &frontier))
        }
        ShardNetEffect::SignProof(claim) => {
            ShardNetEvent::Proof(claim.signed_with(|message| net.sign(message)))
        }
        ShardNetEffect::FetchManifest {
            peer,
            realm_id,
            placement,
        } => ShardNetEvent::Manifest(
            fetch_manifest(net, peer, realm_id, placement)
                .await
                .map(Box::new),
        ),
    }
}

// Irokle returns empty facts for a genesis-less topic and a zero digest on storage errors.
// Verification therefore checks topic existence separately.
fn topic_digest_cursor(net_handle: &NetHandle, topic: irokle::TopicId) -> ([u8; 32], Vec<u8>) {
    let node = net_handle.document_sync_node();
    let digest = node
        .sync_fingerprint(topic)
        .map(|fingerprint| fingerprint.fingerprint)
        .unwrap_or([0u8; 32]);
    let cursor = node
        .storage()
        .actor_clock(&topic)
        .ok()
        .and_then(|clock| postcard::to_allocvec(&clock).ok())
        .unwrap_or_default();
    (digest, cursor)
}

/// Hashes the topic lineage and the exact operation at every required frontier
/// position. Later writes can extend the topic, but cannot change this root.
pub(crate) fn frontier_root(
    net_handle: &NetHandle,
    topic: irokle::TopicId,
    frontier: &irokle::ActorClock,
) -> Result<[u8; 32], String> {
    let node = net_handle.document_sync_node();
    let storage = node.storage();
    let state = storage
        .topic_state(&topic)
        .map_err(|error| format!("failed to read shard topic state: {error}"))?
        .ok_or_else(|| "shard topic has no genesis".to_string())?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"aruna-shard-frontier-root-v1");
    hasher.update(state.genesis.as_bytes());
    hasher.update(&(frontier.iter().count() as u64).to_be_bytes());
    for (actor_id, actor_seq) in frontier.iter() {
        let op_id = storage
            .actor_index(&topic, actor_id, *actor_seq)
            .map_err(|error| format!("failed to read shard frontier operation: {error}"))?
            .ok_or_else(|| {
                format!(
                    "shard frontier operation missing for actor {actor_id} sequence {actor_seq}"
                )
            })?;
        hasher.update(actor_id.as_bytes());
        hasher.update(&actor_seq.to_be_bytes());
        hasher.update(op_id.as_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Fetches a co-holder's paged manifest for one shard over the shard ALPN and assembles it.
async fn fetch_manifest(
    net_handle: &NetHandle,
    node_id: NodeId,
    realm_id: RealmId,
    placement: PlacementRef,
) -> Result<ShardManifest, String> {
    let mut stream = net_handle
        .open_stream(node_id, Alpn::Shard)
        .await
        .map_err(|error| error.to_string())?;
    let request = ShardTransportMessage::ManifestRequest {
        realm_id,
        placement,
    };
    let frame = encode_frame(&request, MAX_REQUEST_SIZE)?;
    timeout(SHARD_IO_TIMEOUT, write_frame(&mut stream, &frame))
        .await
        .map_err(|_| "timed out writing shard message".to_string())??;
    stream.0.finish().map_err(|error| error.to_string())?;

    let deadline = Instant::now() + SHARD_IO_TIMEOUT;
    let mut assembler = ManifestAssembler::new(node_id, placement);
    loop {
        let response = match read_response_before(&mut stream, deadline).await {
            Ok(response) => response,
            Err(error) => {
                close_stream(&mut stream).await;
                return Err(error);
            }
        };
        match response {
            ShardTransportResponse::ManifestPage(page) => match assembler.push(*page) {
                Ok(Some(manifest)) => {
                    close_stream(&mut stream).await;
                    return validate_manifest_response(manifest, node_id, placement);
                }
                Ok(None) => {}
                Err(error) => {
                    close_stream(&mut stream).await;
                    return Err(error);
                }
            },
            ShardTransportResponse::Reject(reason) => {
                close_stream(&mut stream).await;
                return Err(reason);
            }
        }
    }
}

async fn read_response_before(
    stream: &mut BiStream,
    deadline: Instant,
) -> Result<ShardTransportResponse, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    timeout(remaining, read_frame(stream, MAX_RESPONSE_SIZE))
        .await
        .map_err(|_| "timed out waiting for shard response".to_string())?
}

async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

/// Answers one inbound shard manifest request on `stream`.
#[tracing::instrument(
    name = "shard.incoming.stream",
    level = "debug",
    skip(context, stream),
    fields(peer = %peer)
)]
pub(crate) async fn serve_stream(context: &DriverContext, mut stream: BiStream, peer: NodeId) {
    let Some(local) = context.node_and_realm() else {
        warn!(peer = %peer, "Dropping inbound shard stream without net handle");
        return;
    };
    let reading = "reading shard manifest request";
    let message = match with_deadline(SHARD_IO_TIMEOUT, reading, read_request(&mut stream)).await {
        Ok(message) => message,
        Err(error) => {
            warn!(peer = %peer, error = %error, "Failed to read shard manifest request");
            return;
        }
    };
    let serve = ServeManifestOperation::new(local, peer, message);
    let answer = drive(serve, context)
        .await
        .unwrap_or_else(|error| ManifestAnswer::Reject(error.to_string()));
    let writing = "writing shard manifest response";
    let written = with_deadline(SHARD_IO_TIMEOUT, writing, write_answer(&mut stream, answer));
    if let Err(error) = written.await {
        warn!(peer = %peer, error = %error, "Failed to write shard manifest response");
    }
    close_stream(&mut stream).await;
}

async fn write_answer(stream: &mut BiStream, answer: ManifestAnswer) -> Result<(), String> {
    let manifest = match answer {
        ManifestAnswer::Manifest(manifest) => manifest,
        ManifestAnswer::Reject(reason) => return write_reject(stream, reason).await,
    };
    let plan = match plan_manifest_pages(&manifest, MAX_RESPONSE_SIZE) {
        Ok(plan) => plan,
        Err(error) => return write_reject(stream, error).await,
    };
    debug!(
        strategy = %manifest.placement.strategy_id,
        shard = manifest.placement.shard,
        entries = manifest.entries.len(),
        "Served shard manifest"
    );
    for frame in page_frames(&manifest, &plan) {
        write_frame(stream, &frame?).await?;
    }
    Ok(())
}

async fn write_reject(stream: &mut BiStream, reason: String) -> Result<(), String> {
    let response = ShardTransportResponse::Reject(reason);
    let frame = encode_frame(&response, MAX_RESPONSE_SIZE)?;
    write_frame(stream, &frame).await
}

async fn with_deadline<T>(
    duration: Duration,
    operation: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    timeout(duration, future)
        .await
        .unwrap_or_else(|_| Err(format!("timed out {operation}")))
}

pub(crate) async fn read_request(stream: &mut BiStream) -> Result<ShardTransportMessage, String> {
    read_frame(stream, MAX_REQUEST_SIZE).await
}

async fn write_frame(stream: &mut BiStream, bytes: &[u8]) -> Result<(), String> {
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    stream
        .0
        .write_all(bytes)
        .await
        .map_err(|error| error.to_string())?;
    stream.0.flush().await.map_err(|error| error.to_string())
}

async fn read_frame<T: DeserializeOwned>(
    stream: &mut BiStream,
    max_size: usize,
) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|error| error.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size {
        return Err("shard frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timeout_reports_operation() {
        let error = with_deadline(
            Duration::from_millis(1),
            "reading shard manifest request",
            std::future::pending::<Result<(), String>>(),
        )
        .await
        .expect_err("pending shard IO must time out");

        assert_eq!(error, "timed out reading shard manifest request");
    }
}
