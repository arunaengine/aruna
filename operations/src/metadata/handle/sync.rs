//! Syncs one graph with its peers once, binding or minting the topic that sync needs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_graph::GraphHandle;
use aruna_net::NetHandle;
use tracing::{Span, debug, field};

use super::handles;
use crate::driver::{DriverContext, drive};
use crate::metadata::graph_deleted::GraphDeletedOperation;

#[tracing::instrument(
    name = "metadata.graph_sync.once",
    level = "debug",
    skip(context),
    fields(
        graph_iri = %graph_iri,
        peer_count = peers.len() as u64,
        local_peer_setup_ms = field::Empty,
        network_sync_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn sync_graph_once(
    context: &DriverContext,
    graph_iri: String,
    peers: Vec<NodeId>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    if peers.is_empty() {
        return Ok(());
    }
    let (_, graph) = handles(context)?;
    if drive(GraphDeletedOperation::new(&graph_iri), context).await? {
        return Ok(());
    }
    let net_handle = context
        .net_handle
        .clone()
        .ok_or(MetadataError::HandleMissing)?;

    let setup_started = Instant::now();
    let topic_id = graph.graph_topic(graph_iri.clone()).await?;
    record_elapsed_ms(&span, "local_peer_setup_ms", setup_started);

    let sync_started = Instant::now();
    if !sync_topic_exists(&net_handle, topic_id)? {
        if let Err(error) = net_handle.sync_topic_peers(topic_id, peers.clone()).await {
            debug!(%topic_id, error = %error, "graph topic join attempt failed");
        }
        graph.bind_topic(graph_iri.clone()).await?;
    }
    if !sync_topic_exists(&net_handle, topic_id)? {
        ensure_topic_genesis(graph, &net_handle, &graph_iri, topic_id, &peers).await?;
    }
    add_topic_peers(graph, &net_handle, &graph_iri, topic_id, &peers).await?;

    net_handle
        .sync_topic_peers(topic_id, peers)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed_ms(&span, "network_sync_ms", sync_started);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok(())
}

async fn add_topic_peers(
    graph: &GraphHandle,
    net_handle: &NetHandle,
    graph_iri: &str,
    topic_id: irokle::TopicId,
    peers: &[NodeId],
) -> Result<(), MetadataError> {
    let sync_node = net_handle.document_sync_node();
    let Some(state) = irokle::Storage::topic_state(sync_node.storage(), &topic_id)
        .map_err(|error| MetadataError::Backend(error.to_string()))?
    else {
        return Ok(());
    };
    let peers = peers
        .iter()
        .copied()
        .filter(|peer| {
            !state
                .members
                .contains(&irokle::PeerId::from_bytes(*peer.as_bytes()))
        })
        .collect::<Vec<_>>();
    if peers.is_empty() {
        return Ok(());
    }
    graph.add_topic_peers(graph_iri.to_string(), peers).await
}

async fn ensure_topic_genesis(
    graph: &GraphHandle,
    net_handle: &NetHandle,
    graph_iri: &str,
    topic_id: irokle::TopicId,
    peers: &[NodeId],
) -> Result<(), MetadataError> {
    let local = net_handle.node_id();
    let local_is_rank0 = peers.iter().all(|peer| local.as_bytes() < peer.as_bytes());
    if !local_is_rank0 {
        return Ok(());
    }
    let probe = net_handle
        .probe_shard_geneses(vec![topic_id], peers.to_vec())
        .await;
    if probe.known_co_holder.contains(&topic_id) {
        if let Err(error) = net_handle.sync_topic_peers(topic_id, peers.to_vec()).await {
            debug!(%topic_id, error = %error, "graph topic adopt attempt failed");
        }
        graph.bind_topic(graph_iri.to_string()).await?;
        return Ok(());
    }
    if !probe.unreachable.is_empty() || probe.unconfirmed.contains(&topic_id) {
        return Err(MetadataError::Backend(format!(
            "withholding graph topic {topic_id} genesis: co-holder unreachable or unconfirmed"
        )));
    }
    let mut members = peers.iter().copied().collect::<BTreeSet<_>>();
    members.insert(local);
    graph.mint_topic(graph_iri.to_string(), members).await?;
    Ok(())
}

fn sync_topic_exists(
    net_handle: &NetHandle,
    topic_id: irokle::TopicId,
) -> Result<bool, MetadataError> {
    net_handle
        .sync_topic_exists(topic_id)
        .map_err(|error| MetadataError::Backend(error.to_string()))
}
