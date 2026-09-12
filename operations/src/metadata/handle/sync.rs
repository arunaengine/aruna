use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use aruna_core::NodeId;
use aruna_core::metadata::MetadataError;
use aruna_core::telemetry::record_elapsed_ms;
use aruna_net::NetHandle;
use craqle::{CraqleError, GraphId};
use tracing::{Span, debug, field};

use super::MetadataInner;
use super::entity_convert::{error_from_craqle, irokle_peer_id};
use super::lifecycle::graph_lifecycle_deleted;

#[tracing::instrument(
    name = "metadata.graph_sync.once",
    level = "debug",
    skip(inner),
    fields(
        graph_iri = %graph_iri,
        peer_count = peers.len() as u64,
        local_peer_setup_ms = field::Empty,
        network_sync_ms = field::Empty,
        elapsed_ms = field::Empty,
    )
)]
pub(super) async fn sync_graph_once(
    inner: Arc<MetadataInner>,
    graph_iri: String,
    peers: Vec<NodeId>,
) -> Result<(), MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    if peers.is_empty() {
        return Ok(());
    }
    if graph_lifecycle_deleted(inner.storage_handle.clone(), &graph_iri).await? {
        return Ok(());
    }
    let net_handle = inner
        .net_handle
        .clone()
        .ok_or(MetadataError::HandleMissing)?;

    // Deterministic topic id, bound locally only when its genesis is already
    // present; deriving never mints one, so concurrent holders cannot fork.
    let setup_started = Instant::now();
    let topic_id = resolve_topic(&inner, &graph_iri).await?;
    record_elapsed_ms(&span, "local_peer_setup_ms", setup_started);

    let sync_started = Instant::now();
    // Join-before-create: adopt an existing co-holder genesis first (raw sync
    // bootstraps an unknown topic), and only then consider minting one.
    if !sync_topic_exists(&net_handle, topic_id)? {
        if let Err(error) = net_handle.sync_topic_peers(topic_id, peers.clone()).await {
            debug!(%topic_id, error = %error, "graph topic join attempt failed");
        }
        bind_graph_topic(&inner, &graph_iri).await?;
    }
    if !sync_topic_exists(&net_handle, topic_id)? {
        ensure_topic_genesis(&inner, &net_handle, &graph_iri, topic_id, &peers).await?;
    }
    add_topic_peers(&inner, &net_handle, &graph_iri, topic_id, &peers).await?;

    net_handle
        .sync_topic_peers(topic_id, peers)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    record_elapsed_ms(&span, "network_sync_ms", sync_started);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok(())
}

async fn add_topic_peers(
    inner: &Arc<MetadataInner>,
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
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    let peers = peers
        .iter()
        .copied()
        .filter(|peer| !state.members.contains(&irokle_peer_id(*peer)))
        .collect::<Vec<_>>();
    if peers.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        let graph = GraphId::new(&graph_iri);
        for peer in peers {
            node.add_irokle_peer(&graph, irokle_peer_id(peer))?;
        }
        Ok::<_, CraqleError>(())
    })
    .await
    .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    .map_err(error_from_craqle)
}

/// Creates the graph topic genesis under the single-minter discipline: only
/// rank-0 mints, after confirming no co-holder holds a genesis, so a rank-0 move
/// cannot fork a rival. Rank-0 ties only; content materializes on every holder.
async fn ensure_topic_genesis(
    inner: &Arc<MetadataInner>,
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
    if probe.known_by_co_holder.contains(&topic_id) {
        if let Err(error) = net_handle.sync_topic_peers(topic_id, peers.to_vec()).await {
            debug!(%topic_id, error = %error, "graph topic adopt attempt failed");
        }
        bind_graph_topic(inner, graph_iri).await?;
        return Ok(());
    }
    if !probe.unreachable.is_empty() || probe.unconfirmed.contains(&topic_id) {
        return Err(MetadataError::Backend(format!(
            "withholding graph topic {topic_id} genesis: co-holder unreachable or unconfirmed"
        )));
    }
    let mut members: BTreeSet<irokle::PeerId> = peers.iter().copied().map(irokle_peer_id).collect();
    members.insert(irokle_peer_id(local));
    mint_graph_topic(inner, graph_iri, members).await?;
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

async fn resolve_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<irokle::TopicId, MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.bind_or_derive_irokle_topic(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(error_from_craqle)
}

async fn bind_graph_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
) -> Result<(), MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.bind_irokle_topic(&GraphId::new(&graph_iri)))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(error_from_craqle)?;
    Ok(())
}

async fn mint_graph_topic(
    inner: &Arc<MetadataInner>,
    graph_iri: &str,
    members: BTreeSet<irokle::PeerId>,
) -> Result<irokle::TopicId, MetadataError> {
    let node = inner.node.clone();
    let graph_iri = graph_iri.to_string();
    tokio::task::spawn_blocking(move || node.mint_irokle_topic(&GraphId::new(&graph_iri), members))
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        .map_err(error_from_craqle)
}
