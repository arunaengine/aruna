//! Holds the path lookup types and picks and merges the holders of a path.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    AuthContext, GroupId, HashSet, MetaResourceId, MetadataApiError, MetadataPathCandidate,
    MetadataPathWinner, MetadataRegistryRecord, NodeId, PlacementRef, QUERY_MAX_NODES, ROLE_NODE,
    RealmConfigDocument, RealmId, Ulid, meta_bucket_subject, neg_log2_q48, peer_rank,
    resolve_holders_limit, resolve_shard_holders, select_top_peers, selector_hash,
};

#[derive(Debug, Clone)]
pub struct MetadataLookupRequest {
    pub group_id: GroupId,
    pub document_path: String,
    pub auth: Option<AuthContext>,
}

#[derive(Debug, Clone)]
pub struct MetadataLookupResult {
    pub winner: MetadataPathWinner,
    pub conflicts: Vec<Ulid>,
}

#[derive(Debug)]
pub(super) struct PathHolderSelection {
    pub(super) node_id: NodeId,
    pub(super) shards: Vec<u32>,
}

pub(super) struct PathShardView {
    pub(super) shard: u32,
    pub(super) candidates: Vec<MetadataPathCandidate>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn select_path_holders(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    normalized: &str,
    strategy_id: Ulid,
    shard_count: u32,
    replica_count: Option<u32>,
    local_node: NodeId,
    deadline: tokio::time::Instant,
) -> Result<(Vec<PathHolderSelection>, Vec<usize>), MetadataApiError> {
    if shard_count == 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let subject = meta_bucket_subject(realm_id, group_id, normalized);
    let mut ranked = Vec::with_capacity(QUERY_MAX_NODES);
    let mut local_score = None;
    let scan_shards = if replica_count.is_none() {
        1
    } else {
        shard_count
    };
    let mut shard_holders = Vec::with_capacity(scan_shards as usize);
    for shard in 0..scan_shards {
        if tokio::time::Instant::now() >= deadline {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let placement = PlacementRef { strategy_id, shard };
        let holders = resolve_holders_limit(config, &placement, QUERY_MAX_NODES);
        if holders.is_empty() {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        for holder in &holders {
            let score = neg_log2_q48(selector_hash(ROLE_NODE, &subject, holder.as_bytes()));
            if *holder == local_node {
                local_score = Some(score);
                continue;
            }
            if ranked.iter().any(|(candidate, _)| *candidate == *holder) {
                continue;
            }
            if ranked.len() < QUERY_MAX_NODES {
                ranked.push((*holder, score));
                continue;
            }
            let Some((worst_index, (worst_node, worst_score))) =
                ranked.iter().enumerate().max_by(|(_, left), (_, right)| {
                    left.1
                        .cmp(&right.1)
                        .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
                })
            else {
                continue;
            };
            if score < *worst_score
                || (score == *worst_score && holder.as_bytes() < worst_node.as_bytes())
            {
                ranked[worst_index] = (*holder, score);
            }
        }
        shard_holders.push(holders);
    }
    if let Some(score) = local_score {
        if ranked.len() < QUERY_MAX_NODES {
            ranked.push((local_node, score));
        } else if !ranked.iter().any(|(node, _)| *node == local_node)
            && let Some((worst_index, _)) =
                ranked.iter().enumerate().max_by(|(_, left), (_, right)| {
                    left.1
                        .cmp(&right.1)
                        .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
                })
        {
            ranked[worst_index] = (local_node, score);
        }
    }
    ranked.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
    });
    let mut selections = ranked
        .into_iter()
        .map(|(node_id, _)| PathHolderSelection {
            node_id,
            shards: Vec::new(),
        })
        .collect::<Vec<_>>();
    if replica_count.is_none() {
        if selections.is_empty() {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        for selection in &mut selections {
            selection.shards = (0..shard_count).collect();
        }
        return Ok((
            selections,
            vec![shard_holders[0].len(); shard_count as usize],
        ));
    }
    let mut replica_counts = vec![0usize; shard_count as usize];
    for (shard, holders) in shard_holders.into_iter().enumerate() {
        if tokio::time::Instant::now() >= deadline {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        let mut selected = 0usize;
        for holder in holders {
            if let Some(selection) = selections
                .iter_mut()
                .find(|selection| selection.node_id == holder)
            {
                selection.shards.push(shard as u32);
                selected += 1;
            }
        }
        if selected == 0 {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        replica_counts[shard] = selected;
    }
    Ok((selections, replica_counts))
}

/// Nodes a document query fans out to: the live holders of the bucket the
/// document was created into, not the holder set stamped at event time (which a
/// rebalance leaves stale).
pub fn replica_query_nodes(
    config: Option<&RealmConfigDocument>,
    record: &MetadataRegistryRecord,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    let holders = config
        .map(|config| resolve_shard_holders(config, &record.placement))
        .unwrap_or_default();
    let nodes = deduplicate_fanout_nodes(holders);
    if nodes.is_empty() {
        vec![local_node_id]
    } else {
        nodes
    }
}

pub fn deduplicate_fanout_nodes(nodes: Vec<NodeId>) -> Vec<NodeId> {
    let mut seen = HashSet::with_capacity(nodes.len());
    nodes
        .into_iter()
        .filter(|node_id| seen.insert(*node_id))
        .collect()
}

pub(super) fn select_fanout_nodes(
    nodes: &[NodeId],
    local_node_id: NodeId,
    subject: &[u8],
) -> Vec<NodeId> {
    let mut ranked = select_top_peers(
        nodes
            .iter()
            .copied()
            .filter(|node_id| *node_id != local_node_id),
        subject,
        QUERY_MAX_NODES,
        |_| {},
    );
    if ranked.len() < QUERY_MAX_NODES {
        ranked.push(local_node_id);
    } else if !ranked.is_empty() {
        ranked.pop();
        ranked.push(local_node_id);
    }
    ranked.sort_unstable_by(|left, right| {
        peer_rank(subject, *left)
            .cmp(&peer_rank(subject, *right))
            .then_with(|| left.as_bytes().cmp(right.as_bytes()))
    });
    ranked
}

pub(super) fn merge_path_views(
    expected: &[usize],
    views: Vec<PathShardView>,
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let mut consensus = vec![None; expected.len()];
    let mut seen = vec![0usize; expected.len()];
    for mut view in views {
        let shard = view.shard as usize;
        if shard >= expected.len() || seen[shard] >= expected[shard] {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        normalize_path_view(&mut view.candidates)?;
        seen[shard] += 1;
        match consensus[shard].as_ref() {
            Some(current) if current != &view.candidates => {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Some(_) => {}
            None => consensus[shard] = Some(view.candidates),
        }
    }

    let mut candidates = Vec::new();
    for (shard, expected) in expected.iter().copied().enumerate() {
        if expected == 0 || seen[shard] != expected {
            return Err(MetadataApiError::ServiceUnavailable);
        }
        candidates.extend(consensus[shard].take().unwrap_or_default());
    }
    Ok(candidates)
}

pub(super) fn normalize_path_view(
    candidates: &mut [MetadataPathCandidate],
) -> Result<(), MetadataApiError> {
    candidates.sort_by_key(|candidate| {
        (
            candidate.claim.document_id,
            candidate.claim.establishing_event_id,
        )
    });
    if candidates
        .windows(2)
        .any(|pair| pair[0].claim.document_id == pair[1].claim.document_id)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(())
}

pub(super) fn reduce_path_candidates(
    candidates: Vec<MetadataPathCandidate>,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let claims = candidates
        .iter()
        .map(|candidate| candidate.claim.clone())
        .collect::<Vec<_>>();
    let resolution =
        aruna_core::structs::resolve_path_claim(&claims).ok_or(MetadataApiError::NotFound)?;
    let winner = candidates
        .iter()
        .filter(|candidate| candidate.claim == resolution.winner)
        .find_map(|candidate| candidate.record.clone())
        .ok_or(MetadataApiError::NotFound)?;
    let winner = sanitize_path_winner(winner)?;
    let conflicts = resolution
        .conflicts
        .iter()
        .filter_map(|claim| {
            candidates
                .iter()
                .filter(|candidate| candidate.claim == *claim)
                .find_map(|candidate| candidate.record.as_ref())
                .map(|record| record.document_id)
        })
        .collect::<Vec<_>>();
    Ok(MetadataLookupResult { winner, conflicts })
}

pub(super) fn sanitize_path_winner(
    record: MetadataRegistryRecord,
) -> Result<MetadataPathWinner, MetadataApiError> {
    MetaResourceId::from_bytes(record.document_id.to_bytes())
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
        || record.permission_path
            != MetadataRegistryRecord::permission_path_for(
                &record.realm_id,
                record.group_id,
                &record.document_path,
                record.document_id,
            )
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(MetadataPathWinner {
        realm_id: record.realm_id,
        group_id: record.group_id,
        document_id: record.document_id,
        document_path: record.document_path,
        graph_iri: record.graph_iri,
        public: record.public,
        replicas: record.holder_node_ids.len(),
        created_at_ms: record.created_at_ms,
        updated_at_ms: record.updated_at_ms,
    })
}
