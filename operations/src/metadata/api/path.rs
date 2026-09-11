use super::*;

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
    let mut ranked = Vec::with_capacity(METADATA_DISTRIBUTED_QUERY_MAX_NODES);
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
        let holders =
            resolve_holders_limit(config, &placement, METADATA_DISTRIBUTED_QUERY_MAX_NODES);
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
            if ranked.len() < METADATA_DISTRIBUTED_QUERY_MAX_NODES {
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
        if ranked.len() < METADATA_DISTRIBUTED_QUERY_MAX_NODES {
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

pub(super) fn select_forward_peers(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    normalized: &str,
    local_node: NodeId,
) -> Result<Vec<NodeId>, MetadataApiError> {
    let mut subject = meta_bucket_subject(realm_id, group_id, normalized);
    subject.extend_from_slice(local_node.as_bytes());
    let mut ranked = Vec::with_capacity(METADATA_DISTRIBUTED_QUERY_MAX_NODES);
    for node in config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
    {
        let Ok(peer) = node.node_id.parse::<NodeId>() else {
            continue;
        };
        if peer == local_node || ranked.iter().any(|(candidate, _)| *candidate == peer) {
            continue;
        }
        let score = neg_log2_q48(selector_hash(ROLE_NODE, &subject, peer.as_bytes()));
        if ranked.len() < METADATA_DISTRIBUTED_QUERY_MAX_NODES {
            ranked.push((peer, score));
            continue;
        }
        let Some((worst_index, (worst_peer, worst_score))) =
            ranked.iter().enumerate().max_by(|(_, left), (_, right)| {
                left.1
                    .cmp(&right.1)
                    .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
            })
        else {
            continue;
        };
        if score < *worst_score
            || (score == *worst_score && peer.as_bytes() < worst_peer.as_bytes())
        {
            ranked[worst_index] = (peer, score);
        }
    }
    ranked.sort_unstable_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.as_bytes().cmp(right.0.as_bytes()))
    });
    Ok(ranked.into_iter().map(|(peer, _)| peer).collect())
}
