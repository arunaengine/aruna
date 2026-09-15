use super::{
    AuthContext, AuthFailure, AuthToken, DriverContext, GroupId, GroupPermissionRules, HashSet,
    METADATA_DISTRIBUTED_QUERY_DEADLINE, METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT,
    METADATA_DISTRIBUTED_QUERY_MAX_NODES, METADATA_REGISTRY_CANDIDATE_LIMIT, MetaResourceId,
    MetadataApiError, MetadataPathCandidate, MetadataPathResolution, MetadataPathWinner,
    MetadataReadError, MetadataRegistryRecord, MetadataTransportMessage, NodeId, PathClaimRecord,
    PlacementRef, ROLE_NODE, ReadDecision, RealmConfigDocument, RealmId, Ulid, holds_placement,
    load_realm_config, meta_bucket_subject, metadata_read_request, neg_log2_q48, peer_rank,
    reduce_holder_reads, registry_placement, registry_placement_for, registry_strategy,
    resolve_holders_limit, resolve_shard_holders, select_top_peers, selector_hash, stream,
};

use super::read::load_claim_records;
use aruna_core::StructuredId;
use futures_util::StreamExt;

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

pub(super) async fn forward_path_resolution(
    context: &DriverContext,
    realm_id: RealmId,
    config: &RealmConfigDocument,
    request: MetadataLookupRequest,
    auth_token: Option<AuthToken>,
    config_digest: [u8; 32],
    deadline: tokio::time::Instant,
) -> Result<MetadataLookupResult, MetadataApiError> {
    if request.auth.is_some() && auth_token.is_none() {
        return Err(MetadataApiError::Unauthorized);
    }
    let local_node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    let peers = select_forward_peers(config, realm_id, request.group_id, &normalized, local_node)?;
    if peers.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let requests = stream::iter(peers.into_iter().map(|peer| {
        let auth_token = auth_token.clone();
        let document_path = request.document_path.clone();
        async move {
            let response = tokio::time::timeout_at(
                deadline,
                metadata.request_forwarded_write(
                    peer,
                    MetadataTransportMessage::ForwardPathResolution {
                        auth_token,
                        group_id: request.group_id,
                        document_path,
                        config_digest,
                    },
                ),
            )
            .await;
            (response, peer)
        }
    }))
    .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT);
    futures_util::pin_mut!(requests);
    let mut auth_error = None;
    let mut divergent = false;
    let mut not_found = false;
    let mut unavailable = false;
    let mut success: Option<MetadataLookupResult> = None;
    loop {
        let response = match tokio::time::timeout_at(deadline, requests.next()).await {
            Ok(response) => response,
            Err(_) => return Err(MetadataApiError::ServiceUnavailable),
        };
        let Some((response, _peer)) = response else {
            break;
        };
        match response {
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution { result: Ok(result) })) => {
                if validate_path_resolution(
                    realm_id,
                    request.group_id,
                    &request.document_path,
                    &result,
                )
                .is_ok()
                {
                    let candidate = MetadataLookupResult {
                        winner: result.winner,
                        conflicts: result.conflicts,
                    };
                    if success.as_ref().is_some_and(|current| {
                        current.winner != candidate.winner
                            || current.conflicts != candidate.conflicts
                    }) {
                        divergent = true;
                    } else {
                        success.get_or_insert(candidate);
                    }
                } else {
                    unavailable = true;
                }
            }
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution {
                result:
                    Err(error @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden)),
            })) => {
                auth_error.get_or_insert(error);
            }
            Ok(Ok(MetadataTransportMessage::ForwardedPathResolution {
                result: Err(MetadataReadError::NotFound),
            })) => not_found = true,
            _ => unavailable = true,
        }
    }
    reduce_path_response(success, auth_error, divergent, not_found, unavailable)
}

pub(super) fn reduce_path_response(
    success: Option<MetadataLookupResult>,
    auth_error: Option<MetadataReadError>,
    divergent: bool,
    not_found: bool,
    unavailable: bool,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let conflict = divergent || (success.is_some() && (not_found || unavailable));
    match reduce_holder_reads(
        success,
        auth_error,
        not_found,
        conflict,
        unavailable,
        AuthFailure::Fatal,
    ) {
        ReadDecision::Success(result) => Ok(result),
        ReadDecision::NotFound => Err(MetadataApiError::NotFound),
        ReadDecision::Auth(MetadataReadError::Unauthorized) => Err(MetadataApiError::Unauthorized),
        ReadDecision::Auth(MetadataReadError::Forbidden) => Err(MetadataApiError::Forbidden),
        ReadDecision::Auth(_) | ReadDecision::Unavailable => {
            Err(MetadataApiError::ServiceUnavailable)
        }
    }
}

pub(super) fn validate_path_resolution(
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    resolution: &MetadataPathResolution,
) -> Result<(), MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(document_path);
    let winner_id = MetaResourceId::from_bytes(resolution.winner.document_id.to_bytes())
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    if resolution.winner.realm_id != realm_id
        || resolution.winner.group_id != group_id
        || resolution.winner.document_path != normalized
        || resolution.winner.graph_iri
            != MetadataRegistryRecord::graph_iri_for(resolution.winner.document_id)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let mut seen = HashSet::from([winner_id]);
    for conflict in &resolution.conflicts {
        let conflict = MetaResourceId::from_bytes(conflict.to_bytes())
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        if !seen.insert(conflict) {
            return Err(MetadataApiError::ServiceUnavailable);
        }
    }
    Ok(())
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

pub(super) async fn load_path_holder(
    context: &DriverContext,
    group_id: GroupId,
    document_path: &str,
    holder: NodeId,
    auth_token: Option<AuthToken>,
    config_digest: [u8; 32],
    deadline: tokio::time::Instant,
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    match tokio::time::timeout_at(
        deadline,
        metadata.request_forwarded_write(
            holder,
            MetadataTransportMessage::ForwardPathLookup {
                auth_token,
                group_id,
                document_path: document_path.to_string(),
                config_digest,
            },
        ),
    )
    .await
    {
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup { result: Ok(result) })) => Ok(result),
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup {
            result: Err(MetadataReadError::Unauthorized),
        })) => Err(MetadataApiError::Unauthorized),
        Ok(Ok(MetadataTransportMessage::ForwardedPathLookup {
            result: Err(MetadataReadError::Forbidden),
        })) => Err(MetadataApiError::Forbidden),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

pub(crate) async fn local_path_candidates(
    context: &DriverContext,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    auth: Option<&AuthContext>,
) -> Result<Vec<MetadataPathCandidate>, MetadataApiError> {
    let mut records = load_claim_records(context, realm_id, Some(group_id)).await?;
    records.retain(|record| record.document_path == document_path);
    if let Some(net) = context.net_handle.as_ref() {
        let config = load_realm_config(context, realm_id)
            .await
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
        records.retain(|record| {
            holds_placement(&config, &registry_placement(&config, record), net.node_id())
        });
    }
    let permissions = GroupPermissionRules::collect(
        context,
        auth.filter(|auth| auth.realm_id == realm_id),
        records.iter().map(|record| record.group_id),
    )
    .await;
    let evaluators = crate::auth::request_policy::PolicyEvaluator::load_bulk(
        context,
        records
            .iter()
            .map(|record| (record.realm_id, record.group_id)),
    )
    .await
    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let policy_auth = auth;
    let mut candidates = records
        .into_iter()
        .map(|record| {
            let document_id = MetaResourceId::from_bytes(record.document_id.to_bytes())
                .map_err(|error| MetadataApiError::Internal(error.to_string()))?;
            let claim = PathClaimRecord {
                document_id,
                establishing_event_id: record.establishing_event_id,
                requested_path: record.document_path.clone(),
            };
            let visible = permissions.record_visible(&record)
                && evaluators
                    .get(&(record.realm_id, record.group_id))
                    .is_some_and(|evaluator| {
                        evaluator
                            .evaluate(&metadata_read_request(&record.permission_path, policy_auth))
                            .is_ok()
                    });
            let record = visible.then_some(record);
            Ok(MetadataPathCandidate { claim, record })
        })
        .collect::<Result<Vec<_>, MetadataApiError>>()?;
    candidates.sort_by_key(|candidate| {
        (
            candidate.claim.document_id,
            candidate.claim.establishing_event_id,
        )
    });
    Ok(candidates)
}

pub(super) fn validate_path_candidate(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    document_path: &str,
    candidate: &MetadataPathCandidate,
) -> Result<PlacementRef, MetadataApiError> {
    if candidate.claim.requested_path != document_path {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let placement = registry_placement_for(config, group_id, candidate.claim.document_id.as_ulid());
    if let Some(record) = candidate.record.as_ref()
        && (record.realm_id != realm_id
            || record.group_id != group_id
            || record.document_id != candidate.claim.document_id.as_ulid()
            || record.establishing_event_id != candidate.claim.establishing_event_id
            || record.document_path != document_path
            || record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
            || record.permission_path
                != MetadataRegistryRecord::permission_path_for(
                    &realm_id,
                    group_id,
                    document_path,
                    record.document_id,
                )
            || registry_placement(config, record) != placement)
    {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(placement)
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
        METADATA_DISTRIBUTED_QUERY_MAX_NODES,
        |_| {},
    );
    if ranked.len() < METADATA_DISTRIBUTED_QUERY_MAX_NODES {
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

pub async fn lookup_metadata_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataLookupRequest,
    auth_token: Option<AuthToken>,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + METADATA_DISTRIBUTED_QUERY_DEADLINE;
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    if normalized.is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    if context.net_handle.is_none() {
        return resolve_local_path(context, realm_id, request).await;
    }
    let config = tokio::time::timeout_at(deadline, load_realm_config(context, realm_id))
        .await
        .ok()
        .flatten()
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let local_node = context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .ok_or(MetadataApiError::ServiceUnavailable)?;
    let trusted_origin = config
        .nodes
        .iter()
        .any(|node| node.node_id == local_node.to_string() && node.kind.is_sync_eligible());
    if !trusted_origin {
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        return forward_path_resolution(
            context,
            realm_id,
            &config,
            request,
            auth_token,
            config_digest,
            deadline,
        )
        .await;
    }
    let strategy = registry_strategy(&config).ok_or(MetadataApiError::ServiceUnavailable)?;
    if strategy.shard_count == 0 {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let config_digest = config
        .digest()
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let shard_count = strategy.shard_count;
    let auth_token = auth_token.or_else(|| request.auth.clone().map(AuthToken::internal));
    let group_id = request.group_id;
    let auth = request.auth.as_ref();
    let (holders, replica_counts) = select_path_holders(
        &config,
        realm_id,
        group_id,
        &normalized,
        strategy.strategy_id,
        shard_count,
        strategy.replica_count,
        local_node,
        deadline,
    )?;
    let requests = stream::iter(holders.into_iter().map(|selection| {
        let holder = selection.node_id;
        let shards = selection.shards;
        let auth_token = auth_token.clone();
        let normalized = normalized.clone();
        async move {
            let result = if holder == local_node {
                match tokio::time::timeout_at(
                    deadline,
                    local_path_candidates(context, realm_id, group_id, &normalized, auth),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(MetadataApiError::ServiceUnavailable),
                }
            } else {
                load_path_holder(
                    context,
                    group_id,
                    &normalized,
                    holder,
                    auth_token,
                    config_digest,
                    deadline,
                )
                .await
            };
            (holder, shards, result)
        }
    }))
    .buffer_unordered(METADATA_DISTRIBUTED_QUERY_FANOUT_LIMIT)
    .collect::<Vec<_>>();
    let responses = tokio::time::timeout_at(deadline, requests)
        .await
        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
    let mut views = Vec::new();
    let mut auth_error = None;
    let mut failed = false;
    let mut only_auth = true;
    let mut candidate_count = 0usize;
    for (_holder, shards, response) in responses {
        match response {
            Ok(returned) => {
                candidate_count = candidate_count.saturating_add(returned.len());
                if candidate_count > METADATA_REGISTRY_CANDIDATE_LIMIT {
                    return Err(MetadataApiError::ServiceUnavailable);
                }
                only_auth = false;
                let mut partitions = shards.iter().map(|_| Vec::new()).collect::<Vec<_>>();
                for candidate in returned {
                    let placement = validate_path_candidate(
                        &config,
                        realm_id,
                        group_id,
                        &normalized,
                        &candidate,
                    )?;
                    let index = shards
                        .binary_search(&placement.shard)
                        .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                    partitions[index].push(candidate);
                }
                views.extend(
                    shards
                        .into_iter()
                        .zip(partitions)
                        .map(|(shard, candidates)| PathShardView { shard, candidates }),
                );
            }
            Err(error @ (MetadataApiError::Unauthorized | MetadataApiError::Forbidden)) => {
                failed = true;
                auth_error.get_or_insert(error);
            }
            Err(_) => {
                failed = true;
                only_auth = false;
            }
        }
    }
    if failed {
        if only_auth && let Some(error) = auth_error {
            return Err(error);
        }
        return Err(MetadataApiError::ServiceUnavailable);
    }
    let candidates = merge_path_views(&replica_counts, views)?;
    reduce_path_candidates(candidates)
}

pub(crate) async fn resolve_local_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataLookupRequest,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let normalized = MetadataRegistryRecord::normalize_document_path(&request.document_path);
    if normalized.is_empty() {
        return Err(MetadataApiError::BadRequest);
    }
    let candidates = local_path_candidates(
        context,
        realm_id,
        request.group_id,
        &normalized,
        request.auth.as_ref(),
    )
    .await?;
    reduce_path_candidates(candidates)
}

pub(super) struct PathShardView {
    pub(super) shard: u32,
    pub(super) candidates: Vec<MetadataPathCandidate>,
}
