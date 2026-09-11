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

pub(super) async fn forward_path_resolution(
    context: &DriverContext,
    realm_id: RealmId,
    config: &RealmConfigDocument,
    request: MetadataPathLookupRequest,
    auth_token: Option<MetadataAuthToken>,
    config_digest: [u8; 32],
    deadline: tokio::time::Instant,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
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
    let mut success: Option<MetadataPathLookupResult> = None;
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
                    let candidate = MetadataPathLookupResult {
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
    success: Option<MetadataPathLookupResult>,
    auth_error: Option<MetadataReadError>,
    divergent: bool,
    not_found: bool,
    unavailable: bool,
) -> Result<MetadataPathLookupResult, MetadataApiError> {
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

pub(super) fn normalize_path_view(candidates: &mut [MetadataPathCandidate]) -> Result<(), MetadataApiError> {
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
    auth_token: Option<MetadataAuthToken>,
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
