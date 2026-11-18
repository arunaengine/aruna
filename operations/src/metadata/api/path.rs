//! Resolves metadata paths by picking holder nodes, forwarding lookups and merging replies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::forward::read::{AuthFailure, ReadDecision, reduce_holder_reads};
use super::path_types::select_path_holders;
use super::path_types::{MetadataLookupRequest, MetadataLookupResult, PathShardView};
use super::path_types::{merge_path_views, reduce_path_candidates};
use super::{
    AuthContext, AuthToken, DISTRIBUTED_QUERY_DEADLINE, GroupId, GroupPermissionRules, HashSet,
    MetaResourceId, MetadataApiError, MetadataPathCandidate, MetadataPathResolution,
    MetadataReadError, MetadataRegistryRecord, MetadataTransportMessage, NodeId, PathClaimRecord,
    PlacementRef, QUERY_FANOUT_LIMIT, QUERY_MAX_NODES, REGISTRY_CANDIDATE_LIMIT, ROLE_NODE,
    RealmConfigDocument, RealmId, holds_placement, meta_bucket_subject, metadata_read_request,
    neg_log2_q48, registry_placement, registry_placement_for, registry_strategy, selector_hash,
    stream,
};
use crate::driver::DriverContext;
use crate::realm::load_config::load_realm_config;

use super::read::load_claim_records;
use aruna_core::StructuredId;
use futures_util::StreamExt;
use crate::driver::drive;
use crate::auth::request_authorization::GroupRulesOperation;
use crate::auth::enforcement::PolicyBulkOperation;

pub(super) fn select_forward_peers(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    group_id: GroupId,
    normalized: &str,
    local_node: NodeId,
) -> Result<Vec<NodeId>, MetadataApiError> {
    let mut subject = meta_bucket_subject(realm_id, group_id, normalized);
    subject.extend_from_slice(local_node.as_bytes());
    let mut ranked = Vec::with_capacity(QUERY_MAX_NODES);
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
        if ranked.len() < QUERY_MAX_NODES {
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
    .buffer_unordered(QUERY_FANOUT_LIMIT);
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
    let permissions = drive(GroupRulesOperation::new(auth.filter(|auth| auth.realm_id == realm_id), records.iter().map(|record| record.group_id)), context)
    .await;
    let evaluators = drive(PolicyBulkOperation::new(records
            .iter()
            .map(|record| (record.realm_id, record.group_id))), context)
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

pub async fn lookup_metadata_path(
    context: &DriverContext,
    realm_id: RealmId,
    request: MetadataLookupRequest,
    auth_token: Option<AuthToken>,
) -> Result<MetadataLookupResult, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
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
    .buffer_unordered(QUERY_FANOUT_LIMIT)
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
                if candidate_count > REGISTRY_CANDIDATE_LIMIT {
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
