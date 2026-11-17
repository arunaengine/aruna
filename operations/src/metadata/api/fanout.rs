//! Checks query form and fans metadata calls out to the selected realm nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::fanout_types::{
    MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats, MetadataNodeCall,
    RealmNodeDiscovery,
};
use super::fanout_types::{fanout_bearer, metadata_node_call, pattern_contains_service};
use super::field;

use super::super::handle::QUERY_MAX_BYTES;
use super::preflight::discover_realm_nodes;
use super::{
    ApiQueryMode, BucketPage, BucketSearchExecution, BucketSearchRequest,
    DISTRIBUTED_QUERY_DEADLINE, HashSet, Instant, MetadataApiError, MetadataReadError, NodeId,
    QUERY_FANOUT_LIMIT, QUERY_MAX_NODES, QUERY_MAX_ROWS, RealmId, SearchBucketsInput, Span,
    debug_span, deduplicate_fanout_nodes, map_read_error, query_fingerprint, record_elapsed_ms,
    select_fanout_nodes, short_display_id, stream, warn,
};
use crate::driver::DriverContext;
use crate::s3::bucket::local_search::search_local_buckets;

use super::bucket_page::{NodePosition, decode_cursor, encode_cursor, merge_pages, retired_nodes};
use super::distributed::record_bucket_result;
use futures_util::StreamExt;
use tracing::Instrument;

pub(super) fn ensure_query_mode(mode: &Option<ApiQueryMode>) {
    match mode {
        None | Some(ApiQueryMode::Local) | Some(ApiQueryMode::Distributed) => {}
    }
}

pub(super) fn ensure_query_form(query: &str) -> Result<(), MetadataApiError> {
    if query.len() > QUERY_MAX_BYTES {
        return Err(MetadataApiError::BadRequest);
    }
    let parsed = spargebra::SparqlParser::new()
        .parse_query(query)
        .map_err(|_| MetadataApiError::BadRequest)?;
    let pattern = match &parsed {
        spargebra::Query::Select { pattern, .. } | spargebra::Query::Ask { pattern, .. } => pattern,
        _ => return Err(MetadataApiError::BadRequest),
    };
    if pattern_contains_service(pattern) {
        return Err(MetadataApiError::BadRequest);
    }
    if matches!(
        pattern,
        spargebra::algebra::GraphPattern::Slice {
            length: Some(length),
            ..
        } if *length > QUERY_MAX_ROWS
    ) {
        return Err(MetadataApiError::BadRequest);
    }
    Ok(())
}

pub(super) fn fanout_node_span(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
) -> Span {
    match operation {
        MetadataFanoutOperation::Query => debug_span!(
            "metadata.operation.query_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::Search => debug_span!(
            "metadata.operation.search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::BucketSearch => debug_span!(
            "metadata.operation.bucket_search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::ObjectSearch => debug_span!(
            "metadata.operation.object_search_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::ReferencePreflight => debug_span!(
            "metadata.operation.reference_preflight_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
        MetadataFanoutOperation::IdentifierLookup => debug_span!(
            "metadata.operation.identifier_lookup_node",
            peer = ?node_id,
            local,
            elapsed_ms = field::Empty,
            hit_count = field::Empty,
            result = field::Empty,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_fanout_node<T>(
    operation: MetadataFanoutOperation,
    node_id: NodeId,
    local: bool,
    deadline: tokio::time::Instant,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataReadError>),
    record_stage_detail: bool,
) -> Result<T, MetadataReadError> {
    let node_span = fanout_node_span(operation, node_id, local);
    let node_started = Instant::now();
    let result = if local {
        match tokio::time::timeout_at(deadline, local_call(node_id).instrument(node_span.clone()))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    } else {
        match tokio::time::timeout_at(deadline, remote_call(node_id).instrument(node_span.clone()))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    };
    let elapsed = record_elapsed_ms(&node_span, "elapsed_ms", node_started);
    if record_stage_detail {
        aruna_core::telemetry::record_stage_detail(
            "fanout_node",
            || short_display_id(node_id),
            elapsed,
        );
    }
    record_result(&node_span, &result);
    result
}

pub(super) async fn metadata_fanout_nodes(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    span: &Span,
    target_nodes: Option<Vec<NodeId>>,
    deadline: tokio::time::Instant,
) -> RealmNodeDiscovery {
    match target_nodes {
        Some(nodes) => {
            span.record("discovery_ms", 0u64);
            RealmNodeDiscovery {
                nodes: deduplicate_fanout_nodes(nodes),
                failed: false,
            }
        }
        None => {
            let discovery_started = Instant::now();
            let discovery = tokio::time::timeout_at(
                deadline,
                aruna_core::telemetry::time_stage(
                    "discovery",
                    discover_realm_nodes(context, realm_id, local_node_id),
                ),
            )
            .await
            .unwrap_or(RealmNodeDiscovery {
                nodes: vec![local_node_id],
                failed: true,
            });
            record_elapsed_ms(span, "discovery_ms", discovery_started);
            discovery
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_metadata_fanout<T>(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    scope: MetadataFanoutScope,
    operation: MetadataFanoutOperation,
    local_call: MetadataNodeCall<T>,
    remote_call: MetadataNodeCall<T>,
    record_result: fn(&Span, &Result<T, MetadataReadError>),
    map_local_error: fn(MetadataReadError) -> MetadataApiError,
) -> Result<(Vec<(NodeId, T)>, MetadataFanoutStats), MetadataApiError>
where
    T: Send + 'static,
{
    let span = Span::current();
    let MetadataFanoutScope {
        mode,
        target_nodes,
        allow_partial,
        discovery_failed: scope_discovery_failed,
        subject: request_subject,
        deadline: scope_deadline,
        skip,
    } = scope;
    let deadline =
        scope_deadline.unwrap_or_else(|| tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE);
    ensure_query_mode(&mode);
    match mode.unwrap_or(ApiQueryMode::Distributed) {
        ApiQueryMode::Local => {
            let result = run_fanout_node(
                operation,
                local_node_id,
                true,
                deadline,
                local_call,
                remote_call,
                record_result,
                false,
            )
            .await;
            let fanout_stats = MetadataFanoutStats {
                nodes_queried: 1,
                nodes_failed: 0,
                failed_partitions: Vec::new(),
                discovery_failed: false,
                omitted: Vec::new(),
            };
            match result {
                Ok(result) => Ok((vec![(local_node_id, result)], fanout_stats)),
                Err(error) => Err(map_local_error(error)),
            }
        }
        ApiQueryMode::Distributed => {
            let discovery = metadata_fanout_nodes(
                context,
                realm_id,
                local_node_id,
                &span,
                target_nodes,
                deadline,
            )
            .await;
            let discovery_failed = scope_discovery_failed || discovery.failed;
            let mut nodes = discovery.nodes;
            nodes.retain(|node_id| !skip.contains(node_id));
            if discovery_failed && !allow_partial {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            let failed_partitions = Vec::new();
            let mut omitted = Vec::new();
            if nodes.len() > QUERY_MAX_NODES {
                let mut subject = Vec::with_capacity(32 + operation.label().len() + 32 + 1);
                subject.extend_from_slice(realm_id.as_bytes());
                subject.extend_from_slice(operation.label().as_bytes());
                if let Some(request_subject) = request_subject {
                    subject.extend_from_slice(&request_subject);
                }
                subject.extend_from_slice(local_node_id.as_bytes());
                let selected = select_fanout_nodes(&nodes, local_node_id, &subject);
                omitted = (nodes.iter().copied())
                    .filter(|node_id| !selected.contains(node_id))
                    .collect();
                nodes = selected;
            }
            let omitted_nodes = omitted.len();
            span.record("node_count", nodes.len() as u64);
            let mut fanout_stats = MetadataFanoutStats {
                nodes_queried: nodes.len(),
                nodes_failed: failed_partitions.len()
                    + omitted_nodes
                    + usize::from(discovery_failed),
                failed_partitions,
                discovery_failed,
                omitted,
            };
            let fanout_started = Instant::now();
            let mut node_parts = Vec::new();
            let mut auth_error = None;
            let mut not_found = false;
            let node_order = nodes.clone();
            let mut outstanding = nodes.iter().copied().collect::<HashSet<_>>();
            // Offline partitions become failures while partial-tolerant callers still use
            // reachable results within the shared deadline.
            let pending =
                stream::iter(nodes.into_iter().enumerate().map(|(node_index, node_id)| {
                    let local_call = local_call.clone();
                    let remote_call = remote_call.clone();
                    async move {
                        let result = run_fanout_node(
                            operation,
                            node_id,
                            node_id == local_node_id,
                            deadline,
                            local_call,
                            remote_call,
                            record_result,
                            true,
                        )
                        .await;
                        (node_index, node_id, result)
                    }
                }))
                .buffer_unordered(QUERY_FANOUT_LIMIT);
            futures_util::pin_mut!(pending);

            loop {
                let next = match tokio::time::timeout_at(deadline, pending.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        fanout_stats
                            .failed_partitions
                            .extend(outstanding.iter().copied());
                        fanout_stats.nodes_failed = fanout_stats.failed_partitions.len()
                            + omitted_nodes
                            + usize::from(fanout_stats.discovery_failed);
                        if !allow_partial && fanout_stats.nodes_failed > 0 {
                            return Err(MetadataApiError::ServiceUnavailable);
                        }
                        break;
                    }
                };
                let Some((node_index, node_id, result)) = next else {
                    break;
                };
                outstanding.remove(&node_id);
                match result {
                    Ok(result) => node_parts.push((node_index, node_id, result)),
                    // A rejected forwarded credential is a failed partition;
                    // local authorization already vouched for the caller.
                    Err(MetadataReadError::Unauthorized) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            "distributed metadata skipped unauthorized node result"
                        );
                    }
                    // An authenticated denial anywhere fails the whole query.
                    Err(error @ MetadataReadError::Forbidden) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        auth_error.get_or_insert(map_read_error(error));
                    }
                    Err(MetadataReadError::NotFound) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        not_found = true;
                    }
                    Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
                        fanout_stats.nodes_failed += 1;
                        fanout_stats.failed_partitions.push(node_id);
                        warn!(
                            node_id = ?node_id,
                            operation = operation.label(),
                            error = "unavailable",
                            "distributed metadata skipped failed node result"
                        );
                    }
                }
            }

            if let Some(error) = auth_error {
                return Err(error);
            }
            if not_found {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            // Nodes omitted by the fanout cap truncate the answer, so a caller
            // that asked for a complete result must not be told it is complete.
            if !allow_partial && fanout_stats.nodes_failed > 0 {
                return Err(MetadataApiError::ServiceUnavailable);
            }
            node_parts.sort_by_key(|(node_index, _, _)| *node_index);
            fanout_stats.failed_partitions.sort_by_key(|node_id| {
                node_order
                    .iter()
                    .position(|candidate| candidate == node_id)
                    .unwrap_or(usize::MAX)
            });
            aruna_core::telemetry::record_stage("fanout", fanout_started.elapsed());
            Ok((
                node_parts
                    .into_iter()
                    .map(|(_, node_id, result)| (node_id, result))
                    .collect(),
                fanout_stats,
            ))
        }
    }
}

pub async fn search_buckets_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    request: BucketSearchRequest,
) -> Result<BucketSearchExecution, MetadataApiError> {
    let deadline = tokio::time::Instant::now() + DISTRIBUTED_QUERY_DEADLINE;
    let limit = request.limit.clamp(1, 50);
    let subject = query_fingerprint(
        &request.query,
        None,
        Some(ApiQueryMode::Distributed),
        None,
        None,
    );
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(request.bearer_token.as_deref());
    let cursor = decode_cursor(request.cursor.as_deref())?;
    let positions = cursor.positions.clone();
    let group_id = request.group_id;
    let database = request.database;
    let done = (positions.iter())
        .filter(|entry| entry.done)
        .filter_map(|entry| NodeId::from_bytes(&entry.node_id).ok())
        .collect();
    let local_call: MetadataNodeCall<BucketPage> = metadata_node_call(
        (
            context.clone(),
            request.auth,
            realm_id,
            (
                request.query.clone(),
                request.key.clone(),
                request.extras.clone(),
            ),
            limit,
            positions.clone(),
        ),
        move |(context, auth, realm_id, (query, key, extras), limit, positions), node_id| async move {
            let Some(start_after) = start_after(&positions, node_id) else {
                return Ok(BucketPage::default());
            };
            search_local_buckets(
                &context,
                SearchBucketsInput {
                    auth,
                    realm_id,
                    node_id,
                    query,
                    key,
                    extras,
                    limit,
                    group_id,
                    start_after: start_after.map(Into::into),
                    database,
                },
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<BucketPage> = metadata_node_call(
        (
            handle,
            remote_auth_token,
            (request.query, request.key, request.extras),
            limit,
            positions.clone(),
        ),
        move |(handle, auth_token, (query, key, extras), limit, positions), node_id| async move {
            let Some(start_after) = start_after(&positions, node_id) else {
                return Ok(BucketPage::default());
            };
            handle
                .request_bucket_search(
                    node_id,
                    auth_token,
                    (query, key, extras, limit),
                    (group_id, database),
                    start_after,
                )
                .await
        },
    );
    let (parts, mut fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), request.target_nodes, true)
            .with_subject(subject)
            .with_deadline(deadline)
            .with_skip(done),
        MetadataFanoutOperation::BucketSearch,
        local_call,
        remote_call,
        record_bucket_result,
        map_read_error,
    )
    .await?;
    let asked = (
        &fanout_stats.failed_partitions[..],
        &fanout_stats.omitted[..],
    );
    let retired = retired_nodes(&parts);
    let (hits, cursor) = merge_pages(parts, asked, cursor, limit);
    // A retired node's part ended early, so the page is partial though its scan is done.
    fanout_stats.nodes_failed += retired.len();
    fanout_stats.failed_partitions.extend(retired);
    Ok(BucketSearchExecution {
        hits,
        next_cursor: encode_cursor(&cursor),
        fanout_stats,
    })
}

/// Where a node's next page starts; `None` for a node that already reached its end.
fn start_after(positions: &[NodePosition], node_id: NodeId) -> Option<Option<Vec<u8>>> {
    match NodePosition::of(positions, node_id) {
        Some(entry) if entry.done => None,
        entry => Some(entry.and_then(|entry| entry.after.clone())),
    }
}

type Matches = Vec<crate::metadata::secondary_ids::IdentifierMatch>;

/// Asks every realm node's reverse index, since each node indexes only the mappings it holds.
/// Returns the merged readable matches and whether any node failed to answer.
pub async fn lookup_identifier_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<super::AuthContext>,
    bearer_token: Option<String>,
    identifier: (
        aruna_core::structs::secondary_id::SecondaryIdKind,
        String,
        Option<String>,
    ),
) -> Result<(Matches, bool), MetadataApiError> {
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let local_call: MetadataNodeCall<Matches> = metadata_node_call(
        (context.clone(), auth, realm_id, identifier.clone()),
        |(context, auth, realm_id, (kind, value, endpoint)), _| async move {
            crate::metadata::identifier_lookup::lookup_local(
                &context,
                realm_id,
                auth.as_ref(),
                kind,
                &value,
                endpoint.as_deref(),
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)
        },
    );
    let remote_call: MetadataNodeCall<Matches> = metadata_node_call(
        (handle, fanout_bearer(bearer_token.as_deref()), identifier),
        |(handle, auth_token, (kind, value, endpoint)), node_id| async move {
            handle
                .request_identifier_lookup(node_id, auth_token, kind, value, endpoint)
                .await
        },
    );
    let (parts, stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        MetadataFanoutScope::new(Some(ApiQueryMode::Distributed), None, true),
        MetadataFanoutOperation::IdentifierLookup,
        local_call,
        remote_call,
        record_lookup_result,
        map_read_error,
    )
    .await?;
    let matches = crate::metadata::secondary_ids::merge_matches(
        parts.into_iter().flat_map(|(_, matches)| matches),
    );
    Ok((matches, stats.nodes_failed > 0 || stats.discovery_failed))
}

fn record_lookup_result(span: &Span, result: &Result<Matches, MetadataReadError>) {
    match result {
        Ok(matches) => {
            span.record("result", "ok");
            span.record("hit_count", matches.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}
