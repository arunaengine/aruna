//! Runs metadata queries and searches across realm nodes and merges the results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::handle::QUERY_MAX_BYTES;
use super::distributed_types::query_select_limit;
use super::fanout::run_metadata_fanout;
use super::{
    Arc, AuthContext, BucketPage, GroupId, HashMap, Instant, MAX_PAGINATION_DEPTH,
    MetadataApiError, MetadataFanoutOperation, MetadataFanoutScope, MetadataFanoutStats,
    MetadataNodeCall, MetadataQueryResults, MetadataReadError, MetadataSearchHit, NodeId,
    NodeSearchResult, QUERY_MAX_ROWS, RealmId, SearchPageCursor, SearchWatermark, Span,
    fanout_bearer, field, map_read_error, merge_search_hits, metadata_node_call, paginate,
    record_elapsed_ms, resume_fetch_limit,
};
use crate::driver::DriverContext;

pub(super) fn record_bucket_result(span: &Span, result: &Result<BucketPage, MetadataReadError>) {
    match result {
        Ok(page) => {
            span.record("result", "ok");
            span.record("hit_count", page.hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_query_result(
    span: &Span,
    result: &Result<MetadataQueryResults, MetadataReadError>,
) {
    match result {
        Ok(result) => {
            span.record("result", result.kind());
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

pub(super) fn record_search_node(
    span: &Span,
    result: &Result<(Vec<MetadataSearchHit>, usize), MetadataReadError>,
) {
    match result {
        Ok((hits, _)) => {
            span.record("result", "ok");
            span.record("hit_count", hits.len() as u64);
        }
        Err(_) => {
            span.record("result", "error");
        }
    }
}

#[tracing::instrument(
    name = "metadata.operation.search_distributed",
    level = "debug",
    skip(context, auth, bearer_token, query, resume, watermark, scope),
    fields(
        mode = ?scope.mode,
        query_len = query.len() as u64,
        page_size = page_size as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        node_count = field::Empty,
        discovery_ms = field::Empty,
        elapsed_ms = field::Empty,
        hit_count = field::Empty,
    )
)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_search_distributed(
    context: &DriverContext,
    realm_id: RealmId,
    local_node_id: NodeId,
    auth: Option<AuthContext>,
    bearer_token: Option<String>,
    graph_iris: Option<Vec<String>>,
    query: String,
    conforms_to: Option<String>,
    group_id: Option<GroupId>,
    resume: HashMap<NodeId, u32>,
    watermark: Option<SearchWatermark>,
    page_size: usize,
    scope: MetadataFanoutScope,
) -> Result<
    (
        Vec<MetadataSearchHit>,
        Option<SearchPageCursor>,
        bool,
        MetadataFanoutStats,
    ),
    MetadataApiError,
> {
    let span = Span::current();
    let total_started = Instant::now();
    let handle = context
        .metadata_handle
        .clone()
        .ok_or_else(|| MetadataApiError::Internal("metadata handle unavailable".to_string()))?;
    let remote_auth_token = fanout_bearer(bearer_token.as_deref());
    let resume = Arc::new(resume);

    let local_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            context.clone(),
            auth.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to.clone(),
            group_id,
            resume.clone(),
            page_size,
        ),
        |(context, auth, graph_iris, query, conforms_to, group_id, resume, page_size), node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile::validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            crate::metadata::search_local_filtered(
                                &context,
                                auth.clone(),
                                graph_iris.clone(),
                                query.clone(),
                                limit,
                                aruna_core::entity_patch::DCTERMS_CONFORMS_IRI.to_string(),
                                object_iri,
                                group_id,
                            )
                            .await
                            .map_err(crate::metadata::protocol::metadata_read_error)?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => crate::metadata::search_authorized_local(
                    &context, auth, graph_iris, query, limit, group_id,
                )
                .await
                .map_err(crate::metadata::protocol::metadata_read_error)?,
            };
            Ok((hits, limit))
        },
    );
    let remote_call: MetadataNodeCall<(Vec<MetadataSearchHit>, usize)> = metadata_node_call(
        (
            handle.clone(),
            remote_auth_token.clone(),
            graph_iris.clone(),
            query.clone(),
            conforms_to,
            group_id,
            resume.clone(),
            page_size,
        ),
        |(handle, auth_token, graph_iris, query, conforms_to, group_id, resume, page_size),
         node_id| async move {
            let limit = resume_fetch_limit(&resume, node_id, page_size, MAX_PAGINATION_DEPTH);
            let hits = match conforms_to {
                Some(object_iri) => {
                    let mut hits = Vec::new();
                    for object_iri in
                        crate::metadata::profile::validation::equivalent_profile_iris(&object_iri)
                    {
                        hits.extend(
                            handle
                                .search_remote_filtered(
                                    node_id,
                                    auth_token.clone(),
                                    graph_iris.clone(),
                                    query.clone(),
                                    limit,
                                    aruna_core::entity_patch::DCTERMS_CONFORMS_IRI.to_string(),
                                    object_iri,
                                    group_id,
                                )
                                .await?,
                        );
                    }
                    merge_search_hits(hits).into_iter().take(limit).collect()
                }
                None => {
                    handle
                        .search_remote_graphs(
                            node_id, auth_token, graph_iris, query, limit, group_id,
                        )
                        .await?
                }
            };
            Ok((hits, limit))
        },
    );
    let (node_parts, fanout_stats) = run_metadata_fanout(
        context,
        realm_id,
        local_node_id,
        scope,
        MetadataFanoutOperation::Search,
        local_call,
        remote_call,
        record_search_node,
        map_read_error,
    )
    .await?;

    let node_results = node_parts
        .into_iter()
        .map(|(node_id, (hits, requested))| NodeSearchResult {
            node_id,
            saturated: hits.len() >= requested,
            hits,
        })
        .collect();
    let page = paginate(node_results, watermark, page_size, MAX_PAGINATION_DEPTH);
    span.record("hit_count", page.hits.len() as u64);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    Ok((page.hits, page.next, page.truncated, fanout_stats))
}

/// Returns the query each partition runs, ordered by its `ORDER BY` keys, every other projected
/// variable, then each variable's term parts, so each `LIMIT` cuts a total order.
pub(super) fn ordered_query(query: &str) -> Result<Option<String>, MetadataApiError> {
    use spargebra::algebra::{Expression, Function, GraphPattern, OrderExpression};

    let Ok(mut parsed) = spargebra::SparqlParser::new().parse_query(query) else {
        return Ok(None);
    };
    let spargebra::Query::Select { pattern, .. } = &mut parsed else {
        return Ok(None);
    };
    let mut pattern = pattern;
    let mut projected = Vec::new();
    loop {
        pattern = match pattern {
            GraphPattern::Slice { inner, .. } | GraphPattern::Distinct { inner } => inner,
            GraphPattern::Project { inner, variables } => {
                projected = variables.clone();
                inner
            }
            GraphPattern::OrderBy { expression, .. } => {
                for variable in &projected {
                    let keyed = expression.iter().any(|order| {
                        matches!(order, OrderExpression::Asc(Expression::Variable(key))
                            | OrderExpression::Desc(Expression::Variable(key)) if key == variable)
                    });
                    if !keyed {
                        expression
                            .push(OrderExpression::Asc(Expression::Variable(variable.clone())));
                    }
                }
                // Value-equal terms such as 1 and 1.0 still differ in these term parts.
                for variable in &projected {
                    for function in [Function::Str, Function::Datatype, Function::LangDir] {
                        let argument = Expression::Variable(variable.clone());
                        expression.push(OrderExpression::Asc(Expression::FunctionCall(
                            function,
                            vec![argument],
                        )));
                    }
                }
                break;
            }
            _ => return Ok(None),
        };
    }
    let partition = parsed.to_string();
    // Tie fetches may raise `LIMIT` up to the row limit, which lengthens the partition text.
    let widest = query_select_limit(query).map_or(0, |limit| {
        QUERY_MAX_ROWS.max(limit).to_string().len() - limit.to_string().len()
    });
    if partition.len() + widest > QUERY_MAX_BYTES {
        return Err(MetadataApiError::InvalidQuery(
            "This ordered query is too long to run across the realm; shorten its variable names \
             or run it locally"
                .to_string(),
        ));
    }
    Ok(Some(partition))
}
