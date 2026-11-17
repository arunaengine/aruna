//! Runs local SPARQL queries through craqle over the graphs a caller may read, with limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::lifecycle::list_read_records;
use super::search::{LocalReadScope, resolve_visibility_scope};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use aruna_core::metadata::{MetadataError, MetadataQueryResults, QUERY_PREFIXES};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::telemetry::{record_duration_ms, record_elapsed_ms, record_error};
use aruna_graph::{GraphHandle, ReadScope};
use spargebra::{Query, SparqlParser};
use tracing::{Span, debug_span, field};

use super::effects::record_query_counts;
use super::search::select_authorized_graphs;
use super::{
    METADATA_QUERY_DEADLINE, MetadataInner, QUERY_MAX_BYTES, REGISTRY_CANDIDATE_LIMIT, handles,
};
use crate::driver::DriverContext;
use crate::metadata::query_cache::{CachedQuery, LocalScopeKind, graphs_digest, local_key};
#[tracing::instrument(
    name = "metadata.query.local",
    level = "debug",
    skip(inner, graph, auth_context, sparql),
    fields(
        query_len = sparql.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        registry_records = field::Empty,
        authorized_graphs = field::Empty,
        registry_ms = field::Empty,
        authorization_ms = field::Empty,
        craqle_query_ms = field::Empty,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
        cache = field::Empty,
    )
)]
pub(super) async fn query_local_graphs(
    inner: Arc<MetadataInner>,
    graph: &GraphHandle,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    parse_metadata_query(&sparql)?;
    if graph_iris
        .as_ref()
        .is_some_and(|graphs| graphs.len() > REGISTRY_CANDIDATE_LIMIT)
    {
        return Err(MetadataError::Backend(
            "metadata candidate limit exceeded".to_string(),
        ));
    }
    // Stamped before any read so a mutation racing this query invalidates the
    // entry it stores.
    let cache_stamp = inner
        .query_cache
        .stamp(inner.visibility_cache.current_generation());

    let records = list_read_records(inner.clone(), graph, &span).await?;

    let authorization_started = Instant::now();
    // Document-scoped queries keep the eager per-record selection; the
    // all-metadata path defers per-graph visibility to query evaluation.
    let scope = match graph_iris {
        Some(graph_iris) => {
            let requested_graphs = graph_iris.iter().collect::<HashSet<_>>().len();
            let allowed =
                select_authorized_graphs(&inner, auth_context, records, Some(graph_iris)).await?;
            if allowed.len() != requested_graphs {
                return Err(MetadataError::GraphNotFound);
            }
            LocalReadScope::Eager(allowed)
        }
        None => LocalReadScope::Lazy(Arc::new(
            resolve_visibility_scope(&inner, auth_context, records).await?,
        )),
    };
    record_elapsed_ms(&span, "authorization_ms", authorization_started);
    let lazy = match &scope {
        LocalReadScope::Eager(allowed) => {
            span.record("authorized_graphs", allowed.len() as u64);
            false
        }
        LocalReadScope::Lazy(scope) => {
            span.record("readable_groups", scope.permissions.group_count() as u64);
            true
        }
    };

    let cache_key = match &scope {
        LocalReadScope::Eager(allowed) => {
            local_key(LocalScopeKind::Eager, &graphs_digest(allowed), &sparql)
        }
        LocalReadScope::Lazy(scope) => local_key(
            LocalScopeKind::Lazy,
            &scope.visible_digest(&inner.visibility_cache),
            &sparql,
        ),
    };
    if let Some(cached) = inner
        .query_cache
        .get(&cache_key, cache_stamp, Instant::now())
    {
        span.record("cache", "hit");
        span.record("result", cached.results.kind());
        record_query_counts(&span, &cached.results);
        record_elapsed_ms(&span, "elapsed_ms", total_started);
        return Ok((*cached.results).clone());
    }
    span.record("cache", "miss");

    let query_span = debug_span!(
        "metadata.backend.craqle.query_graphs",
        lazy,
        graph_count = field::Empty,
        query_len = sparql.len() as u64,
        elapsed_ms = field::Empty,
        result = field::Empty,
        row_count = field::Empty,
        triple_count = field::Empty,
    );
    if let LocalReadScope::Eager(allowed) = &scope {
        query_span.record("graph_count", allowed.len() as u64);
    }
    let cache_inner = inner.clone();
    let query_started = Instant::now();
    let read_scope = match scope {
        LocalReadScope::Eager(allowed) => ReadScope::Graphs(allowed),
        LocalReadScope::Lazy(scope) => {
            let inner = inner.clone();
            ReadScope::Visible(Arc::new(move |graph_iri: &str| {
                scope.graph_visible(&inner.visibility_cache, graph_iri)
            }))
        }
    };
    let result = graph
        .query(read_scope, sparql, METADATA_QUERY_DEADLINE, &query_span)
        .await;
    let query_elapsed = query_started.elapsed();
    record_duration_ms(&query_span, "elapsed_ms", query_elapsed);
    record_duration_ms(&span, "craqle_query_ms", query_elapsed);
    match &result {
        Ok(results) => {
            query_span.record("result", results.kind());
            span.record("result", results.kind());
            record_query_counts(&query_span, results);
            record_query_counts(&span, results);
            let stored = cache_inner.query_cache.insert(
                cache_key,
                CachedQuery {
                    results: Arc::new(results.clone()),
                    nodes_queried: 0,
                },
                cache_stamp,
                cache_inner.visibility_cache.current_generation(),
                Instant::now(),
            );
            if stored {
                span.record("cache", "stored");
            }
        }
        Err(error) => {
            record_error(&query_span, &error.to_string());
            record_error(&span, &error.to_string());
        }
    }
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    result
}

pub(super) fn parse_metadata_query(sparql: &str) -> Result<Query, MetadataError> {
    if sparql.len() > QUERY_MAX_BYTES {
        return Err(MetadataError::InvalidInput(format!(
            "SPARQL query exceeds the {QUERY_MAX_BYTES}-byte limit"
        )));
    }
    let query = SparqlParser::new()
        .parse_query(&format!("{QUERY_PREFIXES}{sparql}"))
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let pattern = match &query {
        Query::Select { pattern, .. } | Query::Ask { pattern, .. } => pattern,
        Query::Construct { .. } | Query::Describe { .. } => {
            return Err(MetadataError::InvalidInput(
                "only SELECT and ASK metadata queries are supported".to_string(),
            ));
        }
    };
    if super::super::api::fanout_types::pattern_contains_service(pattern) {
        return Err(MetadataError::InvalidInput(
            "SERVICE is not supported in metadata queries".to_string(),
        ));
    }
    Ok(query)
}

/// Runs one SPARQL query over the local graphs `auth_context` may read.
#[tracing::instrument(
    name = "metadata.query.local_authorized",
    level = "debug",
    skip(context, auth_context, sparql),
    fields(
        query_len = sparql.len() as u64,
        graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
    )
)]
pub async fn query_authorized_local(
    context: &DriverContext,
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let (metadata, graph) = handles(context)?;
    query_local_graphs(
        metadata.inner.clone(),
        graph,
        auth_context,
        graph_iris,
        sparql,
    )
    .await
}
