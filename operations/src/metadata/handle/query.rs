use super::effects::warn_slow_call;
use super::lifecycle::list_read_records;
use super::search::{LocalReadScope, resolve_visibility_scope};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use aruna_core::metadata::{MetadataError, MetadataQueryResults};
use aruna_core::structs::AuthContext;
use aruna_core::telemetry::{record_duration_ms, record_elapsed_ms};
use craqle::{CraqleNode, GraphId};
use oxrdf::{BlankNode, Dataset, GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use spareval::{CancellationToken, QueryEvaluator};
use spargebra::{Query, SparqlParser};
use tokio::time::timeout_at;
use tracing::{Span, debug_span, field};

use super::effects::{graph_ids, record_error, record_query_counts};
use super::search::select_authorized_graphs;
use super::{
    METADATA_QUERY_COMMON_PREFIXES, METADATA_QUERY_DEADLINE, METADATA_QUERY_MAX_BYTES,
    METADATA_QUERY_MAX_RESULT_BYTES, METADATA_QUERY_MAX_ROWS, MetadataHandle, MetadataInner,
};
use crate::metadata::query_cache::{CachedQuery, LocalScopeKind, graphs_digest, local_key};
#[tracing::instrument(
    name = "metadata.query.local",
    level = "debug",
    skip(inner, auth_context, sparql),
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
    auth_context: Option<AuthContext>,
    graph_iris: Option<Vec<String>>,
    sparql: String,
) -> Result<MetadataQueryResults, MetadataError> {
    let span = Span::current();
    let total_started = Instant::now();
    let query = parse_metadata_query(&sparql)?;
    if graph_iris
        .as_ref()
        .is_some_and(|graphs| graphs.len() > METADATA_REGISTRY_CANDIDATE_LIMIT)
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

    let records = list_read_records(inner.clone(), &span).await?;

    let authorization_started = Instant::now();
    // Document-scoped queries keep the eager per-record selection; the
    // all-metadata path defers per-graph visibility to query evaluation.
    let scope = match graph_iris {
        Some(graph_iris) => {
            let requested_graphs = graph_iris.iter().collect::<HashSet<_>>().len();
            let allowed =
                select_authorized_graphs(inner.clone(), auth_context, records, Some(graph_iris))
                    .await?;
            if allowed.len() != requested_graphs {
                return Err(MetadataError::GraphNotFound);
            }
            LocalReadScope::Eager(allowed)
        }
        None => {
            LocalReadScope::Lazy(resolve_visibility_scope(&inner, auth_context, records).await?)
        }
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
    let blocking_span = query_span.clone();
    let cache_inner = inner.clone();
    let query_started = Instant::now();
    // Queries are reads: take from the read pool so they never queue behind
    // long-running materializations holding the mutation permits.
    let query_deadline = tokio::time::Instant::now() + METADATA_QUERY_DEADLINE;
    let permit = tokio::time::timeout_at(
        query_deadline,
        inner.craqle_read_permits.clone().acquire_owned(),
    )
    .await
    .map_err(|_| {
        MetadataError::InvalidInput(format!(
            "metadata query timed out after {} seconds",
            METADATA_QUERY_DEADLINE.as_secs()
        ))
    })?
    .ok();
    let cancellation = CancellationToken::new();
    let _cancel_on_drop = MetadataQueryCancellationGuard(cancellation.clone());
    let blocking_cancellation = cancellation.clone();
    let mut blocking = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        blocking_span
            .in_scope(|| evaluate_query_snapshot(&inner, scope, &query, &blocking_cancellation))
    });
    let result = match tokio::time::timeout_at(query_deadline, &mut blocking).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(MetadataError::TaskJoin(error.to_string())),
        Err(_) => {
            cancellation.cancel();
            Err(MetadataError::InvalidInput(format!(
                "metadata query timed out after {} seconds",
                METADATA_QUERY_DEADLINE.as_secs()
            )))
        }
    };
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
    warn_slow_call("query_graphs", None, query_elapsed);
    record_elapsed_ms(&span, "elapsed_ms", total_started);
    result
}

struct MetadataQueryCancellationGuard(CancellationToken);

impl Drop for MetadataQueryCancellationGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn parse_metadata_query(sparql: &str) -> Result<Query, MetadataError> {
    if sparql.len() > METADATA_QUERY_MAX_BYTES {
        return Err(MetadataError::InvalidInput(format!(
            "SPARQL query exceeds the {METADATA_QUERY_MAX_BYTES}-byte limit"
        )));
    }
    let query = SparqlParser::new()
        .parse_query(&format!("{METADATA_QUERY_COMMON_PREFIXES}{sparql}"))
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let pattern = match &query {
        Query::Select { pattern, .. } | Query::Ask { pattern, .. } => pattern,
        Query::Construct { .. } | Query::Describe { .. } => {
            return Err(MetadataError::InvalidInput(
                "only SELECT and ASK metadata queries are supported".to_string(),
            ));
        }
    };
    if super::super::api::pattern_contains_service(pattern) {
        return Err(MetadataError::InvalidInput(
            "SERVICE is not supported in metadata queries".to_string(),
        ));
    }
    Ok(query)
}

fn evaluate_query_snapshot(
    inner: &MetadataInner,
    scope: LocalReadScope<Vec<String>>,
    query: &Query,
    cancellation: &CancellationToken,
) -> Result<MetadataQueryResults, MetadataError> {
    let graphs = match scope {
        LocalReadScope::Eager(allowed) => graph_ids(&allowed),
        LocalReadScope::Lazy(scope) => inner
            .node
            .graphs()
            .map_err(|error| MetadataError::Backend(error.to_string()))?
            .into_iter()
            .filter(|graph| scope.graph_visible(&inner.visibility_cache, graph.as_str()))
            .collect(),
    };
    let mut dataset = Dataset::new();
    for graph in graphs {
        ensure_not_cancelled(cancellation)?;
        if !inner
            .node
            .contains_graph(&graph)
            .map_err(|error| MetadataError::Backend(error.to_string()))?
        {
            return Err(MetadataError::GraphNotFound);
        }
        let snapshot = inner
            .node
            .graph_snapshot(&graph)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let orphaned = inner
            .node
            .graph_diagnostics(&graph)
            .map_err(|error| MetadataError::Backend(error.to_string()))?
            .orphaned_entities
            .into_iter()
            .map(|entity| craqle::EncodedTerm::from_named_node(&NamedNode::new_unchecked(entity)))
            .collect::<HashSet<_>>();
        for quad in snapshot.quads {
            ensure_not_cancelled(cancellation)?;
            if orphaned.contains(&quad.subject) || orphaned.contains(&quad.object) {
                continue;
            }
            let subject = match quad.subject.to_term() {
                Some(Term::NamedNode(subject)) => NamedOrBlankNode::NamedNode(subject),
                Some(Term::BlankNode(subject)) => NamedOrBlankNode::BlankNode(subject),
                _ => return Err(invalid_snapshot_term(&quad.subject.0)),
            };
            let predicate = quad
                .predicate
                .to_named_node()
                .ok_or_else(|| invalid_snapshot_term(&quad.predicate.0))?;
            let object = quad
                .object
                .to_term()
                .ok_or_else(|| invalid_snapshot_term(&quad.object.0))?;
            dataset.insert(&Quad::new(
                subject.clone(),
                predicate.clone(),
                object.clone(),
                snapshot.graph.0.clone(),
            ));
            dataset.insert(&Quad::new(
                subject,
                predicate,
                object,
                GraphName::DefaultGraph,
            ));
        }
    }

    ensure_not_cancelled(cancellation)?;
    let evaluator = QueryEvaluator::new().with_cancellation_token(cancellation.clone());
    let mut prepared = evaluator.prepare(query);
    prepared
        .dataset_mut()
        .set_default_graph(vec![GraphName::DefaultGraph]);
    let evaluated = prepared
        .execute(&dataset)
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let results = collect_query_results(evaluated)?;
    ensure_not_cancelled(cancellation)?;
    let serialized =
        serde_json::to_vec(&results).map_err(|error| MetadataError::Backend(error.to_string()))?;
    if serialized.len() > METADATA_QUERY_MAX_RESULT_BYTES {
        return Err(MetadataError::InvalidInput(format!(
            "metadata query result exceeds the {METADATA_QUERY_MAX_RESULT_BYTES}-byte limit"
        )));
    }
    Ok(results)
}

fn collect_query_results(
    results: spareval::QueryResults<'_>,
) -> Result<MetadataQueryResults, MetadataError> {
    match results {
        spareval::QueryResults::Solutions(solutions) => {
            let mut rows = Vec::new();
            let mut serialized_bytes = 32usize;
            for solution in solutions {
                let solution =
                    solution.map_err(|error| MetadataError::Backend(error.to_string()))?;
                if rows.len() == METADATA_QUERY_MAX_ROWS {
                    return Err(MetadataError::InvalidInput(format!(
                        "metadata query result exceeds the {METADATA_QUERY_MAX_ROWS}-row limit"
                    )));
                }
                let row = solution
                    .iter()
                    .map(|(variable, term)| {
                        let encoded = craqle::EncodedTerm::from_term(term)
                            .map_err(|error| MetadataError::Backend(error.to_string()))?;
                        Ok((variable.as_str().to_string(), encoded.0))
                    })
                    .collect::<Result<BTreeMap<_, _>, MetadataError>>()?;
                serialized_bytes = serialized_bytes.saturating_add(
                    serde_json::to_vec(&row)
                        .map_err(|error| MetadataError::Backend(error.to_string()))?
                        .len()
                        .saturating_add(1),
                );
                if serialized_bytes > METADATA_QUERY_MAX_RESULT_BYTES {
                    return Err(MetadataError::InvalidInput(format!(
                        "metadata query result exceeds the {METADATA_QUERY_MAX_RESULT_BYTES}-byte limit"
                    )));
                }
                rows.push(row);
            }
            Ok(MetadataQueryResults::Solutions(rows))
        }
        spareval::QueryResults::Boolean(value) => Ok(MetadataQueryResults::Boolean(value)),
        spareval::QueryResults::Graph(_) => Err(MetadataError::InvalidInput(
            "only SELECT and ASK metadata queries are supported".to_string(),
        )),
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), MetadataError> {
    if cancellation.is_cancelled() {
        Err(MetadataError::InvalidInput(
            "metadata query was cancelled".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn invalid_snapshot_term(term: &str) -> MetadataError {
    MetadataError::Backend(format!("invalid RDF term in metadata snapshot: {term}"))
}

pub(super) fn snapshot_iri_references(
    node: &CraqleNode,
    graph: &GraphId,
) -> Result<Vec<(String, String, String)>, MetadataError> {
    let snapshot = node
        .graph_snapshot(graph)
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let orphaned = node
        .graph_diagnostics(graph)
        .map_err(|error| MetadataError::Backend(error.to_string()))?
        .orphaned_entities
        .into_iter()
        .map(|entity| craqle::EncodedTerm::from_named_node(&NamedNode::new_unchecked(entity)))
        .collect::<HashSet<_>>();
    let mut references = Vec::new();
    for quad in snapshot.quads {
        if orphaned.contains(&quad.subject) || orphaned.contains(&quad.object) {
            continue;
        }
        let subject = match quad.subject.to_term() {
            Some(Term::NamedNode(node)) => node.as_str().to_string(),
            Some(Term::BlankNode(node)) => format!("_:{}", node.as_str()),
            _ => continue,
        };
        let Some(predicate) = quad.predicate.to_named_node() else {
            continue;
        };
        let Some(Term::NamedNode(object)) = quad.object.to_term() else {
            continue;
        };
        references.push((
            subject,
            predicate.as_str().to_string(),
            object.as_str().to_string(),
        ));
    }
    Ok(references)
}

impl MetadataHandle {
    #[tracing::instrument(
        name = "metadata.query.local_authorized",
        level = "debug",
        skip(self, auth_context, sparql),
        fields(
            query_len = sparql.len() as u64,
            graph_filter_count = graph_iris.as_ref().map_or(0, Vec::len) as u64,
        )
    )]
    pub async fn query_authorized_local(
        &self,
        auth_context: Option<AuthContext>,
        graph_iris: Option<Vec<String>>,
        sparql: String,
    ) -> Result<MetadataQueryResults, MetadataError> {
        query_local_graphs(self.inner.clone(), auth_context, graph_iris, sparql).await
    }
}
