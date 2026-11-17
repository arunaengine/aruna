//! Runs SPARQL queries, searches and subject describes over the graphs one read may see.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::metadata::{
    MAX_RESULT_BYTES, MetadataError, MetadataQueryResults, QUERY_MAX_ROWS, QUERY_PREFIXES,
};
use craqle::{
    Action as CraqleAction, AuthorizationError as CraqleAuthError, Authorizer as CraqleAuthorizer,
    CraqleError, CraqleErrorKind, CraqleNode, DescribeRequest, GraphId, GraphPolicy,
    GraphSearchRequest, QueryCancellation, QueryOptions, QueryRequest, QueryResults, SearchHit,
    SearchRequest,
};
use oxrdf::{NamedNode, Term};
use tracing::{Span, warn};

use crate::GraphHandle;
use crate::run::{graph_ids, warn_slow_call};

// Craqle rebuilds a describe context per hit, so search enrichment overlaps
// instead of memoizing; the craqle read semaphore is the real concurrency cap.
const ENRICH_TASKS: usize = 8;

/// The graphs one read may see.
#[derive(Clone)]
pub enum ReadScope {
    /// Exactly these graphs; a query fails when one of them is missing.
    Graphs(Vec<String>),
    /// Every graph the check accepts, asked per graph while craqle evaluates.
    Visible(Arc<dyn Fn(&str) -> bool + Send + Sync>),
}

/// Properties of one described subject, as predicate IRI and object term.
pub type Properties = Vec<(String, Term)>;

impl ReadScope {
    fn authorizer(&self) -> ScopeAuthorizer<'_> {
        match self {
            Self::Graphs(graphs) => {
                ScopeAuthorizer::Graphs(graphs.iter().map(String::as_str).collect())
            }
            Self::Visible(visible) => ScopeAuthorizer::Visible(visible.as_ref()),
        }
    }
}

// Craqle's stored policy is ignored on purpose: the caller's scope is authoritative.
enum ScopeAuthorizer<'a> {
    Graphs(HashSet<&'a str>),
    Visible(&'a (dyn Fn(&str) -> bool + Send + Sync)),
}

impl CraqleAuthorizer for ScopeAuthorizer<'_> {
    fn authorize(
        &self,
        graph: &GraphId,
        _policy: &GraphPolicy,
        action: CraqleAction,
    ) -> Result<(), CraqleAuthError> {
        let visible = match self {
            Self::Graphs(graphs) => graphs.contains(graph.as_str()),
            Self::Visible(visible) => visible(graph.as_str()),
        };
        if matches!(action, CraqleAction::Read) && visible {
            return Ok(());
        }
        Err(CraqleAuthError::PermissionDenied {
            action,
            graph: graph.as_str().to_string(),
        })
    }
}

struct CancelOnDrop(QueryCancellation);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl GraphHandle {
    /// Evaluates one parsed SELECT or ASK query within `scope`, cancelled after `timeout`.
    pub async fn query(
        &self,
        scope: ReadScope,
        sparql: String,
        timeout: Duration,
        span: &Span,
    ) -> Result<MetadataQueryResults, MetadataError> {
        let started = std::time::Instant::now();
        let deadline = tokio::time::Instant::now() + timeout;
        let timed_out = || {
            MetadataError::InvalidInput(format!(
                "metadata query timed out after {} seconds",
                timeout.as_secs()
            ))
        };
        // Queries are reads: take from the read pool so they never queue behind
        // long-running materializations holding the mutation permits.
        let permit =
            tokio::time::timeout_at(deadline, self.inner.read_permits.clone().acquire_owned())
                .await
                .map_err(|_| timed_out())?
                .ok();
        let cancellation = QueryCancellation::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let inner = self.inner.clone();
        let blocking_span = span.clone();
        let blocking_cancellation = cancellation.clone();
        let mut blocking = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            blocking_span
                .in_scope(|| evaluate_query(&inner.node, scope, &sparql, blocking_cancellation))
        });
        let result = match tokio::time::timeout_at(deadline, &mut blocking).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(MetadataError::TaskJoin(error.to_string())),
            Err(_) => {
                cancellation.cancel();
                Err(timed_out())
            }
        };
        warn_slow_call("query_graphs", None, started.elapsed());
        result
    }

    /// Searches `scope` and describes each hit `keep` accepts, at most `limit` of them,
    /// in the order craqle returned them.
    pub async fn search(
        &self,
        scope: ReadScope,
        query: String,
        limit: usize,
        keep: impl Fn(&str) -> bool,
        span: &Span,
    ) -> Result<Vec<(SearchHit, Properties)>, MetadataError> {
        let started = std::time::Instant::now();
        let hits = {
            let inner = self.inner.clone();
            let task_scope = scope.clone();
            let blocking_span = span.clone();
            let _permit = self.inner.read_permits.clone().acquire_owned().await.ok();
            tokio::task::spawn_blocking(move || {
                blocking_span.in_scope(|| search_scope(&inner.node, &task_scope, &query, limit))
            })
            .await
            .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
        };
        let hits = match hits {
            Ok(hits) => hits,
            Err(error) => {
                warn_slow_call("search", None, started.elapsed());
                return Err(error);
            }
        };
        let hits = hits
            .into_iter()
            .filter(|hit| keep(&hit.graph_id))
            .take(limit)
            .collect::<Vec<_>>();
        let targets = hits
            .iter()
            .map(|hit| (hit.graph_id.clone(), hit.subject_iri.clone()))
            .collect::<Vec<_>>();
        let properties = self.describe(scope, targets, span).await;
        warn_slow_call("search", None, started.elapsed());
        Ok(hits.into_iter().zip(properties).collect())
    }

    /// Describes each `(graph_iri, subject_iri)` in parallel chunks. Results align with
    /// `targets` by index; a failed describe yields no properties.
    pub async fn describe(
        &self,
        scope: ReadScope,
        targets: Vec<(String, String)>,
        span: &Span,
    ) -> Vec<Properties> {
        if targets.is_empty() {
            return Vec::new();
        }
        let chunk_size = targets.len().div_ceil(ENRICH_TASKS);
        let tasks = targets
            .chunks(chunk_size)
            .map(<[(String, String)]>::to_vec)
            .map(|chunk| {
                let permits = self.inner.read_permits.clone();
                let inner = self.inner.clone();
                let scope = scope.clone();
                let span = span.clone();
                async move {
                    let chunk_len = chunk.len();
                    let _permit = permits.acquire_owned().await.ok();
                    tokio::task::spawn_blocking(move || {
                        let authorizer = scope.authorizer();
                        span.in_scope(|| {
                            chunk
                                .iter()
                                .map(|(graph_iri, subject_iri)| {
                                    describe_subject(&inner.node, &authorizer, graph_iri, subject_iri)
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .await
                    .unwrap_or_else(|error| {
                        warn!(%error, "metadata search enrichment task failed");
                        vec![Vec::new(); chunk_len]
                    })
                }
            })
            .collect::<Vec<_>>();
        futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Properties of a document's root subject (its graph IRI); empty on any backend
    /// error, so a lookup never fails on a pending or raced projection.
    pub async fn describe_root(&self, graph_iri: String) -> Properties {
        let inner = self.inner.clone();
        let _permit = self.inner.read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            let scope = ReadScope::Graphs(vec![graph_iri.clone()]);
            describe_subject(&inner.node, &scope.authorizer(), &graph_iri, &graph_iri)
        })
        .await
        .unwrap_or_default()
    }

    /// Every `(subject, predicate, object)` IRI reference of one graph, without orphans.
    pub async fn iri_references(
        &self,
        graph_iri: String,
    ) -> Result<Vec<(String, String, String)>, MetadataError> {
        let inner = self.inner.clone();
        let _permit = self.inner.read_permits.clone().acquire_owned().await.ok();
        tokio::task::spawn_blocking(move || {
            snapshot_iri_references(&inner.node, &GraphId::new(&graph_iri))
        })
        .await
        .map_err(|error| MetadataError::TaskJoin(error.to_string()))?
    }
}

fn search_scope(
    node: &CraqleNode,
    scope: &ReadScope,
    query: &str,
    limit: usize,
) -> Result<Vec<SearchHit>, MetadataError> {
    let authorizer = scope.authorizer();
    match scope {
        ReadScope::Graphs(graphs) => node.search_graphs(
            &authorizer,
            GraphSearchRequest {
                graphs: &graph_ids(graphs),
                query,
                limit,
            },
        ),
        ReadScope::Visible(_) => node.search(&authorizer, SearchRequest { query, limit }),
    }
    .map_err(|error| MetadataError::Backend(error.to_string()))
}

// Enrichment is best-effort: a pending or raced projection must never fail the
// search, so fall back to an empty property set.
fn describe_subject(
    node: &CraqleNode,
    authorizer: &dyn CraqleAuthorizer,
    graph_iri: &str,
    subject_iri: &str,
) -> Properties {
    node.describe_subject(
        authorizer,
        DescribeRequest {
            graph: &GraphId::new(graph_iri),
            subject_id: subject_iri,
        },
    )
    .map(|properties| {
        properties
            .into_iter()
            .filter_map(|(predicate, object)| {
                let Some(Term::NamedNode(predicate)) = predicate.to_term() else {
                    return None;
                };
                Some((predicate.as_str().to_string(), object.to_term()?))
            })
            .collect()
    })
    .unwrap_or_default()
}

fn evaluate_query(
    node: &CraqleNode,
    scope: ReadScope,
    sparql: &str,
    cancellation: QueryCancellation,
) -> Result<MetadataQueryResults, MetadataError> {
    let sparql = format!("{QUERY_PREFIXES}{sparql}");
    let mut options = QueryOptions::results_only();
    options.cancellation = cancellation;
    options.limits.max_query_bytes = sparql.len();
    options.limits.max_result_rows = QUERY_MAX_ROWS;
    options.limits.max_result_bytes = MAX_RESULT_BYTES;
    let authorizer = scope.authorizer();
    let execution = match &scope {
        ReadScope::Graphs(allowed) => {
            let graphs = graph_ids(allowed);
            for graph in &graphs {
                if !node.contains_graph(graph).map_err(query_error)? {
                    return Err(MetadataError::GraphNotFound);
                }
            }
            node.query_in_graphs_with_options(&authorizer, &graphs, &sparql, &options)
        }
        ReadScope::Visible(_) => node.query_with_options(
            &authorizer,
            QueryRequest {
                sparql: &sparql,
                options: &options,
            },
        ),
    }
    .map_err(query_error)?;
    let results = match execution.results {
        QueryResults::Solutions(rows) => MetadataQueryResults::Solutions(
            rows.into_iter()
                .map(|row| row.into_iter().map(|(name, term)| (name, term.0)).collect())
                .collect(),
        ),
        QueryResults::Boolean(value) => MetadataQueryResults::Boolean(value),
        QueryResults::Graph(_) => {
            return Err(MetadataError::InvalidInput(
                "only SELECT and ASK metadata queries are supported".to_string(),
            ));
        }
    };
    let serialized =
        serde_json::to_vec(&results).map_err(|error| MetadataError::Backend(error.to_string()))?;
    if serialized.len() > MAX_RESULT_BYTES {
        return Err(MetadataError::InvalidInput(format!(
            "metadata query result exceeds the {MAX_RESULT_BYTES}-byte limit"
        )));
    }
    Ok(results)
}

// Limits, cancellation and rejected queries are the caller's input, not a backend fault.
fn query_error(error: CraqleError) -> MetadataError {
    match error.kind() {
        CraqleErrorKind::InvalidInput
        | CraqleErrorKind::Unsupported
        | CraqleErrorKind::QueryLimit
        | CraqleErrorKind::Cancelled => MetadataError::InvalidInput(error.to_string()),
        _ => MetadataError::Backend(error.to_string()),
    }
}

fn snapshot_iri_references(
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
        let object = match quad.object.to_term() {
            Some(Term::NamedNode(object)) => object.as_str().to_string(),
            // A literal `contentUrl` that names Aruna data is indexed like an IRI.
            Some(Term::Literal(literal))
                if CONTENT_URL_IRIS.contains(&predicate.as_str())
                    && names_aruna_data(literal.value()) =>
            {
                literal.value().to_string()
            }
            _ => continue,
        };
        references.push((subject, predicate.as_str().to_string(), object));
    }
    Ok(references)
}

const CONTENT_URL_IRIS: [&str; 2] = [
    "http://schema.org/contentUrl",
    "https://schema.org/contentUrl",
];

fn names_aruna_data(value: &str) -> bool {
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    aruna_core::structs::storage::data::link::DataLink::parse(value).is_some()
        || VersionedObjectArn::parse(value).is_ok()
        || VersionedObjectArn::parse(
            value
                .strip_prefix(aruna_core::structs::storage::replication::ARUNA_DATA_PREFIX)
                .unwrap_or_default(),
        )
        .is_ok()
}
