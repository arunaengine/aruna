//! Answers the query, search and describe effects within a read scope an operation decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::metadata::{MetadataEffect, MetadataEvent, MetadataSearchHit};
use aruna_core::search_enrichment::{hit_snippet, hit_title, hit_types};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::telemetry::{record_elapsed_ms, record_error};
use craqle::SearchHit;
use tracing::{Span, debug_span, field};

use crate::GraphHandle;
use crate::read::{Properties, ReadScope};

/// How long one local SPARQL query may run.
const QUERY_DEADLINE: Duration = Duration::from_secs(10);

fn search_hit(
    hit: SearchHit,
    record: &MetadataRegistryRecord,
    properties: &Properties,
    query: &str,
) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        title: hit_title(properties, &record.document_path, &hit.subject_iri),
        snippet: hit_snippet(properties, query),
        subject_types: hit_types(properties),
        graph_iri: hit.graph_id,
        subject_iri: hit.subject_iri,
        score: hit.score,
    }
}

fn failed(error: aruna_core::metadata::MetadataError) -> MetadataEvent {
    MetadataEvent::Error {
        graph_iri: None,
        error,
    }
}

impl GraphHandle {
    // Exactly `graph_iris`, or every one of them the store holds; the latter also drops a graph
    // marked deleted while the read runs.
    fn scope_of(&self, graph_iris: Vec<String>, exact: bool) -> ReadScope {
        if exact {
            return ReadScope::Graphs(graph_iris);
        }
        let visible: HashSet<String> = graph_iris.into_iter().collect();
        let inner = self.inner.clone();
        ReadScope::Visible(Arc::new(move |graph_iri: &str| {
            let deleted = inner.visibility.lifecycle_deleted_any(graph_iri);
            visible.contains(graph_iri) && !matches!(deleted, Some((true, _)))
        }))
    }

    /// Answers a scoped read, or hands any other effect back.
    pub(crate) async fn run_scoped(
        &self,
        effect: MetadataEffect,
    ) -> Result<MetadataEvent, MetadataEffect> {
        let span = Span::current();
        Ok(match effect {
            MetadataEffect::QueryScope {
                graph_iris,
                exact,
                sparql,
            } => {
                let span = debug_span!(
                    "metadata.backend.craqle.query_graphs",
                    lazy = !exact,
                    graph_count = field::Empty,
                    query_len = sparql.len() as u64,
                    elapsed_ms = field::Empty,
                    result = field::Empty,
                    row_count = field::Empty,
                    triple_count = field::Empty,
                );
                if exact {
                    span.record("graph_count", graph_iris.len() as u64);
                }
                let started = Instant::now();
                let scope = self.scope_of(graph_iris, exact);
                let result = self.query(scope, sparql, QUERY_DEADLINE, &span).await;
                record_elapsed_ms(&span, "elapsed_ms", started);
                match result {
                    Ok(results) => {
                        results.record_on(&span);
                        MetadataEvent::QueryResult { results }
                    }
                    Err(error) => {
                        record_error(&span, &error.to_string());
                        failed(error)
                    }
                }
            }
            MetadataEffect::SearchScope {
                records,
                exact,
                query,
                limit,
            } => {
                let by_graph: HashMap<String, MetadataRegistryRecord> = (records.into_iter())
                    .map(|record| (record.graph_iri.clone(), record))
                    .collect();
                let mut graphs = by_graph.keys().cloned().collect::<Vec<_>>();
                graphs.sort_unstable();
                let span = debug_span!(
                    "metadata.backend.craqle.search",
                    lazy = !exact,
                    graph_count = field::Empty,
                    query_len = query.len() as u64,
                    limit = limit as u64,
                    elapsed_ms = field::Empty,
                    result = field::Empty,
                    hit_count = field::Empty,
                );
                if exact {
                    span.record("graph_count", graphs.len() as u64);
                }
                let started = Instant::now();
                let keep = |graph_iri: &str| by_graph.contains_key(graph_iri);
                let scope = self.scope_of(graphs, exact);
                let result = self.search(scope, query.clone(), limit, keep, &span).await;
                record_elapsed_ms(&span, "elapsed_ms", started);
                match result {
                    Ok(hits) => {
                        let hits = (hits.into_iter())
                            .filter_map(|(hit, properties)| {
                                let record = by_graph.get(&hit.graph_id)?;
                                Some(search_hit(hit, record, &properties, &query))
                            })
                            .collect::<Vec<_>>();
                        span.record("result", "ok");
                        span.record("hit_count", hits.len() as u64);
                        MetadataEvent::SearchResult { hits }
                    }
                    Err(error) => {
                        record_error(&span, &error.to_string());
                        failed(error)
                    }
                }
            }
            MetadataEffect::DescribeHits { targets } => {
                let graphs = (targets.iter())
                    .map(|(record, _)| record.graph_iri.clone())
                    .collect::<HashSet<_>>();
                let pairs = (targets.iter())
                    .map(|(record, subject)| (record.graph_iri.clone(), subject.clone()))
                    .collect();
                let scope = ReadScope::Graphs(graphs.into_iter().collect());
                let properties = self.describe(scope, pairs, &span).await;
                let hits = (targets.into_iter().zip(properties))
                    .map(|((record, subject_iri), properties)| MetadataSearchHit {
                        document_id: record.document_id.to_string(),
                        group_id: record.group_id.to_string(),
                        title: hit_title(&properties, &record.document_path, &subject_iri),
                        subject_types: hit_types(&properties),
                        document_path: record.document_path,
                        graph_iri: record.graph_iri,
                        subject_iri,
                        score: 1.0,
                        snippet: None,
                    })
                    .collect();
                MetadataEvent::SearchResult { hits }
            }
            other => return Err(other),
        })
    }
}
