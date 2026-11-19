//! Answers the query, search and describe effects within a read scope an operation decided.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::metadata::{MetadataEffect, MetadataEvent, MetadataSearchHit};
use aruna_core::search_enrichment::{hit_snippet, hit_title, hit_types};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use craqle::SearchHit;
use tracing::Span;

use crate::GraphHandle;
use crate::read::{Properties, ReadScope};

/// How long one local SPARQL query may run.
const QUERY_DEADLINE: Duration = Duration::from_secs(10);

/// Exactly `graph_iris`, or every one of them the store holds.
fn scope_of(graph_iris: Vec<String>, exact: bool) -> ReadScope {
    if exact {
        return ReadScope::Graphs(graph_iris);
    }
    let visible: HashSet<String> = graph_iris.into_iter().collect();
    ReadScope::Visible(Arc::new(move |graph_iri: &str| visible.contains(graph_iri)))
}

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
                let scope = scope_of(graph_iris, exact);
                match self.query(scope, sparql, QUERY_DEADLINE, &span).await {
                    Ok(results) => MetadataEvent::QueryResult { results },
                    Err(error) => failed(error),
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
                let keep = |graph_iri: &str| by_graph.contains_key(graph_iri);
                let scope = scope_of(graphs, exact);
                match self.search(scope, query.clone(), limit, keep, &span).await {
                    Ok(hits) => MetadataEvent::SearchResult {
                        hits: (hits.into_iter())
                            .filter_map(|(hit, properties)| {
                                let record = by_graph.get(&hit.graph_id)?;
                                Some(search_hit(hit, record, &properties, &query))
                            })
                            .collect(),
                    },
                    Err(error) => failed(error),
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
