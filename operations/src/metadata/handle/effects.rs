//! Guards each metadata effect with the graph fence and the deleted-graph check, then runs it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::LazyLock;
use std::time::Instant;

use aruna_core::events::Event;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent, MetadataQueryResults};
use aruna_core::telemetry::{record_elapsed_ms, record_error};
use tokio::sync::Semaphore;
use tracing::{Instrument, Span, debug_span, field};

use super::MetadataHandle;
use super::lifecycle::{effect_rejects_deleted, list_local_records, metadata_graph_deleted};
use super::search::list_visible_graphs;
use crate::driver::DriverContext;
use crate::metadata::query_cache::{CacheStamp, CachedQuery, QueryCacheKey};

const GRAPH_FENCE_SHARDS: usize = 32;

// Craqle already serializes same-graph writes internally. This bounded fence
// extends that ordering over the lifecycle read performed by the Aruna handle.
static METADATA_GRAPH_FENCES: LazyLock<[Semaphore; GRAPH_FENCE_SHARDS]> =
    LazyLock::new(|| std::array::from_fn(|_| Semaphore::new(1)));

pub(crate) fn metadata_graph_fence(graph_iri: &str) -> &'static Semaphore {
    let hash = blake3::hash(graph_iri.as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash.as_bytes()[..8]);
    let shard = u64::from_be_bytes(prefix) as usize;
    &METADATA_GRAPH_FENCES[shard % GRAPH_FENCE_SHARDS]
}

/// Runs one metadata effect on this node's graph store and answers with its event.
pub async fn send_metadata_effect(context: &DriverContext, effect: MetadataEffect) -> Event {
    match &context.metadata_handle {
        Some(metadata) => metadata.run_effect(context, effect).await,
        None => Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }),
    }
}

impl MetadataHandle {
    /// Runs one metadata effect on the graph store of `context`, after the graph fence
    /// and the deleted-graph check.
    pub(crate) async fn run_effect(
        &self,
        context: &DriverContext,
        effect: MetadataEffect,
    ) -> Event {
        let started = Instant::now();
        let event = run_effect(self, context, effect).await;
        aruna_core::telemetry::record_stage("craqle", started.elapsed());
        Event::Metadata(event)
    }
}

async fn run_effect(
    metadata: &MetadataHandle,
    context: &DriverContext,
    effect: MetadataEffect,
) -> MetadataEvent {
    let cache = &metadata.inner.visibility_cache;
    let queries = metadata.query_cache();
    match effect {
        MetadataEffect::QueryCacheGet { key } => {
            let stamp = queries.stamp(cache.current_generation());
            let entry = queries.get(&QueryCacheKey::from_bytes(key), stamp, Instant::now());
            let entry = entry.map(|cached| ((*cached.results).clone(), cached.nodes_queried));
            return MetadataEvent::QueryCacheEntry {
                stamp: stamp.parts(),
                entry,
            };
        }
        MetadataEffect::QueryCachePut {
            key,
            stamp,
            results,
            nodes_queried,
        } => {
            let cached = CachedQuery {
                results: std::sync::Arc::new(results),
                nodes_queried,
            };
            let generation = cache.current_generation();
            let key = QueryCacheKey::from_bytes(key);
            let stamp = CacheStamp::from_parts(stamp);
            queries.insert(key, cached, stamp, generation, Instant::now());
            return MetadataEvent::QueryCacheEntry {
                stamp: stamp.parts(),
                entry: None,
            };
        }
        MetadataEffect::CacheGeneration => {
            return MetadataEvent::CacheGeneration {
                generation: cache.current_generation(),
            };
        }
        MetadataEffect::CacheRecords {
            records,
            generation,
        } => {
            cache.upsert_at(&records, generation);
            return MetadataEvent::CacheGeneration {
                generation: cache.current_generation(),
            };
        }
        MetadataEffect::UncacheRecord { document_id } => {
            cache.remove_registry_record(document_id);
            return MetadataEvent::CacheGeneration {
                generation: cache.current_generation(),
            };
        }
        MetadataEffect::CachedGroup { group_id } => {
            let records = match cache.records_group_any(group_id) {
                Some((records, true)) => Some(records.as_ref().clone()),
                _ => None,
            };
            return MetadataEvent::CachedGroup { records };
        }
        _ => {}
    }
    let Some(graph) = context.graph_handle.as_ref() else {
        return MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        };
    };
    if matches!(effect, MetadataEffect::FlushGraph) {
        return match graph.flush_persistence().await {
            Ok(()) => MetadataEvent::GraphFlushed,
            Err(error) => MetadataEvent::Error {
                graph_iri: None,
                error,
            },
        };
    }
    let graph_iri = effect.graph_iri();
    let _graph_fence = match graph_iri.as_deref() {
        Some(graph_iri) if effect.mutates_graph() => {
            match metadata_graph_fence(graph_iri).acquire().await {
                Ok(permit) => Some(permit),
                Err(error) => {
                    return MetadataEvent::Error {
                        graph_iri: Some(graph_iri.to_string()),
                        error: MetadataError::Backend(format!(
                            "metadata graph fence unavailable: {error}"
                        )),
                    };
                }
            }
        }
        _ => None,
    };
    if let MetadataEffect::DeleteGraph { graph_iri } = &effect {
        metadata
            .inner
            .visibility_cache
            .remove_graph_records(graph_iri);
        metadata
            .inner
            .visibility_cache
            .remove_lifecycle_entry(graph_iri);
    }
    if let Some(event) = deleted_event(context, &effect, graph_iri.as_deref()).await {
        return event;
    }
    match effect {
        MetadataEffect::SyncBestEffort { graph_iri, peers } => {
            super::lifecycle::sync_best_effort(context, graph_iri, peers)
        }
        MetadataEffect::QueryGraphs {
            auth_context,
            graph_iris,
            sparql,
        } => {
            match super::query::query_authorized_local(context, auth_context, graph_iris, sparql)
                .await
            {
                Ok(results) => MetadataEvent::QueryResult { results },
                Err(error) => MetadataEvent::Error {
                    graph_iri: None,
                    error,
                },
            }
        }
        MetadataEffect::SearchGraphs {
            auth_context,
            graph_iris,
            query,
            limit,
            group_id,
            conforms_to,
        } => {
            let searched = match conforms_to {
                Some(object_iri) => {
                    super::search::search_local_filtered(
                        context,
                        auth_context,
                        graph_iris,
                        query,
                        limit,
                        aruna_core::entity_patch::DCTERMS_CONFORMS_IRI.to_string(),
                        object_iri,
                        group_id,
                    )
                    .await
                }
                None => {
                    super::search::search_authorized_local(
                        context,
                        auth_context,
                        graph_iris,
                        query,
                        limit,
                        group_id,
                    )
                    .await
                }
            };
            match searched {
                Ok(hits) => MetadataEvent::SearchResult { hits },
                Err(error) => MetadataEvent::Error {
                    graph_iri: None,
                    error,
                },
            }
        }
        MetadataEffect::ListGraphs => match list_visible_graphs(context).await {
            Ok(graph_iris) => MetadataEvent::GraphListResult { graph_iris },
            Err(error) => MetadataEvent::Error {
                graph_iri: None,
                error,
            },
        },
        MetadataEffect::CachedRecords => {
            match list_local_records(metadata.inner.clone(), graph).await {
                Ok(records) => MetadataEvent::CachedRecords { records },
                Err(error) => MetadataEvent::Error {
                    graph_iri: None,
                    error,
                },
            }
        }
        other => {
            let mutates_graph = other.mutates_graph();
            let event = graph.run(other).await;
            // Successful mutations invalidate query results through this
            // counter; lifecycle cache changes use their own generation.
            if mutates_graph && !matches!(event, MetadataEvent::Error { .. }) {
                metadata.inner.query_cache.bump_apply();
            }
            event
        }
    }
}

async fn deleted_event(
    context: &DriverContext,
    effect: &MetadataEffect,
    graph_iri: Option<&str>,
) -> Option<MetadataEvent> {
    let graph_iri = graph_iri?;
    if matches!(
        effect,
        MetadataEffect::ValidateCreateCrate { .. } | MetadataEffect::ValidateRoCrate { .. }
    ) {
        return None;
    }
    let span = debug_span!(
        "metadata.graph_lifecycle.read_before_effect",
        effect = effect.kind(),
        graph_iri,
        deleted = field::Empty,
        elapsed_ms = field::Empty,
    );
    let started = Instant::now();
    let result = metadata_graph_deleted(context, graph_iri)
        .instrument(span.clone())
        .await;
    match result {
        Ok(true) => {
            span.record("deleted", true);
            record_elapsed_ms(&span, "elapsed_ms", started);
            match effect {
                MetadataEffect::DeleteGraph { .. } => None,
                MetadataEffect::SyncBestEffort { graph_iri, peers } => {
                    Some(MetadataEvent::GraphSyncScheduled {
                        graph_iri: graph_iri.clone(),
                        peers: peers.clone(),
                    })
                }
                MetadataEffect::ContainsGraph { graph_iri } => {
                    Some(MetadataEvent::ContainsGraphResult {
                        graph_iri: graph_iri.clone(),
                        exists: false,
                    })
                }
                // A deleted graph applies nothing more, so nothing is left to apply.
                MetadataEffect::ContainsDot { graph_iri, .. } => {
                    Some(MetadataEvent::ContainsDotResult {
                        graph_iri: graph_iri.clone(),
                        contains: true,
                    })
                }
                _ if effect_rejects_deleted(effect) => Some(MetadataEvent::Error {
                    graph_iri: Some(graph_iri.to_string()),
                    error: MetadataError::InvalidInput(format!(
                        "metadata graph `{graph_iri}` is deleted"
                    )),
                }),
                _ => None,
            }
        }
        Ok(false) => {
            span.record("deleted", false);
            record_elapsed_ms(&span, "elapsed_ms", started);
            None
        }
        Err(error) => {
            record_error(&span, &error.to_string());
            record_elapsed_ms(&span, "elapsed_ms", started);
            Some(MetadataEvent::Error {
                graph_iri: Some(graph_iri.to_string()),
                error,
            })
        }
    }
}

async fn export(context: &DriverContext, effect: MetadataEffect) -> MetadataEvent {
    match send_metadata_effect(context, effect).await {
        Event::Metadata(event) => event,
        other => MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::Backend(format!("unexpected metadata event: {}", other.label())),
        },
    }
}

/// The RO-Crate JSON-LD of one graph.
pub async fn export_rocrate_jsonld(
    context: &DriverContext,
    graph_iri: String,
) -> Result<String, MetadataError> {
    match export(context, MetadataEffect::ExportRoCrate { graph_iri }).await {
        MetadataEvent::RoCrateExportResult { jsonld, .. } => Ok(jsonld),
        MetadataEvent::Error { error, .. } => Err(error),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata export event: {}",
            other.label()
        ))),
    }
}

pub(super) fn record_query_counts(span: &Span, results: &MetadataQueryResults) {
    match results {
        MetadataQueryResults::Solutions(rows) => {
            span.record("row_count", rows.len() as u64);
        }
        MetadataQueryResults::Boolean(_) => {
            span.record("row_count", 1u64);
        }
        MetadataQueryResults::Graph(triples) => {
            span.record("triple_count", triples.len() as u64);
        }
    }
}
