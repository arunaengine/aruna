use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent, MetadataQueryResults};
use aruna_core::telemetry::{duration_ms, record_duration_ms, record_elapsed_ms};
use async_trait::async_trait;
use craqle::{ActorId, AllowAllAuthorizer, CraqleError, CraqleNode, GraphId};
use tracing::{Instrument, Span, debug_span, field, warn};

use super::entity_convert::{
    batch_from_craqle, craqle_create_request, craqle_graph_policy, craqle_request_durability,
    error_from_craqle, irokle_peer_id, page_from_craqle, plan_batch, policy_from_craqle,
    to_craqle_batch, upsert_contextual_entity, upsert_data_entity,
};
use super::lifecycle::{effect_mutates_graph, effect_rejects_deleted, metadata_graph_deleted};
use super::persist::{
    effect_defers_persist, effect_persists_sync, flush_sync_journal, schedule_deferred_persist,
};
use super::search::list_visible_graphs;
use super::{
    CRAQLE_LATENCY, MetadataHandle, MetadataInner, SLOW_METADATA_BACKEND_THRESHOLD,
    metadata_graph_fence,
};
use crate::metadata::protocol::MetadataReadError;

impl MetadataHandle {
    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let started = Instant::now();
        let event = self.run_effect(effect).await;
        aruna_core::telemetry::record_stage("craqle", started.elapsed());
        event
    }

    async fn run_effect(&self, effect: MetadataEffect) -> Event {
        let effect_name = metadata_effect_kind(&effect);
        let graph_iri = effect_graph_iri(&effect);
        let _graph_fence = match graph_iri.as_deref() {
            Some(graph_iri) if effect_mutates_graph(&effect) => {
                match metadata_graph_fence(graph_iri).acquire().await {
                    Ok(permit) => Some(permit),
                    Err(error) => {
                        return Event::Metadata(MetadataEvent::Error {
                            graph_iri: Some(graph_iri.to_string()),
                            error: MetadataError::Backend(format!(
                                "metadata graph fence unavailable: {error}"
                            )),
                        });
                    }
                }
            }
            _ => None,
        };
        if let MetadataEffect::DeleteGraph { graph_iri } = &effect {
            self.inner.visibility_cache.remove_graph_records(graph_iri);
            self.inner
                .visibility_cache
                .remove_lifecycle_entry(graph_iri);
        }
        if let Some(event) = self
            .deleted_event(&effect, graph_iri.as_deref(), effect_name)
            .await
        {
            return event;
        }
        match effect {
            MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                Event::Metadata(self.sync_best_effort(graph_iri, peers).await)
            }
            MetadataEffect::QueryGraphs {
                auth_context,
                graph_iris,
                sparql,
            } => Event::Metadata(
                match self
                    .query_authorized_local(auth_context, graph_iris, sparql)
                    .await
                {
                    Ok(results) => MetadataEvent::QueryResult { results },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::SearchGraphs {
                auth_context,
                graph_iris,
                query,
                limit,
            } => Event::Metadata(
                match self
                    .search_authorized_local(auth_context, graph_iris, query, limit, None)
                    .await
                {
                    Ok(hits) => MetadataEvent::SearchResult { hits },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                },
            ),
            MetadataEffect::ListGraphs => {
                Event::Metadata(match list_visible_graphs(self.inner.clone()).await {
                    Ok(graph_iris) => MetadataEvent::GraphListResult { graph_iris },
                    Err(error) => MetadataEvent::Error {
                        graph_iri: None,
                        error,
                    },
                })
            }
            other => self.run_blocking(other, graph_iri).await,
        }
    }

    async fn deleted_event(
        &self,
        effect: &MetadataEffect,
        graph_iri: Option<&str>,
        effect_name: &'static str,
    ) -> Option<Event> {
        let graph_iri = graph_iri?;
        if super::persist::effect_skips_lifecycle(effect) {
            return None;
        }
        let span = debug_span!(
            "metadata.graph_lifecycle.read_before_effect",
            effect = effect_name,
            graph_iri,
            deleted = field::Empty,
            elapsed_ms = field::Empty,
        );
        let started = Instant::now();
        let result =
            metadata_graph_deleted(self.inner.clone(), self.lifecycle_storage(), graph_iri)
                .instrument(span.clone())
                .await;
        match result {
            Ok(true) => {
                span.record("deleted", true);
                record_elapsed_ms(&span, "elapsed_ms", started);
                match effect {
                    MetadataEffect::DeleteGraph { .. } => {}
                    MetadataEffect::SyncGraphBestEffort { graph_iri, peers } => {
                        return Some(Event::Metadata(MetadataEvent::GraphSyncScheduled {
                            graph_iri: graph_iri.clone(),
                            peers: peers.clone(),
                        }));
                    }
                    MetadataEffect::ContainsGraph { graph_iri } => {
                        return Some(Event::Metadata(MetadataEvent::ContainsGraphResult {
                            graph_iri: graph_iri.clone(),
                            exists: false,
                        }));
                    }
                    _ if effect_rejects_deleted(&effect) => {
                        return Some(Event::Metadata(MetadataEvent::Error {
                            graph_iri: Some(graph_iri.to_string()),
                            error: MetadataError::InvalidInput(format!(
                                "metadata graph `{graph_iri}` is deleted"
                            )),
                        }));
                    }
                    _ => {}
                }
            }
            Ok(false) => {
                span.record("deleted", false);
                record_elapsed_ms(&span, "elapsed_ms", started);
            }
            Err(error) => {
                record_error(&span, &error.to_string());
                record_elapsed_ms(&span, "elapsed_ms", started);
                return Some(Event::Metadata(MetadataEvent::Error {
                    graph_iri: Some(graph_iri.to_string()),
                    error,
                }));
            }
        }
        None
    }

    async fn run_blocking(&self, effect: MetadataEffect, graph_iri: Option<String>) -> Event {
        let inner = self.inner.clone();
        let span = debug_span!(
            "metadata.backend.blocking_task",
            effect = metadata_effect_kind(&effect),
            graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
            elapsed_ms = field::Empty,
            result = field::Empty,
        );
        let blocking_span = span.clone();
        let started = Instant::now();
        // Heavy mutations and cheap reads queue on separate pools so
        // trivial reads never wait behind long materializations.
        let mutates_graph = effect_mutates_graph(&effect);
        let permits = if mutates_graph {
            self.inner.craqle_permits.clone()
        } else {
            self.inner.craqle_read_permits.clone()
        };
        let _permit = permits.acquire_owned().await.ok();
        let metadata_event = match tokio::task::spawn_blocking(move || {
            blocking_span.in_scope(|| handle_effect(inner, effect))
        })
        .await
        {
            Ok(event) => event,
            Err(error) => {
                record_error(&span, &error.to_string());
                MetadataEvent::Error {
                    graph_iri,
                    error: MetadataError::TaskJoin(error.to_string()),
                }
            }
        };
        record_elapsed_ms(&span, "elapsed_ms", started);
        span.record("result", metadata_event_kind(&metadata_event));
        // Successful mutations invalidate query results through this
        // counter; lifecycle cache changes use their own generation.
        if mutates_graph && !matches!(metadata_event, MetadataEvent::Error { .. }) {
            self.inner.query_cache.bump_apply();
        }
        Event::Metadata(metadata_event)
    }
}

pub(super) fn record_error(span: &Span, error: &str) {
    span.record("result", "error");
    span.record("error", field::display(error));
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", field::display(error));
}

pub(super) fn warn_slow_call(operation: &'static str, graph_iri: Option<&str>, duration: Duration) {
    CRAQLE_LATENCY.record(operation, duration);
    if duration >= SLOW_METADATA_BACKEND_THRESHOLD {
        warn!(
            event = "metadata.backend.slow_call",
            operation,
            graph_iri = graph_iri.unwrap_or("<none>"),
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_METADATA_BACKEND_THRESHOLD),
            "Slow metadata backend call"
        );
    }
}

pub(super) fn record_craqle_result<T>(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<T, CraqleError>,
) {
    let duration = started.elapsed();
    record_duration_ms(span, "elapsed_ms", duration);
    match result {
        Ok(_) => {
            span.record("result", "ok");
            span.record("otel.status_code", "OK");
        }
        Err(error) => record_error(span, &error.to_string()),
    }
    warn_slow_call(operation, graph_iri, duration);
}

pub(super) fn record_metadata_result(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<MetadataEvent, CraqleError>,
) {
    record_craqle_result(span, operation, graph_iri, started, result);
}

pub(super) fn metadata_effect_kind(effect: &MetadataEffect) -> &'static str {
    match effect {
        MetadataEffect::ValidateCreateCrate { .. } => "validate_create_crate",
        MetadataEffect::ValidateRoCrate { .. } => "validate_rocrate",
        MetadataEffect::CreateCrate { .. } => "create_crate",
        MetadataEffect::ApplyRoCrate { .. } => "apply_rocrate",
        MetadataEffect::UpsertDataEntity { .. } => "upsert_data_entity",
        MetadataEffect::UpsertContextualEntity { .. } => "upsert_contextual_entity",
        MetadataEffect::SetGraphPolicy { .. } => "set_graph_policy",
        MetadataEffect::AddGraphPeer { .. } => "add_graph_peer",
        MetadataEffect::SyncGraphBestEffort { .. } => "sync_best_effort",
        MetadataEffect::GetGraphPolicy { .. } => "get_graph_policy",
        MetadataEffect::ExportRoCrate { .. } => "export_rocrate",
        MetadataEffect::ExportRoCrateSummary { .. } => "export_rocrate_summary",
        MetadataEffect::ExportRoCratePage { .. } => "export_rocrate_page",
        MetadataEffect::SearchGraphs { .. } => "search_graphs",
        MetadataEffect::QueryGraphs { .. } => "query_graphs",
        MetadataEffect::DeleteGraph { .. } => "delete_graph",
        MetadataEffect::ListGraphs => "list_graphs",
        MetadataEffect::ContainsGraph { .. } => "contains_graph",
        MetadataEffect::PlanBatch { .. } => "plan_batch",
        MetadataEffect::MergeBatch { .. } => "merge_batch",
        MetadataEffect::GraphSnapshot { .. } => "graph_snapshot",
        MetadataEffect::InstallSnapshot { .. } => "install_snapshot",
    }
}

pub(super) fn metadata_event_kind(event: &MetadataEvent) -> &'static str {
    match event {
        MetadataEvent::ValidationResult { .. } => "validation_result",
        MetadataEvent::CreateCrateResult { .. } => "create_crate_result",
        MetadataEvent::ApplyRoCrateResult { .. } => "apply_rocrate_result",
        MetadataEvent::EntityUpsertResult { .. } => "entity_upsert_result",
        MetadataEvent::GraphPolicySet { .. } => "graph_policy_set",
        MetadataEvent::GraphPeerAdded { .. } => "graph_peer_added",
        MetadataEvent::GraphSyncScheduled { .. } => "graph_sync_scheduled",
        MetadataEvent::GraphPolicyResult { .. } => "graph_policy_result",
        MetadataEvent::RoCrateExportResult { .. } => "rocrate_export_result",
        MetadataEvent::RoCrateSummaryResult { .. } => "rocrate_summary_result",
        MetadataEvent::RoCratePageResult { .. } => "rocrate_page_result",
        MetadataEvent::SearchResult { .. } => "search_result",
        MetadataEvent::QueryResult { .. } => "query_result",
        MetadataEvent::GraphDeleted { .. } => "graph_deleted",
        MetadataEvent::GraphListResult { .. } => "graph_list_result",
        MetadataEvent::ContainsGraphResult { .. } => "contains_graph_result",
        MetadataEvent::BatchPlanned { .. } => "batch_planned",
        MetadataEvent::BatchMerged { .. } => "batch_merged",
        MetadataEvent::GraphSnapshotResult { .. } => "graph_snapshot_result",
        MetadataEvent::SnapshotInstalled { .. } => "snapshot_installed",
        MetadataEvent::Error { .. } => "error",
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

pub(crate) fn metadata_read_error(error: MetadataError) -> MetadataReadError {
    match error {
        MetadataError::GraphNotFound => MetadataReadError::NotFound,
        MetadataError::InvalidInput(_)
        | MetadataError::ChannelClosed
        | MetadataError::InvalidEffect
        | MetadataError::HandleMissing
        | MetadataError::TaskJoin(_)
        | MetadataError::Validation(_)
        | MetadataError::ProfileValidation(_)
        | MetadataError::Persist(_)
        | MetadataError::Storage(_)
        | MetadataError::Backend(_) => MetadataReadError::Unavailable,
    }
}

pub(super) fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::ValidateCreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ValidateRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::CreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::ApplyRoCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::UpsertDataEntity { request }
        | MetadataEffect::UpsertContextualEntity { request } => Some(request.graph_iri.clone()),
        MetadataEffect::SetGraphPolicy { graph_iri, .. }
        | MetadataEffect::AddGraphPeer { graph_iri, .. }
        | MetadataEffect::SyncGraphBestEffort { graph_iri, .. }
        | MetadataEffect::GetGraphPolicy { graph_iri }
        | MetadataEffect::ExportRoCrate { graph_iri }
        | MetadataEffect::ExportRoCrateSummary { graph_iri }
        | MetadataEffect::DeleteGraph { graph_iri }
        | MetadataEffect::ContainsGraph { graph_iri }
        | MetadataEffect::GraphSnapshot { graph_iri }
        | MetadataEffect::InstallSnapshot { graph_iri, .. }
        | MetadataEffect::PlanBatch { graph_iri, .. }
        | MetadataEffect::MergeBatch { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::ExportRoCratePage { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::SearchGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::QueryGraphs { graph_iris, .. } => graph_iris
            .as_ref()
            .and_then(|graph_iris| graph_iris.first().cloned()),
        MetadataEffect::ListGraphs => None,
    }
}

pub(super) fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
}

#[async_trait]
impl Handle for MetadataHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Metadata(metadata_effect) => self.send_metadata_effect(metadata_effect).await,
            _ => Event::Metadata(MetadataEvent::Error {
                graph_iri: None,
                error: MetadataError::InvalidEffect,
            }),
        }
    }
}

fn validate_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::ValidateCreateCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let call_span = debug_span!(
                "metadata.backend.craqle.validate_create_crate",
                graph_iri = %graph_iri,
                name_len = request.name.len() as u64,
                description_len = request.description.len() as u64,
                public = request.policy.public,
                permission_path_count = request.policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.validate_create_crate(auth, craqle_create_request(request)))
                .map(|_| MetadataEvent::ValidationResult {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "validate_create_crate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ValidateRoCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let policy = request.policy;
            let jsonld = request.jsonld;
            let call_span = debug_span!(
                "metadata.backend.craqle.validate_rocrate",
                graph_iri = %graph_iri,
                jsonld_len = jsonld.len() as u64,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.validate_rocrate_document_checked_with_policy(
                        auth,
                        GraphId::new(&graph_iri),
                        &jsonld,
                        craqle_graph_policy(policy),
                    )
                })
                .map(|_| MetadataEvent::ValidationResult {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "validate_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn crate_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::CreateCrate { request } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.create_crate",
                graph_iri = %request.graph_iri,
                name_len = request.name.len() as u64,
                description_len = request.description.len() as u64,
                public = request.policy.public,
                permission_path_count = request.policy.permission_paths.len() as u64,
                durability = ?request.durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let durability = request.durability;
            let actor = request.deterministic_actor.map(ActorId::from_bytes);
            let result = call_span.in_scope(|| {
                node.create_crate_with_durability_as(
                    auth,
                    craqle_create_request(request.clone()),
                    craqle_request_durability(durability),
                    actor,
                )
            });
            record_craqle_result(
                &call_span,
                "create_crate",
                Some(&request.graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::CreateCrateResult {
                graph_iri: request.graph_iri,
                batch: batch_from_craqle(batch),
            })
        }
        MetadataEffect::ApplyRoCrate { request } => {
            let graph_iri = request.graph_iri.clone();
            let policy = request.policy;
            let jsonld = request.jsonld;
            let durability = request.durability;
            let actor = request.deterministic_actor.map(ActorId::from_bytes);
            let call_span = debug_span!(
                "metadata.backend.craqle.apply_rocrate",
                graph_iri = %graph_iri,
                jsonld_len = jsonld.len() as u64,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| {
                node.apply_rocrate_document_checked_with_policy_and_durability_as(
                    auth,
                    GraphId::new(&graph_iri),
                    &jsonld,
                    craqle_graph_policy(policy),
                    craqle_request_durability(durability),
                    actor,
                )
            });
            record_craqle_result(
                &call_span,
                "apply_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            if let Ok(batch) = &result {
                call_span.record("batch_ops", batch.ops.len() as u64);
            }
            result.map(|batch| MetadataEvent::ApplyRoCrateResult {
                graph_iri,
                batch: batch_from_craqle(batch),
            })
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn entity_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::UpsertDataEntity { request } => {
            let graph_iri = request.graph_iri.clone();
            let durability = request.durability;
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_data_entity",
                graph_iri = %graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_data_entity(&node, auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_data_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        MetadataEffect::UpsertContextualEntity { request } => {
            let graph_iri = request.graph_iri.clone();
            let durability = request.durability;
            let call_span = debug_span!(
                "metadata.backend.craqle.upsert_contextual_entity",
                graph_iri = %graph_iri,
                jsonld_len = request.jsonld.len() as u64,
                durability = ?durability,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| upsert_contextual_entity(&node, auth, request));
            let converted = result.map(|batch| {
                call_span.record("batch_ops", batch.ops.len() as u64);
                MetadataEvent::EntityUpsertResult {
                    graph_iri: batch.graph_iri.clone(),
                    batch,
                }
            });
            record_metadata_result(
                &call_span,
                "upsert_contextual_entity",
                Some(&graph_iri),
                started,
                &converted,
            );
            converted
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn policy_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::SetGraphPolicy { graph_iri, policy } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.set_graph_policy",
                graph_iri = %graph_iri,
                public = policy.public,
                permission_path_count = policy.permission_paths.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.set_graph_policy(
                        auth,
                        &GraphId::new(&graph_iri),
                        craqle_graph_policy(policy),
                    )
                })
                .map(|_| MetadataEvent::GraphPolicySet {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "set_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::AddGraphPeer { graph_iri, node_id } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.add_graph_peer",
                graph_iri = %graph_iri,
                peer = ?node_id,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| {
                    node.add_irokle_peer(&GraphId::new(&graph_iri), irokle_peer_id(node_id))
                })
                .map(|_| MetadataEvent::GraphPeerAdded {
                    graph_iri: graph_iri.clone(),
                    node_id,
                });
            record_metadata_result(
                &call_span,
                "add_graph_peer",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::GetGraphPolicy { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.get_graph_policy",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.graph_policy(&GraphId::new(&graph_iri)))
                .map(|policy| MetadataEvent::GraphPolicyResult {
                    graph_iri: graph_iri.clone(),
                    policy: policy_from_craqle(policy),
                });
            record_metadata_result(
                &call_span,
                "get_graph_policy",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn export_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::ExportRoCrate { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate(auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateExportResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCrateSummary { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_summary",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                jsonld_len = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.export_rocrate_summary(auth, &GraphId::new(&graph_iri)))
                .map(|jsonld| {
                    call_span.record("jsonld_len", jsonld.len() as u64);
                    MetadataEvent::RoCrateSummaryResult {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                    }
                });
            record_metadata_result(
                &call_span,
                "export_rocrate_summary",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ExportRoCratePage {
            graph_iri,
            offset,
            after,
            limit,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.export_rocrate_page",
                graph_iri = %graph_iri,
                offset = offset.unwrap_or(0) as u64,
                after_present = after.is_some(),
                limit = limit as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                returned_data_entities = field::Empty,
                total_data_entities = field::Empty,
            );
            let started = Instant::now();
            let graph = GraphId::new(&graph_iri);
            let page = call_span.in_scope(|| {
                if let Some(after) = after.as_deref() {
                    node.export_rocrate_page_after(auth, &graph, Some(after), limit)
                } else {
                    node.export_rocrate_page(auth, &graph, offset.unwrap_or(0), limit)
                }
            });
            let result = page.map(|page| {
                call_span.record("returned_data_entities", page.returned_data_entities as u64);
                call_span.record("total_data_entities", page.total_data_entities as u64);
                MetadataEvent::RoCratePageResult {
                    graph_iri: graph_iri.clone(),
                    page: page_from_craqle(page),
                }
            });
            record_metadata_result(
                &call_span,
                "export_rocrate_page",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn graph_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        MetadataEffect::DeleteGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.delete_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.delete_graph(auth, &GraphId::new(&graph_iri)))
                .map(|_| MetadataEvent::GraphDeleted {
                    graph_iri: graph_iri.clone(),
                });
            record_metadata_result(
                &call_span,
                "delete_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::ListGraphs => {
            let call_span = debug_span!(
                "metadata.backend.craqle.list_graphs",
                elapsed_ms = field::Empty,
                result = field::Empty,
                graph_count = field::Empty,
            );
            let started = Instant::now();
            let result = call_span.in_scope(|| node.graphs()).map(|graphs| {
                call_span.record("graph_count", graphs.len() as u64);
                MetadataEvent::GraphListResult {
                    graph_iris: graphs
                        .into_iter()
                        .map(|graph| graph.as_str().to_string())
                        .collect(),
                }
            });
            record_metadata_result(&call_span, "list_graphs", None, started, &result);
            result
        }
        MetadataEffect::ContainsGraph { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.contains_graph",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                exists = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.contains_graph(&GraphId::new(&graph_iri)))
                .map(|exists| {
                    call_span.record("exists", exists);
                    MetadataEvent::ContainsGraphResult {
                        graph_iri: graph_iri.clone(),
                        exists,
                    }
                });
            record_metadata_result(
                &call_span,
                "contains_graph",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn sync_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, CraqleError> {
    match effect {
        // Device replicas
        MetadataEffect::GraphSnapshot { graph_iri } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.graph_snapshot",
                graph_iri = %graph_iri,
                elapsed_ms = field::Empty,
                result = field::Empty,
                quad_count = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.graph_snapshot(&GraphId::new(&graph_iri)))
                .map(|snapshot| {
                    call_span.record("quad_count", snapshot.quads.len() as u64);
                    MetadataEvent::GraphSnapshotResult {
                        graph_iri: graph_iri.clone(),
                        snapshot: Box::new(snapshot),
                    }
                });
            record_metadata_result(
                &call_span,
                "graph_snapshot",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        MetadataEffect::InstallSnapshot {
            graph_iri,
            snapshot,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.install_snapshot",
                graph_iri = %graph_iri,
                quad_count = snapshot.quads.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                applied = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| node.install_graph_snapshot(&snapshot))
                .map(|merged| {
                    call_span.record("applied", merged.applied);
                    MetadataEvent::SnapshotInstalled {
                        graph_iri: graph_iri.clone(),
                        applied: merged.applied,
                    }
                });
            record_metadata_result(
                &call_span,
                "install_snapshot",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        // OR-Set metadata graphs
        MetadataEffect::PlanBatch {
            graph_iri,
            actor,
            source,
        } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.plan_batch",
                graph_iri = %graph_iri,
                jsonld_len = source.jsonld().len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                batch_ops = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| plan_batch(&node, auth, &graph_iri, actor, &source))
                .map(|batch| {
                    call_span.record("batch_ops", batch.ops.len() as u64);
                    MetadataEvent::BatchPlanned {
                        graph_iri: graph_iri.clone(),
                        batch,
                    }
                });
            record_metadata_result(&call_span, "plan_batch", Some(&graph_iri), started, &result);
            result
        }
        MetadataEffect::MergeBatch { graph_iri, batch } => {
            let call_span = debug_span!(
                "metadata.backend.craqle.merge_batch",
                graph_iri = %graph_iri,
                batch_ops = batch.ops.len() as u64,
                elapsed_ms = field::Empty,
                result = field::Empty,
                applied = field::Empty,
            );
            let started = Instant::now();
            let result = call_span
                .in_scope(|| to_craqle_batch(&batch).and_then(|batch| node.merge_batch(&batch)))
                .map(|merged| {
                    call_span.record("applied", merged.applied);
                    MetadataEvent::BatchMerged {
                        graph_iri: graph_iri.clone(),
                        applied: merged.applied,
                    }
                });
            record_metadata_result(
                &call_span,
                "merge_batch",
                Some(&graph_iri),
                started,
                &result,
            );
            result
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn handle_effect(inner: Arc<MetadataInner>, effect: MetadataEffect) -> MetadataEvent {
    let effect_name = metadata_effect_kind(&effect);
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let needs_existing_graph = matches!(
        effect,
        MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
    );
    let persist_document_sync_after_success = effect_persists_sync(&effect);
    let deferred_persist_after_success = effect_defers_persist(&effect);
    let node = inner.node.clone();
    let effect_span = debug_span!(
        "metadata.backend.effect",
        effect = effect_name,
        graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
        elapsed_ms = field::Empty,
        result = field::Empty,
    );
    let effect_started = Instant::now();

    let result = effect_span.in_scope(|| match effect {
        effect @ (MetadataEffect::ValidateCreateCrate { .. }
        | MetadataEffect::ValidateRoCrate { .. }) => validate_effect(&node, &auth, effect),
        effect @ (MetadataEffect::CreateCrate { .. } | MetadataEffect::ApplyRoCrate { .. }) => {
            crate_effect(&node, &auth, effect)
        }
        effect @ (MetadataEffect::UpsertDataEntity { .. }
        | MetadataEffect::UpsertContextualEntity { .. }) => entity_effect(&node, &auth, effect),
        effect @ (MetadataEffect::SetGraphPolicy { .. }
        | MetadataEffect::AddGraphPeer { .. }
        | MetadataEffect::GetGraphPolicy { .. }) => policy_effect(&node, &auth, effect),
        effect @ (MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportRoCratePage { .. }) => export_effect(&node, &auth, effect),
        MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SyncGraphBestEffort { .. } => unreachable!("handled asynchronously"),
        effect @ (MetadataEffect::DeleteGraph { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::ContainsGraph { .. }) => graph_effect(&node, &auth, effect),
        effect @ (MetadataEffect::GraphSnapshot { .. }
        | MetadataEffect::InstallSnapshot { .. }
        | MetadataEffect::PlanBatch { .. }
        | MetadataEffect::MergeBatch { .. }) => sync_effect(&node, &auth, effect),
    });

    let persist_error = if persist_document_sync_after_success && result.is_ok() {
        flush_sync_journal(&inner, effect_name, graph_iri.as_deref()).err()
    } else {
        None
    };
    if result.is_ok() && persist_error.is_none() && deferred_persist_after_success {
        schedule_deferred_persist(inner.clone(), effect_name, graph_iri.clone());
    }
    record_elapsed_ms(&effect_span, "elapsed_ms", effect_started);
    let event = match (result, persist_error) {
        (_, Some(error)) => MetadataEvent::Error { graph_iri, error },
        (Ok(event), None) => event,
        (Err(error), None) => {
            // Craqle has no typed missing-graph error, so probe existence to tell
            // an unmaterialized graph (retryable, 503) from a backend failure.
            let error = if needs_existing_graph
                && graph_iri
                    .as_deref()
                    .is_some_and(|iri| matches!(node.contains_graph(&GraphId::new(iri)), Ok(false)))
            {
                MetadataError::GraphNotFound
            } else {
                error_from_craqle(error)
            };
            MetadataEvent::Error { graph_iri, error }
        }
    };
    effect_span.record("result", metadata_event_kind(&event));
    if let MetadataEvent::Error { error, .. } = &event {
        record_error(&effect_span, &error.to_string());
    }
    event
}
