//! Runs each metadata effect against the craqle store and turns the result into an event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::telemetry::{duration_ms, record_duration_ms, record_elapsed_ms, record_error};
use craqle::{
    ActorId, AllowAllAuthorizer, CraqleError, CraqleNode, CraqleRequestDurability, GraphId,
    PolicyWrite, RoCrateError,
};
use tracing::{Span, debug_span, field, warn};

use crate::convert::{
    batch_from_craqle, craqle_create_request, craqle_graph_policy, craqle_request_durability,
    error_from_craqle, irokle_peer_id, page_from_craqle, plan_batch, policy_from_craqle,
    to_craqle_batch, upsert_contextual_entity, upsert_data_entity,
};
use crate::persist::{
    effect_defers_persist, effect_persists_sync, flush_sync_journal, schedule_deferred_persist,
};
use crate::{CRAQLE_LATENCY, GraphHandle, GraphInner, SLOW_CALL};

impl GraphHandle {
    /// Runs one store effect on the blocking pool and answers with its event. Searches,
    /// queries and best-effort sync need a read scope or the network and are refused here.
    pub async fn run(&self, effect: MetadataEffect) -> MetadataEvent {
        let graph_iri = effect.graph_iri();
        if matches!(
            effect,
            MetadataEffect::SearchGraphs { .. }
                | MetadataEffect::QueryGraphs { .. }
                | MetadataEffect::SyncBestEffort { .. }
        ) {
            return MetadataEvent::Error {
                graph_iri,
                error: MetadataError::InvalidEffect,
            };
        }
        let inner = self.inner.clone();
        let span = debug_span!(
            "metadata.backend.blocking_task",
            effect = effect.kind(),
            graph_iri = graph_iri.as_deref().unwrap_or("<none>"),
            elapsed_ms = field::Empty,
            result = field::Empty,
        );
        let blocking_span = span.clone();
        let started = Instant::now();
        // Heavy mutations and cheap reads queue on separate pools so
        // trivial reads never wait behind long materializations.
        let permits = if effect.mutates_graph() {
            self.inner.write_permits.clone()
        } else {
            self.inner.read_permits.clone()
        };
        let _permit = permits.acquire_owned().await.ok();
        let event = match tokio::task::spawn_blocking(move || {
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
        span.record("result", event.kind());
        event
    }
}

pub(crate) fn warn_slow_call(operation: &'static str, graph_iri: Option<&str>, duration: Duration) {
    CRAQLE_LATENCY.record(operation, duration);
    if duration >= SLOW_CALL {
        warn!(
            event = "metadata.backend.slow_call",
            operation,
            graph_iri = graph_iri.unwrap_or("<none>"),
            duration_ms = duration_ms(duration),
            threshold_ms = duration_ms(SLOW_CALL),
            "Slow metadata backend call"
        );
    }
}

pub(crate) fn record_craqle_result<T>(
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

pub(crate) fn record_metadata_result(
    span: &Span,
    operation: &'static str,
    graph_iri: Option<&str>,
    started: Instant,
    result: &Result<MetadataEvent, CraqleError>,
) {
    record_craqle_result(span, operation, graph_iri, started, result);
}

pub(crate) fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
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
            let result = call_span.in_scope(|| upsert_data_entity(node, auth, request));
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
            let result = call_span.in_scope(|| upsert_contextual_entity(node, auth, request));
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
                    // Every holder applies the policy; publishing it would mint a rival topic genesis.
                    node.set_policy_with(
                        auth,
                        PolicyWrite {
                            graph: &GraphId::new(&graph_iri),
                            policy: craqle_graph_policy(policy),
                            durability: CraqleRequestDurability::WalAlreadyDurable,
                        },
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

