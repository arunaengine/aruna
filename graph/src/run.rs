//! Runs each metadata effect against the craqle store and turns the result into an event.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::metadata::{MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::profile_shapes::ProfileShaclReport;
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
        let effect = match effect {
            MetadataEffect::CheckStructure { jsonld } => {
                let report = self.run_profile_shacl(move |engine| {
                    let structural = engine.structural(&jsonld)?;
                    Ok(ProfileShaclReport {
                        results: Vec::new(),
                        structural,
                    })
                });
                let report = report.await.map(Box::new);
                return MetadataEvent::ProfileChecked { report };
            }
            MetadataEffect::CheckProfile { shapes, jsonld } => {
                let report =
                    self.run_profile_shacl(move |engine| engine.evaluate(&shapes, &jsonld));
                let report = report.await.map(Box::new);
                return MetadataEvent::ProfileChecked { report };
            }
            MetadataEffect::CachedShapes {
                profile_id,
                revision,
            } => {
                let shapes = self.inner.profiles.get(profile_id, revision);
                let shapes = shapes.map(|shapes| shapes.as_ref().clone());
                return MetadataEvent::ProfileShapes { shapes };
            }
            MetadataEffect::CacheShapes {
                profile_id,
                revision,
                shapes,
            } => {
                self.inner
                    .profiles
                    .insert(profile_id, revision, Arc::new(shapes));
                return MetadataEvent::ProfileShapes { shapes: None };
            }
            effect => effect,
        };
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
        span.record("result", event.label());
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
    result: &Result<T, Box<CraqleError>>,
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
    result: &Result<MetadataEvent, Box<CraqleError>>,
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                .in_scope(|| {
                    node.validate_create_crate(auth, craqle_create_request(request))
                        .map_err(Box::new)
                })
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
                    .map_err(Box::new)
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                .map_err(Box::new)
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
                .map_err(Box::new)
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                    let write = PolicyWrite {
                        graph: &GraphId::new(&graph_iri),
                        policy: craqle_graph_policy(policy),
                        durability: CraqleRequestDurability::WalAlreadyDurable,
                    };
                    node.set_policy_with(auth, write).map_err(Box::new)
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
                        .map_err(Box::new)
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
                .in_scope(|| {
                    node.graph_policy(&GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                .in_scope(|| {
                    node.export_rocrate(auth, &GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
        MetadataEffect::ExportVersioned { graph_iri } => {
            // The version only counts when no dot landed during the export.
            let graph = GraphId::new(&graph_iri);
            for _ in 0..8 {
                let before = node.vector_clock(&graph)?;
                let jsonld = node.export_rocrate(auth, &graph)?;
                if node.vector_clock(&graph)? == before {
                    return Ok(MetadataEvent::VersionedExport {
                        graph_iri: graph_iri.clone(),
                        jsonld,
                        version: aruna_core::metadata::graph_version(&before),
                    });
                }
            }
            Err(Box::new(CraqleError::RoCrate(RoCrateError::InvalidGraph(
                format!("metadata graph `{graph_iri}` kept changing while exporting"),
            ))))
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
                .in_scope(|| {
                    node.export_rocrate_summary(auth, &GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
                let page = match after.as_deref() {
                    Some(after) => node.export_rocrate_page_after(auth, &graph, Some(after), limit),
                    None => node.export_rocrate_page(auth, &graph, offset.unwrap_or(0), limit),
                };
                page.map_err(Box::new)
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
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                .in_scope(|| {
                    node.delete_graph(auth, &GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
            let result = call_span
                .in_scope(|| node.graphs().map_err(Box::new))
                .map(|graphs| {
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
                .in_scope(|| {
                    node.contains_graph(&GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
        MetadataEffect::ContainsDot {
            graph_iri,
            actor,
            counter,
        } => {
            let graph = GraphId::new(&graph_iri);
            let dot = craqle::Dot {
                actor: craqle::ActorId::from_bytes(actor),
                counter,
            };
            let contains = match node.contains_graph(&graph) {
                Ok(true) => node.vector_clock(&graph).map(|clock| clock.contains(&dot)),
                other => other,
            };
            contains
                .map_err(Box::new)
                .map(|contains| MetadataEvent::ContainsDotResult {
                    graph_iri: graph_iri.clone(),
                    contains,
                })
        }
        _ => unreachable!("effect family routed incorrectly"),
    }
}

fn sync_effect(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    effect: MetadataEffect,
) -> Result<MetadataEvent, Box<CraqleError>> {
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
                .in_scope(|| {
                    node.graph_snapshot(&GraphId::new(&graph_iri))
                        .map_err(Box::new)
                })
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
                .in_scope(|| node.install_graph_snapshot(&snapshot).map_err(Box::new))
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
            counter,
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
                .in_scope(|| plan_batch(node, auth, &graph_iri, (actor, counter), &source))
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
                .in_scope(|| {
                    to_craqle_batch(&batch)
                        .and_then(|batch| node.merge_batch(&batch).map_err(Box::new))
                })
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

fn handle_effect(inner: Arc<GraphInner>, effect: MetadataEffect) -> MetadataEvent {
    let effect_name = effect.kind();
    let auth = AllowAllAuthorizer;
    let graph_iri = effect.graph_iri();
    let needs_existing_graph = matches!(
        effect,
        MetadataEffect::ExportRoCrate { .. }
            | MetadataEffect::ExportRoCrateSummary { .. }
            | MetadataEffect::ExportRoCratePage { .. }
            | MetadataEffect::PlanBatch { .. }
    );
    let persist_sync_success = effect_persists_sync(&effect);
    let deferred_persist_success = effect_defers_persist(&effect);
    let node = &inner.node;
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
        | MetadataEffect::ValidateRoCrate { .. }) => validate_effect(node, &auth, effect),
        effect @ (MetadataEffect::CreateCrate { .. } | MetadataEffect::ApplyRoCrate { .. }) => {
            crate_effect(node, &auth, effect)
        }
        effect @ (MetadataEffect::UpsertDataEntity { .. }
        | MetadataEffect::UpsertContextualEntity { .. }) => entity_effect(node, &auth, effect),
        effect @ (MetadataEffect::SetGraphPolicy { .. }
        | MetadataEffect::AddGraphPeer { .. }
        | MetadataEffect::GetGraphPolicy { .. }) => policy_effect(node, &auth, effect),
        effect @ (MetadataEffect::ExportRoCrate { .. }
        | MetadataEffect::ExportRoCrateSummary { .. }
        | MetadataEffect::ExportRoCratePage { .. }
        | MetadataEffect::ExportVersioned { .. }) => export_effect(node, &auth, effect),
        MetadataEffect::SearchGraphs { .. }
        | MetadataEffect::QueryGraphs { .. }
        | MetadataEffect::SyncBestEffort { .. }
        | MetadataEffect::CheckStructure { .. }
        | MetadataEffect::CheckProfile { .. }
        | MetadataEffect::CachedShapes { .. }
        | MetadataEffect::CacheShapes { .. } => unreachable!("handled asynchronously"),
        effect @ (MetadataEffect::DeleteGraph { .. }
        | MetadataEffect::ListGraphs
        | MetadataEffect::ContainsGraph { .. }
        | MetadataEffect::ContainsDot { .. }) => graph_effect(node, &auth, effect),
        effect @ (MetadataEffect::GraphSnapshot { .. }
        | MetadataEffect::InstallSnapshot { .. }
        | MetadataEffect::PlanBatch { .. }
        | MetadataEffect::MergeBatch { .. }) => sync_effect(node, &auth, effect),
    });

    let persist_error = if persist_sync_success && result.is_ok() {
        flush_sync_journal(&inner, effect_name, graph_iri.as_deref()).err()
    } else {
        None
    };
    if result.is_ok() && persist_error.is_none() && deferred_persist_success {
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
                error_from_craqle(*error)
            };
            MetadataEvent::Error { graph_iri, error }
        }
    };
    effect_span.record("result", event.label());
    if let MetadataEvent::Error { error, .. } = &event {
        record_error(&effect_span, &error.to_string());
    }
    event
}
