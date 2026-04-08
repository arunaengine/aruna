use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::metadata::{
    MetadataBatch, MetadataCompactSnapshot, MetadataCompactSnapshotQuadState,
    MetadataCreateCrateRequest, MetadataDot, MetadataEffect, MetadataError, MetadataEvent,
    MetadataGraphPolicy, MetadataQuadOp, MetadataQueryResults, MetadataVectorClock,
};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use craqle::{
    ActorId, AllowAllAuthorizer, Batch, CraqleNode, CreateCrateRequest, GraphId, GraphPolicy,
    QueryResults,
};

#[derive(Clone)]
pub struct MetadataHandle {
    inner: Arc<CraqleNode>,
}

impl std::fmt::Debug for MetadataHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataHandle").finish_non_exhaustive()
    }
}

impl MetadataHandle {
    pub fn new(path: impl AsRef<Path>, node_id: NodeId) -> Result<Self, MetadataError> {
        let actor = ActorId::from_bytes(*node_id.as_bytes());
        let node = CraqleNode::open_with_actor(path, actor)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(node),
        })
    }

    pub async fn send_metadata_effect(&self, effect: MetadataEffect) -> Event {
        let graph_iri = effect_graph_iri(&effect);
        let node = self.inner.clone();
        match tokio::task::spawn_blocking(move || handle_effect(node, effect)).await {
            Ok(event) => Event::Metadata(event),
            Err(error) => Event::Metadata(MetadataEvent::Error {
                graph_iri,
                error: MetadataError::TaskJoin(error.to_string()),
            }),
        }
    }
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

fn handle_effect(node: Arc<CraqleNode>, effect: MetadataEffect) -> MetadataEvent {
    let auth = AllowAllAuthorizer;
    let graph_iri = effect_graph_iri(&effect);
    let result = match effect {
        MetadataEffect::CreateCrate { request } => node
            .create_crate(&auth, craqle_create_request(request.clone()))
            .map(|batch| MetadataEvent::CreateCrateResult {
                graph_iri: request.graph_iri,
                batch: metadata_batch_from_craqle(batch),
            }),
        MetadataEffect::SetGraphPolicy { graph_iri, policy } => node
            .import_graph_policy(&GraphId::new(&graph_iri), craqle_graph_policy(policy))
            .map(|_| MetadataEvent::GraphPolicySet { graph_iri }),
        MetadataEffect::GetGraphPolicy { graph_iri } => node
            .graph_policy(&GraphId::new(&graph_iri))
            .map(|policy| MetadataEvent::GraphPolicyResult {
                graph_iri,
                policy: metadata_graph_policy_from_craqle(policy),
            }),
        MetadataEffect::ExportRoCrate { graph_iri } => node
            .export_rocrate(&auth, &GraphId::new(&graph_iri))
            .map(|jsonld| MetadataEvent::RoCrateExportResult { graph_iri, jsonld }),
        MetadataEffect::QueryGraphs { graph_iris, sparql } => node
            .query_graphs(&graph_ids(&graph_iris), &sparql)
            .map(|results| MetadataEvent::QueryResult {
                results: metadata_query_results_from_craqle(results),
            }),
        MetadataEffect::DeleteGraph { graph_iri } => node
            .delete_graph_unchecked(&GraphId::new(&graph_iri))
            .map(|_| MetadataEvent::GraphDeleted { graph_iri }),
        MetadataEffect::ListGraphs => node.graphs().map(|graphs| MetadataEvent::GraphListResult {
            graph_iris: graphs
                .into_iter()
                .map(|graph| graph.as_str().to_string())
                .collect(),
        }),
        MetadataEffect::ContainsGraph { graph_iri } => node
            .contains_graph(&GraphId::new(&graph_iri))
            .map(|exists| MetadataEvent::ContainsGraphResult { graph_iri, exists }),
        MetadataEffect::VectorClock { graph_iri } => node
            .vector_clock(&GraphId::new(&graph_iri))
            .map(|clock| MetadataEvent::VectorClockResult {
                graph_iri,
                clock: metadata_vector_clock_from_craqle(clock),
            }),
        MetadataEffect::CatchupBatches {
            graph_iri,
            remote_clock,
        } => node
            .catchup_batches(
                &GraphId::new(&graph_iri),
                &craqle_vector_clock(remote_clock),
            )
            .map(|batches| MetadataEvent::CatchupBatchesResult {
                graph_iri,
                batches: batches
                    .into_iter()
                    .map(metadata_batch_from_craqle)
                    .collect(),
            }),
        MetadataEffect::CompactSnapshot { graph_iri } => node
            .compact_graph_snapshot(&GraphId::new(&graph_iri))
            .map(|snapshot| MetadataEvent::CompactSnapshotResult {
                graph_iri,
                snapshot: metadata_compact_snapshot_from_craqle(snapshot),
            }),
        MetadataEffect::ImportCompactSnapshot { snapshot, policy } => {
            let graph_iri = snapshot.graph_iri.clone();
            node.import_compact_graph_snapshot(
                &craqle_compact_snapshot(snapshot),
                craqle_graph_policy(policy),
            )
            .map(|_| MetadataEvent::CompactSnapshotImported { graph_iri })
        }
        MetadataEffect::ApplyRemoteBatch { batch } => {
            let graph_iri = batch.graph_iri.clone();
            node.apply_remote_batch(craqle_batch(batch))
                .map(|_| MetadataEvent::RemoteBatchApplied { graph_iri })
        }
    };

    result.unwrap_or_else(|error| MetadataEvent::Error {
        graph_iri,
        error: MetadataError::Backend(error.to_string()),
    })
}

fn effect_graph_iri(effect: &MetadataEffect) -> Option<String> {
    match effect {
        MetadataEffect::CreateCrate { request } => Some(request.graph_iri.clone()),
        MetadataEffect::SetGraphPolicy { graph_iri, .. }
        | MetadataEffect::GetGraphPolicy { graph_iri }
        | MetadataEffect::ExportRoCrate { graph_iri }
        | MetadataEffect::DeleteGraph { graph_iri }
        | MetadataEffect::ContainsGraph { graph_iri }
        | MetadataEffect::VectorClock { graph_iri }
        | MetadataEffect::CompactSnapshot { graph_iri } => Some(graph_iri.clone()),
        MetadataEffect::QueryGraphs { graph_iris, .. } => graph_iris.first().cloned(),
        MetadataEffect::CatchupBatches { graph_iri, .. } => Some(graph_iri.clone()),
        MetadataEffect::ImportCompactSnapshot { snapshot, .. } => Some(snapshot.graph_iri.clone()),
        MetadataEffect::ApplyRemoteBatch { batch } => Some(batch.graph_iri.clone()),
        MetadataEffect::ListGraphs => None,
    }
}

fn graph_ids(graph_iris: &[String]) -> Vec<GraphId> {
    graph_iris
        .iter()
        .map(|graph_iri| GraphId::new(graph_iri))
        .collect()
}

fn craqle_create_request(request: MetadataCreateCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn metadata_graph_policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

fn metadata_query_results_from_craqle(results: QueryResults) -> MetadataQueryResults {
    match results {
        QueryResults::Solutions(rows) => MetadataQueryResults::Solutions(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(key, value)| (key, value.0))
                        .collect::<BTreeMap<_, _>>()
                })
                .collect(),
        ),
        QueryResults::Boolean(value) => MetadataQueryResults::Boolean(value),
        QueryResults::Graph(triples) => MetadataQueryResults::Graph(
            triples
                .into_iter()
                .map(|(subject, predicate, object)| (subject.0, predicate.0, object.0))
                .collect(),
        ),
    }
}

fn metadata_vector_clock_from_craqle(clock: craqle::VectorClock) -> MetadataVectorClock {
    MetadataVectorClock(
        clock
            .0
            .into_iter()
            .map(|(actor, counter)| (*actor.as_bytes(), counter))
            .collect(),
    )
}

fn craqle_vector_clock(clock: MetadataVectorClock) -> craqle::VectorClock {
    craqle::VectorClock(
        clock
            .0
            .into_iter()
            .map(|(actor, counter)| (ActorId::from_bytes(actor), counter))
            .collect(),
    )
}

fn metadata_dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

fn craqle_dot(dot: MetadataDot) -> craqle::Dot {
    craqle::Dot {
        actor: ActorId::from_bytes(dot.actor),
        counter: dot.counter,
    }
}

fn metadata_batch_from_craqle(batch: Batch) -> MetadataBatch {
    MetadataBatch {
        graph_iri: batch.graph.as_str().to_string(),
        actor: *batch.actor.as_bytes(),
        counter: batch.counter,
        base_clock: metadata_vector_clock_from_craqle(batch.base_clock),
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                craqle::QuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => MetadataQuadOp::Add {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    dot: metadata_dot_from_craqle(dot),
                },
                craqle::QuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => MetadataQuadOp::Remove {
                    subject: subject.0,
                    predicate: predicate.0,
                    object: object.0,
                    witnessed: metadata_vector_clock_from_craqle(witnessed),
                },
            })
            .collect(),
        timestamp_millis: batch.timestamp.timestamp_millis(),
    }
}

fn craqle_batch(batch: MetadataBatch) -> Batch {
    Batch {
        graph: GraphId::new(&batch.graph_iri),
        actor: ActorId::from_bytes(batch.actor),
        counter: batch.counter,
        base_clock: craqle_vector_clock(batch.base_clock),
        ops: batch
            .ops
            .into_iter()
            .map(|op| match op {
                MetadataQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => craqle::QuadOp::Add {
                    subject: craqle::EncodedTerm(subject),
                    predicate: craqle::EncodedTerm(predicate),
                    object: craqle::EncodedTerm(object),
                    dot: craqle_dot(dot),
                },
                MetadataQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => craqle::QuadOp::Remove {
                    subject: craqle::EncodedTerm(subject),
                    predicate: craqle::EncodedTerm(predicate),
                    object: craqle::EncodedTerm(object),
                    witnessed: craqle_vector_clock(witnessed),
                },
            })
            .collect(),
        timestamp: Utc
            .timestamp_millis_opt(batch.timestamp_millis)
            .single()
            .unwrap_or_else(|| {
                Utc.timestamp_millis_opt(0)
                    .single()
                    .expect("unix epoch exists")
            }),
    }
}

fn metadata_compact_snapshot_from_craqle(
    snapshot: craqle::GraphReplicaCompactSnapshot,
) -> MetadataCompactSnapshot {
    MetadataCompactSnapshot {
        graph_iri: snapshot.graph.as_str().to_string(),
        clock: metadata_vector_clock_from_craqle(snapshot.clock),
        terms: snapshot.terms.into_iter().map(|term| term.0).collect(),
        quads: snapshot
            .quads
            .into_iter()
            .map(|quad| MetadataCompactSnapshotQuadState {
                subject: quad.subject,
                predicate: quad.predicate,
                object: quad.object,
                dots: quad
                    .dots
                    .into_iter()
                    .map(metadata_dot_from_craqle)
                    .collect(),
            })
            .collect(),
    }
}

fn craqle_compact_snapshot(
    snapshot: MetadataCompactSnapshot,
) -> craqle::GraphReplicaCompactSnapshot {
    craqle::GraphReplicaCompactSnapshot {
        graph: GraphId::new(&snapshot.graph_iri),
        clock: craqle_vector_clock(snapshot.clock),
        terms: snapshot
            .terms
            .into_iter()
            .map(craqle::EncodedTerm)
            .collect(),
        quads: snapshot
            .quads
            .into_iter()
            .map(|quad| craqle::CompactSnapshotQuadState {
                subject: quad.subject,
                predicate: quad.predicate,
                object: quad.object,
                dots: quad.dots.into_iter().map(craqle_dot).collect(),
            })
            .collect(),
    }
}
