//! Converts between Aruna metadata requests and craqle types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::metadata::{
    MetadataBatch, MetadataBatchSource, MetadataCrateRequest, MetadataDot, MetadataError,
    MetadataGraphPolicy, MetadataQuadOp, MetadataRequestDurability, MetadataRoCratePage,
    MetadataValidationViolation, UpsertEntityRequest,
};
use aruna_core::entity_patch::craqle_patch_request;
use craqle::{
    ActorId, AllowAllAuthorizer, Batch, CraqleError, CraqleFjallPersistMode, CraqleNode,
    CraqleRequestDurability, CreateCrateRequest, CreateEntityRequest, DescribeRequest, Dot,
    GraphId, GraphPolicy, PatchEntityRequest, RemoveEntityRequest, RoCrateError, vocab,
};
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde_json::Value;

pub(crate) fn upsert_data_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: UpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    // An event materializes under its own actor, so a dot of it here means it is applied; a
    // replay changes nothing, as the re-added type, name and link would take a second dot.
    if let Some(actor) = actor {
        let clock = node.vector_clock(&graph)?;
        if clock.contains(&Dot { actor, counter: 1 }) {
            return Ok(MetadataBatch {
                graph_iri: request.graph_iri,
                actor: *actor.as_bytes(),
                counter: 0,
                base_clock: clock,
                ops: Vec::new(),
                timestamp_millis: chrono::Utc::now().timestamp_millis(),
            });
        }
    }
    let entity_request = craqle_patch_request(&graph, &request.jsonld)?;
    node.patch_data_with(
        auth,
        entity_request,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(batch_from_craqle)
}

pub(crate) fn upsert_contextual_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: UpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    let entity_request = craqle_patch_request(&graph, &request.jsonld)?;
    node.patch_contextual_with(
        auth,
        entity_request,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(batch_from_craqle)
}


pub(crate) fn error_from_craqle(error: CraqleError) -> MetadataError {
    match error {
        CraqleError::RoCrate(rocrate_error) => match rocrate_error {
            RoCrateError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
                metadata_violations(violations)
            }
            RoCrateError::Json(_) | RoCrateError::JsonLd(_) => {
                MetadataError::InvalidInput(rocrate_error.to_string())
            }
            RoCrateError::InvalidGraph(_)
            | RoCrateError::EntityNotFound(_)
            | RoCrateError::UnsupportedJsonLd(_)
            | RoCrateError::UnsupportedTerm(_)
            | RoCrateError::InvalidBatch(_) => {
                MetadataError::InvalidInput(rocrate_error.to_string())
            }
            other => MetadataError::Backend(other.to_string()),
        },
        CraqleError::Merge(error @ craqle::MergeError::MissingDependencies(_)) => {
            MetadataError::MissingDependencies(error.to_string())
        }
        CraqleError::SyncInputRejected(message) => MetadataError::InvalidInput(message),
        CraqleError::MultiGraphUpdateUnsupported => {
            MetadataError::InvalidInput("unsupported update across multiple graphs".to_string())
        }
        CraqleError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            metadata_violations(violations)
        }
        // Backend infrastructure, not the document: an apply that fails on disk
        // or in the search worker must keep retrying instead of being parked.
        error @ (CraqleError::Io(_) | CraqleError::Store(_) | CraqleError::SearchWorker(_)) => {
            MetadataError::Persist(error.to_string())
        }
        error if transient_kind(error.kind()) => MetadataError::Persist(error.to_string()),
        other => MetadataError::Backend(other.to_string()),
    }
}

// Search waits, memory budgets and cancelled work clear on retry, like storage faults.
fn transient_kind(kind: craqle::CraqleErrorKind) -> bool {
    use craqle::CraqleErrorKind::*;
    matches!(
        kind,
        Storage | CorruptDerivedData | DependencyUnavailable | Cancelled | ResourceLimit
    )
}

fn metadata_violations(violations: Vec<craqle::CrateViolation>) -> MetadataError {
    MetadataError::Validation(
        violations
            .into_iter()
            .map(|violation| MetadataValidationViolation {
                code: violation.code.to_string(),
                message: violation.message,
                pointer: violation.pointer,
                entity_id: violation.entity_id,
            })
            .collect(),
    )
}

pub(crate) fn craqle_create_request(request: MetadataCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

pub(crate) fn craqle_request_durability(
    durability: MetadataRequestDurability,
) -> CraqleRequestDurability {
    match durability {
        MetadataRequestDurability::Durable => CraqleRequestDurability::Durable,
        MetadataRequestDurability::WalAlreadyDurable => CraqleRequestDurability::WalAlreadyDurable,
    }
}

pub(crate) fn fjall_persist_mode(sync_all: bool) -> CraqleFjallPersistMode {
    if sync_all {
        CraqleFjallPersistMode::SyncAll
    } else {
        CraqleFjallPersistMode::Buffer
    }
}

pub(crate) fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(crate) fn irokle_peer_id(node_id: NodeId) -> irokle::PeerId {
    irokle::PeerId::from_bytes(*node_id.as_bytes())
}

pub(crate) fn policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(crate) fn dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

pub(crate) fn batch_from_craqle(batch: Batch) -> MetadataBatch {
    MetadataBatch {
        graph_iri: batch.graph.as_str().to_string(),
        actor: *batch.actor.as_bytes(),
        counter: batch.counter,
        base_clock: batch.base_clock,
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
                    dot: dot_from_craqle(dot),
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
                    witnessed,
                },
            })
            .collect(),
        timestamp_millis: batch.timestamp.timestamp_millis(),
    }
}

pub(crate) fn to_craqle_batch(batch: &MetadataBatch) -> Result<Batch, CraqleError> {
    let timestamp =
        chrono::DateTime::from_timestamp_millis(batch.timestamp_millis).ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::InvalidBatch(
                "batch timestamp is out of range".to_string(),
            ))
        })?;
    Ok(Batch {
        graph: GraphId::new(&batch.graph_iri),
        actor: ActorId::from_bytes(batch.actor),
        counter: batch.counter,
        base_clock: batch.base_clock.clone(),
        ops: batch
            .ops
            .iter()
            .map(|op| match op {
                MetadataQuadOp::Add {
                    subject,
                    predicate,
                    object,
                    dot,
                } => craqle::QuadOp::Add {
                    subject: craqle::EncodedTerm(subject.clone()),
                    predicate: craqle::EncodedTerm(predicate.clone()),
                    object: craqle::EncodedTerm(object.clone()),
                    dot: craqle::Dot {
                        actor: ActorId::from_bytes(dot.actor),
                        counter: dot.counter,
                    },
                },
                MetadataQuadOp::Remove {
                    subject,
                    predicate,
                    object,
                    witnessed,
                } => craqle::QuadOp::Remove {
                    subject: craqle::EncodedTerm(subject.clone()),
                    predicate: craqle::EncodedTerm(predicate.clone()),
                    object: craqle::EncodedTerm(object.clone()),
                    witnessed: witnessed.clone(),
                },
            })
            .collect(),
        timestamp,
    })
}

/// Plans `source` against the local graph and publishes it as a batch under
/// `actor`, witnessing the graph's clock at plan time.
pub(crate) fn plan_batch(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    graph_iri: &str,
    dot: ([u8; 32], u64),
    source: &MetadataBatchSource,
) -> Result<MetadataBatch, CraqleError> {
    let (actor, counter) = dot;
    let graph = GraphId::new(graph_iri);
    // Planning against a graph this node has not materialized yet would omit
    // the removals the change set needs, so the caller must retry instead.
    if !node.contains_graph(&graph)? {
        return Err(CraqleError::RoCrate(RoCrateError::InvalidGraph(format!(
            "metadata graph `{graph_iri}` is not materialized yet"
        ))));
    }
    let plan = || match source {
        MetadataBatchSource::ReplaceRoCrate { jsonld } => {
            node.plan_rocrate_document_checked(auth, &graph, jsonld)
        }
        MetadataBatchSource::UpsertDataEntity { jsonld } => {
            node.plan_patch_data(auth, &craqle_patch_request(&graph, jsonld)?)
        }
        MetadataBatchSource::UpsertContextualEntity { jsonld } => {
            node.plan_patch_contextual(auth, &craqle_patch_request(&graph, jsonld)?)
        }
        MetadataBatchSource::RemoveDataEntity { id } => {
            let request = RemoveEntityRequest {
                graph: graph.clone(),
                entity_id: id.clone(),
            };
            node.plan_remove_data(auth, &request)
        }
        MetadataBatchSource::ReplaceDataEntity { jsonld } => {
            // Every stored predicate of the subject is replaced, so the ones the entity omits go.
            let mut request = craqle_patch_request(&graph, jsonld)?;
            let subject = DescribeRequest {
                graph: &graph,
                subject_id: &request.entity.entity_id,
            };
            let stored = node.describe_subject(auth, subject)?;
            let stored = stored
                .iter()
                .filter_map(|(predicate, _)| predicate.to_named_node());
            request.replaced_predicates.extend(stored);
            node.plan_patch_data(auth, &request)
        }
    };
    // A merge landing mid-plan would let the batch witness dots the plan never saw, so
    // the plan only counts when the graph clock stayed the same around it.
    let mut attempt = 0;
    let (changes, mut base_clock) = loop {
        attempt += 1;
        let before = node.vector_clock(&graph)?;
        let planned = plan();
        let after = node.vector_clock(&graph)?;
        match planned {
            Ok(changes) if before == after => break (changes, before),
            Err(error)
                if attempt < 8 && error.kind() == craqle::CraqleErrorKind::StalePreparedState => {}
            Err(error) => return Err(error),
            Ok(_) if attempt < 8 => {}
            Ok(_) => {
                return Err(CraqleError::RoCrate(RoCrateError::InvalidGraph(format!(
                    "metadata graph `{graph_iri}` kept changing while planning"
                ))));
            }
        }
    };
    // The clock keeps one counter per actor, so a later dot must never land first.
    if counter > 1 {
        base_clock.advance(ActorId::from_bytes(actor), counter - 1);
    }
    let batch = Batch::from_changes(
        graph,
        ActorId::from_bytes(actor),
        counter,
        base_clock,
        changes,
        chrono::Utc::now(),
    )
    .map_err(|error| CraqleError::RoCrate(RoCrateError::InvalidBatch(error.to_string())))?;
    Ok(batch_from_craqle(batch))
}

pub(crate) fn page_from_craqle(page: craqle::RoCratePage) -> MetadataRoCratePage {
    MetadataRoCratePage {
        jsonld: page.jsonld,
        total_data_entities: page.total_data_entities,
        returned_data_entities: page.returned_data_entities,
        next_offset: page.next_offset,
        next_cursor: page.next_cursor,
    }
}
