use aruna_core::NodeId;
use aruna_core::metadata::{
    MetadataBatch, MetadataBatchSource, MetadataCreateCrateRequest, MetadataDot, MetadataError,
    MetadataGraphPolicy, MetadataQuadOp, MetadataRequestDurability, MetadataRoCratePage,
    MetadataSearchHit, MetadataUpsertEntityRequest, MetadataValidationViolation,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_storage::FjallPersistPolicy;
use craqle::{
    ActorId, AllowAllAuthorizer, Batch, CraqleError, CraqleFjallPersistMode, CraqleNode,
    CraqleRequestDurability, CreateCrateRequest, CreateEntityRequest, GraphId, GraphPolicy,
    PatchEntityRequest, RoCrateError, vocab,
};
use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde_json::Value;

use crate::metadata::search_enrichment::{hit_snippet, hit_title, hit_types};

pub(super) fn upsert_data_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(&request.graph_iri);
    let actor = request.deterministic_actor.map(ActorId::from_bytes);
    let entity_request = craqle_patch_request(&graph, &request.jsonld)?;
    node.patch_data_with(
        auth,
        entity_request,
        craqle_request_durability(request.durability),
        actor,
    )
    .map(batch_from_craqle)
}

pub(super) fn upsert_contextual_entity(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    request: MetadataUpsertEntityRequest,
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

pub(super) fn craqle_patch_request(
    graph: &GraphId,
    jsonld: &str,
) -> Result<PatchEntityRequest, CraqleError> {
    let value: Value = serde_json::from_str(jsonld).map_err(|error| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(error.to_string()))
    })?;
    let object = value.as_object().ok_or_else(|| {
        CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must be a JSON object".to_string(),
        ))
    })?;
    if object.contains_key("@graph") || object.contains_key("graph") {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity payload must not contain `@graph`; send a single JSON-LD entity object"
                .to_string(),
        )));
    }

    let entity_id = entity_identifier(object)?;
    let mut entity_types = entity_types(object)?;
    let entity_type = entity_types.remove(0);
    let name = entity_name(object)?;
    let mut additional_triples = Vec::new();
    let mut replaced_predicates = Vec::new();
    for extra_type in entity_types {
        additional_triples.push((vocab::rdf_type(), class_term(&extra_type)?));
    }

    for (property, property_value) in object {
        if matches!(
            property.as_str(),
            "@context" | "@id" | "id" | "@type" | "type" | "name"
        ) {
            continue;
        }
        let property = normalize_property(property);
        let predicate = property_named_node(&property)?;
        replaced_predicates.push(predicate.clone());
        for object in property_value_terms(&property, property_value)? {
            additional_triples.push((predicate.clone(), object));
        }
    }

    Ok(PatchEntityRequest {
        entity: CreateEntityRequest {
            graph: graph.clone(),
            entity_id,
            entity_type,
            name,
            additional_triples,
        },
        replaced_predicates,
    })
}

fn entity_identifier(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("@id")
        .or_else(|| object.get("id"))
        .and_then(Value::as_str)
        .map(normalize_entity_id)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `@id`".to_string(),
            ))
        })
}

fn entity_types(object: &serde_json::Map<String, Value>) -> Result<Vec<String>, CraqleError> {
    let value = object
        .get("@type")
        .or_else(|| object.get("type"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define `@type`".to_string(),
            ))
        })?;
    let mut types = Vec::new();
    match value {
        Value::String(value) => types.push(value.clone()),
        Value::Array(values) => {
            for value in values {
                let Some(value) = value.as_str() else {
                    return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                        "entity `@type` arrays must contain only strings".to_string(),
                    )));
                };
                types.push(value.to_string());
            }
        }
        _ => {
            return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity `@type` must be a string or array of strings".to_string(),
            )));
        }
    }
    if types.is_empty() {
        return Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            "entity `@type` must not be empty".to_string(),
        )));
    }
    Ok(types)
}

fn entity_name(object: &serde_json::Map<String, Value>) -> Result<String, CraqleError> {
    object
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "entity payload must define string `name`".to_string(),
            ))
        })
}

fn property_named_node(property: &str) -> Result<NamedNode, CraqleError> {
    match property {
        "@type" | "type" => Ok(vocab::rdf_type()),
        "name" => Ok(vocab::schema_name()),
        "description" => Ok(vocab::schema_description()),
        "keywords" => Ok(vocab::schema_keywords()),
        "datePublished" => Ok(vocab::schema_date_published()),
        "license" => Ok(vocab::schema_license()),
        "about" => Ok(vocab::schema_about()),
        "conformsTo" => Ok(NamedNode::new_unchecked(
            super::super::iri_index::DCTERMS_CONFORMS_TO_IRI,
        )),
        other if other.contains("://") => Ok(NamedNode::new_unchecked(other)),
        other if other.contains(':') => expand_compact_iri(other),
        other => Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{}",
            normalize_term(other)
        ))),
    }
}

fn property_value_terms(property: &str, value: &Value) -> Result<Vec<Term>, CraqleError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Bool(boolean) => Ok(vec![Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
        ))]),
        Value::Number(number) => Ok(vec![number_literal(number)]),
        Value::String(text) => {
            let mapped = normalize_entity_id(text);
            let value = if property_expects_identifier(property) {
                mapped.as_str()
            } else {
                text
            };
            Ok(vec![property_value_term(property, value)?])
        }
        Value::Array(values) => {
            let mut objects = Vec::new();
            for entry in values {
                objects.extend(property_value_terms(property, entry)?);
            }
            Ok(objects)
        }
        Value::Object(object) if is_reference_object(object) => {
            let id = object
                .get("@id")
                .or_else(|| object.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(format!(
                        "property `{property}` reference object is missing string `@id`"
                    )))
                })?;
            Ok(vec![reference_term(&normalize_entity_id(id))?])
        }
        Value::Object(object) if is_value_object(object) => Ok(vec![value_object_term(object)?]),
        Value::Object(_) => Err(CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
            format!(
                "property `{property}` contains an inline nested object; nested entities must be separate top-level entities referenced by `@id`"
            ),
        ))),
    }
}

fn property_value_term(property: &str, value: &str) -> Result<Term, CraqleError> {
    match property {
        "@type" | "type" => class_term(value),
        "license" | "about" | "conformsTo" => {
            if looks_like_identifier(value) {
                reference_term(value)
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(value)))
            }
        }
        _ => Ok(Term::Literal(Literal::new_simple_literal(value))),
    }
}

fn class_term(value: &str) -> Result<Term, CraqleError> {
    let iri = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else if value.contains(':') {
        expand_compact_iri(value)?.as_str().to_string()
    } else {
        format!("http://schema.org/{}", normalize_term(value))
    };
    Ok(Term::NamedNode(NamedNode::new_unchecked(iri)))
}

fn reference_term(value: &str) -> Result<Term, CraqleError> {
    if let Some(value) = value.strip_prefix("_:") {
        Ok(Term::BlankNode(BlankNode::new_unchecked(value)))
    } else if value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.contains("://")
    {
        Ok(Term::NamedNode(NamedNode::new_unchecked(value)))
    } else if value.contains(':') {
        Ok(Term::NamedNode(expand_compact_iri(value)?))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn number_literal(number: &serde_json::Number) -> Term {
    let datatype = if number.as_i64().is_some() || number.as_u64().is_some() {
        "http://www.w3.org/2001/XMLSchema#integer"
    } else {
        "http://www.w3.org/2001/XMLSchema#double"
    };
    Term::Literal(Literal::new_typed_literal(
        number.to_string(),
        NamedNode::new_unchecked(datatype),
    ))
}

fn value_object_term(object: &serde_json::Map<String, Value>) -> Result<Term, CraqleError> {
    let value = object
        .get("@value")
        .or_else(|| object.get("value"))
        .ok_or_else(|| {
            CraqleError::RoCrate(RoCrateError::UnsupportedJsonLd(
                "value object missing `@value`".to_string(),
            ))
        })?;
    let language = object
        .get("@language")
        .or_else(|| object.get("language"))
        .and_then(Value::as_str);
    let datatype = object
        .get("@type")
        .or_else(|| object.get("type"))
        .and_then(Value::as_str);

    match value {
        Value::String(text) => {
            if let Some(language) = language {
                Ok(Term::Literal(
                    Literal::new_language_tagged_literal_unchecked(text, language),
                ))
            } else if let Some(datatype) = datatype {
                Ok(Term::Literal(Literal::new_typed_literal(
                    text.clone(),
                    datatype_named_node(datatype)?,
                )))
            } else {
                Ok(Term::Literal(Literal::new_simple_literal(text)))
            }
        }
        Value::Bool(boolean) => Ok(Term::Literal(Literal::new_typed_literal(
            boolean.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
                }),
        ))),
        Value::Number(number) => Ok(Term::Literal(Literal::new_typed_literal(
            number.to_string(),
            datatype
                .map(datatype_named_node)
                .transpose()?
                .unwrap_or_else(|| {
                    if number.as_i64().is_some() || number.as_u64().is_some() {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer")
                    } else {
                        NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#double")
                    }
                }),
        ))),
        Value::Null => Ok(Term::Literal(Literal::new_simple_literal(""))),
        Value::Array(_) | Value::Object(_) => Err(CraqleError::RoCrate(
            RoCrateError::UnsupportedJsonLd("value object `@value` must be scalar".to_string()),
        )),
    }
}

fn datatype_named_node(datatype: &str) -> Result<NamedNode, CraqleError> {
    if datatype.starts_with("http://") || datatype.starts_with("https://") {
        Ok(NamedNode::new_unchecked(datatype))
    } else {
        expand_compact_iri(datatype)
    }
}

fn expand_compact_iri(value: &str) -> Result<NamedNode, CraqleError> {
    if let Some(local) = value.strip_prefix("schema:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://schema.org/{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdf:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}"
        )))
    } else if let Some(local) = value.strip_prefix("rdfs:") {
        Ok(NamedNode::new_unchecked(format!(
            "http://www.w3.org/2000/01/rdf-schema#{local}"
        )))
    } else {
        Err(CraqleError::RoCrate(RoCrateError::UnsupportedTerm(
            value.to_string(),
        )))
    }
}

fn normalize_property(property: &str) -> String {
    property
        .strip_prefix("schema:")
        .or_else(|| property.strip_prefix("http://schema.org/"))
        .or_else(|| property.strip_prefix("https://schema.org/"))
        .map(str::to_string)
        .unwrap_or_else(|| property.to_string())
}

fn normalize_term(term: &str) -> String {
    normalize_property(term)
}

fn normalize_entity_id(id: &str) -> String {
    if id == "ro-crate-metadata.json"
        || id.starts_with("./")
        || id.starts_with("../")
        || id.starts_with('#')
        || id.starts_with("_:")
        || id.contains("://")
        || (id.contains(':') && !id.contains('/'))
    {
        id.to_string()
    } else {
        format!("./{id}")
    }
}

fn property_expects_identifier(property: &str) -> bool {
    matches!(property, "license" | "about" | "conformsTo")
}

fn is_reference_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_identifier = object.contains_key("@id") || object.contains_key("id");
    has_identifier
        && object
            .keys()
            .all(|key| matches!(key.as_str(), "@id" | "id" | "@type" | "type"))
}

fn is_value_object(object: &serde_json::Map<String, Value>) -> bool {
    let has_value = object.contains_key("@value") || object.contains_key("value");
    has_value
        && object.keys().all(|key| {
            matches!(
                key.as_str(),
                "@value" | "value" | "@type" | "type" | "@language" | "language"
            )
        })
}

fn looks_like_identifier(value: &str) -> bool {
    value.starts_with("./")
        || value.starts_with("../")
        || value.starts_with('#')
        || value.starts_with("_:")
        || value.contains("://")
        || (value.contains(':') && !value.contains(' '))
}

pub(super) fn error_from_craqle(error: CraqleError) -> MetadataError {
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
        other => MetadataError::Backend(other.to_string()),
    }
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

pub(super) fn craqle_create_request(request: MetadataCreateCrateRequest) -> CreateCrateRequest {
    CreateCrateRequest::new(
        GraphId::new(&request.graph_iri),
        request.name,
        request.description,
        request.date_published,
        request.license,
        craqle_graph_policy(request.policy),
    )
}

pub(super) fn craqle_request_durability(
    durability: MetadataRequestDurability,
) -> CraqleRequestDurability {
    match durability {
        MetadataRequestDurability::Durable => CraqleRequestDurability::Durable,
        MetadataRequestDurability::WalAlreadyDurable => CraqleRequestDurability::WalAlreadyDurable,
    }
}

pub(super) fn fjall_persist_mode(policy: FjallPersistPolicy) -> CraqleFjallPersistMode {
    match policy {
        FjallPersistPolicy::Buffer => CraqleFjallPersistMode::Buffer,
        FjallPersistPolicy::SyncAll => CraqleFjallPersistMode::SyncAll,
    }
}

pub(super) fn craqle_graph_policy(policy: MetadataGraphPolicy) -> GraphPolicy {
    GraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(super) fn irokle_peer_id(node_id: NodeId) -> irokle::PeerId {
    irokle::PeerId::from_bytes(*node_id.as_bytes())
}

pub(super) fn policy_from_craqle(policy: GraphPolicy) -> MetadataGraphPolicy {
    MetadataGraphPolicy {
        public: policy.public,
        permission_paths: policy.permission_paths,
    }
}

pub(super) fn dot_from_craqle(dot: craqle::Dot) -> MetadataDot {
    MetadataDot {
        actor: *dot.actor.as_bytes(),
        counter: dot.counter,
    }
}

pub(super) fn batch_from_craqle(batch: Batch) -> MetadataBatch {
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

pub(super) fn to_craqle_batch(batch: &MetadataBatch) -> Result<Batch, CraqleError> {
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
pub(super) fn plan_batch(
    node: &CraqleNode,
    auth: &AllowAllAuthorizer,
    graph_iri: &str,
    actor: [u8; 32],
    source: &MetadataBatchSource,
) -> Result<MetadataBatch, CraqleError> {
    let graph = GraphId::new(graph_iri);
    // Planning against a graph this node has not materialized yet would omit
    // the removals the change set needs, so the caller must retry instead.
    if !node.contains_graph(&graph)? {
        return Err(CraqleError::RoCrate(RoCrateError::InvalidGraph(format!(
            "metadata graph `{graph_iri}` is not materialized yet"
        ))));
    }
    let changes = match source {
        MetadataBatchSource::ReplaceRoCrate { jsonld } => {
            node.plan_rocrate_document_checked(auth, &graph, jsonld)?
        }
        MetadataBatchSource::UpsertDataEntity { jsonld } => {
            node.plan_patch_data(auth, &craqle_patch_request(&graph, jsonld)?)?
        }
        MetadataBatchSource::UpsertContextualEntity { jsonld } => {
            node.plan_patch_contextual(auth, &craqle_patch_request(&graph, jsonld)?)?
        }
    };
    let base_clock = node.vector_clock(&graph)?;
    let batch = Batch::from_changes(
        graph,
        ActorId::from_bytes(actor),
        1,
        base_clock,
        changes,
        chrono::Utc::now(),
    )
    .map_err(|error| CraqleError::RoCrate(RoCrateError::InvalidBatch(error.to_string())))?;
    Ok(batch_from_craqle(batch))
}

pub(super) fn page_from_craqle(page: craqle::RoCratePage) -> MetadataRoCratePage {
    MetadataRoCratePage {
        jsonld: page.jsonld,
        total_data_entities: page.total_data_entities,
        returned_data_entities: page.returned_data_entities,
        next_offset: page.next_offset,
        next_cursor: page.next_cursor,
    }
}

pub(super) fn hit_from_craqle(
    hit: craqle::SearchHit,
    record: &MetadataRegistryRecord,
    properties: &[(String, Term)],
    query: &str,
) -> MetadataSearchHit {
    let title = hit_title(properties, &record.document_path, &hit.subject_iri);
    let snippet = hit_snippet(properties, query);
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: hit.graph_id,
        subject_iri: hit.subject_iri,
        score: hit.score,
        title,
        snippet,
        subject_types: hit_types(properties),
    }
}

pub(super) fn decode_hit_properties(
    properties: Vec<(craqle::EncodedTerm, craqle::EncodedTerm)>,
) -> Vec<(String, Term)> {
    properties
        .into_iter()
        .filter_map(|(predicate, object)| {
            let Some(Term::NamedNode(predicate)) = predicate.to_term() else {
                return None;
            };
            let object = object.to_term()?;
            Some((predicate.as_str().to_string(), object))
        })
        .collect()
}
