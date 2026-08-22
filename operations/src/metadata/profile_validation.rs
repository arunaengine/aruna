use std::collections::{BTreeSet, HashSet};

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_PROFILE_VALIDATION_STATUS_KEYSPACE;
use aruna_core::metadata::{
    MetadataError, MetadataProfileValidationCompleteness, MetadataProfileValidationFinding,
    MetadataProfileValidationSeverity, MetadataProfileValidationState,
    MetadataProfileValidationStatus, is_rocrate_specification,
};
use aruna_core::storage_entries::{
    metadata_profile_validation_status_key, metadata_profile_validation_status_write_entry,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::TxnId;
use chrono::Utc;
use oxrdf::{Dataset, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxttl::{NQuadsParser, TurtleParser};
use spareval::{QueryEvaluator, QueryResults};
use spargebra::SparqlParser;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::raw::load_raw_revision;
use crate::metadata::repository::{
    StorageReadError, parse_registry_read, read_registry_by_document_effect,
};

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const DCTERMS_CONFORMS_TO: &str = "http://purl.org/dc/terms/conformsTo";
const SCHEMA_CONFORMS_TO: &str = "http://schema.org/conformsTo";
const SCHEMA_HTTPS_CONFORMS_TO: &str = "https://schema.org/conformsTo";
const SCHEMA_ABOUT: &str = "http://schema.org/about";
const SCHEMA_HTTPS_ABOUT: &str = "https://schema.org/about";
const SCHEMA_ENCODING_FORMAT: &str = "http://schema.org/encodingFormat";
const SCHEMA_HTTPS_ENCODING_FORMAT: &str = "https://schema.org/encodingFormat";
const SCHEMA_TEXT: &str = "http://schema.org/text";
const SCHEMA_HTTPS_TEXT: &str = "https://schema.org/text";
const DX_PROFILE: &str = "http://www.w3.org/ns/dx/prof/Profile";
const PROFILE_PUBLIC_PREFIX: &str = "https://w3id.org/aruna/profile/";
const LEGACY_GRAPH_PREFIX: &str = "https://w3id.org/aruna/";
const EVALUATOR_NAME: &str = "shacl-core-to-sparql/spareval-0.2.6";
const RDF_LIST_LIMIT: usize = 4096;

/// Authoritative backend SHACL support for profile validation.
///
/// Evaluator decision (P0-1): SHACL Core is compiled to SPARQL and executed by
/// the repository's pinned `spareval` engine. The rudof `shacl_validation`
/// candidate was not present in the offline registry, so it could not be added
/// and built in this sandbox. The exact supported constraint/dispatch terms are
/// `sh:targetClass`, `sh:targetNode`, `sh:property`, predicate-IRI `sh:path`,
/// `sh:minCount`, `sh:maxCount`, `sh:datatype`, `sh:class`, `sh:nodeKind`,
/// `sh:pattern`, `sh:in`, `sh:hasValue`, `sh:closed`,
/// `sh:ignoredProperties`, `sh:severity`, and `sh:deactivated`. Harmless
/// annotations `sh:message`, `sh:name`, `sh:description`, `sh:order`, and
/// `sh:group` are preserved. Every other SHACL term fails closed with an
/// `unsupported_constraint` finding.
pub const SUPPORTED_PROFILE_CONSTRAINTS: &[&str] = &[
    "sh:targetClass",
    "sh:targetNode",
    "sh:property",
    "sh:path (predicate IRI only)",
    "sh:minCount",
    "sh:maxCount",
    "sh:datatype",
    "sh:class",
    "sh:nodeKind",
    "sh:pattern",
    "sh:in",
    "sh:hasValue",
    "sh:closed",
    "sh:ignoredProperties",
    "sh:severity",
    "sh:deactivated",
    "sh:message (annotation)",
    "sh:name (annotation)",
    "sh:description (annotation)",
    "sh:order (annotation)",
    "sh:group (annotation)",
];

const SUPPORTED_SHACL_PREDICATES: &[&str] = &[
    "targetClass",
    "targetNode",
    "property",
    "path",
    "minCount",
    "maxCount",
    "datatype",
    "class",
    "nodeKind",
    "pattern",
    "in",
    "hasValue",
    "closed",
    "ignoredProperties",
    "severity",
    "deactivated",
    "message",
    "name",
    "description",
    "order",
    "group",
];

pub struct ProfileEvaluationRequest<'a> {
    pub data: &'a Dataset,
    pub root: &'a NamedOrBlankNode,
    pub shapes: &'a [&'a str],
    pub profile_revision: Ulid,
}

#[derive(Debug)]
pub enum ProfileEvaluatorError {
    Unsupported(Vec<MetadataProfileValidationFinding>),
    Unavailable(String),
}

/// Stable evaluator seam. Registry resolution and write/status persistence do
/// not depend on a concrete SHACL implementation.
pub trait ProfileConstraintEvaluator: Send + Sync {
    fn name(&self) -> &'static str;
    fn supported_constraints(&self) -> &'static [&'static str];
    fn evaluate(
        &self,
        request: ProfileEvaluationRequest<'_>,
    ) -> Result<Vec<MetadataProfileValidationFinding>, ProfileEvaluatorError>;
}

#[derive(Debug, Default)]
pub struct SparevalShaclEvaluator;

impl ProfileConstraintEvaluator for SparevalShaclEvaluator {
    fn name(&self) -> &'static str {
        EVALUATOR_NAME
    }

    fn supported_constraints(&self) -> &'static [&'static str] {
        SUPPORTED_PROFILE_CONSTRAINTS
    }

    fn evaluate(
        &self,
        request: ProfileEvaluationRequest<'_>,
    ) -> Result<Vec<MetadataProfileValidationFinding>, ProfileEvaluatorError> {
        let mut shapes = Dataset::new();
        for source in request.shapes {
            for triple in TurtleParser::new().for_slice(source.as_bytes()) {
                let triple = triple.map_err(|error| {
                    ProfileEvaluatorError::Unsupported(vec![unsupported_finding(
                        "shapes_turtle",
                        format!("the registered Profile's SHACL could not be parsed: {error}"),
                        request.profile_revision,
                    )])
                })?;
                shapes.insert(&Quad::new(
                    triple.subject,
                    triple.predicate,
                    triple.object,
                    GraphName::DefaultGraph,
                ));
            }
        }
        validate_supported_terms(&shapes, request.profile_revision)?;
        evaluate_shapes(
            request.data,
            &shapes,
            request.root,
            request.profile_revision,
        )
    }
}

#[derive(Debug)]
struct ResolvedProfile {
    id: Ulid,
    requested_iri: String,
    revision: Ulid,
    shapes: Vec<String>,
}

pub fn evaluator_name() -> &'static str {
    EVALUATOR_NAME
}

pub fn profile_public_iri(profile_id: Ulid) -> String {
    format!("{PROFILE_PUBLIC_PREFIX}{profile_id}")
}

pub fn equivalent_profile_iris(iri: &str) -> Vec<String> {
    profile_id_from_iri(iri).map_or_else(
        || vec![iri.to_string()],
        |profile_id| {
            vec![
                profile_public_iri(profile_id),
                MetadataRegistryRecord::graph_iri_for(profile_id),
            ]
        },
    )
}

pub(crate) fn submission_has_profile_tag(jsonld: &str) -> bool {
    data_graph(jsonld)
        .map(|(data, root)| !profile_tags(&data, &root).is_empty())
        .unwrap_or(true)
}

pub async fn validate_submission(
    context: &DriverContext,
    document_id: Ulid,
    jsonld: &str,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let status = assess_submission(context, document_id, jsonld).await?;
    if status.state == MetadataProfileValidationState::Invalid {
        return Err(MetadataError::ProfileValidation(status.findings));
    }
    Ok(status)
}

async fn assess_submission(
    context: &DriverContext,
    document_id: Ulid,
    jsonld: &str,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let (data, root) = data_graph(jsonld)?;
    let profile_tags = profile_tags(&data, &root);
    if profile_tags.is_empty() {
        return Ok(not_profiled_status(document_id));
    }
    if profile_tags.len() != 1 {
        return Err(MetadataError::ProfileValidation(vec![unsupported_finding(
            "multiple_profile_tags",
            "multiple root conformsTo Profile tags are not supported by the revision-bound status contract"
                .to_string(),
            Ulid::nil(),
        )]));
    }
    let requested_iri = profile_tags.into_iter().next().expect("one profile tag");
    let profile = resolve_registered_profile(context, &requested_iri).await?;
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(unavailable_error(
            "validator_unavailable",
            "the profile evaluator is unavailable; retry or remove the Profile tag",
            Some(profile.revision),
        ));
    };
    if !metadata.profile_validation_available() {
        return Err(unavailable_error(
            "validator_unavailable",
            "the profile evaluator is unavailable; retry or remove the Profile tag",
            Some(profile.revision),
        ));
    }
    let evaluator = SparevalShaclEvaluator;
    let shape_refs = profile
        .shapes
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let findings = match evaluator.evaluate(ProfileEvaluationRequest {
        data: &data,
        root: &root,
        shapes: &shape_refs,
        profile_revision: profile.revision,
    }) {
        Ok(findings) | Err(ProfileEvaluatorError::Unsupported(findings)) => findings,
        Err(ProfileEvaluatorError::Unavailable(message)) => {
            return Err(unavailable_error(
                "validator_unavailable",
                &message,
                Some(profile.revision),
            ));
        }
    };
    let invalid = findings
        .iter()
        .any(|finding| finding.severity == MetadataProfileValidationSeverity::Violation);
    let completeness = if findings
        .iter()
        .any(|finding| finding.completeness == MetadataProfileValidationCompleteness::Incomplete)
    {
        MetadataProfileValidationCompleteness::Incomplete
    } else {
        MetadataProfileValidationCompleteness::Complete
    };
    Ok(MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: if invalid {
            MetadataProfileValidationState::Invalid
        } else {
            MetadataProfileValidationState::Valid
        },
        profile_id: Some(profile.id),
        profile_iri: Some(profile.requested_iri),
        profile_revision: Some(profile.revision),
        evaluator: evaluator.name().to_string(),
        validated_at_ms: Some(now_ms()),
        findings,
        completeness,
        stale_reason: None,
    })
}

pub fn not_profiled_status(document_id: Ulid) -> MetadataProfileValidationStatus {
    MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: MetadataProfileValidationState::NotProfiled,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: Some(now_ms()),
        findings: Vec::new(),
        completeness: MetadataProfileValidationCompleteness::Complete,
        stale_reason: None,
    }
}

pub fn stale_status(document_id: Ulid, reason: &str) -> MetadataProfileValidationStatus {
    MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: MetadataProfileValidationState::Stale,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: None,
        findings: Vec::new(),
        completeness: MetadataProfileValidationCompleteness::Incomplete,
        stale_reason: Some(reason.to_string()),
    }
}

pub async fn load_validation_status(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Option<MetadataProfileValidationStatus>, MetadataError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_PROFILE_VALIDATION_STATUS_KEYSPACE.to_string(),
            key: metadata_profile_validation_status_key(document_id),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| {
                postcard::from_bytes(&value)
                    .map_err(aruna_core::errors::ConversionError::from)
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataError::Backend(format!(
            "unexpected profile validation status read: {other:?}"
        ))),
    }
}

pub async fn current_validation_status(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let Some(mut status) = load_validation_status(context, record.document_id, None).await? else {
        return Ok(stale_status(
            record.document_id,
            "validation_status_missing",
        ));
    };
    if status.dataset_revision != record.last_event_id {
        status.state = MetadataProfileValidationState::Stale;
        status.completeness = MetadataProfileValidationCompleteness::Incomplete;
        status.stale_reason = Some("dataset_revision_changed".to_string());
        return Ok(status);
    }
    if let (Some(profile_id), Some(validated_revision)) =
        (status.profile_id, status.profile_revision)
    {
        match read_registry(context, profile_id).await {
            Ok(Some(profile)) if profile.last_event_id == validated_revision => {}
            Ok(Some(_)) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_revision_changed".to_string());
            }
            Ok(None) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_not_registered".to_string());
            }
            Err(_) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_unavailable".to_string());
            }
        }
    }
    Ok(status)
}

pub async fn revalidate_current(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let raw = load_raw_revision(context, record.document_id, None)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?
        .ok_or(MetadataError::GraphNotFound)?;
    if raw.winning_event_id != record.last_event_id {
        return Err(MetadataError::Backend(
            "metadata raw revision changed during profile revalidation".to_string(),
        ));
    }
    let mut status = assess_submission(context, record.document_id, &raw.jsonld).await?;
    status.dataset_revision = raw.winning_event_id;
    let mut owner = context
        .storage_handle
        .start_transaction(false)
        .await
        .map_err(MetadataError::Storage)?;
    let txn_id = owner.id().ok_or_else(|| {
        MetadataError::Backend("profile revalidation transaction is missing".to_string())
    })?;
    let fenced = parse_registry_read(
        context
            .storage_handle
            .send_effect(read_registry_by_document_effect(
                record.document_id,
                Some(txn_id),
            ))
            .await,
    )
    .map_err(map_registry_error)?;
    if fenced.as_ref().map(|record| record.last_event_id) != Some(raw.winning_event_id) {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        owner.finish();
        return Err(MetadataError::Backend(
            "metadata revision changed during profile revalidation; retry".to_string(),
        ));
    }
    let (key_space, key, value) = metadata_profile_validation_status_write_entry(&status)
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(MetadataError::Backend(format!(
                "unexpected profile validation status write: {other:?}"
            )));
        }
    }
    owner.unknown();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            owner.finish();
            Ok(status)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataError::Backend(format!(
            "unexpected profile revalidation commit: {other:?}"
        ))),
    }
}

async fn resolve_registered_profile(
    context: &DriverContext,
    requested_iri: &str,
) -> Result<ResolvedProfile, MetadataError> {
    let Some(profile_id) = profile_id_from_iri(requested_iri) else {
        return Err(profile_not_registered(requested_iri));
    };
    let record = match read_registry(context, profile_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return Err(profile_not_registered(requested_iri)),
        Err(_) => {
            return Err(unavailable_error(
                "profile_unavailable",
                "the registered Profile is temporarily unavailable; retry or remove the Profile tag",
                None,
            ));
        }
    };
    if record.graph_iri != MetadataRegistryRecord::graph_iri_for(profile_id) {
        return Err(profile_not_registered(requested_iri));
    }
    let raw = load_raw_revision(context, profile_id, None)
        .await
        .map_err(|_| {
            unavailable_error(
                "profile_unavailable",
                "the registered Profile revision is temporarily unavailable; retry or remove the Profile tag",
                Some(record.last_event_id),
            )
        })?
        .ok_or_else(|| {
            unavailable_error(
                "profile_unavailable",
                "the registered Profile revision is not materialized; retry or remove the Profile tag",
                Some(record.last_event_id),
            )
        })?;
    if raw.winning_event_id != record.last_event_id {
        return Err(unavailable_error(
            "profile_unavailable",
            "the registered Profile revision is changing; retry or remove the Profile tag",
            Some(record.last_event_id),
        ));
    }
    let (profile_data, profile_root) = data_graph(&raw.jsonld).map_err(|_| {
        unavailable_error(
            "profile_unavailable",
            "the registered Profile cannot be read; retry or remove the Profile tag",
            Some(record.last_event_id),
        )
    })?;
    if !has_type(&profile_data, &profile_root, DX_PROFILE) {
        return Err(profile_not_registered(requested_iri));
    }
    let shapes = profile_shapes(&profile_data).map_err(|message| {
        unavailable_error("profile_unavailable", &message, Some(record.last_event_id))
    })?;
    Ok(ResolvedProfile {
        id: profile_id,
        requested_iri: requested_iri.to_string(),
        revision: record.last_event_id,
        shapes,
    })
}

async fn read_registry(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, MetadataError> {
    parse_registry_read(
        context
            .storage_handle
            .send_effect(read_registry_by_document_effect(document_id, None))
            .await,
    )
    .map_err(map_registry_error)
}

fn map_registry_error(error: StorageReadError) -> MetadataError {
    match error {
        StorageReadError::Storage(error) => error.into(),
        StorageReadError::Conversion(error) => MetadataError::Backend(error.to_string()),
    }
}

fn profile_id_from_iri(iri: &str) -> Option<Ulid> {
    let value = iri
        .strip_prefix(PROFILE_PUBLIC_PREFIX)
        .or_else(|| iri.strip_prefix(LEGACY_GRAPH_PREFIX))?;
    if value.is_empty() || value.contains('/') {
        return None;
    }
    let id = Ulid::from_string(value).ok()?;
    aruna_core::MetaResourceId::from_bytes(id.to_bytes()).ok()?;
    Some(id)
}

fn data_graph(jsonld: &str) -> Result<(Dataset, NamedOrBlankNode), MetadataError> {
    let canonical = craqle::canonicalize_jsonld(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let mut dataset = Dataset::new();
    for quad in NQuadsParser::new().for_slice(canonical.nquads.as_bytes()) {
        let quad = quad.map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
        dataset.insert(&quad);
        if !quad.graph_name.is_default_graph() {
            dataset.insert(&Quad::new(
                quad.subject,
                quad.predicate,
                quad.object,
                GraphName::DefaultGraph,
            ));
        }
    }
    let root = crate_root(&dataset).ok_or_else(|| {
        MetadataError::InvalidInput(
            "RO-Crate descriptor does not identify a root entity".to_string(),
        )
    })?;
    Ok((dataset, root))
}

fn crate_root(dataset: &Dataset) -> Option<NamedOrBlankNode> {
    for predicate in [SCHEMA_ABOUT, SCHEMA_HTTPS_ABOUT] {
        let predicate = NamedNode::new_unchecked(predicate);
        for quad in dataset.quads_for_predicate(&predicate) {
            if !quad.graph_name.is_default_graph() {
                continue;
            }
            if let Some(root) = term_as_node(quad.object.into_owned()) {
                return Some(root);
            }
        }
    }
    None
}

fn profile_tags(dataset: &Dataset, root: &NamedOrBlankNode) -> Vec<String> {
    let mut tags = BTreeSet::new();
    for predicate in [
        DCTERMS_CONFORMS_TO,
        SCHEMA_CONFORMS_TO,
        SCHEMA_HTTPS_CONFORMS_TO,
    ] {
        for object in objects(dataset, root, predicate) {
            if let Term::NamedNode(iri) = object
                && !is_rocrate_specification(iri.as_str())
            {
                tags.insert(iri.as_str().to_string());
            }
        }
    }
    tags.into_iter().collect()
}

fn profile_shapes(dataset: &Dataset) -> Result<Vec<String>, String> {
    let mut candidates = HashSet::new();
    for predicate in [SCHEMA_ENCODING_FORMAT, SCHEMA_HTTPS_ENCODING_FORMAT] {
        let predicate = NamedNode::new_unchecked(predicate);
        for quad in dataset.quads_for_predicate(&predicate) {
            if quad.graph_name.is_default_graph()
                && matches!(quad.object, oxrdf::TermRef::Literal(value) if value.value() == "text/turtle")
            {
                candidates.insert(quad.subject.into_owned());
            }
        }
    }
    let mut shapes = Vec::new();
    for candidate in candidates {
        let mut text = None;
        for predicate in [SCHEMA_TEXT, SCHEMA_HTTPS_TEXT] {
            for object in objects(dataset, &candidate, predicate) {
                if let Term::Literal(value) = object {
                    text = Some(value.value().to_string());
                    break;
                }
            }
        }
        match text {
            Some(text) => shapes.push(text),
            None => {
                return Err(
                    "the registered Profile's SHACL artifact is not locally available; retry or remove the Profile tag"
                        .to_string(),
                );
            }
        }
    }
    Ok(shapes)
}

fn validate_supported_terms(
    shapes: &Dataset,
    profile_revision: Ulid,
) -> Result<(), ProfileEvaluatorError> {
    let mut unsupported = BTreeSet::new();
    for quad in shapes.quads_for_graph_name(&GraphName::DefaultGraph) {
        let predicate = quad.predicate.as_str();
        if let Some(local) = predicate.strip_prefix(SH)
            && !SUPPORTED_SHACL_PREDICATES.contains(&local)
        {
            unsupported.insert(predicate.to_string());
        }
    }
    if unsupported.is_empty() {
        return Ok(());
    }
    Err(ProfileEvaluatorError::Unsupported(
        unsupported
            .into_iter()
            .map(|term| {
                unsupported_finding(
                    &term,
                    format!("the registered Profile uses unsupported SHACL construct `{term}`"),
                    profile_revision,
                )
            })
            .collect(),
    ))
}

fn evaluate_shapes(
    data: &Dataset,
    shapes: &Dataset,
    root: &NamedOrBlankNode,
    profile_revision: Ulid,
) -> Result<Vec<MetadataProfileValidationFinding>, ProfileEvaluatorError> {
    let node_shape_type = Term::NamedNode(NamedNode::new_unchecked(format!("{SH}NodeShape")));
    let property_shape_type =
        Term::NamedNode(NamedNode::new_unchecked(format!("{SH}PropertyShape")));
    let mut node_shapes = subjects(shapes, RDF_TYPE, &node_shape_type);
    let mut property_shapes = subjects(shapes, RDF_TYPE, &property_shape_type);
    for predicate in ["targetClass", "targetNode", "property"] {
        node_shapes.extend(subjects_for_predicate(shapes, &format!("{SH}{predicate}")));
    }
    property_shapes.extend(subjects_for_predicate(shapes, &format!("{SH}path")));
    node_shapes.sort_by_key(node_token);
    node_shapes.dedup();
    property_shapes.sort_by_key(node_token);
    property_shapes.dedup();

    let property_members = node_shapes
        .iter()
        .flat_map(|shape| objects(shapes, shape, &format!("{SH}property")))
        .filter_map(term_as_node)
        .collect::<HashSet<_>>();
    let mut findings = Vec::new();
    for shape in &node_shapes {
        if deactivated(shapes, shape, profile_revision)? {
            continue;
        }
        let bind_root = target_terms(shapes, shape).is_empty()
            && !property_members.contains(shape)
            && objects(shapes, shape, &format!("{SH}class")).is_empty();
        let focuses = resolve_targets(data, shapes, shape, bind_root.then_some(root))?;
        evaluate_shape_constraints(
            data,
            shapes,
            shape,
            &focuses,
            None,
            true,
            profile_revision,
            &mut findings,
        )?;
        for property in objects(shapes, shape, &format!("{SH}property")) {
            let property = term_as_node(property).ok_or_else(|| {
                unsupported_error(
                    "sh:property",
                    "sh:property must identify a property shape",
                    profile_revision,
                )
            })?;
            if deactivated(shapes, &property, profile_revision)? {
                continue;
            }
            let path = predicate_path(shapes, &property, profile_revision)?;
            evaluate_shape_constraints(
                data,
                shapes,
                &property,
                &focuses,
                Some(&path),
                false,
                profile_revision,
                &mut findings,
            )?;
        }
    }
    for shape in property_shapes {
        if property_members.contains(&shape) || deactivated(shapes, &shape, profile_revision)? {
            continue;
        }
        let focuses = resolve_targets(data, shapes, &shape, None)?;
        let path = predicate_path(shapes, &shape, profile_revision)?;
        evaluate_shape_constraints(
            data,
            shapes,
            &shape,
            &focuses,
            Some(&path),
            false,
            profile_revision,
            &mut findings,
        )?;
    }
    findings.sort_by(|left, right| {
        (&left.focus_node, &left.path, &left.rule, &left.message).cmp(&(
            &right.focus_node,
            &right.path,
            &right.rule,
            &right.message,
        ))
    });
    findings.dedup();
    Ok(findings)
}

#[allow(clippy::too_many_arguments)]
fn evaluate_shape_constraints(
    data: &Dataset,
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    focuses: &[Term],
    path: Option<&NamedNode>,
    node_shape: bool,
    profile_revision: Ulid,
    findings: &mut Vec<MetadataProfileValidationFinding>,
) -> Result<(), ProfileEvaluatorError> {
    if focuses.is_empty() {
        return Ok(());
    }
    let severity = severity(shapes, shape, profile_revision)?;
    let message = message(shapes, shape);
    let source = node_value(shape);
    let values = values_clause(focuses);
    let value_pattern = path.map_or_else(
        || "BIND(?focus AS ?value)".to_string(),
        |path| format!("?focus {path} ?value ."),
    );

    for (local, comparison) in [("minCount", "<"), ("maxCount", ">")] {
        let terms = objects(shapes, shape, &format!("{SH}{local}"));
        if terms.is_empty() {
            continue;
        }
        if node_shape || path.is_none() || terms.len() != 1 {
            return Err(unsupported_error(
                &format!("sh:{local}"),
                &format!("sh:{local} requires one predicate-path property shape"),
                profile_revision,
            ));
        }
        let count = unsigned_integer(&terms[0]).ok_or_else(|| {
            unsupported_error(
                &format!("sh:{local}"),
                &format!("sh:{local} must be one non-negative integer"),
                profile_revision,
            )
        })?;
        let query = format!(
            "SELECT ?focus WHERE {{ VALUES ?focus {{ {values} }} OPTIONAL {{ {value_pattern} }} }} GROUP BY ?focus HAVING(COUNT(?value) {comparison} {count})"
        );
        push_query_findings(
            data,
            &query,
            local,
            severity,
            &source,
            path.map(|path| path.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }

    if let Some(datatype) = single_named(shapes, shape, "datatype", profile_revision)? {
        let query = format!(
            "SELECT ?focus ?value WHERE {{ VALUES ?focus {{ {values} }} {value_pattern} FILTER(!isLiteral(?value) || datatype(?value) != {datatype}) }}"
        );
        push_query_findings(
            data,
            &query,
            "datatype",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }
    if let Some(class) = single_named(shapes, shape, "class", profile_revision)? {
        let query = format!(
            "SELECT ?focus ?value WHERE {{ VALUES ?focus {{ {values} }} {value_pattern} FILTER NOT EXISTS {{ ?value <{RDF_TYPE}> {class} }} }}"
        );
        push_query_findings(
            data,
            &query,
            "class",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }
    if let Some(kind) = single_named(shapes, shape, "nodeKind", profile_revision)? {
        let valid = match kind.as_str().strip_prefix(SH) {
            Some("IRI") => "isIRI(?value)",
            Some("BlankNode") => "isBlank(?value)",
            Some("Literal") => "isLiteral(?value)",
            Some("BlankNodeOrIRI") => "(isBlank(?value) || isIRI(?value))",
            Some("BlankNodeOrLiteral") => "(isBlank(?value) || isLiteral(?value))",
            Some("IRIOrLiteral") => "(isIRI(?value) || isLiteral(?value))",
            _ => {
                return Err(unsupported_error(
                    "sh:nodeKind",
                    "sh:nodeKind uses an unsupported node-kind value",
                    profile_revision,
                ));
            }
        };
        let query = format!(
            "SELECT ?focus ?value WHERE {{ VALUES ?focus {{ {values} }} {value_pattern} FILTER(!{valid}) }}"
        );
        push_query_findings(
            data,
            &query,
            "nodeKind",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }
    if let Some(pattern) = single_literal(shapes, shape, "pattern", profile_revision)? {
        let pattern = Literal::new_simple_literal(pattern).to_string();
        let query = format!(
            "SELECT ?focus ?value WHERE {{ VALUES ?focus {{ {values} }} {value_pattern} FILTER(!isLiteral(?value) || !regex(str(?value), {pattern})) }}"
        );
        push_query_findings(
            data,
            &query,
            "pattern",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }
    if let Some(head) = single_term(shapes, shape, "in", profile_revision)? {
        let allowed = rdf_list(shapes, &head, profile_revision)?;
        let filter = if allowed.is_empty() {
            "true".to_string()
        } else {
            format!(
                "?value NOT IN ({})",
                allowed
                    .iter()
                    .map(Term::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let query = format!(
            "SELECT ?focus ?value WHERE {{ VALUES ?focus {{ {values} }} {value_pattern} FILTER({filter}) }}"
        );
        push_query_findings(
            data,
            &query,
            "in",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }
    if let Some(required) = single_term(shapes, shape, "hasValue", profile_revision)? {
        let query = match path {
            Some(path) => format!(
                "SELECT ?focus WHERE {{ VALUES ?focus {{ {values} }} FILTER NOT EXISTS {{ ?focus {path} {required} }} }}"
            ),
            None => format!(
                "SELECT ?focus WHERE {{ VALUES ?focus {{ {values} }} FILTER(!sameTerm(?focus, {required})) }}"
            ),
        };
        push_query_findings(
            data,
            &query,
            "hasValue",
            severity,
            &source,
            path.map(|p| p.as_str()),
            message.as_deref(),
            profile_revision,
            findings,
        )?;
    }

    let closed = optional_boolean(shapes, shape, "closed", profile_revision)?;
    let ignored = single_term(shapes, shape, "ignoredProperties", profile_revision)?;
    if ignored.is_some() && closed != Some(true) {
        return Err(unsupported_error(
            "sh:ignoredProperties",
            "sh:ignoredProperties is supported only with sh:closed true",
            profile_revision,
        ));
    }
    if closed == Some(true) {
        if !node_shape || path.is_some() {
            return Err(unsupported_error(
                "sh:closed",
                "sh:closed is supported on node shapes",
                profile_revision,
            ));
        }
        let mut allowed = objects(shapes, shape, &format!("{SH}property"))
            .into_iter()
            .map(|property| {
                term_as_node(property)
                    .ok_or_else(|| {
                        unsupported_error(
                            "sh:property",
                            "sh:property must identify a property shape",
                            profile_revision,
                        )
                    })
                    .and_then(|property| predicate_path(shapes, &property, profile_revision))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(head) = ignored {
            for term in rdf_list(shapes, &head, profile_revision)? {
                let Term::NamedNode(predicate) = term else {
                    return Err(unsupported_error(
                        "sh:ignoredProperties",
                        "sh:ignoredProperties entries must be predicate IRIs",
                        profile_revision,
                    ));
                };
                allowed.push(predicate);
            }
        }
        allowed.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        allowed.dedup();
        for focus in focuses {
            let Some(node) = term_as_node(focus.clone()) else {
                continue;
            };
            for quad in data.quads_for_subject(&node).filter(|quad| {
                quad.graph_name.is_default_graph()
                    && !allowed
                        .iter()
                        .any(|predicate| predicate.as_ref() == quad.predicate)
            }) {
                findings.push(MetadataProfileValidationFinding {
                    code: "constraint_violation".to_string(),
                    severity,
                    focus_node: Some(term_value(focus)),
                    path: Some(quad.predicate.as_str().to_string()),
                    rule: format!("{SH}closed"),
                    message: message
                        .clone()
                        .unwrap_or_else(|| default_message("closed").to_string()),
                    profile_revision: Some(profile_revision),
                    completeness: MetadataProfileValidationCompleteness::Complete,
                });
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_query_findings(
    data: &Dataset,
    query: &str,
    component: &str,
    severity: MetadataProfileValidationSeverity,
    source: &str,
    fixed_path: Option<&str>,
    message: Option<&str>,
    profile_revision: Ulid,
    findings: &mut Vec<MetadataProfileValidationFinding>,
) -> Result<(), ProfileEvaluatorError> {
    for solution in select(data, query)? {
        let focus = solution.get("focus").map(term_value);
        let path = fixed_path.map(str::to_string).or_else(|| {
            solution.get("unexpectedPath").and_then(|term| match term {
                Term::NamedNode(path) => Some(path.as_str().to_string()),
                _ => None,
            })
        });
        findings.push(MetadataProfileValidationFinding {
            code: "constraint_violation".to_string(),
            severity,
            focus_node: focus,
            path,
            rule: format!("{SH}{component}"),
            message: message.map_or_else(|| default_message(component).to_string(), str::to_string),
            profile_revision: Some(profile_revision),
            completeness: MetadataProfileValidationCompleteness::Complete,
        });
        let _ = source;
    }
    Ok(())
}

fn select(
    data: &Dataset,
    query: &str,
) -> Result<Vec<spareval::QuerySolution>, ProfileEvaluatorError> {
    let query = SparqlParser::new()
        .parse_query(query)
        .map_err(|error| ProfileEvaluatorError::Unavailable(error.to_string()))?;
    let evaluator = QueryEvaluator::new();
    match evaluator
        .prepare(&query)
        .execute(data)
        .map_err(|error| ProfileEvaluatorError::Unavailable(error.to_string()))?
    {
        QueryResults::Solutions(solutions) => solutions
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ProfileEvaluatorError::Unavailable(error.to_string())),
        _ => Err(ProfileEvaluatorError::Unavailable(
            "compiled SHACL query did not return solutions".to_string(),
        )),
    }
}

fn resolve_targets(
    data: &Dataset,
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    root: Option<&NamedOrBlankNode>,
) -> Result<Vec<Term>, ProfileEvaluatorError> {
    let mut focuses = objects(shapes, shape, &format!("{SH}targetNode"));
    for class in objects(shapes, shape, &format!("{SH}targetClass")) {
        let Term::NamedNode(class) = class else {
            return Err(ProfileEvaluatorError::Unavailable(
                "sh:targetClass must be an IRI".to_string(),
            ));
        };
        let predicate = NamedNode::new_unchecked(RDF_TYPE);
        focuses.extend(
            data.quads_for_predicate(&predicate)
                .filter(|quad| {
                    quad.graph_name.is_default_graph()
                        && matches!(quad.object, oxrdf::TermRef::NamedNode(value) if value == class.as_ref())
                })
                .map(|quad| quad.subject.into_owned().into()),
        );
    }
    if let Some(root) = root {
        focuses.push(root.clone().into());
    }
    let mut unique = Vec::with_capacity(focuses.len());
    for focus in focuses {
        if !unique.contains(&focus) {
            unique.push(focus);
        }
    }
    Ok(unique)
}

fn target_terms(shapes: &Dataset, shape: &NamedOrBlankNode) -> Vec<Term> {
    ["targetClass", "targetNode"]
        .into_iter()
        .flat_map(|local| objects(shapes, shape, &format!("{SH}{local}")))
        .collect()
}

fn predicate_path(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    profile_revision: Ulid,
) -> Result<NamedNode, ProfileEvaluatorError> {
    let paths = objects(shapes, shape, &format!("{SH}path"));
    match paths.as_slice() {
        [Term::NamedNode(path)] => Ok(path.clone()),
        _ => Err(unsupported_error(
            "sh:path",
            "each property shape must have exactly one predicate-IRI sh:path",
            profile_revision,
        )),
    }
}

fn severity(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    profile_revision: Ulid,
) -> Result<MetadataProfileValidationSeverity, ProfileEvaluatorError> {
    let values = objects(shapes, shape, &format!("{SH}severity"));
    match values.as_slice() {
        [] => Ok(MetadataProfileValidationSeverity::Violation),
        [Term::NamedNode(value)] if value.as_str() == format!("{SH}Violation") => {
            Ok(MetadataProfileValidationSeverity::Violation)
        }
        [Term::NamedNode(value)] if value.as_str() == format!("{SH}Warning") => {
            Ok(MetadataProfileValidationSeverity::Warning)
        }
        [Term::NamedNode(value)] if value.as_str() == format!("{SH}Info") => {
            Ok(MetadataProfileValidationSeverity::Info)
        }
        _ => Err(unsupported_error(
            "sh:severity",
            "sh:severity must be one of sh:Violation, sh:Warning, or sh:Info",
            profile_revision,
        )),
    }
}

fn deactivated(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    profile_revision: Ulid,
) -> Result<bool, ProfileEvaluatorError> {
    Ok(optional_boolean(shapes, shape, "deactivated", profile_revision)?.unwrap_or(false))
}

fn optional_boolean(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    local: &str,
    profile_revision: Ulid,
) -> Result<Option<bool>, ProfileEvaluatorError> {
    let values = objects(shapes, shape, &format!("{SH}{local}"));
    match values.as_slice() {
        [] => Ok(None),
        [Term::Literal(value)] if matches!(value.value(), "true" | "1") => Ok(Some(true)),
        [Term::Literal(value)] if matches!(value.value(), "false" | "0") => Ok(Some(false)),
        _ => Err(unsupported_error(
            &format!("sh:{local}"),
            &format!("sh:{local} must be one boolean"),
            profile_revision,
        )),
    }
}

fn single_named(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    local: &str,
    profile_revision: Ulid,
) -> Result<Option<NamedNode>, ProfileEvaluatorError> {
    match objects(shapes, shape, &format!("{SH}{local}")).as_slice() {
        [] => Ok(None),
        [Term::NamedNode(value)] => Ok(Some(value.clone())),
        _ => Err(unsupported_error(
            &format!("sh:{local}"),
            &format!("sh:{local} must have exactly one IRI value"),
            profile_revision,
        )),
    }
}

fn single_literal(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    local: &str,
    profile_revision: Ulid,
) -> Result<Option<String>, ProfileEvaluatorError> {
    match objects(shapes, shape, &format!("{SH}{local}")).as_slice() {
        [] => Ok(None),
        [Term::Literal(value)] => Ok(Some(value.value().to_string())),
        _ => Err(unsupported_error(
            &format!("sh:{local}"),
            &format!("sh:{local} must have exactly one literal value"),
            profile_revision,
        )),
    }
}

fn single_term(
    shapes: &Dataset,
    shape: &NamedOrBlankNode,
    local: &str,
    profile_revision: Ulid,
) -> Result<Option<Term>, ProfileEvaluatorError> {
    match objects(shapes, shape, &format!("{SH}{local}")).as_slice() {
        [] => Ok(None),
        [value] => Ok(Some(value.clone())),
        _ => Err(unsupported_error(
            &format!("sh:{local}"),
            &format!("sh:{local} must have exactly one value"),
            profile_revision,
        )),
    }
}

fn message(shapes: &Dataset, shape: &NamedOrBlankNode) -> Option<String> {
    objects(shapes, shape, &format!("{SH}message"))
        .into_iter()
        .find_map(|term| match term {
            Term::Literal(value) => Some(value.value().to_string()),
            _ => None,
        })
}

fn rdf_list(
    shapes: &Dataset,
    head: &Term,
    profile_revision: Ulid,
) -> Result<Vec<Term>, ProfileEvaluatorError> {
    let mut cursor = head.clone();
    let mut values = Vec::new();
    let mut visited = HashSet::new();
    loop {
        if matches!(&cursor, Term::NamedNode(node) if node.as_str() == RDF_NIL) {
            return Ok(values);
        }
        let node = term_as_node(cursor.clone()).ok_or_else(|| {
            unsupported_error(
                "rdf:list",
                "SHACL list value is not an RDF list",
                profile_revision,
            )
        })?;
        if !visited.insert(node.clone()) || visited.len() > RDF_LIST_LIMIT {
            return Err(unsupported_error(
                "rdf:list",
                "SHACL list is cyclic or exceeds the supported bound",
                profile_revision,
            ));
        }
        let first = objects(shapes, &node, RDF_FIRST);
        let rest = objects(shapes, &node, RDF_REST);
        let ([first], [rest]) = (first.as_slice(), rest.as_slice()) else {
            return Err(unsupported_error(
                "rdf:list",
                "SHACL list is malformed",
                profile_revision,
            ));
        };
        values.push(first.clone());
        cursor = rest.clone();
    }
}

fn objects(dataset: &Dataset, subject: &NamedOrBlankNode, predicate: &str) -> Vec<Term> {
    let predicate = NamedNode::new_unchecked(predicate);
    dataset
        .quads_for_subject(subject)
        .filter(|quad| quad.graph_name.is_default_graph() && quad.predicate == predicate.as_ref())
        .map(|quad| quad.object.into_owned())
        .collect()
}

fn subjects(dataset: &Dataset, predicate: &str, object: &Term) -> Vec<NamedOrBlankNode> {
    let predicate = NamedNode::new_unchecked(predicate);
    dataset
        .quads_for_predicate(&predicate)
        .filter(|quad| quad.graph_name.is_default_graph() && quad.object == object.as_ref())
        .map(|quad| quad.subject.into_owned())
        .collect()
}

fn subjects_for_predicate(dataset: &Dataset, predicate: &str) -> Vec<NamedOrBlankNode> {
    let predicate = NamedNode::new_unchecked(predicate);
    dataset
        .quads_for_predicate(&predicate)
        .filter(|quad| quad.graph_name.is_default_graph())
        .map(|quad| quad.subject.into_owned())
        .collect()
}

fn has_type(dataset: &Dataset, subject: &NamedOrBlankNode, class: &str) -> bool {
    objects(dataset, subject, RDF_TYPE)
        .iter()
        .any(|term| matches!(term, Term::NamedNode(value) if value.as_str() == class))
}

fn term_as_node(term: Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node)),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node)),
        _ => None,
    }
}

fn unsigned_integer(term: &Term) -> Option<u64> {
    match term {
        Term::Literal(value) => value.value().parse().ok(),
        _ => None,
    }
}

fn values_clause(focuses: &[Term]) -> String {
    focuses
        .iter()
        .map(Term::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

fn node_token(node: &NamedOrBlankNode) -> String {
    node.to_string()
}

fn node_value(node: &NamedOrBlankNode) -> String {
    match node {
        NamedOrBlankNode::NamedNode(node) => node.as_str().to_string(),
        NamedOrBlankNode::BlankNode(node) => node.to_string(),
    }
}

fn term_value(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => node.as_str().to_string(),
        Term::BlankNode(node) => node.to_string(),
        Term::Literal(value) => value.to_string(),
        Term::Triple(triple) => triple.to_string(),
    }
}

fn default_message(component: &str) -> &'static str {
    match component {
        "minCount" => "fewer values are present than the Profile requires",
        "maxCount" => "more values are present than the Profile allows",
        "datatype" => "a value has the wrong RDF datatype",
        "class" => "a value is not an instance of the required class",
        "nodeKind" => "a value has the wrong RDF node kind",
        "pattern" => "a value does not match the required pattern",
        "in" => "a value is outside the allowed set",
        "hasValue" => "the required value is missing",
        "closed" => "a closed shape contains a property that is not allowed",
        _ => "the submitted crate does not satisfy the Profile constraint",
    }
}

fn unsupported_error(rule: &str, message: &str, profile_revision: Ulid) -> ProfileEvaluatorError {
    ProfileEvaluatorError::Unsupported(vec![unsupported_finding(
        rule,
        message.to_string(),
        profile_revision,
    )])
}

fn unsupported_finding(
    rule: &str,
    message: String,
    profile_revision: Ulid,
) -> MetadataProfileValidationFinding {
    MetadataProfileValidationFinding {
        code: "unsupported_constraint".to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: rule.to_string(),
        message,
        profile_revision: (profile_revision != Ulid::nil()).then_some(profile_revision),
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }
}

fn profile_not_registered(iri: &str) -> MetadataError {
    MetadataError::ProfileValidation(vec![MetadataProfileValidationFinding {
        code: "profile_not_registered".to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: DCTERMS_CONFORMS_TO.to_string(),
        message: format!("Profile `{iri}` is not registered; remove the Profile tag before saving"),
        profile_revision: None,
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }])
}

fn unavailable_error(code: &str, message: &str, revision: Option<Ulid>) -> MetadataError {
    MetadataError::ProfileValidation(vec![MetadataProfileValidationFinding {
        code: code.to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: code.to_string(),
        message: message.to_string(),
        profile_revision: revision,
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }])
}

fn now_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::{BucketId, MetaResourceId, PlacementHandle, StructuredId};

    const DATA: &str = r#"{
      "@context": "https://w3id.org/ro/crate/1.2/context",
      "@graph": [
        {"@id":"ro-crate-metadata.json","@type":"CreativeWork","conformsTo":{"@id":"https://w3id.org/ro/crate/1.2"},"about":{"@id":"https://example.test/dataset"}},
        {"@id":"https://example.test/dataset","@type":"Dataset","name":"Example","description":"allowed"}
      ]
    }"#;

    fn evaluate(
        shapes: &str,
    ) -> Result<Vec<MetadataProfileValidationFinding>, ProfileEvaluatorError> {
        let (data, root) = data_graph(DATA).unwrap();
        SparevalShaclEvaluator.evaluate(ProfileEvaluationRequest {
            data: &data,
            root: &root,
            shapes: &[shapes],
            profile_revision: Ulid::from_parts(1, 1),
        })
    }

    #[test]
    fn enforces_supported_core_constraints_and_open_world_default() {
        let findings = evaluate(r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            <urn:shape> a sh:NodeShape ;
              sh:targetClass schema:Dataset ;
              sh:property [ sh:path schema:name ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string ] .
        "#).unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn enforces_value_class_kind_pattern_set_and_required_value() {
        let findings = evaluate(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            <urn:shape> a sh:NodeShape ;
              sh:targetNode <https://example.test/dataset> ;
              sh:class schema:Dataset ;
              sh:property [
                sh:path schema:name ;
                sh:nodeKind sh:Literal ;
                sh:pattern "^Ex" ;
                sh:in ( "Example" "Alternative" ) ;
                sh:hasValue "Example"
              ] .
        "#,
        )
        .unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn preserves_non_violation_severity_in_findings() {
        let findings = evaluate(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            <urn:shape> a sh:NodeShape ;
              sh:targetNode <https://example.test/dataset> ;
              sh:property [ sh:path schema:missing ; sh:minCount 1 ; sh:severity sh:Warning ] .
        "#,
        )
        .unwrap();
        assert_eq!(findings.len(), 1);
        assert_eq!(
            findings[0].severity,
            MetadataProfileValidationSeverity::Warning
        );
    }

    #[test]
    fn closed_shape_rejects_extra_data() {
        let source = r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            <urn:shape> a sh:NodeShape ;
              sh:targetClass schema:Dataset ; sh:closed true ; sh:ignoredProperties ( rdf:type ) ;
              sh:property [ sh:path schema:name ] .
        "#;
        let findings = evaluate(source).unwrap();
        assert!(
            findings.iter().any(|finding| {
                finding.rule == format!("{SH}closed")
                    && finding
                        .path
                        .as_deref()
                        .is_some_and(|path| path.ends_with("/description"))
            }),
            "{findings:#?}"
        );
    }

    #[test]
    fn unsupported_constraint_fails_closed() {
        let error = evaluate(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            <urn:shape> a sh:NodeShape ; sh:minLength 2 .
        "#,
        )
        .unwrap_err();
        let ProfileEvaluatorError::Unsupported(findings) = error else {
            panic!("expected unsupported finding");
        };
        assert_eq!(findings[0].code, "unsupported_constraint");
        assert_eq!(findings[0].rule, "http://www.w3.org/ns/shacl#minLength");
    }

    #[test]
    fn deactivated_shape_does_not_run() {
        let findings = evaluate(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix schema: <http://schema.org/> .
            <urn:shape> a sh:NodeShape ; sh:targetNode <https://example.test/dataset> ;
              sh:deactivated true ; sh:property [ sh:path schema:missing ; sh:minCount 1 ] .
        "#,
        )
        .unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn canonical_and_legacy_profile_iris_parse_to_same_id() {
        let id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(1).unwrap(),
            BucketId::new(1).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        assert_eq!(profile_id_from_iri(&profile_public_iri(id)), Some(id));
        assert_eq!(
            profile_id_from_iri(&MetadataRegistryRecord::graph_iri_for(id)),
            Some(id)
        );
    }

    #[test]
    fn spec_tags_ignored() {
        for version in ["1.2", "1.3"] {
            let specification = format!("https://w3id.org/ro/crate/{version}");
            let document = serde_json::json!({
                "@context": format!("{specification}/context"),
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": {"@id": specification},
                        "about": {"@id": "https://example.test/dataset"}
                    },
                    {
                        "@id": "https://example.test/dataset",
                        "@type": "Dataset",
                        "name": "Versioned crate",
                        "description": "Specification IRIs are not Profiles",
                        "datePublished": "2026-08-19",
                        "conformsTo": {"@id": specification}
                    }
                ]
            })
            .to_string();
            let (data, root) = data_graph(&document).unwrap();
            assert!(profile_tags(&data, &root).is_empty());
        }
    }
}
