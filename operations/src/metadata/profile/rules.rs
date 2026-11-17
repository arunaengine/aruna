//! Pure Profile rules: whether a crate or batch is or tags a Profile, where Profiles may live,
//! and the statuses of unprofiled and stale documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::forward::read::PROFILE_DOCUMENT_PREFIX;
use aruna_core::metadata::MetadataBatch;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::MetadataQuadOp;
use aruna_core::metadata::ProfileValidationCompleteness;
use aruna_core::metadata::ProfileValidationState;
use aruna_core::metadata::ProfileValidationStatus;
use aruna_core::profile_shapes::CRAQLE_BASE_IRI;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::time::unix_timestamp_millis as now_ms;
use oxrdf::Dataset;
use oxrdf::GraphName;
use oxrdf::NamedNode;
use oxrdf::NamedOrBlankNode;
use oxrdf::NamedOrBlankNodeRef;
use oxrdf::Quad;
use oxrdf::Term;
use oxttl::NQuadsParser;
use std::collections::BTreeSet;
use ulid::Ulid;

pub(super) const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

pub(super) const DCTERMS_CONFORMS_TO: &str = "http://purl.org/dc/terms/conformsTo";

pub(super) const SCHEMA_CONFORMS_TO: &str = "http://schema.org/conformsTo";

pub(super) const SCHEMA_HTTPS_CONFORMS: &str = "https://schema.org/conformsTo";

pub(super) const SCHEMA_ABOUT: &str = "http://schema.org/about";

pub(super) const SCHEMA_HTTPS_ABOUT: &str = "https://schema.org/about";

pub(super) const DX_PROFILE: &str = "http://www.w3.org/ns/dx/prof/Profile";

pub(super) const EVALUATOR_NAME: &str = "craqle-shacl-core/0.3";

pub(crate) fn submission_profile_tag(jsonld: &str) -> bool {
    data_graph(jsonld)
        .map(|(data, root)| !profile_tags(&data, &root).is_empty())
        .unwrap_or(true)
}

/// Refuses a Profile root outside `profiles/`, the only path clients list as Profiles.
pub(crate) fn profile_path_rule(
    document_path: &str,
    profile: impl FnOnce() -> bool,
) -> Result<(), MetadataError> {
    let path = MetadataRegistryRecord::normalize_document_path(document_path);
    if path.starts_with(PROFILE_DOCUMENT_PREFIX) || !profile() {
        return Ok(());
    }
    Err(MetadataError::InvalidInput(
        "Profiles must be stored under profiles/; use a document path that starts with profiles/"
            .to_string(),
    ))
}

pub(crate) fn crate_is_profile(jsonld: &str) -> bool {
    data_graph(jsonld).is_ok_and(|(data, root)| has_type(&data, &root, DX_PROFILE))
}

pub(crate) fn batch_adds_profile(batch: &MetadataBatch) -> bool {
    let (root, rdf_type, profile) = (
        format!("<{}>", batch.graph_iri),
        format!("<{RDF_TYPE}>"),
        format!("<{DX_PROFILE}>"),
    );
    batch.ops.iter().any(|op| {
        matches!(op, MetadataQuadOp::Add { subject, predicate, object, .. }
            if *subject == root && *predicate == rdf_type && *object == profile)
    })
}

pub(crate) fn not_profiled_status(document_id: Ulid) -> ProfileValidationStatus {
    ProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: ProfileValidationState::NotProfiled,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: Some(now_ms()),
        findings: Vec::new(),
        completeness: ProfileValidationCompleteness::Complete,
        stale_reason: None,
        dataset_digest: None,
    }
}

pub(crate) fn stale_status(document_id: Ulid, reason: &str) -> ProfileValidationStatus {
    ProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: ProfileValidationState::Stale,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: None,
        findings: Vec::new(),
        completeness: ProfileValidationCompleteness::Incomplete,
        stale_reason: Some(reason.to_string()),
        dataset_digest: None,
    }
}

pub(super) fn data_graph(jsonld: &str) -> Result<(Dataset, NamedOrBlankNode), MetadataError> {
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

/// The entity the metadata descriptor is `about`; an `about` of any other entity names no root.
pub(super) fn crate_root(dataset: &Dataset) -> Option<NamedOrBlankNode> {
    for predicate in [SCHEMA_ABOUT, SCHEMA_HTTPS_ABOUT] {
        let predicate = NamedNode::new_unchecked(predicate);
        for quad in dataset.quads_for_predicate(&predicate) {
            let descriptor = matches!(quad.subject, NamedOrBlankNodeRef::NamedNode(subject)
                if subject.as_str().strip_prefix(CRAQLE_BASE_IRI) == Some("ro-crate-metadata.json"));
            if !quad.graph_name.is_default_graph() || !descriptor {
                continue;
            }
            if let Some(root) = term_as_node(quad.object.into_owned()) {
                return Some(root);
            }
        }
    }
    None
}

pub(super) fn profile_tags(dataset: &Dataset, root: &NamedOrBlankNode) -> Vec<String> {
    let mut tags = BTreeSet::new();
    for predicate in [
        DCTERMS_CONFORMS_TO,
        SCHEMA_CONFORMS_TO,
        SCHEMA_HTTPS_CONFORMS,
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

pub(super) fn objects(dataset: &Dataset, subject: &NamedOrBlankNode, predicate: &str) -> Vec<Term> {
    let predicate = NamedNode::new_unchecked(predicate);
    dataset
        .quads_for_subject(subject)
        .filter(|quad| quad.graph_name.is_default_graph() && quad.predicate == predicate.as_ref())
        .map(|quad| quad.object.into_owned())
        .collect()
}

pub(super) fn has_type(dataset: &Dataset, subject: &NamedOrBlankNode, class: &str) -> bool {
    objects(dataset, subject, RDF_TYPE)
        .iter()
        .any(|term| matches!(term, Term::NamedNode(value) if value.as_str() == class))
}

pub(super) fn term_as_node(term: Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node)),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node)),
        _ => None,
    }
}
