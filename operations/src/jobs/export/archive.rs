//! Plans the export ZIP layout, recognizes crate entities and builds the rewritten report rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};

use aruna_core::archive_path::{KeyLayout, StorageKey, safe_zip_path, synthesized_path};
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportOmissionCounts, ExportReportDetail, ExportReportRow, ExportRoCrateSpec,
    JobError, ReasonCode,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::data::identity::{DataIdentity, ObjectLocation, content_id};
use oxrdf::{NamedOrBlankNode, Term};
use oxttl::NQuadsParser;
use serde_json::{Value as JsonValue, json};
use ulid::Ulid;
use url::Url;

use super::resolve::CandidateSource;
use super::{
    ABOUT_HTTPS_IRI, CONTENT_HTTPS_IRI, ENCODING_HTTPS_IRI, ExportCheckpoint, ExportEntity,
    ExportFailure, ExportPhase, JSONLD_BASE_IRI, LOCAL_PATH_IRI, METADATA_PATH, NAME_HTTPS_IRI,
    PART_HTTPS_IRI, PATH_HTTP_IRI, ProbedEntry, REPORT_PATH, SCHEMA_ABOUT_IRI, SCHEMA_CONTENT_IRI,
    SCHEMA_ENCODING_IRI, SCHEMA_NAME_IRI, SCHEMA_PART_IRI, SCHEMA_SUBJECT_IRI, SUBJECT_HTTPS_IRI,
};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::rocrate_jsonld::{
    JsonLdKeywords, MEDIA_HTTPS_IRI, RDF_TYPE_IRI, SCHEMA_MEDIA_IRI, is_file_type,
};

pub(super) fn plan_export(
    spec: &ExportRoCrateSpec,
    checkpoint: &mut ExportCheckpoint,
    opened: &[ProbedEntry],
) -> Result<(), ExportFailure> {
    let sources = opened
        .iter()
        .map(|entry| {
            source_key(
                &checkpoint.entities[entry.entity_index],
                entry.candidate_index,
            )
        })
        .collect::<Vec<_>>();
    let layout = KeyLayout::new(&sources);
    let mut paths = HashSet::new();
    for (entry, source) in opened.iter().zip(&sources) {
        let entity = &mut checkpoint.entities[entry.entity_index];
        entity.report_source = Some(entry.report_source);
        entity.resolved_version = entry.resolved_version;
        entity.content = Some((entry.hash, entry.size));
        let reserved = |path: &String| path == METADATA_PATH || path == REPORT_PATH;
        let explicit = entity
            .local_path
            .as_deref()
            .and_then(safe_zip_path)
            .filter(|path| !reserved(path));
        let derived = source
            .as_ref()
            .and_then(|source| layout.path(source))
            .filter(|path| !reserved(path));
        let path = match explicit {
            Some(path) => path,
            None => {
                entity.path_synthesized = true;
                derived.unwrap_or_else(|| synthesized_path(entry.hash, &entity.entity_id))
            }
        };
        if path.len() as u64 > spec.limits.key_bytes {
            return Err(ExportFailure::Permanent(format!(
                "ZIP path exceeds the {} byte limit",
                spec.limits.key_bytes
            )));
        }
        if !paths.insert(path.clone()) {
            return Err(ExportFailure::Permanent(format!(
                "multiple File entities resolve to ZIP path `{path}`"
            )));
        }
        entity.zip_path = Some(path);
    }

    let raw = checkpoint
        .raw_jsonld
        .as_deref()
        .ok_or_else(|| ExportFailure::Permanent("raw RO-Crate snapshot is missing".to_string()))?;
    let mut document: JsonValue =
        serde_json::from_str(raw).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    let replacements = checkpoint
        .entities
        .iter()
        .filter_map(|entity| {
            entity
                .zip_path
                .as_ref()
                .map(|path| (entity.entity_id.clone(), jsonld_path(path)))
        })
        .collect::<BTreeMap<_, _>>();
    let unrewritten = scan_unrewritten(&document, &replacements);
    rewrite_ids(&mut document, &replacements);
    if spec.selection.is_some() {
        add_references(&mut document, &checkpoint.entities)?;
    }
    checkpoint.report = build_rows(&checkpoint.entities, &unrewritten);
    let has_omissions = if spec.destination.is_some() {
        blocking_omissions(&checkpoint.report) > 0
    } else {
        checkpoint.report.iter().any(|row| {
            matches!(
                row.code,
                ReasonCode::External
                    | ReasonCode::Denied
                    | ReasonCode::Missing
                    | ReasonCode::Offline
                    | ReasonCode::Unsupported
            )
        })
    };
    checkpoint.report_json = if has_omissions {
        let report = build_report(checkpoint)?;
        add_report(&mut document)?;
        Some(
            serde_json::to_vec(&report)
                .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
        )
    } else {
        None
    };
    let rewritten = serde_json::to_vec(&document)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
    if rewritten.len() as u64 > spec.limits.metadata_bytes {
        return Err(ExportFailure::Permanent(format!(
            "rewritten RO-Crate metadata exceeds the {} byte limit",
            spec.limits.metadata_bytes
        )));
    }
    craqle::validate_rocrate_jsonld(
        std::str::from_utf8(&rewritten)
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?,
    )
    .map_err(map_crate_error)?;
    precheck_size(
        spec,
        &rewritten,
        checkpoint.report_json.as_deref(),
        opened,
        &checkpoint.entities,
    )?;
    checkpoint.rewritten_jsonld = Some(rewritten);
    checkpoint.phase = ExportPhase::Assemble;
    Ok(())
}

pub fn recognize_entities(
    document: &JsonValue,
    nquads: &str,
    realm_id: RealmId,
) -> Result<Vec<ExportEntity>, ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let raw_ids = raw_entity_ids(document, &keywords)?;
    let mut files = BTreeSet::new();
    let mut content_urls = BTreeMap::<String, Vec<String>>::new();
    let mut local_paths = BTreeMap::<String, Vec<String>>::new();
    for quad in NQuadsParser::new().for_slice(nquads) {
        let quad = quad.map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let NamedOrBlankNode::NamedNode(subject) = quad.subject else {
            continue;
        };
        let subject = subject.as_str().to_string();
        match quad.predicate.as_str() {
            RDF_TYPE_IRI
                if matches!(
                    &quad.object,
                    Term::NamedNode(node)
                        if is_file_type(node.as_str())
                ) =>
            {
                files.insert(subject);
            }
            SCHEMA_CONTENT_IRI | CONTENT_HTTPS_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    content_urls.entry(subject).or_default().push(value);
                }
            }
            LOCAL_PATH_IRI | PATH_HTTP_IRI => {
                if let Some(value) = term_value(&quad.object) {
                    local_paths.entry(subject).or_default().push(value);
                }
            }
            _ => {}
        }
    }

    let mut entities = Vec::new();
    for (subject, entity_id, raw_path) in raw_ids {
        if !files.remove(&subject) {
            continue;
        }
        let urls = content_urls.get(&subject).map_or(&[][..], Vec::as_slice);
        let identity = DataIdentity::read(&entity_id, urls);
        let storage_key = identity
            .exact
            .as_ref()
            .filter(|exact| exact.realm_id == realm_id)
            .map(|exact| StorageKey {
                bucket: exact.bucket.clone(),
                key: exact.key.clone(),
            })
            .or_else(|| {
                identity.location.as_ref().map(|location| StorageKey {
                    bucket: location.bucket.clone(),
                    key: location.key.clone(),
                })
            });
        let hash_realm = identity.hash_realm;
        let supported_exact = identity
            .exact
            .as_ref()
            .is_some_and(|exact| exact.realm_id == realm_id);
        let supported_hash =
            identity.hash.is_some() && hash_realm.is_none_or(|hash_realm| hash_realm == realm_id);
        let located = identity.location.is_some();
        let foreign = identity.is_aruna() && !supported_exact && !supported_hash && !located;
        // A web identifier of another realm is a reference, as an export into a realm leaves it.
        let external = !identity.is_aruna() || (foreign && web_entity(&entity_id));
        let unsupported_realm = foreign && !external;
        let paths = local_paths.remove(&subject).unwrap_or_default();
        let local_path = raw_path
            .filter(|raw_path| paths.contains(raw_path))
            .or_else(|| paths.into_iter().next());
        entities.push(ExportEntity {
            entity_id,
            local_path,
            storage_key,
            exact: identity.exact,
            hash: identity.hash,
            hash_realm,
            candidates: Vec::new(),
            omission: if external {
                Some(ReasonCode::External)
            } else if unsupported_realm {
                Some(ReasonCode::Unsupported)
            } else {
                None
            },
            message: if external {
                Some("external File entity was not fetched".to_string())
            } else if unsupported_realm {
                Some("Aruna identifier belongs to another realm".to_string())
            } else {
                None
            },
            zip_path: None,
            report_source: None,
            resolved_version: None,
            path_synthesized: false,
            content: None,
        });
    }
    if let Some(subject) = files.into_iter().next() {
        return Err(ExportFailure::Permanent(format!(
            "expanded File entity `{subject}` has no raw JSON-LD definition"
        )));
    }
    Ok(entities)
}

fn raw_entity_ids(
    document: &JsonValue,
    keywords: &JsonLdKeywords,
) -> Result<Vec<(String, String, Option<String>)>, ExportFailure> {
    fn collect(
        value: &JsonValue,
        keywords: &JsonLdKeywords,
        entities: &mut Vec<(String, String, Option<String>)>,
    ) -> Result<(), ExportFailure> {
        match value {
            JsonValue::Array(values) => {
                for value in values {
                    collect(value, keywords, entities)?;
                }
            }
            JsonValue::Object(object) => {
                if object.len() > 1
                    && let Some((_, id)) = keywords.object_id(object)
                {
                    let expanded = expanded_id(id)?;
                    if let Some((_, existing_id, _)) = entities
                        .iter()
                        .find(|(existing, _, _)| existing == &expanded)
                    {
                        if existing_id != id {
                            return Err(ExportFailure::Permanent(format!(
                                "JSON-LD entity `{expanded}` uses ambiguous identifiers"
                            )));
                        }
                    } else {
                        entities.push((expanded, id.to_string(), raw_local_path(object, keywords)));
                    }
                }
                for value in object.values() {
                    collect(value, keywords, entities)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let mut entities = Vec::new();
    collect(document, keywords, &mut entities)?;
    Ok(entities)
}

fn raw_local_path(
    object: &serde_json::Map<String, JsonValue>,
    keywords: &JsonLdKeywords,
) -> Option<String> {
    object.iter().find_map(|(key, value)| {
        keywords
            .expands_to(key, &["localPath", LOCAL_PATH_IRI, PATH_HTTP_IRI])
            .then(|| match value {
                JsonValue::String(value) => Some(value.clone()),
                JsonValue::Array(values) => values
                    .iter()
                    .find_map(JsonValue::as_str)
                    .map(str::to_string),
                _ => None,
            })
            .flatten()
    })
}

pub(super) fn expanded_id(id: &str) -> Result<String, ExportFailure> {
    if let Ok(url) = Url::parse(id) {
        return Ok(url.to_string());
    }
    Url::parse(JSONLD_BASE_IRI)
        .expect("static JSON-LD base is valid")
        .join(id)
        .map(String::from)
        .map_err(|error| ExportFailure::Permanent(error.to_string()))
}

fn term_value(term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(value) => Some(value.as_str().to_string()),
        Term::Literal(value) => Some(value.value().to_string()),
        _ => None,
    }
}

pub(super) fn jsonld_path(path: &str) -> String {
    let mut url = Url::parse(JSONLD_BASE_IRI).expect("static JSON-LD base is valid");
    {
        let mut segments = url
            .path_segments_mut()
            .expect("static JSON-LD base supports path segments");
        segments.clear();
        for segment in path.split('/') {
            segments.push(segment);
        }
    }
    url.path().trim_start_matches('/').to_string()
}

/// The authored location wins over the resolved candidate: a content hash may
/// be served from any alias, and only the authored key matches the crate.
pub(super) fn source_key(entity: &ExportEntity, candidate_index: usize) -> Option<StorageKey> {
    if let Some(storage_key) = entity.storage_key.clone() {
        return Some(storage_key);
    }
    match &entity.candidates.get(candidate_index)?.source {
        CandidateSource::Local { bucket, key, .. } => Some(StorageKey {
            bucket: bucket.clone(),
            key: key.clone(),
        }),
        CandidateSource::RemoteExact { target, .. } => Some(StorageKey {
            bucket: target.bucket.clone(),
            key: target.key.clone(),
        }),
        CandidateSource::RemoteHash { .. } => None,
    }
}

pub(super) fn scan_unrewritten(
    document: &JsonValue,
    replacements: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    fn scan(
        value: &JsonValue,
        key: Option<&str>,
        replacements: &BTreeMap<String, String>,
        keywords: &JsonLdKeywords,
        found: &mut BTreeSet<String>,
    ) {
        match value {
            JsonValue::String(value)
                if !key.is_some_and(|key| keywords.is_id(key))
                    && replacements.contains_key(value.as_str()) =>
            {
                found.insert(value.clone());
            }
            JsonValue::Array(values) => {
                for value in values {
                    scan(value, key, replacements, keywords, found);
                }
            }
            JsonValue::Object(values) => {
                for (key, value) in values {
                    scan(value, Some(key), replacements, keywords, found);
                }
            }
            _ => {}
        }
    }
    let keywords = JsonLdKeywords::new(document);
    let mut found = BTreeSet::new();
    scan(document, None, replacements, &keywords, &mut found);
    found
}

pub(super) fn rewrite_ids(value: &mut JsonValue, replacements: &BTreeMap<String, String>) {
    let keywords = JsonLdKeywords::new(value);
    rewrite_id_values(value, replacements, &keywords);
}

fn rewrite_id_values(
    value: &mut JsonValue,
    replacements: &BTreeMap<String, String>,
    keywords: &JsonLdKeywords,
) {
    match value {
        JsonValue::Array(values) => {
            for value in values {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        JsonValue::Object(values) => {
            let id_key = keywords.object_id(values).map(|(key, _)| key.to_string());
            if let Some(JsonValue::String(value)) =
                id_key.as_deref().and_then(|key| values.get_mut(key))
                && let Some(replacement) = replacements.get(value)
            {
                *value = replacement.clone();
            }
            for value in values.values_mut() {
                rewrite_id_values(value, replacements, keywords);
            }
        }
        _ => {}
    }
}

pub(super) fn build_rows(
    entities: &[ExportEntity],
    unrewritten: &BTreeSet<String>,
) -> Vec<ExportReportRow> {
    let mut rows = Vec::new();
    for (index, entity) in entities.iter().enumerate() {
        let main_code = entity.omission.unwrap_or(ReasonCode::Included);
        rows.push(ExportReportRow {
            entry_key: format!("{index:016x}:main"),
            code: main_code,
            message: entity.message.clone(),
            detail: ExportReportDetail {
                entity_id: entity.entity_id.clone(),
                zip_path: entity.zip_path.clone(),
                source: entity.report_source,
                resolved_version: entity.resolved_version,
                validation: None,
            },
        });
        if entity.path_synthesized {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:path"),
                code: ReasonCode::PathSynthesized,
                message: Some("unsafe, absent, or reserved localPath was synthesized".to_string()),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
        if unrewritten.contains(&entity.entity_id) {
            rows.push(ExportReportRow {
                entry_key: format!("{index:016x}:reference"),
                code: ReasonCode::UnrewrittenReference,
                message: Some(
                    "a string-form reference outside an @id field was preserved".to_string(),
                ),
                detail: ExportReportDetail {
                    entity_id: entity.entity_id.clone(),
                    zip_path: entity.zip_path.clone(),
                    source: entity.report_source,
                    resolved_version: entity.resolved_version,
                    validation: None,
                },
            });
        }
    }
    rows
}

/// The time of the pinned snapshot revision, so exports of one snapshot are byte-identical.
pub(super) fn snapshot_moment(checkpoint: &ExportCheckpoint) -> Result<u64, ExportFailure> {
    checkpoint
        .winning_event_id
        .map(|event_id| event_id.timestamp_ms())
        .ok_or_else(|| ExportFailure::Permanent("snapshot event cursor is missing".to_string()))
}

fn build_report(checkpoint: &ExportCheckpoint) -> Result<JsonValue, ExportFailure> {
    let event_id = checkpoint
        .winning_event_id
        .ok_or_else(|| ExportFailure::Permanent("snapshot event cursor is missing".to_string()))?;
    let context_digest = checkpoint.context_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot context digest is missing".to_string())
    })?;
    let dataset_digest = checkpoint.dataset_digest.ok_or_else(|| {
        ExportFailure::Permanent("snapshot dataset digest is missing".to_string())
    })?;
    let omissions = checkpoint
        .report
        .iter()
        .filter(|row| {
            matches!(
                row.code,
                ReasonCode::External
                    | ReasonCode::Denied
                    | ReasonCode::Missing
                    | ReasonCode::Offline
                    | ReasonCode::Unsupported
            )
        })
        .map(|row| {
            json!({
                "entity_id": &row.detail.entity_id,
                "code": row.code,
                "message": &row.message,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "winning_event_id": event_id,
        "context_digest": hex::encode(context_digest),
        "dataset_digest": hex::encode(dataset_digest),
        "omissions": omissions,
    }))
}

pub(super) fn add_report(document: &mut JsonValue) -> Result<(), ExportFailure> {
    let keywords = JsonLdKeywords::new(document);
    let graph = keywords
        .graph(document)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate @graph is missing".to_string()))?;
    if graph.iter().any(|entity| {
        entity
            .as_object()
            .and_then(|entity| keywords.object_id(entity))
            .is_some_and(|(_, id)| id == REPORT_PATH || id == "#aruna-export-report")
    }) {
        return Err(ExportFailure::Permanent(
            "RO-Crate uses a reserved export report identifier".to_string(),
        ));
    }
    let root_id = report_root_id(graph, &keywords).ok_or_else(|| {
        ExportFailure::Permanent("RO-Crate metadata descriptor has no root".to_string())
    })?;
    let graph = keywords
        .graph_mut(document)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate @graph is missing".to_string()))?;
    let root = graph
        .iter_mut()
        .find(|entity| {
            entity
                .as_object()
                .and_then(|entity| keywords.object_id(entity))
                .is_some_and(|(_, id)| id == root_id)
        })
        .and_then(JsonValue::as_object_mut)
        .ok_or_else(|| ExportFailure::Permanent("RO-Crate root Dataset is missing".to_string()))?;
    let subject_key = property_key(
        root,
        &keywords,
        &[
            "subjectOf",
            "schema:subjectOf",
            SCHEMA_SUBJECT_IRI,
            SUBJECT_HTTPS_IRI,
        ],
        "subjectOf",
        SUBJECT_HTTPS_IRI,
    );
    match root.get_mut(&subject_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": "#aruna-export-report"})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": "#aruna-export-report"}]);
        }
        None => {
            root.insert(subject_key, json!({"@id": "#aruna-export-report"}));
        }
    }
    let part_key = property_key(
        root,
        &keywords,
        &["hasPart", "schema:hasPart", SCHEMA_PART_IRI, PART_HTTPS_IRI],
        "hasPart",
        PART_HTTPS_IRI,
    );
    match root.get_mut(&part_key) {
        Some(JsonValue::Array(values)) => values.push(json!({"@id": REPORT_PATH})),
        Some(value) => {
            let previous = std::mem::take(value);
            *value = json!([previous, {"@id": REPORT_PATH}]);
        }
        None => {
            root.insert(part_key, json!({"@id": REPORT_PATH}));
        }
    }
    let encoding_key = safe_term(
        &keywords,
        "encodingFormat",
        &[
            SCHEMA_ENCODING_IRI,
            ENCODING_HTTPS_IRI,
            "schema:encodingFormat",
        ],
        ENCODING_HTTPS_IRI,
    );
    let about_key = safe_term(
        &keywords,
        "about",
        &[SCHEMA_ABOUT_IRI, ABOUT_HTTPS_IRI, "schema:about"],
        ABOUT_HTTPS_IRI,
    );
    let name_key = safe_term(
        &keywords,
        "name",
        &[SCHEMA_NAME_IRI, NAME_HTTPS_IRI, "schema:name"],
        NAME_HTTPS_IRI,
    );
    let file_type = if keywords.term_matches(
        "File",
        &[SCHEMA_MEDIA_IRI, MEDIA_HTTPS_IRI, "schema:MediaObject"],
    ) {
        "File"
    } else {
        MEDIA_HTTPS_IRI
    };
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!(REPORT_PATH)),
        ("@type".to_string(), json!(file_type)),
        (encoding_key, json!("application/json")),
        (about_key.clone(), json!({"@id": "#aruna-export-report"})),
    ])));
    graph.push(JsonValue::Object(serde_json::Map::from_iter([
        ("@id".to_string(), json!("#aruna-export-report")),
        ("@type".to_string(), json!("http://schema.org/CreativeWork")),
        (name_key, json!("Aruna RO-Crate export completeness report")),
        (about_key, json!({"@id": root_id})),
    ])));
    Ok(())
}

fn report_root_id(graph: &[JsonValue], keywords: &JsonLdKeywords) -> Option<String> {
    graph.iter().find_map(|entity| {
        let entity = entity.as_object()?;
        let (_, id) = keywords.object_id(entity)?;
        if id.trim_start_matches("./") != METADATA_PATH {
            return None;
        }
        entity.iter().find_map(|(key, value)| {
            keywords
                .expands_to(
                    key,
                    &["about", "schema:about", SCHEMA_ABOUT_IRI, ABOUT_HTTPS_IRI],
                )
                .then(|| reference_id(value, keywords).map(str::to_string))
                .flatten()
        })
    })
}

fn reference_id<'a>(value: &'a JsonValue, keywords: &JsonLdKeywords) -> Option<&'a str> {
    match value {
        JsonValue::String(value) => Some(value),
        JsonValue::Object(value) => keywords.object_id(value).map(|(_, id)| id),
        JsonValue::Array(values) => values
            .iter()
            .find_map(|value| reference_id(value, keywords)),
        _ => None,
    }
}

fn property_key(
    object: &serde_json::Map<String, JsonValue>,
    keywords: &JsonLdKeywords,
    values: &[&str],
    compact: &str,
    absolute: &str,
) -> String {
    object
        .keys()
        .find(|key| keywords.expands_to(key, values))
        .cloned()
        .unwrap_or_else(|| safe_term(keywords, compact, values, absolute))
}

fn safe_term(keywords: &JsonLdKeywords, compact: &str, values: &[&str], absolute: &str) -> String {
    if keywords.term_matches(compact, values) {
        compact.to_string()
    } else {
        absolute.to_string()
    }
}

pub(super) fn precheck_size(
    spec: &ExportRoCrateSpec,
    metadata: &[u8],
    report: Option<&[u8]>,
    opened: &[ProbedEntry],
    entities: &[ExportEntity],
) -> Result<(), ExportFailure> {
    let mut size = (metadata.len() as u64)
        .checked_add(256 + 2 * METADATA_PATH.len() as u64)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    let mut entries = 1u64;
    if let Some(report) = report {
        size = size
            .checked_add(report.len() as u64)
            .and_then(|size| size.checked_add(256 + 2 * REPORT_PATH.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    for entry in opened {
        let path = entities
            .get(entry.entity_index)
            .and_then(|entity| entity.zip_path.as_deref())
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        size = size
            .checked_add(entry.size)
            .and_then(|size| size.checked_add(256 + 2 * path.len() as u64))
            .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
        entries += 1;
    }
    size = size
        .checked_add(256)
        .ok_or_else(|| ExportFailure::Permanent("export size overflow".to_string()))?;
    if entries > spec.limits.max_entries.saturating_add(2) {
        return Err(ExportFailure::Permanent(format!(
            "export has more than {} payload entries",
            spec.limits.max_entries
        )));
    }
    if size > spec.limits.export_artifact_bytes {
        return Err(ExportFailure::Permanent(format!(
            "planned ZIP exceeds the {} byte artifact limit",
            spec.limits.export_artifact_bytes
        )));
    }
    Ok(())
}

pub(super) fn report_counts(rows: &[ExportReportRow]) -> (u64, ExportOmissionCounts) {
    let mut included = 0u64;
    let mut omitted = ExportOmissionCounts::default();
    for row in rows {
        match row.code {
            ReasonCode::Included => included = included.saturating_add(1),
            ReasonCode::External => omitted.external = omitted.external.saturating_add(1),
            ReasonCode::Denied => omitted.denied = omitted.denied.saturating_add(1),
            ReasonCode::Missing => omitted.missing = omitted.missing.saturating_add(1),
            ReasonCode::Offline => omitted.offline = omitted.offline.saturating_add(1),
            ReasonCode::Unsupported => omitted.unsupported = omitted.unsupported.saturating_add(1),
            _ => {}
        }
    }
    (included, omitted)
}

/// Omissions a repository record cannot hold; web data entities stay as `references`.
pub(crate) fn blocking_omissions(rows: &[ExportReportRow]) -> usize {
    rows.iter()
        .filter(|row| match row.code {
            ReasonCode::External => !web_entity(&row.detail.entity_id),
            ReasonCode::Denied
            | ReasonCode::Missing
            | ReasonCode::Offline
            | ReasonCode::Unsupported => true,
            _ => false,
        })
        .count()
}

/// What a grant for a finished export into another realm pins.
#[derive(Debug)]
pub(crate) struct ExportFacts {
    pub(crate) revision: Ulid,
    pub(crate) dataset_digest: [u8; 32],
    pub(crate) versions: Vec<aruna_core::transfer::SelectedVersion>,
    pub(crate) sources: Vec<aruna_core::transfer::PinnedSource>,
    pub(crate) artifact: ArtifactRef,
}

impl ExportCheckpoint {
    /// The facts of a finished export, unless a selected file was left out or not read here.
    pub(crate) fn export_facts(&self, files: &[String]) -> Option<ExportFacts> {
        let mut versions = Vec::new();
        let mut sources = Vec::new();
        for file in files {
            let entity = self
                .entities
                .iter()
                .find(|entity| &entity.entity_id == file)?;
            let (blake3, size) = entity.content.filter(|_| entity.omission.is_none())?;
            let version_id = entity.resolved_version?;
            versions.push(aruna_core::transfer::SelectedVersion {
                version_id,
                blake3,
                size,
            });
            let source = entity.candidates.iter().find_map(|candidate| {
                let CandidateSource::Local {
                    location,
                    permission_path,
                    bucket,
                    key,
                    ..
                } = &candidate.source
                else {
                    return None;
                };
                (candidate.resolved_version == Some(version_id)).then(|| {
                    aruna_core::transfer::PinnedSource {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        path: permission_path.clone(),
                        version_id,
                        blake3,
                        size,
                        key_ref: location.format.bucket_key(),
                    }
                })
            });
            sources.push(source?);
        }
        Some(ExportFacts {
            revision: self.winning_event_id?,
            dataset_digest: self.dataset_digest?,
            versions,
            sources,
            artifact: self.artifact.clone()?,
        })
    }
}

/// Leaves every File entity outside `files` as a reference to this realm.
pub(super) fn keep_references(entities: &mut [ExportEntity], files: &[String]) {
    for entity in entities.iter_mut() {
        if entity.omission.is_none() && !files.contains(&entity.entity_id) {
            entity.omission = Some(ReasonCode::External);
            entity.message = Some("left as a reference to the source realm".to_string());
        }
    }
}

/// The web identifier and the original identifier of an entity left as a reference.
fn reference_ids(entity: &ExportEntity) -> Option<(String, String)> {
    let id = match (&entity.exact, entity.hash) {
        (Some(exact), _) => (exact.to_w3id(), exact.to_string()),
        (None, Some(hash)) => (content_id(hash), entity.entity_id.clone()),
        (None, None) => return None,
    };
    Some(id)
}

/// Names each Aruna entity left out of an export into a realm by its web identifier, keeps the
/// original identifier as `identifier` and drops its source-local locations, so the receiving
/// realm keeps it as an external reference. A source location without a version fails.
pub(super) fn add_references(
    document: &mut JsonValue,
    entities: &[ExportEntity],
) -> Result<(), ExportFailure> {
    let mut references = BTreeMap::new();
    for entity in entities.iter().filter(|entity| entity.omission.is_some()) {
        match reference_ids(entity) {
            Some(ids) => references.insert(entity.entity_id.clone(), ids),
            None if entity.storage_key.is_some() => {
                return Err(ExportFailure::Permanent(format!(
                    "File entity `{}` names a source location without a current version",
                    entity.entity_id
                )));
            }
            None => continue,
        };
    }
    let replacements = references
        .iter()
        .map(|(id, (web, _))| (id.clone(), web.clone()))
        .collect();
    rewrite_ids(document, &replacements);
    let keywords = JsonLdKeywords::new(document);
    let identifiers = references
        .into_values()
        .collect::<BTreeMap<String, String>>();
    if let Some(graph) = keywords.graph_mut(document) {
        for value in graph.iter_mut() {
            clean_references(value, &identifiers, &keywords);
        }
    }
    Ok(())
}

/// Gives every entity left as a reference, nested ones too, its original identifier and drops
/// its source-local paths and content URLs.
fn clean_references(
    value: &mut JsonValue,
    identifiers: &BTreeMap<String, String>,
    keywords: &JsonLdKeywords,
) {
    let object = match value {
        JsonValue::Array(values) => {
            for value in values {
                clean_references(value, identifiers, keywords);
            }
            return;
        }
        JsonValue::Object(object) => object,
        _ => return,
    };
    let id = keywords.object_id(object).map(|(_, id)| id.to_string());
    // A bare link holds only its id; a property would make it a second definition.
    let defines = object.len() > 1;
    if let Some(original) = id.filter(|_| defines).and_then(|id| identifiers.get(&id)) {
        object
            .entry("identifier")
            .or_insert_with(|| JsonValue::String(original.clone()));
        let local_path = ["localPath", LOCAL_PATH_IRI, PATH_HTTP_IRI];
        let content_url = ["contentUrl", SCHEMA_CONTENT_IRI, CONTENT_HTTPS_IRI];
        object.retain(|key, value| {
            if keywords.expands_to(key, &local_path) {
                return false;
            }
            if !keywords.expands_to(key, &content_url) {
                return true;
            }
            if let JsonValue::Array(values) = value {
                values.retain(|value| !source_local(value, keywords));
                return !values.is_empty();
            }
            !source_local(value, keywords)
        });
    }
    for value in object.values_mut() {
        clean_references(value, identifiers, keywords);
    }
}

/// A source-local location written as a string, an `@id` node or an `@value` literal.
fn source_local(value: &JsonValue, keywords: &JsonLdKeywords) -> bool {
    let text = match value {
        JsonValue::Object(object) => keywords
            .object_id(object)
            .map(|(_, id)| id)
            .or_else(|| object.get("@value").and_then(JsonValue::as_str)),
        value => value.as_str(),
    };
    text.and_then(ObjectLocation::parse).is_some()
}

/// A data entity on the web, which a crate may name without carrying its bytes.
pub(crate) fn web_entity(entity_id: &str) -> bool {
    entity_id.starts_with("https://") || entity_id.starts_with("http://")
}

pub(super) fn map_crate_error(error: craqle::RoCrateError) -> ExportFailure {
    match error {
        craqle::RoCrateError::Update(craqle::UpdateError::ValidationFailed(violations)) => {
            ExportFailure::Validation(violations.into_iter().map(Into::into).collect())
        }
        error => ExportFailure::Permanent(error.to_string()),
    }
}

/// The report rows of a crate that failed validation.
pub(super) fn validation_rows(violations: &[MetadataValidationViolation]) -> Vec<ExportReportRow> {
    (violations.iter().enumerate())
        .map(|(index, violation)| ExportReportRow {
            entry_key: format!("validation/{index:08}"),
            code: if violation.code == "unsupported_crate_version" {
                ReasonCode::UnsupportedCrateVersion
            } else {
                ReasonCode::Failed
            },
            message: Some(violation.message.clone()),
            detail: ExportReportDetail {
                entity_id: violation.entity_id.clone().unwrap_or_default(),
                zip_path: None,
                source: None,
                resolved_version: None,
                validation: Some(violation.clone()),
            },
        })
        .collect()
}

pub fn failure_outcome(error: ExportFailure) -> JobRunOutcome {
    match error {
        ExportFailure::Permanent(message) => permanent(message),
        ExportFailure::Retryable(message) => retryable(message),
        ExportFailure::KeyLocked(wait) => JobRunOutcome::AwaitingKey(vec![wait]),
        ExportFailure::RemoteLocked { .. } => retryable("a remote input's bucket key is locked"),
        ExportFailure::Validation(violations) => permanent(validation_message(&violations)),
        ExportFailure::Candidate { message, .. } => retryable(message),
        ExportFailure::Cancelled => JobRunOutcome::Cancelled,
        ExportFailure::Interrupted => JobRunOutcome::Interrupted,
    }
}

pub(super) fn validation_message(violations: &[MetadataValidationViolation]) -> String {
    violations
        .iter()
        .map(|violation| {
            format!(
                "{} at {}: {}",
                violation.code, violation.pointer, violation.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub(super) fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

pub(super) fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

/// 2026-02-03T04:05:06Z: a fixed moment keeps fixture archives byte-identical.
#[cfg(test)]
pub(super) const FIXTURE_MOMENT_MS: u64 = 1_770_091_506_000;
