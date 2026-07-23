use std::collections::{HashMap, HashSet};

use craqle::{CrateViolation, RoCrateError, UpdateError};
use oxrdf::{NamedOrBlankNode, Term};
use oxttl::NQuadsParser;
use serde_json::{Map, Value, json};
use thiserror::Error;
use url::Url;

const JSONLD_BASE_IRI: &str = "https://craqle.invalid/";
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_MEDIA_IRI: &str = "http://schema.org/MediaObject";
const LOCAL_PATH_IRI: &str = "https://w3id.org/ro/terms#localPath";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationIssue {
    pub code: String,
    pub message: String,
    pub pointer: String,
    pub entity_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum CrateValidationError {
    #[error("RO-Crate validation failed")]
    Violations(Vec<ValidationIssue>),
    #[error("RO-Crate validation failed: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug)]
pub struct ValidatedDocument {
    pub value: Value,
    pub file_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct RewriteTarget {
    pub w3id: String,
    pub hash_w3id: String,
    pub local_path: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RewriteOutcome {
    pub jsonld: String,
    pub warnings: Vec<String>,
}

pub fn validate_document(jsonld: &str) -> Result<ValidatedDocument, CrateValidationError> {
    let canonical = craqle::canonicalize_jsonld(jsonld).map_err(map_validation_error)?;
    let file_subjects = file_subjects(&canonical.nquads)?;
    let value: Value = serde_json::from_str(jsonld)
        .map_err(|error| CrateValidationError::Invalid(error.to_string()))?;
    let mut file_ids = Vec::new();
    collect_file_ids(&value, &file_subjects, &mut file_ids)?;
    Ok(ValidatedDocument { value, file_ids })
}

pub fn rewrite_document(
    mut value: Value,
    targets: &HashMap<String, RewriteTarget>,
) -> Result<RewriteOutcome, CrateValidationError> {
    let mut warnings = HashSet::new();
    rewrite_value(&mut value, targets, &mut warnings);
    if uses_v11(&value) && !targets.is_empty() {
        ensure_local_context(&mut value)?;
    }
    let jsonld = serde_json::to_string(&value)
        .map_err(|error| CrateValidationError::Invalid(error.to_string()))?;
    let _ = validate_document(&jsonld)?;
    let mut warnings = warnings.into_iter().collect::<Vec<_>>();
    warnings.sort();
    Ok(RewriteOutcome { jsonld, warnings })
}

fn file_subjects(nquads: &str) -> Result<HashSet<String>, CrateValidationError> {
    let mut subjects = HashSet::new();
    for quad in NQuadsParser::new().for_slice(nquads) {
        let quad = quad.map_err(|error| CrateValidationError::Invalid(error.to_string()))?;
        if quad.predicate.as_str() != RDF_TYPE_IRI
            || !matches!(&quad.object, Term::NamedNode(node) if node.as_str() == SCHEMA_MEDIA_IRI)
        {
            continue;
        }
        if let NamedOrBlankNode::NamedNode(subject) = quad.subject {
            subjects.insert(subject.as_str().to_string());
        }
    }
    Ok(subjects)
}

fn collect_file_ids(
    value: &Value,
    subjects: &HashSet<String>,
    file_ids: &mut Vec<String>,
) -> Result<(), CrateValidationError> {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_file_ids(value, subjects, file_ids)?;
            }
        }
        Value::Object(object) => {
            if object.len() > 1
                && let Some(id) = object.get("@id").and_then(Value::as_str)
                && subjects.contains(&expanded_id(id)?)
            {
                if file_ids.iter().any(|existing| existing == id) {
                    return Err(CrateValidationError::Invalid(format!(
                        "File entity `{id}` is defined more than once"
                    )));
                }
                file_ids.push(id.to_string());
            }
            for value in object.values() {
                collect_file_ids(value, subjects, file_ids)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn expanded_id(id: &str) -> Result<String, CrateValidationError> {
    if let Ok(url) = Url::parse(id) {
        return Ok(url.to_string());
    }
    Url::parse(JSONLD_BASE_IRI)
        .expect("static JSON-LD base is valid")
        .join(id)
        .map(|url| url.to_string())
        .map_err(|error| CrateValidationError::Invalid(error.to_string()))
}

fn rewrite_value(
    value: &mut Value,
    targets: &HashMap<String, RewriteTarget>,
    warnings: &mut HashSet<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                if let Value::String(raw) = value
                    && targets.contains_key(raw)
                {
                    warnings.insert(raw.clone());
                }
                rewrite_value(value, targets, warnings);
            }
        }
        Value::Object(object) => {
            let original_id = object
                .get("@id")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(target) = original_id
                .as_deref()
                .and_then(|id| targets.get(id))
                .cloned()
            {
                object.insert("@id".to_string(), Value::String(target.w3id.clone()));
                if object.len() > 1 {
                    prepend_value(object, "localPath", Value::String(target.local_path));
                    prepend_value(object, "contentUrl", Value::String(target.hash_w3id));
                }
            }
            for (key, value) in object {
                if key == "localPath" {
                    continue;
                }
                if key != "@id"
                    && let Value::String(raw) = value
                    && targets.contains_key(raw)
                {
                    warnings.insert(raw.clone());
                }
                rewrite_value(value, targets, warnings);
            }
        }
        _ => {}
    }
}

fn prepend_value(object: &mut Map<String, Value>, key: &str, value: Value) {
    match object.remove(key) {
        None => {
            object.insert(key.to_string(), value);
        }
        Some(existing) if existing == value => {
            object.insert(key.to_string(), existing);
        }
        Some(Value::Array(mut values)) => {
            if !values.contains(&value) {
                values.insert(0, value);
            }
            object.insert(key.to_string(), Value::Array(values));
        }
        Some(existing) => {
            object.insert(key.to_string(), Value::Array(vec![value, existing]));
        }
    }
}

fn uses_v11(value: &Value) -> bool {
    value
        .get("@context")
        .is_some_and(|context| contains_string(context, "https://w3id.org/ro/crate/1.1/context"))
}

fn contains_string(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values.iter().any(|value| contains_string(value, expected)),
        _ => false,
    }
}

fn ensure_local_context(value: &mut Value) -> Result<(), CrateValidationError> {
    let object = value.as_object_mut().ok_or_else(|| {
        CrateValidationError::Invalid("RO-Crate document must be an object".to_string())
    })?;
    let mapping = json!({"localPath": LOCAL_PATH_IRI});
    match object.remove("@context") {
        Some(Value::Array(mut values)) => {
            if !values.iter().any(has_local_context) {
                values.push(mapping);
            }
            object.insert("@context".to_string(), Value::Array(values));
        }
        Some(Value::Object(mut context)) => {
            context.insert(
                "localPath".to_string(),
                Value::String(LOCAL_PATH_IRI.to_string()),
            );
            object.insert("@context".to_string(), Value::Object(context));
        }
        Some(context) => {
            object.insert("@context".to_string(), Value::Array(vec![context, mapping]));
        }
        None => {
            object.insert("@context".to_string(), mapping);
        }
    }
    Ok(())
}

fn has_local_context(value: &Value) -> bool {
    value
        .as_object()
        .and_then(|object| object.get("localPath"))
        .and_then(Value::as_str)
        == Some(LOCAL_PATH_IRI)
}

fn map_validation_error(error: RoCrateError) -> CrateValidationError {
    match error {
        RoCrateError::Update(UpdateError::ValidationFailed(violations)) => {
            CrateValidationError::Violations(violations.into_iter().map(validation_issue).collect())
        }
        other => CrateValidationError::Invalid(other.to_string()),
    }
}

fn validation_issue(violation: CrateViolation) -> ValidationIssue {
    ValidationIssue {
        code: violation.code.to_string(),
        message: violation.message,
        pointer: violation.pointer,
        entity_id: violation.entity_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crate_json(version: &str) -> String {
        json!({
            "@context": format!("https://w3id.org/ro/crate/{version}/context"),
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "about": {"@id": "./"},
                    "conformsTo": {"@id": format!("https://w3id.org/ro/crate/{version}")}
                },
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "name": "test",
                    "description": "test crate",
                    "datePublished": "2026-07-23",
                    "hasPart": {"@id": "data/a.txt"}
                },
                {
                    "@id": "data/a.txt",
                    "@type": "File",
                    "name": "a"
                }
            ]
        })
        .to_string()
    }

    #[test]
    fn finds_file_types() {
        let validated = validate_document(&crate_json("1.2")).unwrap();
        assert_eq!(validated.file_ids, vec!["data/a.txt"]);
    }

    #[test]
    fn rewrite_updates_refs() {
        let validated = validate_document(&crate_json("1.1")).unwrap();
        let target = RewriteTarget {
            w3id: "https://w3id.org/aruna/data/arn:example".to_string(),
            hash_w3id: format!("https://w3id.org/aruna/data/{}", "a".repeat(64)),
            local_path: "data/a.txt".to_string(),
        };
        let rewritten = rewrite_document(
            validated.value,
            &HashMap::from([("data/a.txt".to_string(), target)]),
        )
        .unwrap();
        assert!(rewritten.warnings.is_empty());
        let value: Value = serde_json::from_str(&rewritten.jsonld).unwrap();
        assert_eq!(
            value["@graph"][1]["hasPart"]["@id"],
            "https://w3id.org/aruna/data/arn:example"
        );
        assert_eq!(value["@graph"][2]["localPath"], "data/a.txt");
        assert!(
            value["@context"]
                .as_array()
                .unwrap()
                .contains(&json!({"localPath": LOCAL_PATH_IRI}))
        );
    }
}
