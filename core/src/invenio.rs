//! Maps repository records to RO-Crate while retaining the complete source metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioDestination {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub draft_id: Option<String>,
    pub metadata_json: String,
    pub publish: bool,
    pub public_files: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioRecord {
    pub id: String,
    pub url: String,
    pub published: bool,
}

#[derive(Debug, Error)]
#[error("invalid Invenio record: {0}")]
pub struct InvenioError(pub &'static str);

pub fn validate_id(id: &str) -> Result<(), InvenioError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(InvenioError("unsafe record identifier"));
    }
    Ok(())
}

pub fn record_id(record: &Value) -> Result<&str, InvenioError> {
    let id = record["id"].as_str().ok_or(InvenioError("missing id"))?;
    validate_id(id)?;
    Ok(id)
}

pub fn file_path(id: &str, key: &str) -> Result<String, InvenioError> {
    validate_id(id)?;
    if key.is_empty() {
        return Err(InvenioError("empty file key"));
    }
    Ok(format!(
        "versions/{id}/files/{}",
        hex::encode(key.as_bytes())
    ))
}

pub fn validate_metadata(metadata: &Value) -> Result<(), InvenioError> {
    if !metadata["title"]
        .as_str()
        .is_some_and(|value| !value.trim().is_empty())
        || !metadata["publication_date"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
        || !metadata["resource_type"]["id"].is_string()
        || !metadata["creators"]
            .as_array()
            .is_some_and(|value| !value.is_empty())
    {
        return Err(InvenioError(
            "title, publication_date, resource_type and creators are required",
        ));
    }
    Ok(())
}

/// Maps searchable fields; the companion JSON files retain every unmapped field.
pub fn record_entity(record: &Value, id: &str) -> Result<Value, InvenioError> {
    let metadata = &record["metadata"];
    let title = metadata["title"]
        .as_str()
        .ok_or(InvenioError("missing title"))?;
    let mut entity = json!({
        "@id": id, "@type": "Dataset", "name": title,
        "description": "Imported repository record"
    });
    for (source, target) in [
        ("description", "description"),
        ("publication_date", "datePublished"),
        ("version", "version"),
        ("publisher", "publisher"),
        ("subjects", "keywords"),
    ] {
        if !metadata[source].is_null() {
            entity[target] = metadata[source].clone();
        }
    }
    let mut identifiers = Vec::new();
    for pids in [&record["pids"], &record["parent"]["pids"]] {
        if let Some(pids) = pids.as_object() {
            for (scheme, pid) in pids {
                if let Some(value) = pid["identifier"].as_str() {
                    identifiers.push(json!({
                        "@type": "PropertyValue", "propertyID": scheme, "value": value
                    }));
                }
            }
        }
    }
    if let Some(ids) = metadata["identifiers"].as_array() {
        for identifier in ids {
            identifiers.push(json!({
                "@type": "PropertyValue", "propertyID": identifier["scheme"],
                "value": identifier["identifier"]
            }));
        }
    }
    entity["identifier"] = Value::Array(identifiers);
    if let Some(creators) = metadata["creators"].as_array() {
        entity["creator"] = Value::Array(creators.iter().map(|creator| {
            let person = &creator["person_or_org"];
            json!({
                "@type": if person["type"] == "organizational" { "Organization" } else { "Person" },
                "name": person["name"], "givenName": person["given_name"],
                "familyName": person["family_name"], "identifier": person["identifiers"]
            })
        }).collect());
    }
    if let Some(rights) = metadata["rights"].as_array() {
        entity["license"] = Value::Array(
            rights
                .iter()
                .filter_map(|right| {
                    right["link"]
                        .as_str()
                        .map(|link| json!({"@id": link}))
                        .or_else(|| right["id"].as_str().map(|id| json!(id)))
                })
                .collect(),
        );
    }
    Ok(entity)
}

/// Every version and its files remain distinct, even when filenames repeat.
pub fn import_crate(
    endpoint: &str,
    selected: &str,
    records: &[(Value, Value)],
) -> Result<Value, InvenioError> {
    let selected_record = records
        .iter()
        .find(|(record, _)| record["id"] == selected)
        .ok_or(InvenioError("requested record absent from history"))?;
    let mut root = record_entity(&selected_record.0, "./")?;
    let mut parts = Vec::new();
    let mut graph = vec![json!({
        "@id": "ro-crate-metadata.json", "@type": "CreativeWork",
        "about": {"@id": "./"},
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}
    })];
    for (record, files) in records {
        let id = record_id(record)?;
        let dataset_id = format!("versions/{id}/");
        let source = format!("{}records/{id}", endpoint.trim_end_matches("api/"));
        let mut dataset = record_entity(record, &dataset_id)?;
        dataset["isBasedOn"] = json!({"@id": source});
        let mut children = Vec::new();
        let provenance = format!("versions/{id}/invenio-record.json");
        children.push(json!({"@id": provenance}));
        graph.push(json!({
            "@id": provenance, "@type": "File", "encodingFormat": "application/json",
            "name": "Complete Invenio record and file metadata", "about": {"@id": dataset_id}
        }));
        let entries = files["entries"]
            .as_array()
            .ok_or(InvenioError("missing file entries"))?;
        for file in entries {
            let key = file["key"]
                .as_str()
                .ok_or(InvenioError("missing file key"))?;
            let path = file_path(id, key)?;
            children.push(json!({"@id": path}));
            graph.push(json!({
                "@id": path, "@type": "File", "name": key,
                "contentSize": file["size"], "encodingFormat": file["mimetype"],
                "identifier": file["file_id"], "isPartOf": {"@id": dataset_id}
            }));
        }
        dataset["hasPart"] = Value::Array(children);
        parts.push(json!({"@id": dataset_id}));
        graph.push(dataset);
    }
    root["hasPart"] = Value::Array(parts);
    graph.push(root);
    Ok(json!({"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": graph}))
}

#[cfg(test)]
mod tests;
