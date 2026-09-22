//! Maps repository records to RO-Crate while retaining the complete source metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use ulid::Ulid;

mod credential;
pub use credential::InvenioCredential;
mod projection;
pub use projection::normalize_metadata;

/// Reference binding config keys: the file a reference reads and the group of its connector.
pub const REFERENCE_RECORD: &str = "record_id";
pub const REFERENCE_FILE: &str = "file_key";
pub const REFERENCE_GROUP: &str = "group_id";

const NATIVE_METADATA: &str = "https://w3id.org/aruna/invenio/metadata";
const CUSTOM_FIELDS: &str = "https://w3id.org/aruna/invenio/customFields";
const PUBLICATION_DATE: &str = "https://w3id.org/aruna/invenio/publicationDate";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvenioQuery {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub q: String,
    pub page: u32,
    pub size: u8,
    pub all_versions: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvenioMode {
    #[default]
    Copy,
    Reference,
    Metadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioOptions {
    pub mode: InvenioMode,
    pub all_versions: bool,
}

impl Default for InvenioOptions {
    fn default() -> Self {
        Self {
            mode: InvenioMode::Copy,
            all_versions: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioDestination {
    pub group_id: Ulid,
    pub connector_id: Ulid,
    pub draft_id: Option<String>,
    pub new_version: Option<String>,
    pub metadata_json: String,
    pub publish: bool,
    pub public_files: bool,
    pub credential: Option<InvenioCredential>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvenioRecord {
    pub id: String,
    pub url: String,
    pub published: bool,
    pub parent_id: String,
    pub revision_id: u64,
    pub doi: Option<String>,
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
    entity[NATIVE_METADATA] = json!(metadata.to_string());
    if record["custom_fields"].is_object() {
        entity[CUSTOM_FIELDS] = json!(record["custom_fields"].to_string());
    }
    let mut properties = Vec::new();
    native_properties(metadata, "metadata", &mut properties);
    native_properties(&record["custom_fields"], "custom_fields", &mut properties);
    entity["additionalProperty"] = Value::Array(properties);
    for (source, target) in [
        ("description", "description"),
        ("version", "version"),
        ("publisher", "publisher"),
    ] {
        if !metadata[source].is_null() {
            entity[target] = metadata[source].clone();
        }
    }
    if let Some(date) = metadata["publication_date"].as_str() {
        entity["datePublished"] = json!(publication_start(date)?);
        entity[PUBLICATION_DATE] = json!(date);
    }
    if let Some(subjects) = metadata["subjects"].as_array() {
        entity["keywords"] = Value::Array(
            subjects
                .iter()
                .filter_map(|subject| subject["subject"].as_str().map(|value| json!(value)))
                .collect(),
        );
    }
    for (source, target) in [("created", "dateCreated"), ("updated", "dateModified")] {
        if let Some(value) = record[source].as_str() {
            entity[target] = json!(value);
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
                "familyName": person["family_name"],
                "identifier": person["identifiers"].as_array().map(|ids| ids.iter().map(|identifier| json!({
                    "@type": "PropertyValue", "propertyID": identifier["scheme"], "value": identifier["identifier"]
                })).collect::<Vec<_>>()).unwrap_or_default(),
                "affiliation": creator["affiliations"].as_array().map(|affiliations| affiliations.iter().map(|affiliation| json!({
                    "@type": "Organization", "name": affiliation["name"], "identifier": affiliation["id"]
                })).collect::<Vec<_>>()).unwrap_or_default()
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
        let source = format!("{endpoint}records/{id}");
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

/// Supplied native fields override mapped crate fields; missing mandatory fields fail closed.
pub fn export_metadata(document: &Value, overrides: &Value) -> Result<Value, InvenioError> {
    let mut metadata = json!({"resource_type": {"id": "dataset"}, "rights": []});
    let graph = document["@graph"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if let Some(root) = crate_root(document) {
        for (source, target) in [
            ("name", "title"),
            ("description", "description"),
            ("version", "version"),
            ("publisher", "publisher"),
        ] {
            if let Some(value) = schema_value(root, source).as_str() {
                metadata[target] = json!(value);
            }
        }
        if let Some(date) = schema_value(root, "datePublished").as_str() {
            let date = date.split('T').next().unwrap_or(date);
            let original = root[PUBLICATION_DATE]
                .as_str()
                .filter(|original| publication_start(original).is_ok_and(|start| start == date));
            metadata["publication_date"] = json!(original.unwrap_or(date));
        }
        let creators = schema_value(root, "creator");
        let creators = if creators.is_null() {
            schema_value(root, "author")
        } else {
            creators
        };
        let expected_creators = values(creators).len();
        let creators = values(creators)
            .iter()
            .filter_map(|creator| {
                let creator = creator["@id"]
                    .as_str()
                    .and_then(|id| graph.iter().find(|entity| entity["@id"] == id))
                    .unwrap_or(creator);
                let family = schema_value(creator, "familyName").as_str();
                let name = schema_value(creator, "name").as_str();
                let organizational = values(&creator["@type"]).iter().any(|kind| {
                    kind.as_str().is_some_and(|kind| {
                        matches!(
                            kind,
                            "Organization"
                                | "schema:Organization"
                                | "http://schema.org/Organization"
                                | "https://schema.org/Organization"
                        )
                    })
                });
                let mut person = if organizational {
                    json!({"type": "organizational", "name": name?})
                } else {
                    let family = family.or(name)?;
                    let mut person = json!({"type": "personal", "family_name": family});
                    if let Some(given) = schema_value(creator, "givenName").as_str() {
                        person["given_name"] = json!(given);
                    }
                    person
                };
                person["identifiers"] = Value::Array(
                    values(schema_value(creator, "identifier"))
                        .iter()
                        .filter_map(identifier)
                        .collect(),
                );
                let affiliations = values(schema_value(creator, "affiliation"))
                    .iter()
                    .filter_map(|value| {
                        let value = value["@id"]
                            .as_str()
                            .and_then(|id| graph.iter().find(|entity| entity["@id"] == id))
                            .unwrap_or(value);
                        let name = schema_value(value, "name").as_str()?;
                        Some(json!({"name": name}))
                    })
                    .collect::<Vec<_>>();
                Some(json!({"person_or_org": person, "affiliations": affiliations}))
            })
            .collect::<Vec<_>>();
        if !creators.is_empty() && creators.len() == expected_creators {
            metadata["creators"] = Value::Array(creators);
        }
        let identifiers = values(schema_value(root, "identifier"))
            .iter()
            .filter_map(identifier)
            .map(|mut id| {
                id["relation_type"] = json!({"id": "isderivedfrom"});
                id
            })
            .collect::<Vec<_>>();
        if !identifiers.is_empty() {
            metadata["related_identifiers"] = Value::Array(identifiers);
        }
        let subjects = values(schema_value(root, "keywords"))
            .iter()
            .filter_map(Value::as_str)
            .map(|subject| json!({"subject": subject}))
            .collect::<Vec<_>>();
        if !subjects.is_empty() {
            metadata["subjects"] = Value::Array(subjects);
        }
        metadata["rights"] = Value::Array(
            values(schema_value(root, "license"))
                .iter()
                .filter_map(|license| {
                    let text = license.as_str().or_else(|| license["@id"].as_str())?;
                    let mut right = json!({"title": {"en": text}});
                    if text.starts_with("https://") || text.starts_with("http://") {
                        right["link"] = json!(text);
                    }
                    Some(right)
                })
                .collect(),
        );
    }
    if let Some(root) = crate_root(document)
        && let Some(native) = root[NATIVE_METADATA].as_str()
    {
        let native: Value =
            serde_json::from_str(native).map_err(|_| InvenioError("invalid native metadata"))?;
        let baseline = record_entity(&json!({"metadata": native}), "./")?;
        for (key, field) in [("description", "description"), ("rights", "license")] {
            if native.get(key).is_none() && schema_value(root, field) == &baseline[field] {
                if let Some(metadata) = metadata.as_object_mut() {
                    metadata.remove(key);
                }
            }
        }
        for (key, value) in native
            .as_object()
            .ok_or(InvenioError("native metadata must be an object"))?
        {
            let field = match key.as_str() {
                "title" => Some("name"),
                "description" => Some("description"),
                "publication_date" => Some("datePublished"),
                "version" => Some("version"),
                "publisher" => Some("publisher"),
                "creators" => Some("creator"),
                "subjects" => Some("keywords"),
                "rights" => Some("license"),
                "related_identifiers" => continue,
                _ => None,
            };
            if field.is_none_or(|field| schema_value(root, field) == &baseline[field]) {
                metadata[key] = value.clone();
            }
        }
        let mut related = native["related_identifiers"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for value in values(&metadata["related_identifiers"]) {
            if !related.contains(value) {
                related.push(value.clone());
            }
        }
        if !related.is_empty() {
            metadata["related_identifiers"] = Value::Array(related);
        }
    }
    normalize_metadata(&mut metadata, None);
    if let Some(overrides) = overrides.as_object() {
        for (key, value) in overrides {
            metadata[key] = value.clone();
        }
    } else if !overrides.is_null() {
        return Err(InvenioError("metadata overrides must be an object"));
    }
    validate_metadata(&metadata)?;
    Ok(metadata)
}

/// Returns native descriptive fields without copying source ownership, access settings or managed PIDs.
pub fn export_fields(document: &Value, overrides: &Value) -> Result<Value, InvenioError> {
    let mut result =
        json!({"metadata": export_metadata(document, overrides)?, "custom_fields": {}});
    if let Some(fields) = crate_root(document).and_then(|root| root[CUSTOM_FIELDS].as_str()) {
        let fields: Value =
            serde_json::from_str(fields).map_err(|_| InvenioError("invalid custom fields"))?;
        if !fields.is_object() {
            return Err(InvenioError("custom fields must be an object"));
        }
        result["custom_fields"] = fields;
    }
    Ok(result)
}

fn crate_root(document: &Value) -> Option<&Value> {
    let graph = document["@graph"].as_array()?;
    let id = graph
        .iter()
        .find(|entity| {
            entity["@id"].as_str().is_some_and(|id| {
                id == "ro-crate-metadata.json" || id.ends_with("/ro-crate-metadata.json")
            })
        })
        .and_then(|entity| schema_value(entity, "about")["@id"].as_str())
        .unwrap_or("./");
    graph.iter().find(|entity| entity["@id"] == id)
}

fn native_properties(value: &Value, path: &str, result: &mut Vec<Value>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let key = key.replace('~', "~0").replace('/', "~1");
                native_properties(value, &format!("{path}/{key}"), result);
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                native_properties(value, &format!("{path}/{index}"), result);
            }
        }
        Value::Null => {}
        value => result.push(json!({"@type": "PropertyValue", "propertyID": path, "value": value})),
    }
}

fn schema_value<'a>(entity: &'a Value, name: &str) -> &'a Value {
    for key in [
        name.to_string(),
        format!("schema:{name}"),
        format!("http://schema.org/{name}"),
        format!("https://schema.org/{name}"),
    ] {
        if let Some(value) = entity.get(key) {
            return value;
        }
    }
    &Value::Null
}

/// Uses the earliest day represented by an EDTF date; the source precision is retained separately.
fn publication_start(value: &str) -> Result<String, InvenioError> {
    let mut start = None;
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() > 2 {
        return Err(InvenioError("invalid publication interval"));
    }
    for part in parts {
        let full = match part.len() {
            4 => format!("{part}-01-01"),
            7 => format!("{part}-01"),
            10 => part.to_string(),
            _ => return Err(InvenioError("invalid publication date")),
        };
        let date = chrono::NaiveDate::parse_from_str(&full, "%Y-%m-%d")
            .map_err(|_| InvenioError("invalid publication date"))?;
        if start.is_none() {
            start = Some(date.to_string());
        }
    }
    start.ok_or(InvenioError("missing publication date"))
}

fn values(value: &Value) -> &[Value] {
    match value {
        Value::Null => &[],
        Value::Array(values) => values,
        value => std::slice::from_ref(value),
    }
}

fn identifier(value: &Value) -> Option<Value> {
    let scheme = schema_value(value, "propertyID").as_str();
    let text = schema_value(value, "value")
        .as_str()
        .or_else(|| value.as_str())
        .or_else(|| value["@id"].as_str())?;
    let (scheme, text) = if let Some(doi) = text
        .strip_prefix("https://doi.org/")
        .or_else(|| text.strip_prefix("http://doi.org/"))
    {
        ("doi", doi)
    } else if let Some(scheme) = scheme {
        (scheme, text)
    } else if text.starts_with("https://") || text.starts_with("http://") {
        ("url", text)
    } else {
        return None;
    };
    Some(json!({"scheme": scheme, "identifier": text}))
}

#[cfg(test)]
mod tests;
