//! Maps repository records to RO-Crate while retaining the complete source metadata.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::fields::{
    crate_root, entity, identifier, keywords, licenses, person, publication_start, schema_value,
    values,
};
use super::{ExportIdentity, RepositoryError};
use crate::structs::secondary_id::{
    IdentifierOrigin, SecondaryIdKind, SecondaryIdentifier, normalize_doi,
};
use serde_json::{Value, json};

mod projection;
pub use projection::normalize_metadata;

/// Reference binding config keys: the file a reference reads and the group of its connector.
pub const REFERENCE_RECORD: &str = "record_id";
pub const REFERENCE_FILE: &str = "file_key";
pub const REFERENCE_GROUP: &str = "group_id";

const NATIVE_METADATA: &str = "https://w3id.org/aruna/invenio/metadata";
const CUSTOM_FIELDS: &str = "https://w3id.org/aruna/invenio/customFields";
const PUBLICATION_DATE: &str = "https://w3id.org/aruna/invenio/publicationDate";

/// Adds the registered DOIs to the crate root `identifier`, skipping DOIs it already names.
pub fn add_root_identifiers(document: &mut Value, identity: &ExportIdentity) {
    let Some(id) = crate_root(document).and_then(|root| root["@id"].as_str().map(str::to_string))
    else {
        return;
    };
    let Some(root) = document["@graph"]
        .as_array_mut()
        .and_then(|graph| graph.iter_mut().find(|entity| entity["@id"] == id.as_str()))
    else {
        return;
    };
    let key = [
        "identifier",
        "schema:identifier",
        "http://schema.org/identifier",
    ]
    .into_iter()
    .find(|key| root.get(*key).is_some())
    .unwrap_or("identifier");
    let mut current = values(&root[key]).to_vec();
    let known = current
        .iter()
        .filter_map(identifier)
        .filter(|id| id.scheme == "doi")
        .filter_map(|id| normalize_doi(&id.value).ok())
        .collect::<Vec<_>>();
    for doi in &identity.identifiers {
        if doi.kind == SecondaryIdKind::Doi && !known.contains(&doi.value) {
            current.push(json!({
                "@type": "PropertyValue", "propertyID": "doi",
                "value": format!("https://doi.org/{}", doi.value)
            }));
        }
    }
    root[key] = Value::Array(current);
}

pub fn validate_id(id: &str) -> Result<(), RepositoryError> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(RepositoryError("unsafe record identifier"));
    }
    Ok(())
}

pub fn record_id(record: &Value) -> Result<&str, RepositoryError> {
    let id = record["id"].as_str().ok_or(RepositoryError("missing id"))?;
    validate_id(id)?;
    Ok(id)
}

pub fn file_path(id: &str, key: &str) -> Result<String, RepositoryError> {
    validate_id(id)?;
    if key.is_empty() {
        return Err(RepositoryError("empty file key"));
    }
    Ok(format!(
        "versions/{id}/files/{}",
        hex::encode(key.as_bytes())
    ))
}

pub fn validate_metadata(metadata: &Value) -> Result<(), RepositoryError> {
    if !missing_fields(metadata).is_empty() {
        return Err(RepositoryError(
            "title, publication_date, resource_type and creators are required",
        ));
    }
    Ok(())
}

/// The mandatory repository fields that `metadata` lacks.
pub fn missing_fields(metadata: &Value) -> Vec<&'static str> {
    let text = |name: &str| {
        metadata[name]
            .as_str()
            .is_some_and(|v| !v.trim().is_empty())
    };
    [
        ("title", text("title")),
        ("publication_date", text("publication_date")),
        ("resource_type", metadata["resource_type"]["id"].is_string()),
        (
            "creators",
            metadata["creators"]
                .as_array()
                .is_some_and(|value| !value.is_empty()),
        ),
    ]
    .into_iter()
    .filter_map(|(name, present)| (!present).then_some(name))
    .collect()
}

/// The mandatory fields the mapped crate still lacks after `overrides`, before any draft exists.
pub fn missing_metadata(
    document: &Value,
    overrides: &Value,
    endpoint: &str,
) -> Result<Vec<&'static str>, RepositoryError> {
    let metadata = map_metadata(document, overrides, &ExportIdentity::default())?;
    let mut missing = missing_fields(&metadata);
    if lacks_publisher(&metadata, endpoint) {
        missing.push("publisher");
    }
    Ok(missing)
}

/// Zenodo sets `publisher` itself; other InvenioRDM instances need it to publish with a DOI.
pub fn requires_publisher(endpoint: &str) -> bool {
    let host = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split(['/', ':'])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    !matches!(host.as_str(), "zenodo.org" | "sandbox.zenodo.org")
}

fn lacks_publisher(metadata: &Value, endpoint: &str) -> bool {
    requires_publisher(endpoint)
        && !metadata["publisher"]
            .as_str()
            .is_some_and(|value| !value.trim().is_empty())
}

/// Maps searchable fields; the companion JSON files retain every unmapped field.
pub fn record_entity(record: &Value, id: &str) -> Result<Value, RepositoryError> {
    let metadata = &record["metadata"];
    let title = metadata["title"]
        .as_str()
        .ok_or(RepositoryError("missing title"))?;
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
) -> Result<Value, RepositoryError> {
    let selected_record = records
        .iter()
        .find(|(record, _)| record["id"] == selected)
        .ok_or(RepositoryError("requested record absent from history"))?;
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
            .ok_or(RepositoryError("missing file entries"))?;
        for file in entries {
            let key = file["key"]
                .as_str()
                .ok_or(RepositoryError("missing file key"))?;
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

/// The dataset crate after a pull: the root takes `latest`'s metadata and keeps its parts, and
/// each version in `added` becomes a new part with its files. Parts already present stay as they are.
pub fn pull_crate(
    current: &Value,
    endpoint: &str,
    latest: &Value,
    added: &[(Value, Value)],
) -> Result<Value, RepositoryError> {
    let root = crate_root(current).ok_or(RepositoryError("missing crate root"))?;
    let root_id = root["@id"]
        .as_str()
        .ok_or(RepositoryError("missing crate root"))?;
    let mut parts = values(&root["hasPart"]).to_vec();
    let mut graph = current["@graph"]
        .as_array()
        .ok_or(RepositoryError("missing crate graph"))?
        .clone();
    let known = graph
        .iter()
        .filter_map(|entity| entity["@id"].as_str().map(str::to_string))
        .collect::<std::collections::HashSet<_>>();
    if let Some((first, _)) = added.first() {
        let fresh = import_crate(endpoint, record_id(first)?, added)?;
        for entity in fresh["@graph"].as_array().into_iter().flatten() {
            match entity["@id"].as_str() {
                Some("./") => parts.extend(
                    values(&entity["hasPart"])
                        .iter()
                        .filter(|part| !parts.contains(part))
                        .cloned()
                        .collect::<Vec<_>>(),
                ),
                Some("ro-crate-metadata.json") => {}
                Some(id) if !known.contains(id) => graph.push(entity.clone()),
                _ => {}
            }
        }
    }
    let mut entity = record_entity(latest, root_id)?;
    entity["hasPart"] = Value::Array(parts);
    let slot = graph
        .iter_mut()
        .find(|candidate| candidate["@id"] == root_id)
        .ok_or(RepositoryError("missing crate root"))?;
    *slot = entity;
    Ok(json!({"@context": current["@context"].clone(), "@graph": graph}))
}

/// Version ids whose `versions/{id}/` part the crate already holds.
pub fn crate_versions(document: &Value) -> Vec<String> {
    document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entity| {
            let id = entity["@id"].as_str()?;
            // A stored crate names its parts relative to the root, such as `./versions/1/`.
            let id = id
                .strip_prefix("./")
                .unwrap_or(id)
                .strip_prefix("versions/")?;
            let id = id.strip_suffix('/')?;
            validate_id(id).ok().map(|_| id.to_string())
        })
        .collect()
}

/// The record's version DOI, concept DOI, id and parent id as secondary identifiers.
/// Values the repository does not provide or that fail validation are left out.
pub fn record_identifiers(
    endpoint: &str,
    record: &Value,
    origin: IdentifierOrigin,
) -> Vec<SecondaryIdentifier> {
    let doi = record["pids"]["doi"]["identifier"]
        .as_str()
        .or_else(|| record["doi"].as_str());
    build_identifiers(
        endpoint,
        origin,
        [
            doi,
            record["parent"]["pids"]["doi"]["identifier"].as_str(),
            record["id"].as_str(),
            record["parent"]["id"].as_str(),
        ],
    )
}

/// Takes the version DOI, concept DOI, record id and parent id in this order.
pub(crate) fn build_identifiers(
    endpoint: &str,
    origin: IdentifierOrigin,
    values: [Option<&str>; 4],
) -> Vec<SecondaryIdentifier> {
    let kinds = [
        SecondaryIdKind::Doi,
        SecondaryIdKind::Doi,
        SecondaryIdKind::InvenioRecord,
        SecondaryIdKind::InvenioParent,
    ];
    kinds
        .into_iter()
        .zip(values)
        .filter_map(|(kind, value)| {
            SecondaryIdentifier::new(kind, value?, Some(endpoint), origin).ok()
        })
        .collect()
}

/// Supplied native fields override mapped crate fields; missing mandatory fields fail closed.
/// Root identifiers become `isderivedfrom`, the dataset's own PID `isidenticalto`, and DOIs
/// this dataset published are left out.
pub fn export_metadata(
    document: &Value,
    overrides: &Value,
    identity: &ExportIdentity,
) -> Result<Value, RepositoryError> {
    let metadata = map_metadata(document, overrides, identity)?;
    validate_metadata(&metadata)?;
    Ok(metadata)
}

fn map_metadata(
    document: &Value,
    overrides: &Value,
    identity: &ExportIdentity,
) -> Result<Value, RepositoryError> {
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
        // A publisher entity or reference maps to its name.
        if let Some(publisher) = values(schema_value(root, "publisher")).first() {
            if let Some(name) = schema_value(entity(graph, publisher), "name").as_str() {
                metadata["publisher"] = json!(name);
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
                let creator = person(graph, creator);
                let mut person = if creator.organization {
                    json!({"type": "organizational", "name": creator.name?})
                } else {
                    let family = creator.family_name.or(creator.name)?;
                    let mut person = json!({"type": "personal", "family_name": family});
                    if let Some(given) = creator.given_name {
                        person["given_name"] = json!(given);
                    }
                    person
                };
                person["identifiers"] = Value::Array(
                    creator
                        .identifiers
                        .into_iter()
                        .map(|id| json!({"scheme": id.scheme, "identifier": id.value}))
                        .collect(),
                );
                let affiliations = creator
                    .affiliations
                    .into_iter()
                    .map(|name| json!({"name": name}))
                    .collect::<Vec<_>>();
                Some(json!({"person_or_org": person, "affiliations": affiliations}))
            })
            .collect::<Vec<_>>();
        if !creators.is_empty() && creators.len() == expected_creators {
            metadata["creators"] = Value::Array(creators);
        }
        let mut identifiers = values(schema_value(root, "identifier"))
            .iter()
            .filter_map(identifier)
            .filter(|id| id.scheme != "doi" || !identity.published_doi(&id.value))
            .map(|id| {
                let mut id = json!({"scheme": id.scheme, "identifier": id.value});
                let own = identity
                    .own
                    .iter()
                    .any(|own| id["identifier"] == own.as_str());
                let relation = if own {
                    "isidenticalto"
                } else {
                    "isderivedfrom"
                };
                id["relation_type"] = json!({"id": relation});
                id
            })
            .collect::<Vec<_>>();
        for own in &identity.own {
            if !identifiers
                .iter()
                .any(|id| id["identifier"] == own.as_str())
            {
                identifiers.push(json!({"scheme": "url", "identifier": own,
                    "relation_type": {"id": "isidenticalto"}}));
            }
        }
        for reference in &identity.references {
            identifiers.push(json!({"scheme": "url", "identifier": reference,
                "relation_type": {"id": "references"}}));
        }
        if !identifiers.is_empty() {
            metadata["related_identifiers"] = Value::Array(identifiers);
        }
        let subjects = keywords(root)
            .into_iter()
            .map(|subject| json!({"subject": subject}))
            .collect::<Vec<_>>();
        if !subjects.is_empty() {
            metadata["subjects"] = Value::Array(subjects);
        }
        metadata["rights"] = Value::Array(
            licenses(root)
                .into_iter()
                .map(|text| {
                    let mut right = json!({"title": {"en": text}});
                    if text.starts_with("https://") || text.starts_with("http://") {
                        right["link"] = json!(text);
                    }
                    right
                })
                .collect(),
        );
    }
    if let Some(root) = crate_root(document)
        && let Some(native) = root[NATIVE_METADATA].as_str()
    {
        let native: Value =
            serde_json::from_str(native).map_err(|_| RepositoryError("invalid native metadata"))?;
        let baseline = record_entity(&json!({"metadata": native}), "./")?;
        for (key, field) in [("description", "description"), ("rights", "license")] {
            if native.get(key).is_none()
                && schema_value(root, field) == &baseline[field]
                && let Some(metadata) = metadata.as_object_mut()
            {
                metadata.remove(key);
            }
        }
        for (key, value) in native
            .as_object()
            .ok_or(RepositoryError("native metadata must be an object"))?
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
        return Err(RepositoryError("metadata overrides must be an object"));
    }
    Ok(metadata)
}

/// Returns native descriptive fields without copying source ownership, access settings or managed PIDs.
pub fn export_fields(
    document: &Value,
    overrides: &Value,
    identity: &ExportIdentity,
    endpoint: &str,
) -> Result<Value, RepositoryError> {
    let metadata = export_metadata(document, overrides, identity)?;
    if lacks_publisher(&metadata, endpoint) {
        return Err(RepositoryError("publisher is required by this repository"));
    }
    let mut result = json!({"metadata": metadata, "custom_fields": {}});
    if let Some(fields) = crate_root(document).and_then(|root| root[CUSTOM_FIELDS].as_str()) {
        let fields: Value =
            serde_json::from_str(fields).map_err(|_| RepositoryError("invalid custom fields"))?;
        if !fields.is_object() {
            return Err(RepositoryError("custom fields must be an object"));
        }
        result["custom_fields"] = fields;
    }
    Ok(result)
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

#[cfg(test)]
mod tests;
