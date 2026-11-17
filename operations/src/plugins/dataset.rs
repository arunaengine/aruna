//! Reads and changes the dataset entity of an RO-Crate as plugins see it: plain fields, JSON
//! merge patches, file entities and the crate a plugin create stores.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_plugins::broker::{ErrorKind, HostError};
use serde_json::Value;

use super::unavailable;

/// The type a JSON literal comes back with from the stored graph.
const JSON_LITERAL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON";

/// The dataset entity of a crate, the one its metadata descriptor is about.
fn dataset_mut(crate_json: &mut Value) -> Result<&mut Value, HostError> {
    let graph = (crate_json.get_mut("@graph").and_then(Value::as_array_mut))
        .ok_or_else(|| unavailable("the stored crate has no graph"))?;
    let id_of = |entity: &Value| {
        entity
            .get("@id")
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    let root = (graph.iter())
        .find(|entity| id_of(entity).as_deref() == Some("ro-crate-metadata.json"))
        .and_then(|descriptor| {
            descriptor
                .get("about")?
                .get("@id")?
                .as_str()
                .map(str::to_string)
        })
        .unwrap_or_else(|| "./".to_string());
    (graph.iter_mut())
        .find(|entity| id_of(entity).as_deref() == Some(root.as_str()))
        .ok_or_else(|| HostError::new(ErrorKind::Invalid, "the crate has no dataset entity"))
}

/// The fields of a crate's dataset entity, as a plugin reads and patches them.
pub(crate) fn dataset_of(jsonld: &str) -> Result<String, HostError> {
    let mut crate_json: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored crate is not JSON"))?;
    let dataset = dataset_mut(&mut crate_json)?;
    plain_objects(dataset);
    Ok(dataset.to_string())
}

/// The change a patch makes to a crate's dataset entity: its id, types and name, and each
/// patched field with its value after a JSON merge patch, `null` removing one. Other fields and
/// other entities are not part of it, so they stay as they are.
pub(crate) fn dataset_change(jsonld: &str, patch: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let patch: Value = serde_json::from_str(patch).map_err(|_| invalid("the patch is not JSON"))?;
    let Value::Object(changes) = patch else {
        return Err(invalid("the patch is not a JSON object"));
    };
    if changes.contains_key("@id") {
        return Err(invalid("a patch cannot change the dataset's @id"));
    }
    let mut crate_json: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored crate is not JSON"))?;
    let dataset = dataset_mut(&mut crate_json)?;
    plain_objects(dataset);
    let mut merged = dataset.clone();
    let fields: Vec<String> = changes.keys().cloned().collect();
    merge_into(&mut merged, Value::Object(changes));
    let mut change = serde_json::Map::new();
    for field in ["@id", "@type", "name"]
        .map(str::to_string)
        .into_iter()
        .chain(fields)
    {
        let value = merged.get(&field).cloned().unwrap_or(Value::Null);
        change.insert(field, value);
    }
    let mut change = Value::Object(change);
    literal_objects(&mut change);
    Ok(change.to_string())
}

/// The `File` entities of an exported page, each as the plain JSON object a plugin writes.
pub(crate) fn file_entities(jsonld: &str) -> Result<Vec<String>, HostError> {
    let page: Value =
        serde_json::from_str(jsonld).map_err(|_| unavailable("the stored page is not JSON"))?;
    let graph = (page.get("@graph").and_then(Value::as_array))
        .ok_or_else(|| unavailable("the stored page has no graph"))?;
    // RO-Crate's `File` is `schema:MediaObject`; an export may name it by either term.
    let named = |kind: &Value| kind == "File" || kind == "MediaObject";
    let file = |entity: &&Value| match entity.get("@type") {
        Some(Value::Array(kinds)) => kinds.iter().any(named),
        Some(kind) => named(kind),
        None => false,
    };
    let files = graph.iter().filter(file).cloned().map(|mut entity| {
        plain_objects(&mut entity);
        entity.to_string()
    });
    Ok(files.collect())
}

/// A file entity as a change stores it: one object with an `@id` that is neither the root nor
/// the descriptor, typed `File`, naming other entities by `@id` alone.
pub(crate) fn file_entity(file: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let file: Value = serde_json::from_str(file).map_err(|_| invalid("the file is not JSON"))?;
    let Value::Object(fields) = &file else {
        return Err(invalid("the file is not a JSON object"));
    };
    let id = fields
        .get("@id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if matches!(id, "" | "./" | "ro-crate-metadata.json") {
        return Err(invalid("the file needs its own @id"));
    }
    let named = |kind: &Value| kind == "File" || kind == "MediaObject";
    let typed = match fields.get("@type") {
        Some(Value::Array(kinds)) => kinds.iter().any(named),
        Some(kind) => named(kind),
        None => false,
    };
    if !typed || fields.contains_key("@graph") || nested_entity(fields) {
        return Err(invalid("the file is one File entity naming others by @id"));
    }
    let mut file = file;
    literal_objects(&mut file);
    Ok(file.to_string())
}

/// Whether a field holds an entity of its own rather than a reference by `@id` alone.
fn nested_entity(fields: &serde_json::Map<String, Value>) -> bool {
    let entity = |value: &Value| {
        (value.as_object()).is_some_and(|object| {
            let reference = object.len() == 1 && object.contains_key("@id");
            object.keys().any(|key| key.starts_with('@')) && !reference
        })
    };
    fields.values().any(|value| match value {
        Value::Array(entries) => entries.iter().any(entity),
        value => entity(value),
    })
}

/// The crate a plugin create stores: the metadata descriptor and the dataset entity as its root.
/// Plain object values become JSON literals as in a patch; nested entities are refused.
pub(crate) fn dataset_crate(dataset: &str) -> Result<String, HostError> {
    let invalid = |message: &str| HostError::new(ErrorKind::Invalid, message);
    let dataset: Value =
        serde_json::from_str(dataset).map_err(|_| invalid("the dataset is not JSON"))?;
    let Value::Object(mut fields) = dataset else {
        return Err(invalid("the dataset is not a JSON object"));
    };
    if fields.get("@id").is_some_and(|id| id != "./") || fields.contains_key("@graph") {
        return Err(invalid("the dataset is one root entity without @graph"));
    }
    let kinds = fields
        .get("@type")
        .cloned()
        .unwrap_or(Value::from("Dataset"));
    let dataset_kind = match &kinds {
        Value::String(kind) => kind == "Dataset",
        Value::Array(kinds) => kinds.iter().any(|kind| kind == "Dataset"),
        _ => false,
    };
    if !dataset_kind {
        return Err(invalid("the dataset's @type is not Dataset"));
    }
    if nested_entity(&fields) {
        return Err(invalid("the dataset names other entities only by @id"));
    }
    fields.insert("@id".to_string(), Value::from("./"));
    fields.insert("@type".to_string(), kinds);
    let mut dataset = Value::Object(fields);
    literal_objects(&mut dataset);
    let descriptor = serde_json::json!({
        "@id": "ro-crate-metadata.json",
        "@type": "CreativeWork",
        "about": {"@id": "./"},
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
    });
    let graph = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [descriptor, dataset],
    });
    let graph = graph.to_string();
    craqle::validate_rocrate_jsonld(&graph).map_err(|error| invalid(&error.to_string()))?;
    Ok(graph)
}

/// Turns the dataset's JSON literals back into the objects a plugin wrote.
fn plain_objects(dataset: &mut Value) {
    let Value::Object(fields) = dataset else {
        return;
    };
    for value in fields.values_mut() {
        let literal = (value.get("@type").and_then(Value::as_str))
            .is_some_and(|kind| kind == "@json" || kind == JSON_LITERAL);
        let inner = value.get("@value").cloned();
        if let (true, Some(inner)) = (literal, inner) {
            *value = match inner {
                Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
                other => other,
            };
        }
    }
}

/// Applies a JSON merge patch to `target`.
pub(crate) fn merge_into(target: &mut Value, patch: Value) {
    let Value::Object(changes) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Default::default());
    }
    let Value::Object(fields) = target else {
        return;
    };
    for (key, value) in changes {
        match value {
            Value::Null => drop(fields.remove(&key)),
            value => merge_into(fields.entry(key).or_insert(Value::Null), value),
        }
    }
}

/// Stores each plain object value of the change, one without JSON-LD keywords, as a JSON
/// literal, so it stays one value instead of becoming an untyped node.
fn literal_objects(change: &mut Value) {
    let Value::Object(fields) = change else {
        return;
    };
    let plain = |value: &Value| {
        (value.as_object()).is_some_and(|object| !object.keys().any(|key| key.starts_with('@')))
    };
    let literal =
        |value: &Value| serde_json::json!({ "@type": JSON_LITERAL, "@value": value.to_string() });
    for value in fields.values_mut() {
        match value {
            Value::Array(entries) => {
                for entry in entries.iter_mut().filter(|entry| plain(entry)) {
                    *entry = literal(entry);
                }
            }
            entry if plain(entry) => *entry = literal(entry),
            _ => {}
        }
    }
}
