//! Merges the RO-Crate changes of a plain RO-Crate commit into the live metadata graph.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::repo_layout::{CRATE_FILE, root_id};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MergeError {
    #[error("the metadata graph is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("RO-Crate root Dataset is required")]
    MissingRoot,
}

type Properties = BTreeMap<String, Vec<Value>>;

fn listed(value: &Value) -> Vec<Value> {
    match value {
        Value::Null => Vec::new(),
        Value::Array(values) => values.clone(),
        value => vec![value.clone()],
    }
}

fn properties(entity: Option<&Map<String, Value>>) -> Properties {
    entity
        .into_iter()
        .flatten()
        .filter(|(key, _)| *key != "@id")
        .map(|(key, value)| (key.clone(), listed(value)))
        .collect()
}

fn entities(document: Option<&Value>) -> BTreeMap<String, &Map<String, Value>> {
    let graph = document.and_then(|document| document["@graph"].as_array());
    graph
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|entity| Some((entity.get("@id")?.as_str()?.to_owned(), entity)))
        .collect()
}

/// Applies the values changed from `base` to `new` onto the `graph` JSON-LD; ids match
/// exactly or with a leading `./`. Without `base`, the new values replace the graph's.
/// Returns the merged JSON-LD, or `None` when the graph stays the same.
pub fn merge(graph: &str, base: Option<&Value>, new: &Value) -> Result<Option<String>, MergeError> {
    let mut document: Value = serde_json::from_str(graph)?;
    let before = document.clone();
    let root = root_id(&document).ok_or(MergeError::MissingRoot)?;
    let (old, changed) = (entities(base), entities(Some(new)));
    let roots: BTreeSet<String> = base.into_iter().chain([new]).filter_map(root_id).collect();
    let mut nodes: Vec<Map<String, Value>> = match document["@graph"].take() {
        Value::Array(nodes) => nodes.into_iter().filter_map(|node| match node {
            Value::Object(node) => Some(node),
            _ => None,
        }),
        _ => return Err(MergeError::MissingRoot),
    }
    .collect();
    let position = |nodes: &[Map<String, Value>], id: &str| {
        nodes
            .iter()
            .position(|node| node.get("@id").and_then(Value::as_str) == Some(id))
    };
    if position(&nodes, &root).is_none() {
        return Err(MergeError::MissingRoot);
    }
    let target = |nodes: &[Map<String, Value>], id: &str| -> String {
        if roots.contains(id) {
            return root.clone();
        }
        let dotted = format!("./{id}");
        let bare = id.strip_prefix("./").unwrap_or(id);
        [id, dotted.as_str(), bare]
            .into_iter()
            .find(|candidate| position(nodes, candidate).is_some())
            .unwrap_or(id)
            .to_owned()
    };
    let ids: BTreeSet<&String> = old.keys().chain(changed.keys()).collect();
    let mut removed = BTreeSet::new();
    for id in ids {
        let (before_entity, after_entity) = (old.get(id).copied(), changed.get(id).copied());
        if id == CRATE_FILE || properties(before_entity) == properties(after_entity) {
            continue;
        }
        let into = target(&nodes, id);
        let Some(after_entity) = after_entity else {
            if into != root
                && let Some(index) = position(&nodes, &into)
            {
                nodes.remove(index);
                removed.insert(into);
            }
            continue;
        };
        let index = match position(&nodes, &into) {
            Some(index) => index,
            None => {
                nodes.push(Map::from_iter([("@id".to_owned(), Value::String(into))]));
                nodes.len() - 1
            }
        };
        let local = |value: &Value| match value["@id"].as_str() {
            Some(reference) if value.is_object() => {
                let mut value = value.clone();
                value["@id"] = Value::String(target(&nodes, reference));
                value
            }
            _ => value.clone(),
        };
        let (was, now) = (properties(before_entity), properties(Some(after_entity)));
        let names: BTreeSet<&String> = was.keys().chain(now.keys()).collect();
        let mut updates = Vec::new();
        for name in names {
            let current = listed(nodes[index].get(name).unwrap_or(&Value::Null));
            let previous: Vec<Value> = if base.is_none() && now.contains_key(name) {
                current.clone()
            } else {
                was.get(name).into_iter().flatten().map(local).collect()
            };
            let values: Vec<Value> = now.get(name).into_iter().flatten().map(local).collect();
            let same = previous.len() == values.len()
                && values.iter().all(|value| previous.contains(value))
                && previous.iter().all(|value| values.contains(value));
            if same {
                continue;
            }
            let mut kept: Vec<Value> = current
                .into_iter()
                .filter(|value| !previous.contains(value))
                .collect();
            for value in values {
                if !kept.contains(&value) {
                    kept.push(value);
                }
            }
            updates.push((name.clone(), kept));
        }
        let node = &mut nodes[index];
        for (name, mut kept) in updates {
            let array = node.get(&name).is_some_and(Value::is_array);
            match kept.len() {
                0 => node.remove(&name),
                1 if !array => node.insert(name, kept.remove(0)),
                _ => node.insert(name, Value::Array(kept)),
            };
        }
    }
    let dangling = |item: &Value| {
        item["@id"]
            .as_str()
            .is_some_and(|reference| removed.contains(reference))
    };
    for node in &mut nodes {
        let names: Vec<String> = node
            .iter()
            .filter(|(name, value)| *name != "@id" && listed(value).iter().any(dangling))
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            let array = node.get(&name).is_some_and(Value::is_array);
            let values = listed(node.get(&name).unwrap_or(&Value::Null));
            let mut kept: Vec<Value> = values.into_iter().filter(|item| !dangling(item)).collect();
            match kept.len() {
                0 => node.remove(&name),
                1 if !array => node.insert(name, kept.remove(0)),
                _ => node.insert(name, Value::Array(kept)),
            };
        }
    }
    document["@graph"] = Value::Array(nodes.into_iter().map(Value::Object).collect());
    Ok((document != before).then(|| document.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn crate_with(parts: Value, extra: Vec<Value>) -> Value {
        let mut graph = vec![
            json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                "about": {"@id": "./"}}),
            json!({"@id": "./", "@type": "Dataset", "name": "Plain", "hasPart": parts}),
        ];
        graph.extend(extra);
        json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
    }

    fn entity<'a>(document: &'a Value, id: &str) -> Option<&'a Value> {
        document["@graph"]
            .as_array()?
            .iter()
            .find(|entity| entity["@id"] == id)
    }

    #[test]
    fn adds_new_file() {
        let live = crate_with(json!([]), Vec::new());
        let base = crate_with(json!([]), Vec::new());
        let file = json!({"@id": "data/a.csv", "@type": "File", "name": "a.csv"});
        let new = crate_with(json!([{"@id": "data/a.csv"}]), vec![file.clone()]);
        let merged = merge(&live.to_string(), Some(&base), &new).unwrap();
        let merged: Value = serde_json::from_str(&merged.expect("graph changes")).unwrap();
        assert_eq!(entity(&merged, "data/a.csv"), Some(&file));
        assert_eq!(
            entity(&merged, "./").unwrap()["hasPart"],
            json!([{"@id": "data/a.csv"}])
        );
    }

    #[test]
    fn keeps_concurrent_edits() {
        let live = crate_with(json!([]), Vec::new());
        let mut live = live;
        live["@graph"][1]["description"] = json!("Edited through the API");
        let base = crate_with(json!([]), Vec::new());
        let mut new = base.clone();
        new["@graph"][1]["name"] = json!("Renamed");
        let merged = merge(&live.to_string(), Some(&base), &new)
            .unwrap()
            .unwrap();
        let merged: Value = serde_json::from_str(&merged).unwrap();
        let root = entity(&merged, "./").unwrap();
        assert_eq!(root["name"], "Renamed");
        assert_eq!(root["description"], "Edited through the API");
        assert_eq!(merge(&merged.to_string(), Some(&base), &new).unwrap(), None);
    }

    #[test]
    fn removes_dropped_entities() {
        let file = json!({"@id": "./data/a.csv", "@type": "File"});
        let live = crate_with(json!([{"@id": "./data/a.csv"}]), vec![file]);
        let file = json!({"@id": "data/a.csv", "@type": "File"});
        let base = crate_with(json!([{"@id": "data/a.csv"}]), vec![file]);
        let new = crate_with(json!([]), Vec::new());
        let merged = merge(&live.to_string(), Some(&base), &new)
            .unwrap()
            .unwrap();
        let merged: Value = serde_json::from_str(&merged).unwrap();
        assert_eq!(entity(&merged, "./data/a.csv"), None);
        assert_eq!(entity(&merged, "./").unwrap().get("hasPart"), None);
    }

    #[test]
    fn requires_graph_root() {
        let new = crate_with(json!([]), Vec::new());
        let graph = json!({"@graph": []}).to_string();
        assert!(matches!(
            merge(&graph, None, &new),
            Err(MergeError::MissingRoot)
        ));
    }
}
