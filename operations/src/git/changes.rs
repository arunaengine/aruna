//! Compares RO-Crate documents by entity and property and finds conflicting changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::PropertyConflict;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityChangeKind {
    Added,
    Removed,
    Changed,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PropertyChange {
    pub name: String,
    pub before: Vec<Value>,
    pub after: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityChange {
    pub id: String,
    pub change: EntityChangeKind,
    pub properties: Vec<PropertyChange>,
}

fn listed(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values.clone(),
        Value::Null => Vec::new(),
        value => vec![value.clone()],
    }
}

/// Values compared as a set, so reordering alone is no change.
fn same(first: &[Value], second: &[Value]) -> bool {
    let set =
        |values: &[Value]| -> BTreeSet<String> { values.iter().map(Value::to_string).collect() };
    set(first) == set(second)
}

type Properties = BTreeMap<String, Vec<Value>>;

fn entities(document: &Value) -> BTreeMap<String, Properties> {
    document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entity| {
            let id = entity["@id"].as_str()?;
            let properties = entity
                .as_object()?
                .iter()
                .filter(|(key, _)| *key != "@id")
                .map(|(key, value)| (key.clone(), listed(value)))
                .collect();
            Some((id.to_string(), properties))
        })
        .collect()
}

/// Entity and property differences between two RO-Crate documents.
pub fn entity_changes(before: &Value, after: &Value) -> Vec<EntityChange> {
    let (before, after) = (entities(before), entities(after));
    let ids: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    let empty = Properties::new();
    ids.into_iter()
        .filter_map(|id| {
            let (old, new) = (before.get(id), after.get(id));
            let change = match (old, new) {
                (None, _) => EntityChangeKind::Added,
                (_, None) => EntityChangeKind::Removed,
                _ => EntityChangeKind::Changed,
            };
            let (old, new) = (old.unwrap_or(&empty), new.unwrap_or(&empty));
            let names: BTreeSet<&String> = old.keys().chain(new.keys()).collect();
            let properties: Vec<PropertyChange> = names
                .into_iter()
                .filter_map(|name| {
                    let before = old.get(name).cloned().unwrap_or_default();
                    let after = new.get(name).cloned().unwrap_or_default();
                    (!same(&before, &after)).then(|| PropertyChange {
                        name: name.clone(),
                        before,
                        after,
                    })
                })
                .collect();
            (!properties.is_empty()).then(|| EntityChange {
                id: id.clone(),
                change,
                properties,
            })
        })
        .collect()
}

/// Properties both `source` and `target` changed from `base` to different values.
pub fn property_conflicts(base: &Value, source: &Value, target: &Value) -> Vec<PropertyConflict> {
    let changed = |side: &Value| -> BTreeMap<(String, String), Vec<Value>> {
        entity_changes(base, side)
            .into_iter()
            .flat_map(|entity| {
                let id = entity.id;
                entity
                    .properties
                    .into_iter()
                    .map(move |property| ((id.clone(), property.name), property.after))
            })
            .collect()
    };
    let (theirs, ours) = (changed(source), changed(target));
    theirs
        .into_iter()
        .filter_map(|(key, source)| {
            let target = ours.get(&key)?;
            (!same(&source, target)).then(|| PropertyConflict {
                entity: key.0,
                property: key.1,
                source,
                target: target.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn crate_with(entities: Value) -> Value {
        json!({ "@context": "https://w3id.org/ro/crate/1.2/context", "@graph": entities })
    }

    #[test]
    fn entity_diff() {
        let before = crate_with(json!([
            {"@id": "./", "name": "Study", "hasPart": [{"@id": "a"}, {"@id": "b"}]},
            {"@id": "#Person_Ada", "@type": "Person", "affiliation": "Old"},
            {"@id": "gone", "@type": "File"}
        ]));
        let after = crate_with(json!([
            {"@id": "./", "name": "Study", "hasPart": [{"@id": "b"}, {"@id": "a"}]},
            {"@id": "#Person_Ada", "@type": ["Person"], "affiliation": "New"},
            {"@id": "added", "@type": "File"}
        ]));
        let changes = entity_changes(&before, &after);
        let summary: Vec<_> = changes
            .iter()
            .map(|entity| (entity.id.as_str(), entity.change, entity.properties.len()))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("#Person_Ada", EntityChangeKind::Changed, 1),
                ("added", EntityChangeKind::Added, 1),
                ("gone", EntityChangeKind::Removed, 1),
            ]
        );
        assert_eq!(changes[0].properties[0].name, "affiliation");
        assert_eq!(changes[0].properties[0].before, vec![json!("Old")]);
        assert_eq!(changes[0].properties[0].after, vec![json!("New")]);
    }

    #[test]
    fn merge_conflicts() {
        let base = crate_with(json!([{"@id": "./", "name": "A", "description": "D"}]));
        let source = crate_with(json!([{"@id": "./", "name": "B", "description": "E"}]));
        let target = crate_with(json!([{"@id": "./", "name": "C", "description": "E"}]));
        let conflicts = property_conflicts(&base, &source, &target);
        assert_eq!(conflicts.len(), 1, "equal changes do not conflict");
        assert_eq!(conflicts[0].entity, "./");
        assert_eq!(conflicts[0].property, "name");
        assert_eq!(conflicts[0].source, vec![json!("B")]);
        assert_eq!(conflicts[0].target, vec![json!("C")]);
        let untouched = crate_with(json!([{"@id": "./", "name": "A", "description": "F"}]));
        assert_eq!(property_conflicts(&base, &source, &untouched).len(), 1);
        assert!(property_conflicts(&base, &source, &base).is_empty());
    }
}
