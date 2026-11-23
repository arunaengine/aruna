//! Separates native vocabulary references from their generated response fields.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::Value;

/// Without expected input, builds request metadata; otherwise drops only unrequested enrichment.
/// Expected references must have matching IDs. Free text and unknown fields remain unchanged.
pub fn normalize_metadata(value: &mut Value, expected: Option<&Value>) {
    normalize_value(value, expected, "");
}

fn normalize_value(value: &mut Value, expected: Option<&Value>, path: &str) {
    let reference = value["id"].as_str().is_some_and(|id| !id.trim().is_empty())
        && expected.is_none_or(|expected| expected["id"] == value["id"]);
    if reference {
        let generated: &[&str] = match path {
            "resource_type"
            | "creators[].role"
            | "contributors[].role"
            | "related_identifiers[].relation_type"
            | "related_identifiers[].resource_type"
            | "additional_titles[].type"
            | "additional_titles[].lang"
            | "additional_descriptions[].type"
            | "additional_descriptions[].lang"
            | "dates[].type"
            | "languages[]" => &["title"],
            "rights[]" => &["title", "description", "icon"],
            "creators[].affiliations[]" | "contributors[].affiliations[]" => {
                &["name", "identifiers"]
            }
            "subjects[]" => &["subject", "scheme"],
            "funding[].funder" => &["name"],
            "funding[].award" => &["title", "number", "identifiers", "acronym", "program"],
            _ => &[],
        };
        if let Some(object) = value.as_object_mut() {
            for key in generated {
                if expected.is_none_or(|expected| expected.get(*key).is_none()) {
                    object.remove(*key);
                }
            }
            if path == "rights[]" {
                if let Some(props) = object.get_mut("props").and_then(Value::as_object_mut) {
                    for key in ["url", "scheme"] {
                        if expected.is_none_or(|expected| expected["props"].get(key).is_none()) {
                            props.remove(key);
                        }
                    }
                }
                if object
                    .get("props")
                    .and_then(Value::as_object)
                    .is_some_and(serde_json::Map::is_empty)
                    && expected.is_none_or(|expected| expected.get("props").is_none())
                {
                    object.remove("props");
                }
            }
        }
    }
    if matches!(
        path,
        "creators[].person_or_org" | "contributors[].person_or_org"
    ) && value["type"] == "personal"
        && value["family_name"].is_string()
        && expected.is_none_or(|expected| {
            expected["type"] == value["type"]
                && expected["family_name"] == value["family_name"]
                && expected.get("name").is_none()
        })
        && let Some(object) = value.as_object_mut()
    {
        object.remove("name");
    }
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                normalize_value(
                    value,
                    expected.map(|expected| expected.get(key).unwrap_or(&Value::Null)),
                    &path,
                );
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                normalize_value(
                    value,
                    expected.map(|expected| expected.get(index).unwrap_or(&Value::Null)),
                    &format!("{path}[]"),
                );
            }
        }
        _ => {}
    }
}
