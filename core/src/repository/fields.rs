//! Reads the crate fields that repository mappings share, independent of any repository format.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde_json::Value;

use super::RepositoryError;

/// An identifier with its scheme, such as `doi`, `url` or `orcid`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identifier {
    pub scheme: String,
    pub value: String,
}

/// A creator entity with its person identifiers and named affiliations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Person {
    pub organization: bool,
    pub name: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    /// Only ORCID, GND, ISNI and ROR identifiers.
    pub identifiers: Vec<Identifier>,
    pub affiliations: Vec<String>,
}

/// The data entity the metadata descriptor is `about`, else `./`.
pub fn crate_root(document: &Value) -> Option<&Value> {
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

/// A schema.org property under its plain, `schema:` or full IRI key; `Null` when absent.
pub fn schema_value<'a>(entity: &'a Value, name: &str) -> &'a Value {
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

/// A property value as a list: `Null` is empty and a single value is one item.
pub fn values(value: &Value) -> &[Value] {
    match value {
        Value::Null => &[],
        Value::Array(values) => values,
        value => std::slice::from_ref(value),
    }
}

/// The graph entity a `{"@id": ...}` reference names; other values stay as they are.
pub fn entity<'a>(graph: &'a [Value], value: &'a Value) -> &'a Value {
    value["@id"]
        .as_str()
        .and_then(|id| graph.iter().find(|entity| entity["@id"] == id))
        .unwrap_or(value)
}

/// Reads a person or organization, following references into `graph`.
pub fn person(graph: &[Value], value: &Value) -> Person {
    let value = entity(graph, value);
    let text = |name| schema_value(value, name).as_str().map(str::to_string);
    let organization = values(&value["@type"]).iter().any(|kind| {
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
    Person {
        organization,
        name: text("name"),
        given_name: text("givenName"),
        family_name: text("familyName"),
        identifiers: values(schema_value(value, "identifier"))
            .iter()
            .filter_map(person_identifier)
            .collect(),
        affiliations: values(schema_value(value, "affiliation"))
            .iter()
            .filter_map(|affiliation| {
                let name = schema_value(entity(graph, affiliation), "name").as_str()?;
                Some(name.to_string())
            })
            .collect(),
    }
}

/// Keeps ORCID, GND, ISNI and ROR identifiers with a lower case scheme; URL forms are recognized.
pub fn person_identifier(value: &Value) -> Option<Identifier> {
    let id = identifier(value)?;
    let url_form = id
        .value
        .strip_prefix("https://")
        .or_else(|| id.value.strip_prefix("http://"))
        .and_then(|text| {
            [
                ("orcid", "orcid.org/"),
                ("gnd", "d-nb.info/gnd/"),
                ("isni", "isni.org/isni/"),
                ("ror", "ror.org/"),
            ]
            .into_iter()
            .find_map(|(scheme, prefix)| Some((scheme, text.strip_prefix(prefix)?)))
        });
    let (scheme, value) = match url_form {
        Some((scheme, value)) => (scheme, value.trim_end_matches('/')),
        None => (id.scheme.as_str(), id.value.as_str()),
    };
    let scheme = scheme.to_ascii_lowercase();
    matches!(scheme.as_str(), "orcid" | "gnd" | "isni" | "ror").then(|| Identifier {
        scheme,
        value: value.to_string(),
    })
}

/// A DOI URL is a `doi`, a `propertyID` names the scheme, and any other web address is a `url`.
pub fn identifier(value: &Value) -> Option<Identifier> {
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
    Some(Identifier {
        scheme: scheme.to_string(),
        value: text.to_string(),
    })
}

/// Each license of a property value as its text or the `@id` it references.
pub fn licenses(value: &Value) -> Vec<String> {
    values(value)
        .iter()
        .filter_map(|license| license.as_str().or_else(|| license["@id"].as_str()))
        .map(str::to_string)
        .collect()
}

/// The text keywords of a property value; other values are skipped.
pub fn keywords(value: &Value) -> Vec<String> {
    values(value)
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

/// Uses the earliest day represented by an EDTF date; the source precision is retained separately.
pub fn publication_start(value: &str) -> Result<String, RepositoryError> {
    let mut start = None;
    let parts = value.split('/').collect::<Vec<_>>();
    if parts.len() > 2 {
        return Err(RepositoryError("invalid publication interval"));
    }
    for part in parts {
        let full = match part.len() {
            4 => format!("{part}-01-01"),
            7 => format!("{part}-01"),
            10 => part.to_string(),
            _ => return Err(RepositoryError("invalid publication date")),
        };
        let date = chrono::NaiveDate::parse_from_str(&full, "%Y-%m-%d")
            .map_err(|_| RepositoryError("invalid publication date"))?;
        if start.is_none() {
            start = Some(date.to_string());
        }
    }
    start.ok_or(RepositoryError("missing publication date"))
}

#[cfg(test)]
#[path = "fields_tests.rs"]
mod tests;
