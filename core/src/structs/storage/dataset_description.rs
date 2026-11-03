//! Reads what describes a bucket from its dataset: the root entity of the dataset's RO-Crate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::structs::identity::identifier::{Identifier, IdentifierTarget};

/// What a dataset's root entity says: `name`, `description`, `version`, `license` and the
/// `identifier` values that link to a web page. The field order is part of stored job records.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetDescription {
    pub title: Option<String>,
    pub description: Option<String>,
    pub version: Option<String>,
    /// A license URL or SPDX id.
    pub license: Option<String>,
    pub identifiers: Vec<Identifier>,
}

const DESCRIPTOR: &str = "ro-crate-metadata.json";
const DOI_RESOLVER: &str = "https://doi.org/";

/// Reads the root entity the metadata descriptor names. It must be a Dataset with a name.
pub fn describe(jsonld: &str) -> Result<DatasetDescription, String> {
    let document: Value = serde_json::from_str(jsonld)
        .map_err(|error| format!("the dataset is not JSON: {error}"))?;
    let graph = document["@graph"]
        .as_array()
        .ok_or("the dataset has no @graph")?;
    let entity = |id: &str| graph.iter().find(|entity| entity["@id"] == id);
    let root_id = entity(DESCRIPTOR)
        .and_then(|descriptor| id_of(&descriptor["about"]))
        .unwrap_or("./");
    let root = entity(root_id).ok_or("the dataset has no root entity")?;
    if !one_or_many(&root["@type"]).any(|kind| kind == "Dataset") {
        return Err("the dataset's root entity is not a Dataset".to_string());
    }
    let title = text(&root["name"]).ok_or("the dataset has no name")?;
    let license = one_or_many(&root["license"]).find_map(|value| match value {
        Value::Object(_) => id_of(value).map(str::to_string),
        value => text(value),
    });
    let identifiers = one_or_many(&root["identifier"])
        .filter_map(|value| match id_of(value).and_then(entity) {
            Some(linked) => identifier_of(linked),
            None => identifier_of(value),
        })
        .collect();
    Ok(DatasetDescription {
        title: Some(title),
        description: text(&root["description"]),
        version: text(&root["version"]),
        license,
        identifiers,
    })
}

fn one_or_many(value: &Value) -> impl Iterator<Item = &Value> {
    match value {
        Value::Array(values) => values.iter().collect::<Vec<_>>().into_iter(),
        Value::Null => Vec::new().into_iter(),
        value => vec![value].into_iter(),
    }
}

fn id_of(value: &Value) -> Option<&str> {
    value["@id"].as_str()
}

/// A plain value as text: a string, a number, a `@value` or the first of a list.
fn text(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(text) => text.trim().to_string(),
        Value::Number(number) => number.to_string(),
        Value::Array(values) => return values.iter().find_map(text),
        Value::Object(object) => return object.get("@value").and_then(text),
        _ => return None,
    };
    (!text.is_empty()).then_some(text)
}

/// One identifier with a web link: a URL, a DOI, or a `PropertyValue`. Others have no link.
fn identifier_of(value: &Value) -> Option<Identifier> {
    let (name, value_text, url) = match value {
        Value::Object(entity) if entity.contains_key("value") || entity.contains_key("url") => {
            let value_text = text(&value["value"]);
            let url = text(&value["url"]).or_else(|| id_of(value).map(str::to_string));
            let name = text(&value["name"])
                .or_else(|| text(&value["propertyID"]).filter(|name| !name.contains("://")));
            (name, value_text, url)
        }
        Value::Object(_) => (None, None, id_of(value).map(str::to_string)),
        value => (None, text(value), None),
    };
    let link = |text: &str| link_of(text);
    let (doi, url) = match url.as_deref().and_then(link) {
        Some(found) => found,
        None => value_text.as_deref().and_then(link)?,
    };
    let value_text = match (&value_text, &doi) {
        (Some(text), _) if !text.contains("://") => text.clone(),
        (_, Some(doi)) => doi.clone(),
        _ => url.clone(),
    };
    let name = name.unwrap_or_else(|| match doi {
        Some(_) => "DOI".to_string(),
        None => "Identifier".to_string(),
    });
    Some(Identifier {
        name,
        value: value_text,
        url,
        target: IdentifierTarget::Key,
    })
}

/// The DOI a text names, if any, and the web link of the text.
fn link_of(text: &str) -> Option<(Option<String>, String)> {
    let doi = [
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "doi:",
    ]
    .iter()
    .find_map(|prefix| text.strip_prefix(prefix))
    .or_else(|| text.starts_with("10.").then_some(text))
    .filter(|doi| doi.contains('/'));
    match doi {
        Some(doi) => Some((Some(doi.to_string()), format!("{DOI_RESOLVER}{doi}"))),
        None => matches!(text.split_once("://"), Some(("http" | "https", _)))
            .then(|| (None, text.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn crate_of(root: Value, more: Vec<Value>) -> String {
        let mut graph = vec![
            json!({"@id": DESCRIPTOR, "@type": "CreativeWork", "about": {"@id": "./"}}),
            root,
        ];
        graph.extend(more);
        json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph}).to_string()
    }

    #[test]
    fn root_read() {
        let jsonld = crate_of(
            json!({
                "@id": "./",
                "@type": ["Dataset"],
                "name": "Bakta database",
                "description": "Annotation database",
                "version": 6,
                "license": {"@id": "https://spdx.org/licenses/CC-BY-4.0"},
                "identifier": [
                    "https://doi.org/10.5281/zenodo.14916843",
                    {"@id": "#accession"},
                    {"@type": "PropertyValue", "propertyID": "ENA", "value": "PRJEB1",
                     "url": "https://www.ebi.ac.uk/ena/browser/view/PRJEB1"},
                    "local-name-only"
                ]
            }),
            vec![
                json!({"@id": "#accession", "@type": "PropertyValue", "name": "Zenodo",
                        "value": "doi:10.5281/zenodo.1"}),
            ],
        );
        let described = describe(&jsonld).unwrap();
        assert_eq!(described.title.as_deref(), Some("Bakta database"));
        assert_eq!(
            described.description.as_deref(),
            Some("Annotation database")
        );
        assert_eq!(described.version.as_deref(), Some("6"));
        assert_eq!(
            described.license.as_deref(),
            Some("https://spdx.org/licenses/CC-BY-4.0")
        );
        let found: Vec<_> = (described.identifiers.iter())
            .map(|found| {
                (
                    found.name.as_str(),
                    found.value.as_str(),
                    found.url.as_str(),
                )
            })
            .collect();
        assert_eq!(
            found,
            [
                (
                    "DOI",
                    "10.5281/zenodo.14916843",
                    "https://doi.org/10.5281/zenodo.14916843"
                ),
                (
                    "Zenodo",
                    "doi:10.5281/zenodo.1",
                    "https://doi.org/10.5281/zenodo.1"
                ),
                (
                    "ENA",
                    "PRJEB1",
                    "https://www.ebi.ac.uk/ena/browser/view/PRJEB1"
                ),
            ],
            "an identifier without a link is left out"
        );
    }

    #[test]
    fn spdx_license_read() {
        let root = json!({"@id": "./", "@type": "Dataset", "name": "x", "license": "MIT"});
        let described = describe(&crate_of(root, Vec::new())).unwrap();
        assert_eq!(described.license.as_deref(), Some("MIT"));
        assert_eq!(described.description, None);
    }

    #[test]
    fn root_refused() {
        let unnamed = json!({"@id": "./", "@type": "Dataset", "name": " "});
        assert!(describe(&crate_of(unnamed, Vec::new())).is_err());
        let file = json!({"@id": "./", "@type": "File", "name": "x"});
        assert!(describe(&crate_of(file, Vec::new())).is_err());
        let elsewhere = json!({"@id": "#other", "@type": "Dataset", "name": "x"});
        assert!(describe(&crate_of(elsewhere, Vec::new())).is_err());
        assert!(describe("[]").is_err());
        assert!(describe("not json").is_err());
    }
}
