//! Repository layouts and the rules for files in plain RO-Crate commits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use thiserror::Error;

/// The root workbook that makes a commit an ARC.
pub const INVESTIGATION: &str = "isa.investigation.xlsx";
pub const CRATE_FILE: &str = "ro-crate-metadata.json";
/// The full graph that ARC snapshots store next to the ISA files.
pub const ARUNA_FILE: &str = "aruna-metadata.json";

/// Characters a relative RO-Crate `@id` encodes; `/` keeps separating folders.
const PATH_ID: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Layout {
    Arc,
    RoCrate,
}

impl Layout {
    /// A tree with an ISA investigation at its root is an ARC; any other is a plain RO-Crate.
    pub fn detect<'a>(mut paths: impl Iterator<Item = &'a str>) -> Self {
        if paths.any(|path| path == INVESTIGATION) {
            Self::Arc
        } else {
            Self::RoCrate
        }
    }
}

#[derive(Debug, Error)]
pub enum CrateError {
    #[error("ro-crate-metadata.json is required at the repository root")]
    Missing,
    #[error("ro-crate-metadata.json is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ro-crate-metadata.json is not a valid RO-Crate: {0}")]
    Invalid(#[from] craqle::RoCrateError),
    #[error(
        "{0} is still in the repository, but its entity was removed from \
         ro-crate-metadata.json; remove the file as well or keep its entity"
    )]
    EntityRemoved(String),
}

/// A file of a commit tree with its content size; LFS files report the size of their content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataFile {
    pub path: String,
    pub size: u64,
}

/// Files that configure Git or hold Aruna metadata, which never become data entities.
pub fn control_file(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    matches!(path, CRATE_FILE | ARUNA_FILE) || matches!(name, ".gitattributes" | ".gitignore")
}

/// The repository path a relative `@id` names; URLs, fragments and folders name none.
pub fn data_path(id: &str) -> Option<String> {
    let id = id.strip_prefix("./").unwrap_or(id);
    let scheme = id
        .split('/')
        .next()
        .is_some_and(|first| first.contains(':'));
    if id.is_empty() || id.starts_with(['#', '/']) || id.ends_with('/') || scheme {
        return None;
    }
    let path = percent_decode_str(id).decode_utf8().ok()?;
    let unsafe_part = path
        .split('/')
        .any(|part| matches!(part, "" | "." | ".." | ".git"));
    (!unsafe_part && !path.contains(['\\', '\0'])).then(|| path.into_owned())
}

/// The relative `@id` of a repository path.
pub fn path_id(path: &str) -> String {
    utf8_percent_encode(path, PATH_ID).to_string()
}

/// Parses `ro-crate-metadata.json` and checks it with the RO-Crate rules of metadata writes.
pub fn validate(bytes: Option<&[u8]>) -> Result<Value, CrateError> {
    let bytes = bytes.ok_or(CrateError::Missing)?;
    let value: Value = serde_json::from_slice(bytes)?;
    craqle::validate_rocrate_jsonld(&String::from_utf8_lossy(bytes))?;
    Ok(value)
}

/// The root data entity the metadata descriptor is about.
pub fn root_id(document: &Value) -> Option<String> {
    let graph = document["@graph"].as_array()?;
    let descriptor = graph.iter().find(|entity| entity["@id"] == CRATE_FILE)?;
    let about = &descriptor["about"];
    about
        .as_str()
        .or_else(|| about["@id"].as_str())
        .map(str::to_owned)
}

fn data_paths(document: &Value) -> BTreeSet<String> {
    document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entity| entity["@id"].as_str())
        .filter_map(data_path)
        .collect()
}

/// Adds a `File` entity and a root `hasPart` link for each file no entity names yet.
/// Returns how many files were added.
pub fn add_files(document: &mut Value, files: &[DataFile]) -> usize {
    let known = data_paths(document);
    let root = root_id(document);
    let Some(graph) = document["@graph"].as_array_mut() else {
        return 0;
    };
    let mut added = Vec::new();
    for file in files {
        if control_file(&file.path) || known.contains(&file.path) {
            continue;
        }
        let id = path_id(&file.path);
        let name = file.path.rsplit('/').next().unwrap_or(&file.path);
        graph.push(json!({"@id": id, "@type": "File", "name": name,
            "contentSize": file.size.to_string()}));
        added.push(json!({"@id": id}));
    }
    let count = added.len();
    let entity = graph
        .iter_mut()
        .find(|entity| root.is_some() && entity["@id"].as_str() == root.as_deref());
    if let (Some(entity), false) = (entity, added.is_empty()) {
        let mut parts = match entity.get_mut("hasPart").map(Value::take) {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(parts)) => parts,
            Some(part) => vec![part],
        };
        parts.extend(added);
        entity["hasPart"] = Value::Array(parts);
    }
    count
}

/// Refuses an entity that `old` described, `new` dropped, and whose file is still in `paths`.
pub fn keeps_entities(
    old: &Value,
    new: &Value,
    paths: &BTreeSet<String>,
) -> Result<(), CrateError> {
    let kept = data_paths(new);
    match data_paths(old)
        .into_iter()
        .find(|path| !kept.contains(path) && paths.contains(path) && !control_file(path))
    {
        Some(path) => Err(CrateError::EntityRemoved(path)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(parts: Value, extra: Vec<Value>) -> Value {
        let mut graph = vec![
            json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
                "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}}),
            json!({"@id": "./", "@type": "Dataset", "name": "Plain", "description": "Plain crate",
                "datePublished": "2026-09-28", "hasPart": parts,
                "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"}}),
        ];
        graph.extend(extra);
        json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": graph})
    }

    fn file(path: &str, size: u64) -> DataFile {
        DataFile {
            path: path.into(),
            size,
        }
    }

    #[test]
    fn detects_by_workbook() {
        let arc = ["isa.investigation.xlsx", "studies/s/isa.study.xlsx"];
        assert_eq!(Layout::detect(arc.into_iter()), Layout::Arc);
        let nested = ["ro-crate-metadata.json", "copy/isa.investigation.xlsx"];
        assert_eq!(Layout::detect(nested.into_iter()), Layout::RoCrate);
        assert_eq!(serde_json::to_value(Layout::RoCrate).unwrap(), "rocrate");
        assert_eq!(serde_json::to_value(Layout::Arc).unwrap(), "arc");
    }

    #[test]
    fn maps_ids_paths() {
        assert_eq!(data_path("./data/a b.csv").as_deref(), Some("data/a b.csv"));
        assert_eq!(data_path("data/a%20b.csv").as_deref(), Some("data/a b.csv"));
        assert_eq!(path_id("data/a b#1.csv"), "data/a%20b%231.csv");
        for id in [
            "./",
            "data/",
            "#person",
            "https://example.org/a",
            "arn:aruna:x",
            "../a",
            "a/.git/config",
            "/abs",
        ] {
            assert_eq!(data_path(id), None, "{id}");
        }
    }

    #[test]
    fn adds_unlisted_files() {
        let listed = json!({"@id": "data/a.csv", "@type": "File"});
        let mut crate_value = document(json!({"@id": "data/a.csv"}), vec![listed]);
        let files = [
            file("data/a.csv", 4),
            file("data/b c.csv", 12),
            file(".gitattributes", 1),
            file("sub/.gitignore", 1),
            file("ro-crate-metadata.json", 900),
            file("aruna-metadata.json", 900),
            file("LICENSE", 20),
        ];
        assert_eq!(add_files(&mut crate_value, &files), 2);
        let graph = crate_value["@graph"].as_array().unwrap();
        let added = graph
            .iter()
            .find(|entity| entity["@id"] == "data/b%20c.csv");
        let added = added.expect("new file is described");
        assert_eq!(added["@type"], "File");
        assert_eq!(added["name"], "b c.csv");
        assert_eq!(added["contentSize"], "12");
        assert!(graph.iter().any(|entity| entity["@id"] == "LICENSE"));
        assert_eq!(
            graph[1]["hasPart"],
            json!([{"@id": "data/a.csv"}, {"@id": "data/b%20c.csv"}, {"@id": "LICENSE"}])
        );
        let text = crate_value.to_string();
        validate(Some(text.as_bytes())).expect("files keep the crate valid");
        assert_eq!(add_files(&mut crate_value, &files), 0);
    }

    #[test]
    fn refuses_invalid_crates() {
        assert!(matches!(validate(None), Err(CrateError::Missing)));
        assert!(matches!(validate(Some(b"{")), Err(CrateError::Json(_))));
        let no_root = json!({"@context": "https://w3id.org/ro/crate/1.2/context", "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"}}]});
        let text = no_root.to_string();
        assert!(matches!(
            validate(Some(text.as_bytes())),
            Err(CrateError::Invalid(_))
        ));
    }

    #[test]
    fn keeps_listed_entities() {
        let entity = json!({"@id": "data/a.csv", "@type": "File"});
        let old = document(json!([{"@id": "data/a.csv"}]), vec![entity]);
        let new = document(json!([]), Vec::new());
        let with_file = BTreeSet::from(["data/a.csv".to_string()]);
        let error = keeps_entities(&old, &new, &with_file).expect_err("file stays");
        assert!(
            error
                .to_string()
                .starts_with("data/a.csv is still in the repository")
        );
        keeps_entities(&old, &new, &BTreeSet::new()).expect("file and entity removed");
        keeps_entities(&old, &old, &BTreeSet::new()).expect("only the file removed");
    }
}
