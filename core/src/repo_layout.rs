//! Repository layouts and the rules for files in plain RO-Crate commits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::structs::storage::data_identity::{
    CONTENT_URL, DataEntity, LOCAL_PATH, LOCAL_PATH_IRI, ObjectLocation, content_id,
    ensure_local_term, local_paths, normalized_id, text_values,
};
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
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

/// The repository path of an entity: its relative `@id`, or else its `localPath`.
pub fn entity_path(entity: &Value) -> Option<String> {
    entity["@id"].as_str().and_then(data_path).or_else(|| {
        local_paths(entity)
            .iter()
            .find_map(|path| data_path(&path_id(path)))
    })
}

/// Whether an entity is typed as RO-Crate `File`, an alias of schema.org MediaObject.
pub fn is_file(entity: &Value) -> bool {
    text_values(entity.get("@type")).iter().any(|kind| {
        let local = kind
            .trim_start_matches("http://schema.org/")
            .trim_start_matches("https://schema.org/");
        matches!(local, "File" | "MediaObject")
    })
}

fn data_paths(document: &Value) -> BTreeSet<String> {
    document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(entity_path)
        .collect()
}

/// The layout the metadata asks for: an ARC when the root is an ISA investigation or any
/// entity is an ISA study or assay by `additionalType`; otherwise a plain RO-Crate.
pub fn metadata_layout(document: &Value) -> Layout {
    let root = root_id(document);
    let marked = |entity: &Value, kinds: &[&str]| {
        text_values(entity.get("additionalType"))
            .iter()
            .any(|kind| {
                let local = kind
                    .trim_start_matches("http://schema.org/")
                    .trim_start_matches("https://schema.org/")
                    .trim_start_matches("schema:");
                kinds.contains(&local)
            })
    };
    let arc = document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|entity| {
            let is_root = root.is_some() && entity["@id"].as_str() == root.as_deref();
            (is_root && marked(entity, &["Investigation"])) || marked(entity, &["Study", "Assay"])
        });
    if arc { Layout::Arc } else { Layout::RoCrate }
}

/// Repository paths whose files must be stored: File entities that still name a relative
/// path, and stored entities whose file changed in the pushed commit.
pub fn unstored_paths(document: &Value, changed: &BTreeSet<String>) -> BTreeSet<String> {
    document["@graph"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entity| is_file(entity))
        .filter_map(|entity| {
            let relative = entity["@id"].as_str().and_then(data_path);
            let path = entity_path(entity)?;
            (relative.is_some() || changed.contains(&path)).then_some(path)
        })
        .filter(|path| !control_file(path))
        .collect()
}

/// Rewrites every entity whose repository path was stored to the normalized data entity
/// form, including references to it. Returns whether the graph changed.
pub fn name_stored(document: &mut Value, stored: &BTreeMap<String, StoredFile>) -> bool {
    let Some(graph) = document["@graph"].as_array_mut() else {
        return false;
    };
    let mut used: BTreeSet<String> = graph
        .iter()
        .filter_map(|entity| entity["@id"].as_str().map(str::to_owned))
        .collect();
    let mut renamed = BTreeMap::new();
    let mut changed = false;
    for entity in graph.iter_mut() {
        let Some(path) = entity_path(entity).filter(|_| is_file(entity)) else {
            continue;
        };
        let (Some(file), Some(object)) = (stored.get(&path), entity.as_object_mut()) else {
            continue;
        };
        let old = object.get("@id").and_then(Value::as_str).map(str::to_owned);
        let content = content_id(file.hash);
        // Keeps the entity's own address when it already names this content.
        let id = if old.as_deref() == Some(content.as_str()) {
            content
        } else {
            if let Some(old) = &old {
                used.remove(old);
            }
            normalized_id(file.hash, &file.location, &mut used)
        };
        let before = object.clone();
        object.remove(LOCAL_PATH_IRI);
        DataEntity {
            id: id.clone(),
            location: file.location.clone(),
            local_path: Some(path),
            size: Some(file.size),
            encoding_format: None,
        }
        .apply(object);
        changed |= *object != before;
        if let Some(old) = old.filter(|old| *old != id) {
            // References may spell a relative path differently, so its canonical form counts.
            if let Some(path) = data_path(&old) {
                renamed.insert(path_id(&path), id.clone());
            }
            renamed.insert(old, id);
        }
    }
    rename_ids(document, &renamed);
    if changed {
        ensure_local_term(document);
    }
    changed
}

/// The Git copy of a graph: stored entities get their repository path as `@id` and lose
/// the Aruna-only `contentUrl` and `localPath`. `paths` maps entity ids to paths.
pub fn git_copy(document: &mut Value, paths: &BTreeMap<String, String>) {
    let mut renamed = BTreeMap::new();
    for entity in document["@graph"].as_array_mut().into_iter().flatten() {
        let Some(id) = entity["@id"].as_str().map(str::to_owned) else {
            continue;
        };
        let (Some(path), Some(object)) = (paths.get(&id), entity.as_object_mut()) else {
            continue;
        };
        object.remove(CONTENT_URL);
        object.remove(LOCAL_PATH);
        object.remove(LOCAL_PATH_IRI);
        renamed.insert(id, path_id(path));
    }
    rename_ids(document, &renamed);
}

/// The one stable text of a metadata file: two-space indentation, a trailing newline, keys
/// sorted with `@` keys first, and the descriptor, the root, then entities by `@id`. So one
/// changed value is a small line diff.
pub fn metadata_text(document: &Value) -> Vec<u8> {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut keys: Vec<&String> = object.keys().collect();
                keys.sort();
                let mut map = serde_json::Map::new();
                for key in keys {
                    map.insert(key.clone(), sorted(&object[key]));
                }
                Value::Object(map)
            }
            Value::Array(values) => Value::Array(values.iter().map(sorted).collect()),
            value => value.clone(),
        }
    }
    let mut value = sorted(document);
    let root = root_id(document);
    if let Some(graph) = value.get_mut("@graph").and_then(Value::as_array_mut) {
        let rank = |entity: &Value| {
            let id = entity["@id"].as_str();
            let place = if id == Some(CRATE_FILE) {
                0
            } else if root.is_some() && id == root.as_deref() {
                1
            } else {
                2
            };
            (place, id.map(str::to_owned), entity.to_string())
        };
        graph.sort_by_cached_key(rank);
    }
    let mut text = serde_json::to_vec_pretty(&value).unwrap_or_default();
    text.push(b'\n');
    text
}

/// Replaces every `@id` value found in `renamed`, in entities and in references alike.
fn rename_ids(value: &mut Value, renamed: &BTreeMap<String, String>) {
    match value {
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| rename_ids(value, renamed)),
        Value::Object(object) => {
            for (key, value) in object.iter_mut() {
                match value {
                    Value::String(id) if key == "@id" => {
                        let canonical = data_path(id).map(|path| path_id(&path));
                        let new = renamed
                            .get(id.as_str())
                            .or_else(|| canonical.and_then(|path| renamed.get(&path)));
                        if let Some(new) = new {
                            *id = new.clone();
                        }
                    }
                    _ => rename_ids(value, renamed),
                }
            }
        }
        _ => {}
    }
}

/// A repository file stored as an Aruna object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredFile {
    pub hash: [u8; 32],
    pub location: ObjectLocation,
    pub size: u64,
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

    fn stored(key: &str, seed: u8) -> StoredFile {
        StoredFile {
            hash: [seed; 32],
            location: ObjectLocation {
                bucket: "datasets-g".into(),
                key: key.into(),
            },
            size: 4,
        }
    }

    #[test]
    fn layout_from_markers() {
        let plain = document(json!([]), Vec::new());
        assert_eq!(metadata_layout(&plain), Layout::RoCrate);
        let mut investigation = plain.clone();
        investigation["@graph"][1]["additionalType"] = json!("Investigation");
        assert_eq!(metadata_layout(&investigation), Layout::Arc);
        let study = json!({"@id": "#s", "@type": "Dataset", "additionalType": ["schema:Study"]});
        assert_eq!(
            metadata_layout(&document(json!([]), vec![study])),
            Layout::Arc
        );
        // Only the root may make the dataset an investigation.
        let nested = json!({"@id": "#i", "@type": "Dataset", "additionalType": "Investigation"});
        assert_eq!(
            metadata_layout(&document(json!([]), vec![nested])),
            Layout::RoCrate
        );
    }

    #[test]
    fn renames_every_spelling() {
        let files = vec![
            json!({"@id": "data/a.csv", "@type": "File"}),
            json!({"@id": "./data/b.csv", "@type": "File"}),
        ];
        let parts = json!([{"@id": "./data/a.csv"}, {"@id": "data/b.csv"}]);
        let mut crate_value = document(parts, files);
        let map = BTreeMap::from([
            ("data/a.csv".to_string(), stored("doc/data/a.csv", 1)),
            ("data/b.csv".to_string(), stored("doc/data/b.csv", 2)),
        ]);
        assert!(name_stored(&mut crate_value, &map));
        assert_eq!(
            crate_value["@graph"][1]["hasPart"],
            json!([{"@id": content_id([1; 32])}, {"@id": content_id([2; 32])}])
        );
        let context = crate_value["@context"].to_string();
        assert!(
            context.contains(LOCAL_PATH_IRI),
            "localPath term in {context}"
        );
    }

    #[test]
    fn reads_path_iri() {
        let entity = json!({"@id": content_id([1; 32]), "@type": "File",
            "contentUrl": "s3://datasets-g/doc/data/a.csv", LOCAL_PATH_IRI: "data/a.csv"});
        assert_eq!(entity_path(&entity).as_deref(), Some("data/a.csv"));
        let mut crate_value = document(json!([{"@id": content_id([1; 32])}]), vec![entity]);
        git_copy(
            &mut crate_value,
            &BTreeMap::from([(content_id([1; 32]), "data/a.csv".to_string())]),
        );
        let copied = &crate_value["@graph"][2];
        assert_eq!(copied["@id"], "data/a.csv");
        assert!(copied.get(LOCAL_PATH_IRI).is_none() && copied.get("contentUrl").is_none());
    }

    #[test]
    fn names_stored_files() {
        let files = vec![
            json!({"@id": "data/a.csv", "@type": "File", "name": "a.csv"}),
            json!({"@id": "data/b.csv", "@type": "File"}),
            json!({"@id": "data/c.csv", "@type": "File"}),
            json!({"@id": "#person", "@type": "Person"}),
        ];
        let parts = json!([{"@id": "data/a.csv"}, {"@id": "data/b.csv"}, {"@id": "data/c.csv"}]);
        let mut crate_value = document(parts, files);
        let unstored = unstored_paths(&crate_value, &BTreeSet::new());
        let paths: Vec<&str> = unstored.iter().map(String::as_str).collect();
        assert_eq!(paths, ["data/a.csv", "data/b.csv", "data/c.csv"]);
        // Two files with the same content: the second keeps its `s3://` URL as `@id`.
        let map = BTreeMap::from([
            ("data/a.csv".to_string(), stored("doc/data/a.csv", 1)),
            ("data/b.csv".to_string(), stored("doc/data/b.csv", 1)),
        ]);
        assert!(name_stored(&mut crate_value, &map));
        let graph = crate_value["@graph"].as_array().unwrap();
        let first = &graph[2];
        assert_eq!(first["@id"], content_id([1; 32]));
        assert_eq!(first["contentUrl"], "s3://datasets-g/doc/data/a.csv");
        assert_eq!(first["localPath"], "data/a.csv");
        assert_eq!(first["name"], "a.csv");
        assert_eq!(graph[3]["@id"], "s3://datasets-g/doc/data/b.csv");
        assert_eq!(graph[4]["@id"], "data/c.csv");
        assert_eq!(
            graph[1]["hasPart"],
            json!([{"@id": content_id([1; 32])}, {"@id": "s3://datasets-g/doc/data/b.csv"},
                {"@id": "data/c.csv"}])
        );
        assert!(!name_stored(&mut crate_value, &map), "naming is stable");
        let unstored = unstored_paths(&crate_value, &BTreeSet::from(["data/a.csv".into()]));
        assert_eq!(
            unstored,
            BTreeSet::from(["data/a.csv".into(), "data/c.csv".into()])
        );
        // Changed content gets the new content address.
        let map = BTreeMap::from([("data/a.csv".to_string(), stored("doc/data/a.csv", 2))]);
        assert!(name_stored(&mut crate_value, &map));
        assert_eq!(crate_value["@graph"][2]["@id"], content_id([2; 32]));
        assert_eq!(
            entity_path(&crate_value["@graph"][2]).as_deref(),
            Some("data/a.csv")
        );
        let text = crate_value.to_string();
        validate(Some(text.as_bytes())).expect("normalized crate stays valid");
    }

    #[test]
    fn copies_to_git() {
        let file = json!({"@id": content_id([1; 32]), "@type": "File", "name": "a b.csv",
            "contentUrl": "s3://datasets-g/doc/data/a b.csv", "localPath": "data/a b.csv"});
        let mut crate_value = document(json!([{"@id": content_id([1; 32])}]), vec![file]);
        let paths = BTreeMap::from([(content_id([1; 32]), "data/a b.csv".to_string())]);
        git_copy(&mut crate_value, &paths);
        assert_eq!(
            crate_value["@graph"][2],
            json!({"@id": "data/a%20b.csv", "@type": "File", "name": "a b.csv"})
        );
        assert_eq!(
            crate_value["@graph"][1]["hasPart"],
            json!([{"@id": "data/a%20b.csv"}])
        );
    }

    #[test]
    fn stable_metadata_text() {
        let person = json!({"name": "Ada", "@type": "Person", "@id": "#ada"});
        let mut first = document(json!([]), vec![person]);
        first["@graph"][1]["variableMeasured"] = json!(["depth"]);
        let mut shuffled = first.clone();
        shuffled["@graph"].as_array_mut().unwrap().reverse();
        let text = metadata_text(&first);
        assert_eq!(text, metadata_text(&shuffled));
        assert!(text.ends_with(b"}\n"));
        let lines = String::from_utf8(text).unwrap();
        assert!(lines.starts_with("{\n  \"@context\""));
        let ada = lines.find("\"@id\": \"#ada\"").unwrap();
        assert!(lines.find("\"@id\": \"./\"").unwrap() < ada);
        assert!(ada < lines[ada..].find("\"name\"").unwrap() + ada);
        // One added value changes a few lines, not the whole file.
        let mut second = first.clone();
        second["@graph"][1]["variableMeasured"] = json!(["depth", "stuff"]);
        let after = String::from_utf8(metadata_text(&second)).unwrap();
        let before: BTreeSet<&str> = lines.lines().collect();
        let added = after.lines().filter(|line| !before.contains(line)).count();
        assert!(added <= 2, "{added} lines changed");
        assert_eq!(after.lines().count(), lines.lines().count() + 1);
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
        // A stored entity names its file through `localPath`.
        let stored = json!({"@id": content_id([1; 32]), "@type": "File",
            "localPath": "data/a.csv"});
        let old = document(json!([{"@id": content_id([1; 32])}]), vec![stored]);
        let error = keeps_entities(&old, &new, &with_file).expect_err("file stays");
        assert!(matches!(error, CrateError::EntityRemoved(path) if path == "data/a.csv"));
    }
}
