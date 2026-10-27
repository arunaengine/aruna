//! Assembles one RO-Crate of a file's or dataset's provenance from the hop answers a caller may
//! read: versions from origin reads, runs with their crates as nested crates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Pure: the walker reads each hop with the caller's credential and feeds the answers here.
// A hop the caller may not read is named by its global id or the digest that referred to it.

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::structs::storage::data::identity::text_values;
use aruna_core::structs::storage::replication::W3idIdentifier;
use serde_json::{Map, Value, json};

const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const CRATE_SPEC: &str = "https://w3id.org/ro/crate/1.2";
/// Marks a folder as a nested crate (RO-Crate 1.2).
const NESTED_CRATE: &str = "https://w3id.org/ro/crate";

/// What a hop reads next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// An exact version id: a global id URL or a versioned object w3id.
    Version(String),
    Run(String),
}

/// One link to follow: the entity that refers to it, by which property, and the digest it
/// used, which names the hop if the caller may not read it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hop {
    pub target: Target,
    pub from: String,
    pub property: &'static str,
    pub digest: Option<String>,
}

/// A file written into the crate next to its metadata, such as a nested run crate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrateFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct ProvenanceCrate {
    entities: BTreeMap<String, Map<String, Value>>,
    /// The entity each asked id became, so a repeated id links without a second read.
    labels: BTreeMap<String, String>,
    parts: BTreeSet<String>,
    start: String,
    published: String,
    hidden: usize,
    stopped: Option<String>,
}

impl ProvenanceCrate {
    /// A crate about `start`, published at `published` (an ISO 8601 date-time).
    pub fn new(start: &str, published: &str) -> Self {
        Self {
            entities: BTreeMap::new(),
            labels: BTreeMap::new(),
            parts: BTreeSet::new(),
            start: start.to_string(),
            published: published.to_string(),
            hidden: 0,
            stopped: None,
        }
    }

    /// The first hop of a version start.
    pub fn version_start(&self) -> Hop {
        Hop {
            target: Target::Version(self.start.clone()),
            from: "./".to_string(),
            property: "mainEntity",
            digest: None,
        }
    }

    /// Links a hop whose target was read before; `false` when it still has to be read.
    pub fn known(&mut self, hop: &Hop) -> bool {
        let asked = asked(&hop.target);
        match self.labels.get(&asked).cloned() {
            Some(entity) => {
                self.link(hop, &entity);
                true
            }
            None => false,
        }
    }

    /// Records a version's origin answer; returns the versions and run it came from.
    pub fn version(&mut self, hop: &Hop, origin: &Value) -> Vec<Hop> {
        let asked = asked(&hop.target);
        let id = origin["global_id"].as_str().unwrap_or(&asked).to_string();
        self.labels.insert(asked.clone(), id.clone());
        let duplicate = self.labels.insert(id.clone(), id.clone()).is_some() && id != asked;
        self.link(hop, &id);
        if duplicate {
            return Vec::new();
        }
        let mut file = entity(&id, "File");
        file.insert("name".into(), json!(file_name(&asked)));
        if let Some(created) = origin["created_at"].as_str() {
            file.insert("dateCreated".into(), json!(created));
        }
        let mut identifiers: Vec<Value> = origin["content_id"]
            .as_str()
            .map(|content| json!(content))
            .into_iter()
            .collect();
        for found in origin["identifiers"].as_array().into_iter().flatten() {
            let (name, value) = (text(&found["name"]), text(&found["value"]));
            let key = format!("#identifier-{name}-{value}");
            let mut property = entity(&key, "PropertyValue");
            property.insert("name".into(), json!(name));
            property.insert("value".into(), json!(value));
            property.insert("url".into(), found["url"].clone());
            self.entities.insert(key.clone(), property);
            identifiers.push(json!({ "@id": key }));
        }
        if !identifiers.is_empty() {
            file.insert("identifier".into(), Value::Array(identifiers));
        }
        self.entities.insert(id.clone(), file);
        let mut next = Vec::new();
        let based_on = |target: &str| Hop {
            target: Target::Version(target.to_string()),
            from: id.clone(),
            property: "isBasedOn",
            digest: None,
        };
        let origin_body = &origin["origin"];
        match origin_body["kind"].as_str() {
            Some("copy") => next.push(based_on(&text(&origin_body["from"]))),
            Some("job") => next.push(Hop {
                target: Target::Run(text(&origin_body["run_id"])),
                from: id.clone(),
                property: "result",
                digest: None,
            }),
            Some("source") => self.source(&id, origin),
            _ => {}
        }
        if let Some(from) = origin["hop"]["from"].as_str() {
            next.push(based_on(from));
        }
        next
    }

    /// The external source and the database that list the version now.
    fn source(&mut self, id: &str, origin: &Value) {
        if let Some(url) = origin["source"]["url"].as_str() {
            self.add_ref(id, "isBasedOn", url);
        }
        if let Some(entry) = origin["database_entry"].as_str() {
            let key = format!("#database-{entry}");
            let mut database = entity(&key, "Dataset");
            database.insert("name".into(), json!("Database (current listing)"));
            database.insert("identifier".into(), json!(entry));
            self.entities.insert(key.clone(), database);
            self.add_ref(id, "isPartOf", &key);
        }
    }

    /// Records a run and its crate, written as a nested crate; returns the run's inputs.
    pub fn run(&mut self, hop: &Hop, run_crate: Option<&Value>) -> (Vec<Hop>, Option<CrateFile>) {
        let Target::Run(run_id) = &hop.target else {
            return (Vec::new(), None);
        };
        let key = format!("#run-{run_id}");
        self.labels.insert(asked(&hop.target), key.clone());
        let mut run = entity(&key, "CreateAction");
        run.insert("name".into(), json!(format!("Run {run_id}")));
        self.entities.insert(key.clone(), run);
        // A run is linked from the file it made, as that file's `result` side.
        self.add_ref(&key, "result", &hop.from);
        let Some(run_crate) = run_crate else {
            self.add_text(&key, "description", "The run record is not visible to you");
            return (Vec::new(), None);
        };
        let folder = format!("runs/{run_id}/");
        let mut nested = entity(&folder, "Dataset");
        nested.insert("name".into(), json!(format!("Run crate of {run_id}")));
        nested.insert("conformsTo".into(), json!({ "@id": NESTED_CRATE }));
        self.entities.insert(folder.clone(), nested);
        self.parts.insert(folder.clone());
        self.add_ref(&key, "subjectOf", &folder);
        let hops = inputs(run_crate)
            .into_iter()
            .filter_map(|(entity_id, exact)| match exact {
                Some(exact) => Some(Hop {
                    target: Target::Version(exact),
                    from: key.clone(),
                    property: "object",
                    digest: Some(entity_id),
                }),
                None => {
                    self.unversioned(&key, &entity_id);
                    None
                }
            })
            .collect();
        let bytes = serde_json::to_vec_pretty(run_crate).unwrap_or_default();
        let path = format!("{folder}ro-crate-metadata.json");
        (hops, Some(CrateFile { path, bytes }))
    }

    /// Records a dataset start: the dataset as a nested crate and its parts with exact ids.
    pub fn dataset(&mut self, document: &Value) -> (Vec<Hop>, CrateFile) {
        let folder = "dataset/".to_string();
        let mut nested = entity(&folder, "Dataset");
        nested.insert("name".into(), json!("The dataset"));
        nested.insert("conformsTo".into(), json!({ "@id": NESTED_CRATE }));
        self.entities.insert(folder.clone(), nested);
        self.parts.insert(folder.clone());
        self.add_ref("./", "mainEntity", &folder);
        let mut hops = Vec::new();
        for node in graph(document) {
            let id = text(&node["@id"]);
            let exact = exact_ids(node).into_iter().next();
            match exact {
                Some(exact) if is_file(node) => hops.push(Hop {
                    target: Target::Version(exact),
                    from: folder.clone(),
                    property: "hasPart",
                    digest: Some(id),
                }),
                None if is_file(node) => self.unversioned(&folder, &id),
                _ => {}
            }
        }
        let bytes = serde_json::to_vec_pretty(document).unwrap_or_default();
        let path = format!("{folder}ro-crate-metadata.json");
        (hops, CrateFile { path, bytes })
    }

    /// A hop the caller may not read: named by its global id or the referring digest only.
    pub fn hidden(&mut self, hop: &Hop) {
        let asked = asked(&hop.target);
        let global = matches!(W3idIdentifier::parse(&asked), Ok(W3idIdentifier::Global(_)));
        let id = match (&hop.target, global, &hop.digest) {
            (Target::Run(run_id), _, _) => format!("#run-{run_id}"),
            (_, true, _) => asked.clone(),
            (_, false, Some(digest)) if !is_location(digest) => digest.clone(),
            _ => {
                self.hidden += 1;
                format!("#hidden-{}", self.hidden)
            }
        };
        self.labels.insert(asked, id.clone());
        let mut file = entity(&id, "File");
        file.insert("name".into(), json!("Not visible to you"));
        self.entities.entry(id.clone()).or_insert(file);
        self.link(hop, &id);
    }

    /// A hop whose node did not answer; the crate is then incomplete.
    pub fn unreachable(&mut self, hop: &Hop) {
        let asked = asked(&hop.target);
        self.labels.insert(asked.clone(), asked.clone());
        let mut file = entity(&asked, "File");
        file.insert(
            "description".into(),
            json!("The node holding it did not answer"),
        );
        self.entities.entry(asked.clone()).or_insert(file);
        self.link(hop, &asked);
        self.stopped
            .get_or_insert_with(|| "some nodes did not answer".to_string());
    }

    /// Ends the walk early; the crate says it is incomplete and why.
    pub fn stop(&mut self, reason: &str) {
        self.stopped = Some(reason.to_string());
    }

    /// The root `ro-crate-metadata.json`.
    pub fn finish(self) -> Value {
        let mut root = entity("./", "Dataset");
        root.insert(
            "name".into(),
            json!(format!("Provenance of {}", self.start)),
        );
        root.insert(
            "description".into(),
            json!("Where the data came from, as far as the exporting user may read it."),
        );
        root.insert("datePublished".into(), json!(self.published));
        let status = match &self.stopped {
            None => "Complete".to_string(),
            Some(reason) => format!("Incomplete: {reason}"),
        };
        root.insert("creativeWorkStatus".into(), json!(status));
        let parts: Vec<Value> = (self.parts.iter()).map(|id| json!({ "@id": id })).collect();
        root.insert("hasPart".into(), Value::Array(parts));
        let mut entities = self.entities;
        if let Some(own) = entities.remove("./") {
            for (key, value) in own.into_iter().filter(|(key, _)| key != "@id") {
                root.insert(key, value);
            }
        }
        let descriptor = json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": { "@id": CRATE_SPEC },
            "about": { "@id": "./" }
        });
        let mut graph = vec![descriptor, Value::Object(root)];
        graph.extend(entities.into_values().map(Value::Object));
        json!({ "@context": CRATE_CONTEXT, "@graph": graph })
    }

    fn unversioned(&mut self, from: &str, id: &str) {
        let mut file = entity(id, "File");
        file.insert("description".into(), json!("No exact version recorded"));
        self.entities.entry(id.to_string()).or_insert(file);
        let property = if from.starts_with("#run-") {
            "object"
        } else {
            "hasPart"
        };
        self.add_ref(from, property, id);
    }

    fn link(&mut self, hop: &Hop, to: &str) {
        // A run names the file it made from its own side.
        if matches!(hop.target, Target::Run(_)) {
            return;
        }
        self.add_ref(&hop.from, hop.property, to);
    }

    fn add_ref(&mut self, from: &str, property: &str, to: &str) {
        let source = self
            .entities
            .entry(from.to_string())
            .or_insert_with(|| entity(from, "Dataset"));
        let reference = json!({ "@id": to });
        match source.get_mut(property) {
            Some(Value::Array(values)) if !values.contains(&reference) => values.push(reference),
            Some(_) => {}
            None => {
                source.insert(property.to_string(), json!([reference]));
            }
        }
    }

    fn add_text(&mut self, id: &str, property: &str, value: &str) {
        if let Some(target) = self.entities.get_mut(id) {
            target.insert(property.to_string(), json!(value));
        }
    }
}

fn entity(id: &str, kind: &str) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("@id".into(), json!(id));
    map.insert("@type".into(), json!(kind));
    map
}

fn asked(target: &Target) -> String {
    match target {
        Target::Version(id) => id.clone(),
        Target::Run(run_id) => format!("run:{run_id}"),
    }
}

fn text(value: &Value) -> String {
    value.as_str().unwrap_or_default().to_string()
}

/// A location id names a bucket and key, which a hidden hop must not show.
fn is_location(id: &str) -> bool {
    !matches!(
        W3idIdentifier::parse(id),
        Ok(W3idIdentifier::ContentHash(_) | W3idIdentifier::Global(_))
    )
}

fn file_name(asked: &str) -> String {
    match W3idIdentifier::parse(asked) {
        Ok(W3idIdentifier::VersionedObject(arn)) => {
            arn.key.rsplit('/').next().unwrap_or(&arn.key).to_string()
        }
        _ => asked.rsplit('/').next().unwrap_or(asked).to_string(),
    }
}

fn graph(document: &Value) -> impl Iterator<Item = &Value> {
    document["@graph"].as_array().into_iter().flatten()
}

fn is_file(node: &Value) -> bool {
    text_values(node.get("@type"))
        .iter()
        .any(|kind| kind == "File")
}

/// The exact version ids among an entity's `contentUrl` values.
fn exact_ids(node: &Value) -> Vec<String> {
    text_values(node.get("contentUrl"))
        .into_iter()
        .filter(|url| {
            matches!(
                W3idIdentifier::parse(url),
                Ok(W3idIdentifier::VersionedObject(_) | W3idIdentifier::Global(_))
            )
        })
        .collect()
}

/// The run's inputs: each `object` entity of its actions, with its exact version id if any.
fn inputs(run_crate: &Value) -> Vec<(String, Option<String>)> {
    let nodes: BTreeMap<String, &Value> = graph(run_crate)
        .map(|node| (text(&node["@id"]), node))
        .collect();
    let actions = graph(run_crate).filter(|node| {
        text_values(node.get("@type"))
            .iter()
            .any(|kind| kind == "CreateAction")
    });
    let mut found = Vec::new();
    for action in actions {
        for id in text_values(action.get("object")) {
            let exact = nodes
                .get(&id)
                .and_then(|node| exact_ids(node).into_iter().next());
            if !found.iter().any(|(known, _)| *known == id) {
                found.push((id, exact));
            }
        }
    }
    found
}

#[cfg(test)]
#[path = "provenance_tests.rs"]
mod tests;
