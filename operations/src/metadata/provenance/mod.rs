//! Assembles one RO-Crate of a file's or dataset's provenance from the hop answers a caller may
//! read: versions from origin reads, runs with their crates as nested crates.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The walker reads each hop with the caller's credential and feeds the answers here. A hop the
// caller may not read is named by its global id or the digest that referred to it. The working
// state is kept on disk, so an export holds no more in memory than the record it reads.

use std::collections::BTreeMap;
use std::io::Write;

use self::spool::{Spool, SpoolResult};
pub use self::spool::{scratch_dir, scratch_file, scratch_name};

use aruna_core::structs::storage::data::identity::text_values;
use aruna_core::structs::storage::replication::W3idIdentifier;
use serde_json::{Map, Value, json};

const CRATE_CONTEXT: &str = "https://w3id.org/ro/crate/1.2/context";
const CRATE_SPEC: &str = "https://w3id.org/ro/crate/1.2";
/// Marks a folder as a nested crate (RO-Crate 1.2).
const NESTED_CRATE: &str = "https://w3id.org/ro/crate";

/// Marks an entity whose release membership could not be checked everywhere.
pub const RELEASE_MEMBERSHIP: &str = "https://w3id.org/aruna/terms/release-membership";
pub const RELEASE_INCOMPLETE: &str = "Release membership could not be checked everywhere";

/// What a hop reads next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Target {
    /// An exact version id: a global id URL or a versioned object w3id.
    Version(String),
    Run(String),
    /// A release crate's exact version, read as a nested crate.
    Release(String),
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
    /// The entities, the id each asked id became, the root's parts and the links to read.
    spool: Spool,
    start: String,
    published: String,
    hidden: usize,
    stopped: Option<String>,
}

impl ProvenanceCrate {
    /// A crate about `start`, published at `published` (an ISO 8601 date-time).
    pub fn new(start: &str, published: &str) -> Result<Self, fjall::Error> {
        Ok(Self {
            spool: Spool::open()?,
            start: start.to_string(),
            published: published.to_string(),
            hidden: 0,
            stopped: None,
        })
    }

    /// Queues links to read after every one queued before.
    pub fn push(&mut self, hops: impl IntoIterator<Item = Hop>) {
        for hop in hops {
            let pushed = self.spool.push(&hop);
            self.keep(pushed);
        }
    }

    /// The next link to read, if any.
    pub fn pop(&mut self) -> Option<Hop> {
        let popped = self.spool.pop();
        self.keep(popped).flatten()
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
        match self.label(&asked) {
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
        let duplicate = id != asked && self.label(&id).is_some();
        self.set_label(&asked, &id);
        self.set_label(&id, &id);
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
            self.put(&key, &property);
            identifiers.push(json!({ "@id": key }));
        }
        if !identifiers.is_empty() {
            file.insert("identifier".into(), Value::Array(identifiers));
        }
        self.put(&id, &file);
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
        self.database(&id, &origin["bucket_metadata"]);
        next.extend(self.releases(&id, origin));
        next
    }

    /// The releases that pin the version, and whether that could be checked everywhere; returns
    /// the release crates to read.
    fn releases(&mut self, id: &str, origin: &Value) -> Vec<Hop> {
        let mut crates = Vec::new();
        for release in origin["releases"].as_array().into_iter().flatten() {
            let Some(key) = release["crate_version"].as_str() else {
                continue;
            };
            let mut entity = entity(key, "Dataset");
            entity.insert(
                "name".into(),
                json!(format!("Release {}", text(&release["label"]))),
            );
            entity.insert("version".into(), release["label"].clone());
            let links: Vec<Value> = (release["upstream"].as_array().into_iter().flatten())
                .filter_map(|identifier| identifier["url"].as_str())
                .map(|url| json!({ "@id": url }))
                .collect();
            if !links.is_empty() {
                entity.insert("isBasedOn".into(), Value::Array(links));
            }
            self.put_new(key, &entity);
            self.add_ref(id, "isPartOf", key);
            crates.push(Hop {
                target: Target::Release(key.to_string()),
                from: key.to_string(),
                property: "subjectOf",
                digest: None,
            });
        }
        if origin["releases_complete"] == json!(false) {
            let key = format!("{id}#release-membership");
            let mut property = entity(&key, "PropertyValue");
            property.insert("propertyID".into(), json!(RELEASE_MEMBERSHIP));
            property.insert("value".into(), json!("incomplete"));
            property.insert("description".into(), json!(RELEASE_INCOMPLETE));
            self.put(&key, &property);
            self.add_ref(id, "additionalProperty", &key);
        }
        crates
    }

    /// Records a release crate, written as a nested crate when the caller may read it.
    pub fn release(&mut self, hop: &Hop, release_crate: Option<&Value>) -> Option<CrateFile> {
        let Target::Release(version) = &hop.target else {
            return None;
        };
        let digest = blake3::hash(version.as_bytes()).to_hex();
        let folder = format!("releases/{}/", &digest[..16]);
        self.set_label(&asked(&hop.target), &folder);
        let Some(release_crate) = release_crate else {
            self.add_text(
                version,
                "description",
                "The release crate is not visible to you",
            );
            return None;
        };
        let mut nested = entity(&folder, "Dataset");
        nested.insert("name".into(), json!(format!("Release crate {version}")));
        nested.insert("conformsTo".into(), json!({ "@id": NESTED_CRATE }));
        self.put(&folder, &nested);
        self.add_part(&folder);
        self.link(hop, &folder);
        let bytes = serde_json::to_vec_pretty(release_crate).unwrap_or_default();
        let path = format!("{folder}ro-crate-metadata.json");
        Some(CrateFile { path, bytes })
    }

    /// The bucket's database metadata now, as a Dataset the version is part of.
    fn database(&mut self, id: &str, metadata: &Value) {
        if metadata["database"] != json!(true) {
            return;
        }
        let digest = blake3::hash(metadata.to_string().as_bytes()).to_hex();
        let key = format!("#database-{}", &digest[..16]);
        let mut database = entity(&key, "Dataset");
        let title = metadata["title"]
            .as_str()
            .unwrap_or("Bucket with database metadata");
        database.insert("name".into(), json!(format!("{title} (current metadata)")));
        for property in ["version", "license", "description"] {
            if let Some(value) = metadata[property].as_str() {
                database.insert(property.into(), json!(value));
            }
        }
        let links: Vec<Value> = (metadata["identifiers"].as_array().into_iter().flatten())
            .filter_map(|identifier| identifier["url"].as_str())
            .map(|url| json!(url))
            .collect();
        if !links.is_empty() {
            database.insert("identifier".into(), Value::Array(links));
        }
        self.put(&key, &database);
        self.add_ref(id, "isPartOf", &key);
    }

    /// The external source the version was registered from.
    fn source(&mut self, id: &str, origin: &Value) {
        if let Some(url) = origin["source"]["url"].as_str() {
            self.add_ref(id, "isBasedOn", url);
        }
    }

    /// Records a run and its crate, written as a nested crate; returns the run's inputs.
    pub fn run(&mut self, hop: &Hop, run_crate: Option<&Value>) -> (Vec<Hop>, Option<CrateFile>) {
        let Target::Run(run_id) = &hop.target else {
            return (Vec::new(), None);
        };
        let key = format!("#run-{run_id}");
        self.set_label(&asked(&hop.target), &key);
        let mut run = entity(&key, "CreateAction");
        run.insert("name".into(), json!(format!("Run {run_id}")));
        self.put(&key, &run);
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
        self.put(&folder, &nested);
        self.add_part(&folder);
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

    /// Records a run whose crate was not written, with the state its node reported. `false`
    /// when the state names no such outcome.
    pub fn unwritten(&mut self, hop: &Hop, status: &str, message: Option<&str>) -> bool {
        let state = match status {
            "denied" => "RunRecordDenied",
            "failed" => "RunRecordFailed",
            "local" => "RunRecordLocal",
            _ => return false,
        };
        self.run(hop, None);
        let Target::Run(run_id) = &hop.target else {
            return false;
        };
        let key = format!("#run-{run_id}");
        self.add_text(&key, "creativeWorkStatus", state);
        let text = match status {
            "local" => "The run record is kept on the device that ran it",
            _ => message.unwrap_or("The run record was not written"),
        };
        self.add_text(&key, "description", text);
        true
    }

    /// Records a dataset start: the dataset as a nested crate and its parts with exact ids.
    pub fn dataset(&mut self, document: &Value) -> (Vec<Hop>, CrateFile) {
        let folder = "dataset/".to_string();
        let mut nested = entity(&folder, "Dataset");
        nested.insert("name".into(), json!("The dataset"));
        nested.insert("conformsTo".into(), json!({ "@id": NESTED_CRATE }));
        self.put(&folder, &nested);
        self.add_part(&folder);
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
        self.set_label(&asked, &id);
        let mut file = entity(&id, "File");
        file.insert("name".into(), json!("Not visible to you"));
        self.put_new(&id, &file);
        self.link(hop, &id);
    }

    /// A hop whose node did not answer; the crate is then incomplete.
    pub fn unreachable(&mut self, hop: &Hop) {
        let asked = asked(&hop.target);
        self.set_label(&asked, &asked);
        let mut file = entity(&asked, "File");
        file.insert(
            "description".into(),
            json!("The node holding it did not answer"),
        );
        self.put_new(&asked, &file);
        self.link(hop, &asked);
        self.incomplete("some nodes did not answer");
    }

    /// Marks the crate incomplete without ending the walk; the first reason is kept.
    pub fn incomplete(&mut self, reason: &str) {
        self.stopped.get_or_insert_with(|| reason.to_string());
    }

    /// Ends the walk early; the crate says it is incomplete and why.
    pub fn stop(&mut self, reason: &str) {
        self.stopped = Some(reason.to_string());
    }

    /// Writes the root `ro-crate-metadata.json` one entity at a time; ends early with
    /// `Interrupted` once `stop` says so.
    pub fn write_root(
        mut self,
        out: &mut impl Write,
        stop: &dyn Fn() -> bool,
    ) -> std::io::Result<()> {
        let check = || match stop() {
            true => Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
            false => Ok(()),
        };
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
        for (key, value) in self.get("./").into_iter().flatten() {
            if key != "@id" {
                root.insert(key, value);
            }
        }
        let status = match &self.stopped {
            None => "Complete".to_string(),
            Some(reason) => format!("Incomplete: {reason}"),
        };
        root.insert("creativeWorkStatus".into(), json!(status));
        let descriptor = json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": { "@id": CRATE_SPEC },
            "about": { "@id": "./" }
        });
        let failed = std::io::Error::other;
        write!(out, "{{\"@context\":{},\"@graph\":[", json!(CRATE_CONTEXT))?;
        serde_json::to_writer(&mut *out, &descriptor)?;
        // The root object without its closing brace, so its parts follow as they are read.
        let root = serde_json::to_string(&root)?;
        write!(out, ",{},\"hasPart\":[", &root[..root.len() - 1])?;
        for (index, part) in self.spool.parts().enumerate() {
            check()?;
            let part = part.map_err(failed)?;
            let separator = if index == 0 { "" } else { "," };
            write!(out, "{separator}{}", json!({ "@id": part }))?;
        }
        out.write_all(b"]}")?;
        for entity in self.spool.entities() {
            check()?;
            let entity = entity.map_err(failed)?;
            if entity["@id"] != "./" {
                out.write_all(b",")?;
                serde_json::to_writer(&mut *out, &entity)?;
            }
        }
        out.write_all(b"]}")
    }

    fn unversioned(&mut self, from: &str, id: &str) {
        let mut file = entity(id, "File");
        file.insert("description".into(), json!("No exact version recorded"));
        self.put_new(id, &file);
        let property = if from.starts_with("#run-") {
            "object"
        } else {
            "hasPart"
        };
        self.add_ref(from, property, id);
    }

    fn link(&mut self, hop: &Hop, to: &str) {
        // A run names the file it made from its own side.
        match hop.target {
            Target::Run(_) => self.add_ref(to, "result", &hop.from),
            Target::Version(_) | Target::Release(_) => self.add_ref(&hop.from, hop.property, to),
        }
    }

    fn add_ref(&mut self, from: &str, property: &str, to: &str) {
        let mut source = self.get(from).unwrap_or_else(|| entity(from, "Dataset"));
        let reference = json!({ "@id": to });
        match source.get_mut(property) {
            Some(Value::Array(values)) if !values.contains(&reference) => values.push(reference),
            Some(_) => {}
            None => {
                source.insert(property.to_string(), json!([reference]));
            }
        }
        self.put(from, &source);
    }

    fn add_text(&mut self, id: &str, property: &str, value: &str) {
        if let Some(mut target) = self.get(id) {
            target.insert(property.to_string(), json!(value));
            self.put(id, &target);
        }
    }

    /// A failed read or write of the working state leaves the crate incomplete.
    fn keep<T>(&mut self, result: SpoolResult<T>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(_) => {
                self.incomplete("the export could not keep its working state");
                None
            }
        }
    }

    fn label(&mut self, asked: &str) -> Option<String> {
        let label = self.spool.label(asked);
        self.keep(label).flatten()
    }

    fn set_label(&mut self, asked: &str, id: &str) {
        let set = self.spool.set_label(asked, id);
        self.keep(set);
    }

    fn get(&mut self, id: &str) -> Option<Map<String, Value>> {
        let found = self.spool.entity(id);
        self.keep(found).flatten()
    }

    fn put(&mut self, id: &str, entity: &Map<String, Value>) {
        let put = self.spool.put_entity(id, entity);
        self.keep(put);
    }

    /// Stores an entity only when none of its id is stored yet.
    fn put_new(&mut self, id: &str, entity: &Map<String, Value>) {
        if self.get(id).is_none() {
            self.put(id, entity);
        }
    }

    fn add_part(&mut self, id: &str) {
        let added = self.spool.add_part(id);
        self.keep(added);
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
        Target::Release(id) => format!("release:{id}"),
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

/// A stored crate names files `MediaObject`, the term `File` stands for.
fn is_file(node: &Value) -> bool {
    text_values(node.get("@type"))
        .iter()
        .any(|kind| kind == "File" || kind == "MediaObject")
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

mod spool;

#[cfg(test)]
mod tests;
