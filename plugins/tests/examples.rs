//! Builds each plugin example once and runs its cases against one fake group of datasets, objects,
//! service answers, secrets, state and copies. Opt in with ARUNA_PLUGIN_LANGUAGES=1.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(all(feature = "runtime", feature = "build"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, FileChange, FilePage,
    HostError, Intent, JobCopy, JobStatus, Listing, MetadataCreate, MetadataPatch, ObjectInfo,
    ObjectPage, Response, SendRequest, StatePut, StateRemove,
};
use aruna_plugins::limits::RuntimeConfig;
use aruna_plugins::package::build_project;
use aruna_plugins::permission::Permission;
use aruna_plugins::{
    CancellationToken, Compiled, Failure, FailureKind, Invocation, InvokeError, Runtime, Trigger,
    WatchEvent, WebhookDelivery, WebhookRequest,
};

use common::{available, limits};

mod common;

/// Items of one page of a fake listing.
const PAGE: usize = 2;

/// One stored object version; a `marked` one was written by a job with a secret.
#[derive(Clone)]
struct Version {
    id: String,
    content: Vec<u8>,
    marked: bool,
}

/// One dataset: its revision, entity and file entities.
#[derive(Clone, Default)]
struct Stored {
    revision: u64,
    entity: Map<String, Value>,
    files: Vec<Value>,
}

#[derive(Default)]
struct State {
    datasets: BTreeMap<String, Stored>,
    /// Datasets the install may read but not write.
    read_only: BTreeSet<String>,
    /// Versions of each object by bucket path, the current one last.
    objects: BTreeMap<String, Vec<Version>>,
    /// Service answers by connection and path: status and body.
    replies: BTreeMap<(String, String), (u16, Vec<u8>)>,
    sent: Vec<SendRequest>,
    values: BTreeMap<String, (u64, Vec<u8>)>,
    versions: u64,
    /// Copies by job id, and the id each run's call ordinal got.
    copies: BTreeMap<String, JobCopy>,
    ids: BTreeMap<(String, u32), String>,
    lines: Vec<String>,
    runs: u64,
}

/// The fake group every example runs against; the test seeds it and reads what the plugin did.
#[derive(Default)]
struct Group(Mutex<State>);

impl Group {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn with<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        change(&mut self.0.lock().unwrap())
    }

    /// Stores dataset `id` at revision 1.
    fn dataset(&self, id: &str, entity: Value) {
        let entity = entity.as_object().cloned().unwrap_or_default();
        let stored = Stored {
            revision: 1,
            entity,
            files: Vec::new(),
        };
        self.with(|state| state.datasets.insert(id.into(), stored));
    }

    fn reply(&self, connection: &str, path: &str, status: u16, body: Value) {
        let key = (connection.to_string(), path.to_string());
        let body = body.to_string().into_bytes();
        self.with(|state| state.replies.insert(key, (status, body)));
    }

    fn entity(&self, id: &str) -> Value {
        Value::Object(self.with(|state| state.datasets[id].entity.clone()))
    }

    /// What the plugin sent: connection, path and the body as JSON, or null.
    fn sent(&self) -> Vec<(String, String, Value)> {
        let sent = self.with(|state| state.sent.clone());
        let body =
            |request: &SendRequest| serde_json::from_slice(&request.body).unwrap_or_default();
        let sent = sent.iter().map(|request| {
            (
                request.connection.clone(),
                request.path.clone(),
                body(request),
            )
        });
        sent.collect()
    }

    fn read_only(&self, id: &str) {
        self.with(|state| state.read_only.insert(id.into()));
    }

    /// Stores a new current version `id` of the object at `path`.
    fn object(&self, path: &str, id: &str, content: &[u8]) {
        let version = Version {
            id: id.into(),
            content: content.to_vec(),
            marked: false,
        };
        self.with(|state| state.objects.entry(path.into()).or_default().push(version));
    }

    fn files(&self, id: &str) -> Vec<Value> {
        self.with(|state| state.datasets[id].files.clone())
    }

    fn copies(&self) -> Vec<JobCopy> {
        self.with(|state| state.copies.values().cloned().collect())
    }

    fn value(&self, key: &str) -> Option<Value> {
        let value = self.with(|state| state.values.get(key).cloned());
        value.map(|(_, value)| serde_json::from_slice(&value).unwrap())
    }

    fn stored(&self, id: &str) -> Result<Stored, HostError> {
        let stored = self.with(|state| state.datasets.get(id).cloned());
        stored.ok_or_else(|| HostError::new(ErrorKind::NotFound, format!("no dataset {id}")))
    }

    /// Runs `change` on dataset `id` at revision `expected`, as a write does.
    fn write(
        &self,
        id: &str,
        expected: &str,
        change: impl FnOnce(&mut Stored),
    ) -> Result<String, HostError> {
        let mut state = self.0.lock().unwrap();
        if state.read_only.contains(id) {
            let message = format!("the install may not write datasets/{id}");
            return Err(HostError::new(ErrorKind::Denied, message));
        }
        let missing = || HostError::new(ErrorKind::NotFound, format!("no dataset {id}"));
        let stored = state.datasets.get_mut(id).ok_or_else(missing)?;
        if stored.revision.to_string() != expected {
            return Err(HostError::new(ErrorKind::Conflict, "changed meanwhile"));
        }
        change(stored);
        stored.revision += 1;
        Ok(stored.revision.to_string())
    }

    fn version(&self, path: &str, version: Option<&str>) -> Result<(String, Version), HostError> {
        let versions = self
            .with(|state| state.objects.get(path).cloned())
            .unwrap_or_default();
        let found = match version {
            Some(id) => versions.into_iter().find(|kept| kept.id == id),
            None => versions.into_iter().last(),
        };
        let missing = || HostError::new(ErrorKind::NotFound, format!("no object at {path}"));
        Ok((path.to_string(), found.ok_or_else(missing)?))
    }
}

fn info((path, version): (String, Version)) -> ObjectInfo {
    ObjectInfo {
        path,
        size: version.content.len() as u64,
        version: version.id,
        marked: version.marked,
    }
}

/// The items after `cursor`, one page of them, and the cursor of the next page.
fn paged<T: Clone>(items: &[T], cursor: Option<String>) -> (Vec<T>, Option<String>) {
    let start: usize = cursor.map_or(0, |cursor| cursor.parse().unwrap());
    let next = start + PAGE;
    let page = items.iter().skip(start).take(PAGE).cloned().collect();
    (page, (next < items.len()).then(|| next.to_string()))
}

/// Applies a JSON merge patch by the host's rules; `merge_cases.json` holds both to the same cases.
fn merge_into(target: &mut Value, patch: Value) {
    let Value::Object(changes) = patch else {
        *target = patch;
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
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

/// An entity id as the node stores it, by the rule of `normalize_entity_id` in
/// `operations/src/metadata/handle/entity_convert.rs`: a relative id gets a leading `./`.
fn entity_id(id: &str) -> String {
    let kept = ["./", "../", "#", "_:"]
        .iter()
        .any(|start| id.starts_with(start));
    let named = id.contains("://") || (id.contains(':') && !id.contains('/'));
    match kept || named || id == "ro-crate-metadata.json" {
        true => id.to_string(),
        false => format!("./{id}"),
    }
}

/// Whether a value is an entity of its own, which the node refuses: an object with JSON-LD keys
/// that is neither a reference nor a value (`property_value_terms`, `entity_convert.rs`).
fn inline_entity(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let reference = (object.keys()).all(|key| matches!(key.as_str(), "@id" | "@type"));
    let keyed = object.keys().any(|key| key.starts_with('@'));
    keyed && !(reference && object.contains_key("@id")) && !object.contains_key("@value")
}

/// The dataset as the node's export answers it: a field with one value reads as that value.
fn document(id: &str, stored: &Stored, whole: bool) -> Document {
    let mut entity = stored.entity.clone();
    for value in entity.values_mut() {
        if let Some([single]) = value.as_array().map(Vec::as_slice) {
            *value = single.clone();
        }
    }
    if whole {
        let parts = stored
            .files
            .iter()
            .map(|file| json!({ "@id": file["@id"] }));
        entity.insert("hasPart".into(), parts.collect());
    }
    Document {
        resource: id.into(),
        revision: stored.revision.to_string(),
        json: Value::Object(entity).to_string(),
    }
}

#[async_trait::async_trait]
impl Broker for Group {
    async fn metadata_get(&self, _: &CallContext, resource: &str) -> Result<Document, HostError> {
        Ok(document(resource, &self.stored(resource)?, true))
    }

    async fn metadata_summary(
        &self,
        _: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        Ok(document(resource, &self.stored(resource)?, false))
    }

    async fn metadata_list(
        &self,
        _: &CallContext,
        cursor: Option<String>,
    ) -> Result<Listing, HostError> {
        let ids: Vec<String> = self.with(|state| state.datasets.keys().cloned().collect());
        let (resources, cursor) = paged(&ids, cursor);
        Ok(Listing { resources, cursor })
    }

    async fn metadata_files(
        &self,
        _: &CallContext,
        resource: &str,
        cursor: Option<String>,
    ) -> Result<FilePage, HostError> {
        let files: Vec<String> = (self.stored(resource)?.files.iter())
            .map(Value::to_string)
            .collect();
        let (files, cursor) = paged(&files, cursor);
        Ok(FilePage { files, cursor })
    }

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        let changes: Value = serde_json::from_str(&patch.patch).unwrap();
        let values = changes
            .as_object()
            .into_iter()
            .flat_map(|fields| fields.values());
        let mut entries =
            values.flat_map(|value| value.as_array().cloned().unwrap_or(vec![value.clone()]));
        if entries.any(|entry| inline_entity(&entry)) {
            let message = "a dataset names other entities only by @id";
            return Err(HostError::new(ErrorKind::Invalid, message));
        }
        self.write(&patch.resource, &patch.expected, |stored| {
            let mut entity = Value::Object(std::mem::take(&mut stored.entity));
            merge_into(&mut entity, changes);
            stored.entity = entity.as_object().cloned().unwrap_or_default();
        })
    }

    async fn metadata_put_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        let mut file: Value = serde_json::from_str(&change.file).unwrap();
        file["@id"] = entity_id(file["@id"].as_str().unwrap_or_default()).into();
        self.write(&change.resource, &change.expected, |stored| {
            stored.files.retain(|kept| kept["@id"] != file["@id"]);
            stored.files.push(file);
        })
    }

    async fn metadata_remove_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        self.write(&change.resource, &change.expected, |stored| {
            stored
                .files
                .retain(|kept| kept["@id"] != change.file.as_str());
        })
    }

    /// The path names the dataset, so a repeat answers the stored one. A new dataset needs a
    /// name, a description and one publication date, as Craqle's import check requires.
    async fn metadata_create(
        &self,
        _: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        let dataset: Value = serde_json::from_str(&create.dataset).unwrap();
        for field in ["name", "description", "datePublished"] {
            if !dataset[field].is_string() {
                let message = format!("missing required property `schema:{field}`");
                return Err(HostError::new(ErrorKind::Invalid, message));
            }
        }
        let id = format!("D-{}", create.path);
        if self.stored(&id).is_err() {
            self.dataset(&id, serde_json::from_str(&create.dataset).unwrap());
        }
        let revision = self.stored(&id)?.revision.to_string();
        Ok(Created {
            resource: id,
            revision,
        })
    }

    async fn objects_info(
        &self,
        _: &CallContext,
        path: &str,
        version: Option<String>,
    ) -> Result<ObjectInfo, HostError> {
        Ok(info(self.version(path, version.as_deref())?))
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        prefix: &str,
        cursor: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let paths: Vec<String> = self.with(|state| state.objects.keys().cloned().collect());
        let below = paths.iter().filter(|path| path.starts_with(prefix));
        let infos: Vec<ObjectInfo> = below
            .map(|path| info(self.version(path, None).unwrap()))
            .collect();
        let (objects, cursor) = paged(&infos, cursor);
        Ok(ObjectPage { objects, cursor })
    }

    async fn objects_read(
        &self,
        _: &CallContext,
        path: &str,
        version: Option<String>,
    ) -> Result<Vec<u8>, HostError> {
        let (_, version) = self.version(path, version.as_deref())?;
        if version.marked {
            return Err(HostError::new(
                ErrorKind::Denied,
                "written by a job with a secret",
            ));
        }
        Ok(version.content)
    }

    async fn connection_send(
        &self,
        _: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        let key = (request.connection.clone(), request.path.clone());
        let reply = self.with(|state| {
            state.sent.push(request);
            state.replies.get(&key).cloned()
        });
        let (status, body) = reply.unwrap_or((404, b"not found".to_vec()));
        let headers = vec![Field {
            name: "content-type".into(),
            value: "application/json".into(),
        }];
        Ok(Response {
            status,
            headers,
            body,
        })
    }

    /// A rerun of the same run reaches the same call ordinal, so it gets the same job back. The
    /// copy writes its target at once, a new version named by the job, as an upload does.
    async fn jobs_copy(&self, cx: &CallContext, copy: JobCopy) -> Result<String, HostError> {
        let (_, source) = self.version(&copy.source, copy.version.as_deref())?;
        Ok(self.with(|state| {
            let count = state.copies.len() + 1;
            let key = (cx.run.clone(), copy.intent.ordinal);
            let id = state
                .ids
                .entry(key)
                .or_insert_with(|| format!("job-{count}"))
                .clone();
            if state.copies.insert(id.clone(), copy.clone()).is_none() {
                let target = Version {
                    id: id.clone(),
                    ..source
                };
                state.objects.entry(copy.target).or_default().push(target);
            }
            id
        }))
    }

    async fn jobs_get(&self, _: &CallContext, id: &str) -> Result<JobStatus, HostError> {
        let copy = self.with(|state| state.copies.get(id).cloned());
        let copy =
            copy.ok_or_else(|| HostError::new(ErrorKind::NotFound, format!("no job {id}")))?;
        Ok(JobStatus {
            id: id.into(),
            kind: "copy".into(),
            path: copy.target,
            state: "succeeded".into(),
            message: None,
            bytes: None,
            version: None,
            reply_status: None,
            reply_headers: Vec::new(),
            reply_body: None,
            exit_code: None,
            report: None,
        })
    }

    async fn state_get(&self, _: &CallContext, key: &str) -> Result<Option<Entry>, HostError> {
        let entry = self.with(|state| state.values.get(key).cloned());
        Ok(entry.map(|(version, value)| Entry { version, value }))
    }

    async fn state_put(&self, _: &CallContext, put: StatePut) -> Result<u64, HostError> {
        self.with(|state| {
            if state.values.get(&put.key).map(|(version, _)| *version) != put.expected {
                return Err(HostError::new(ErrorKind::Conflict, "changed meanwhile"));
            }
            state.versions += 1;
            state.values.insert(put.key, (state.versions, put.value));
            Ok(state.versions)
        })
    }

    async fn state_remove(&self, _: &CallContext, remove: StateRemove) -> Result<(), HostError> {
        self.with(|state| match state.values.get(&remove.key) {
            Some((version, _)) if *version == remove.expected => {
                state.values.remove(&remove.key);
                Ok(())
            }
            _ => Err(HostError::new(ErrorKind::Conflict, "changed meanwhile")),
        })
    }

    async fn abandoned(&self, _: &CallContext, _: &Intent) {}

    async fn log(&self, _: &CallContext, level: &str, message: &str) {
        self.with(|state| state.lines.push(format!("{level} {message}")));
    }
}

/// A built example, compiled once for all its cases, with what its manifest asks for granted.
struct Example {
    runtime: Runtime,
    compiled: Arc<Compiled>,
    permissions: BTreeSet<Permission>,
    connections: BTreeSet<String>,
}

/// Builds `plugins/examples/<name>`, or answers none when its toolchain is missing.
async fn example(name: &str) -> Option<Example> {
    let project = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name);
    let (language, program) = match project {
        _ if project.join("Cargo.toml").is_file() => ("rust", "cargo"),
        _ if project.join("plugin.js").is_file() => ("javascript", "npx"),
        _ => ("python", "uv"),
    };
    if !available(language, program) {
        return None;
    }
    let built = tokio::task::spawn_blocking(move || build_project(&project))
        .await
        .unwrap();
    let built = built.unwrap_or_else(|error| panic!("{name}: {error}"));
    let config = RuntimeConfig {
        max_artifact: 64 << 20,
        ceiling: limits(),
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::new(config).unwrap();
    let compiled = runtime.prepare(built.artifact).await.unwrap();
    Some(Example {
        runtime,
        compiled,
        permissions: built.manifest.requested(),
        connections: built.manifest.connections.keys().cloned().collect(),
    })
}

impl Example {
    /// Runs `trigger` with `settings` as a new run; answers its output as JSON, or its failure.
    async fn run(
        &self,
        group: &Arc<Group>,
        settings: Value,
        trigger: Trigger,
    ) -> Result<Value, Failure> {
        let run = group.with(|state| {
            state.runs += 1;
            format!("run-{}", state.runs)
        });
        self.rerun(group, &run, settings, trigger).await
    }

    /// Runs `trigger` as run `run`, again when that run ran before.
    async fn rerun(
        &self,
        group: &Arc<Group>,
        run: &str,
        settings: Value,
        trigger: Trigger,
    ) -> Result<Value, Failure> {
        let call = Invocation {
            run: run.into(),
            install: "install-1".into(),
            group: "group-1".into(),
            settings: settings.to_string(),
            trigger,
            permissions: self.permissions.clone(),
            connections: self.connections.clone(),
            limits: limits(),
            cancel: CancellationToken::new(),
        };
        match self
            .runtime
            .invoke(&self.compiled, call, group.clone())
            .await
        {
            Ok(output) => {
                Ok(output.map_or(Value::Null, |text| serde_json::from_str(&text).unwrap()))
            }
            Err(InvokeError::Guest(failure)) => Err(failure),
            Err(other) => panic!("the run did not end in the plugin: {other:?}"),
        }
    }
}

fn manual(form: Value) -> Trigger {
    Trigger::Manual(form.to_string())
}

/// An event of `kind` about `resource` at `revision`, as the node delivers it.
fn event(kind: &str, resource: &str, revision: &str) -> Trigger {
    Trigger::Event(WatchEvent {
        kind: kind.into(),
        path: format!("/realm/g/group-1/{resource}"),
        resource: resource.into(),
        revision: revision.into(),
    })
}

#[test]
fn merge_cases() {
    // The host's own merge patch is tested against the same cases.
    let cases = include_str!("../sdk/merge_cases.json");
    let cases: Vec<(Value, Value, Value)> = serde_json::from_str(cases).unwrap();
    for (target, patch, merged) in cases {
        let mut result = target.clone();
        merge_into(&mut result, patch.clone());
        assert_eq!(result, merged, "{target} {patch}");
    }
}

/// Posts a new dataset to the catalog and saves the catalog's id on it, then again by hand; an
/// answer without an id or with status 500 fails the run before anything is written.
async fn announce(language: &str) {
    let Some(example) = example(&format!("announcer/{language}")).await else {
        return;
    };
    let settings = json!({ "catalog_url": "https://catalog.example.org" });
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7" }));
    group.reply("catalog", "", 201, json!({ "id": "c-1" }));
    let created = example.run(
        &group,
        settings.clone(),
        event("dataset_created", "D1", "1"),
    );
    created.await.unwrap();
    assert_eq!(
        group.entity("D1")["identifier"],
        json!({ "catalog": "c-1" }),
        "{language}"
    );
    let sent = (
        "catalog".to_string(),
        String::new(),
        json!({ "id": "D1", "title": "Run 7" }),
    );
    assert_eq!(group.sent(), [sent], "{language}");
    group.dataset("D2", json!({ "name": "Run 8" }));
    let by_hand = example.run(&group, settings.clone(), manual(json!({ "dataset": "D2" })));
    by_hand.await.unwrap();
    assert_eq!(
        group.entity("D2")["identifier"]["catalog"],
        "c-1",
        "{language}"
    );
    for (status, answer) in [(201, json!({ "other": 1 })), (500, json!({ "id": "c-2" }))] {
        group.reply("catalog", "", status, answer);
        group.dataset("D3", json!({ "name": "Run 9" }));
        let failed = example.run(
            &group,
            settings.clone(),
            event("dataset_created", "D3", "1"),
        );
        let failure = failed.await.unwrap_err();
        assert_eq!(failure.kind, FailureKind::Permanent, "{language} {status}");
        assert_eq!(
            group.entity("D3"),
            json!({ "name": "Run 9" }),
            "{language} {status}"
        );
    }
}

#[tokio::test]
async fn announcer_python() {
    announce("python").await;
}

#[tokio::test]
async fn announcer_javascript() {
    announce("javascript").await;
}

#[tokio::test]
async fn announcer_rust() {
    announce("rust").await;
}

#[tokio::test]
async fn import_record() {
    // A record becomes a dataset named by its path, so a second import answers the same one.
    let Some(example) = example("import-record").await else {
        return;
    };
    let group = Group::new();
    let meta = json!({ "title": "Gel images", "keywords": ["gel"], "license": { "id": "cc-by-4.0" },
        "publication_date": "2026-01-02", "creators": [{ "name": "Ada" }] });
    group.reply(
        "zenodo",
        "123",
        200,
        json!({ "doi": "10.5281/zenodo.123", "metadata": meta }),
    );
    let form = manual(json!({ "record": " 123 " }));
    let imported = example.run(&group, json!({}), form.clone()).await.unwrap();
    assert_eq!(
        imported,
        json!({ "dataset": "D-zenodo/123", "title": "Gel images" })
    );
    let entity = group.entity("D-zenodo/123");
    assert_eq!(
        entity["identifier"],
        json!({ "doi": "10.5281/zenodo.123", "zenodo": "123" })
    );
    assert_eq!(
        (&entity["author"], &entity["license"]),
        (&json!(["Ada"]), &json!("cc-by-4.0"))
    );
    assert_eq!(
        example.run(&group, json!({}), form).await.unwrap(),
        imported
    );
    let invalid = example
        .run(&group, json!({}), manual(json!({ "record": "x1" })))
        .await;
    assert_eq!(invalid.unwrap_err().kind, FailureKind::InvalidInput);
    let missing = example
        .run(&group, json!({}), manual(json!({ "record": "9" })))
        .await;
    assert_eq!(
        missing.unwrap_err().kind,
        FailureKind::Permanent,
        "the service answered 404"
    );
}

#[tokio::test]
async fn fill_defaults() {
    // Only missing fields are filled; a dataset that has them all makes no write.
    let Some(example) = example("fill-defaults").await else {
        return;
    };
    let settings = json!({ "license": "CC0-1.0", "publisher": "Lab" });
    let group = Group::new();
    group.dataset(
        "D1",
        json!({ "name": "Run 7", "keywords": "Gel, pcr, gel" }),
    );
    let filled = example.run(
        &group,
        settings.clone(),
        event("dataset_created", "D1", "1"),
    );
    assert_eq!(
        filled.await.unwrap(),
        json!({ "changed": ["keywords", "license", "publisher"] })
    );
    let entity = group.entity("D1");
    assert_eq!(
        (&entity["license"], &entity["publisher"]),
        (&json!("CC0-1.0"), &json!("Lab"))
    );
    assert_eq!(entity["keywords"], json!(["gel", "pcr"]));
    let again = example.run(&group, settings, manual(json!({ "dataset": "D1" })));
    assert_eq!(again.await.unwrap(), json!({ "changed": [] }));
}

#[tokio::test]
async fn link_objects() {
    // Every object below the path is linked once, by its `s3://` address, across listing pages.
    let Some(example) = example("link-objects").await else {
        return;
    };
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7" }));
    for (name, content) in [("a.fq", "ACGT"), ("b.fq", "ACGTAC"), ("c.fq", "AC")] {
        group.object(&format!("raw/run-7/{name}"), "v1", content.as_bytes());
    }
    group.object("raw/other.fq", "v1", b"A");
    let form = manual(json!({ "dataset": "D1", "prefix": "raw/run-7/" }));
    let linked = example.run(&group, json!({}), form.clone()).await.unwrap();
    assert_eq!(linked, json!({ "added": 3, "bytes": 12 }));
    let files = group.files("D1");
    let first = json!({ "@id": "./objects/raw/run-7/a.fq", "@type": "File", "name": "a.fq",
        "contentSize": 4, "contentUrl": "s3://raw/run-7/a.fq" });
    assert_eq!((files.len(), &files[0]), (3, &first));
    // A second run finds every file by its stored id, so a description added since stays.
    group.with(|state| {
        state.datasets.get_mut("D1").unwrap().files[0]["description"] = "kept".into()
    });
    let again = example.run(&group, json!({}), form).await.unwrap();
    assert_eq!(again, json!({ "added": 0, "bytes": 12 }));
    assert_eq!(group.files("D1")[0]["description"], "kept");
}

#[tokio::test]
async fn archive_uploads() {
    // The copy is pinned to the uploaded version, also when a newer one came since; a rerun of
    // the run gets the same job, and an upload elsewhere is left alone.
    let Some(example) = example("archive-uploads").await else {
        return;
    };
    let settings = json!({ "incoming": "lab-incoming/", "archive": "lab-archive/" });
    let group = Group::new();
    group.object("lab-incoming/run/a.txt", "v1", b"first");
    group.object("lab-incoming/run/a.txt", "v2", b"second");
    let upload = event("data_uploaded", "lab-incoming/run/a.txt", "v1");
    let copied = example
        .rerun(&group, "run-1", settings.clone(), upload.clone())
        .await;
    let answer = json!({ "job": "job-1", "target": "lab-archive/run/a.txt" });
    assert_eq!(copied.unwrap(), answer);
    let again = example
        .rerun(&group, "run-1", settings.clone(), upload)
        .await;
    assert_eq!(again.unwrap(), answer, "a rerun gets the same job");
    let copies = group.copies();
    let pinned = (
        copies[0].source.as_str(),
        copies[0].version.as_deref(),
        copies.len(),
    );
    assert_eq!(pinned, ("lab-incoming/run/a.txt", Some("v1"), 1));
    group.object("other/b.txt", "v1", b"b");
    let elsewhere = example.run(
        &group,
        settings,
        event("data_uploaded", "other/b.txt", "v1"),
    );
    assert_eq!(elsewhere.await.unwrap(), Value::Null);
    assert_eq!(group.copies().len(), 1);
}

#[tokio::test]
async fn archive_inside() {
    // The node delivers the copy's own upload too; an archive inside the watched path is left
    // alone, so nothing is copied twice.
    let Some(example) = example("archive-uploads").await else {
        return;
    };
    let settings = json!({ "incoming": "lab/", "archive": "lab/archive/" });
    let group = Group::new();
    group.object("lab/run/a.txt", "v1", b"first");
    let upload = event("data_uploaded", "lab/run/a.txt", "v1");
    let copied = example.run(&group, settings.clone(), upload).await.unwrap();
    assert_eq!(copied["target"], "lab/archive/run/a.txt");
    let own = event("data_uploaded", "lab/archive/run/a.txt", "job-1");
    assert_eq!(
        example.run(&group, settings, own).await.unwrap(),
        Value::Null
    );
    assert_eq!(group.copies().len(), 1);
    // An archive above the watched path still gets every upload.
    let settings = json!({ "incoming": "lab/incoming/", "archive": "lab/" });
    let group = Group::new();
    group.object("lab/incoming/a.txt", "v1", b"first");
    let upload = event("data_uploaded", "lab/incoming/a.txt", "v1");
    let copied = example.run(&group, settings, upload).await.unwrap();
    assert_eq!(copied["target"], "lab/a.txt");
}

#[tokio::test]
async fn nightly_report() {
    // Datasets without a description are reported once; the next night reports nothing new.
    let Some(example) = example("nightly-report").await else {
        return;
    };
    let settings = json!({ "chat_url": "https://chat.example.org", "channel": "c-9" });
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7" }));
    group.dataset(
        "D2",
        json!({ "name": "Run 8", "description": "Gel images" }),
    );
    group.dataset("D3", json!({}));
    group.reply("chat", "", 201, json!({}));
    let night = Trigger::Timer(1_762_700_000_000);
    let reported = example.run(&group, settings.clone(), night.clone()).await;
    assert_eq!(reported.unwrap(), json!({ "reported": 2 }));
    let message = "Datasets without a description on 2025-11-09: Run 7, D3";
    let sent = json!({ "channel_id": "c-9", "message": message });
    assert_eq!(group.sent(), [("chat".into(), String::new(), sent)]);
    assert_eq!(group.value("reported"), Some(json!(["D1", "D3"])));
    let next = example.run(&group, settings, night).await;
    assert_eq!(next.unwrap(), json!({ "reported": 0 }));
    assert_eq!(group.sent().len(), 1);
}

#[tokio::test]
async fn set_keywords() {
    // Keywords are added once each, or replace the old ones; an empty form is invalid input.
    let Some(example) = example("set-keywords").await else {
        return;
    };
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7", "keywords": ["seq", "pcr"] }));
    let form = |keywords: &str, replace: bool| {
        manual(json!({ "dataset": "D1", "keywords": keywords, "replace": replace }))
    };
    let added = example
        .run(&group, json!({}), form("pcr, gel", false))
        .await;
    let keywords = json!({ "dataset": "D1", "keywords": ["seq", "pcr", "gel"] });
    assert_eq!(added.unwrap(), keywords);
    assert_eq!(group.entity("D1")["keywords"], json!(["seq", "pcr", "gel"]));
    let replaced = example.run(&group, json!({}), form("lab", true)).await;
    assert_eq!(replaced.unwrap()["keywords"], json!(["lab"]));
    let empty = example.run(&group, json!({}), form(" , ", false)).await;
    assert_eq!(empty.unwrap_err().kind, FailureKind::InvalidInput);
}

#[tokio::test]
async fn tag_updates() {
    // A changed dataset gets the keyword once; the change that adds it writes nothing more.
    let Some(example) = example("tag-updates").await else {
        return;
    };
    let settings = json!({ "tag": "changed" });
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7", "keywords": ["seq"] }));
    let updated = example.run(
        &group,
        settings.clone(),
        event("dataset_updated", "D1", "2"),
    );
    assert_eq!(updated.await.unwrap(), json!({ "tagged": "D1" }));
    assert_eq!(group.entity("D1")["keywords"], json!(["seq", "changed"]));
    let again = example.run(&group, settings, event("dataset_updated", "D1", "3"));
    assert_eq!(again.await.unwrap(), Value::Null);
}

#[tokio::test]
async fn link_uploads() {
    // An upload below the path becomes a file of the dataset, named by its `s3://` address.
    let Some(example) = example("link-uploads").await else {
        return;
    };
    let settings = json!({ "dataset": "D1", "prefix": "lab-incoming/" });
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7" }));
    group.object("lab-incoming/gel.png", "v1", b"png");
    group.object("other/x.txt", "v1", b"x");
    let upload = event("data_uploaded", "lab-incoming/gel.png", "v1");
    let linked = example.run(&group, settings.clone(), upload).await.unwrap();
    let file = "s3://lab-incoming/gel.png";
    assert_eq!(linked, json!({ "dataset": "D1", "file": file }));
    let files = group.files("D1");
    assert_eq!(
        (&files[0]["@id"], &files[0]["contentUrl"]),
        (&json!(file), &json!(file))
    );
    let elsewhere = example.run(
        &group,
        settings,
        event("data_uploaded", "other/x.txt", "v1"),
    );
    assert_eq!(elsewhere.await.unwrap(), Value::Null);
    assert_eq!(group.files("D1").len(), 1);
}

#[tokio::test]
async fn upload_counter() {
    // Each bucket keeps its own count across runs.
    let Some(example) = example("upload-counter").await else {
        return;
    };
    let group = Group::new();
    for (path, content) in [("raw/a", "ab"), ("raw/b", "abc"), ("other/c", "a")] {
        group.object(path, "v1", content.as_bytes());
        example
            .run(&group, json!({}), event("data_uploaded", path, "v1"))
            .await
            .unwrap();
    }
    assert_eq!(
        group.value("bucket/raw"),
        Some(json!({ "uploads": 2, "bytes": 5 }))
    );
    assert_eq!(
        group.value("bucket/other"),
        Some(json!({ "uploads": 1, "bytes": 1 }))
    );
}

#[tokio::test]
async fn object_report() {
    // The report reads every page of the listing; an empty path reports no objects.
    let Some(example) = example("object-report").await else {
        return;
    };
    let group = Group::new();
    for (name, content) in [("a.fq", "ACGT"), ("b.fq", "ACGTACGT"), ("c.fq", "AC")] {
        group.object(&format!("raw/run-7/{name}"), "v1", content.as_bytes());
    }
    let report = example.run(&group, json!({}), manual(json!({ "prefix": "raw/run-7/" })));
    let answer = json!({ "objects": 3, "bytes": 14, "largest": "raw/run-7/b.fq" });
    assert_eq!(report.await.unwrap(), answer);
    let empty = example.run(&group, json!({}), manual(json!({ "prefix": "none/" })));
    assert_eq!(
        empty.await.unwrap(),
        json!({ "objects": 0, "bytes": 0, "largest": null })
    );
}

/// A verified webhook delivery with a JSON body, as the node hands it to the plugin.
fn webhook(delivery: &str, body: Value) -> Trigger {
    Trigger::Webhook(WebhookDelivery {
        delivery_id: Some(delivery.into()),
        request: WebhookRequest {
            method: "POST".into(),
            query: String::new(),
            headers: Vec::new(),
            body: body.to_string().into_bytes(),
        },
    })
}

#[tokio::test]
async fn release_datasets() {
    // A published release becomes a dataset; a repeated delivery answers the same one.
    let Some(example) = example("release-datasets").await else {
        return;
    };
    let group = Group::new();
    let release = json!({ "id": 7, "tag_name": "v1.0", "name": "First", "body": "Notes",
        "html_url": "https://github.com/lab/tool/releases/v1.0",
        "published_at": "2026-01-02T10:00:00Z" });
    let body = json!({ "action": "published", "release": release,
        "repository": { "full_name": "lab/tool" } });
    let created = example
        .run(&group, json!({}), webhook("d-1", body.clone()))
        .await;
    let answer = json!({ "dataset": "D-releases/lab/tool/v1.0", "delivery": "d-1" });
    assert_eq!(created.unwrap(), answer);
    let entity = group.entity("D-releases/lab/tool/v1.0");
    assert_eq!(
        (&entity["name"], &entity["identifier"]),
        (&json!("First"), &json!({ "github_release": "7" }))
    );
    assert_eq!(entity["datePublished"], "2026-01-02T10:00:00Z");
    let again = example.run(&group, json!({}), webhook("d-1", body)).await;
    assert_eq!(again.unwrap()["dataset"], "D-releases/lab/tool/v1.0");
    let other = example.run(
        &group,
        json!({}),
        webhook("d-2", json!({ "action": "created" })),
    );
    assert_eq!(other.await.unwrap(), Value::Null);
}

#[tokio::test]
async fn fill_license() {
    // A dataset outside the install's write scope answers `NotAllowed` and is skipped.
    let Some(example) = example("fill-license").await else {
        return;
    };
    let group = Group::new();
    group.dataset("D1", json!({ "name": "Run 7" }));
    group.dataset("D2", json!({ "name": "Run 8", "license": "CC0-1.0" }));
    group.dataset("D3", json!({ "name": "Run 9" }));
    group.read_only("D3");
    let filled = example.run(&group, json!({ "license": "CC-BY-4.0" }), manual(json!({})));
    assert_eq!(
        filled.await.unwrap(),
        json!({ "filled": ["D1"], "skipped": ["D3"] })
    );
    assert_eq!(group.entity("D1")["license"], "CC-BY-4.0");
    assert_eq!(group.entity("D3"), json!({ "name": "Run 9" }));
}

#[tokio::test]
async fn orcid_names() {
    // The author is added as a reference to the ORCID iD, next to the one already there, and
    // once only; the name from the public record is the run's answer.
    let Some(example) = example("orcid-names").await else {
        return;
    };
    let group = Group::new();
    let before = json!({ "@id": "https://orcid.org/0000-0001-5109-3700" });
    group.dataset("D1", json!({ "name": "Run 7", "author": [before] }));
    let id = "0000-0002-1825-0097";
    let name =
        json!({ "given-names": { "value": "Josiah" }, "family-name": { "value": "Carberry" } });
    group.reply(
        "orcid",
        &format!("{id}/person"),
        200,
        json!({ "name": name }),
    );
    let form = manual(json!({ "dataset": "D1", "orcid": format!("https://orcid.org/{id}") }));
    let added = example.run(&group, json!({}), form.clone()).await.unwrap();
    assert_eq!(
        added,
        json!({ "dataset": "D1", "author": "Josiah Carberry" })
    );
    let author = json!([before, { "@id": format!("https://orcid.org/{id}") }]);
    assert_eq!(group.entity("D1")["author"], author);
    example.run(&group, json!({}), form).await.unwrap();
    assert_eq!(group.entity("D1")["author"], author);
}

#[tokio::test]
async fn sync_alerts() {
    // The first failure after a success is sent; a post the chat refused is tried again at the
    // next failure, and a completed sync starts again.
    let Some(example) = example("sync-alerts").await else {
        return;
    };
    let settings = json!({ "chat_url": "https://chat.example.org" });
    let group = Group::new();
    let sync = |kind: &str, job: &str| {
        Trigger::Event(WatchEvent {
            kind: kind.into(),
            path: "raw/run-7/".into(),
            resource: "rel-1".into(),
            revision: job.into(),
        })
    };
    group.reply("chat", "", 500, json!({}));
    let refused = example
        .run(&group, settings.clone(), sync("sync_failed", "j1"))
        .await;
    assert_eq!(refused.unwrap_err().kind, FailureKind::Permanent);
    group.reply("chat", "", 201, json!({}));
    let mut counts = Vec::new();
    for (kind, job) in [
        ("sync_failed", "j2"),
        ("sync_failed", "j3"),
        ("sync_completed", "j4"),
        ("sync_failed", "j5"),
    ] {
        let answer = example
            .run(&group, settings.clone(), sync(kind, job))
            .await
            .unwrap();
        counts.push(answer["failures"].clone());
    }
    assert_eq!(counts, [json!(2), json!(3), json!(0), json!(1)]);
    let texts: Vec<Value> = group
        .sent()
        .into_iter()
        .map(|(_, _, body)| body["text"].clone())
        .collect();
    let text = |job: &str| json!(format!("The sync of raw/run-7/ failed (job {job})"));
    assert_eq!(texts, [text("j1"), text("j2"), text("j5")]);
}
