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
    Broker, CallContext, Created, Document, Entry, ErrorKind, Field, HostError, Intent, Listing,
    MetadataCreate, MetadataPatch, ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::limits::RuntimeConfig;
use aruna_plugins::package::build_project;
use aruna_plugins::permission::Permission;
use aruna_plugins::{
    CancellationToken, Compiled, Failure, FailureKind, Invocation, InvokeError, Runtime, Trigger,
    WatchEvent,
};

use common::{available, limits};

mod common;

/// Items of one page of a fake listing.
const PAGE: usize = 2;

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
    /// Service answers by connection and path: status and body.
    replies: BTreeMap<(String, String), (u16, Vec<u8>)>,
    sent: Vec<SendRequest>,
    values: BTreeMap<String, (u64, Vec<u8>)>,
    versions: u64,
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
        let missing = || HostError::new(ErrorKind::NotFound, format!("no dataset {id}"));
        let stored = state.datasets.get_mut(id).ok_or_else(missing)?;
        if stored.revision.to_string() != expected {
            return Err(HostError::new(ErrorKind::Conflict, "changed meanwhile"));
        }
        change(stored);
        stored.revision += 1;
        Ok(stored.revision.to_string())
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

fn document(id: &str, stored: &Stored, whole: bool) -> Document {
    let mut entity = stored.entity.clone();
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

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        let changes: Value = serde_json::from_str(&patch.patch).unwrap();
        self.write(&patch.resource, &patch.expected, |stored| {
            let mut entity = Value::Object(std::mem::take(&mut stored.entity));
            merge_into(&mut entity, changes);
            stored.entity = entity.as_object().cloned().unwrap_or_default();
        })
    }

    /// The path names the dataset, so a repeat answers the stored one.
    async fn metadata_create(
        &self,
        _: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
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
        _: Option<String>,
    ) -> Result<ObjectInfo, HostError> {
        Err(HostError::new(
            ErrorKind::NotFound,
            format!("no object at {path}"),
        ))
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        _: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        let (objects, cursor) = (Vec::new(), None);
        Ok(ObjectPage { objects, cursor })
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
