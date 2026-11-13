//! Builds the eLabFTW Python example and runs one timer run in the plugin runtime, against a
//! broker with one tagged experiment. Opt in with ARUNA_PLUGIN_LANGUAGES=1 and uv installed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(all(feature = "runtime", feature = "build"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, FilePage, FullResponse, HostError,
    Intent, Listing, MetadataCreate, MetadataPatch, Method, ObjectInfo, ObjectPage, Response,
    SendRequest, StatePut,
};
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::package::build_project;
use aruna_plugins::permission::Permission;
use aruna_plugins::{CancellationToken, Invocation, Runtime, Trigger};

const DATASET: &str = "01JD000000000000000000000C";

/// A group with one bucket and an eLabFTW team with experiment 12 tagged `aruna`.
#[derive(Default)]
struct Team {
    calls: Mutex<Vec<String>>,
    state: Mutex<BTreeMap<String, (u64, Vec<u8>)>>,
    dataset: Mutex<Option<Value>>,
}

impl Team {
    fn note(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }
}

fn experiment() -> Value {
    json!({
        "id": 12, "title": "Run 7", "body": "<p>b</p>", "body_html": "<p>b</p>",
        "content_type": 1, "tags": "aruna|pcr", "tags_id": "1,2", "metadata": "{}",
        "canwrite_base": 30, "userid": 7, "state": 1
    })
}

fn unused() -> HostError {
    HostError::new(ErrorKind::Unavailable, "not used by the example")
}

#[async_trait::async_trait]
impl Broker for Team {
    async fn granted(&self, _: &CallContext, _: Permission, _: &str) -> bool {
        true
    }

    async fn metadata_get(&self, _: &CallContext, resource: &str) -> Result<Document, HostError> {
        self.note(format!("get {resource}"));
        let dataset = self.dataset.lock().unwrap().clone();
        let json = dataset.ok_or_else(unused)?.to_string();
        let (resource, revision) = (resource.to_string(), "01JD00000000000000000000R1".into());
        Ok(Document {
            resource,
            revision,
            json,
        })
    }

    async fn metadata_list(
        &self,
        _: &CallContext,
        _: Option<String>,
    ) -> Result<Listing, HostError> {
        Ok(Listing {
            resources: Vec::new(),
            cursor: None,
        })
    }

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        self.note(format!("patch {}", patch.patch));
        Ok("01JD00000000000000000000R2".into())
    }

    async fn metadata_files(
        &self,
        _: &CallContext,
        _: &str,
        _: Option<String>,
    ) -> Result<FilePage, HostError> {
        Ok(FilePage {
            files: Vec::new(),
            cursor: None,
        })
    }

    async fn metadata_create(
        &self,
        _: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        self.note(format!("create {} {}", create.path, create.key));
        *self.dataset.lock().unwrap() = Some(serde_json::from_str(&create.dataset).unwrap());
        let revision = "01JD00000000000000000000R1".into();
        Ok(Created {
            resource: DATASET.into(),
            revision,
        })
    }

    async fn objects_info(&self, _: &CallContext, _: &str) -> Result<ObjectInfo, HostError> {
        Err(unused())
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        prefix: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        self.note(format!("objects {prefix}"));
        Ok(ObjectPage {
            objects: Vec::new(),
            cursor: None,
        })
    }

    async fn connection_send(
        &self,
        _: &CallContext,
        _: SendRequest,
    ) -> Result<Response, HostError> {
        Err(unused())
    }

    async fn connection_send_full(
        &self,
        _: &CallContext,
        request: SendRequest,
    ) -> Result<FullResponse, HostError> {
        let body = String::from_utf8_lossy(&request.body).to_string();
        self.note(format!("elab {:?} {} {body}", request.method, request.path));
        let offset = (request.query.iter()).find(|field| field.name == "offset");
        let answer = match (request.method, request.path.as_str()) {
            (Method::Get, "") if offset.is_some_and(|field| field.value == "0") => {
                json!([{ "id": 12, "title": "Run 7" }])
            }
            (Method::Get, "") | (Method::Get, "12/uploads") => json!([]),
            _ => experiment(),
        };
        let body = answer.to_string().into_bytes();
        Ok(FullResponse {
            status: 200,
            headers: Vec::new(),
            body,
        })
    }

    async fn state_get(&self, _: &CallContext, key: &str) -> Result<Option<Entry>, HostError> {
        let state = self.state.lock().unwrap();
        let entry = state.get(key).cloned();
        Ok(entry.map(|(version, value)| Entry { version, value }))
    }

    async fn state_put(&self, _: &CallContext, put: StatePut) -> Result<u64, HostError> {
        let mut state = self.state.lock().unwrap();
        let version = state.get(&put.key).map(|(version, _)| *version);
        if version != put.expected {
            return Err(HostError::new(ErrorKind::Conflict, "another version"));
        }
        let next = version.unwrap_or_default() + 1;
        state.insert(put.key, (next, put.value));
        Ok(next)
    }

    async fn abandoned(&self, _: &CallContext, _: &Intent) {}
}

/// Whether the opt-in is exactly `1` and uv runs; prints why the test is skipped.
fn available() -> bool {
    if std::env::var("ARUNA_PLUGIN_LANGUAGES").as_deref() != Ok("1") {
        eprintln!("skipping: set ARUNA_PLUGIN_LANGUAGES=1 to build the example");
        return false;
    }
    let runs = Command::new("uv").arg("--version").output();
    runs.is_ok_and(|out| out.status.success())
}

fn limits() -> Limits {
    Limits {
        memory: 512 << 20,
        fuel: 1 << 40,
        ..Limits::default()
    }
}

#[tokio::test]
async fn enrols_tagged() {
    if !available() {
        return;
    }
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/elabftw-python");
    let built = build_project(&project).unwrap_or_else(|err| panic!("{err}"));
    let config = RuntimeConfig {
        max_artifact: 64 << 20,
        ceiling: limits(),
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::new(config).unwrap();
    let compiled = runtime.prepare(built.artifact).await.unwrap();
    let settings = json!({
        "elab_url": "https://elab.example.org", "sync_user": 7, "bucket": "/r/g/lab/data/n/b",
        "prefix": "elabftw", "tag": "aruna", "aruna_url": "https://aruna.example.org"
    });
    let permissions = [
        Permission::MetadataRead,
        Permission::MetadataWrite,
        Permission::MetadataCreate,
        Permission::ObjectsRead,
    ];
    let call = Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        handler: "poll".into(),
        settings: settings.to_string(),
        trigger: Trigger::Timer(1_762_700_000_000),
        permissions: BTreeSet::from(permissions),
        connections: BTreeSet::from(["elab".to_string()]),
        limits: limits(),
        cancel: CancellationToken::new(),
    };
    let team = Arc::new(Team::default());
    let answer = runtime.invoke(&compiled, call, team.clone()).await;
    let answer: Value = serde_json::from_str(&answer.unwrap().unwrap()).unwrap();
    assert_eq!(answer, json!({ "cycle": 2, "visits": 1 }));

    let calls = team.calls.lock().unwrap().clone();
    assert_eq!(calls[0], "objects /r/g/lab/data/n/b/elabftw/");
    assert!(
        calls.contains(&"create elabftw/12 elabftw/12".to_string()),
        "{calls:?}"
    );
    let merge = (calls.iter()).find(|call| call.contains("metadatamerge"));
    assert!(
        merge.is_some_and(|call| call.contains(DATASET)),
        "{calls:?}"
    );
    let dataset = team.dataset.lock().unwrap().clone().unwrap();
    assert_eq!(dataset["name"], "Run 7");
    assert_eq!(dataset["keywords"], json!(["pcr"]));
    assert_eq!(dataset["identifier"]["elabftw"]["id"], "12");

    let state = team.state.lock().unwrap();
    let link: Value = serde_json::from_slice(&state["link/12"].1).unwrap();
    assert_eq!(
        (link["dataset"].as_str(), &link["mark"]),
        (Some(DATASET), &Value::Null)
    );
}
