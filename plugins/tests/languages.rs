//! Builds the dataset announcer example in each language with its toolchain and runs it.
//! Opt in with ARUNA_PLUGIN_LANGUAGES=1: the toolchains download packages on first use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(all(feature = "runtime", feature = "build"))]

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_plugins::broker::{
    Broker, CallContext, Document, Entry, ErrorKind, HostError, Intent, Listing, MetadataPatch,
    ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::package::build_project;
use aruna_plugins::permission::Permission;
use aruna_plugins::{CancellationToken, Invocation, Runtime, Trigger, WatchEvent};

/// Generous cap so a broken run fails the test instead of hanging it.
const HANG_CAP: Duration = Duration::from_secs(600);

/// Answers like a group with one dataset and a catalog, and records what the plugin sent.
#[derive(Default)]
struct Catalog {
    calls: Mutex<Vec<String>>,
}

impl Catalog {
    fn note(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }
}

fn unused() -> HostError {
    HostError::new(ErrorKind::Unavailable, "not used by the example")
}

#[async_trait::async_trait]
impl Broker for Catalog {
    async fn granted(&self, _: &CallContext, permission: Permission, resource: &str) -> bool {
        self.note(format!("granted {permission} {resource}"));
        permission == Permission::MetadataWrite
    }

    async fn metadata_get(&self, _: &CallContext, resource: &str) -> Result<Document, HostError> {
        self.note(format!("get {resource}"));
        let json = r#"{"name":"Run 7"}"#.into();
        let (resource, revision) = (resource.to_string(), "r1".into());
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
        Err(unused())
    }

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        self.note(format!(
            "patch {} {} {}",
            patch.resource, patch.expected, patch.patch
        ));
        Ok("r2".into())
    }

    async fn objects_info(&self, _: &CallContext, _: &str) -> Result<ObjectInfo, HostError> {
        Err(unused())
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        _: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        Err(unused())
    }

    async fn connection_send(
        &self,
        _: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        let body = String::from_utf8_lossy(&request.body).to_string();
        self.note(format!(
            "send {} {:?} {body}",
            request.connection, request.method
        ));
        let json = Some(r#"{"id":"c-1","secret":"s"}"#.into());
        Ok(Response { status: 201, json })
    }

    async fn state_get(&self, _: &CallContext, _: &str) -> Result<Option<Entry>, HostError> {
        Err(unused())
    }

    async fn state_put(&self, _: &CallContext, _: StatePut) -> Result<u64, HostError> {
        Err(unused())
    }

    async fn abandoned(&self, _: &CallContext, _: &Intent) {}
}

/// Whether the opt-in is set and `program` runs; prints why a language is skipped otherwise.
fn available(language: &str, program: &str) -> bool {
    if std::env::var_os("ARUNA_PLUGIN_LANGUAGES").is_none() {
        eprintln!("skipping {language}: set ARUNA_PLUGIN_LANGUAGES=1 to build the examples");
        return false;
    }
    let runs = Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success());
    if !runs {
        eprintln!("skipping {language}: `{program}` is not installed");
    }
    runs
}

async fn announce(language: &str) {
    let project = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(language);
    let built = build_project(&project).unwrap_or_else(|err| panic!("{language}: {err}"));
    let runtime = Runtime::new(RuntimeConfig {
        max_artifact: 64 << 20,
        ceiling: Limits {
            memory: 512 << 20,
            fuel: 1 << 40,
            ..Limits::default()
        },
        ..RuntimeConfig::default()
    })
    .unwrap();
    let compiled = runtime.prepare(built.artifact).await.unwrap();
    let event = WatchEvent {
        kind: "dataset_created".into(),
        path: "meta/group-1/D1".into(),
        resource: "D1".into(),
        revision: "r1".into(),
    };
    let call = Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        handler: "dataset_created".into(),
        settings: r#"{"catalog_url":"https://catalog.example.org"}"#.into(),
        trigger: Trigger::Event(event),
        permissions: BTreeSet::from([Permission::MetadataRead, Permission::MetadataWrite]),
        connections: BTreeSet::from(["catalog".to_string()]),
        limits: Limits {
            memory: 512 << 20,
            fuel: 1 << 40,
            ..Limits::default()
        },
        cancel: CancellationToken::new(),
    };
    let catalog = Arc::new(Catalog::default());
    let ended = tokio::time::timeout(HANG_CAP, runtime.invoke(&compiled, call, catalog.clone()))
        .await
        .expect("the run ended within the hang cap");
    assert!(ended.is_ok(), "{language}: {ended:?}");
    let calls = catalog.calls.lock().unwrap().clone();
    let sent: serde_json::Value =
        serde_json::from_str(calls[1].splitn(4, ' ').nth(3).unwrap()).unwrap();
    assert_eq!(calls[0], "get D1", "{language}");
    assert!(
        calls[1].starts_with("send catalog Method::Post "),
        "{language}: {calls:?}"
    );
    assert_eq!(
        sent,
        serde_json::json!({"id": "D1", "title": "Run 7"}),
        "{language}"
    );
    assert_eq!(calls[2], "granted metadata.write D1", "{language}");
    let (_, patch) = calls[3]
        .split_once("D1 r1 ")
        .expect("a patch at the read revision");
    let patch: serde_json::Value = serde_json::from_str(patch).unwrap();
    assert_eq!(
        patch,
        serde_json::json!({"identifier": {"catalog": "c-1"}}),
        "{language}"
    );
    assert_eq!(calls.len(), 4, "{language}: {calls:?}");
}

#[tokio::test]
async fn announces_rust() {
    if available("rust", "cargo") {
        announce("rust").await;
    }
}

#[tokio::test]
async fn announces_javascript() {
    if available("javascript", "npx") {
        announce("javascript").await;
    }
}

#[tokio::test]
async fn announces_python() {
    if available("python", "uv") {
        announce("python").await;
    }
}
