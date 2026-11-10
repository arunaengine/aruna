//! Builds the dataset announcer and template probes in each language with its toolchain and runs
//! them. Opt in with ARUNA_PLUGIN_LANGUAGES=1: the toolchains download packages on first use.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(all(feature = "runtime", feature = "build"))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
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
use aruna_plugins::{
    CancellationToken, Compiled, FailureKind, Invocation, InvokeError, Runtime, Trigger, WatchEvent,
};

/// Generous cap on one build and run, downloads included, so a stuck toolchain fails the test.
const HANG_CAP: Duration = Duration::from_secs(1800);

/// The catalog's answer to a new record; the manifest lets the code read only `/id`.
const RECORD: &str = r#"{"id":"c-1","secret":"s"}"#;

/// Answers like a group with one dataset and a catalog, and records what the plugin sent.
/// Reading `missing` is unavailable and reading `denied` is denied.
struct Catalog {
    calls: Mutex<Vec<String>>,
    reply: &'static str,
}

impl Catalog {
    fn new(reply: &'static str) -> Arc<Self> {
        let calls = Mutex::default();
        Arc::new(Self { calls, reply })
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

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
        match resource {
            "missing" => return Err(HostError::new(ErrorKind::Unavailable, "try later")),
            "denied" => return Err(HostError::new(ErrorKind::Denied, "no")),
            _ => {}
        }
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
        let json = Some(self.reply.into());
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

/// Whether the opt-in is exactly `1` and `program` runs; prints why a language is skipped.
fn available(language: &str, program: &str) -> bool {
    if std::env::var("ARUNA_PLUGIN_LANGUAGES").as_deref() != Ok("1") {
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
    runs && (language != "rust" || wasm_target())
}

/// Whether the toolchain has the `wasm32-unknown-unknown` standard library.
fn wasm_target() -> bool {
    let libdir = Command::new("rustc")
        .args([
            "--print",
            "target-libdir",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()));
    let installed = libdir.is_some_and(|dir| dir.is_dir());
    if !installed {
        eprintln!("skipping rust: the wasm32-unknown-unknown target is not installed");
    }
    installed
}

fn limits() -> Limits {
    Limits {
        memory: 512 << 20,
        fuel: 1 << 40,
        ..Limits::default()
    }
}

/// Builds the project and compiles it, all within the hang cap.
async fn build(project: PathBuf) -> (Runtime, Arc<Compiled>) {
    let shown = project.display().to_string();
    let building = tokio::task::spawn_blocking(move || build_project(&project));
    let built = tokio::time::timeout(HANG_CAP, building)
        .await
        .unwrap_or_else(|_| panic!("{shown}: the build ended within the hang cap"))
        .unwrap()
        .unwrap_or_else(|err| panic!("{shown}: {err}"));
    let runtime = Runtime::new(RuntimeConfig {
        max_artifact: 64 << 20,
        ceiling: limits(),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let compiled = runtime.prepare(built.artifact).await.unwrap();
    (runtime, compiled)
}

async fn run(
    (runtime, compiled): &(Runtime, Arc<Compiled>),
    handler: &str,
    trigger: Trigger,
    broker: Arc<Catalog>,
) -> Result<Option<String>, InvokeError> {
    let call = Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        handler: handler.into(),
        settings: r#"{"catalog_url":"https://catalog.example.org"}"#.into(),
        trigger,
        permissions: BTreeSet::from([Permission::MetadataRead, Permission::MetadataWrite]),
        connections: BTreeSet::from(["catalog".to_string()]),
        limits: limits(),
        cancel: CancellationToken::new(),
    };
    tokio::time::timeout(HANG_CAP, runtime.invoke(compiled, call, broker))
        .await
        .expect("the run ended within the hang cap")
}

fn example(language: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(language)
}

fn created() -> Trigger {
    Trigger::Event(WatchEvent {
        kind: "dataset_created".into(),
        path: "meta/group-1/D1".into(),
        resource: "D1".into(),
        revision: "r1".into(),
    })
}

/// Runs the example on a new dataset: it reads it, posts it, and records the catalog id.
async fn announce(language: &str) {
    let plugin = build(example(language)).await;
    let catalog = Catalog::new(RECORD);
    let ended = run(&plugin, "dataset_created", created(), catalog.clone()).await;
    assert!(ended.is_ok(), "{language}: {ended:?}");
    let calls = catalog.calls();
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

    // A catalog answer without `/id` fails the run before any metadata is written.
    let catalog = Catalog::new(r#"{"other":1}"#);
    let ended = run(&plugin, "dataset_created", created(), catalog.clone()).await;
    let Err(InvokeError::Guest(failure)) = ended else {
        panic!("{language}: a missing id must fail the run: {ended:?}");
    };
    assert_eq!(failure.kind, FailureKind::Permanent, "{language}");
    let calls = catalog.calls();
    assert!(
        !calls.iter().any(|call| call.starts_with("patch")),
        "{language}: {calls:?}"
    );
}

/// A project from the language's template whose `run` handler is `code`, triggered by hand and
/// by a timer; the build reads the WIT from this crate.
fn probe(language: &str, code: &str) -> PathBuf {
    let templates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("templates")
        .join(language);
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("probe-{language}"));
    std::fs::create_dir_all(&dir).unwrap();
    let wit = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("wit");
    let template = std::fs::read_to_string(templates.join("plugin.toml")).unwrap();
    let build = &template[template.find("[build]").unwrap()..];
    let build = build.replace("\"wit\"", &format!("{:?}", wit.display().to_string()));
    let manifest = format!(
        "name = \"probe\"\nversion = \"0.1.0\"\n\n[triggers]\nmanual = {{ handler = \"run\" }}\n\
         every = {{ handler = \"run\", interval = \"1h\" }}\n\n[permissions]\n\
         required = [\"metadata.read\"]\n\n{build}"
    );
    std::fs::write(dir.join("plugin.toml"), manifest).unwrap();
    match language {
        "javascript" => {
            let source = std::fs::read_to_string(templates.join("plugin.js")).unwrap();
            let end = source.find("\n};\n").unwrap() + 4;
            let rest = &source[end..];
            std::fs::write(dir.join("plugin.js"), format!("{code}\n{rest}")).unwrap();
        }
        _ => {
            std::fs::copy(templates.join("aruna.py"), dir.join("aruna.py")).unwrap();
            std::fs::write(dir.join("app.py"), code).unwrap();
        }
    }
    dir
}

/// Runs a probe by hand with `input` and by a timer, and checks what each answer became.
async fn check_probe(language: &str, plugin: &(Runtime, Arc<Compiled>), plain: &str) {
    let manual = |input: &str| Trigger::Manual(serde_json::json!(input).to_string());
    let answer = run(plugin, "run", manual("plain"), Catalog::new(RECORD)).await;
    assert_eq!(answer.unwrap().as_deref(), Some(plain), "{language}");
    let tick = run(
        plugin,
        "run",
        Trigger::Timer(1_762_700_000_000),
        Catalog::new(RECORD),
    )
    .await;
    assert_eq!(
        tick.unwrap().as_deref(),
        Some("tick 1762700000000"),
        "{language}"
    );
    for (input, kind) in [
        ("retry", FailureKind::Retryable),
        ("missing", FailureKind::Retryable),
        ("denied", FailureKind::Permanent),
    ] {
        let ended = run(plugin, "run", manual(input), Catalog::new(RECORD)).await;
        let Err(InvokeError::Guest(failure)) = ended else {
            panic!("{language} {input}: {ended:?}");
        };
        assert_eq!(
            failure.kind, kind,
            "{language} {input}: {}",
            failure.message
        );
    }
}

const JAVASCRIPT_PROBE: &str = r#"import { get } from "aruna:plugin/metadata-read@0.2.0";

const handlers = {
  async run(ctx, event) {
    if (event.kind === "timer") return `tick ${event.tick}`;
    if (event.input === "retry") throw { kind: "retryable", message: "try later" };
    if (event.input !== "plain") return get(event.input).json;
    await Promise.resolve();
    return "awaited";
  },
};
"#;

const PYTHON_PROBE: &str = r#"from aruna import WitWorld, handler  # noqa: F401
from componentize_py_types import Err
from wit_world.imports import metadata_read, types


@handler
def run(ctx, event):
    if event["kind"] == "timer":
        return f"tick {event['tick']}"
    if event["input"] == "retry":
        raise Err(types.Failure(types.FailureKind.RETRYABLE, "try later"))
    if event["input"] != "plain":
        return metadata_read.get(event["input"]).json
    return "plain"
"#;

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

#[tokio::test]
async fn probes_javascript() {
    if available("javascript", "npx") {
        let plugin = build(probe("javascript", JAVASCRIPT_PROBE)).await;
        check_probe("javascript", &plugin, "awaited").await;
    }
}

#[tokio::test]
async fn probes_python() {
    if available("python", "uv") {
        let plugin = build(probe("python", PYTHON_PROBE)).await;
        check_probe("python", &plugin, "plain").await;
    }
}
