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

use tokio::time::{Instant, timeout_at};

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

/// Generous deadline for one case, build with downloads, compile and runs together.
const HANG_CAP: Duration = Duration::from_secs(1800);
/// Set for a child run of this binary that only builds the project it names.
const BUILD_VAR: &str = "ARUNA_PLUGIN_BUILD";
/// Where that child run writes the built artifact.
const OUT_VAR: &str = "ARUNA_PLUGIN_BUILT";

/// The catalog's answer to a new record.
const RECORD: &str = r#"{"id":"c-1","created":true}"#;

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

    /// The library reads a dataset without its file links; the dataset is the same.
    async fn metadata_summary(
        &self,
        cx: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        self.metadata_get(cx, resource).await
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
        Ok(Response {
            status: 201,
            headers: Vec::new(),
            body: self.reply.into(),
        })
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

/// A built and compiled plugin and the deadline its case must end by.
struct Plugin {
    runtime: Runtime,
    compiled: Arc<Compiled>,
    deadline: Instant,
}

/// Builds the project in a child process and compiles it; the whole case shares one deadline.
async fn build(project: PathBuf) -> Plugin {
    let deadline = Instant::now() + HANG_CAP;
    let artifact = build_child(&project, None, deadline).await.unwrap();
    let runtime = Runtime::new(RuntimeConfig {
        max_artifact: 64 << 20,
        ceiling: limits(),
        ..RuntimeConfig::default()
    })
    .unwrap();
    let compiled = timeout_at(deadline, runtime.prepare(artifact))
        .await
        .expect("the compile ended before the deadline")
        .unwrap();
    Plugin {
        runtime,
        compiled,
        deadline,
    }
}

/// Builds `project` in a child run of this binary, in a process group of its own, so a stuck
/// toolchain is killed with everything it started once the deadline passes. `tools` comes first
/// in the child's PATH.
async fn build_child(
    project: &Path,
    tools: Option<&Path>,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    let name = project.file_name().unwrap().to_string_lossy().to_string();
    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("built-{name}.wasm"));
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command.args(["build_entry", "--exact", "--nocapture", "--test-threads=1"]);
    // Cargo resolves from its local cache only, so a build needs no network.
    command
        .env(BUILD_VAR, project)
        .env(OUT_VAR, &out)
        .env("CARGO_NET_OFFLINE", "true")
        .kill_on_drop(true);
    if let Some(tools) = tools {
        let rest = std::env::var_os("PATH").unwrap_or_default();
        let paths = [tools.to_path_buf()]
            .into_iter()
            .chain(std::env::split_paths(&rest));
        command.env("PATH", std::env::join_paths(paths).unwrap());
    }
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().unwrap();
    match timeout_at(deadline, child.wait()).await {
        Ok(Ok(status)) if status.success() => std::fs::read(out).map_err(|err| err.to_string()),
        Ok(status) => Err(format!("{name}: the build failed: {status:?}")),
        Err(_) => {
            stop_tree(&mut child).await;
            Err(format!(
                "{name}: the build passed the deadline and was killed"
            ))
        }
    }
}

/// Kills a child and everything it started, and reaps it.
async fn stop_tree(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child
        .id()
        .and_then(|id| rustix::process::Pid::from_raw(id as i32))
    {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    #[cfg(windows)]
    if let Some(pid) = child.id() {
        let tree = ["/F", "/T", "/PID", &pid.to_string()];
        let _ = Command::new("taskkill").args(tree).status();
    }
    let _ = child.kill().await;
}

/// The child side of [`build_child`]: does nothing unless it was started for a build.
#[test]
fn build_entry() {
    let (Some(project), Some(out)) = (std::env::var_os(BUILD_VAR), std::env::var_os(OUT_VAR))
    else {
        return;
    };
    let built = build_project(Path::new(&project)).unwrap_or_else(|err| panic!("{err}"));
    std::fs::write(out, built.artifact).unwrap();
}

async fn run(
    plugin: &Plugin,
    trigger: Trigger,
    broker: Arc<Catalog>,
) -> Result<Option<String>, InvokeError> {
    let call = Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        settings: r#"{"catalog_url":"https://catalog.example.org"}"#.into(),
        trigger,
        permissions: BTreeSet::from([Permission::MetadataRead, Permission::MetadataWrite]),
        connections: BTreeSet::from(["catalog".to_string()]),
        limits: limits(),
        cancel: CancellationToken::new(),
    };
    let invoke = plugin.runtime.invoke(&plugin.compiled, call, broker);
    timeout_at(plugin.deadline, invoke)
        .await
        .expect("the run ended before the deadline")
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
    let ended = run(&plugin, created(), catalog.clone()).await;
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
    let (_, patch) = calls[2]
        .split_once("D1 r1 ")
        .expect("a patch at the read revision");
    let patch: serde_json::Value = serde_json::from_str(patch).unwrap();
    assert_eq!(
        patch,
        serde_json::json!({"identifier": {"catalog": "c-1"}}),
        "{language}"
    );
    assert_eq!(calls.len(), 3, "{language}: {calls:?}");

    // A catalog answer without `/id` fails the run before any metadata is written.
    let catalog = Catalog::new(r#"{"other":1}"#);
    let ended = run(&plugin, created(), catalog.clone()).await;
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

/// A project whose plugin code is `code`, triggered by hand and by a timer.
fn probe(language: &str, code: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("probe-{language}"));
    std::fs::create_dir_all(&dir).unwrap();
    let manifest = "name = \"probe\"\nversion = \"0.1.0\"\non = [\"manual\", \"timer\"]\n\n\
                    [timer]\nevery = \"1h\"\n";
    std::fs::write(dir.join("plugin.toml"), manifest).unwrap();
    let file = match language {
        "javascript" => "plugin.js",
        "rust" => {
            std::fs::write(dir.join("Cargo.toml"), RUST_PROJECT).unwrap();
            std::fs::create_dir_all(dir.join("src")).unwrap();
            "src/lib.rs"
        }
        _ => "plugin.py",
    };
    std::fs::write(dir.join(file), code).unwrap();
    dir
}

/// Runs a probe by hand with the form field `input` and by a timer, and checks what each answer
/// became.
async fn check_probe(language: &str, plugin: &Plugin, plain: &str, tick: &str) {
    let manual = |input: &str| Trigger::Manual(serde_json::json!({ "input": input }).to_string());
    let answer = run(plugin, manual("plain"), Catalog::new(RECORD)).await;
    assert_eq!(answer.unwrap().as_deref(), Some(plain), "{language}");
    let timer = Trigger::Timer(1_762_700_000_000);
    let answer = run(plugin, timer, Catalog::new(RECORD)).await;
    assert_eq!(answer.unwrap().as_deref(), Some(tick), "{language}");
    for (input, kind) in [
        ("retry", FailureKind::Retryable),
        ("missing", FailureKind::Retryable),
        ("denied", FailureKind::Permanent),
    ] {
        let ended = run(plugin, manual(input), Catalog::new(RECORD)).await;
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

const JAVASCRIPT_PROBE: &str = r#"import aruna from "aruna";

aruna.on("manual", async (form) => {
  if (form.input === "retry") throw new aruna.Retry("try later");
  if (form.input !== "plain") return aruna.datasets.get(form.input).name;
  return await Promise.resolve("awaited");
});
aruna.on("timer", (tick) => ({ tick: tick.getTime() }));
"#;

/// A new project's manifest, on the library the build writes into it.
const RUST_PROJECT: &str = r#"[package]
name = "probe"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
aruna = { package = "aruna-plugin", path = ".aruna/rust/aruna-plugin" }
serde_json = "1"

# The probe is a project of its own, not a member of the repository's workspace.
[workspace]
"#;

const RUST_PROBE: &str = r#"use std::time::{SystemTime, UNIX_EPOCH};

use aruna::{Form, Result};
use serde_json::{Value, json};

fn run(form: Form) -> Result<String> {
    match form.text("input")?.as_str() {
        "retry" => Err(aruna::retry("try later")),
        "plain" => Ok("plain".into()),
        other => Ok(aruna::datasets::get(other)?.name),
    }
}

fn tick(tick: SystemTime) -> Result<Value> {
    let since = tick.duration_since(UNIX_EPOCH).unwrap_or_default();
    Ok(json!({ "tick": since.as_millis() as u64 }))
}

aruna::plugin! { manual => run, timer => tick }
"#;

const PYTHON_PROBE: &str = r#"import aruna


@aruna.on("manual")
def run(form):
    if form.input == "retry":
        raise aruna.Retry("try later")
    if form.input != "plain":
        return aruna.datasets.get(form.input).name
    return "plain"


@aruna.on("timer")
def tick(tick):
    return {"tick": int(tick.timestamp() * 1000)}
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
        let tick = r#"{"tick":1762700000000}"#;
        check_probe("javascript", &plugin, "\"awaited\"", tick).await;
    }
}

#[tokio::test]
async fn probes_rust() {
    // A new project builds offline against the library the build writes into it.
    if available("rust", "cargo") {
        let plugin = build(probe("rust", RUST_PROBE)).await;
        check_probe("rust", &plugin, "\"plain\"", r#"{"tick":1762700000000}"#).await;
    }
}

#[tokio::test]
async fn probes_python() {
    if available("python", "uv") {
        let plugin = build(probe("python", PYTHON_PROBE)).await;
        check_probe("python", &plugin, "\"plain\"", r#"{"tick": 1762700000000}"#).await;
    }
}

/// Copies the folder `from` to `to`, naming the plugin `starter` as `plugin new` would.
fn copy_template(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        if path.is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_template(&path, &target);
        } else {
            let text = std::fs::read_to_string(&path).unwrap();
            std::fs::write(target, text.replace("{{name}}", "starter")).unwrap();
        }
    }
}

#[tokio::test]
async fn builds_templates() {
    // Each language's starter project builds on its library and answers a manual run.
    for (language, program) in [("rust", "cargo"), ("javascript", "npx"), ("python", "uv")] {
        if !available(language, program) {
            continue;
        }
        let project = tempfile::tempdir().unwrap();
        let template = Path::new(env!("CARGO_MANIFEST_DIR")).join("templates");
        copy_template(&template.join(language), project.path());
        let plugin = build(project.path().to_path_buf()).await;
        let answer = run(&plugin, Trigger::Manual("{}".into()), Catalog::new(RECORD)).await;
        let hello = "\"hello from install-1\"";
        assert_eq!(answer.unwrap().as_deref(), Some(hello), "{language}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn kills_stuck_builds() {
    // A toolchain that never ends is killed with its process group once the deadline passes.
    use std::os::unix::fs::PermissionsExt;
    let project = Path::new(env!("CARGO_TARGET_TMPDIR")).join("stuck");
    let tools = project.join("tools");
    std::fs::create_dir_all(&tools).unwrap();
    let manifest = "name = \"stuck\"\nversion = \"0.1.0\"\non = [\"manual\"]\n";
    std::fs::write(project.join("plugin.toml"), manifest).unwrap();
    std::fs::write(project.join("plugin.py"), "").unwrap();
    let uv = tools.join("uv");
    std::fs::write(&uv, "#!/bin/sh\nexec sleep 600\n").unwrap();
    std::fs::set_permissions(&uv, std::fs::Permissions::from_mode(0o755)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let built = build_child(&project, Some(&tools), deadline).await;
    assert!(built.is_err_and(|why| why.contains("deadline")));
}
