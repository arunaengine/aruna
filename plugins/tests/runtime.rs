//! Runs the committed test guest: host calls, grant gates, effect ordinals and run budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_plugins::artifact::read_manifest;
use aruna_plugins::broker::{
    Broker, CallContext, Document, Entry, ErrorKind, HostError, Intent, Listing, MetadataPatch,
    ObjectInfo, ObjectPage, Response, SendRequest, StatePut,
};
use aruna_plugins::limits::{Limits, RuntimeConfig};
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::Permission;
use aruna_plugins::{
    CancellationToken, CompileError, FailureKind, Invocation, InvokeError, Limit, Runtime, Trigger,
    WatchEvent,
};
use wasm_encoder::{ComponentSection, CustomSection};

const FIXTURE: &[u8] = include_bytes!("fixture.wasm");
/// Generous cap so a broken run fails the test instead of hanging it.
const HANG_CAP: Duration = Duration::from_secs(120);

/// Records every call and answers with fixed values.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    intents: Mutex<Vec<Intent>>,
}

impl Recorder {
    fn note(&self, call: &str) {
        self.calls.lock().unwrap().push(call.to_string());
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn intents(&self) -> Vec<Intent> {
        self.intents.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Broker for Recorder {
    async fn granted(&self, _: &CallContext, permission: Permission, resource: &str) -> bool {
        self.note("granted");
        permission == Permission::MetadataRead && resource == "D1"
    }

    async fn metadata_get(&self, _: &CallContext, resource: &str) -> Result<Document, HostError> {
        self.note("metadata_get");
        Ok(Document {
            resource: resource.to_string(),
            revision: "r1".into(),
            json: r#"{"name":"Run 7"}"#.into(),
        })
    }

    async fn metadata_list(
        &self,
        _: &CallContext,
        _: Option<String>,
    ) -> Result<Listing, HostError> {
        self.note("metadata_list");
        Ok(Listing {
            resources: vec!["D1".into()],
            cursor: None,
        })
    }

    async fn metadata_patch(
        &self,
        _: &CallContext,
        patch: MetadataPatch,
    ) -> Result<String, HostError> {
        self.note("metadata_patch");
        self.intents.lock().unwrap().push(patch.intent);
        Ok(format!("{}+", patch.expected))
    }

    async fn objects_info(&self, _: &CallContext, path: &str) -> Result<ObjectInfo, HostError> {
        self.note("objects_info");
        Ok(ObjectInfo {
            path: path.to_string(),
            size: 7,
            version: "v1".into(),
        })
    }

    async fn objects_list(
        &self,
        _: &CallContext,
        _: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        self.note("objects_list");
        Err(HostError::new(ErrorKind::Unavailable, "not in this test"))
    }

    async fn connection_send(
        &self,
        _: &CallContext,
        request: SendRequest,
    ) -> Result<Response, HostError> {
        self.note("connection_send");
        if let Some(intent) = request.intent {
            self.intents.lock().unwrap().push(intent);
        }
        Ok(Response {
            status: 201,
            json: Some(r#"{"id":"c-1"}"#.into()),
        })
    }

    async fn state_get(&self, _: &CallContext, _: &str) -> Result<Option<Entry>, HostError> {
        self.note("state_get");
        Ok(Some(Entry {
            version: 1,
            value: b"1".to_vec(),
        }))
    }

    async fn state_put(&self, _: &CallContext, _: StatePut) -> Result<u64, HostError> {
        self.note("state_put");
        Ok(1)
    }
}

/// Node ceilings above the run budgets, so each test's own limit is the one that ends a run.
fn runtime() -> Runtime {
    let mut config = RuntimeConfig::default();
    config.ceiling.fuel = 1 << 50;
    Runtime::new(config).unwrap()
}

fn call(handler: &str, trigger: Trigger) -> Invocation {
    Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        handler: handler.into(),
        settings: r#"{"api_url":"https://api.example.org"}"#.into(),
        trigger,
        permissions: BTreeSet::from([Permission::MetadataRead, Permission::MetadataWrite]),
        connections: BTreeSet::from(["api".to_string()]),
        limits: Limits::default(),
        cancel: CancellationToken::new(),
    }
}

fn probe(input: serde_json::Value) -> Invocation {
    call("probe", Trigger::Manual(input.to_string()))
}

async fn invoke(call: Invocation, broker: Arc<Recorder>) -> Result<Option<String>, InvokeError> {
    let runtime = runtime();
    let compiled = runtime.prepare(FIXTURE.to_vec()).await.unwrap();
    tokio::time::timeout(HANG_CAP, runtime.invoke(&compiled, call, broker))
        .await
        .expect("the run ended within the hang cap")
}

fn output(result: Result<Option<String>, InvokeError>) -> serde_json::Value {
    serde_json::from_str(&result.unwrap().unwrap()).unwrap()
}

#[tokio::test]
async fn echo_round_trip() {
    let event = WatchEvent {
        kind: "dataset_created".into(),
        path: "meta/group-1/D1".into(),
        resource: "D1".into(),
        revision: "r1".into(),
    };
    let reply = invoke(call("echo", Trigger::Event(event)), Arc::default()).await;
    assert_eq!(
        reply.unwrap().as_deref(),
        Some(
            r#"run-1 install-1 group-1 {"api_url":"https://api.example.org"} change dataset_created D1 r1"#
        )
    );
}

#[tokio::test]
async fn calls_reach_broker() {
    let broker = Arc::new(Recorder::default());
    let doc = invoke(
        probe(serde_json::json!({"call": "metadata_get", "resource": "D1"})),
        broker.clone(),
    )
    .await;
    assert_eq!(
        output(doc),
        serde_json::json!({"revision": "r1", "json": {"name": "Run 7"}})
    );
    let sent = invoke(probe(serde_json::json!({"call": "send", "connection": "api", "method": "POST", "path": "/x", "body": {"a": 1}})), broker.clone()).await;
    assert_eq!(
        output(sent),
        serde_json::json!({"status": 201, "json": {"id": "c-1"}})
    );
    let state = invoke(probe(serde_json::json!({"call": "state"})), broker.clone()).await;
    assert_eq!(
        output(state),
        serde_json::json!({"version": 1, "stored": 1})
    );
    let granted = invoke(
        probe(
            serde_json::json!({"call": "granted", "permission": "metadata.read", "resource": "D1"}),
        ),
        broker.clone(),
    )
    .await;
    assert_eq!(output(granted), serde_json::json!(true));
    let unknown = invoke(
        probe(serde_json::json!({"call": "granted", "permission": "files.read", "resource": "D1"})),
        broker.clone(),
    )
    .await;
    assert_eq!(output(unknown), serde_json::json!(false));
    let calls = [
        "metadata_get",
        "connection_send",
        "state_put",
        "state_get",
        "granted",
    ];
    assert_eq!(broker.calls(), calls);
}

#[tokio::test]
async fn missing_grant_stops() {
    let broker = Arc::new(Recorder::default());
    let mut objects = probe(serde_json::json!({"call": "objects_info", "path": "s3/x"}));
    objects.permissions.remove(&Permission::ObjectsRead);
    let Err(InvokeError::Guest(failure)) = invoke(objects, broker.clone()).await else {
        panic!("a call without a grant fails the run");
    };
    assert_eq!(failure.kind, FailureKind::Permanent);
    assert_eq!(
        failure.message,
        "permission `objects.read` is not granted for `s3/x`"
    );
    let mut send = probe(serde_json::json!({"call": "send", "connection": "api", "path": "/x"}));
    send.connections.clear();
    let Err(InvokeError::Guest(failure)) = invoke(send, broker.clone()).await else {
        panic!("a connection without a grant fails the run");
    };
    assert_eq!(
        failure.message,
        "permission `connections.send` is not granted for `api`"
    );
    assert!(broker.calls().is_empty(), "nothing reached the broker");
}

#[tokio::test]
async fn effects_get_ordinals() {
    let patch = || {
        probe(
            serde_json::json!({"call": "metadata_patch", "resource": "D1", "patch": {"a": 1}, "expected": "r1"}),
        )
    };
    let first = Arc::new(Recorder::default());
    assert_eq!(
        output(invoke(patch(), first.clone()).await),
        serde_json::json!(["r1+", "r1++"])
    );
    let rerun = Arc::new(Recorder::default());
    invoke(patch(), rerun.clone()).await.unwrap();
    let intents = first.intents();
    assert_eq!(
        intents.iter().map(|i| i.ordinal).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_ne!(
        intents[0].digest, intents[1].digest,
        "other arguments, other digest"
    );
    assert_eq!(
        rerun.intents(),
        intents,
        "a rerun asks for the same effects"
    );
    let reads = Arc::new(Recorder::default());
    invoke(
        probe(
            serde_json::json!({"call": "send", "connection": "api", "method": "GET", "path": "/x"}),
        ),
        reads.clone(),
    )
    .await
    .unwrap();
    assert!(reads.intents().is_empty(), "a GET is not an effect");
}

#[tokio::test]
async fn budgets_end_runs() {
    let limited = |handler: &str, edit: fn(&mut Limits)| {
        let mut run = call(handler, Trigger::Timer(0));
        edit(&mut run.limits);
        run
    };
    type Case = (&'static str, fn(&mut Limits), Limit);
    let cases: [Case; 3] = [
        ("spin", |l| l.fuel = 50_000_000, Limit::Fuel),
        ("hoard", |l| l.memory = 8 << 20, Limit::Memory),
        ("chatty", |l| l.calls = 50, Limit::Calls),
    ];
    for (handler, edit, limit) in cases {
        match invoke(limited(handler, edit), Arc::default()).await {
            Err(InvokeError::Limit(actual)) => assert_eq!(actual, limit, "{handler}"),
            other => panic!("{handler}: {other:?}"),
        }
    }
    let slow = limited("spin", |l| {
        l.fuel = 1 << 50;
        l.wall = Duration::from_millis(200);
    });
    let ended = invoke(slow, Arc::default()).await;
    assert!(matches!(ended, Err(InvokeError::Timeout)), "{ended:?}");
    let cancelled = limited("spin", |_| {});
    cancelled.cancel.cancel();
    assert!(matches!(
        invoke(cancelled, Arc::default()).await,
        Err(InvokeError::Cancelled)
    ));
    let mut big = probe(serde_json::json!({"call": "settings"}));
    big.settings = format!(r#"{{"pad":"{}"}}"#, "x".repeat(2 << 20));
    assert!(matches!(
        invoke(big, Arc::default()).await,
        Err(InvokeError::Limit(Limit::Input))
    ));
    let unknown = call("resync", Trigger::Timer(0));
    assert!(matches!(
        invoke(unknown, Arc::default()).await,
        Err(InvokeError::Handler(_))
    ));
}

#[tokio::test]
async fn compile_checks_imports() {
    let manifest = read_manifest(FIXTURE).unwrap();
    let json = serde_json::to_vec(&manifest).unwrap();
    let mut section = Vec::new();
    CustomSection {
        name: "aruna-plugin".into(),
        data: json.into(),
    }
    .append_to_component(&mut section);
    let component = FIXTURE
        .strip_suffix(section.as_slice())
        .expect("the manifest is the last section");
    let mut narrow: Manifest = manifest.clone();
    narrow
        .permissions
        .optional
        .remove(&Permission::MetadataWrite);
    let artifact = aruna_plugins::artifact::attach_manifest(component, &narrow).unwrap();
    match runtime().compile(&artifact) {
        Err(CompileError::Manifest(error)) => assert_eq!(
            error.to_string(),
            "the code imports `aruna:plugin/metadata-write@0.2.0`, which the manifest does not request"
        ),
        Err(other) => panic!("{other}"),
        Ok(_) => panic!("an undeclared import must not compile"),
    }
    let compiled = runtime().compile(FIXTURE).unwrap();
    let handlers: Vec<&str> = compiled
        .manifest
        .handlers
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(handlers, ["chatty", "echo", "hoard", "probe", "spin"]);
}
