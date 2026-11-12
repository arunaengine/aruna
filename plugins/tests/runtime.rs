//! Runs the committed test guest: host calls, grant gates, effect ordinals and run budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(feature = "runtime")]

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
    CancellationToken, FailureKind, Invocation, InvokeError, Limit, Runtime, Trigger, WatchEvent,
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
    /// Ordinals of writes whose outcome the runtime gave up on.
    abandoned: Mutex<Vec<u32>>,
    /// Signals that a call to the `block` resource is waiting.
    blocked: tokio::sync::Notify,
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

    fn abandoned(&self) -> Vec<u32> {
        self.abandoned.lock().unwrap().clone()
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
        match resource {
            "missing" => return Err(HostError::new(ErrorKind::NotFound, "x".repeat(200))),
            "huge" => {
                return Ok(Document {
                    resource: resource.to_string(),
                    revision: "r1".into(),
                    json: format!(r#"{{"name":"{}"}}"#, "x".repeat(8192)),
                });
            }
            "slow" => tokio::time::sleep(Duration::from_millis(50)).await,
            "block" => {
                self.blocked.notify_one();
                std::future::pending::<()>().await;
            }
            _ => {}
        }
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
        match patch.resource.as_str() {
            "stall" => std::future::pending::<()>().await,
            "slow" => tokio::time::sleep(Duration::from_millis(1500)).await,
            _ => {}
        }
        self.note("metadata_patch_done");
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
        // The broker answers with the exposed fields only, keyed by pointer.
        let answer = r#"{"/id":"c-1","/meta/name":"n","/items/1":{"b":2}}"#;
        Ok(Response {
            status: 201,
            json: Some(answer.into()),
        })
    }

    async fn state_get(&self, _: &CallContext, _: &str) -> Result<Option<Entry>, HostError> {
        self.note("state_get");
        Ok(Some(Entry {
            version: 1,
            value: b"1".to_vec(),
        }))
    }

    async fn state_put(&self, _: &CallContext, put: StatePut) -> Result<u64, HostError> {
        self.note("state_put");
        self.intents.lock().unwrap().push(put.intent);
        Ok(1)
    }

    async fn abandoned(&self, _: &CallContext, intent: &Intent) {
        self.abandoned.lock().unwrap().push(intent.ordinal);
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
    invoke_on(&runtime(), call, broker).await
}

async fn invoke_on(
    runtime: &Runtime,
    call: Invocation,
    broker: Arc<Recorder>,
) -> Result<Option<String>, InvokeError> {
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
        serde_json::json!({"status": 201, "fields": {"/id": "c-1", "/meta/name": "n", "/items/1": {"b": 2}}})
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
async fn narrow_requests_compile() {
    // Toolchains import every interface of the world, so grants, not imports, bound calls.
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
    assert!(runtime().compile(&artifact).is_ok());
    let compiled = runtime().compile(FIXTURE).unwrap();
    let handlers: Vec<&str> = compiled
        .manifest
        .handlers
        .iter()
        .map(String::as_str)
        .collect();
    assert_eq!(handlers, ["chatty", "echo", "hoard", "probe", "spin"]);
}

#[tokio::test(start_paused = true)]
async fn stalled_write_abandoned() {
    let broker = Arc::new(Recorder::default());
    let mut patch = probe(
        serde_json::json!({"call": "metadata_patch", "resource": "stall", "patch": {}, "expected": "r1"}),
    );
    patch.limits.wall = Duration::from_secs(1);
    patch.limits.write = Duration::from_secs(2);
    let ended = invoke(patch, broker.clone()).await;
    assert!(matches!(ended, Err(InvokeError::Timeout)), "{ended:?}");
    assert_eq!(
        broker.abandoned(),
        [1],
        "the stalled write is recorded as unknown"
    );
    assert!(!broker.calls().contains(&"metadata_patch_done".to_string()));
}

#[tokio::test(start_paused = true)]
async fn write_outlives_timeout() {
    let broker = Arc::new(Recorder::default());
    let mut patch = probe(
        serde_json::json!({"call": "metadata_patch", "resource": "slow", "patch": {}, "expected": "r1"}),
    );
    patch.limits.wall = Duration::from_secs(1);
    patch.limits.write = Duration::from_secs(5);
    let ended = invoke(patch, broker.clone()).await;
    assert!(matches!(ended, Err(InvokeError::Timeout)), "{ended:?}");
    assert_eq!(
        broker.calls(),
        ["metadata_patch", "metadata_patch_done"],
        "the write finished first"
    );
    assert!(broker.abandoned().is_empty());
}

#[tokio::test]
async fn state_put_ordinal() {
    let broker = Arc::new(Recorder::default());
    invoke(probe(serde_json::json!({"call": "state"})), broker.clone())
        .await
        .unwrap();
    let rerun = Arc::new(Recorder::default());
    invoke(probe(serde_json::json!({"call": "state"})), rerun.clone())
        .await
        .unwrap();
    let intents = broker.intents();
    assert_eq!(intents.iter().map(|i| i.ordinal).collect::<Vec<_>>(), [1]);
    assert_eq!(rerun.intents(), intents, "a rerun asks for the same write");
}

#[tokio::test]
async fn error_bytes_bounded() {
    let mut errors = probe(serde_json::json!({"call": "errors"}));
    errors.limits.bytes = 4096;
    let ended = invoke(errors, Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Bytes))),
        "{ended:?}"
    );
    let mut refused = probe(serde_json::json!({"call": "errors"}));
    refused.limits.bytes = 4096;
    refused.permissions.clear();
    let ended = invoke(refused, Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Bytes))),
        "missing grants count: {ended:?}"
    );
}

#[tokio::test]
async fn oversized_replies_caught() {
    // A reply over the per-call bound reaches the guest as an error it returns as a failure,
    // instead of ending the run on the byte budget.
    let mut huge = probe(serde_json::json!({"call": "metadata_get", "resource": "huge"}));
    huge.limits.call_bytes = 4096;
    let ended = invoke(huge, Arc::default()).await;
    let Err(InvokeError::Guest(failure)) = ended else {
        panic!("the guest saw no error: {ended:?}");
    };
    assert!(
        failure.message.contains("larger than 4096 bytes"),
        "{failure:?}"
    );
    // A run whose remaining bytes cannot take a reply is refused before the call is sent.
    let broker = Arc::new(Recorder::default());
    let mut short = probe(serde_json::json!({"call": "metadata_get", "resource": "D1"}));
    (short.limits.call_bytes, short.limits.bytes) = (4096, 4096);
    let ended = invoke(short, broker.clone()).await;
    let Err(InvokeError::Guest(failure)) = ended else {
        panic!("the guest saw no error: {ended:?}");
    };
    assert!(failure.message.contains("too few bytes"), "{failure:?}");
    assert!(broker.calls().is_empty(), "{:?}", broker.calls());
}

#[tokio::test]
async fn pending_calls_bounded() {
    let mut get = probe(serde_json::json!({"call": "metadata_get", "resource": "D1"}));
    get.limits.pending = 0;
    let ended = invoke(get, Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Pending))),
        "{ended:?}"
    );
}

#[tokio::test]
async fn node_admission_bounded() {
    let config = RuntimeConfig {
        invocations: 1,
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::new(config).unwrap();
    let broker = Arc::new(Recorder::default());
    let first = probe(serde_json::json!({"call": "metadata_get", "resource": "block"}));
    let cancel = first.cancel.clone();
    let running = {
        let (runtime, broker) = (runtime.clone(), broker.clone());
        tokio::spawn(async move { invoke_on(&runtime, first, broker).await })
    };
    broker.blocked.notified().await;
    let second = invoke_on(&runtime, call("echo", Trigger::Timer(0)), broker.clone()).await;
    assert!(matches!(second, Err(InvokeError::Overloaded)), "{second:?}");
    cancel.cancel();
    let first = running.await.unwrap();
    assert!(matches!(first, Err(InvokeError::Cancelled)), "{first:?}");
}

#[tokio::test]
async fn wasi_calls_bounded() {
    let random = |len: u64| probe(serde_json::json!({"call": "random", "len": len}));
    assert_eq!(
        output(invoke(random(64), Arc::default()).await),
        serde_json::json!(64)
    );
    let large = invoke(random(2 << 20), Arc::default()).await;
    assert!(
        matches!(large, Err(InvokeError::Limit(Limit::Bytes))),
        "per call: {large:?}"
    );
    let mut total = random(200);
    total.limits.bytes = 100;
    let ended = invoke(total, Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Bytes))),
        "in total: {ended:?}"
    );
    let clock = probe(serde_json::json!({"call": "clock"}));
    assert_eq!(
        output(invoke(clock.clone(), Arc::default()).await),
        serde_json::json!(true)
    );
    let mut counted = random(8);
    counted.limits.calls = 0;
    let ended = invoke(counted, Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Calls))),
        "WASI calls count: {ended:?}"
    );
}

#[tokio::test]
async fn fill_keeps_calls() {
    // A large fill costs much fuel at once; neither it nor the yields may change the count.
    for slow in [false, true] {
        let mut input =
            serde_json::json!({"call": "fill", "size": 8 << 20, "spin": 50_000_000u64, "logs": 5});
        if slow {
            input["slow"] = true.into();
        }
        let allowed = 5 + u32::from(slow);
        let mut exact = probe(input.clone());
        exact.limits.calls = allowed;
        assert_eq!(
            output(invoke(exact, Arc::default()).await),
            serde_json::json!(1),
            "slow {slow}"
        );
        let mut short = probe(input);
        short.limits.calls = allowed - 1;
        let ended = invoke(short, Arc::default()).await;
        assert!(
            matches!(ended, Err(InvokeError::Limit(Limit::Calls))),
            "slow {slow}: {ended:?}"
        );
    }
}

#[tokio::test]
async fn streams_charged() {
    let bounded = |input: serde_json::Value| {
        let mut run = probe(input);
        run.limits.bytes = 64 << 10;
        run
    };
    for call in ["write", "zeroes"] {
        let within = bounded(serde_json::json!({"call": call, "size": 4096, "times": 10}));
        assert_eq!(
            output(invoke(within, Arc::default()).await),
            serde_json::json!(true),
            "{call}"
        );
        let over = bounded(serde_json::json!({"call": call, "size": 4096, "times": 20}));
        let ended = invoke(over, Arc::default()).await;
        assert!(
            matches!(ended, Err(InvokeError::Limit(Limit::Bytes))),
            "{call}: {ended:?}"
        );
    }
    // Poll lists stay under the store's handle limit; a 1 KiB budget makes 500 entries too many.
    let polled = |size: u64| {
        let mut run = probe(serde_json::json!({"call": "poll", "size": size}));
        run.limits.bytes = 1024;
        run
    };
    assert_eq!(
        output(invoke(polled(10), Arc::default()).await),
        serde_json::json!(10)
    );
    let ended = invoke(polled(500), Arc::default()).await;
    assert!(
        matches!(ended, Err(InvokeError::Limit(Limit::Bytes))),
        "poll: {ended:?}"
    );
}
