//! Runs the committed test guest: host calls, grant gates, effect ordinals and run budgets.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![cfg(feature = "runtime")]

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aruna_plugins::artifact::read_manifest;
use aruna_plugins::broker::{
    Broker, CallContext, Created, Document, Entry, ErrorKind, FileChange, FilePage, HostError,
    Intent, JobCopy, JobStatus, Listing, MetadataCreate, MetadataPatch, ObjectInfo, ObjectPage,
    Response, SendRequest, StatePut, StateRemove,
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

    async fn metadata_summary(
        &self,
        _: &CallContext,
        resource: &str,
    ) -> Result<Document, HostError> {
        self.note("metadata_summary");
        Ok(Document {
            resource: resource.to_string(),
            revision: "r1".into(),
            json: r#"{"name":"Run 7"}"#.into(),
        })
    }

    async fn metadata_files(
        &self,
        _: &CallContext,
        _: &str,
        cursor: Option<String>,
    ) -> Result<FilePage, HostError> {
        self.note("metadata_files");
        Ok(FilePage {
            files: vec![r#"{"@id":"a.txt","@type":"File"}"#.into()],
            cursor: cursor.map(|cursor| format!("{cursor}+")),
        })
    }

    async fn metadata_put_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        self.note("metadata_put_file");
        self.intents.lock().unwrap().push(change.intent);
        Ok(format!("{}+", change.expected))
    }

    async fn metadata_remove_file(
        &self,
        _: &CallContext,
        change: FileChange,
    ) -> Result<String, HostError> {
        self.note("metadata_remove_file");
        self.intents.lock().unwrap().push(change.intent);
        Ok(format!("{}-", change.expected))
    }

    async fn metadata_create(
        &self,
        _: &CallContext,
        create: MetadataCreate,
    ) -> Result<Created, HostError> {
        self.note("metadata_create");
        assert_eq!(create.key, create.path, "a create is named by its path");
        self.intents.lock().unwrap().push(create.intent);
        Ok(Created {
            resource: format!("{}-doc", create.key),
            revision: "r0".into(),
        })
    }

    async fn objects_info(&self, _: &CallContext, path: &str) -> Result<ObjectInfo, HostError> {
        self.note("objects_info");
        Ok(ObjectInfo {
            path: path.to_string(),
            size: 7,
            version: "v1".into(),
            marked: false,
        })
    }

    async fn objects_read(
        &self,
        _: &CallContext,
        path: &str,
        version: Option<String>,
    ) -> Result<Vec<u8>, HostError> {
        self.note(&format!("read {path} {}", version.unwrap_or_default()));
        Ok(b"small".to_vec())
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
        // The broker answers with the whole answer.
        let answer = r#"{"id":"c-1","meta":{"name":"n"},"items":[1,{"b":2}]}"#;
        Ok(Response {
            status: 201,
            headers: Vec::new(),
            body: answer.into(),
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

    async fn jobs_copy(&self, _: &CallContext, copy: JobCopy) -> Result<String, HostError> {
        let version = copy.version.unwrap_or_default();
        self.note(&format!("copy {} {version} {}", copy.source, copy.target));
        self.intents.lock().unwrap().push(copy.intent);
        Ok("job-1".into())
    }

    async fn jobs_get(&self, _: &CallContext, id: &str) -> Result<JobStatus, HostError> {
        self.note(&format!("job {id}"));
        Ok(JobStatus {
            id: id.into(),
            kind: "copy".into(),
            path: "out/a".into(),
            state: "succeeded".into(),
            message: None,
            bytes: Some(5),
            version: None,
            reply_status: None,
            reply_headers: Vec::new(),
            reply_body: None,
            exit_code: None,
            report: None,
        })
    }

    async fn jobs_cancel(
        &self,
        _: &CallContext,
        id: &str,
        intent: Intent,
    ) -> Result<(), HostError> {
        self.note(&format!("cancel {id}"));
        self.intents.lock().unwrap().push(intent);
        Ok(())
    }

    async fn jobs_outputs(
        &self,
        _: &CallContext,
        id: &str,
        _: Option<String>,
    ) -> Result<ObjectPage, HostError> {
        self.note(&format!("outputs {id}"));
        let object = ObjectInfo {
            path: "out/b".into(),
            size: 1,
            version: "v".into(),
            marked: true,
        };
        Ok(ObjectPage {
            objects: vec![object],
            cursor: None,
        })
    }

    async fn state_remove(&self, _: &CallContext, remove: StateRemove) -> Result<(), HostError> {
        self.note(&format!("state_remove {} {}", remove.key, remove.expected));
        self.intents.lock().unwrap().push(remove.intent);
        Ok(())
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

fn call(trigger: Trigger) -> Invocation {
    Invocation {
        run: "run-1".into(),
        install: "install-1".into(),
        group: "group-1".into(),
        settings: r#"{"api_url":"https://api.example.org"}"#.into(),
        trigger,
        permissions: BTreeSet::from([Permission::MetadataRead, Permission::MetadataWrite]),
        connections: BTreeSet::from(["api".to_string()]),
        limits: Limits::default(),
        cancel: CancellationToken::new(),
    }
}

fn probe(input: serde_json::Value) -> Invocation {
    call(Trigger::Manual(input.to_string()))
}

/// An event of `kind`; the fixture runs `echo` or `hoard` for it by its manifest.
fn event(kind: &str) -> Trigger {
    Trigger::Event(WatchEvent {
        kind: kind.into(),
        path: "p".into(),
        resource: "r".into(),
        revision: "1".into(),
    })
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
    let reply = invoke(call(Trigger::Event(event)), Arc::default()).await;
    assert_eq!(
        output(reply),
        r#"run-1 install-1 group-1 {"api_url":"https://api.example.org"} change dataset_created D1 r1"#
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
        serde_json::json!({"status": 201, "fields": {"id": "c-1", "meta": {"name": "n"}, "items": [1, {"b": 2}]}})
    );
    let state = invoke(probe(serde_json::json!({"call": "state"})), broker.clone()).await;
    assert_eq!(
        output(state),
        serde_json::json!({"version": 1, "stored": 1})
    );
    let calls = ["metadata_get", "connection_send", "state_put", "state_get"];
    assert_eq!(broker.calls(), calls);
}

#[tokio::test]
async fn dataset_calls_reach() {
    // The rebuilt guest reaches every dataset call; a create needs its own permission.
    let broker = Arc::new(Recorder::default());
    let mut create = probe(
        serde_json::json!({"call": "metadata_create", "path": "eln/1", "dataset": {"name": "E"}}),
    );
    create.permissions.insert(Permission::MetadataCreate);
    let created = invoke(create, broker.clone()).await;
    assert_eq!(
        output(created),
        serde_json::json!({"resource": "eln/1-doc", "revision": "r0"})
    );
    let summary = probe(serde_json::json!({"call": "metadata_summary", "resource": "D1"}));
    let summary = invoke(summary, broker.clone()).await;
    assert_eq!(
        output(summary),
        serde_json::json!({"revision": "r1", "json": {"name": "Run 7"}})
    );
    let files =
        probe(serde_json::json!({"call": "metadata_files", "resource": "D1", "cursor": "c"}));
    let files = invoke(files, broker.clone()).await;
    assert_eq!(
        output(files),
        serde_json::json!({"files": [{"@id": "a.txt", "@type": "File"}], "cursor": "c+"})
    );
    let file = serde_json::json!({"@id": "a.txt", "@type": "File", "name": "a"});
    let put = probe(
        serde_json::json!({"call": "put_file", "resource": "D1", "file": file, "expected": "r1"}),
    );
    assert_eq!(
        output(invoke(put, broker.clone()).await),
        serde_json::json!("r1+")
    );
    let removed = probe(
        serde_json::json!({"call": "remove_file", "resource": "D1", "id": "a.txt", "expected": "r1"}),
    );
    assert_eq!(
        output(invoke(removed, broker.clone()).await),
        serde_json::json!("r1-")
    );
    let calls = [
        "metadata_create",
        "metadata_summary",
        "metadata_files",
        "metadata_put_file",
        "metadata_remove_file",
    ];
    assert_eq!(broker.calls(), calls);

    let refused =
        probe(serde_json::json!({"call": "metadata_create", "path": "eln/2", "dataset": {}}));
    let Err(InvokeError::Guest(failure)) = invoke(refused, broker.clone()).await else {
        panic!("a create without its permission fails the run");
    };
    assert!(
        failure.message.contains("metadata.create"),
        "{}",
        failure.message
    );
    let event = WatchEvent {
        kind: "dataset_updated".into(),
        path: "meta/group-1/D1".into(),
        resource: "D1".into(),
        revision: "r2".into(),
    };
    let reply = invoke(call(Trigger::Event(event)), Arc::default()).await;
    let reply = output(reply);
    assert!(
        reply
            .as_str()
            .unwrap()
            .ends_with("change dataset_updated D1 r2"),
        "{reply}"
    );
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
        "not allowed: permission `objects.read` is not granted to this install"
    );
    let mut send = probe(serde_json::json!({"call": "send", "connection": "api", "path": "/x"}));
    send.connections.clear();
    let Err(InvokeError::Guest(failure)) = invoke(send, broker.clone()).await else {
        panic!("a connection without a grant fails the run");
    };
    assert_eq!(
        failure.message,
        "not allowed: connection `api` is not granted to this install"
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
    let limited = |mut run: Invocation, edit: fn(&mut Limits)| {
        edit(&mut run.limits);
        run
    };
    let timer = || call(Trigger::Timer(0));
    type Case = (Invocation, fn(&mut Limits), Limit);
    let cases: [Case; 3] = [
        (timer(), |l| l.fuel = 50_000_000, Limit::Fuel),
        (
            call(event("data_uploaded")),
            |l| l.memory = 8 << 20,
            Limit::Memory,
        ),
        (
            probe(serde_json::json!({"call": "chatty"})),
            |l| l.calls = 50,
            Limit::Calls,
        ),
    ];
    for (run, edit, limit) in cases {
        match invoke(limited(run, edit), Arc::default()).await {
            Err(InvokeError::Limit(actual)) => assert_eq!(actual, limit),
            other => panic!("{limit}: {other:?}"),
        }
    }
    let slow = limited(timer(), |l| {
        l.fuel = 1 << 50;
        l.wall = Duration::from_millis(200);
    });
    let ended = invoke(slow, Arc::default()).await;
    assert!(matches!(ended, Err(InvokeError::Timeout)), "{ended:?}");
    let cancelled = limited(timer(), |_| {});
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
    // A trigger the code registered no function for fails as invalid input.
    let unknown = call(event("dataset_deleted"));
    match invoke(unknown, Arc::default()).await {
        Err(InvokeError::Guest(failure)) => assert_eq!(failure.kind, FailureKind::InvalidInput),
        other => panic!("an unknown trigger ran: {other:?}"),
    }
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
    let runtime = runtime();
    let compiled = runtime.compile(FIXTURE).unwrap();
    let triggers = runtime.triggers(&compiled).await.unwrap();
    let triggers: Vec<&str> = triggers.iter().map(String::as_str).collect();
    let all = [
        "data_uploaded",
        "dataset_created",
        "dataset_updated",
        "job_finished",
        "manual",
    ];
    assert_eq!(
        triggers,
        [&all[..], &["sync_completed", "sync_failed", "timer"]].concat()
    );
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
async fn copy_answers_id() {
    // A copy reaches the broker with its paths and pinned version, as a write with an ordinal,
    // and needs the copy permission.
    let input =
        serde_json::json!({"call": "copy", "source": "in/a", "version": "V1", "target": "out/a"});
    let mut copy = probe(input.clone());
    copy.permissions.insert(Permission::ObjectsCopy);
    let broker = Arc::new(Recorder::default());
    assert_eq!(output(invoke(copy, broker.clone()).await), "job-1");
    assert_eq!(broker.calls(), ["copy in/a V1 out/a"]);
    assert_eq!(broker.intents()[0].ordinal, 1);
    let refused = invoke(probe(input), Arc::default()).await;
    assert!(matches!(refused, Err(InvokeError::Guest(_))), "{refused:?}");
}

#[tokio::test]
async fn small_reads_granted() {
    // A read needs `objects.read` and returns the bytes the broker read.
    let input = serde_json::json!({"call": "read", "path": "in/a", "version": "V1"});
    let mut read = probe(input.clone());
    read.permissions.insert(Permission::ObjectsRead);
    let broker = Arc::new(Recorder::default());
    assert_eq!(output(invoke(read, broker.clone()).await), "small");
    assert_eq!(broker.calls(), ["read in/a V1"]);
    let refused = invoke(probe(input), Arc::default()).await;
    assert!(matches!(refused, Err(InvokeError::Guest(_))), "{refused:?}");
}

#[tokio::test]
async fn remove_after_put() {
    // A removal is a write after the put, at the version the put answered.
    let broker = Arc::new(Recorder::default());
    let reply = invoke(
        probe(serde_json::json!({"call": "state_remove", "key": "k"})),
        broker.clone(),
    );
    assert_eq!(output(reply.await), false);
    assert_eq!(
        broker.calls(),
        ["state_put", "state_remove k 1", "state_get"]
    );
    let ordinals: Vec<u32> = broker
        .intents()
        .iter()
        .map(|intent| intent.ordinal)
        .collect();
    assert_eq!(ordinals, [1, 2]);
}

#[tokio::test]
async fn cancel_takes_ordinal() {
    let broker = Arc::new(Recorder::default());
    let reply = invoke(
        probe(serde_json::json!({"call": "cancel", "id": "j1"})),
        broker.clone(),
    );
    assert_eq!(output(reply.await), true);
    assert_eq!(broker.calls(), ["cancel j1"]);
    assert_eq!(broker.intents()[0].ordinal, 1);
}

#[tokio::test]
async fn job_status_read() {
    // Status and outputs need no grant beyond the install's own jobs.
    let broker = Arc::new(Recorder::default());
    let reply = invoke(
        probe(serde_json::json!({"call": "job", "id": "j1"})),
        broker.clone(),
    );
    let expected = serde_json::json!({"kind": "copy", "state": "succeeded", "path": "out/a",
        "bytes": 5, "outputs": ["out/b"]});
    assert_eq!(output(reply.await), expected);
    assert_eq!(broker.calls(), ["job j1", "outputs j1"]);
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
    let second = invoke_on(&runtime, call(event("dataset_created")), broker.clone()).await;
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
