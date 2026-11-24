//! Tests the workflow run routes through their router: start, list, snapshot, tasks, logs, the
//! event stream, cancel, resume, repeat, delete, the reports, and runs through the real coordinator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::execution::workflow_run::RunState;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_ops_work::workflows::runs::events::task_json;
use axum::Extension;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::run_wire::WorkflowTaskBody;
use super::tests::{Node, Scripted, node, upload};
use crate::auth::ValidatedBearer;
use crate::tests::routes::write_doc;

/// One request with extra headers; the body comes back as text, so a stream can be read whole.
async fn send(
    node: &Node,
    auth: &AuthContext,
    (method, uri): (&str, &str),
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (StatusCode, String) {
    let (router, _) = super::router().split_for_parts();
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let body = body.map_or_else(Body::empty, |body| Body::from(body.to_string()));
    let router = router
        .layer(Extension(Some(auth.clone())))
        .layer(Extension(Option::<ValidatedBearer>::None))
        .with_state(node.state.clone());
    let response = router.oneshot(request.body(body).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

fn json_of(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or(Value::Null)
}

/// The `(id, event)` pairs of an event stream, and the data of each frame.
fn frames(text: &str) -> Vec<(u64, String, Value)> {
    let mut found = Vec::new();
    for block in text.split("\n\n").filter(|block| !block.trim().is_empty()) {
        let field = |name: &str| {
            (block.lines())
                .find_map(|line| line.strip_prefix(&format!("{name}:")))
                .map(|value| value.trim().to_owned())
        };
        let (Some(id), Some(kind), Some(data)) = (field("id"), field("event"), field("data"))
        else {
            continue;
        };
        found.push((id.parse().unwrap(), kind, json_of(&data)));
    }
    found
}

/// Gives this node a job-control band, so it mints the jobs of its runs like an onboarded node.
async fn bind_jobs(node: &Node) {
    let (node_id, realm_id) = (node.state.get_node_id(), node.state.get_realm_id());
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::Server);
    config.seed_default_placement();
    config.seed_job_control(node_id, 0);
    let actor = Actor {
        node_id,
        user_id: node.owner.user_id,
        realm_id,
    };
    let bytes = config.to_bytes(&actor).unwrap();
    let key = (*realm_id.as_bytes()).into();
    write_doc(
        &node.state.get_ctx(),
        REALM_CONFIG_KEYSPACE,
        key,
        bytes.into(),
    )
    .await;
}

async fn workflow(node: &Node) -> String {
    let (status, created) = node.create(upload(node.group_id)).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    bind_jobs(node).await;
    let context = node.state.get_ctx();
    let owner = node.owner.user_id;
    aruna_runner::testing::seed_bucket(&context, "results", node.group_id, owner, Vec::new()).await;
    created["workflow_id"].as_str().unwrap().to_owned()
}

fn run_body(node: &Node, workflow_id: &str, message: &str) -> Value {
    json!({"group_id": node.group_id.to_string(), "workflow_id": workflow_id, "version": "1",
        "values": {"message": message}, "output": {"bucket": "results", "prefix": "hello/"}})
}

#[tokio::test]
async fn serves_runs() {
    let node = node(Some(Arc::new(Scripted))).await;
    let workflow_id = workflow(&node).await;
    let owner = node.owner.clone();
    let start = ("POST", "/compute/workflow-runs");
    let key = [("Idempotency-Key", "launch-1")];

    let (status, refused) = send(
        &node,
        &node.stranger,
        start,
        &key,
        Some(run_body(&node, &workflow_id, "Hola")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{refused}");
    let mut bare = run_body(&node, &workflow_id, "Hola");
    bare.as_object_mut().unwrap().remove("output");
    let (status, _) = send(&node, &owner, start, &[], Some(bare)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, text) = send(
        &node,
        &owner,
        start,
        &key,
        Some(run_body(&node, &workflow_id, "Hola")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{text}");
    let run = json_of(&text);
    assert_eq!(
        (run["state"].as_str(), run["version"].as_str()),
        (Some("queued"), Some("1"))
    );
    let run_id = run["run_id"].as_str().unwrap().to_owned();
    let (_, again) = send(
        &node,
        &owner,
        start,
        &key,
        Some(run_body(&node, &workflow_id, "Hola")),
    )
    .await;
    assert_eq!(json_of(&again)["run_id"].as_str(), Some(run_id.as_str()));
    let (status, _) = send(
        &node,
        &owner,
        start,
        &key,
        Some(run_body(&node, &workflow_id, "Ciao")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, page) = send(
        &node,
        &owner,
        ("GET", "/compute/workflow-runs?state=queued"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        (status, json_of(&page)["total"].as_u64()),
        (StatusCode::OK, Some(1)),
        "{page}"
    );
    let path = format!("/compute/workflow-runs/{run_id}");
    let (status, snapshot) = send(&node, &owner, ("GET", &path), &[], None).await;
    let snapshot = json_of(&snapshot);
    assert_eq!(status, StatusCode::OK, "{snapshot}");
    assert_eq!(snapshot["last_event_id"], 1);
    assert_eq!(snapshot["values"], json!({"message": "Hola"}));
    assert_eq!(snapshot["output"]["bucket"], "results");
    let (status, _) = send(&node, &node.stranger, ("GET", &path), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, tasks) = send(&node, &owner, ("GET", &format!("{path}/tasks")), &[], None).await;
    assert_eq!(
        (status, json_of(&tasks)["total"].as_u64()),
        (StatusCode::OK, Some(0))
    );
    for missing in ["/tasks/1", "/tasks/1/logs?stream=stderr", "/origin"] {
        let (status, _) = send(
            &node,
            &owner,
            ("GET", &format!("{path}{missing}")),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    }
    let (status, graph) = send(&node, &owner, ("GET", &format!("{path}/graph")), &[], None).await;
    assert_eq!(status, StatusCode::OK, "{graph}");
    assert_eq!(json_of(&graph)["nodes"][0]["id"], "echo");

    let (status, _) = send(&node, &owner, ("DELETE", &path), &[], None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let cancel = format!("{path}/cancel");
    let (status, cancelled) = send(&node, &owner, ("POST", &cancel), &[], None).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{cancelled}");
    assert_eq!(json_of(&cancelled)["state"], RunState::Cancelled.as_str());
    let (status, _) = send(&node, &owner, ("POST", &cancel), &[], None).await;
    assert_eq!(status, StatusCode::CONFLICT);

    let events = format!("{path}/events");
    let (status, stream) = send(&node, &owner, ("GET", &events), &[], None).await;
    assert_eq!(status, StatusCode::OK);
    let all = frames(&stream);
    let kinds: Vec<(u64, &str)> = all
        .iter()
        .map(|(id, kind, _)| (*id, kind.as_str()))
        .collect();
    assert_eq!(kinds, [(1, "run"), (2, "state"), (3, "state")], "{stream}");
    assert!(
        all.iter()
            .all(|(_, _, data)| data["run_id"] == run_id.as_str())
    );
    assert_eq!(all[2].2["state"], "cancelled");
    let after = format!("{events}?after=1");
    let (_, stream) = send(&node, &owner, ("GET", &after), &[], None).await;
    assert_eq!(frames(&stream).len(), 2, "{stream}");
    let (_, stream) = send(
        &node,
        &owner,
        ("GET", &events),
        &[("Last-Event-ID", "2")],
        None,
    )
    .await;
    let last: Vec<u64> = frames(&stream).iter().map(|(id, ..)| *id).collect();
    assert_eq!(last, [3], "{stream}");

    let (status, _) = send(&node, &owner, ("DELETE", &path), &[], None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = send(&node, &owner, ("DELETE", &path), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&node, &owner, ("GET", &events), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn refuses_bad_runs() {
    let node = node(Some(Arc::new(Scripted))).await;
    let workflow_id = workflow(&node).await;
    let owner = node.owner.clone();
    let start = ("POST", "/compute/workflow-runs");
    let mut wrong = run_body(&node, &workflow_id, "Hola");
    wrong["version"] = json!("first");
    let (status, _) = send(&node, &owner, start, &[], Some(wrong)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let mut elsewhere = run_body(&node, &workflow_id, "Hola");
    let other = iroh::SecretKey::from_bytes(&[9; 32]).public().to_string();
    elsewhere["placement"] = json!({"node_id": other});
    let (status, _) = send(&node, &owner, start, &[], Some(elsewhere)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(
        &node,
        &owner,
        ("GET", "/compute/workflow-runs?state=done"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(
        &node,
        &owner,
        ("GET", "/compute/workflow-runs/nope"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// The response of one GET through `router`, with its headers.
async fn fetch(
    node: &Node,
    auth: &AuthContext,
    router: utoipa_axum::router::OpenApiRouter<Arc<crate::server::state::ServerState>>,
    uri: &str,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let (router, _) = router.split_for_parts();
    let router = router
        .layer(Extension(Some(auth.clone())))
        .layer(Extension(Option::<ValidatedBearer>::None))
        .with_state(node.state.clone());
    let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = router.oneshot(request).await.unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, headers, body)
}

// Resume, repeat and the crate wait for the end of a run; every report answers from the run's
// records without a coordinator, `compare` is its own route, and the report is a file to save.
#[tokio::test]
async fn serves_run_reports() {
    let node = node(Some(Arc::new(Scripted))).await;
    let workflow_id = workflow(&node).await;
    let owner = node.owner.clone();
    let start = ("POST", "/compute/workflow-runs");
    let (_, text) = send(
        &node,
        &owner,
        start,
        &[],
        Some(run_body(&node, &workflow_id, "Hola")),
    )
    .await;
    let run_id = json_of(&text)["run_id"].as_str().unwrap().to_owned();
    let path = format!("/compute/workflow-runs/{run_id}");
    for action in ["resume", "repeat"] {
        let (status, _) = send(
            &node,
            &owner,
            ("POST", &format!("{path}/{action}")),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{action}");
    }
    let (status, _) = send(&node, &owner, ("GET", &format!("{path}/crate")), &[], None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, time) = send(&node, &owner, ("GET", &format!("{path}/time")), &[], None).await;
    assert_eq!(
        (status, json_of(&time)["source"].as_str()),
        (StatusCode::OK, Some("record"))
    );

    let compare = "/compute/workflow-runs/compare";
    let (status, _) = send(&node, &owner, ("GET", compare), &[], None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "compare is no run id");
    let both = format!("{compare}?first={run_id}&second={run_id}");
    let (status, compared) = send(&node, &owner, ("GET", &both), &[], None).await;
    assert_eq!(status, StatusCode::OK, "{compared}");
    assert_eq!(json_of(&compared)["same_version"], true);
    let unknown = format!(
        "{compare}?first={run_id}&second={}",
        ulid::Ulid::from_bytes([9; 16])
    );
    let (status, _) = send(&node, &owner, ("GET", &unknown), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&node, &node.stranger, ("GET", &both), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = send(
        &node,
        &owner,
        ("POST", &format!("{path}/cancel")),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _, built) = fetch(&node, &owner, super::router(), &format!("{path}/crate")).await;
    assert_eq!(status, StatusCode::OK, "{built}");
    assert_eq!(
        built["@context"][0],
        "https://w3id.org/ro/crate/1.2/context"
    );
    let graph = built["@graph"].as_array().unwrap();
    let root = graph.iter().find(|entity| entity["@id"] == "./").unwrap();
    assert_eq!(root["mentions"]["@id"], format!("#workflow-run-{run_id}"));
    assert_eq!(root["conformsTo"].as_array().unwrap().len(), 3, "{root}");
    let (status, headers, report) =
        fetch(&node, &owner, super::router(), &format!("{path}/report")).await;
    assert_eq!(status, StatusCode::OK);
    let header = |name: header::HeaderName| headers[name].to_str().unwrap().to_owned();
    assert_eq!(header(header::CONTENT_TYPE), "text/html; charset=utf-8");
    assert_eq!(
        header(header::CONTENT_DISPOSITION),
        format!("attachment; filename=\"run-{run_id}-report.html\"")
    );
    assert_eq!(header(header::X_CONTENT_TYPE_OPTIONS), "nosniff");
    assert_eq!(header(header::CONTENT_SECURITY_POLICY), "sandbox");
    assert!(report.as_str().unwrap().contains(&run_id));
    let (status, _) = send(
        &node,
        &owner,
        ("GET", &format!("{path}/tasks/1/why")),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, changes) = send(
        &node,
        &owner,
        ("GET", &format!("{path}/changes")),
        &[],
        None,
    )
    .await;
    assert_eq!(
        (status, json_of(&changes)["changed"].as_bool()),
        (StatusCode::OK, Some(false))
    );

    let (status, resumed) = send(
        &node,
        &owner,
        ("POST", &format!("{path}/resume")),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{resumed}");
    let resumed = json_of(&resumed);
    assert_eq!(
        (resumed["lineage"].as_str(), resumed["state"].as_str()),
        (Some(run_id.as_str()), Some("queued"))
    );
    assert_ne!(resumed["run_id"].as_str(), Some(run_id.as_str()));
    let (status, _) = send(
        &node,
        &owner,
        ("POST", &format!("{path}/repeat")),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _) = send(
        &node,
        &node.stranger,
        ("POST", &format!("{path}/repeat")),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let runs = crate::routes::execution::runs::router();
    let (status, _, link) = fetch(&node, &owner, runs, &format!("/compute/runs/{run_id}")).await;
    assert_eq!(
        (status, link["run_id"].as_str()),
        (StatusCode::OK, Some(run_id.as_str())),
        "{link}"
    );
    let runs = crate::routes::execution::runs::router();
    let (status, ..) = fetch(
        &node,
        &node.stranger,
        runs,
        &format!("/compute/runs/{run_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// The `task` event and the task route give the same fields, so the portal merges them alike.
#[test]
fn event_matches_route() {
    let task = aruna_ops_work::workflows::runs::run::changed_task(
        ulid::Ulid::from_bytes([1; 16]),
        &aruna_workflow_contract::message::TaskChange {
            task_id: 4,
            name: "echo-4-1".into(),
            step: "echo".into(),
            state: aruna_workflow_contract::message::TaskState::Finished,
            attempt: Some(1),
            time_ms: 5,
            started_ms: Some(1_000),
            ended_ms: Some(2_000),
            cached: false,
            exit_code: Some(0),
            error: None,
        },
    );
    let mut event = task_json(&task);
    event.as_object_mut().unwrap().remove("run_id");
    let route = serde_json::to_value(WorkflowTaskBody::from(&task)).unwrap();
    assert_eq!(event, route);
}

/// The keys of a JSON object, in order.
fn keys(value: &Value) -> Vec<&str> {
    value
        .as_object()
        .map_or_else(Vec::new, |map| map.keys().map(String::as_str).collect())
}

// A plan answers every field the portal reads and writes nothing: values that do not pass
// end it with no tasks, and a task no node advertises a backend for is placed at run time.
#[tokio::test]
async fn plans_runs() {
    let node = node(Some(Arc::new(Scripted))).await;
    let workflow_id = workflow(&node).await;
    let plan = ("POST", "/compute/workflow-runs/plan");
    let body = run_body(&node, &workflow_id, "Hola");
    let (status, planned) = send(&node, &node.owner, plan, &[], Some(body.clone())).await;
    assert_eq!(status, StatusCode::OK, "{planned}");
    let planned = json_of(&planned);
    let mut fields = keys(&planned);
    fields.sort_unstable();
    assert_eq!(fields, ["complete", "diagnostics", "rejected", "tasks"]);
    let task = &planned["tasks"][0];
    let mut fields = keys(task);
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "bytes",
            "cached",
            "mode",
            "node_id",
            "step",
            "task_count",
            "transfer_ms"
        ]
    );
    assert_eq!(task["step"], "echo");
    assert_eq!(task["mode"], "placed_at_runtime");
    assert_eq!(
        (task["bytes"].clone(), task["task_count"].clone()),
        (Value::Null, json!(1))
    );
    assert_eq!(planned["complete"], false);
    let (_, listed) = send(
        &node,
        &node.owner,
        ("GET", "/compute/workflow-runs"),
        &[],
        None,
    )
    .await;
    assert_eq!(json_of(&listed)["total"], 0, "a plan starts no run");

    let mut invalid = run_body(&node, &workflow_id, "");
    invalid["values"] = json!({});
    let (status, refused) = send(&node, &node.owner, plan, &[], Some(invalid)).await;
    assert_eq!(status, StatusCode::OK, "{refused}");
    let refused = json_of(&refused);
    assert_eq!(
        (refused["complete"].clone(), refused["tasks"].clone()),
        (json!(false), json!([]))
    );
    let (status, _) = send(&node, &node.stranger, plan, &[], Some(body)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let mut malformed = run_body(&node, &workflow_id, "Hola");
    malformed["values"] = json!([1]);
    let (status, _) = send(&node, &node.owner, plan, &[], Some(malformed)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The node and a CWL hello workflow for runs through the real coordinator binary.
async fn real_node() -> (Node, String) {
    let settings = aruna_workflow::WorkflowSettings {
        binary: Some(super::tests::coordinator_binary()),
        ..aruna_workflow::WorkflowSettings::default()
    };
    let runtime = aruna_workflow::WorkflowRuntime::configured(&settings, None).unwrap();
    let node = node(Some(Arc::new(runtime))).await;
    let cwl = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\nhints:\n  DockerRequirement:\n    dockerPull: debian:stable-slim\ninputs:\n  message:\n    type: string\n    inputBinding:\n      position: 1\noutputs:\n  said:\n    type: stdout\nstdout: said.txt\n";
    let create = json!({"group_id": node.group_id.to_string(), "name": "hello", "upload": {
        "entrypoint": "hello.cwl", "files": [{"path": "hello.cwl", "content": cwl}]}});
    let (status, created) = node.create(create).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let workflow_id = created["workflow_id"].as_str().unwrap().to_owned();
    bind_jobs(&node).await;
    let context = node.state.get_ctx();
    aruna_runner::testing::seed_bucket(
        &context,
        "results",
        node.group_id,
        node.owner.user_id,
        Vec::new(),
    )
    .await;
    (node, workflow_id)
}

/// One run through the real coordinator and the run's own operations, to its end. This node has
/// no compute backend, so the coordinator's task cannot be placed and the run fails.
async fn real_run(
    node: &Node,
    workflow_id: &str,
) -> aruna_core::structs::execution::workflow_run::WorkflowRunRecord {
    use aruna_ops_work::jobs::lifecycle::submit_external::IngressNode;
    use aruna_ops_work::workflows::runs::run::{
        AnswerCallOperation, ApplyEventOperation, CallNode, FinishRunOperation, Inbound,
        PrepareRunOperation, Prepared, SessionOperation, inbound,
    };
    use aruna_runner::driver::drive;

    let context = node.state.get_ctx();
    let owner = node.owner.clone();
    let (status, text) = send(
        node,
        &owner,
        ("POST", "/compute/workflow-runs"),
        &[],
        Some(run_body(node, workflow_id, "Hola")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{text}");
    let run_id: ulid::Ulid = json_of(&text)["run_id"].as_str().unwrap().parse().unwrap();

    let (node_id, realm_id) = (node.state.get_node_id(), node.state.get_realm_id());
    let spec = aruna_core::structs::execution::workflow_run::WorkflowRunSpec {
        run_id,
        group_id: node.group_id,
        created_by: owner.user_id,
        realm_id,
    };
    let prepared = drive(
        PrepareRunOperation::new(&spec, node_id, "blake3:test".into()),
        &context,
    )
    .await;
    let Ok(Prepared::Ready(record)) = prepared else {
        panic!("the coordinator starts the run: {prepared:?}");
    };
    let call_node = CallNode {
        node_id,
        realm_id,
        routing: Default::default(),
        ingress: IngressNode {
            local: node_id,
            realm_id,
            now_ms: 1,
            fresh: Default::default(),
        },
    };
    loop {
        let frame = drive(SessionOperation::receive(run_id), &context).await;
        let frame = frame
            .expect("the session answers")
            .expect("the run ends before the channel");
        match inbound(frame) {
            Inbound::Call { call_id, request } => {
                let answer =
                    AnswerCallOperation::new(*record.clone(), call_node.clone(), call_id, request);
                drive(answer, &context).await.expect("the call is answered");
            }
            Inbound::Event(event) => {
                drive(ApplyEventOperation::new(run_id, &event), &context)
                    .await
                    .unwrap();
            }
            Inbound::Finished(finished) => {
                break drive(FinishRunOperation::reported(run_id, &finished), &context)
                    .await
                    .unwrap();
            }
            other => panic!("an unexpected frame {other:?}"),
        }
    }
}

/// A run of a CWL workflow through the real coordinator binary and the run's own operations.
/// The run must still record the task, store its log, end as failed with the reason and close
/// its stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn drives_real_run() {
    use aruna_ops_work::workflows::runs::log::LoadLogOperation;
    use aruna_runner::driver::drive;

    let (node, workflow_id) = real_node().await;
    let ended = real_run(&node, &workflow_id).await;
    let run_id = ended.run_id;
    assert_eq!(ended.state, RunState::Failed, "{ended:?}");
    assert!(ended.error.is_some(), "{ended:?}");
    let pages = drive(LoadLogOperation::new(run_id), &node.state.get_ctx())
        .await
        .unwrap();
    assert!(!pages.is_empty(), "the run log holds the start of the run");
    let path = format!("/compute/workflow-runs/{run_id}");
    let (_, stream) = send(
        &node,
        &node.owner,
        ("GET", &format!("{path}/events")),
        &[],
        None,
    )
    .await;
    let kinds: Vec<String> = frames(&stream)
        .into_iter()
        .map(|(_, kind, _)| kind)
        .collect();
    assert!(kinds.iter().any(|kind| kind == "task"), "{stream}");
    assert_eq!(kinds.last().map(String::as_str), Some("state"), "{stream}");
}

/// A plan through the real coordinator binary: the simulated echo task, placed at run time on
/// this node without a compute backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn plans_real_run() {
    let (node, workflow_id) = real_node().await;
    let body = run_body(&node, &workflow_id, "Hola");
    let plan = ("POST", "/compute/workflow-runs/plan");
    let (status, planned) = send(&node, &node.owner, plan, &[], Some(body)).await;
    assert_eq!(status, StatusCode::OK, "{planned}");
    let planned = json_of(&planned);
    let steps: Vec<&str> = (planned["tasks"].as_array().into_iter().flatten())
        .filter_map(|task| task["step"].as_str())
        .collect();
    assert!(!steps.is_empty(), "{planned}");
}

/// The reports of two recorded runs through report sessions of the real coordinator binary:
/// time, why, compare, changes, the merged crate and the HTML report.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn reports_real_runs() {
    let (node, workflow_id) = real_node().await;
    let owner = node.owner.clone();
    let first = real_run(&node, &workflow_id).await.run_id;
    let second = real_run(&node, &workflow_id).await.run_id;
    let path = format!("/compute/workflow-runs/{second}");
    let get = |rest: String| (String::from("GET"), format!("{path}{rest}"));

    let (method, uri) = get("/time".into());
    let (status, time) = send(&node, &owner, (&method, &uri), &[], None).await;
    let time = json_of(&time);
    assert_eq!(status, StatusCode::OK, "{time}");
    assert_eq!(time["source"], "uwe", "{time}");
    assert!(
        time["started"].is_number() && time["ended"].is_number(),
        "{time}"
    );

    let (_, tasks) = send(&node, &owner, ("GET", &format!("{path}/tasks")), &[], None).await;
    let task = json_of(&tasks)["items"][0]["task_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let (method, uri) = get(format!("/tasks/{task}/why"));
    let (status, why) = send(&node, &owner, (&method, &uri), &[], None).await;
    let why = json_of(&why);
    assert_eq!(status, StatusCode::OK, "{why}");
    assert!(
        ["ran", "reran"].contains(&why["verdict"].as_str().unwrap()),
        "{why}"
    );

    let compare = format!("/compute/workflow-runs/compare?first={first}&second={second}");
    let (status, compared) = send(&node, &owner, ("GET", &compare), &[], None).await;
    let compared = json_of(&compared);
    assert_eq!(status, StatusCode::OK, "{compared}");
    assert!(compared["comparison"].is_object(), "{compared}");

    let (method, uri) = get("/changes".into());
    let (status, changes) = send(&node, &owner, (&method, &uri), &[], None).await;
    let changes = json_of(&changes);
    assert_eq!(status, StatusCode::OK, "{changes}");
    assert!(changes["recorded"].is_object(), "{changes}");

    let (method, uri) = get("/crate".into());
    let (status, built) = send(&node, &owner, (&method, &uri), &[], None).await;
    let built = json_of(&built);
    assert_eq!(status, StatusCode::OK, "{built}");
    let graph = built["@graph"].as_array().unwrap();
    let action = format!("#workflow-run-{second}");
    assert!(
        graph.iter().any(|entity| entity["@id"] == action.as_str()),
        "{built}"
    );
    assert!(
        graph.iter().any(|entity| entity["@id"]
            .as_str()
            .is_some_and(|id| id.starts_with("#engine/"))),
        "the crate is uwe's: {built}"
    );

    let (method, uri) = get("/report".into());
    let (status, html) = send(&node, &owner, (&method, &uri), &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("<html"), "{}", &html[..html.len().min(200)]);
}
