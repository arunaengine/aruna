//! Tests the workflow run routes through their router: start under an idempotency key, list,
//! snapshot, tasks, logs, origin, the event stream with `after` and `Last-Event-ID`, cancel and
//! delete, and a run driven through the real coordinator binary.
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

/// A run of a CWL workflow through the real coordinator binary and the run's own operations.
/// This node has no compute backend, so the coordinator's task cannot be placed: the run must
/// still record the task, store its log, end as failed with the reason and close its stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs the aruna-coordinator binary"]
async fn drives_real_run() {
    use aruna_ops_work::jobs::lifecycle::submit_external::IngressNode;
    use aruna_ops_work::workflows::runs::log::LoadLogOperation;
    use aruna_ops_work::workflows::runs::run::{
        AnswerCallOperation, ApplyEventOperation, CallNode, FinishRunOperation, Inbound,
        PrepareRunOperation, Prepared, SessionOperation, inbound,
    };
    use aruna_runner::driver::drive;

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
    let owner = node.owner.clone();
    aruna_runner::testing::seed_bucket(
        &context,
        "results",
        node.group_id,
        owner.user_id,
        Vec::new(),
    )
    .await;
    let (status, text) = send(
        &node,
        &owner,
        ("POST", "/compute/workflow-runs"),
        &[],
        Some(run_body(&node, &workflow_id, "Hola")),
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
    let ended = loop {
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
    };
    assert_eq!(ended.state, RunState::Failed, "{ended:?}");
    assert!(ended.error.is_some(), "{ended:?}");
    let pages = drive(LoadLogOperation::new(run_id), &context)
        .await
        .unwrap();
    assert!(!pages.is_empty(), "the run log holds the start of the run");
    let path = format!("/compute/workflow-runs/{run_id}");
    let (_, stream) = send(&node, &owner, ("GET", &format!("{path}/events")), &[], None).await;
    let kinds: Vec<String> = frames(&stream)
        .into_iter()
        .map(|(_, kind, _)| kind)
        .collect();
    assert!(kinds.iter().any(|kind| kind == "task"), "{stream}");
    assert_eq!(kinds.last().map(String::as_str), Some("state"), "{stream}");
}
