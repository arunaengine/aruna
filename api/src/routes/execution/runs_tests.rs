//! Tests the run route: local answers by node header, refusals, and node-directed relays.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Mutex;

use aruna_core::structs::execution::job::{JobId, RunCrateStatus, RunLink};
use aruna_operations::jobs::store::put_crate_outcome;
use axum::Router;
use axum::http::StatusCode;
use ulid::Ulid;

use super::*;
use crate::routes::storage::relay::RELAY_NODE_HEADER;
use crate::tests::assistant::spawn_mock;
use crate::tests::distributed::{LOCAL, Node, PEER, node_id, node_state, publish_api};

async fn run(
    node: &Node,
    auth: &AuthContext,
    named: [u8; 32],
    run_id: JobId,
) -> ServerResult<RunResponse> {
    let mut headers = HeaderMap::new();
    headers.insert(
        RELAY_NODE_HEADER,
        node_id(named).to_string().parse().unwrap(),
    );
    get_run(
        State(node.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(ValidatedBearer::new_for_test("caller-token"))),
        headers,
        Path(run_id.to_string()),
    )
    .await
    .map(|Json(response)| response)
}

#[tokio::test]
async fn answers_directed_reads() {
    let node = node_state().await;
    let run_id = JobId::from_bytes([6; 16]);
    let document_id = Ulid::from_bytes([7; 16]);
    let link = RunLink {
        family: None,
        logical_job_id: Some(JobId::from_bytes([8; 16])),
        group_id: node.group_id,
        created_by: node.owner.user_id,
        run_crate: Some(document_id),
    };
    let status = RunCrateStatus::Written {
        resource: document_id.to_string(),
    };
    let storage = &node.state.get_ctx().storage_handle;
    put_crate_outcome(storage, run_id, &status, &link)
        .await
        .unwrap();

    let own = run(&node, &node.owner, LOCAL, run_id).await.unwrap();
    assert_eq!(
        own.run_crate.as_deref(),
        Some(document_id.to_string().as_str())
    );
    assert_eq!(
        own.run_crate_iri,
        Some(MetadataRegistryRecord::graph_iri_for(document_id))
    );
    assert!(own.logical_job_id.is_some());
    let hidden = run(&node, &node.stranger, LOCAL, run_id).await;
    assert!(matches!(hidden, Err(ServerError::NotFound)));
    let unknown = run(&node, &node.owner, LOCAL, JobId::from_bytes([9; 16])).await;
    assert!(matches!(unknown, Err(ServerError::NotFound)));
    // A read directed at another node is refused, never answered or passed on.
    let elsewhere = run(&node, &node.owner, PEER, run_id).await;
    assert!(matches!(elsewhere, Err(ServerError::Conflict(_))));
}

#[tokio::test]
async fn node_reads_relay() {
    let node = node_state().await;
    let seen: Arc<Mutex<Vec<HeaderMap>>> = Arc::default();
    let record = seen.clone();
    let body = serde_json::json!({
        "run_id": "r", "logical_job_id": null, "run_crate": null, "run_crate_iri": null
    })
    .to_string();
    let answer = move |headers: HeaderMap| {
        record.lock().unwrap().push(headers);
        let body = body.clone();
        async move { (StatusCode::OK, [("content-type", "application/json")], body) }
    };
    let (url, task) = spawn_mock(Router::new().fallback(answer)).await;
    publish_api(&node.state, node_id(PEER), &url).await;
    let bearer = ValidatedBearer::new_for_test("caller-token");
    let deadline = tokio::time::Instant::now() + RELAY_DEADLINE;
    let path = "/compute/runs/r";
    let answered: RunResponse =
        node_read(&node.state, node_id(PEER), path, Some(&bearer), deadline)
            .await
            .unwrap();
    assert_eq!(answered.run_id, "r");
    let headers = seen.lock().unwrap().pop().unwrap();
    let named = headers.get(RELAY_NODE_HEADER).unwrap().to_str().unwrap();
    assert_eq!(named, node_id(PEER).to_string());
    assert!(
        headers
            .get(crate::routes::storage::relay::RELAY_SET_HEADER)
            .is_none()
    );
    task.abort();

    // A node that never answers fails at the deadline, body included.
    let silent = |_: HeaderMap| std::future::pending::<StatusCode>();
    let (url, task) = spawn_mock(Router::new().fallback(silent)).await;
    publish_api(&node.state, node_id(PEER), &url).await;
    let soon = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
    let late: ServerResult<RunResponse> =
        node_read(&node.state, node_id(PEER), path, Some(&bearer), soon).await;
    assert!(matches!(
        late,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
    task.abort();
}
