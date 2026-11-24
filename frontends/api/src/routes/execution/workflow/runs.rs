//! Serves workflow runs: start one under an idempotency key, list, read the snapshot and the
//! graph, find the origin, cancel and delete.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::workflow_run::{RunOutput, RunState};
use aruna_ops_work::workflows::runs::admit::{PinnedInput, RunRequest, StartRunOperation};
use aruna_ops_work::workflows::runs::cancel::{CancelRunOperation, DeleteRunOperation};
use aruna_ops_work::workflows::runs::reads::{
    ListRunsOperation, RunFilter, RunGraphOperation, RunOriginOperation, RunSnapshotOperation,
};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{
    PinnedInputBody, PinnedInputs, RunOriginBody, RunSnapshotBody, WorkflowRunBody,
    WorkflowRunPage, WorkflowRunQuery, WorkflowRunRequest,
};
use super::wire::WorkflowGraphBody;
use super::{catalog_error, limit, ulid};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

/// The header that makes a repeated start answer the run it started.
const IDEMPOTENCY_HEADER: &str = "idempotency-key";
/// The longest idempotency key a caller may send.
const MAX_KEY_BYTES: usize = 256;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_runs, start_run))
        .routes(routes!(get_run, delete_run))
        .routes(routes!(run_graph))
        .routes(routes!(run_origin))
        .routes(routes!(cancel_run))
}

/// The run id of a path; one that is no ULID names no run.
pub(super) fn run_id(text: &str) -> Result<Ulid, ServerError> {
    Ulid::from_string(text).map_err(|_| ServerError::NotFound)
}

fn node_id(text: &str, name: &str) -> Result<aruna_core::id::NodeId, ServerError> {
    text.parse()
        .map_err(|_| ServerError::BadRequestReason(format!("{name} is no node id")))
}

fn pinned(name: &str, pin: &PinnedInputBody) -> Result<PinnedInput, ServerError> {
    let version_id = Ulid::from_string(&pin.version_id).map_err(|_| {
        ServerError::BadRequestReason(format!("the version of input {name} is not a ULID"))
    })?;
    Ok(PinnedInput {
        name: name.to_owned(),
        bucket: pin.bucket.clone(),
        key: pin.key.clone(),
        version_id,
        source_node_id: (pin.source_node_id.as_deref())
            .map(|node| node_id(node, "source_node_id"))
            .transpose()?,
    })
}

/// The domain request of a start, with every id and pin checked.
fn run_request(
    state: &ServerState,
    request: WorkflowRunRequest,
    idempotency_key: Option<String>,
) -> Result<RunRequest, ServerError> {
    let here = state.get_node_id();
    let placed = request
        .placement
        .as_ref()
        .and_then(|placement| placement.node_id.as_deref());
    if placed
        .map(|node| node_id(node, "placement.node_id"))
        .transpose()?
        .is_some_and(|node| node != here)
    {
        return Err(ServerError::BadRequestReason(
            "a run runs on the node that takes it; send it to the node named in placement".into(),
        ));
    }
    let version = request.version.parse().map_err(|_| {
        ServerError::BadRequestReason(format!("{} is no version number", request.version))
    })?;
    let mut inputs = Vec::new();
    for (name, pins) in request.inputs.iter().flatten() {
        match pins {
            PinnedInputs::One(pin) => inputs.push(pinned(name, pin)?),
            PinnedInputs::Many(pins) => {
                for pin in pins {
                    inputs.push(pinned(name, pin)?);
                }
            }
        }
    }
    let inputs_json = (request.inputs.as_ref())
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let output = match request.output {
        Some(output) => Some(RunOutput {
            bucket: output.bucket,
            prefix: output.prefix.unwrap_or_default(),
            node_id: (output.node_id.as_deref())
                .map(|node| node_id(node, "output.node_id"))
                .transpose()?,
            dataset_json: (output.dataset.as_ref())
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| ServerError::BadRequestReason(error.to_string()))?,
        }),
        None => None,
    };
    Ok(RunRequest {
        group_id: ulid(&request.group_id, "group_id")?,
        workflow_id: ulid(&request.workflow_id, "workflow_id")?,
        version,
        values: request.values,
        inputs,
        inputs_json,
        output,
        provenance_crate: (request
            .provenance
            .and_then(|provenance| provenance.run_crate))
        .unwrap_or(false),
        idempotency_key,
    })
}

fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, ServerError> {
    let Some(value) = headers.get(IDEMPOTENCY_HEADER) else {
        return Ok(None);
    };
    let key = value.to_str().map_err(|_| {
        ServerError::BadRequestReason("the Idempotency-Key header is not text".into())
    })?;
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(ServerError::BadRequestReason(format!(
            "the Idempotency-Key header must hold 1 to {MAX_KEY_BYTES} bytes"
        )));
    }
    Ok(Some(key.to_owned()))
}

#[utoipa::path(
    post,
    path = "/compute/workflow-runs",
    tag = "compute/workflow-runs",
    summary = "Start a workflow run",
    description = r#"Starts one run of a workflow version on this node and answers at once.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The caller must read the version, read every pinned input this node holds and write the
  output bucket below its prefix. The run's task files are kept below
  `<prefix>/.uwe/runs/<run_id>/`, so a run needs an output bucket.
- The same `Idempotency-Key` header with the same request answers the run it started, never a
  second one; with another request it is a 409.
- The run is queued as a job of this node and runs as its creator. Its progress follows on
  `GET /compute/workflow-runs/{run_id}/events`.

**Errors**
- 400 for malformed ids, a version that is no number, values that are no object, no output, a
  missing bucket, or a placement on another node.
- 403 without WRITE on the group's workflows or on the output, or without READ on an input.
- 404 when the group or the version is not visible to the caller."#,
    params(("Idempotency-Key" = Option<String>, Header, description = "Starts the run once; 1 to 256 bytes")),
    request_body(content = WorkflowRunRequest, example = json!({
        "group_id": "01JABCDEF0123456789ABCDEFG", "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
        "version": "1", "values": {"message": "Hola"},
        "inputs": {"reads": {"bucket": "reads", "key": "run1/a.fastq", "version_id": "01K8W1C0000000000000000000"}},
        "output": {"bucket": "results", "prefix": "hello/"}
    })),
    responses(
        (status = 202, description = "The run, queued or as it stands", body = WorkflowRunBody, example = json!({
            "run_id": "01K8W2A0000000000000000000", "group_id": "01JABCDEF0123456789ABCDEFG",
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "state": "queued",
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "created_at": "2026-11-24T08:00:00+00:00", "finished_at": null,
            "created_by": "01K8W1A0000000000000000000", "error": null,
            "lineage": "01K8W2A0000000000000000000", "workflow_name": "hello", "local_only": false
        })),
        (status = 400, description = "An invalid request", body = ErrorResponse),
        (status = 403, description = "A missing permission", body = ErrorResponse),
        (status = 404, description = "The group or version is not visible", body = ErrorResponse),
        (status = 409, description = "The key started another run", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    headers: HeaderMap,
    Json(request): Json<WorkflowRunRequest>,
) -> ServerResult<(StatusCode, Json<WorkflowRunBody>)> {
    let request = run_request(&state, request, idempotency_key(&headers)?)?;
    let node = (state.get_node_id(), state.get_realm_id());
    let start = StartRunOperation::new(&auth, node, request);
    let admitted = drive(start, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(WorkflowRunBody::from(&admitted.record)),
    ))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs",
    tag = "compute/workflow-runs",
    summary = "List workflow runs",
    description = r#"Lists the runs of this node whose group the caller reads, newest first.

**Authentication**: realm bearer token.

**Behavior**
- `group_id`, `state` and `workflow_id` narrow the list; `next_cursor` continues it and `total`
  counts every match."#,
    params(WorkflowRunQuery),
    responses(
        (status = 200, description = "One page of runs", body = WorkflowRunPage, example = json!({
            "items": [{
            "run_id": "01K8W2A0000000000000000000", "group_id": "01JABCDEF0123456789ABCDEFG",
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "state": "running",
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "created_at": "2026-11-24T08:00:00+00:00", "finished_at": null,
            "created_by": "01K8W1A0000000000000000000", "error": null,
            "lineage": "01K8W2A0000000000000000000", "workflow_name": "hello", "local_only": false
        }], "next_cursor": null, "total": 1
        })),
        (status = 400, description = "A malformed id, state or cursor", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_runs(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<WorkflowRunQuery>,
) -> ServerResult<Json<WorkflowRunPage>> {
    let filter =
        RunFilter {
            group_id: (query.group_id.as_deref())
                .map(|group| ulid(group, "group_id"))
                .transpose()?,
            state: match query.state.as_deref() {
                Some(text) => Some(RunState::parse(text).ok_or_else(|| {
                    ServerError::BadRequestReason(format!("{text} is no run state"))
                })?),
                None => None,
            },
            workflow_id: (query.workflow_id.as_deref())
                .map(|workflow| ulid(workflow, "workflow_id"))
                .transpose()?,
        };
    let after = (query.cursor.as_deref())
        .map(|cursor| ulid(cursor, "cursor"))
        .transpose()?;
    let list = ListRunsOperation::new(&auth, filter, after, limit(query.limit));
    let page = drive(list, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(WorkflowRunPage {
        items: page.items.iter().map(WorkflowRunBody::from).collect(),
        next_cursor: page.next.map(|run| run.to_string()),
        total: Some(page.total as u64),
    }))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}",
    tag = "compute/workflow-runs",
    summary = "Read a workflow run",
    description = r#"Returns a run with its steps, inputs, outputs and provenance in one read.

**Authentication**: realm bearer token.

**Behavior**
- Each step counts its tasks by state; a step with at most 25 tasks lists them.
- `last_event_id` is the newest event the snapshot includes; the event stream continues after
  it.
- A run the caller cannot read is not found."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The run", body = RunSnapshotBody, example = json!({
            "run_id": "01K8W2A0000000000000000000", "group_id": "01JABCDEF0123456789ABCDEFG",
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "state": "running",
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "created_at": "2026-11-24T08:00:00+00:00", "finished_at": null,
            "created_by": "01K8W1A0000000000000000000", "error": null,
            "lineage": "01K8W2A0000000000000000000", "workflow_name": "hello", "local_only": false,
            "steps": [{"step": "echo", "state": "running", "total": 2, "counts": {"queued": 0,
                "running": 1, "succeeded": 1, "failed": 0, "cancelled": 0, "cached": 0},
                "node_id": null, "mode": "compute_to_data", "tasks": [
                    {"task_id": "1", "state": "succeeded", "index": null, "cached": false},
                    {"task_id": "2", "state": "running", "index": null, "cached": false}]}],
            "values": {"message": "Hola"}, "inputs": null, "outputs": [],
            "output": {"bucket": "results", "prefix": "hello/"}, "provenance": null,
            "last_event_id": 12
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<RunSnapshotBody>> {
    let read = RunSnapshotOperation::new(&auth, run_id(&run)?);
    let snapshot = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(snapshot.into()))
}

#[utoipa::path(
    delete,
    path = "/compute/workflow-runs/{run_id}",
    tag = "compute/workflow-runs",
    summary = "Delete a workflow run",
    description = r#"Deletes an ended run with its tasks, events and run log.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The files the run wrote stay in their buckets.

**Errors**
- 404 for an unknown run or one the caller cannot read; a second delete is a 404.
- 409 while the run is queued, planning, running or cancelling."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 403, description = "No WRITE on the group's workflows", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 409, description = "The run has not ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<StatusCode> {
    let delete = DeleteRunOperation::new(&auth, run_id(&run)?);
    drive(delete, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/graph",
    tag = "compute/workflow-runs",
    summary = "Read the graph of a workflow run",
    description = r#"Returns the step graph of the run's workflow version.

**Authentication**: realm bearer token.

**Behavior**
- Task states follow from the run's steps and events.
- The graph is read as the run's creator once the caller may read the run."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The step graph", body = WorkflowGraphBody, example = json!({
            "nodes": [{"id": "echo", "label": "echo", "kind": "step"}],
            "edges": [{"from": {"node": "input:message", "port": "message"},
                "to": {"node": "echo", "port": ""}}],
            "complete": false
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "No workflow coordinator answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_graph(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<WorkflowGraphBody>> {
    let read = RunGraphOperation::new(&auth, run_id(&run)?);
    let graph = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(graph.into()))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/origin",
    tag = "compute/workflow-runs",
    summary = "Read the origin of a workflow run",
    description = r#"Returns the first object version the run delivered to its output.

**Authentication**: realm bearer token.

**Behavior**
- Every version the run wrote names the run as its origin.

**Errors**
- 404 for an unknown run, one the caller cannot read, or one that delivered nothing yet."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The origin", body = RunOriginBody, example = json!({
            "bucket": "results", "key": "hello/said.txt", "version_id": "01K8W2F0000000000000000000"
        })),
        (status = 404, description = "No origin yet, or not visible", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_origin(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<RunOriginBody>> {
    let read = RunOriginOperation::new(&auth, run_id(&run)?);
    let origin = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(origin.into()))
}

#[utoipa::path(
    post,
    path = "/compute/workflow-runs/{run_id}/cancel",
    tag = "compute/workflow-runs",
    summary = "Cancel a workflow run",
    description = r#"Stops a run, which turns `cancelling` and ends `cancelled` once its tasks stopped.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The run's job and every child job that has not ended are cancelled; a run that never started
  ends at once.

**Errors**
- 404 for an unknown run or one the caller cannot read.
- 409 when the run already ended."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 202, description = "The run as it stands", body = WorkflowRunBody, example = json!({
            "run_id": "01K8W2A0000000000000000000", "group_id": "01JABCDEF0123456789ABCDEFG",
            "workflow_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "version": "1", "state": "cancelling",
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "created_at": "2026-11-24T08:00:00+00:00", "finished_at": null,
            "created_by": "01K8W1A0000000000000000000", "error": null,
            "lineage": "01K8W2A0000000000000000000", "workflow_name": "hello", "local_only": false
        })),
        (status = 403, description = "No WRITE on the group's workflows", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 409, description = "The run already ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<(StatusCode, Json<WorkflowRunBody>)> {
    let node = (state.get_node_id(), state.get_realm_id());
    let cancel = CancelRunOperation::new(&auth, run_id(&run)?, node);
    let cancelled = drive(cancel, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    let runtime = state.jobs_runtime();
    for job_id in cancelled.running {
        runtime.request_cancel(job_id);
    }
    if let Some(registry) = state.get_ctx().compute_handle.as_ref() {
        registry.wake_run(cancelled.record.run_id.to_string());
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(WorkflowRunBody::from(&cancelled.record)),
    ))
}
