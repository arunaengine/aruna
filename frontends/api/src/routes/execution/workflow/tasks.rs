//! Serves the tasks of workflow runs: the paged list, one task, and the output streams of a task.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::workflow_run::RunState;
use aruna_ops_work::workflows::runs::reads::{
    GetTaskOperation, ListTasksOperation, TaskFilter, log_chunk,
};
use aruna_runner::driver::drive;
use aruna_workflow_contract::message::LogStream;
use axum::Json;
use axum::extract::{Path, Query, State};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{
    LogChunkBody, TaskLogQuery, WorkflowTaskBody, WorkflowTaskPage, WorkflowTaskQuery,
};
use super::runs::run_id;
use super::{catalog_error, limit};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_tasks))
        .routes(routes!(get_task))
        .routes(routes!(task_logs))
}

/// The task number of a path; one that is no number names no task.
pub(super) fn task_id(text: &str) -> Result<u64, ServerError> {
    text.parse().map_err(|_| ServerError::NotFound)
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/tasks",
    tag = "compute/workflow-runs",
    summary = "List the tasks of a workflow run",
    description = r#"Lists the tasks of a run in task order, each with its newest attempt.

**Authentication**: realm bearer token.

**Behavior**
- `step` and `state` narrow the list; `next_cursor` continues it and `total` counts every
  match.
- A run the caller cannot read is not found."#,
    params(
        ("run_id" = String, Path, description = "Run id, as a 26-character ULID"),
        WorkflowTaskQuery
    ),
    responses(
        (status = 200, description = "One page of tasks", body = WorkflowTaskPage, example = json!({
            "items": [{
            "task_id": "1", "step": "echo", "state": "succeeded", "attempt": 1,
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "cached": false, "started_at": "2026-11-24T08:00:05+00:00",
            "finished_at": "2026-11-24T08:00:09+00:00", "exit_code": 0, "index": null,
            "mode": "compute_to_data", "staged_bytes": null, "cpu_seconds": null,
            "max_rss_bytes": null, "cache_key": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            "inputs": [], "outputs": [{"name": "said.txt", "bucket": "results",
                "key": "hello/.uwe/runs/01K8W2A0000000000000000000/tasks/run-1-1/work/said.txt",
                "version_id": null, "node_id": null, "size": null, "digest": null}]
        }], "next_cursor": null, "total": 1
        })),
        (status = 400, description = "A malformed state or cursor", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_tasks(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
    Query(query): Query<WorkflowTaskQuery>,
) -> ServerResult<Json<WorkflowTaskPage>> {
    let filter = TaskFilter {
        step: query.step,
        state: match query.state.as_deref() {
            Some(text) => Some(RunState::parse(text).ok_or_else(|| {
                ServerError::BadRequestReason(format!("{text} is no task state"))
            })?),
            None => None,
        },
    };
    let after = match query.cursor.as_deref() {
        Some(cursor) => Some(
            cursor
                .parse()
                .map_err(|_| ServerError::BadRequestReason("cursor is no task id".into()))?,
        ),
        None => None,
    };
    let list = ListTasksOperation::new(&auth, run_id(&run)?, filter, (after, limit(query.limit)));
    let page = drive(list, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(WorkflowTaskPage {
        items: page.items.iter().map(WorkflowTaskBody::from).collect(),
        next_cursor: page.next.map(|task| task.to_string()),
        total: Some(page.total as u64),
    }))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/tasks/{task_id}",
    tag = "compute/workflow-runs",
    summary = "Read a task of a workflow run",
    description = r#"Returns one task with its newest attempt, inputs and outputs.

**Authentication**: realm bearer token.

**Errors**
- 404 for an unknown run or task, or a run the caller cannot read."#,
    params(
        ("run_id" = String, Path, description = "Run id, as a 26-character ULID"),
        ("task_id" = String, Path, description = "Task id, as its number in the run")
    ),
    responses(
        (status = 200, description = "The task", body = WorkflowTaskBody, example = json!({
            "task_id": "1", "step": "echo", "state": "succeeded", "attempt": 1,
            "node_id": "4f0c9d1e2b3a495867788990aabbccddeeff00112233445566778899aabbccdd",
            "cached": false, "started_at": "2026-11-24T08:00:05+00:00",
            "finished_at": "2026-11-24T08:00:09+00:00", "exit_code": 0, "index": null,
            "mode": "compute_to_data", "staged_bytes": null, "cpu_seconds": null,
            "max_rss_bytes": null, "cache_key": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            "inputs": [], "outputs": [{"name": "said.txt", "bucket": "results",
                "key": "hello/.uwe/runs/01K8W2A0000000000000000000/tasks/run-1-1/work/said.txt",
                "version_id": null, "node_id": null, "size": null, "digest": null}]
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_task(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((run, task)): Path<(String, String)>,
) -> ServerResult<Json<WorkflowTaskBody>> {
    let read = GetTaskOperation::new(&auth, run_id(&run)?, task_id(&task)?);
    let task = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(WorkflowTaskBody::from(&task)))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/tasks/{task_id}/logs",
    tag = "compute/workflow-runs",
    summary = "Read the output of a task",
    description = r#"Returns the stored tail of a task's stdout or stderr from a byte offset.

**Authentication**: realm bearer token.

**Behavior**
- At most 64 KiB answer per read; `next_offset` continues the stream.
- `complete` is true once the task ended and nothing follows `next_offset`.
- Only the last 64 KiB of each stream of an ended attempt are kept."#,
    params(
        ("run_id" = String, Path, description = "Run id, as a 26-character ULID"),
        ("task_id" = String, Path, description = "Task id, as its number in the run"),
        TaskLogQuery
    ),
    responses(
        (status = 200, description = "One piece of the stream", body = LogChunkBody, example = json!({
            "stream": "stdout", "offset": 0, "text": "Hola\n", "next_offset": 5, "complete": true
        })),
        (status = 400, description = "A stream other than stdout or stderr", body = ErrorResponse),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn task_logs(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((run, task)): Path<(String, String)>,
    Query(query): Query<TaskLogQuery>,
) -> ServerResult<Json<LogChunkBody>> {
    let stream = match query.stream.as_deref() {
        None | Some("stdout") => LogStream::Stdout,
        Some("stderr") => LogStream::Stderr,
        Some(other) => {
            return Err(ServerError::BadRequestReason(format!(
                "{other} is no stream; ask for stdout or stderr"
            )));
        }
    };
    let read = GetTaskOperation::new(&auth, run_id(&run)?, task_id(&task)?);
    let task = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    let chunk = log_chunk(&task, stream, query.offset.unwrap_or(0));
    Ok(Json(chunk.into()))
}
