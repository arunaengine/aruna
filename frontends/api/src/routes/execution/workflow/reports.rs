//! Serves the reports of workflow runs: why a task ran, two runs compared, the time, what changed
//! since, the run crate and the HTML report, which is only ever sent as a file to save.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_ops_work::workflows::runs::provenance::RunCrateOperation;
use aruna_ops_work::workflows::runs::reports::{
    CompareRunsOperation, RunChangesOperation, RunReportOperation, RunTimeOperation,
    TaskWhyOperation,
};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::catalog_error;
use super::run_wire::{CompareQuery, WhyReportBody};
use super::runs::run_id;
use super::tasks::task_id;
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(task_why))
        .routes(routes!(compare_runs))
        .routes(routes!(run_time))
        .routes(routes!(run_changes))
        .routes(routes!(run_crate))
        .routes(routes!(run_report))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/tasks/{task_id}/why",
    tag = "compute/workflow-runs",
    summary = "Explain why a task ran",
    description = r#"Explains why a task ran, ran again or reused a cached result, as `uwe why` does.

**Authentication**: realm bearer token.

**Behavior**
- uwe compares the task with the same task in the run it resumes, or in the latest earlier run
  of its lineage, from the recorded run logs.
- `verdict` is `cached`, `reran` or `ran`; `changes` holds one entry per difference.
- Without an earlier run, or on a node without a coordinator, the answer comes from the task's
  record and `source` is `record`.
- A result reused from another group never names that group, its run or its user.

**Errors**
- 404 for an unknown run or task, or a run the caller cannot read.
- 503 when the coordinator failed."#,
    params(
        ("run_id" = String, Path, description = "Run id, as a 26-character ULID"),
        ("task_id" = String, Path, description = "Task number within the run")
    ),
    responses(
        (status = 200, description = "Why the task ran", body = WhyReportBody, example = json!({
            "run_id": "01K8W2A0000000000000000000", "task_id": "1", "verdict": "reran",
            "summary": "The task ran again: the input `b.txt` changed: 6 bytes before, 10 now",
            "cache_key": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            "deterministic": true,
            "changes": [{"field": "the input `b.txt` changed: 6 bytes before, 10 now"}],
            "earlier": {"run_id": "01K8W1Z0000000000000000000", "task_id": 1}, "source": "uwe"
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "The coordinator failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn task_why(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path((run, task)): Path<(String, String)>,
) -> ServerResult<Json<WhyReportBody>> {
    let why = TaskWhyOperation::new(&auth, run_id(&run)?, task_id(&task)?);
    let answer = drive(why, &state.get_ctx()).await.map_err(catalog_error)?;
    let body = serde_json::from_value(answer)
        .map_err(|error| ServerError::InternalError(error.to_string().into()))?;
    Ok(Json(body))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/compare",
    tag = "compute/workflow-runs",
    summary = "Compare two workflow runs",
    description = r#"Compares two runs: their values, inputs and uwe's task by task comparison.

**Authentication**: realm bearer token with READ on both runs.

**Behavior**
- `values` and `inputs` list what differs by JSON pointer, as `added`, `removed` or `changed`.
- `comparison` is uwe's comparison of the two run logs, or null when a run has no log yet.

**Errors**
- 400 without `first` or `second`.
- 404 when either run is unknown or the caller cannot read it.
- 503 when the coordinator failed."#,
    params(CompareQuery),
    responses(
        (status = 200, description = "The comparison", body = Object, example = json!({
            "first": {"run_id": "01K8W1Z0000000000000000000", "state": "succeeded"},
            "second": {"run_id": "01K8W2A0000000000000000000", "state": "succeeded"},
            "same_workflow": true, "same_version": true,
            "values": [{"path": "/message", "first": "Hola", "second": "Ciao", "kind": "changed"}],
            "inputs": [], "comparison": {"tasks": []}
        })),
        (status = 400, description = "A missing run id", body = ErrorResponse),
        (status = 404, description = "A run is unknown or not visible", body = ErrorResponse),
        (status = 503, description = "The coordinator failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn compare_runs(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<CompareQuery>,
) -> ServerResult<Json<Value>> {
    let compare = CompareRunsOperation::new(&auth, run_id(&query.first)?, run_id(&query.second)?);
    let answer = drive(compare, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(answer))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/time",
    tag = "compute/workflow-runs",
    summary = "Read where the time of a run went",
    description = r#"Returns where the time of a run went, as uwe reads its run log.

**Authentication**: realm bearer token.

**Behavior**
- `started` and `ended` are milliseconds since the Unix epoch; `ran`, `waited`, `busy`, `before`
  and `after` are milliseconds. `critical_path` is the chain of tasks that set the run's length.
- Without a run log the answer comes from the task records, `source` is `record`, and waits and
  the critical path are left empty.

**Errors**
- 404 for an unknown run or one the caller cannot read.
- 503 when the coordinator failed."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The time of the run", body = Object, example = json!({
            "run_id": "01K8W2A0000000000000000000", "started": 1_795_000_000_000_u64,
            "ended": 1_795_000_060_000_u64, "tasks": 2, "reused": 1, "ran": 40_000, "waited": 5_000,
            "busy": 40_000, "before": 2_000, "after": 1_000,
            "critical_path": [{"task": 1, "name": "echo-1", "step": "echo", "gap": 0,
                "waited": 5_000, "ran": 40_000, "attempts": 1, "reused": null}],
            "steps": [{"step": "echo", "tasks": 2, "ran": 40_000, "waited": 5_000}], "source": "uwe"
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "The coordinator failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_time(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<Value>> {
    let time = RunTimeOperation::new(&auth, run_id(&run)?);
    let answer = drive(time, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(answer))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/changes",
    tag = "compute/workflow-runs",
    summary = "Read what changed since a run",
    description = r#"Lists what changed since a run: its workflow, its pinned inputs and its images.

**Authentication**: realm bearer token.

**Behavior**
- A newer workflow version, a newer head version of a pinned input on this node, a deleted or
  unreadable input, and an image now pinned to another digest each give one change.
- `recorded` holds the sources, remote files and images the run log recorded.

**Errors**
- 404 for an unknown run or one the caller cannot read.
- 503 when the coordinator failed."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The changes", body = Object, example = json!({
            "run_id": "01K8W2A0000000000000000000", "changed": true,
            "changes": [{"field": "inputs.reads", "before": "s3://reads/a.txt@01K8W1C0000000000000000000",
                "after": "s3://reads/a.txt@01K8W3C0000000000000000000",
                "summary": "the input has a newer version"}],
            "recorded": {"images": [{"named": "debian:stable-slim", "ran": "debian@sha256:00"}]}
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "The coordinator failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_changes(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<Value>> {
    let changes = RunChangesOperation::new(&auth, run_id(&run)?);
    let answer = drive(changes, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(answer))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/crate",
    tag = "compute/workflow-runs",
    summary = "Read the run crate of a workflow run",
    description = r#"Returns the RO-Crate 1.2 of a run as JSON-LD, with Aruna's facts merged in.

**Authentication**: realm bearer token.

**Behavior**
- The crate conforms to the Process, Workflow and Provenance Run Crate profiles 0.5. Its
  workflow action is `#workflow-run-<run_id>` with the run's API path as `url`.
- Inputs and outputs name their exact object versions; tasks name their node, placement mode
  and whether their result was reused. A reused result never names its producer.
- A run that asked for a crate has it stored when it ends; any other ended run gets one built
  for this read.

**Errors**
- 404 for an unknown run or one the caller cannot read.
- 409 while the run has not ended."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The crate JSON-LD", body = Object, example = json!({
            "@context": ["https://w3id.org/ro/crate/1.2/context", "https://w3id.org/ro/terms/workflow-run/context"],
            "@graph": [
                {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"}},
                {"@id": "./", "@type": "Dataset", "mentions": {"@id": "#workflow-run-01K8W2A0000000000000000000"},
                    "conformsTo": [{"@id": "https://w3id.org/ro/wfrun/process/0.5"},
                        {"@id": "https://w3id.org/ro/wfrun/workflow/0.5"},
                        {"@id": "https://w3id.org/ro/wfrun/provenance/0.5"}]},
                {"@id": "#workflow-run-01K8W2A0000000000000000000", "@type": "CreateAction",
                    "url": "/compute/workflow-runs/01K8W2A0000000000000000000"}
            ]
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 409, description = "The run has not ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_crate(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Json<Value>> {
    let read = RunCrateOperation::new(&auth, run_id(&run)?);
    let answer = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(answer))
}

#[utoipa::path(
    get,
    path = "/compute/workflow-runs/{run_id}/report",
    tag = "compute/workflow-runs",
    summary = "Download the report of a workflow run",
    description = r#"Returns the standalone HTML report of a run as a file to save.

**Authentication**: realm bearer token.

**Behavior**
- The report is uwe's run report from the run log; a run without a log gets a short report
  from its records.
- It is sent with `Content-Disposition: attachment`, `X-Content-Type-Options: nosniff` and
  `Content-Security-Policy: sandbox`, so a browser never shows it on this origin.

**Errors**
- 404 for an unknown run or one the caller cannot read.
- 503 when the coordinator failed."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The report as an HTML file to save", content_type = "text/html", body = String),
        (status = 404, description = "Unknown, or not visible to the caller", body = ErrorResponse),
        (status = 503, description = "The coordinator failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_report(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> ServerResult<Response> {
    let run_id = run_id(&run)?;
    let report = RunReportOperation::new(&auth, run_id);
    let html = drive(report, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(report_response(run_id, html))
}

/// The report as an attachment that a browser neither shows inline nor runs scripts of.
fn report_response(run_id: ulid::Ulid, html: String) -> Response {
    let mut response = (StatusCode::OK, html).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    let disposition = format!("attachment; filename=\"run-{run_id}-report.html\"");
    if let Ok(value) = HeaderValue::from_str(&disposition) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
