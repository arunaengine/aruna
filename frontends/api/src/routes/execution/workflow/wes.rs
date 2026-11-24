//! Serves GA4GH WES v1.1 over the workflow runs of this node: service info, start, list, read,
//! status and cancel of runs of registered workflow versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord};
use aruna_ops_work::workflows::runs::events::time_text;
use aruna_ops_work::workflows::runs::reads::{ListRunsOperation, RunFilter, RunSnapshotOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::RunFileBody;
use super::runs::run_id;
use super::{catalog_error, limit, ulid};
use crate::auth::RealmAuth;
use crate::error::ServerError;
use crate::server::state::ServerState;

/// The WES version this facade speaks.
const WES_VERSION: &str = "1.1.0";

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(service_info))
        .routes(routes!(list_runs))
        .routes(routes!(get_run))
        .routes(routes!(run_status))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesServiceType {
    group: &'static str,
    artifact: &'static str,
    version: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesTypeVersions {
    workflow_type_version: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesServiceInfo {
    id: String,
    name: String,
    r#type: WesServiceType,
    description: String,
    version: String,
    workflow_type_versions: BTreeMap<String, WesTypeVersions>,
    supported_wes_versions: Vec<String>,
    supported_filesystem_protocols: Vec<String>,
    workflow_engine_versions: BTreeMap<String, String>,
    #[serde(rename = "default_workflow_engine_parameters")]
    #[schema(value_type = Vec<Object>)]
    default_parameters: Vec<Value>,
    system_state_counts: BTreeMap<String, u64>,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRunStatus {
    run_id: String,
    /// `QUEUED`, `INITIALIZING`, `RUNNING`, `COMPLETE`, `EXECUTOR_ERROR`, `CANCELING`,
    /// `CANCELED` or `UNKNOWN`.
    state: &'static str,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRunSummary {
    run_id: String,
    state: &'static str,
    start_time: Option<String>,
    end_time: Option<String>,
    tags: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRunList {
    runs: Vec<WesRunSummary>,
    next_page_token: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesLog {
    name: String,
    cmd: Vec<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    stdout: Option<String>,
    stderr: Option<String>,
    exit_code: Option<i32>,
    system_logs: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRequestEcho {
    #[schema(value_type = Object)]
    workflow_params: Value,
    workflow_type: String,
    workflow_type_version: String,
    tags: BTreeMap<String, String>,
    #[schema(value_type = Object)]
    workflow_engine_parameters: Value,
    workflow_url: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRunLog {
    run_id: String,
    request: WesRequestEcho,
    state: &'static str,
    run_log: WesLog,
    task_logs_url: String,
    outputs: BTreeMap<String, RunFileBody>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesErrorBody {
    msg: String,
    status_code: u16,
}

#[derive(Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct WesPageQuery {
    /// Runs per page, 50 when left out, at most 500.
    pub page_size: Option<usize>,
    /// The `next_page_token` of the page before.
    pub page_token: Option<String>,
}

/// A node error in the WES error shape; internal detail is logged, never returned.
pub struct WesError(ServerError);

impl From<ServerError> for WesError {
    fn from(error: ServerError) -> Self {
        Self(error)
    }
}

impl IntoResponse for WesError {
    fn into_response(self) -> Response {
        let status = self.0.status_code();
        let msg = match status {
            StatusCode::INTERNAL_SERVER_ERROR => {
                tracing::error!(error = %self.0, "WES internal error");
                "Internal server error".to_owned()
            }
            _ => self.0.to_string(),
        };
        let body = WesErrorBody {
            msg,
            status_code: status.as_u16(),
        };
        (status, Json(body)).into_response()
    }
}

type WesResult<T> = Result<Json<T>, WesError>;

/// The WES name of a run state.
fn wes_state(state: RunState) -> &'static str {
    match state {
        RunState::Queued => "QUEUED",
        RunState::Planning => "INITIALIZING",
        RunState::Running => "RUNNING",
        RunState::Succeeded => "COMPLETE",
        RunState::Failed => "EXECUTOR_ERROR",
        RunState::Cancelling => "CANCELING",
        RunState::Cancelled => "CANCELED",
        RunState::Unknown => "UNKNOWN",
    }
}

#[utoipa::path(
    get,
    path = "/ga4gh/wes/v1/service-info",
    tag = "wes",
    summary = "Describe this GA4GH WES endpoint",
    description = r#"Describes this WES endpoint: its version and the workflow types it runs.

**Authentication**: none; every caller sees the same document.

**Behavior**
- Runs start only from workflow versions registered on this realm; the registered version
  decides the language, so `workflow_type_version` lists no versions.
- `system_state_counts` stays empty: the node does not count runs for this document."#,
    responses((status = 200, description = "Description of this WES endpoint", body = WesServiceInfo, example = json!({
        "id": "org.aruna.wes", "name": "Aruna WES",
        "type": {"group": "org.ga4gh", "artifact": "wes", "version": "1.1.0"},
        "description": "Aruna WES facade over the workflow runs of this node.",
        "version": "3.0.0-alpha.41",
        "workflow_type_versions": {"CWL": {"workflow_type_version": []}},
        "supported_wes_versions": ["1.1.0"], "supported_filesystem_protocols": ["s3"],
        "workflow_engine_versions": {"uwe": "3.0.0-alpha.41"},
        "default_workflow_engine_parameters": [], "system_state_counts": {}, "tags": {}
    })))
)]
pub async fn service_info() -> Json<WesServiceInfo> {
    let types = ["CWL", "NFL", "SMK", "WDL", "GALAXY", "ARGO", "WIR", "UWE"];
    let version = env!("CARGO_PKG_VERSION");
    Json(WesServiceInfo {
        id: "org.aruna.wes".to_owned(),
        name: "Aruna WES".to_owned(),
        r#type: WesServiceType {
            group: "org.ga4gh",
            artifact: "wes",
            version: WES_VERSION,
        },
        description: "Aruna WES facade over the workflow runs of this node.".to_owned(),
        version: version.to_owned(),
        workflow_type_versions: (types.iter())
            .map(|name| {
                let versions = WesTypeVersions {
                    workflow_type_version: Vec::new(),
                };
                ((*name).to_owned(), versions)
            })
            .collect(),
        supported_wes_versions: vec![WES_VERSION.to_owned()],
        supported_filesystem_protocols: vec!["s3".to_owned()],
        workflow_engine_versions: BTreeMap::from([("uwe".to_owned(), version.to_owned())]),
        default_parameters: Vec::new(),
        system_state_counts: BTreeMap::new(),
        tags: BTreeMap::new(),
    })
}

#[utoipa::path(
    get,
    path = "/ga4gh/wes/v1/runs",
    tag = "wes",
    summary = "List workflow runs",
    description = r#"Lists the runs of this node whose group the caller reads, newest first.

**Authentication**: realm bearer token.

**Behavior**
- Runs coordinated by other nodes are not listed here.
- `next_page_token` continues the list and is empty on the last page."#,
    params(WesPageQuery),
    responses(
        (status = 200, description = "One page of runs", body = WesRunList, example = json!({
            "runs": [{"run_id": "01K8W2A0000000000000000000", "state": "RUNNING",
                "start_time": "2026-11-24T08:00:01+00:00", "end_time": null, "tags": {}}],
            "next_page_token": ""
        })),
        (status = 400, description = "A malformed page token", body = WesErrorBody, example = json!({
            "msg": "page_token is not a ULID", "status_code": 400
        }))
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_runs(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<WesPageQuery>,
) -> WesResult<WesRunList> {
    let after = (query.page_token.as_deref())
        .filter(|token| !token.is_empty())
        .map(|token| ulid(token, "page_token"))
        .transpose()?;
    let list = ListRunsOperation::new(&auth, RunFilter::default(), after, limit(query.page_size));
    let page = drive(list, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(WesRunList {
        runs: (page.items.iter())
            .map(|record| WesRunSummary {
                run_id: record.run_id.to_string(),
                state: wes_state(record.state),
                start_time: record.started_at_ms.and_then(time_text),
                end_time: record.finished_at_ms.and_then(time_text),
                tags: BTreeMap::new(),
            })
            .collect(),
        next_page_token: page.next.map(|run| run.to_string()).unwrap_or_default(),
    }))
}

#[utoipa::path(
    get,
    path = "/ga4gh/wes/v1/runs/{run_id}",
    tag = "wes",
    summary = "Read a workflow run",
    description = r#"Returns a run's request, state, times and delivered outputs.

**Authentication**: realm bearer token.

**Behavior**
- `task_logs_url` lists the run's tasks with `GET /compute/workflow-runs/{run_id}/tasks`.
- `outputs` names each delivered file by its bucket, key and exact version.
- A run the caller cannot read is not found."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The run", body = WesRunLog, example = json!({
            "run_id": "01K8W2A0000000000000000000",
            "request": {"workflow_params": {"message": "Hola"}, "workflow_type": "",
                "workflow_type_version": "", "tags": {},
                "workflow_engine_parameters": {"group_id": "01JABCDEF0123456789ABCDEFG"},
                "workflow_url": "01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/1"},
            "state": "COMPLETE",
            "run_log": {"name": "hello", "cmd": [], "start_time": "2026-11-24T08:00:01+00:00",
                "end_time": "2026-11-24T08:01:00+00:00", "stdout": null, "stderr": null,
                "exit_code": null, "system_logs": []},
            "task_logs_url": "/compute/workflow-runs/01K8W2A0000000000000000000/tasks",
            "outputs": {"said.txt": {"name": "said.txt", "bucket": "results",
                "key": "hello/said.txt", "version_id": "01K8W2C0000000000000000000",
                "node_id": null, "size": 5}}
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = WesErrorBody, example = json!({
            "msg": "Not found", "status_code": 404
        }))
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> WesResult<WesRunLog> {
    let read = RunSnapshotOperation::new(&auth, run_id(&run)?);
    let snapshot = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(run_log(&snapshot.record)))
}

#[utoipa::path(
    get,
    path = "/ga4gh/wes/v1/runs/{run_id}/status",
    tag = "wes",
    summary = "Read the state of a workflow run",
    description = r#"Returns the WES state of a run.

**Authentication**: realm bearer token.

**Behavior**
- A run the caller cannot read is not found."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The run's state", body = WesRunStatus, example = json!({
            "run_id": "01K8W2A0000000000000000000", "state": "RUNNING"
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = WesErrorBody, example = json!({
            "msg": "Not found", "status_code": 404
        }))
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_status(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> WesResult<WesRunStatus> {
    let read = RunSnapshotOperation::new(&auth, run_id(&run)?);
    let snapshot = drive(read, &state.get_ctx()).await.map_err(catalog_error)?;
    Ok(Json(WesRunStatus {
        run_id: snapshot.record.run_id.to_string(),
        state: wes_state(snapshot.record.state),
    }))
}

/// The WES view of a run.
fn run_log(record: &WorkflowRunRecord) -> WesRunLog {
    let mut parameters = json!({"group_id": record.group_id.to_string()});
    if let Some(output) = &record.output {
        parameters["output"] = json!({"bucket": output.bucket, "prefix": output.prefix});
    }
    WesRunLog {
        run_id: record.run_id.to_string(),
        request: WesRequestEcho {
            workflow_params: serde_json::from_str(&record.values_json).unwrap_or(json!({})),
            workflow_type: String::new(),
            workflow_type_version: String::new(),
            tags: BTreeMap::new(),
            workflow_engine_parameters: parameters,
            workflow_url: format!("{}/versions/{}", record.workflow_id, record.version),
        },
        state: wes_state(record.state),
        run_log: WesLog {
            name: record.workflow_name.clone(),
            cmd: Vec::new(),
            start_time: record.started_at_ms.and_then(time_text),
            end_time: record.finished_at_ms.and_then(time_text),
            stdout: None,
            stderr: None,
            exit_code: None,
            system_logs: record.error.iter().cloned().collect(),
        },
        task_logs_url: format!("/compute/workflow-runs/{}/tasks", record.run_id),
        outputs: (record.outputs.iter())
            .map(|file| (file.name.clone(), RunFileBody::from(file)))
            .collect(),
    }
}
