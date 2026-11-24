//! Serves GA4GH WES v1.1 over the workflow runs of this node: service info, start, list, read,
//! status and cancel of runs of registered workflow versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::structs::execution::workflow_run::{RunState, WorkflowRunRecord};
use aruna_core::structs::identity::features::Feature;
use aruna_ops_work::workflows::runs::admit::StartRunOperation;
use aruna_ops_work::workflows::runs::events::time_text;
use aruna_ops_work::workflows::runs::reads::{ListRunsOperation, RunFilter, RunSnapshotOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{FromRequest, Multipart, Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::run_wire::{RunFileBody, WorkflowRunRequest};
use super::runs::{cancel, run_id, run_request};
use super::{catalog_error, limit, ulid};
use crate::auth::RealmAuth;
use crate::error::ServerError;
use crate::server::state::ServerState;

/// The WES version this facade speaks.
const WES_VERSION: &str = "1.1.0";

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(service_info))
        .routes(routes!(list_runs, start_run))
        .routes(routes!(get_run))
        .routes(routes!(run_status))
        .routes(routes!(cancel_run))
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

/// A WES run request, as form fields or as one JSON object.
#[derive(Debug, Default, Deserialize, ToSchema)]
pub struct WesRunRequest {
    /// A registered workflow version: `<workflow_id>/versions/<version>`, optionally behind the
    /// node's url of that version.
    pub workflow_url: String,
    /// The run's values, an object or its JSON text.
    #[schema(value_type = Option<Object>)]
    pub workflow_params: Option<Value>,
    pub workflow_type: Option<String>,
    pub workflow_type_version: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub tags: Option<Value>,
    /// `group_id`, `output`, `inputs`, `placement` and `provenance` as `POST
    /// /compute/workflow-runs` takes them, an object or its JSON text.
    #[schema(value_type = Option<Object>)]
    pub workflow_engine_parameters: Option<Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WesRunId {
    run_id: String,
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
    post,
    path = "/ga4gh/wes/v1/runs",
    tag = "wes",
    summary = "Start a workflow run",
    description = r#"Starts a run of a registered workflow version on this node, like `POST /compute/workflow-runs`.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The body is `multipart/form-data` as WES defines it, or one JSON object with the same fields.
- `workflow_url` names `<workflow_id>/versions/<version>`, optionally behind the node's url of
  that version; nothing is fetched from it.
- `workflow_params` are the run's values. `workflow_engine_parameters` carries `group_id`,
  `output`, `inputs`, `placement` and `provenance` as `POST /compute/workflow-runs` takes them.
- `workflow_type`, `workflow_type_version` and `tags` are accepted and not checked; the
  registered version decides the language.

**Errors**
- 400 for a `workflow_url` that names no version, a `workflow_attachment`, a missing `group_id`,
  or any request `POST /compute/workflow-runs` refuses.
- 403 and 404 as for `POST /compute/workflow-runs`."#,
    request_body(content = WesRunRequest, description = "The run request, as form fields or as one JSON object", example = json!({
        "workflow_url": "01K8W1B3D5F7H9KMQ1S3V5X7Z2/versions/1",
        "workflow_params": {"message": "Hola"}, "workflow_type": "CWL",
        "workflow_type_version": "v1.2",
        "workflow_engine_parameters": {"group_id": "01JABCDEF0123456789ABCDEFG",
            "output": {"bucket": "results", "prefix": "hello/"}}
    })),
    responses(
        (status = 200, description = "The run started", body = WesRunId, example = json!({
            "run_id": "01K8W2A0000000000000000000"
        })),
        (status = 400, description = "An invalid request", body = WesErrorBody, example = json!({
            "msg": "workflow_url names no workflow version", "status_code": 400
        })),
        (status = 403, description = "A missing permission", body = WesErrorBody, example = json!({
            "msg": "Forbidden", "status_code": 403
        })),
        (status = 404, description = "The group or version is not visible", body = WesErrorBody, example = json!({
            "msg": "Not found", "status_code": 404
        }))
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    request: Request,
) -> WesResult<WesRunId> {
    let form = run_form(request, &state).await?;
    let request = run_request(&state, workflow_request(form)?, None)?;
    crate::features::require(&state, Feature::Workflows, Some(request.group_id)).await?;
    let node = (state.get_node_id(), state.get_realm_id());
    let start = StartRunOperation::new(&auth, node, request);
    let admitted = drive(start, &state.get_ctx())
        .await
        .map_err(catalog_error)?;
    Ok(Json(WesRunId {
        run_id: admitted.record.run_id.to_string(),
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

#[utoipa::path(
    post,
    path = "/ga4gh/wes/v1/runs/{run_id}/cancel",
    tag = "wes",
    summary = "Cancel a workflow run",
    description = r#"Cancels a run that has not ended, like `POST /compute/workflow-runs/{run_id}/cancel`.

**Authentication**: realm bearer token with WRITE on the group's workflows.

**Behavior**
- The run turns `CANCELING` and then `CANCELED` once its tasks stopped; an ended run stays as it
  is."#,
    params(("run_id" = String, Path, description = "Run id, as a 26-character ULID")),
    responses(
        (status = 200, description = "The cancel was asked for", body = WesRunId, example = json!({
            "run_id": "01K8W2A0000000000000000000"
        })),
        (status = 403, description = "The caller reads the run but may not change it", body = WesErrorBody, example = json!({
            "msg": "Forbidden", "status_code": 403
        })),
        (status = 404, description = "Unknown, or not visible to the caller", body = WesErrorBody, example = json!({
            "msg": "Not found", "status_code": 404
        }))
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_run(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Path(run): Path<String>,
) -> WesResult<WesRunId> {
    let record = cancel(&state, &auth, run_id(&run)?).await?;
    Ok(Json(WesRunId {
        run_id: record.run_id.to_string(),
    }))
}

/// The run request of a WES body: form fields, or one JSON object.
async fn run_form(request: Request, state: &Arc<ServerState>) -> Result<WesRunRequest, WesError> {
    let content_type = (request.headers().get(header::CONTENT_TYPE))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if !content_type.starts_with("multipart/form-data") {
        let Json(form) = Json::<WesRunRequest>::from_request(request, state)
            .await
            .map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
        return Ok(form);
    }
    let bad = |error: String| WesError(ServerError::BadRequestReason(error));
    let mut multipart = Multipart::from_request(request, state)
        .await
        .map_err(|error| bad(error.body_text()))?;
    let mut form = WesRunRequest::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| bad(e.body_text()))?
    {
        let name = field.name().unwrap_or_default().to_owned();
        if name == "workflow_attachment" {
            return Err(bad(
                "workflow_attachment is not supported: register the workflow first".into(),
            ));
        }
        let text = field.text().await.map_err(|error| bad(error.body_text()))?;
        let value = || Some(Value::String(text.clone()));
        match name.as_str() {
            "workflow_url" => form.workflow_url = text.clone(),
            "workflow_params" => form.workflow_params = value(),
            "workflow_type" => form.workflow_type = Some(text.clone()),
            "workflow_type_version" => form.workflow_type_version = Some(text.clone()),
            "tags" => form.tags = value(),
            "workflow_engine_parameters" => form.workflow_engine_parameters = value(),
            _ => {}
        }
    }
    Ok(form)
}

/// An object given as is or as its JSON text; absent is an empty object.
fn object_of(value: Option<Value>, name: &str) -> Result<Map<String, Value>, ServerError> {
    let value = match value {
        Some(Value::String(text)) if text.trim().is_empty() => return Ok(Map::new()),
        Some(Value::String(text)) => serde_json::from_str(&text)
            .map_err(|_| ServerError::BadRequestReason(format!("{name} is no JSON object")))?,
        Some(value) => value,
        None => return Ok(Map::new()),
    };
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(ServerError::BadRequestReason(format!(
            "{name} is no JSON object"
        ))),
    }
}

/// The workflow run request a WES request asks for.
fn workflow_request(form: WesRunRequest) -> Result<WorkflowRunRequest, ServerError> {
    let parts: Vec<&str> = form
        .workflow_url
        .trim_end_matches('/')
        .rsplit('/')
        .collect();
    let [version, "versions", workflow_id, ..] = parts.as_slice() else {
        return Err(ServerError::BadRequestReason(
            "workflow_url names no workflow version".into(),
        ));
    };
    let mut request = object_of(
        form.workflow_engine_parameters,
        "workflow_engine_parameters",
    )?;
    if !request.contains_key("group_id") {
        return Err(ServerError::BadRequestReason(
            "workflow_engine_parameters needs a group_id".into(),
        ));
    }
    let values = object_of(form.workflow_params, "workflow_params")?;
    request.insert("workflow_id".into(), json!(workflow_id));
    request.insert("version".into(), json!(version));
    request.insert("values".into(), Value::Object(values));
    serde_json::from_value(Value::Object(request))
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))
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

#[cfg(test)]
#[path = "wes_tests.rs"]
mod tests;
