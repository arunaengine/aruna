use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId,
    JobPayload, JobRecord, JobResultPayload, JobState,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::jobs::service::{
    CancelJobOutcome, cancel_owned_job, list_owned_jobs, read_owned_job, submit_execution_job,
};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

use crate::auth::require_unrestricted_realm_auth;
use crate::error::ServerError;
use crate::server_state::ServerState;

/// GA4GH TES version this facade implements.
const TES_VERSION: &str = "1.1.0";
/// Required tag naming the workspace parent group a task writes into.
const GROUP_TAG_KEY: &str = "aruna.io/group";
/// Optional tag pinning a backend executor kind.
const EXECUTOR_TAG_KEY: &str = "aruna.io/executor";
/// Optional tag carrying the submission idempotency key.
const IDEMPOTENCY_TAG_KEY: &str = "aruna.io/idempotency-key";

const DEFAULT_PAGE_SIZE: usize = 256;
const MAX_PAGE_SIZE: usize = 512;

#[derive(OpenApi)]
#[openapi(
    tags((name = "tes", description = "GA4GH TES v1.1 task execution facade")),
    paths(service_info, create_task, list_tasks, get_task, cancel_task),
    components(schemas(
        TesServiceInfo,
        TesServiceType,
        TesServiceOrganization,
        TesTask,
        TesExecutor,
        TesInput,
        TesOutput,
        TesResources,
        TesFileType,
        TesState,
        TesExecutorLog,
        TesOutputFileLog,
        TesTaskLog,
        TesCreateTaskResponse,
        TesListTasksResponse,
        TesErrorPayload
    ))
)]
pub struct TesApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/ga4gh/tes/v1/service-info", get(service_info))
        .route("/ga4gh/tes/v1/tasks", get(list_tasks).post(create_task))
        .route("/ga4gh/tes/v1/tasks/{id}", get(get_task).post(cancel_task))
}

// ---------------------------------------------------------------------------
// TES wire types (GA4GH TES v1.1, snake_case per the TES OpenAPI).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TesState {
    Unknown,
    Queued,
    Initializing,
    Running,
    Paused,
    Complete,
    ExecutorError,
    SystemError,
    Canceled,
    Canceling,
    Preempted,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TesFileType {
    #[default]
    File,
    Directory,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesExecutor {
    #[serde(default)]
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub path: String,
    #[serde(rename = "type", default)]
    pub kind: TesFileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub path: String,
    #[serde(rename = "type", default)]
    pub kind: TesFileType,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesResources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preemptible: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_gb: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub zones: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesExecutorLog {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesOutputFileLog {
    pub url: String,
    pub path: String,
    pub size_bytes: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesTaskLog {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<TesExecutorLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<TesOutputFileLog>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub system_logs: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesTask {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<TesState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<TesInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<TesOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<TesResources>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executors: Vec<TesExecutor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<TesTaskLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TesCreateTaskResponse {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TesListTasksResponse {
    pub tasks: Vec<TesTask>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceType {
    group: &'static str,
    artifact: &'static str,
    version: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceOrganization {
    name: String,
    url: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceInfo {
    id: String,
    name: String,
    r#type: TesServiceType,
    description: String,
    organization: TesServiceOrganization,
    documentation_url: Option<String>,
    environment: String,
    version: String,
    storage: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesErrorPayload {
    status_code: u16,
    msg: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ViewQuery {
    view: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListTasksQuery {
    view: Option<String>,
    page_size: Option<usize>,
    page_token: Option<String>,
    name_prefix: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TesView {
    Minimal,
    Basic,
    Full,
}

impl TesView {
    fn parse(view: Option<&str>) -> Result<Self, TesError> {
        match view.unwrap_or("MINIMAL") {
            "MINIMAL" => Ok(Self::Minimal),
            "BASIC" => Ok(Self::Basic),
            "FULL" => Ok(Self::Full),
            other => Err(TesError::bad_request(format!("unknown view `{other}`"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/service-info",
    tag = "tes",
    responses((status = 200, body = TesServiceInfo))
)]
pub async fn service_info(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    let base_url = external_base_url(&headers);
    let info = TesServiceInfo {
        id: format!("org.aruna.{}", state.get_realm_id()),
        name: format!("Aruna Realm {}", state.get_realm_id()),
        r#type: TesServiceType {
            group: "org.ga4gh",
            artifact: "tes",
            version: TES_VERSION.to_string(),
        },
        // Deviations from full TES conformance surface here so a client can discover them.
        description: "Aruna TES facade over the internal execution job model. Deviations: \
             exactly one executor per task (multi-executor arrays are rejected); PAUSE is not \
             supported and never emitted."
            .to_string(),
        organization: TesServiceOrganization {
            name: "Aruna".to_string(),
            url: base_url,
        },
        documentation_url: Some("https://docs.aruna-engine.org".to_string()),
        environment: "dev".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        storage: Vec::new(),
    };
    tes_json_response(StatusCode::OK, info)
}

#[utoipa::path(
    post,
    path = "/ga4gh/tes/v1/tasks",
    tag = "tes",
    request_body = TesTask,
    responses(
        (status = 200, body = TesCreateTaskResponse),
        (status = 400, body = TesErrorPayload),
        (status = 401, body = TesErrorPayload),
        (status = 403, body = TesErrorPayload)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(task): Json<TesTask>,
) -> Response {
    let auth = match require_unrestricted_realm_auth(&state, auth) {
        Ok(auth) => auth,
        Err(error) => return TesError::from_server(error).into_response(),
    };

    let (spec, idempotency_key) = match map_task_to_spec(&task) {
        Ok(mapped) => mapped,
        Err(error) => return error.into_response(),
    };

    // Group-write authorization at the facade boundary (the raw internal POST /jobs/
    // path does not yet check this; see #420). Rejecting here never widens that gap.
    if let Err(error) = ensure_group_write(&state, &auth, spec.group_id).await {
        return error.into_response();
    }

    match submit_execution_job(
        &state.get_ctx(),
        spec,
        auth.user_id,
        state.get_node_id(),
        idempotency_key,
    )
    .await
    {
        Ok(result) => tes_json_response(
            StatusCode::OK,
            TesCreateTaskResponse {
                id: result.job_id.to_string(),
            },
        ),
        Err(aruna_operations::jobs::submit::SubmitJobError::JobPlanConflict {
            existing_job_id,
        }) => TesError::conflict(format!(
            "idempotency key already bound to task {existing_job_id}"
        ))
        .into_response(),
        Err(error) => TesError::internal(error.to_string()).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/tasks/{id}",
    tag = "tes",
    params(
        ("id" = String, Path, description = "TES task id (the JobId)"),
        ("view" = Option<String>, Query, description = "MINIMAL | BASIC | FULL")
    ),
    responses(
        (status = 200, body = TesTask),
        (status = 404, body = TesErrorPayload)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ViewQuery>,
) -> Response {
    let auth = match require_unrestricted_realm_auth(&state, auth) {
        Ok(auth) => auth,
        Err(error) => return TesError::from_server(error).into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let job_id = match JobId::from_str(&id) {
        Ok(job_id) => job_id,
        Err(_) => return TesError::not_found("TES task not found").into_response(),
    };

    let record = match read_owned_job(&state.get_ctx(), auth.user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return TesError::not_found("TES task not found").into_response(),
        Err(error) => return TesError::internal(error).into_response(),
    };
    // Only execution jobs are TES tasks; other job kinds are not addressable here.
    if !matches!(record.payload, JobPayload::Execution(_)) {
        return TesError::not_found("TES task not found").into_response();
    }

    let base_url = external_base_url(&headers);
    tes_json_response(StatusCode::OK, project_task(&record, view, &base_url))
}

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/tasks",
    tag = "tes",
    params(
        ("view" = Option<String>, Query, description = "MINIMAL | BASIC | FULL"),
        ("page_size" = Option<usize>, Query, description = "Max tasks per page"),
        ("page_token" = Option<String>, Query, description = "Opaque page token"),
        ("name_prefix" = Option<String>, Query, description = "Ignored; accepted for TES compatibility")
    ),
    responses(
        (status = 200, body = TesListTasksResponse),
        (status = 400, body = TesErrorPayload),
        (status = 401, body = TesErrorPayload)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_tasks(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Query(query): Query<ListTasksQuery>,
) -> Response {
    let auth = match require_unrestricted_realm_auth(&state, auth) {
        Ok(auth) => auth,
        Err(error) => return TesError::from_server(error).into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let _ = query.name_prefix;
    let cursor = match decode_page_token(query.page_token.as_deref()) {
        Ok(cursor) => cursor,
        Err(error) => return error.into_response(),
    };
    let limit = query
        .page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .min(MAX_PAGE_SIZE);

    let (records, next_cursor) =
        match list_owned_jobs(&state.get_ctx(), auth.user_id, cursor, limit, None).await {
            Ok(page) => page,
            Err(error) => return TesError::internal(error).into_response(),
        };

    let base_url = external_base_url(&headers);
    let tasks = records
        .iter()
        .filter(|record| matches!(record.payload, JobPayload::Execution(_)))
        .map(|record| project_task(record, view, &base_url))
        .collect();

    tes_json_response(
        StatusCode::OK,
        TesListTasksResponse {
            tasks,
            next_page_token: next_cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor)),
        },
    )
}

#[utoipa::path(
    post,
    path = "/ga4gh/tes/v1/tasks/{id}:cancel",
    tag = "tes",
    params(("id" = String, Path, description = "TES task id (the JobId)")),
    responses(
        (status = 200, description = "Cancellation requested"),
        (status = 404, body = TesErrorPayload)
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> Response {
    let auth = match require_unrestricted_realm_auth(&state, auth) {
        Ok(auth) => auth,
        Err(error) => return TesError::from_server(error).into_response(),
    };
    // TES addresses cancellation as `POST /tasks/{id}:cancel`; the `:cancel` action
    // suffix rides on the final path segment, so strip it here.
    let Some(raw_id) = id.strip_suffix(":cancel") else {
        return TesError::bad_request("cancel requires the :cancel action suffix").into_response();
    };
    let job_id = match JobId::from_str(raw_id) {
        Ok(job_id) => job_id,
        Err(_) => return TesError::not_found("TES task not found").into_response(),
    };
    let record = match read_owned_job(&state.get_ctx(), auth.user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return TesError::not_found("TES task not found").into_response(),
        Err(error) => return TesError::internal(error).into_response(),
    };
    if !matches!(record.payload, JobPayload::Execution(_)) {
        return TesError::not_found("TES task not found").into_response();
    }

    match cancel_owned_job(
        &state.get_ctx(),
        &state.jobs_runtime(),
        auth.user_id,
        job_id,
    )
    .await
    {
        Ok(CancelJobOutcome::NotFound) => TesError::not_found("TES task not found").into_response(),
        Ok(CancelJobOutcome::AlreadyTerminal(_) | CancelJobOutcome::Requested(_)) => {
            tes_json_response(StatusCode::OK, serde_json::json!({}))
        }
        Err(error) => TesError::internal(error).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Mapping: TesTask -> ExecutionSpec
// ---------------------------------------------------------------------------

/// Map a TES task onto the internal execution plan and optional dedup key.
/// Pure and self-contained: the group-write permission check happens separately.
fn map_task_to_spec(task: &TesTask) -> Result<(ExecutionSpec, Option<String>), TesError> {
    let executor = match task.executors.as_slice() {
        [executor] => executor,
        [] => {
            return Err(TesError::bad_request(
                "a task requires exactly one executor",
            ));
        }
        _ => {
            return Err(TesError::bad_request(
                "multiple executors are not supported; this facade runs a single executor per task",
            ));
        }
    };

    if executor.image.trim().is_empty() {
        return Err(TesError::bad_request("executor image is required"));
    }
    if executor.command.is_empty() {
        return Err(TesError::bad_request("executor command is required"));
    }
    if executor.workdir.as_deref().is_some_and(|w| !w.is_empty()) {
        return Err(TesError::bad_request(
            "executor workdir is not supported by the internal execution model",
        ));
    }

    let group_raw = task
        .tags
        .get(GROUP_TAG_KEY)
        .ok_or_else(|| TesError::bad_request(format!("a `{GROUP_TAG_KEY}` tag is required")))?;
    let group_id = Ulid::from_string(group_raw)
        .map_err(|_| TesError::bad_request(format!("`{GROUP_TAG_KEY}` is not a valid group id")))?;

    let mut inputs: Vec<InputSelection> = Vec::with_capacity(task.inputs.len());
    for input in &task.inputs {
        let input = map_input(input)?;
        if inputs
            .iter()
            .any(|existing| existing.dest_key == input.dest_key)
        {
            return Err(TesError::bad_request("duplicate input destination"));
        }
        inputs.push(input);
    }
    let output_prefixes = task.outputs.iter().map(map_output).collect();

    let ram_bytes = task
        .resources
        .as_ref()
        .and_then(|r| r.ram_gb)
        .map(|gb| {
            let bytes = (gb * 1_000_000_000.0) as u64;
            if !gb.is_finite() || gb <= 0.0 || bytes == 0 || bytes > i64::MAX as u64 {
                return Err(TesError::bad_request("invalid ram_gb"));
            }
            Ok(bytes)
        })
        .transpose()?;
    let resources = ComputeResources {
        cpu_cores: task.resources.as_ref().and_then(|r| r.cpu_cores),
        ram_bytes,
        max_walltime_ms: None,
    };

    let spec = ExecutionSpec {
        group_id,
        image: executor.image.clone(),
        // TES `command` is the full argv; override the image ENTRYPOINT with it and
        // leave the image CMD unset so exactly the requested argv runs.
        entrypoint: Some(executor.command.clone()),
        command: Vec::new(),
        env: executor.env.clone(),
        resources,
        executor_constraint: task.tags.get(EXECUTOR_TAG_KEY).cloned(),
        inputs,
        output_prefixes,
    };

    // Handed over as the raw idempotency key: `submit_execution_job` applies the per-user
    // `user/` namespacing itself, so TES inherits dedup scoping for free.
    let idempotency_key = task.tags.get(IDEMPOTENCY_TAG_KEY).cloned();

    Ok((spec, idempotency_key))
}

fn map_input(input: &TesInput) -> Result<InputSelection, TesError> {
    if input.content.is_some() {
        return Err(TesError::bad_request(
            "inline input content is not supported",
        ));
    }
    let url = input
        .url
        .as_deref()
        .ok_or_else(|| TesError::bad_request("input url is required"))?;
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| TesError::bad_request("only s3:// input urls are supported"))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| TesError::bad_request("s3 input url must be s3://bucket/key"))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(TesError::bad_request(
            "s3 input url must be s3://bucket/key",
        ));
    }
    let dest_key = if input.path.is_empty() {
        key.to_string()
    } else {
        input.path.trim_start_matches('/').to_string()
    };
    Ok(InputSelection {
        source: InputSource::S3 {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
        },
        dest_key,
        mode: InputMode::Snapshot,
    })
}

fn map_output(output: &TesOutput) -> String {
    output.path.trim_start_matches('/').to_string()
}

// ---------------------------------------------------------------------------
// Mapping: JobRecord -> TesTask / TesState
// ---------------------------------------------------------------------------

/// Map an internal job state onto its TES external state. `Failed` splits on
/// evidence: a captured container exit code is an executor error, an evidence-free
/// failure is a system error. `Indeterminate` maps to TES `UNKNOWN`.
fn tes_state(record: &JobRecord) -> TesState {
    if record.cancel_requested && !record.state.is_terminal() {
        return TesState::Canceling;
    }
    match record.state {
        JobState::Queued | JobState::Claimed => TesState::Queued,
        JobState::Preparing | JobState::Ready => TesState::Initializing,
        JobState::Running => TesState::Running,
        JobState::Cancelling => TesState::Canceling,
        JobState::Indeterminate => TesState::Unknown,
        JobState::Succeeded => TesState::Complete,
        JobState::Failed => match &record.result {
            Some(JobResultPayload::Execution {
                exit_code: Some(_), ..
            }) => TesState::ExecutorError,
            _ => TesState::SystemError,
        },
        JobState::Cancelled => TesState::Canceled,
    }
}

fn project_task(record: &JobRecord, view: TesView, base_url: &str) -> TesTask {
    let id = record.job_id.to_string();
    let state = tes_state(record);
    if view == TesView::Minimal {
        return TesTask {
            id: Some(id),
            state: Some(state),
            ..Default::default()
        };
    }

    let JobPayload::Execution(spec) = &record.payload else {
        // Never reached for a TES task id, but keep the projection total.
        return TesTask {
            id: Some(id),
            state: Some(state),
            ..Default::default()
        };
    };

    let command = spec
        .entrypoint
        .clone()
        .unwrap_or_else(|| spec.command.clone());
    let executors = vec![TesExecutor {
        image: spec.image.clone(),
        command,
        env: spec.env.clone(),
        ..Default::default()
    }];

    let inputs = spec
        .inputs
        .iter()
        .map(|input| {
            let InputSource::S3 { bucket, key, .. } = &input.source;
            TesInput {
                url: Some(format!("s3://{bucket}/{key}")),
                path: input.dest_key.clone(),
                kind: TesFileType::File,
                ..Default::default()
            }
        })
        .collect();

    let workspace = record.workspace_bucket.clone();
    let outputs = spec
        .output_prefixes
        .iter()
        .map(|prefix| TesOutput {
            url: workspace
                .as_ref()
                .map(|bucket| format!("s3://{bucket}/{prefix}")),
            path: prefix.clone(),
            kind: TesFileType::File,
            ..Default::default()
        })
        .collect();

    let resources = Some(TesResources {
        cpu_cores: spec.resources.cpu_cores,
        ram_gb: spec
            .resources
            .ram_bytes
            .map(|bytes| bytes as f64 / 1_000_000_000.0),
        ..Default::default()
    });

    let mut tags = BTreeMap::from([(GROUP_TAG_KEY.to_string(), spec.group_id.to_string())]);
    if let Some(constraint) = &spec.executor_constraint {
        tags.insert(EXECUTOR_TAG_KEY.to_string(), constraint.clone());
    }

    let mut log = build_task_log(record, base_url);
    if view == TesView::Basic {
        log.system_logs.clear();
    }

    TesTask {
        id: Some(id),
        state: Some(state),
        executors,
        inputs,
        outputs,
        resources,
        tags,
        logs: vec![log],
        creation_time: Some(rfc3339(record.created_at_ms)),
        ..Default::default()
    }
}

fn build_task_log(record: &JobRecord, _base_url: &str) -> TesTaskLog {
    let started =
        matches!(record.state, JobState::Running | JobState::Cancelling) || record.result.is_some();
    let start_time = started.then(|| rfc3339(record.created_at_ms));
    let mut executor_log = TesExecutorLog {
        start_time: start_time.clone(),
        end_time: record.finished_at_ms.map(rfc3339),
        ..Default::default()
    };
    let mut outputs = Vec::new();
    if let Some(JobResultPayload::Execution {
        exit_code,
        workspace_bucket,
        outputs: captured,
    }) = &record.result
    {
        executor_log.exit_code = *exit_code;
        outputs = captured
            .iter()
            .map(|output| TesOutputFileLog {
                url: format!("s3://{workspace_bucket}/{}", output.key),
                path: output.key.clone(),
                size_bytes: output.size.to_string(),
            })
            .collect();
    }
    // stdout/stderr live as node-local log chunks and are not stored on the record,
    // so they are not surfaced inline here.
    let system_logs = record
        .last_error
        .as_ref()
        .map(|error| vec![error.message.clone()])
        .unwrap_or_default();

    TesTaskLog {
        logs: vec![executor_log],
        start_time,
        end_time: record.finished_at_ms.map(rfc3339),
        outputs,
        system_logs,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn ensure_group_write(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> Result<(), TesError> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{}/g/{group_id}/data/**", state.get_realm_id()),
            required_permission: aruna_core::structs::Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => TesError::forbidden("no write access to group"),
        other => TesError::internal(other.to_string()),
    })?;
    if allowed {
        Ok(())
    } else {
        Err(TesError::forbidden("no write access to group"))
    }
}

fn decode_page_token(token: Option<&str>) -> Result<Option<Vec<u8>>, TesError> {
    match token {
        Some(token) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(token)
                .map_err(|_| TesError::bad_request("invalid page_token"))?;
            if bytes.len() != 24 {
                return Err(TesError::bad_request("invalid page_token"));
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

fn external_base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("http");
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(http::header::HOST))
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
}

fn tes_json_response<T: Serialize>(status: StatusCode, value: T) -> Response {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

#[derive(Debug)]
struct TesError {
    status: StatusCode,
    message: String,
}

impl TesError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn from_server(error: ServerError) -> Self {
        match error {
            ServerError::Unauthorized => Self {
                status: StatusCode::UNAUTHORIZED,
                message: "unauthorized".to_string(),
            },
            ServerError::Forbidden => Self::forbidden("forbidden"),
            ServerError::NotFound => Self::not_found("TES task not found"),
            other => Self::internal(other.to_string()),
        }
    }
}

impl IntoResponse for TesError {
    fn into_response(self) -> Response {
        tes_json_response(
            self.status,
            TesErrorPayload {
                status_code: self.status.as_u16(),
                msg: self.message,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{NodeCapabilities, OutputObject, RealmId};
    use aruna_core::types::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::insert_job;
    use aruna_storage::FjallStorage;
    use tempfile::TempDir;

    fn realm() -> RealmId {
        RealmId([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    fn user(byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([byte; 16]), realm())
    }

    fn auth_for(user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
        })
    }

    async fn build_state() -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let state = ServerState::new(
            ctx,
            realm(),
            node_id(),
            NodeCapabilities::local_node(realm()).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        (dir, Arc::new(state))
    }

    fn sample_task(group: Ulid) -> TesTask {
        TesTask {
            executors: vec![TesExecutor {
                image: "alpine:3".to_string(),
                command: vec!["echo".to_string(), "hi".to_string()],
                env: BTreeMap::from([("K".to_string(), "V".to_string())]),
                ..Default::default()
            }],
            inputs: vec![TesInput {
                url: Some("s3://src/data.csv".to_string()),
                path: "in/data.csv".to_string(),
                kind: TesFileType::File,
                ..Default::default()
            }],
            outputs: vec![TesOutput {
                path: "out/".to_string(),
                ..Default::default()
            }],
            resources: Some(TesResources {
                cpu_cores: Some(2),
                ram_gb: Some(4.0),
                ..Default::default()
            }),
            tags: BTreeMap::from([(GROUP_TAG_KEY.to_string(), group.to_string())]),
            ..Default::default()
        }
    }

    fn execution_record(job_id: JobId, owner: UserId, spec: ExecutionSpec) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Execution(spec),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        )
    }

    #[test]
    fn maps_task() {
        let group = Ulid::from_bytes([5u8; 16]);
        let (spec, dedup) = map_task_to_spec(&sample_task(group)).unwrap();
        assert_eq!(spec.group_id, group);
        assert_eq!(spec.image, "alpine:3");
        // TES command becomes the entrypoint override; image CMD stays empty.
        assert_eq!(spec.entrypoint, Some(vec!["echo".into(), "hi".into()]));
        assert!(spec.command.is_empty());
        assert_eq!(spec.env.get("K").map(String::as_str), Some("V"));
        assert_eq!(spec.resources.cpu_cores, Some(2));
        assert_eq!(spec.resources.ram_bytes, Some(4_000_000_000));
        assert_eq!(spec.inputs.len(), 1);
        assert_eq!(spec.inputs[0].dest_key, "in/data.csv");
        assert_eq!(spec.output_prefixes, vec!["out/".to_string()]);
        assert!(dedup.is_none());
    }

    #[test]
    fn rejects_duplicate_inputs() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        let mut input = task.inputs[0].clone();
        input.url = Some("s3://src/other.csv".to_string());
        input.path.insert(0, '/');
        task.inputs.push(input);
        assert_eq!(
            map_task_to_spec(&task).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_invalid_ram() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        for ram_gb in [-1.0, 0.0, f64::NAN, 1e-10, f64::MAX] {
            task.resources.as_mut().unwrap().ram_gb = Some(ram_gb);
            assert_eq!(
                map_task_to_spec(&task).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[test]
    fn rejects_multi_executor() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors.push(task.executors[0].clone());
        let error = map_task_to_spec(&task).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("single executor"));
    }

    #[test]
    fn rejects_missing_group() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.tags.clear();
        let error = map_task_to_spec(&task).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains(GROUP_TAG_KEY));
    }

    #[test]
    fn rejects_workdir() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors[0].workdir = Some("/work".to_string());
        assert_eq!(
            map_task_to_spec(&task).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn maps_states() {
        let spec = || ExecutionSpec {
            group_id: Ulid::from_bytes([5u8; 16]),
            image: "img".to_string(),
            entrypoint: None,
            command: vec!["run".to_string()],
            env: BTreeMap::new(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            output_prefixes: Vec::new(),
        };
        let mut record = execution_record(JobId::from_bytes([1u8; 16]), user(2), spec());
        let cases = [
            (JobState::Queued, TesState::Queued),
            (JobState::Claimed, TesState::Queued),
            (JobState::Preparing, TesState::Initializing),
            (JobState::Ready, TesState::Initializing),
            (JobState::Running, TesState::Running),
            (JobState::Cancelling, TesState::Canceling),
            (JobState::Indeterminate, TesState::Unknown),
            (JobState::Succeeded, TesState::Complete),
            (JobState::Cancelled, TesState::Canceled),
        ];
        for (job_state, expected) in cases {
            record.state = job_state;
            assert_eq!(tes_state(&record), expected, "{job_state:?}");
        }
        record.state = JobState::Queued;
        record.cancel_requested = true;
        assert_eq!(tes_state(&record), TesState::Canceling);
        record.cancel_requested = false;
        // Failed splits on evidence.
        record.state = JobState::Failed;
        record.result = None;
        assert_eq!(tes_state(&record), TesState::SystemError);
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(1),
            workspace_bucket: "ws".to_string(),
            outputs: Vec::new(),
        });
        assert_eq!(tes_state(&record), TesState::ExecutorError);
    }

    #[test]
    fn view_projections() {
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16]))).unwrap();
        let mut record = execution_record(JobId::from_bytes([2u8; 16]), user(2), spec);
        let queued = project_task(&record, TesView::Full, "http://x");
        assert!(queued.logs[0].start_time.is_none());
        assert!(queued.logs[0].logs[0].start_time.is_none());
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record.workspace_bucket = Some("ws-x".to_string());
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: "ws-x".to_string(),
            outputs: vec![OutputObject {
                key: "out/r.txt".to_string(),
                size: 12,
                digest: None,
            }],
        });

        let minimal = project_task(&record, TesView::Minimal, "http://x");
        assert!(minimal.executors.is_empty());
        assert!(minimal.logs.is_empty());
        assert_eq!(minimal.state, Some(TesState::Complete));

        let basic = project_task(&record, TesView::Basic, "http://x");
        assert_eq!(basic.executors.len(), 1);
        assert_eq!(basic.executors[0].command, vec!["echo", "hi"]);
        assert_eq!(basic.logs.len(), 1);
        assert_eq!(basic.inputs.len(), 1);

        let full = project_task(&record, TesView::Full, "http://x");
        assert_eq!(full.logs.len(), 1);
        assert_eq!(full.logs[0].logs[0].exit_code, Some(0));
        assert_eq!(full.logs[0].outputs.len(), 1);
        assert_eq!(full.logs[0].outputs[0].url, "s3://ws-x/out/r.txt");
    }

    #[tokio::test]
    async fn get_resolves() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16]))).unwrap();
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();

        let response = get_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            HeaderMap::new(),
            Path(job_id.to_string()),
            Query(ViewQuery {
                view: Some("BASIC".to_string()),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // A foreign caller cannot see it.
        let foreign = get_task(
            State(state.clone()),
            Extension(auth_for(user(3))),
            HeaderMap::new(),
            Path(job_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_maps_through() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16]))).unwrap();
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();

        let ok = cancel_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(format!("{job_id}:cancel")),
        )
        .await;
        assert_eq!(ok.status(), StatusCode::OK);

        // Missing the action suffix is a bad request.
        let bad = cancel_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await;
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn service_info_shape() {
        let info = TesServiceType {
            group: "org.ga4gh",
            artifact: "tes",
            version: TES_VERSION.to_string(),
        };
        assert_eq!(info.artifact, "tes");
        assert_eq!(info.version, "1.1.0");
    }

    #[test]
    fn openapi_has_tes() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/ga4gh/tes/v1/service-info")
        );
        assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks"));
        assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks/{id}"));
    }
}
