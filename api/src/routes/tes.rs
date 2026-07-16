use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId,
    JobPayload, JobRecord, JobResultPayload, JobState, OutputDestination, OutputSelection,
    blob_group_permission_path,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::jobs::service::{
    CancelJobOutcome, cancel_owned_job, list_owned_jobs, read_owned_job, submit_execution_job,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use axum::extract::{Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

use crate::auth::require_unrestricted_realm_auth;
use crate::error::ServerError;
use crate::server_state::ServerState;

/// GA4GH TES version this facade implements.
const TES_VERSION: &str = "1.1.0";
/// Optional tag overriding the caller credential's workspace parent group.
const GROUP_TAG_KEY: &str = "aruna-engine.org/group";
/// Optional tag pinning a backend executor kind.
const EXECUTOR_TAG_KEY: &str = "aruna-engine.org/executor";
/// Optional tag carrying the submission idempotency key.
const IDEMPOTENCY_TAG_KEY: &str = "aruna-engine.org/idempotency-key";

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
    state: Option<String>,
    name_prefix: Option<String>,
}

struct TaskFilters {
    state: Option<TesState>,
    name_prefix: Option<String>,
    tags: Vec<(String, String)>,
}

impl TaskFilters {
    fn from_query(query: &ListTasksQuery, raw_query: Option<&str>) -> Result<Self, TesError> {
        Ok(Self {
            state: query.state.as_deref().map(TesState::parse).transpose()?,
            name_prefix: query
                .name_prefix
                .clone()
                .filter(|prefix| !prefix.is_empty()),
            tags: parse_tag_filters(raw_query),
        })
    }

    fn matches(&self, record: &JobRecord) -> bool {
        let JobPayload::Execution(spec) = &record.payload else {
            return false;
        };
        let tags = project_tags(spec);
        self.state.is_none_or(|state| tes_state(record) == state)
            && self.name_prefix.as_deref().is_none_or(|prefix| {
                spec.name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(prefix))
            })
            && self.tags.iter().all(|(key, value)| {
                tags.get(key)
                    .is_some_and(|stored| value.is_empty() || stored == value)
            })
    }
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

impl TesState {
    fn parse(value: &str) -> Result<Self, TesError> {
        match value {
            "UNKNOWN" => Ok(Self::Unknown),
            "QUEUED" => Ok(Self::Queued),
            "INITIALIZING" => Ok(Self::Initializing),
            "RUNNING" => Ok(Self::Running),
            "PAUSED" => Ok(Self::Paused),
            "COMPLETE" => Ok(Self::Complete),
            "EXECUTOR_ERROR" => Ok(Self::ExecutorError),
            "SYSTEM_ERROR" => Ok(Self::SystemError),
            "CANCELED" => Ok(Self::Canceled),
            "CANCELING" => Ok(Self::Canceling),
            "PREEMPTED" => Ok(Self::Preempted),
            other => Err(TesError::bad_request(format!("unknown state `{other}`"))),
        }
    }
}

fn parse_tag_filters(raw_query: Option<&str>) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (field, value) in url::form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        match field.as_ref() {
            "tag_key" => keys.push(value.into_owned()),
            "tag_value" => values.push(value.into_owned()),
            _ => {}
        }
    }
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| (key, values.get(index).cloned().unwrap_or_default()))
        .collect()
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
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn create_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Json(task): Json<TesTask>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };

    let (spec, idempotency_key) = match map_task_to_spec(&task, caller.credential_group) {
        Ok(mapped) => mapped,
        Err(error) => return error.into_response(),
    };

    // Group-write authorization at the facade boundary (the raw internal POST /jobs/
    // path does not yet check this; see #420). Rejecting here never widens that gap.
    if let Err(error) = ensure_group_write(&state, &caller.auth, spec.group_id).await {
        return error.into_response();
    }

    match submit_execution_job(
        &state.get_ctx(),
        spec,
        caller.auth.user_id,
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
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn get_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ViewQuery>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let job_id = match JobId::from_str(&id) {
        Ok(job_id) => job_id,
        Err(_) => return TesError::not_found("TES task not found").into_response(),
    };

    let record = match read_owned_job(&state.get_ctx(), caller.auth.user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return TesError::not_found("TES task not found").into_response(),
        Err(error) => return TesError::internal(error).into_response(),
    };
    // Only execution jobs are TES tasks; other job kinds are not addressable here.
    if !task_in_group(&record, caller.credential_group) {
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
        ("state" = Option<String>, Query, description = "TES task state"),
        ("name_prefix" = Option<String>, Query, description = "Task name prefix"),
        ("tag_key" = Vec<String>, Query, description = "Repeated tag keys"),
        ("tag_value" = Vec<String>, Query, description = "Repeated tag values")
    ),
    responses(
        (status = 200, body = TesListTasksResponse),
        (status = 400, body = TesErrorPayload),
        (status = 401, body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn list_tasks(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Query(query): Query<ListTasksQuery>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let filters = match TaskFilters::from_query(&query, raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let cursor = match decode_page_token(query.page_token.as_deref()) {
        Ok(cursor) => cursor,
        Err(error) => return error.into_response(),
    };
    let limit = query
        .page_size
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .min(MAX_PAGE_SIZE);

    let (records, next_cursor) = match list_owned_jobs(
        &state.get_ctx(),
        caller.auth.user_id,
        cursor,
        limit,
        |record| filters.matches(record) && task_in_group(record, caller.credential_group),
    )
    .await
    {
        Ok(page) => page,
        Err(error) => return TesError::internal(error).into_response(),
    };

    let base_url = external_base_url(&headers);
    let tasks = records
        .iter()
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
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn cancel_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
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
    let record = match read_owned_job(&state.get_ctx(), caller.auth.user_id, job_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return TesError::not_found("TES task not found").into_response(),
        Err(error) => return TesError::internal(error).into_response(),
    };
    if !task_in_group(&record, caller.credential_group) {
        return TesError::not_found("TES task not found").into_response();
    }

    match cancel_owned_job(
        &state.get_ctx(),
        &state.jobs_runtime(),
        caller.auth.user_id,
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
fn map_task_to_spec(
    task: &TesTask,
    credential_group: Option<Ulid>,
) -> Result<(ExecutionSpec, Option<String>), TesError> {
    if task.id.is_some()
        || task.state.is_some()
        || !task.logs.is_empty()
        || task.creation_time.is_some()
    {
        return Err(TesError::bad_request(
            "task id, state, logs, and creation_time are read-only",
        ));
    }
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
    if let Some(workdir) = executor.workdir.as_deref() {
        validate_path(workdir, "executor workdir", true)?;
    }
    if executor.stdin.is_some() || executor.stdout.is_some() || executor.stderr.is_some() {
        return Err(TesError::bad_request(
            "executor stdin, stdout, and stderr paths are not supported",
        ));
    }
    if !task.volumes.is_empty() {
        return Err(TesError::bad_request("task volumes are not supported"));
    }

    let group_id = match task.tags.get(GROUP_TAG_KEY) {
        Some(group) => Ulid::from_string(group).map_err(|_| {
            TesError::bad_request(format!("`{GROUP_TAG_KEY}` is not a valid group id"))
        })?,
        None => credential_group.ok_or_else(|| {
            TesError::bad_request(format!(
                "a `{GROUP_TAG_KEY}` tag is required for bearer authentication"
            ))
        })?,
    };
    if credential_group.is_some_and(|credential_group| credential_group != group_id) {
        return Err(TesError::forbidden(
            "group tag does not match the caller credential",
        ));
    }

    let mut inputs: Vec<InputSelection> = Vec::with_capacity(task.inputs.len());
    for input in &task.inputs {
        let input = map_input(input)?;
        if inputs
            .iter()
            .any(|existing| existing.container_path == input.container_path)
        {
            return Err(TesError::bad_request("duplicate input path"));
        }
        inputs.push(input);
    }
    let mut file_outputs: Vec<OutputSelection> = Vec::with_capacity(task.outputs.len());
    for output in &task.outputs {
        let output = map_output(output)?;
        if file_outputs
            .iter()
            .any(|existing| existing.container_path == output.container_path)
        {
            return Err(TesError::bad_request("duplicate output path"));
        }
        if file_outputs
            .iter()
            .any(|existing| existing.destination == output.destination)
        {
            return Err(TesError::bad_request("duplicate output destination"));
        }
        file_outputs.push(output);
    }
    if file_outputs.iter().any(|output| {
        inputs
            .iter()
            .any(|input| input.container_path.as_deref() == Some(output.container_path.as_str()))
    }) {
        return Err(TesError::bad_request("input and output paths overlap"));
    }

    let cpu_cores = task.resources.as_ref().and_then(|r| r.cpu_cores);
    if cpu_cores == Some(0) {
        return Err(TesError::bad_request("invalid cpu_cores"));
    }
    if task
        .resources
        .as_ref()
        .is_some_and(|resources| !resources.zones.is_empty())
    {
        return Err(TesError::bad_request("resource zones are not supported"));
    }

    let ram_bytes = task
        .resources
        .as_ref()
        .and_then(|r| r.ram_gb)
        .map(|gb| gb_to_bytes(gb, "ram_gb"))
        .transpose()?;
    let disk_bytes = task
        .resources
        .as_ref()
        .and_then(|r| r.disk_gb)
        .map(|gb| gb_to_bytes(gb, "disk_gb"))
        .transpose()?;
    let resources = ComputeResources {
        cpu_cores,
        ram_bytes,
        disk_bytes,
        max_walltime_ms: None,
        preemptible: task
            .resources
            .as_ref()
            .and_then(|resources| resources.preemptible)
            .unwrap_or(false),
    };

    let spec = ExecutionSpec {
        group_id,
        name: task.name.clone(),
        description: task.description.clone(),
        tags: task.tags.clone(),
        image: executor.image.clone(),
        // TES `command` is the full argv; override the image ENTRYPOINT with it and
        // leave the image CMD unset so exactly the requested argv runs.
        entrypoint: Some(executor.command.clone()),
        command: Vec::new(),
        workdir: executor.workdir.clone(),
        env: executor.env.clone(),
        resources,
        executor_constraint: task.tags.get(EXECUTOR_TAG_KEY).cloned(),
        inputs,
        file_outputs,
        output_prefixes: Vec::new(),
    };

    // Handed over as the raw idempotency key: `submit_execution_job` applies the per-user
    // `user/` namespacing itself, so TES inherits dedup scoping for free.
    let idempotency_key = task.tags.get(IDEMPOTENCY_TAG_KEY).cloned();

    Ok((spec, idempotency_key))
}

fn map_input(input: &TesInput) -> Result<InputSelection, TesError> {
    if input.kind != TesFileType::File {
        return Err(TesError::bad_request("directory inputs are not supported"));
    }
    if input.content.is_some() {
        return Err(TesError::bad_request(
            "inline input content is not supported",
        ));
    }
    let url = input
        .url
        .as_deref()
        .ok_or_else(|| TesError::bad_request("input url is required"))?;
    let (bucket, key) = parse_s3_url(url, "input")?;
    validate_path(&input.path, "input path", false)?;
    Ok(InputSelection {
        source: InputSource::S3 {
            bucket,
            key,
            version_id: None,
        },
        dest_key: input.path[1..].to_string(),
        mode: InputMode::Snapshot,
        container_path: Some(input.path.clone()),
        name: input.name.clone(),
        description: input.description.clone(),
    })
}

fn map_output(output: &TesOutput) -> Result<OutputSelection, TesError> {
    if output.kind != TesFileType::File {
        return Err(TesError::bad_request("directory outputs are not supported"));
    }
    validate_path(&output.path, "output path", false)?;
    let url = output
        .url
        .as_deref()
        .ok_or_else(|| TesError::bad_request("output url is required"))?;
    let (bucket, key) = parse_s3_url(url, "output")?;
    Ok(OutputSelection {
        container_path: output.path.clone(),
        destination: OutputDestination::S3 { bucket, key },
        name: output.name.clone(),
        description: output.description.clone(),
    })
}

fn parse_s3_url(url: &str, role: &str) -> Result<(String, String), TesError> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| TesError::bad_request(format!("only s3:// {role} urls are supported")))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| TesError::bad_request(format!("s3 {role} url must be s3://bucket/key")))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(TesError::bad_request(format!(
            "s3 {role} url must be s3://bucket/key"
        )));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn validate_path(value: &str, role: &str, allow_root: bool) -> Result<(), TesError> {
    let invalid = !value.starts_with('/')
        || value.contains('\0')
        || (!allow_root && value == "/")
        || (value != "/"
            && value
                .split('/')
                .skip(1)
                .any(|component| component.is_empty() || component == "." || component == ".."));
    if invalid {
        return Err(TesError::bad_request(format!(
            "{role} must be an absolute canonical path"
        )));
    }
    Ok(())
}

fn gb_to_bytes(gb: f64, field: &str) -> Result<u64, TesError> {
    let bytes = (gb * 1_000_000_000.0) as u64;
    if !gb.is_finite() || gb <= 0.0 || bytes == 0 || bytes > i64::MAX as u64 {
        return Err(TesError::bad_request(format!("invalid {field}")));
    }
    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Mapping: JobRecord -> TesTask / TesState
// ---------------------------------------------------------------------------

/// Map an internal job state onto its TES external state. `Failed` splits on
/// evidence: a non-zero container exit is an executor error; post-processing and
/// evidence-free failures are system errors. `Indeterminate` maps to TES `UNKNOWN`.
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
                exit_code: Some(code),
                ..
            }) if *code != 0 => TesState::ExecutorError,
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
        workdir: spec.workdir.clone(),
        env: spec.env.clone(),
        ..Default::default()
    }];

    let inputs = spec
        .inputs
        .iter()
        .filter_map(|input| {
            let container_path = input.container_path.as_ref()?;
            let InputSource::S3 { bucket, key, .. } = &input.source;
            Some(TesInput {
                name: input.name.clone(),
                description: input.description.clone(),
                url: Some(format!("s3://{bucket}/{key}")),
                path: container_path.clone(),
                kind: TesFileType::File,
                ..Default::default()
            })
        })
        .collect();

    let outputs = spec
        .file_outputs
        .iter()
        .map(|output| {
            let OutputDestination::S3 { bucket, key } = &output.destination;
            TesOutput {
                name: output.name.clone(),
                description: output.description.clone(),
                url: Some(format!("s3://{bucket}/{key}")),
                path: output.container_path.clone(),
                kind: TesFileType::File,
            }
        })
        .collect();

    let resources = Some(TesResources {
        cpu_cores: spec.resources.cpu_cores,
        ram_gb: spec
            .resources
            .ram_bytes
            .map(|bytes| bytes as f64 / 1_000_000_000.0),
        disk_gb: spec
            .resources
            .disk_bytes
            .map(|bytes| bytes as f64 / 1_000_000_000.0),
        preemptible: Some(spec.resources.preemptible),
        ..Default::default()
    });

    let tags = project_tags(spec);

    let mut log = build_task_log(record, base_url);
    if view == TesView::Basic {
        log.system_logs.clear();
        for executor in &mut log.logs {
            executor.stdout = None;
            executor.stderr = None;
        }
    }

    TesTask {
        id: Some(id),
        state: Some(state),
        name: spec.name.clone(),
        description: spec.description.clone(),
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

fn project_tags(spec: &ExecutionSpec) -> BTreeMap<String, String> {
    let mut tags = spec.tags.clone();
    tags.entry(GROUP_TAG_KEY.to_string())
        .or_insert_with(|| spec.group_id.to_string());
    if let Some(constraint) = &spec.executor_constraint {
        tags.entry(EXECUTOR_TAG_KEY.to_string())
            .or_insert_with(|| constraint.clone());
    }
    tags
}

fn build_task_log(record: &JobRecord, _base_url: &str) -> TesTaskLog {
    let start_time = record.started_at_ms.map(rfc3339);
    let mut executor_log = TesExecutorLog {
        start_time: start_time.clone(),
        end_time: record.finished_at_ms.map(rfc3339),
        ..Default::default()
    };
    let mut outputs = Vec::new();
    if let Some(JobResultPayload::Execution {
        exit_code,
        outputs: captured,
        stdout,
        stderr,
        ..
    }) = &record.result
    {
        executor_log.exit_code = *exit_code;
        executor_log.stdout = (!stdout.is_empty()).then(|| stdout.clone());
        executor_log.stderr = (!stderr.is_empty()).then(|| stderr.clone());
        outputs = captured
            .iter()
            .map(|output| TesOutputFileLog {
                url: format!("s3://{}/{}", output.bucket, output.key),
                path: if output.container_path.is_empty() {
                    output.key.clone()
                } else {
                    output.container_path.clone()
                },
                size_bytes: output.size.to_string(),
            })
            .collect();
    }
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

#[derive(Debug)]
struct TesCaller {
    auth: AuthContext,
    credential_group: Option<Ulid>,
}

async fn authenticate_tes(
    state: &ServerState,
    auth: Option<AuthContext>,
    headers: &HeaderMap,
) -> Result<TesCaller, TesError> {
    if let Some(auth) = auth {
        let auth =
            require_unrestricted_realm_auth(state, Some(auth)).map_err(TesError::from_server)?;
        return Ok(TesCaller {
            auth,
            credential_group: None,
        });
    }

    let (access_key, provided_secret) = parse_basic(headers)?;
    let access = match drive(
        GetUserAccessOperation::new(access_key.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(access))) => access,
        Ok(None)
        | Ok(Some(Err(GetUserAccessError::NotFound)))
        | Err(GetUserAccessError::NotFound) => return Err(TesError::unauthorized()),
        Ok(Some(Err(error))) | Err(error) => return Err(TesError::internal(error.to_string())),
    };
    let secret_matches =
        blake3::hash(provided_secret.as_slice()) == blake3::hash(access.secret.as_bytes());
    if access.access_key != access_key
        || access.user_identity.realm_id != state.get_realm_id()
        || access.issued_by != *state.get_node_id().as_bytes()
        || access.is_revoked()
        || access.is_expired(SystemTime::now())
        || !secret_matches
    {
        return Err(TesError::unauthorized());
    }

    let credential_group = access.group_id;
    let auth = require_unrestricted_realm_auth(
        state,
        Some(AuthContext {
            user_id: access.user_identity,
            realm_id: access.user_identity.realm_id,
            path_restrictions: access.path_restrictions,
        }),
    )
    .map_err(TesError::from_server)?;
    Ok(TesCaller {
        auth,
        credential_group: Some(credential_group),
    })
}

fn parse_basic(headers: &HeaderMap) -> Result<(String, Vec<u8>), TesError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(TesError::unauthorized)?;
    let mut parts = value.split_ascii_whitespace();
    let (Some(scheme), Some(encoded), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(TesError::unauthorized());
    };
    if !scheme.eq_ignore_ascii_case("Basic") {
        return Err(TesError::unauthorized());
    }
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| TesError::unauthorized())?;
    // Aruna access keys contain a colon, while generated access secrets do not.
    let Some(separator) = decoded.iter().rposition(|byte| *byte == b':') else {
        return Err(TesError::unauthorized());
    };
    let access_key = std::str::from_utf8(&decoded[..separator])
        .map_err(|_| TesError::unauthorized())?
        .to_string();
    let secret = decoded[separator + 1..].to_vec();
    if access_key.is_empty() || secret.is_empty() {
        return Err(TesError::unauthorized());
    }
    Ok((access_key, secret))
}

fn task_in_group(record: &JobRecord, credential_group: Option<Ulid>) -> bool {
    let JobPayload::Execution(spec) = &record.payload else {
        return false;
    };
    credential_group.is_none_or(|credential_group| credential_group == spec.group_id)
}

async fn ensure_group_write(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> Result<(), TesError> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
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
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "unauthorized".to_string(),
        }
    }

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
            ServerError::Unauthorized => Self::unauthorized(),
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
    use std::time::Duration;

    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, USER_ACCESS_KEYSPACE};
    use aruna_core::structs::{
        Actor, GroupAuthorizationDocument, JobError, NodeCapabilities, OutputObject,
        RealmAuthorizationDocument, RealmId, UserAccess,
    };
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

    fn credential(group_id: Ulid) -> UserAccess {
        let user_identity = user(2);
        UserAccess {
            access_key: UserAccess::build_access_key(&user_identity, "tes").unwrap(),
            user_identity,
            group_id,
            secret: "tes-secret".to_string(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: *node_id().as_bytes(),
            revoked_at: None,
        }
    }

    fn basic_headers(access: &UserAccess, secret: &str) -> HeaderMap {
        let encoded = STANDARD.encode(format!("{}:{secret}", access.access_key));
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Basic {encoded}").parse().unwrap());
        headers
    }

    async fn write_credential(state: &ServerState, access: &UserAccess) {
        let _ = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    async fn write_auth(state: &ServerState, group_id: Ulid, owner: UserId) {
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: owner,
            realm_id: realm(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm());
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm(), group_id);
        for (key, value) in [
            (
                realm().as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
        ] {
            let _ = state
                .get_ctx()
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: AUTH_KEYSPACE.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
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
            name: Some("align reads".to_string()),
            description: Some("sample task".to_string()),
            executors: vec![TesExecutor {
                image: "alpine:3".to_string(),
                command: vec!["echo".to_string(), "hi".to_string()],
                workdir: Some("/work".to_string()),
                env: BTreeMap::from([("K".to_string(), "V".to_string())]),
                ..Default::default()
            }],
            inputs: vec![TesInput {
                name: Some("reads".to_string()),
                description: Some("input reads".to_string()),
                url: Some("s3://src/data.csv".to_string()),
                path: "/in/data.csv".to_string(),
                kind: TesFileType::File,
                ..Default::default()
            }],
            outputs: vec![TesOutput {
                name: Some("report".to_string()),
                description: Some("output report".to_string()),
                url: Some("s3://dest/out/report.txt".to_string()),
                path: "/out/report.txt".to_string(),
                ..Default::default()
            }],
            resources: Some(TesResources {
                cpu_cores: Some(2),
                ram_gb: Some(4.0),
                disk_gb: Some(8.0),
                preemptible: Some(true),
                ..Default::default()
            }),
            tags: BTreeMap::from([
                (GROUP_TAG_KEY.to_string(), group.to_string()),
                ("project".to_string(), "alpha".to_string()),
            ]),
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
        let (spec, dedup) = map_task_to_spec(&sample_task(group), None).unwrap();
        assert_eq!(spec.group_id, group);
        assert_eq!(spec.name.as_deref(), Some("align reads"));
        assert_eq!(spec.description.as_deref(), Some("sample task"));
        assert_eq!(spec.tags.get("project").map(String::as_str), Some("alpha"));
        assert_eq!(spec.image, "alpine:3");
        // TES command becomes the entrypoint override; image CMD stays empty.
        assert_eq!(spec.entrypoint, Some(vec!["echo".into(), "hi".into()]));
        assert!(spec.command.is_empty());
        assert_eq!(spec.workdir.as_deref(), Some("/work"));
        assert_eq!(spec.env.get("K").map(String::as_str), Some("V"));
        assert_eq!(spec.resources.cpu_cores, Some(2));
        assert_eq!(spec.resources.ram_bytes, Some(4_000_000_000));
        assert_eq!(spec.resources.disk_bytes, Some(8_000_000_000));
        assert!(spec.resources.preemptible);
        assert_eq!(spec.inputs.len(), 1);
        assert_eq!(spec.inputs[0].dest_key, "in/data.csv");
        assert_eq!(
            spec.inputs[0].container_path.as_deref(),
            Some("/in/data.csv")
        );
        assert_eq!(spec.inputs[0].name.as_deref(), Some("reads"));
        assert_eq!(spec.inputs[0].description.as_deref(), Some("input reads"));
        assert_eq!(spec.file_outputs.len(), 1);
        assert_eq!(spec.file_outputs[0].container_path, "/out/report.txt");
        assert_eq!(spec.file_outputs[0].name.as_deref(), Some("report"));
        assert_eq!(
            spec.file_outputs[0].description.as_deref(),
            Some("output report")
        );
        assert_eq!(
            spec.file_outputs[0].destination,
            OutputDestination::S3 {
                bucket: "dest".to_string(),
                key: "out/report.txt".to_string(),
            }
        );
        assert!(spec.output_prefixes.is_empty());
        assert!(dedup.is_none());
    }

    #[test]
    fn filters_tasks() {
        let group = Ulid::from_bytes([5u8; 16]);
        let (spec, _) = map_task_to_spec(&sample_task(group), None).unwrap();
        let mut record = execution_record(JobId::from_bytes([6u8; 16]), user(2), spec);
        record.state = JobState::Running;
        let uri: axum::http::Uri = "/ga4gh/tes/v1/tasks?state=RUNNING&name_prefix=align&tag_key=project&tag_key=aruna-engine.org%2Fgroup&tag_value=alpha"
            .parse()
            .unwrap();
        let Query(query) = Query::<ListTasksQuery>::try_from_uri(&uri).unwrap();
        let filters = TaskFilters::from_query(&query, uri.query()).unwrap();
        assert!(filters.matches(&record));

        let wrong_name = ListTasksQuery {
            name_prefix: Some("other".to_string()),
            ..Default::default()
        };
        assert!(
            !TaskFilters::from_query(&wrong_name, None)
                .unwrap()
                .matches(&record)
        );
        assert!(
            !TaskFilters::from_query(
                &ListTasksQuery::default(),
                Some("tag_key=project&tag_value=beta"),
            )
            .unwrap()
            .matches(&record)
        );
        assert!(
            !TaskFilters::from_query(&ListTasksQuery::default(), Some("tag_key=missing"),)
                .unwrap()
                .matches(&record)
        );
        assert!(
            TaskFilters::from_query(
                &ListTasksQuery {
                    state: Some("INVALID".to_string()),
                    ..Default::default()
                },
                None,
            )
            .is_err()
        );

        let probe = JobRecord::new(
            JobId::from_bytes([7u8; 16]),
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            user(2),
            node_id(),
            1_000,
            1_000,
            None,
        );
        assert!(
            !TaskFilters::from_query(&ListTasksQuery::default(), None)
                .unwrap()
                .matches(&probe)
        );
    }

    #[test]
    fn rejects_duplicate_inputs() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        let mut input = task.inputs[0].clone();
        input.url = Some("s3://src/other.csv".to_string());
        task.inputs.push(input);
        assert_eq!(
            map_task_to_spec(&task, None).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_invalid_size() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        for size_gb in [-1.0, 0.0, f64::NAN, 1e-10, f64::MAX] {
            task.resources.as_mut().unwrap().ram_gb = Some(size_gb);
            assert_eq!(
                map_task_to_spec(&task, None).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
            task.resources.as_mut().unwrap().ram_gb = Some(4.0);
            task.resources.as_mut().unwrap().disk_gb = Some(size_gb);
            assert_eq!(
                map_task_to_spec(&task, None).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
            task.resources.as_mut().unwrap().disk_gb = Some(8.0);
        }
    }

    #[test]
    fn rejects_multi_executor() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors.push(task.executors[0].clone());
        let error = map_task_to_spec(&task, None).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("single executor"));
    }

    #[test]
    fn rejects_missing_group() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.tags.clear();
        let error = map_task_to_spec(&task, None).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains(GROUP_TAG_KEY));
    }

    #[test]
    fn defaults_group() {
        let group = Ulid::from_bytes([5u8; 16]);
        let mut task = sample_task(group);
        task.tags.remove(GROUP_TAG_KEY);
        let (spec, _) = map_task_to_spec(&task, Some(group)).unwrap();
        assert_eq!(spec.group_id, group);
    }

    #[test]
    fn rejects_group_override() {
        let group = Ulid::from_bytes([5u8; 16]);
        let credential_group = Ulid::from_bytes([6u8; 16]);
        let error = map_task_to_spec(&sample_task(group), Some(credential_group)).unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn rejects_invalid_paths() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors[0].workdir = Some("work".to_string());
        assert_eq!(
            map_task_to_spec(&task, None).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
        task.executors[0].workdir = Some("/work".to_string());
        task.inputs[0].path = "/in/../data.csv".to_string();
        assert_eq!(
            map_task_to_spec(&task, None).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
        task.inputs[0].path = "/in/data.csv".to_string();
        task.outputs[0].path = "/out//report.txt".to_string();
        assert_eq!(
            map_task_to_spec(&task, None).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_unsupported_fields() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.id = Some("server-owned".to_string());
        assert!(map_task_to_spec(&task, None).is_err());
        task.id = None;
        task.inputs[0].kind = TesFileType::Directory;
        assert!(map_task_to_spec(&task, None).is_err());
        task.inputs[0].kind = TesFileType::File;
        task.outputs[0].kind = TesFileType::Directory;
        assert!(map_task_to_spec(&task, None).is_err());
        task.outputs[0].kind = TesFileType::File;
        task.executors[0].stdout = Some("/logs/out".to_string());
        assert!(map_task_to_spec(&task, None).is_err());
        task.executors[0].stdout = None;
        task.volumes.push("/data".to_string());
        assert!(map_task_to_spec(&task, None).is_err());
        task.volumes.clear();
        task.resources
            .as_mut()
            .unwrap()
            .zones
            .push("zone-a".to_string());
        assert!(map_task_to_spec(&task, None).is_err());
    }

    #[test]
    fn rejects_duplicate_outputs() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        let mut output = task.outputs[0].clone();
        output.url = Some("s3://dest/out/other.txt".to_string());
        task.outputs.push(output);
        assert!(map_task_to_spec(&task, None).is_err());

        task.outputs[1].path = "/out/other.txt".to_string();
        task.outputs[1].url = task.outputs[0].url.clone();
        assert!(map_task_to_spec(&task, None).is_err());

        task.outputs.truncate(1);
        task.outputs[0].path = task.inputs[0].path.clone();
        assert!(map_task_to_spec(&task, None).is_err());
    }

    #[test]
    fn maps_states() {
        let spec = || ExecutionSpec {
            group_id: Ulid::from_bytes([5u8; 16]),
            name: None,
            description: None,
            tags: BTreeMap::new(),
            image: "img".to_string(),
            entrypoint: None,
            command: vec!["run".to_string()],
            workdir: None,
            env: BTreeMap::new(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
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
            stdout: String::new(),
            stderr: String::new(),
        });
        assert_eq!(tes_state(&record), TesState::ExecutorError);
        if let Some(JobResultPayload::Execution { exit_code, .. }) = &mut record.result {
            *exit_code = Some(0);
        }
        assert_eq!(tes_state(&record), TesState::SystemError);
    }

    #[test]
    fn view_projections() {
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None).unwrap();
        let mut record = execution_record(JobId::from_bytes([2u8; 16]), user(2), spec);
        let queued = project_task(&record, TesView::Full, "http://x");
        assert!(queued.logs[0].start_time.is_none());
        assert!(queued.logs[0].logs[0].start_time.is_none());
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record.workspace_bucket = Some("ws-x".to_string());
        let JobPayload::Execution(spec) = &mut record.payload else {
            unreachable!();
        };
        spec.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "native".to_string(),
                key: "workspace-only".to_string(),
                version_id: None,
            },
            dest_key: "native/input".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        });
        spec.output_prefixes.push("native/".to_string());
        record.last_error = Some(JobError::permanent("prior failure"));
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: "ws-x".to_string(),
            outputs: vec![OutputObject {
                bucket: "dest".to_string(),
                key: "out/r.txt".to_string(),
                container_path: "/out/report.txt".to_string(),
                size: 12,
                digest: None,
            }],
            stdout: "hello".to_string(),
            stderr: "error".to_string(),
        });

        let minimal = project_task(&record, TesView::Minimal, "http://x");
        assert!(minimal.executors.is_empty());
        assert!(minimal.logs.is_empty());
        assert_eq!(minimal.state, Some(TesState::Complete));

        let basic = project_task(&record, TesView::Basic, "http://x");
        assert_eq!(basic.name.as_deref(), Some("align reads"));
        assert_eq!(basic.description.as_deref(), Some("sample task"));
        assert_eq!(basic.tags.get("project").map(String::as_str), Some("alpha"));
        assert_eq!(basic.executors.len(), 1);
        assert_eq!(basic.executors[0].command, vec!["echo", "hi"]);
        assert_eq!(basic.executors[0].workdir.as_deref(), Some("/work"));
        assert_eq!(basic.logs.len(), 1);
        assert!(basic.logs[0].system_logs.is_empty());
        assert!(basic.logs[0].logs[0].stdout.is_none());
        assert!(basic.logs[0].logs[0].stderr.is_none());
        assert_eq!(basic.inputs.len(), 1);
        assert_eq!(basic.inputs[0].path, "/in/data.csv");
        assert_eq!(basic.inputs[0].name.as_deref(), Some("reads"));
        assert_eq!(basic.outputs.len(), 1);
        assert_eq!(basic.outputs[0].path, "/out/report.txt");
        assert_eq!(
            basic.outputs[0].url.as_deref(),
            Some("s3://dest/out/report.txt")
        );
        assert_eq!(basic.resources.as_ref().unwrap().disk_gb, Some(8.0));
        assert_eq!(basic.resources.as_ref().unwrap().preemptible, Some(true));

        let full = project_task(&record, TesView::Full, "http://x");
        assert_eq!(full.logs.len(), 1);
        assert_eq!(full.logs[0].logs[0].exit_code, Some(0));
        assert_eq!(full.logs[0].logs[0].stdout.as_deref(), Some("hello"));
        assert_eq!(full.logs[0].logs[0].stderr.as_deref(), Some("error"));
        assert_eq!(full.logs[0].system_logs, vec!["prior failure"]);
        assert_eq!(full.logs[0].outputs.len(), 1);
        assert_eq!(full.logs[0].outputs[0].url, "s3://dest/out/r.txt");
        assert_eq!(full.logs[0].outputs[0].path, "/out/report.txt");
    }

    #[tokio::test]
    async fn rejects_basic() {
        let (_dir, state) = build_state().await;
        let group = Ulid::from_bytes([5u8; 16]);
        let access = credential(group);
        let mut revoked = access.clone();
        revoked.revoked_at = Some(SystemTime::now());
        let mut expired = access.clone();
        expired.expiry = SystemTime::UNIX_EPOCH;
        let mut foreign_issuer = access.clone();
        foreign_issuer.issued_by = [0u8; 32];

        for (access, secret) in [
            (access, "wrong-secret"),
            (revoked, "tes-secret"),
            (expired, "tes-secret"),
            (foreign_issuer, "tes-secret"),
        ] {
            write_credential(&state, &access).await;
            let error = authenticate_tes(&state, None, &basic_headers(&access, secret))
                .await
                .unwrap_err();
            assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn rejects_restricted_basic() {
        let (_dir, state) = build_state().await;
        let mut access = credential(Ulid::from_bytes([5u8; 16]));
        access.path_restrictions = Some(Vec::new());
        write_credential(&state, &access).await;

        let error = authenticate_tes(
            &state,
            None,
            &basic_headers(&access, access.secret.as_str()),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn creates_tagless_basic() {
        let (_dir, state) = build_state().await;
        let group = Ulid::from_bytes([5u8; 16]);
        let access = credential(group);
        write_credential(&state, &access).await;
        write_auth(&state, group, access.user_identity).await;
        let mut task = sample_task(group);
        task.tags.remove(GROUP_TAG_KEY);

        let response = create_task(
            State(state.clone()),
            Extension(None),
            basic_headers(&access, access.secret.as_str()),
            Json(task),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let created: TesCreateTaskResponse = serde_json::from_slice(&body).unwrap();
        let record = read_owned_job(
            &state.get_ctx(),
            access.user_identity,
            JobId::from_str(&created.id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let JobPayload::Execution(spec) = record.payload else {
            panic!("TES created a non-execution job");
        };
        assert_eq!(spec.group_id, group);
    }

    #[tokio::test]
    async fn basic_scopes_tasks() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let group = Ulid::from_bytes([5u8; 16]);
        let sibling = Ulid::from_bytes([6u8; 16]);
        let access = credential(group);
        write_credential(&state, &access).await;
        let headers = basic_headers(&access, access.secret.as_str());
        let caller = authenticate_tes(&state, None, &headers).await.unwrap();
        assert_eq!(caller.auth.user_id, owner);
        assert_eq!(caller.credential_group, Some(group));

        let visible_id = JobId::from_bytes([9u8; 16]);
        let hidden_id = JobId::from_bytes([10u8; 16]);
        for (job_id, group_id) in [(visible_id, group), (hidden_id, sibling)] {
            let (spec, _) = map_task_to_spec(&sample_task(group_id), None).unwrap();
            insert_job(
                &state.get_ctx().storage_handle,
                &execution_record(job_id, owner, spec),
            )
            .await
            .unwrap();
        }

        let visible = get_task(
            State(state.clone()),
            Extension(None),
            headers.clone(),
            Path(visible_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);
        let hidden = get_task(
            State(state.clone()),
            Extension(None),
            headers.clone(),
            Path(hidden_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

        let hidden_cancel = cancel_task(
            State(state.clone()),
            Extension(None),
            headers.clone(),
            Path(format!("{hidden_id}:cancel")),
        )
        .await;
        assert_eq!(hidden_cancel.status(), StatusCode::NOT_FOUND);
        let visible_cancel = cancel_task(
            State(state.clone()),
            Extension(None),
            headers.clone(),
            Path(format!("{visible_id}:cancel")),
        )
        .await;
        assert_eq!(visible_cancel.status(), StatusCode::OK);

        let listed = list_tasks(
            State(state),
            Extension(None),
            headers,
            RawQuery(None),
            Query(ListTasksQuery::default()),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.tasks[0].id, Some(visible_id.to_string()));
    }

    #[tokio::test]
    async fn get_resolves() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None).unwrap();
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
        let (spec, _) = map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None).unwrap();
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
            HeaderMap::new(),
            Path(format!("{job_id}:cancel")),
        )
        .await;
        assert_eq!(ok.status(), StatusCode::OK);

        // Missing the action suffix is a bad request.
        let bad = cancel_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            HeaderMap::new(),
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
        assert!(
            openapi
                .components
                .is_some_and(|components| components.security_schemes.contains_key("basic_auth"))
        );
    }
}
