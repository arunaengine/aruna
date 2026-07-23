use std::collections::BTreeMap;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::compute::normalize_container_path;
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, ExportReportRow, ImportReportRow, InputMode,
    InputSelection, InputSource, JobId, JobPayload, JobRecord, JobState, Permission, WorkspaceMode,
    WorkspaceOutput, blob_bucket_permission_path, blob_group_permission_path,
};
use aruna_operations::jobs::service::{
    ArtifactLookup, CancelJobOutcome, JobReportLookup, OwnedArtifact, cancel_owned_job,
    list_owned_jobs, read_artifact_range, read_job_run_crate_status, read_owned_artifact,
    read_owned_job, read_owned_report, submit_execution_job,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_object::ObjectRangeRequest;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, RANGE,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_operations::driver::drive;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;
const DEFAULT_REPORT_LIMIT: usize = 200;
const MAX_REPORT_LIMIT: usize = 1000;
const MAX_OUTPUT_PREFIXES: usize = 32;
/// Bounds the quadratic duplicate-input validation.
const MAX_INPUTS: usize = 512;

#[derive(OpenApi)]
#[openapi(
    tags((name = "jobs", description = "Durable background jobs")),
    paths(
        list_jobs,
        get_job,
        cancel_job,
        submit_job,
        get_job_report,
        get_job_artifact,
        head_job_artifact
    )
)]
pub struct JobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/jobs/", get(list_jobs).post(submit_job))
        .route("/jobs/{job_id}", get(get_job))
        .route("/jobs/{job_id}/cancel", post(cancel_job))
        .route("/jobs/{job_id}/report", get(get_job_report))
        .route(
            "/jobs/{job_id}/artifacts/rocrate",
            get(get_job_artifact).head(head_job_artifact),
        )
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecutionInputRequest {
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub dest_key: String,
    /// Absolute container path; defaults to `/inputs/<dest_key>`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecutionOutputRequest {
    /// Absolute container path captured after the task exits.
    pub container_path: String,
    /// Destination key inside the workspace bucket.
    pub dest_key: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceModeRequest {
    Temporary,
    Kept,
    Existing,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkspaceRequest {
    pub mode: WorkspaceModeRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitExecutionRequest {
    pub group_id: String,
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_walltime_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_constraint: Option<String>,
    #[serde(default)]
    pub inputs: Vec<ExecutionInputRequest>,
    #[serde(default)]
    pub outputs: Vec<ExecutionOutputRequest>,
    #[serde(default)]
    pub output_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitJobResponse {
    pub job_id: String,
    pub created: bool,
    pub owner_node_url: String,
    pub status_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobUrls {
    pub owner_node_url: String,
    pub status_url: String,
    pub report_url: String,
    pub artifact_url: String,
}

pub async fn job_urls(state: &ServerState, job_id: JobId) -> ServerResult<JobUrls> {
    let interface = state.interface_state().await;
    let rest = interface.rest.ok_or_else(|| {
        ServerError::InternalError("REST interface public URL is unavailable".to_string())
    })?;
    let api_base_url = rest.api_base_url.trim_end_matches('/');
    Ok(JobUrls {
        owner_node_url: rest.api_base_url.clone(),
        status_url: format!("{api_base_url}/jobs/{job_id}"),
        report_url: format!("{api_base_url}/jobs/{job_id}/report"),
        artifact_url: format!("{api_base_url}/jobs/{job_id}/artifacts/rocrate"),
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListJobsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ReportQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobReportResponse {
    pub rows: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub report_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReportPendingResponse {
    pub code: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReportCursor {
    job_id: JobId,
    report_digest: [u8; 32],
    last_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobProgressResponse {
    pub current: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    pub unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobErrorResponse {
    pub message: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobStatusResponse {
    pub job_id: String,
    pub kind: String,
    pub state: String,
    pub attempts: u32,
    pub cancel_requested: bool,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub progress: JobProgressResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JobErrorResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_bucket: Option<String>,
    pub workspace_mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_crate: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobListResponse {
    pub jobs: Vec<JobStatusResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

fn job_status_response(record: &JobRecord) -> JobStatusResponse {
    JobStatusResponse {
        job_id: record.job_id.to_string(),
        kind: record.payload.kind().to_string(),
        state: record.state.name().to_string(),
        attempts: record.attempts,
        cancel_requested: record.cancel_requested,
        created_at: rfc3339(record.created_at_ms),
        updated_at: rfc3339(record.updated_at_ms),
        finished_at: record.finished_at_ms.map(rfc3339),
        progress: JobProgressResponse {
            current: record.progress.current,
            total: record.progress.total,
            unit: record.progress.unit.clone(),
        },
        error: record.last_error.as_ref().map(|error| JobErrorResponse {
            message: error.message.clone(),
            kind: error.kind.name().to_string(),
        }),
        result: record.result.as_ref().map(|result| result.to_public_json()),
        workspace_bucket: record.workspace_bucket.clone(),
        workspace_mode: record.workspace_mode.name().to_string(),
        run_crate: None,
    }
}

fn parse_state(value: &str) -> ServerResult<JobState> {
    match value {
        "queued" => Ok(JobState::Queued),
        "claimed" => Ok(JobState::Claimed),
        "preparing" => Ok(JobState::Preparing),
        "ready" => Ok(JobState::Ready),
        "running" => Ok(JobState::Running),
        "cancelling" => Ok(JobState::Cancelling),
        "indeterminate" => Ok(JobState::Indeterminate),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancelled" => Ok(JobState::Cancelled),
        _ => Err(ServerError::BadRequest),
    }
}

fn decode_cursor(cursor: Option<&str>) -> ServerResult<Option<Vec<u8>>> {
    match cursor {
        Some(cursor) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)?;
            if bytes.len() != 24 {
                return Err(ServerError::BadRequest);
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

fn encode_cursor(cursor: Option<Vec<u8>>) -> Option<String> {
    cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor))
}

fn decode_report_cursor(cursor: Option<&str>) -> ServerResult<Option<ReportCursor>> {
    cursor
        .map(|cursor| {
            URL_SAFE_NO_PAD
                .decode(cursor)
                .map_err(|_| ServerError::BadRequest)
                .and_then(|bytes| postcard::from_bytes(&bytes).map_err(|_| ServerError::BadRequest))
        })
        .transpose()
}

fn encode_report_cursor(
    job_id: JobId,
    report_digest: [u8; 32],
    last_key: Option<Vec<u8>>,
) -> ServerResult<Option<String>> {
    last_key
        .map(|last_key| {
            postcard::to_allocvec(&ReportCursor {
                job_id,
                report_digest,
                last_key,
            })
            .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
            .map_err(|error| ServerError::InternalError(error.to_string()))
        })
        .transpose()
}

fn parse_job_id(raw: &str) -> ServerResult<JobId> {
    JobId::from_str(raw).map_err(|_| ServerError::NotFound)
}

pub(crate) fn map_submit_error(
    error: aruna_operations::jobs::submit::SubmitJobError,
) -> ServerError {
    use aruna_operations::jobs::submit::SubmitJobError;
    match error {
        SubmitJobError::JobPlanConflict { existing_job_id } => ServerError::Conflict(format!(
            "idempotency key already bound to job {existing_job_id}"
        )),
        SubmitJobError::ActiveJobLimit { limit } => {
            ServerError::Conflict(format!("active RO-Crate job limit of {limit} reached"))
        }
        SubmitJobError::InvalidWorkspace(_) => ServerError::BadRequest,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn workspace_request(
    workspace: Option<WorkspaceRequest>,
) -> ServerResult<(WorkspaceMode, Option<String>)> {
    let Some(workspace) = workspace else {
        return Ok((WorkspaceMode::Kept, None));
    };
    match (workspace.mode, workspace.bucket) {
        (WorkspaceModeRequest::Temporary, None) => Ok((WorkspaceMode::Temporary, None)),
        (WorkspaceModeRequest::Kept, None) => Ok((WorkspaceMode::Kept, None)),
        (WorkspaceModeRequest::Existing, Some(bucket)) if !bucket.trim().is_empty() => {
            Ok((WorkspaceMode::Existing, Some(bucket)))
        }
        _ => Err(ServerError::BadRequest),
    }
}

async fn validate_existing_workspace(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    bucket: &str,
) -> ServerResult<()> {
    let info = match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(info)) => info,
        Ok(None) | Err(GetBucketInfoError::NotFound) => return Err(ServerError::BadRequest),
        Err(error) => return Err(ServerError::InternalError(error.to_string())),
    };
    if info.group_id != group_id {
        return Err(ServerError::BadRequest);
    }
    ensure_permission(
        state,
        auth,
        blob_bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
        Permission::WRITE,
    )
    .await
}

/// Canonical absolute container path or 400.
fn container_path(path: &str) -> ServerResult<String> {
    let normalized = normalize_container_path(path).map_err(|_| ServerError::BadRequest)?;
    normalized
        .to_str()
        .map(str::to_string)
        .ok_or(ServerError::BadRequest)
}

/// Native inputs land in the container at the given path, defaulting to
/// `/inputs/<dest_key>` so `load_inputs` always stages them.
fn native_input(input: ExecutionInputRequest) -> ServerResult<InputSelection> {
    if input.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
    }
    let path = match &input.container_path {
        Some(path) => container_path(path)?,
        None => container_path(&format!("/inputs/{}", input.dest_key))?,
    };
    Ok(InputSelection {
        source: InputSource::S3 {
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
        },
        dest_key: input.dest_key,
        mode: InputMode::Snapshot,
        container_path: Some(path),
        name: None,
        description: None,
    })
}

fn native_output(output: ExecutionOutputRequest) -> ServerResult<WorkspaceOutput> {
    if output.dest_key.is_empty() {
        return Err(ServerError::BadRequest);
    }
    Ok(WorkspaceOutput {
        container_path: container_path(&output.container_path)?,
        dest_key: output.dest_key,
    })
}

fn validate_output_prefixes(prefixes: Vec<String>) -> ServerResult<Vec<String>> {
    if prefixes.len() > MAX_OUTPUT_PREFIXES || prefixes.iter().any(String::is_empty) {
        return Err(ServerError::BadRequest);
    }
    let mut deduplicated = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !deduplicated.contains(&prefix) {
            deduplicated.push(prefix);
        }
    }
    Ok(deduplicated)
}

#[utoipa::path(
    get,
    path = "/jobs/",
    tag = "jobs",
    params(
        ("limit" = Option<usize>, Query, description = "Max jobs to return (default 50, max 200)"),
        ("cursor" = Option<String>, Query, description = "Opaque pagination cursor from a previous page"),
        ("state" = Option<String>, Query, description = "Optional state filter")
    ),
    responses(
        (status = 200, description = "Jobs page", body = JobListResponse),
        (status = 400, description = "Invalid cursor or state", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_jobs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListJobsQuery>,
) -> ServerResult<(StatusCode, Json<JobListResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(MAX_LIST_LIMIT);
    let state_filter = query.state.as_deref().map(parse_state).transpose()?;

    let (records, next_cursor) = list_owned_jobs(
        &state.get_ctx(),
        auth.user_id,
        cursor,
        limit,
        move |record| state_filter.is_none_or(|state| record.state == state),
    )
    .await
    .map_err(ServerError::InternalError)?;

    let jobs = records.iter().map(job_status_response).collect();
    Ok((
        StatusCode::OK,
        Json(JobListResponse {
            jobs,
            next_cursor: encode_cursor(next_cursor),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/jobs/",
    tag = "jobs",
    request_body = SubmitExecutionRequest,
    responses(
        (status = 201, description = "Execution job created", body = SubmitJobResponse),
        (status = 200, description = "Idempotent match of an existing job", body = SubmitJobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 409, description = "Idempotency key bound to a different plan", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SubmitExecutionRequest>,
) -> ServerResult<(StatusCode, Json<SubmitJobResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let group_id = Ulid::from_string(&request.group_id).map_err(|_| ServerError::BadRequest)?;
    let (workspace_mode, workspace_bucket) = workspace_request(request.workspace)?;
    if request.image.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }
    // RAM above i64::MAX would wrap negative in the Docker HostConfig cast.
    if request.cpu_cores == Some(0)
        || request
            .ram_bytes
            .is_some_and(|bytes| bytes == 0 || i64::try_from(bytes).is_err())
    {
        return Err(ServerError::BadRequest);
    }
    let output_prefixes = validate_output_prefixes(request.output_prefixes)?;
    ensure_permission(
        &state,
        &auth,
        blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        Permission::WRITE,
    )
    .await?;
    if let Some(bucket) = workspace_bucket.as_deref() {
        validate_existing_workspace(&state, &auth, group_id, bucket).await?;
    }

    if request.inputs.len() > MAX_INPUTS || request.outputs.len() > MAX_INPUTS {
        return Err(ServerError::BadRequest);
    }
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        let input = native_input(input)?;
        if inputs.iter().any(|existing| {
            existing.dest_key == input.dest_key || existing.container_path == input.container_path
        }) {
            return Err(ServerError::BadRequest);
        }
        inputs.push(input);
    }
    let mut workspace_outputs: Vec<WorkspaceOutput> = Vec::with_capacity(request.outputs.len());
    for output in request.outputs {
        let output = native_output(output)?;
        if workspace_outputs.iter().any(|existing| {
            existing.dest_key == output.dest_key || existing.container_path == output.container_path
        }) {
            return Err(ServerError::BadRequest);
        }
        workspace_outputs.push(output);
    }

    let spec = ExecutionSpec {
        group_id,
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: request.image,
        entrypoint: request.entrypoint,
        command: request.command,
        workdir: None,
        env: request.env,
        resources: ComputeResources {
            cpu_cores: request.cpu_cores,
            ram_bytes: request.ram_bytes,
            disk_bytes: None,
            max_walltime_ms: request.max_walltime_ms,
            preemptible: false,
        },
        executor_constraint: request.executor_constraint,
        inputs,
        file_outputs: Vec::new(),
        workspace_outputs,
        output_prefixes,
    };
    let result = submit_execution_job(
        &state.get_ctx(),
        spec,
        auth.user_id,
        state.get_node_id(),
        request.idempotency_key,
        workspace_mode,
        workspace_bucket,
        state.rocrate_limits().artifact_retention_ms,
    )
    .await
    .map_err(map_submit_error)?;

    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let urls = job_urls(&state, result.job_id).await?;
    Ok((
        status,
        Json(SubmitJobResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            owner_node_url: urls.owner_node_url,
            status_url: urls.status_url,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}",
    tag = "jobs",
    params(("job_id" = String, Path, description = "Job identifier")),
    responses(
        (status = 200, description = "Job status", body = JobStatusResponse),
        (status = 404, description = "Job not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let record = read_owned_job(&state.get_ctx(), auth.user_id, job_id)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)?;
    let mut response = job_status_response(&record);
    response.run_crate = read_job_run_crate_status(&state.get_ctx(), job_id)
        .await
        .map_err(ServerError::InternalError)?
        .map(|status| status.to_public_json());
    Ok((StatusCode::OK, Json(response)))
}

fn coded_response(status: StatusCode, error: &str, code: &str) -> Response {
    (
        status,
        Json(ErrorResponse::new(error).with_code(code.to_string())),
    )
        .into_response()
}

fn decode_report_row(
    payload: &JobPayload,
    entry_key: &[u8],
    value: &[u8],
) -> ServerResult<serde_json::Value> {
    let row = match payload {
        JobPayload::ImportRoCrate(_) => {
            let row: ImportReportRow = postcard::from_bytes(value)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            if row.entry_key.as_bytes() != entry_key {
                return Err(ServerError::InternalError(
                    "stored import report entry key does not match its row".to_string(),
                ));
            }
            serde_json::to_value(row)
        }
        JobPayload::ExportRoCrate(_) => {
            let row: ExportReportRow = postcard::from_bytes(value)
                .map_err(|error| ServerError::InternalError(error.to_string()))?;
            if row.entry_key.as_bytes() != entry_key {
                return Err(ServerError::InternalError(
                    "stored export report entry key does not match its row".to_string(),
                ));
            }
            serde_json::to_value(row)
        }
        _ => return Err(ServerError::NotFound),
    };
    row.map_err(|error| ServerError::InternalError(error.to_string()))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/report",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Job identifier"),
        ("limit" = Option<usize>, Query, description = "Max report rows (default 200, max 1000)"),
        ("cursor" = Option<String>, Query, description = "Opaque report cursor")
    ),
    responses(
        (status = 200, description = "Immutable terminal report page", body = JobReportResponse),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 404, description = "Job not found or report pending", body = ReportPendingResponse),
        (status = 409, description = "Cursor does not match this frozen report", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_report(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Query(query): Query<ReportQuery>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let cursor = decode_report_cursor(query.cursor.as_deref())?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.job_id != job_id)
    {
        return Ok(coded_response(
            StatusCode::CONFLICT,
            "report cursor belongs to a different job",
            "report_cursor_conflict",
        ));
    }
    let limit = query
        .limit
        .filter(|limit| *limit > 0)
        .unwrap_or(DEFAULT_REPORT_LIMIT)
        .min(MAX_REPORT_LIMIT);
    let expected_digest = cursor.as_ref().map(|cursor| cursor.report_digest);
    let last_key = cursor.map(|cursor| cursor.last_key);
    match read_owned_report(
        &state.get_ctx(),
        auth.user_id,
        job_id,
        expected_digest,
        last_key,
        limit,
    )
    .await
    .map_err(ServerError::InternalError)?
    {
        JobReportLookup::NotFound => Err(ServerError::NotFound),
        JobReportLookup::Pending(state) => Ok((
            StatusCode::NOT_FOUND,
            Json(ReportPendingResponse {
                code: "report_pending".to_string(),
                state: state.name().to_string(),
            }),
        )
            .into_response()),
        JobReportLookup::CursorConflict => Ok(coded_response(
            StatusCode::CONFLICT,
            "report cursor does not match the frozen report",
            "report_cursor_conflict",
        )),
        JobReportLookup::Ready {
            record,
            rows,
            next_key,
        } => {
            let report_digest = record.report_digest.ok_or_else(|| {
                ServerError::InternalError(
                    "terminal RO-Crate job is missing its report digest".to_string(),
                )
            })?;
            let rows = rows
                .into_iter()
                .map(|(entry_key, value)| {
                    decode_report_row(&record.payload, &entry_key, value.as_ref())
                })
                .collect::<ServerResult<Vec<_>>>()?;
            Ok((
                StatusCode::OK,
                Json(JobReportResponse {
                    rows,
                    next_cursor: encode_report_cursor(job_id, report_digest, next_key)?,
                    report_digest: hex::encode(report_digest),
                }),
            )
                .into_response())
        }
    }
}

fn range_request(headers: &HeaderMap) -> Result<Option<ObjectRangeRequest>, ()> {
    let Some(value) = headers.get(RANGE) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| ())?;
    let range = value.strip_prefix("bytes=").ok_or(())?;
    if range.contains(',') {
        return Err(());
    }
    let (start, end) = range.split_once('-').ok_or(())?;
    match (start, end) {
        ("", "") => Err(()),
        ("", end) => end
            .parse::<u64>()
            .map(|length| Some(ObjectRangeRequest::Suffix { length }))
            .map_err(|_| ()),
        (start, "") => start
            .parse::<u64>()
            .map(|start| Some(ObjectRangeRequest::Start { start }))
            .map_err(|_| ()),
        (start, end) => {
            let start = start.parse::<u64>().map_err(|_| ())?;
            let end = end.parse::<u64>().map_err(|_| ())?;
            Ok(Some(ObjectRangeRequest::StartEnd { start, end }))
        }
    }
}

fn artifact_headers(owned: &OwnedArtifact) -> ServerResult<HeaderMap> {
    let location_hash: [u8; 32] = owned
        .artifact
        .location
        .get_blake3()
        .ok_or_else(|| {
            ServerError::InternalError("artifact location is missing its BLAKE3 hash".to_string())
        })?
        .try_into()
        .map_err(|_| {
            ServerError::InternalError("artifact location has an invalid BLAKE3 hash".to_string())
        })?;
    if location_hash != owned.artifact.blake3 {
        return Err(ServerError::InternalError(
            "artifact record does not match its blob location".to_string(),
        ));
    }
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/zip"));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{}\"", hex::encode(location_hash)))
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    let encoded = utf8_percent_encode(&owned.filename, NON_ALPHANUMERIC);
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"rocrate.zip\"; filename*=UTF-8''{encoded}"
        ))
        .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    Ok(headers)
}

fn range_error(size: u64) -> Response {
    let mut response = coded_response(
        StatusCode::RANGE_NOT_SATISFIABLE,
        "requested artifact range is not satisfiable",
        "invalid_range",
    );
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(value) = HeaderValue::from_str(&format!("bytes */{size}")) {
        response.headers_mut().insert(CONTENT_RANGE, value);
    }
    response
}

async fn artifact_response(
    state: Arc<ServerState>,
    auth: Option<AuthContext>,
    job_id: String,
    headers: HeaderMap,
    download: bool,
) -> ServerResult<Response> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let owned = match read_owned_artifact(
        &state.get_ctx(),
        auth.user_id,
        job_id,
        aruna_core::util::unix_timestamp_millis(),
    )
    .await
    .map_err(ServerError::InternalError)?
    {
        ArtifactLookup::NotFound => return Err(ServerError::NotFound),
        ArtifactLookup::Pending(state) => {
            return Ok((
                StatusCode::NOT_FOUND,
                Json(
                    ErrorResponse::new("RO-Crate artifact is not ready")
                        .with_code("artifact_pending")
                        .with_details(state.name()),
                ),
            )
                .into_response());
        }
        ArtifactLookup::Gone => {
            return Ok(coded_response(
                StatusCode::GONE,
                "RO-Crate artifact has expired",
                "artifact_expired",
            ));
        }
        ArtifactLookup::Ready(owned) => owned,
    };
    let range_request = match range_request(&headers) {
        Ok(range) => range,
        Err(()) => return Ok(range_error(owned.artifact.size)),
    };
    let (status, range, content_range) = match range_request {
        Some(request) => match request.resolve(owned.artifact.size) {
            Ok(resolved) => (
                StatusCode::PARTIAL_CONTENT,
                resolved.range,
                Some(resolved.content_range),
            ),
            Err(_) => return Ok(range_error(owned.artifact.size)),
        },
        None => (
            StatusCode::OK,
            Range {
                start: 0,
                end: owned.artifact.size,
            },
            None,
        ),
    };
    let content_length = range.end - range.start;
    let mut response_headers = artifact_headers(&owned)?;
    response_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .map_err(|error| ServerError::InternalError(error.to_string()))?,
    );
    if let Some(content_range) = content_range {
        response_headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&content_range)
                .map_err(|error| ServerError::InternalError(error.to_string()))?,
        );
    }
    let body = if download {
        let read = read_artifact_range(&state.get_ctx(), &owned.artifact, range)
            .await
            .map_err(ServerError::InternalError)?;
        if read.stream_size != content_length {
            return Err(ServerError::InternalError(
                "artifact reader returned an unexpected range size".to_string(),
            ));
        }
        Body::from_stream(read.blob)
    } else {
        Body::empty()
    };
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/artifacts/rocrate",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Job identifier"),
        ("Range" = Option<String>, Header, description = "Single byte range")
    ),
    responses(
        (status = 200, description = "Complete RO-Crate ZIP"),
        (status = 206, description = "Requested artifact byte range"),
        (status = 404, description = "Artifact not found or pending", body = ErrorResponse),
        (status = 410, description = "Artifact expired", body = ErrorResponse),
        (status = 416, description = "Range not satisfiable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_artifact(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, job_id, headers, true).await
}

#[utoipa::path(
    head,
    path = "/jobs/{job_id}/artifacts/rocrate",
    tag = "jobs",
    params(
        ("job_id" = String, Path, description = "Job identifier"),
        ("Range" = Option<String>, Header, description = "Single byte range")
    ),
    responses(
        (status = 200, description = "RO-Crate artifact headers"),
        (status = 206, description = "Requested artifact range headers"),
        (status = 404, description = "Artifact not found or pending", body = ErrorResponse),
        (status = 410, description = "Artifact expired", body = ErrorResponse),
        (status = 416, description = "Range not satisfiable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn head_job_artifact(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    artifact_response(state, auth, job_id, headers, false).await
}

#[utoipa::path(
    post,
    path = "/jobs/{job_id}/cancel",
    tag = "jobs",
    params(("job_id" = String, Path, description = "Job identifier")),
    responses(
        (status = 202, description = "Cancellation requested", body = JobStatusResponse),
        (status = 200, description = "Job already terminal", body = JobStatusResponse),
        (status = 404, description = "Job not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn cancel_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<(StatusCode, Json<JobStatusResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;

    let outcome = cancel_owned_job(
        &state.get_ctx(),
        &state.jobs_runtime(),
        auth.user_id,
        job_id,
    )
    .await
    .map_err(ServerError::InternalError)?;

    match outcome {
        CancelJobOutcome::NotFound => Err(ServerError::NotFound),
        CancelJobOutcome::AlreadyTerminal(record) => {
            Ok((StatusCode::OK, Json(job_status_response(&record))))
        }
        CancelJobOutcome::Requested(record) => {
            Ok((StatusCode::ACCEPTED, Json(job_status_response(&record))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::{
        ArtifactRef, BackendLocation, ExportOmissionCounts, ExportRoCrateResult, ExportRoCrateSpec,
        ImportMetadataTarget, ImportReportDetail, ImportRoCrateResult, ImportRoCrateSource,
        ImportRoCrateSpec, ImportRoCrateTarget, JobProgress, JobResultPayload, NodeCapabilities,
        PathRestriction, Permission, RealmId, ReasonCode, RoCrateLimits,
    };
    use aruna_core::types::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::{
        ClaimOutcome, claim_job, complete_job, insert_job, put_job_entry, transition_to_running,
    };
    use aruna_storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

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

    fn restricted_auth_for(user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: Some(vec![PathRestriction {
                pattern: "/realm/g/group/data/**".to_string(),
                permission: Permission::READ,
            }]),
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

    fn job_for(job_id: JobId, owner: UserId, created_at_ms: u64) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            owner,
            node_id(),
            created_at_ms,
            created_at_ms,
            None,
        )
    }

    fn import_job(job_id: JobId, owner: UserId) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::ImportRoCrate(ImportRoCrateSpec {
                auth_context: auth_for(owner).unwrap(),
                source: ImportRoCrateSource::Upload {
                    upload_id: Ulid::from_bytes([3u8; 16]),
                },
                target: ImportRoCrateTarget {
                    bucket: "target".to_string(),
                    prefix: "crate".to_string(),
                },
                metadata: ImportMetadataTarget {
                    group_id: Ulid::from_bytes([4u8; 16]),
                    path: "crate".to_string(),
                    public: false,
                },
                limits: RoCrateLimits::default(),
                document_id: Ulid::from_bytes([5u8; 16]),
            }),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        )
    }

    fn report_row(entry_key: &str) -> ImportReportRow {
        ImportReportRow {
            entry_key: entry_key.to_string(),
            code: ReasonCode::Imported,
            message: None,
            detail: ImportReportDetail {
                archive_path: entry_key.to_string(),
                target_key: Some(entry_key.to_string()),
                version_id: None,
                blake3: None,
                size: None,
                arn: None,
                w3id: None,
                validation: None,
            },
        }
    }

    fn export_job(job_id: JobId, owner: UserId, expires_at_ms: u64) -> JobRecord {
        let document_id = Ulid::from_bytes([6u8; 16]);
        let blake3 = [7u8; 32];
        let mut hashes = HashMap::new();
        hashes.insert(HASH_BLAKE3.to_string(), blake3.to_vec());
        let artifact = ArtifactRef {
            location: BackendLocation {
                root: "/tmp".to_string(),
                storage_bucket: "hidden".to_string(),
                backend_path: format!("_jobs/{job_id}/artifact.zip"),
                ulid: Ulid::from_bytes([8u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 5,
                hashes,
            },
            blake3,
            size: 5,
            expires_at_ms,
        };
        let digest = *blake3::hash(&[]).as_bytes();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::ExportRoCrate(ExportRoCrateSpec {
                auth_context: auth_for(owner).unwrap(),
                document_id,
                limits: RoCrateLimits::default(),
            }),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        );
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record.report_digest = Some(digest);
        record.result = Some(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
            artifact: Some(artifact),
            included: 1,
            omitted: ExportOmissionCounts::default(),
            report_digest: digest,
        }));
        record
    }

    async fn finish_report(state: &ServerState, job_id: JobId, owner: UserId) -> [u8; 32] {
        let context = state.get_ctx();
        let storage = &context.storage_handle;
        insert_job(storage, &import_job(job_id, owner))
            .await
            .unwrap();
        let ClaimOutcome::Claimed(record) =
            claim_job(storage, job_id, node_id(), 2_000).await.unwrap()
        else {
            panic!("job was not claimed")
        };
        let token = record.claim.unwrap().claim_token;
        put_job_entry(storage, job_id, token, b"b", &report_row("b"))
            .await
            .unwrap();
        put_job_entry(storage, job_id, token, b"a", &report_row("a"))
            .await
            .unwrap();
        transition_to_running(storage, job_id, token, 2_500)
            .await
            .unwrap();
        complete_job(
            storage,
            job_id,
            token,
            JobResultPayload::ImportRoCrate(ImportRoCrateResult {
                document_id: Some(Ulid::from_bytes([5u8; 16])),
                entries_total: 2,
                imported: 2,
                unlisted: 0,
                failed: 0,
                report_digest: [0u8; 32],
            }),
            JobProgress {
                current: 2,
                total: Some(2),
                unit: "entries".to_string(),
            },
            3_000,
        )
        .await
        .unwrap()
        .report_digest
        .unwrap()
    }

    async fn response_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn list_newest_first() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        for seq in 1..=3u64 {
            insert_job(
                &state.get_ctx().storage_handle,
                &job_for(JobId(Ulid::from_parts(seq, 0)), owner, seq * 1000),
            )
            .await
            .unwrap();
        }

        let (_, Json(page1)) = list_jobs(
            State(state.clone()),
            Extension(auth_for(owner)),
            Query(ListJobsQuery {
                limit: Some(2),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(page1.jobs.len(), 2);
        // Newest first: seq 3 then seq 2.
        assert_eq!(
            page1.jobs[0].job_id,
            JobId(Ulid::from_parts(3, 0)).to_string()
        );
        let cursor = page1.next_cursor.clone().expect("cursor for next page");

        let (_, Json(page2)) = list_jobs(
            State(state.clone()),
            Extension(auth_for(owner)),
            Query(ListJobsQuery {
                limit: Some(2),
                cursor: Some(cursor),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(page2.jobs.len(), 1);
        assert_eq!(
            page2.jobs[0].job_id,
            JobId(Ulid::from_parts(1, 0)).to_string()
        );
        assert!(page2.next_cursor.is_none());
    }

    #[tokio::test]
    async fn report_pages_frozen() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        let digest = finish_report(&state, job_id, owner).await;

        let first = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: Some(1),
                cursor: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first: JobReportResponse = response_json(first).await;
        assert_eq!(first.rows[0]["entry_key"], "a");
        assert_eq!(first.report_digest, hex::encode(digest));

        let second = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: Some(1),
                cursor: first.next_cursor,
            }),
        )
        .await
        .unwrap();
        let second: JobReportResponse = response_json(second).await;
        assert_eq!(second.rows[0]["entry_key"], "b");
        assert!(second.next_cursor.is_none());

        let conflict = ReportCursor {
            job_id,
            report_digest: [0u8; 32],
            last_key: b"a".to_vec(),
        };
        let conflict = URL_SAFE_NO_PAD.encode(postcard::to_allocvec(&conflict).unwrap());
        let response = get_job_report(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Query(ReportQuery {
                limit: None,
                cursor: Some(conflict),
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let foreign = get_job_report(
            State(state),
            Extension(auth_for(user(3))),
            Path(job_id.to_string()),
            Query(ReportQuery::default()),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn report_pending_typed() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([10u8; 16]);
        insert_job(&state.get_ctx().storage_handle, &import_job(job_id, owner))
            .await
            .unwrap();

        let response = get_job_report(
            State(state),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            Query(ReportQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: ReportPendingResponse = response_json(response).await;
        assert_eq!(body.code, "report_pending");
        assert_eq!(body.state, "queued");
    }

    #[tokio::test]
    async fn artifact_head_headers() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([11u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &export_job(
                job_id,
                owner,
                aruna_core::util::unix_timestamp_millis() + 60_000,
            ),
        )
        .await
        .unwrap();

        let response = head_job_artifact(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
        assert_eq!(response.headers()[CONTENT_LENGTH], "5");
        assert_eq!(
            response.headers()[ETAG].to_str().unwrap(),
            format!("\"{}\"", hex::encode([7u8; 32]))
        );
        assert!(
            response.headers()[CONTENT_DISPOSITION]
                .to_str()
                .unwrap()
                .contains(".zip")
        );

        let foreign = head_job_artifact(
            State(state),
            Extension(auth_for(user(3))),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn artifact_expiry_gone() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([12u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &export_job(job_id, owner, 1),
        )
        .await
        .unwrap();

        let response = head_job_artifact(
            State(state),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::GONE);
    }

    #[test]
    fn range_parses_single() {
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_static("bytes=2-4"));
        assert_eq!(
            range_request(&headers),
            Ok(Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }))
        );
        headers.insert(RANGE, HeaderValue::from_static("bytes=-3"));
        assert_eq!(
            range_request(&headers),
            Ok(Some(ObjectRangeRequest::Suffix { length: 3 }))
        );
        headers.insert(RANGE, HeaderValue::from_static("bytes=2-3,5-6"));
        assert_eq!(range_request(&headers), Err(()));
    }

    #[tokio::test]
    async fn urls_are_absolute() {
        let (_dir, state) = build_state().await;
        state
            .register_rest_interface_with_public_url(
                "127.0.0.1:3000".parse().unwrap(),
                Some("https://owner.example/"),
            )
            .await;
        let job_id = JobId::from_bytes([13u8; 16]);
        let urls = job_urls(&state, job_id).await.unwrap();
        assert_eq!(urls.owner_node_url, "https://owner.example/api/v1");
        assert_eq!(
            urls.status_url,
            format!("https://owner.example/api/v1/jobs/{job_id}")
        );
        assert_eq!(
            urls.report_url,
            format!("https://owner.example/api/v1/jobs/{job_id}/report")
        );
        assert_eq!(
            urls.artifact_url,
            format!("https://owner.example/api/v1/jobs/{job_id}/artifacts/rocrate")
        );
    }

    #[tokio::test]
    async fn foreign_not_found() {
        let (_dir, state) = build_state().await;
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id, user(2), 1000),
        )
        .await
        .unwrap();

        let result = get_job(
            State(state.clone()),
            Extension(auth_for(user(3))),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn cancel_is_idempotent() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        // has_run keeps this job off the never-run direct-cancel fast path (see
        // set_cancel_requested), so it stays live and cancel_requested-flagged across
        // repeated cancel calls, which is what this test exercises.
        let mut record = job_for(job_id, owner, 1000);
        record.has_run = true;
        insert_job(&state.get_ctx().storage_handle, &record)
            .await
            .unwrap();

        let (status, _) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);

        // Repeated cancel of a still-live job stays 202.
        let (status, Json(body)) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.cancel_requested);
    }

    #[tokio::test]
    async fn cancel_terminal_noop() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([4u8; 16]);
        let mut record = job_for(job_id, owner, 1000);
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2000);
        insert_job(&state.get_ctx().storage_handle, &record)
            .await
            .unwrap();

        let (status, _) = cancel_job(
            State(state.clone()),
            Extension(auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn openapi_has_jobs() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(openapi.paths.paths.contains_key("/jobs/"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}/cancel"));
        assert!(openapi.paths.paths.contains_key("/jobs/{job_id}/report"));
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/jobs/{job_id}/artifacts/rocrate")
        );
    }

    // A path-restricted (delegated) token must not reach any user-scoped job surface.
    #[tokio::test]
    async fn restricted_token_rejected() {
        let (_dir, state) = build_state().await;
        let owner = user(2);
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id, owner, 1000),
        )
        .await
        .unwrap();

        let list = list_jobs(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Query(ListJobsQuery::default()),
        )
        .await;
        assert!(matches!(list, Err(ServerError::Forbidden)));

        let get = get_job(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(get, Err(ServerError::Forbidden)));

        let cancel = cancel_job(
            State(state.clone()),
            Extension(restricted_auth_for(owner)),
            Path(job_id.to_string()),
        )
        .await;
        assert!(matches!(cancel, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn rejects_huge_ram() {
        // ram_bytes above i64::MAX would wrap negative in the Docker backend.
        let (_dir, state) = build_state().await;
        for ram_bytes in [u64::MAX, i64::MAX as u64 + 1, 0] {
            let request = SubmitExecutionRequest {
                group_id: Ulid::from_bytes([5u8; 16]).to_string(),
                image: "alpine:3".to_string(),
                entrypoint: None,
                command: vec!["true".to_string()],
                env: BTreeMap::new(),
                cpu_cores: None,
                ram_bytes: Some(ram_bytes),
                max_walltime_ms: None,
                executor_constraint: None,
                inputs: Vec::new(),
                outputs: Vec::new(),
                output_prefixes: Vec::new(),
                idempotency_key: None,
                workspace: None,
            };
            let result = submit_job(
                State(state.clone()),
                Extension(auth_for(user(2))),
                Json(request),
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::BadRequest)),
                "ram_bytes {ram_bytes} must be rejected"
            );
        }
    }

    #[test]
    fn maps_native_input() {
        // Missing container_path defaults to /inputs/<dest_key>.
        let input = ExecutionInputRequest {
            bucket: "src".to_string(),
            key: "data.csv".to_string(),
            version_id: None,
            dest_key: "in/data.csv".to_string(),
            container_path: None,
        };
        let mapped = native_input(input.clone()).unwrap();
        assert_eq!(
            mapped.container_path.as_deref(),
            Some("/inputs/in/data.csv")
        );

        let explicit = ExecutionInputRequest {
            container_path: Some("/data/input.csv".to_string()),
            ..input.clone()
        };
        assert_eq!(
            native_input(explicit).unwrap().container_path.as_deref(),
            Some("/data/input.csv")
        );

        let traversal = ExecutionInputRequest {
            container_path: Some("/in/../etc".to_string()),
            ..input
        };
        assert!(native_input(traversal).is_err());
    }

    #[test]
    fn maps_native_output() {
        let mapped = native_output(ExecutionOutputRequest {
            container_path: "/out/report.txt".to_string(),
            dest_key: "outputs/report.txt".to_string(),
        })
        .unwrap();
        assert_eq!(mapped.container_path, "/out/report.txt");
        assert_eq!(mapped.dest_key, "outputs/report.txt");

        assert!(
            native_output(ExecutionOutputRequest {
                container_path: "relative/path".to_string(),
                dest_key: "k".to_string(),
            })
            .is_err()
        );
        assert!(
            native_output(ExecutionOutputRequest {
                container_path: "/out".to_string(),
                dest_key: String::new(),
            })
            .is_err()
        );
    }

    #[test]
    fn normalizes_prefixes() {
        assert_eq!(
            validate_output_prefixes(vec!["results/".to_string(), "results/".to_string()]).unwrap(),
            ["results/"]
        );
        assert!(validate_output_prefixes(vec![String::new()]).is_err());
        assert!(
            validate_output_prefixes(vec!["result".to_string(); MAX_OUTPUT_PREFIXES + 1]).is_err()
        );
    }

    #[test]
    fn workspace_defaults_kept() {
        assert_eq!(
            workspace_request(None).unwrap(),
            (WorkspaceMode::Kept, None)
        );
        let record = job_for(JobId(Ulid::from_parts(1, 0)), user(2), 1);
        assert_eq!(job_status_response(&record).workspace_mode, "kept");
        assert!(
            workspace_request(Some(WorkspaceRequest {
                mode: WorkspaceModeRequest::Existing,
                bucket: None,
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_cursor_rejected() {
        let (_dir, state) = build_state().await;
        let result = list_jobs(
            State(state.clone()),
            Extension(auth_for(user(2))),
            Query(ListJobsQuery {
                cursor: Some("not-base64!".to_string()),
                ..Default::default()
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }
}
