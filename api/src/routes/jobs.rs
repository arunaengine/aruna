use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId,
    JobRecord, JobState,
};
use aruna_operations::jobs::service::{
    CancelJobOutcome, cancel_owned_job, list_owned_jobs, read_job_run_crate_status, read_owned_job,
    submit_execution_job,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;

#[derive(OpenApi)]
#[openapi(
    tags((name = "jobs", description = "Durable background jobs")),
    paths(list_jobs, get_job, cancel_job, submit_job)
)]
pub struct JobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/jobs/", get(list_jobs).post(submit_job))
        .route("/jobs/{job_id}", get(get_job))
        .route("/jobs/{job_id}/cancel", post(cancel_job))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecutionInputRequest {
    pub bucket: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    pub dest_key: String,
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
    pub output_prefixes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitJobResponse {
    pub job_id: String,
    pub created: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListJobsQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<String>,
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

fn parse_job_id(raw: &str) -> ServerResult<JobId> {
    JobId::from_str(raw).map_err(|_| ServerError::NotFound)
}

fn map_submit_error(error: aruna_operations::jobs::submit::SubmitJobError) -> ServerError {
    use aruna_operations::jobs::submit::SubmitJobError;
    match error {
        SubmitJobError::JobPlanConflict { existing_job_id } => ServerError::Conflict(format!(
            "idempotency key already bound to job {existing_job_id}"
        )),
        other => ServerError::InternalError(other.to_string()),
    }
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
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(MAX_LIST_LIMIT);
    let state_filter = query.state.as_deref().map(parse_state).transpose()?;

    let (records, next_cursor) =
        list_owned_jobs(&state.get_ctx(), auth.user_id, cursor, limit, state_filter)
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
    if request.image.trim().is_empty() {
        return Err(ServerError::BadRequest);
    }

    let inputs = request
        .inputs
        .into_iter()
        .map(|input| InputSelection {
            source: InputSource::S3 {
                bucket: input.bucket,
                key: input.key,
                version_id: input.version_id,
            },
            dest_key: input.dest_key,
            mode: InputMode::Snapshot,
        })
        .collect();

    let spec = ExecutionSpec {
        group_id,
        image: request.image,
        entrypoint: request.entrypoint,
        command: request.command,
        env: request.env,
        resources: ComputeResources {
            cpu_cores: request.cpu_cores,
            ram_bytes: request.ram_bytes,
            max_walltime_ms: request.max_walltime_ms,
        },
        executor_constraint: request.executor_constraint,
        inputs,
        output_prefixes: request.output_prefixes,
    };
    let result = submit_execution_job(
        &state.get_ctx(),
        spec,
        auth.user_id,
        state.get_node_id(),
        request.idempotency_key,
    )
    .await
    .map_err(map_submit_error)?;

    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(SubmitJobResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
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
    use aruna_core::structs::{JobPayload, NodeCapabilities, PathRestriction, Permission, RealmId};
    use aruna_core::types::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::insert_job;
    use aruna_storage::FjallStorage;
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
