use std::str::FromStr;
use std::sync::Arc;

use aruna_core::handle::Handle;
use aruna_core::structs::{AuthContext, JobId, JobRecord, JobState};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::jobs::store::{
    CancelRequestOutcome, JobMutationError, list_jobs_for_user, read_job_record,
    set_cancel_requested,
};
use aruna_operations::jobs::submit::schedule_job_drain_effect;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

const DEFAULT_LIST_LIMIT: usize = 50;
const MAX_LIST_LIMIT: usize = 200;

#[derive(OpenApi)]
#[openapi(
    tags((name = "jobs", description = "Durable background jobs")),
    paths(list_jobs, get_job, cancel_job)
)]
pub struct JobsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/jobs/", get(list_jobs))
        .route("/jobs/{job_id}", get(get_job))
        .route("/jobs/{job_id}/cancel", post(cancel_job))
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
    }
}

fn parse_state(value: &str) -> ServerResult<JobState> {
    match value {
        "queued" => Ok(JobState::Queued),
        "claimed" => Ok(JobState::Claimed),
        "running" => Ok(JobState::Running),
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

/// Load a job, returning 404 (never leaking existence) when it is missing or owned
/// by another user.
async fn load_owned_job(
    state: &ServerState,
    auth: &AuthContext,
    job_id: JobId,
) -> ServerResult<JobRecord> {
    let record = read_job_record(&state.get_ctx().storage_handle, job_id, None)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)?;
    if record.created_by != auth.user_id {
        return Err(ServerError::NotFound);
    }
    Ok(record)
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
    let auth = require_realm_auth(&state, auth)?;
    let cursor = decode_cursor(query.cursor.as_deref())?;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .min(MAX_LIST_LIMIT);
    let state_filter = query.state.as_deref().map(parse_state).transpose()?;

    let (records, next_cursor) = list_jobs_for_user(
        &state.get_ctx().storage_handle,
        auth.user_id,
        cursor,
        limit,
        state_filter,
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
    let auth = require_realm_auth(&state, auth)?;
    let job_id = JobId::from_str(&job_id).map_err(|_| ServerError::NotFound)?;
    let record = load_owned_job(&state, &auth, job_id).await?;
    Ok((StatusCode::OK, Json(job_status_response(&record))))
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
    let auth = require_realm_auth(&state, auth)?;
    let job_id = JobId::from_str(&job_id).map_err(|_| ServerError::NotFound)?;
    load_owned_job(&state, &auth, job_id).await?;

    let outcome = set_cancel_requested(
        &state.get_ctx().storage_handle,
        job_id,
        unix_timestamp_millis(),
    )
    .await
    .map_err(map_mutation_error)?;

    match outcome {
        CancelRequestOutcome::AlreadyTerminal(record) => {
            Ok((StatusCode::OK, Json(job_status_response(&record))))
        }
        CancelRequestOutcome::Flagged(record) => {
            state.jobs_runtime().request_cancel(job_id);
            kick_drain(&state).await;
            Ok((StatusCode::ACCEPTED, Json(job_status_response(&record))))
        }
    }
}

async fn kick_drain(state: &ServerState) {
    if let Some(task_handle) = state.get_ctx().task_handle.as_ref() {
        let _ = task_handle.send_effect(schedule_job_drain_effect()).await;
    }
}

fn map_mutation_error(error: JobMutationError) -> ServerError {
    match error {
        JobMutationError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{JobPayload, NodeCapabilities, RealmId};
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

    async fn build_state() -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
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
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id, owner, 1000),
        )
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
