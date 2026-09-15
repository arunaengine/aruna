//! Interactive session routes. They are served by the node that runs the
//! session job, next to cancel and with the same authorization: only the
//! submitter, and anybody else's job answers 404.

use std::convert::Infallible;
use std::sync::Arc;

use aruna_compute::session::EventKind;
use aruna_compute::session::events::SessionEvent;
use aruna_compute::session::{
    EndReason, MAX_SCRATCH_READ_BYTES, PendingInput, Session, SessionError, StagedInput,
};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::{
    AuthContext, CopyJobSpec, JobId, JobPayload, JobRecord, JobState, Permission, key_content_type,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::lifecycle::ids::session_of;
use aruna_operations::jobs::service::read_session_reason;
use aruna_operations::jobs::service::submit_copy_job;
use aruna_operations::realm::get_config::GetRealmConfigOperation;
use aruna_operations::s3::copy_object::{
    CopyObjectInput, CopyReferences, CopySourceConditions, copy_object,
};
use aruna_operations::s3::get_bucket::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{blob_permission_path, ensure_permission, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::jobs::owned_session_job;
use crate::routes::jobs::{
    JobStatusResponse, coded_response, job_status_response, map_submit_error,
};
use crate::routes::staging::queue_live_replication;
use crate::server_state::ServerState;

/// Envoy idles an upstream at 60 seconds, so the stream keeps itself alive.
const KEEP_ALIVE: Duration = Duration::from_secs(15);
/// Objects one staging call brings into the workspace bucket.
const MAX_STAGED_ITEMS: usize = 64;

#[derive(OpenApi)]
#[openapi(tags((name = "compute/sessions", description = "Interactive notebook sessions")))]
pub struct JobSessionApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(JobSessionApiDoc::openapi())
        .routes(routes!(get_session))
        .routes(routes!(stream_session))
        .routes(routes!(submit_cell))
        .routes(routes!(interrupt_session))
        .routes(routes!(end_session))
        .routes(routes!(stage_inputs))
        .routes(routes!(list_scratch))
        .routes(routes!(read_scratch))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionCellResponse {
    pub cell_id: String,
    /// One of `queued`, `running`, `done`, `error` or `interrupted`.
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionEndedResponse {
    /// One of `ended`, `idle`, `walltime`, `cancelled`, `kernel_exit` or
    /// `node_restart`.
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionResponse {
    pub job_id: String,
    /// One of `starting`, `ready`, `busy` or `ended`.
    pub state: String,
    pub runtime: String,
    pub workspace_bucket: String,
    /// The node serving this session. Session routes are served here only.
    pub executor_node_id: String,
    pub started_at_ms: u64,
    pub idle_after_ms: u64,
    pub idle_deadline_ms: u64,
    pub credential_expires_at_ms: u64,
    pub last_event_id: u64,
    /// Absent on the stream's own `session` frame, which carries no cells.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cells: Option<Vec<SessionCellResponse>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<SessionEndedResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SubmitCellRequest {
    /// 1 to 64 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`.
    pub cell_id: String,
    /// The cell source, at most 256 KiB. The node forwards it unchanged.
    pub code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SubmitCellResponse {
    pub cell_id: String,
    /// Place in the queue, counted from one.
    pub position: usize,
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct EventsQuery {
    /// Resume point, same value as the `Last-Event-ID` header.
    pub after: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct ScratchQuery {
    /// Directory relative to the working directory. `..` is refused.
    pub path: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct ScratchReadQuery {
    /// File relative to the working directory. `..` is refused.
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ScratchEntryResponse {
    pub name: String,
    /// `file` or `dir`.
    pub kind: String,
    pub bytes: u64,
    pub modified_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ScratchListResponse {
    pub path: String,
    pub entries: Vec<ScratchEntryResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionInputRequest {
    /// Source bucket holding the object.
    pub bucket: String,
    /// Source object key inside `bucket`.
    pub key: String,
    /// Exact version to copy. Defaults to the current head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// Realm node holding the object. Defaults to this node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    /// Full key inside the workspace bucket the object lands under.
    pub dest_key: String,
    /// How a source that is itself a reference lands: `snapshot` pulls the
    /// bytes through a background job, `reference` links without copying.
    #[serde(default)]
    pub strategy: SessionInputStrategy,
}

/// What the workspace gets for a source whose bytes sit behind a reference.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionInputStrategy {
    #[default]
    Snapshot,
    Reference,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionInputsRequest {
    pub items: Vec<SessionInputRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StagedInputResponse {
    pub dest_key: String,
    pub bytes: u64,
    pub blake3: String,
    pub source_node_id: String,
    pub version_id: String,
    /// The workspace holds a reference; reads stream from the source.
    #[serde(default)]
    pub linked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PendingInputResponse {
    pub dest_key: String,
    /// The copy job; `GET /compute/jobs/{job_id}` reports its progress in bytes.
    pub job_id: String,
    pub source_node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct FailedInputResponse {
    pub dest_key: String,
    /// Safe reason this item did not land, so the caller can retry or drop it.
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SessionInputsResponse {
    pub staged: Vec<StagedInputResponse>,
    /// Sources a background copy job is pulling in; poll the job for progress.
    pub pending: Vec<PendingInputResponse>,
    /// Items that did not land, named so a partial result stays reconcilable.
    #[serde(default)]
    pub failed: Vec<FailedInputResponse>,
}

/// The live session, or the coded answer that says why there is none here.
/// A node restart re-adopts the container and opens a new session, so a
/// non-terminal job without one is starting, never ended.
async fn live_session(
    state: &ServerState,
    record: &JobRecord,
    physical_job_id: Option<JobId>,
    state_read: bool,
) -> Result<Arc<Session>, Response> {
    if physical_job_id.is_some() && record.owner_node_id != state.get_node_id() {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "the session runs on another node",
                "code": "session_not_here",
                "executor_node_id": record.owner_node_id.to_string(),
            })),
        )
            .into_response());
    }
    let session = state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| {
            physical_job_id.and_then(|id| registry.sessions().get(&id.to_string()))
        });
    if let Some(session) = session {
        return Ok(session);
    }
    if !state_read {
        return Err(session_error(if record.state.is_terminal() {
            SessionError::Ended
        } else {
            SessionError::Starting
        }));
    }
    if !record.state.is_terminal() {
        return Err(starting_response(record).into_response());
    }
    Err(ended_response(state, record, physical_job_id)
        .await
        .into_response())
}

/// A job whose session has not started here yet.
fn starting_response(record: &JobRecord) -> Json<SessionResponse> {
    Json(SessionResponse {
        state: "starting".to_string(),
        ended: None,
        ..base_response(record)
    })
}

/// A finished session job. The reason comes from the report the workflow wrote,
/// so a failed session says why it stopped rather than reading as a clean end.
async fn ended_response(
    state: &ServerState,
    record: &JobRecord,
    physical_job_id: Option<JobId>,
) -> Json<SessionResponse> {
    let reason = read_session_reason(
        &state.get_ctx(),
        record.created_by,
        physical_job_id.unwrap_or(record.job_id),
    )
    .await
    .unwrap_or_else(|| default_reason(record).to_string());
    Json(SessionResponse {
        state: "ended".to_string(),
        ended: Some(SessionEndedResponse { reason }),
        ..base_response(record)
    })
}

fn default_reason(record: &JobRecord) -> &'static str {
    match record.state {
        JobState::Cancelled => EndReason::Cancelled.as_str(),
        _ => EndReason::Ended.as_str(),
    }
}

/// The fields every answer about a job without a live session shares.
fn base_response(record: &JobRecord) -> SessionResponse {
    let requested = match &record.payload {
        JobPayload::Execution(spec) => session_of(spec),
        _ => None,
    };
    SessionResponse {
        job_id: record.job_id.to_string(),
        state: String::new(),
        runtime: requested.map(|session| session.runtime).unwrap_or_default(),
        workspace_bucket: record.workspace_bucket.clone().unwrap_or_default(),
        executor_node_id: record.owner_node_id.to_string(),
        started_at_ms: record.started_at_ms.unwrap_or(record.created_at_ms),
        idle_after_ms: 0,
        idle_deadline_ms: 0,
        credential_expires_at_ms: 0,
        last_event_id: 0,
        cells: Some(Vec::new()),
        ended: None,
    }
}

pub(crate) fn session_response(session: &Session, with_cells: bool) -> SessionResponse {
    let snapshot = session.snapshot();
    SessionResponse {
        job_id: snapshot.job_id,
        state: snapshot.state.as_str().to_string(),
        runtime: snapshot.runtime,
        workspace_bucket: snapshot.workspace_bucket,
        executor_node_id: snapshot.executor_node_id,
        started_at_ms: snapshot.started_at_ms,
        idle_after_ms: snapshot.idle_after_ms,
        idle_deadline_ms: snapshot.idle_deadline_ms,
        credential_expires_at_ms: snapshot.credential_expires_at_ms,
        last_event_id: snapshot.last_event_id,
        cells: with_cells.then(|| {
            snapshot
                .cells
                .into_iter()
                .map(|cell| SessionCellResponse {
                    cell_id: cell.cell_id,
                    state: cell.state.as_str().to_string(),
                    execution_count: cell.execution_count,
                    started_at_ms: cell.started_at_ms,
                    finished_at_ms: cell.finished_at_ms,
                })
                .collect()
        }),
        ended: snapshot.ended.map(|reason| SessionEndedResponse {
            reason: reason.as_str().to_string(),
        }),
    }
}

/// Maps a session refusal to the coded answer the contract names.
fn session_error(error: SessionError) -> Response {
    match error {
        SessionError::Starting => coded_response(
            StatusCode::CONFLICT,
            "the session is still starting",
            "session_starting",
        ),
        SessionError::Ended => coded_response(
            StatusCode::CONFLICT,
            "the session has ended",
            "session_ended",
        ),
        SessionError::CellBusy(_) => coded_response(
            StatusCode::CONFLICT,
            "that cell is already queued or running",
            "cell_busy",
        ),
        SessionError::TooMany => coded_response(
            StatusCode::TOO_MANY_REQUESTS,
            "too many cells queued or submitted",
            "session_busy",
        ),
        SessionError::CellId | SessionError::CodeTooLarge | SessionError::Path => {
            ServerError::BadRequestMessage(error.to_string()).into_response()
        }
        SessionError::NoReply => {
            ServerError::ServiceUnavailableReason("session_helper_silent".to_string())
                .into_response()
        }
        SessionError::Helper(message) => ServerError::BadRequestMessage(message).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/compute/jobs/{job_id}/session",
    tag = "compute/sessions",
    summary = "Read the state of an interactive session",
    description = r#"Returns the live state of the caller's interactive session, including every cell the node still tracks.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel: only the submitter, and anybody else's job answers 404.

**Behavior**
- Session routes are served by the node that runs the job. When another node runs it, the answer is
  409 with code `session_not_here` and the `executor_node_id` to talk to instead.
- A node restart re-adopts the running container and opens a new session for it, so a job that is
  queued, preparing or being re-adopted answers `starting`, never `ended`. Event ids start again
  at one, and a client resuming from an older point receives a `gap` and re-reads this state.
- A finished session answers `ended` with the reason the node recorded: `ended`, `idle`,
  `walltime`, `cancelled` or `kernel_exit`."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    responses(
        (status = 200, description = "The session state", body = SessionResponse, example = json!({
            "job_id": "01JJRSTVWXYZ0123456789ABCD", "state": "ready", "runtime": "python-notebook",
            "workspace_bucket": "lab-data", "executor_node_id": "node-1", "started_at_ms": 1755500000000_u64,
            "idle_after_ms": 1800000, "idle_deadline_ms": 1755501800000_u64,
            "credential_expires_at_ms": 1755503600000_u64, "last_event_id": 1, "cells": []
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "Another node runs this session; code `session_not_here`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    match live_session(&state, &record, physical_job_id, true).await {
        Ok(session) => Ok(Json(session_response(&session, true)).into_response()),
        Err(response) => Ok(response),
    }
}

#[utoipa::path(
    get,
    path = "/compute/jobs/{job_id}/session/events",
    tag = "compute/sessions",
    summary = "Stream the events of an interactive session",
    description = r#"Streams cell states, cell outputs, kernel states, credential refreshes and the end of the session as server-sent events.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- The first frame is always `session`, carrying the state object without its cells.
- Every later frame carries an `id`, a per-session sequence starting at one. Reconnect with
  `Last-Event-ID` or the `after` query parameter to resume from it.
- When the resume point is older than the node still holds, one `gap` frame names the range that
  was lost; the client re-reads the session state and treats running cells as incomplete.
- A keep-alive comment is sent every 15 seconds so an idle proxy does not close the stream.
- The stream closes after the `ended` frame."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID"),
        EventsQuery
    ),
    responses(
        (status = 200, description = "The event stream", body = SessionResponse, content_type = "text/event-stream"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "Another node runs this session; code `session_not_here`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stream_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Query(query): Query<EventsQuery>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, true).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let after = query.after.or_else(|| last_event_id(&headers)).unwrap_or(0);
    let mut initial = vec![frame("session", &session_response(&session, false))];
    let (backlog, receiver) = match session.subscribe(after) {
        Ok(resumed) => resumed,
        Err(_) => {
            let (backlog, receiver) = session.subscribe_all();
            // The lost range is what the ring no longer reaches, so the client
            // sees the window it still holds rather than an inverted one.
            initial.push(frame(
                "gap",
                &json!({
                    "from": backlog.first().map_or(after, |event| event.id),
                    "to": backlog.last().map_or(after, |event| event.id),
                }),
            ));
            (backlog, receiver)
        }
    };
    let last_id = backlog.last().map_or(after, |event| event.id);
    let ended = stream_ended(&backlog);
    Ok(sse(
        stream::iter(initial)
            .chain(stream::iter(backlog.into_iter().map(sse_event)))
            .chain(live_stream(receiver, last_id, ended)),
        &state,
    ))
}

/// True when the replayed frames already carry the end of the session, so the
/// live tail must close instead of waiting for a frame that never comes.
fn stream_ended(backlog: &[SessionEvent]) -> bool {
    backlog
        .last()
        .is_some_and(|event| event.kind == EventKind::Ended)
}

fn sse<S>(events: S, state: &ServerState) -> Response
where
    S: Stream<Item = Event> + Send + 'static,
{
    let shutdown = state.shutdown_token();
    let events = events
        .take_until(async move { shutdown.cancelled().await })
        .map(Ok::<Event, Infallible>);
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response()
}

/// The live tail of the stream. It stops right after `ended`, and a reader that
/// fell behind is told the exact range it lost before the next event.
fn live_stream(
    receiver: tokio::sync::broadcast::Receiver<SessionEvent>,
    last_id: u64,
    ended: bool,
) -> impl Stream<Item = Event> + Send {
    stream::unfold(
        (receiver, last_id, VecDeque::new(), ended),
        |(mut receiver, mut last_id, mut pending, done)| async move {
            if let Some(event) = pending.pop_front() {
                return Some((event, (receiver, last_id, pending, done)));
            }
            if done {
                return None;
            }
            loop {
                match receiver.recv().await {
                    Ok(event) => {
                        let ended = event.kind == EventKind::Ended;
                        let id = event.id;
                        if id > last_id.saturating_add(1) {
                            pending.push_back(sse_event(event));
                            let gap = frame(
                                "gap",
                                &json!({ "from": last_id.saturating_add(1), "to": id }),
                            );
                            return Some((gap, (receiver, id, pending, ended)));
                        }
                        last_id = id;
                        return Some((sse_event(event), (receiver, last_id, pending, ended)));
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => return None,
                }
            }
        },
    )
}

fn sse_event(event: SessionEvent) -> Event {
    Event::default()
        .id(event.id.to_string())
        .event(event.kind.as_str())
        .data(event.data)
}

fn frame<T: Serialize>(name: &str, data: &T) -> Event {
    Event::default()
        .event(name)
        .data(serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string()))
}

fn last_event_id(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[utoipa::path(
    post,
    path = "/compute/jobs/{job_id}/session/cells",
    tag = "compute/sessions",
    summary = "Run one cell in an interactive session",
    description = r#"Queues one cell for the session's kernel and returns its place in the queue.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- 202 means the cell was queued, not that it ran. Outputs arrive on the event stream.
- A submit resets the session's idle timer.
- The node forwards the code unchanged and never interprets it. Cell traffic creates no job record.

**Limits**
- `cell_id` is 1 to 64 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`; `code` is at most 256 KiB.
- 409 `session_starting` or `session_ended` when the session is not ready, and `cell_busy` when
  that cell id is already queued or running.
- 429 when more than 64 cells are queued, or more than 30 submits arrived in 10 seconds."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    request_body(content = SubmitCellRequest, example = json!({"cell_id": "cell-1", "code": "print('hello')"})),
    responses(
        (status = 202, description = "The cell was queued", body = SubmitCellResponse, example = json!({"cell_id": "cell-1", "position": 1})),
        (status = 400, description = "An invalid cell id or oversized code", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session is not ready, or that cell is busy", body = ErrorResponse),
        (status = 429, description = "Too many cells queued or submitted", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before retrying the cell submission")))
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_cell(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Json(request): Json<SubmitCellRequest>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    match session.submit_cell(&request.cell_id, &request.code) {
        Ok(position) => Ok((
            StatusCode::ACCEPTED,
            Json(SubmitCellResponse {
                cell_id: request.cell_id,
                position,
            }),
        )
            .into_response()),
        Err(error) => Ok(session_error(error)),
    }
}

#[utoipa::path(
    post,
    path = "/compute/jobs/{job_id}/session/interrupt",
    tag = "compute/sessions",
    summary = "Interrupt the running cell of an interactive session",
    description = r#"Interrupts the cell the kernel is running and drops everything still queued.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- Every dropped cell gets a `cell` event with state `interrupted` on the event stream.
- 202 means the interrupt was sent, not that the kernel already stopped."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    responses(
        (status = 202, description = "The interrupt was sent"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session has ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn interrupt_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    match session.interrupt() {
        Ok(()) => Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response()),
        Err(error) => Ok(session_error(error)),
    }
}

#[utoipa::path(
    post,
    path = "/compute/jobs/{job_id}/session/end",
    tag = "compute/sessions",
    summary = "End an interactive session",
    description = r#"Ends the session and lets the job finish.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- The job finishes `succeeded` with reason `ended`; whatever the session wrote is already in the
  workspace bucket.
- Ending is asynchronous: 202 carries the job status at this moment, so the caller polls the job
  for the terminal state.
- The call is idempotent: ending an already ended session keeps the first reason."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    responses(
        (status = 202, description = "The session was ended", body = JobStatusResponse, example = json!({
            "job_id": "01JJRSTVWXYZ0123456789ABCD", "kind": "execution", "state": "running",
            "attempts": 1, "cancel_requested": false, "created_at": "2025-08-18T05:33:20Z",
            "updated_at": "2025-08-18T05:33:20Z", "progress": {"current": 0, "total": null, "unit": "items"},
            "workspace_bucket": "lab-data", "workspace_mode": "existing", "locally_exhausted": false
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "Another node runs this session; code `session_not_here`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn end_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    session.end(EndReason::Ended);
    Ok((StatusCode::ACCEPTED, Json(job_status_response(&record))).into_response())
}

#[utoipa::path(
    post,
    path = "/compute/jobs/{job_id}/session/inputs",
    tag = "compute/sessions",
    summary = "Stage objects into the session's workspace bucket",
    description = r#"Copies objects into the workspace bucket the session works inside, so the kernel can open them over S3.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel, and each source additionally needs the caller's read permission on its bucket.

**Behavior**
- A source stored on this node is copied server side, which deduplicates onto the stored blob
  instead of moving bytes, and is answered in `staged`.
- A source that is itself a reference to a connector is pulled by a background copy job and
  answered in `pending` with the job id. `GET /compute/jobs/{job_id}` reports its progress in bytes;
  the object appears under `dest_key` once the job succeeded.
- With `strategy: reference` such a source is not pulled: the workspace gets a reference of its own
  and reads stream from the source. It is answered in `staged` as `linked` with an empty `blake3`.
- Nothing is copied into the container: the object lands in the bucket under `dest_key` and the
  kernel reads it from there.
- Every staged object is recorded in the session inventory and listed in the job report at the end,
  with the node, version and hash it came from. A queued copy joins the report once it finished.
- Staging resets the session's idle timer.
- Destination keys are checked before anything is copied. Once one object landed or was queued the
  answer stays 202 and every item that failed after it is named in `failed`.

**Limits** (refused with 400)
- At most 64 items per call, and a `dest_key` that is relative and traversal-free.
- A source on another node: import it into a bucket of this node first."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    request_body(content = SessionInputsRequest, example = json!({
        "items": [{"bucket": "source-data", "key": "input.txt", "dest_key": "data/input.txt"}]
    })),
    responses(
        (status = 202, description = "The objects that landed or were queued, with every later failure in `failed`", body = SessionInputsResponse, example = json!({
            "staged": [{"dest_key": "data/input.txt", "bytes": 12, "blake3": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                "source_node_id": "node-1", "version_id": "01JJRSVERSION0123456789ABC", "linked": false}],
            "pending": [{"dest_key": "data/genomes/ref.fna", "job_id": "01JJCPYJB00123456789ABCDEF", "source_node_id": "node-1"}],
            "failed": []
        })),
        (status = 400, description = "An invalid destination key, too many items, or a source on another node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted, or the caller may not read a source", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session has ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn stage_inputs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Json(request): Json<SessionInputsRequest>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if request.items.is_empty() || request.items.len() > MAX_STAGED_ITEMS {
        return Err(ServerError::BadRequestMessage(format!(
            "a call stages 1 to {MAX_STAGED_ITEMS} objects"
        )));
    }
    let bucket = record.workspace_bucket.clone().ok_or_else(|| {
        ServerError::InternalError("a session has no workspace bucket".to_string())
    })?;
    let node_id = state.get_node_id().to_string();
    let mut items = Vec::with_capacity(request.items.len());
    for item in request.items {
        let dest_key = check_item(&item, &node_id)?;
        items.push((item, dest_key));
    }
    let mut staged = Vec::with_capacity(items.len());
    let mut pending = Vec::new();
    let mut failed = Vec::new();
    let mut refusal = None;
    for (item, dest_key) in items {
        match stage_one(&state, &auth, &bucket, item, &dest_key).await {
            Ok(InputOutcome::Staged(entry)) => {
                session.record_input(StagedInput {
                    dest_key: entry.dest_key.clone(),
                    bytes: entry.bytes,
                    blake3: entry.blake3.clone(),
                    source_node_id: entry.source_node_id.clone(),
                    version_id: entry.version_id.clone(),
                });
                staged.push(entry);
            }
            Ok(InputOutcome::Pending(entry)) => {
                session.record_pending(PendingInput {
                    dest_key: entry.dest_key.clone(),
                    job_id: entry.job_id.clone(),
                    source_node_id: entry.source_node_id.clone(),
                });
                pending.push(entry);
            }
            Err(error) => {
                failed.push(failed_input(dest_key, &error));
                refusal = refusal.or(Some(error));
            }
        }
    }
    // A call that neither staged nor queued anything made no progress, so it
    // must not keep the session alive.
    if !staged.is_empty() || !pending.is_empty() {
        session.touch();
    }
    inputs_outcome(staged, pending, failed, refusal)
}

/// Transport checks every item must pass before the first copy, so a refused
/// key never leaves part of a call staged.
fn check_item(item: &SessionInputRequest, node_id: &str) -> ServerResult<String> {
    let dest_key = item.dest_key.trim();
    if dest_key.is_empty()
        || dest_key.starts_with('/')
        || dest_key.split('/').any(|part| part == "..")
    {
        return Err(ServerError::BadRequestMessage(
            "a destination key is relative and carries no `..`".to_string(),
        ));
    }
    if let Some(source) = item.source_node_id.as_deref()
        && source != node_id
    {
        return Err(ServerError::BadRequestMessage(
            "a source on another node must be imported into a bucket of this node first"
                .to_string(),
        ));
    }
    Ok(dest_key.to_string())
}

/// A call that staged or queued something answers 202 and names what failed
/// after it. A call that did neither keeps the refusal its first item earned.
fn inputs_outcome(
    staged: Vec<StagedInputResponse>,
    pending: Vec<PendingInputResponse>,
    failed: Vec<FailedInputResponse>,
    refusal: Option<ServerError>,
) -> ServerResult<Response> {
    if staged.is_empty()
        && pending.is_empty()
        && let Some(error) = refusal
    {
        return Err(error);
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(SessionInputsResponse {
            staged,
            pending,
            failed,
        }),
    )
        .into_response())
}

fn failed_input(dest_key: String, error: &ServerError) -> FailedInputResponse {
    FailedInputResponse {
        dest_key,
        error: error.public_message(),
    }
}

/// The bucket a name resolves to here. Absence reads as 404 like every other
/// name the caller may not see.
async fn bucket_info(
    context: &aruna_operations::driver::DriverContext,
    bucket: &str,
) -> ServerResult<aruna_core::structs::BucketInfo> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), context).await {
        Ok(info) => Ok(info),
        Err(GetBucketInfoError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

/// What one item became: an object in the bucket, or a job still bringing it.
enum InputOutcome {
    Staged(StagedInputResponse),
    Pending(PendingInputResponse),
}

/// Brings one object into the workspace bucket. A source stored here is
/// deduplicated inline; one behind a connector is pulled by a job or linked.
async fn stage_one(
    state: &ServerState,
    auth: &AuthContext,
    bucket: &str,
    item: SessionInputRequest,
    dest_key: &str,
) -> ServerResult<InputOutcome> {
    let node_id = state.get_node_id();
    let context = state.get_ctx();
    let source_info = bucket_info(&context, &item.bucket).await?;
    let dest_info = bucket_info(&context, bucket).await?;
    ensure_permission(
        state,
        auth,
        blob_permission_path(state, source_info.group_id, &item.bucket, &item.key),
        Permission::READ,
    )
    .await?;
    let version_id = item
        .version_id
        .as_deref()
        .map(Ulid::from_string)
        .transpose()
        .map_err(|_| ServerError::BadRequestMessage("an unreadable version id".to_string()))?;
    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: item.bucket.clone(),
            key: item.key.clone(),
            version_id,
        }),
        &context,
    )
    .await
    .map_err(head_error)?;
    // A stored source deduplicates inline whatever was asked. A reference is
    // either queued for its pull or kept as a reference, which moves no byte.
    if head.location.is_none() && item.strategy == SessionInputStrategy::Snapshot {
        let spec = CopyJobSpec {
            auth_context: auth.clone(),
            node_id,
            source_bucket: item.bucket,
            source_key: item.key,
            source_version_id: version_id,
            source_group_id: source_info.group_id,
            dest_bucket: bucket.to_string(),
            dest_key: dest_key.to_string(),
            group_id: dest_info.group_id,
        };
        return queue_copy(state, spec, dest_key)
            .await
            .map(InputOutcome::Pending);
    }
    let realm_config = drive(GetRealmConfigOperation::new(state.get_realm_id()), &context)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let input = CopyObjectInput {
        source_bucket: item.bucket,
        source_key: item.key,
        source_version_id: version_id,
        source_group_id: source_info.group_id,
        source_auth_context: auth.clone(),
        dest_bucket: bucket.to_string(),
        dest_key: dest_key.to_string(),
        user_id: auth.user_id,
        group_id: dest_info.group_id,
        realm_id: state.get_realm_id(),
        node_id,
        quota_ceiling: realm_config
            .quota
            .effective_group_ceiling(&dest_info.group_id),
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: auth.path_restrictions.clone(),
        references: match item.strategy {
            SessionInputStrategy::Snapshot => CopyReferences::Materialize,
            SessionInputStrategy::Reference => CopyReferences::Preserve,
        },
    };
    copy_inline(state, auth, input, dest_key)
        .await
        .map(InputOutcome::Staged)
}

/// Copies one object into the workspace bucket: stored bytes deduplicate onto
/// their blob, a kept reference moves none at all.
async fn copy_inline(
    state: &ServerState,
    auth: &AuthContext,
    input: CopyObjectInput,
    dest_key: &str,
) -> ServerResult<StagedInputResponse> {
    let bucket = input.dest_bucket.clone();
    let result = copy_object(&state.get_ctx(), input)
        .await
        .map_err(|error| ServerError::BadRequestMessage(error.to_string()))?;
    let linked = result.location.is_none();
    if linked {
        // A kept reference writes no replication obligation of its own.
        queue_live_replication(
            state,
            auth.clone(),
            bucket,
            dest_key.to_string(),
            result.version_id,
            false,
        )
        .await;
    }
    Ok(StagedInputResponse {
        dest_key: dest_key.to_string(),
        bytes: result.size,
        blake3: result
            .location
            .as_ref()
            .and_then(|location| location.hashes.get(HASH_BLAKE3))
            .map(hex::encode)
            .unwrap_or_default(),
        source_node_id: state.get_node_id().to_string(),
        version_id: result
            .source_version_id
            .map(|version| version.to_string())
            .unwrap_or_default(),
        linked,
    })
}

/// Hands the copy to a job, since pulling a reference can outlive a request.
async fn queue_copy(
    state: &ServerState,
    spec: CopyJobSpec,
    dest_key: &str,
) -> ServerResult<PendingInputResponse> {
    let node_id = state.get_node_id();
    let result = submit_copy_job(
        &state.get_ctx(),
        spec,
        node_id,
        state.rocrate_limits().artifact_retention_ms,
    )
    .await
    .map_err(map_submit_error)?;
    Ok(PendingInputResponse {
        dest_key: dest_key.to_string(),
        job_id: result.job_id.to_string(),
        source_node_id: node_id.to_string(),
    })
}

/// A missing object reads as 404 like a missing bucket; anything else is the
/// node's own failure.
fn head_error(error: HeadObjectError) -> ServerError {
    match error {
        HeadObjectError::NoSuchKey
        | HeadObjectError::NoSuchVersion
        | HeadObjectError::DeleteMarker => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/compute/jobs/{job_id}/session/scratch",
    tag = "compute/sessions",
    summary = "List the session's scratch directory",
    description = r#"Lists one directory of the session container's working directory through the session helper.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- The path is relative to the working directory. A leading `/` or any `..` segment is refused.
- The scratch directory is not durable. Results that must be kept belong in the workspace bucket."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID"),
        ScratchQuery
    ),
    responses(
        (status = 200, description = "The directory listing", body = ScratchListResponse, example = json!({
            "path": "", "entries": [{"name": "result.txt", "kind": "file", "bytes": 42, "modified_ms": 1755500000000_u64}]
        })),
        (status = 400, description = "A path that leaves the working directory", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session has ended", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_scratch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Query(query): Query<ScratchQuery>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    session.touch();
    match session
        .list_scratch(query.path.as_deref().unwrap_or_default())
        .await
    {
        Ok(body) => Ok(Json(serde_json::Value::Object(body)).into_response()),
        Err(error) => Ok(session_error(error)),
    }
}

#[utoipa::path(
    get,
    path = "/compute/jobs/{job_id}/session/scratch/read",
    tag = "compute/sessions",
    summary = "Read one scratch file of the session",
    description = r#"Returns the bytes of one file in the session container's working directory.

**Authentication**: realm bearer token; a path-restricted (delegated) token is refused. Self-scoped
like cancel.

**Behavior**
- The path is relative to the working directory. A leading `/` or any `..` segment is refused.
- At most 8 MiB is returned; a larger file answers 413. Larger results belong in the workspace
  bucket.
- The content type is guessed from the file name."#,
    params(
        ("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID"),
        ScratchReadQuery
    ),
    responses(
        (status = 200, description = "The file bytes", content_type = "application/octet-stream"),
        (status = 400, description = "A path that leaves the working directory", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session has ended", body = ErrorResponse),
        (status = 413, description = "The file is larger than 8 MiB", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn read_scratch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Query(query): Query<ScratchReadQuery>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let (record, physical_job_id) = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record, physical_job_id, false).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    session.touch();
    let body = match session
        .read_scratch(&query.path, 0, MAX_SCRATCH_READ_BYTES)
        .await
    {
        Ok(body) => body,
        Err(error) => return Ok(session_error(error)),
    };
    if body.get("truncated").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err(ServerError::PayloadTooLarge(
            "a scratch read returns at most 8 MiB".to_string(),
        ));
    }
    let encoded = body
        .get("base64")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ServerError::InternalError("session read carries no bytes".to_string()))?;
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
        .map_err(|_| ServerError::InternalError("session read is not decodable".to_string()))?;
    let content_type = key_content_type(&query.path);
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response())
}

#[cfg(test)]
mod tests;
