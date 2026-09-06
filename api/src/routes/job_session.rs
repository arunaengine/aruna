//! Interactive session routes. They are served by the node that runs the
//! session job, next to cancel and with the same authorization: only the
//! submitter, and anybody else's job answers 404.

use std::convert::Infallible;
use std::sync::Arc;

use aruna_compute::session::events::SessionEvent;
use aruna_compute::session::{EndReason, MAX_SCRATCH_READ_BYTES, Session, SessionError};
use aruna_core::structs::{AuthContext, JobPayload, JobRecord, JobState, key_content_type};
use aruna_operations::jobs::lifecycle::ids::session_of;
use aruna_operations::jobs::service::read_owned_job;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::jobs::{JobStatusResponse, coded_response, job_status_response, parse_job_id};
use crate::server_state::ServerState;

/// Envoy idles an upstream at 60 seconds, so the stream keeps itself alive.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

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

/// The caller's session job on this node. Absence and foreign ownership are
/// deliberately indistinguishable.
async fn owned_session_job(
    state: &ServerState,
    auth: &AuthContext,
    raw_job_id: &str,
) -> ServerResult<JobRecord> {
    let job_id = parse_job_id(raw_job_id)?;
    let record = read_owned_job(&state.get_ctx(), auth.user_id, job_id)
        .await
        .map_err(ServerError::InternalError)?
        .ok_or(ServerError::NotFound)?;
    let JobPayload::Execution(spec) = &record.payload else {
        return Err(ServerError::NotFound);
    };
    if session_of(spec).is_none() {
        return Err(ServerError::NotFound);
    }
    Ok(record)
}

/// The caller's live session on this node, for callers that have no coded
/// answer of their own. Absence reads as 404 like every other session route.
pub(crate) async fn caller_session(
    state: &ServerState,
    auth: &AuthContext,
    raw_job_id: &str,
) -> ServerResult<Arc<Session>> {
    let record = owned_session_job(state, auth, raw_job_id).await?;
    state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&record.job_id.to_string()))
        .ok_or(ServerError::NotFound)
}

/// The live session, or the coded answer that says why there is none here.
fn live_session(state: &ServerState, record: &JobRecord) -> Result<Arc<Session>, Response> {
    let job_id = record.job_id.to_string();
    let session = state
        .get_ctx()
        .compute_handle
        .as_ref()
        .and_then(|registry| registry.sessions().get(&job_id));
    match session {
        Some(session) => Ok(session),
        None if record.state.is_terminal() => Err(ended_response(record).into_response()),
        None if record.owner_node_id == state.get_node_id() => {
            Err(ended_response(record).into_response())
        }
        None => Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "the session runs on another node",
                "code": "session_not_here",
                "executor_node_id": record.owner_node_id.to_string(),
            })),
        )
            .into_response()),
    }
}

/// What a job whose session this node no longer holds looks like. A restart
/// ends every session, so the client is told rather than left waiting.
fn ended_response(record: &JobRecord) -> Json<SessionResponse> {
    let requested = match &record.payload {
        JobPayload::Execution(spec) => session_of(spec),
        _ => None,
    };
    let reason = match record.state {
        JobState::Cancelled => EndReason::Cancelled,
        state if state.is_terminal() => EndReason::Ended,
        _ => EndReason::NodeRestart,
    };
    Json(SessionResponse {
        job_id: record.job_id.to_string(),
        state: "ended".to_string(),
        runtime: requested.map(|session| session.runtime).unwrap_or_default(),
        workspace_bucket: record.workspace_bucket.clone().unwrap_or_default(),
        executor_node_id: record.owner_node_id.to_string(),
        started_at_ms: record.started_at_ms.unwrap_or(record.created_at_ms),
        idle_after_ms: 0,
        idle_deadline_ms: 0,
        credential_expires_at_ms: 0,
        last_event_id: 0,
        cells: Some(Vec::new()),
        ended: Some(SessionEndedResponse {
            reason: reason.as_str().to_string(),
        }),
    })
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
- A node restart ends every session, so a job whose session this node no longer holds reports
  `state` `ended` with reason `node_restart` rather than looking live."#,
    params(("job_id" = String, Path, description = "Job id as returned by submission: a 26-character ULID")),
    responses(
        (status = 200, description = "The session state", body = SessionResponse),
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    match live_session(&state, &record) {
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let after = query.after.or_else(|| last_event_id(&headers)).unwrap_or(0);
    let first = frame("session", 0, &session_response(&session, false));
    let (backlog, receiver) = match session.subscribe(after) {
        Ok(resumed) => resumed,
        Err(from) => {
            let gap = frame(
                "gap",
                0,
                &json!({ "from": after.saturating_add(1), "to": from }),
            );
            let (backlog, receiver) = session.subscribe_all();
            return Ok(sse(
                stream::iter(vec![first, gap])
                    .chain(stream::iter(backlog.into_iter().map(sse_event)))
                    .chain(live_stream(receiver)),
                &state,
            ));
        }
    };
    Ok(sse(
        stream::iter(vec![first])
            .chain(stream::iter(backlog.into_iter().map(sse_event)))
            .chain(live_stream(receiver)),
        &state,
    ))
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

fn live_stream(
    receiver: tokio::sync::broadcast::Receiver<SessionEvent>,
) -> impl Stream<Item = Event> + Send {
    stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(event) => Some((sse_event(event), receiver)),
            Err(RecvError::Lagged(count)) => Some((
                frame("gap", 0, &json!({ "from": 0, "to": count })),
                receiver,
            )),
            Err(RecvError::Closed) => None,
        }
    })
}

fn sse_event(event: SessionEvent) -> Event {
    Event::default()
        .id(event.id.to_string())
        .event(event.kind.as_str())
        .data(event.data)
}

fn frame<T: Serialize>(name: &str, id: u64, data: &T) -> Event {
    let event = Event::default()
        .event(name)
        .data(serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string()));
    if id > 0 {
        event.id(id.to_string())
    } else {
        event
    }
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
    request_body = SubmitCellRequest,
    responses(
        (status = 202, description = "The cell was queued", body = SubmitCellResponse),
        (status = 400, description = "An invalid cell id or oversized code", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No such session job, or it was submitted by somebody else", body = ErrorResponse),
        (status = 409, description = "The session is not ready, or that cell is busy", body = ErrorResponse),
        (status = 429, description = "Too many cells queued or submitted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_cell(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Json(request): Json<SubmitCellRequest>,
) -> ServerResult<Response> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
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
        (status = 202, description = "The session was ended", body = JobStatusResponse),
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    session.end(EndReason::Ended);
    Ok((StatusCode::ACCEPTED, Json(job_status_response(&record))).into_response())
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
        (status = 200, description = "The directory listing", body = ScratchListResponse),
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
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
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let record = owned_session_job(&state, &auth, &job_id).await?;
    let session = match live_session(&state, &record) {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
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
