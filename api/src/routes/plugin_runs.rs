//! Serves the runs of a plugin install: start one by hand, list and read runs, and resolve a
//! run that needs attention. Run data lives on the install's receiver, which answers each call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{PluginRun, RunTrigger};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::plugins::dispatch::{StartError, start_run};
use aruna_operations::plugins::install::{InstallError, read_config};
use aruna_operations::plugins::receiver::lost_reason;
use aruna_operations::plugins::runs::{RunError, list_runs, read_run, resolve_run};
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ErrorResponse, ServerError};
use crate::routes::management_relay::relay_to_node;
use crate::routes::plugin_installs::{admin_call, parse_id};
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

#[derive(OpenApi)]
pub struct RunsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(RunsApiDoc::openapi())
        .routes(routes!(start, list))
        .routes(routes!(get_run))
        .routes(routes!(resolve))
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct StartBody {
    /// The manual trigger's input; `{}` when absent.
    #[schema(value_type = Object)]
    pub input: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct ResolveBody {
    /// Whether the run's effects happened; it ends `succeeded` when true, else `failed`.
    pub done: bool,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct RunListQuery {
    /// The id of the last run of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; 100 by default.
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunLogLine {
    pub at_ms: u64,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunView {
    pub id: String,
    pub install: String,
    /// `queued`, `running`, `succeeded`, `failed`, `needs-attention` or `cancelled`.
    pub state: String,
    pub handler: String,
    /// `manual` or `timer`.
    pub trigger: String,
    /// Who started a manual run.
    pub started_by: Option<String>,
    /// A manual run's JSON input.
    pub input: Option<String>,
    /// When a timer occurrence was due.
    pub occurrence_ms: Option<u64>,
    /// The install revision the run was admitted with.
    pub revision: u64,
    pub attempts: u32,
    pub due_ms: u64,
    pub created_ms: u64,
    pub finished_ms: Option<u64>,
    pub message: Option<String>,
    pub output: Option<String>,
    /// The guest's log lines; listings leave them out.
    pub log: Vec<RunLogLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunPage {
    pub runs: Vec<RunView>,
    /// Pass as `cursor` for the next page; absent on the last page.
    pub next: Option<String>,
}

fn view(run: PluginRun) -> RunView {
    let (trigger, started_by, input, occurrence_ms) = match run.trigger {
        RunTrigger::Manual { by, input } => ("manual", Some(by.to_string()), Some(input), None),
        RunTrigger::Timer { occurrence_ms } => ("timer", None, None, Some(occurrence_ms)),
    };
    RunView {
        id: run.run_id.to_string(),
        install: run.install_id.to_string(),
        state: run.state.as_str().to_string(),
        handler: run.handler,
        trigger: trigger.to_string(),
        started_by,
        input,
        occurrence_ms,
        revision: run.revision,
        attempts: run.attempts,
        due_ms: run.due_ms,
        created_ms: run.created_ms,
        finished_ms: run.finished_ms,
        message: run.message,
        output: run.output,
        log: (run.log.into_iter())
            .map(|line| RunLogLine {
                at_ms: line.at_ms,
                level: line.level,
                message: line.message,
            })
            .collect(),
    }
}

/// One run call as it arrived, so it can be relayed to the receiver unchanged.
struct Call {
    method: Method,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Bytes,
}

/// Authorizes the caller as admin of the install's group, then answers 503 while the receiver
/// is lost, relays to the receiver, or returns the install this node serves.
async fn serve(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    id: &str,
    call: Call,
) -> Result<(AuthContext, PluginInstall), Response> {
    let (auth, install_id) = admin_call(state, auth, id)
        .await
        .map_err(IntoResponse::into_response)?;
    let config = read_config(&state.get_ctx(), state.get_realm_id()).await;
    let config = config.map_err(|error| {
        ServerError::ServiceUnavailableReason(error.to_string()).into_response()
    })?;
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .cloned()
        .ok_or_else(|| ServerError::NotFound.into_response())?;
    if let Some(reason) = lost_reason(&config, install.receiver) {
        return Err(lost(install.receiver, reason));
    }
    if install.receiver != state.get_node_id() {
        let relayed = relay_to_node(
            state,
            install.receiver,
            call.method,
            &call.uri,
            &call.headers,
            call.body,
        );
        return Err(relayed.await);
    }
    Ok((auth, install))
}

fn lost(receiver: NodeId, reason: &str) -> Response {
    let message = format!("the install is blocked: {reason} (node {receiver})");
    ServerError::ServiceUnavailableReason(message).into_response()
}

fn run_error(error: RunError) -> Response {
    match error {
        RunError::NotFound => ServerError::NotFound,
        RunError::NotWaiting(_) => ServerError::Conflict(error.to_string()),
        RunError::Full(_) => ServerError::Refused(
            StatusCode::TOO_MANY_REQUESTS,
            "runs_queued",
            error.to_string(),
        ),
        RunError::Store(_) => ServerError::ServiceUnavailableReason(error.to_string()),
    }
    .into_response()
}

fn start_error(error: StartError) -> Response {
    let message = error.to_string();
    match error {
        StartError::Install(InstallError::NotFound) => ServerError::NotFound.into_response(),
        StartError::Install(InstallError::Unauthorized) => ServerError::Forbidden.into_response(),
        StartError::Install(InstallError::State(_)) | StartError::NoTrigger(_) => {
            ServerError::Conflict(message).into_response()
        }
        StartError::Input => ServerError::BadRequestReason(message).into_response(),
        StartError::Lost { receiver, reason } => lost(receiver, reason),
        StartError::Run(error) => run_error(error),
        StartError::Install(_) | StartError::Elsewhere(_) => {
            ServerError::ServiceUnavailableReason(message).into_response()
        }
    }
}

#[utoipa::path(
    post,
    path = "/plugin-installs/{id}/runs",
    tag = "plugins",
    summary = "Start a plugin run",
    description = r#"Queues a run of the install's manual trigger with a JSON input.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- The install's receiver keeps its runs and answers; every other node relays the call there.
- While the receiver is drained, gone or no longer a server, the call answers 503 and names
  the receiver and the reason."#,
    params(("id" = String, Path, description = "The install id")),
    request_body = StartBody,
    responses(
        (status = 201, description = "The queued run", body = RunView),
        (status = 400, description = "The input is not JSON of at most 64 KiB", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install is not active or has no manual trigger", body = ErrorResponse),
        (status = 429, description = "The install has its limit of unfinished runs", body = ErrorResponse),
        (status = 503, description = "The install's receiver is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let parsed = match body.is_empty() {
        true => Ok(StartBody::default()),
        false => serde_json::from_slice::<StartBody>(&body),
    };
    let call = Call {
        method,
        uri,
        headers,
        body,
    };
    let (auth, install) = match serve(&state, auth, &id, call).await {
        Ok(served) => served,
        Err(response) => return response,
    };
    let Ok(parsed) = parsed else {
        return ServerError::BadRequestReason("the body is not a run request".into())
            .into_response();
    };
    let input = parsed
        .input
        .unwrap_or_else(|| Value::Object(Default::default()))
        .to_string();
    let (realm, local, ctx) = (state.get_realm_id(), state.get_node_id(), state.get_ctx());
    let started = start_run(
        &ctx,
        &auth,
        realm,
        local,
        install.install_id,
        &input,
        unix_timestamp_millis(),
    );
    match started.await {
        Ok(run) => (StatusCode::CREATED, Json(view(run))).into_response(),
        Err(error) => start_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/plugin-installs/{id}/runs",
    tag = "plugins",
    summary = "List plugin runs",
    description = r#"Lists the install's runs, oldest first, without their log lines.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- The install's receiver keeps its runs and answers; every other node relays the call there.
- While the receiver is drained, gone or no longer a server, the call answers 503 and names
  the receiver and the reason."#,
    params(("id" = String, Path, description = "The install id"), RunListQuery),
    responses(
        (status = 200, description = "One page of runs", body = RunPage),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 503, description = "The install's receiver is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Query(query): Query<RunListQuery>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let call = Call {
        method,
        uri,
        headers,
        body: Bytes::new(),
    };
    let install = match serve(&state, auth, &id, call).await {
        Ok((_, install)) => install,
        Err(response) => return response,
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return ServerError::BadRequestReason("limit is 1 to 1000".into()).into_response();
    }
    let after = match query.cursor.as_deref().map(parse_id).transpose() {
        Ok(after) => after,
        Err(error) => return error.into_response(),
    };
    match list_runs(&state.get_ctx(), install.install_id, after, limit).await {
        Ok((runs, next)) => {
            let runs = (runs.into_iter())
                .map(|mut run| {
                    run.log.clear();
                    view(run)
                })
                .collect();
            let next = next.map(|id| id.to_string());
            Json(RunPage { runs, next }).into_response()
        }
        Err(error) => run_error(error),
    }
}

#[utoipa::path(
    get,
    path = "/plugin-installs/{id}/runs/{run}",
    tag = "plugins",
    summary = "Read a plugin run",
    description = r#"Reads one run with its outcome and log lines.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- The install's receiver keeps its runs and answers; every other node relays the call there.
- While the receiver is drained, gone or no longer a server, the call answers 503 and names
  the receiver and the reason."#,
    params(
        ("id" = String, Path, description = "The install id"),
        ("run" = String, Path, description = "The run id")
    ),
    responses(
        (status = 200, description = "The run", body = RunView),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install or run has this id", body = ErrorResponse),
        (status = 503, description = "The install's receiver is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_run(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, run)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let call = Call {
        method,
        uri,
        headers,
        body: Bytes::new(),
    };
    let install = match serve(&state, auth, &id, call).await {
        Ok((_, install)) => install,
        Err(response) => return response,
    };
    let run_id = match parse_id(&run) {
        Ok(run_id) => run_id,
        Err(error) => return error.into_response(),
    };
    match read_run(&state.get_ctx(), install.install_id, run_id).await {
        Ok(run) => Json(view(run)).into_response(),
        Err(error) => run_error(error),
    }
}

#[utoipa::path(
    post,
    path = "/plugin-installs/{id}/runs/{run}/resolve",
    tag = "plugins",
    summary = "Resolve a plugin run",
    description = r#"Ends a run that needs attention as done or not done, and audits the decision.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- The install's receiver keeps its runs and answers; every other node relays the call there.
- While the receiver is drained, gone or no longer a server, the call answers 503 and names
  the receiver and the reason."#,
    params(
        ("id" = String, Path, description = "The install id"),
        ("run" = String, Path, description = "The run id")
    ),
    request_body = ResolveBody,
    responses(
        (status = 200, description = "The resolved run", body = RunView),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install or run has this id", body = ErrorResponse),
        (status = 409, description = "The run does not need attention", body = ErrorResponse),
        (status = 503, description = "The install's receiver is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resolve(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, run)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let parsed = serde_json::from_slice::<ResolveBody>(&body);
    let call = Call {
        method,
        uri,
        headers,
        body,
    };
    let (auth, install) = match serve(&state, auth, &id, call).await {
        Ok(served) => served,
        Err(response) => return response,
    };
    let (Ok(parsed), Ok(run_id)) = (parsed, parse_id(&run)) else {
        return ServerError::BadRequestReason("the body or run id is not valid".into())
            .into_response();
    };
    let action = AdminAuditAction::PluginRunResolved;
    let audit = AdminAuditRecord::for_install(action, auth.user_id, &install);
    match resolve_run(
        &state.get_ctx(),
        install.install_id,
        run_id,
        parsed.done,
        &audit,
    )
    .await
    {
        Ok(run) => Json(view(run)).into_response(),
        Err(error) => run_error(error),
    }
}
