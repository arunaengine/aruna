//! Serves the runs of a plugin install: start, list, read and resolve runs, and show how its
//! events are delivered. Run data lives on the install's receiver, which answers each call.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{PluginRun, RunTrigger};
use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord, PluginAudit};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_operations::plugins::decide::event_delivery;
use aruna_operations::plugins::dispatch::{StartError, start_run};
use aruna_operations::plugins::install::{InstallError, pick_version, read_config, read_manifest};
use aruna_operations::plugins::matcher::undelivered;
use aruna_operations::plugins::receiver::lost_reason;
use aruna_operations::plugins::runs::{RunError, list_runs, read_run, resolve_run};
use aruna_operations::users::service_account::group_admin_path;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError};
use crate::routes::management_relay::relay_to_node;
use crate::routes::plugin_installs::parse_id;
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
        .routes(routes!(delivery))
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
    /// `manual`, `timer`, `event` or `webhook`.
    pub trigger: String,
    /// Who started a manual run.
    pub started_by: Option<String>,
    /// A manual run's JSON input.
    pub input: Option<String>,
    /// When a timer occurrence was due.
    pub occurrence_ms: Option<u64>,
    /// The source event of an event run.
    pub event: Option<RunEvent>,
    /// The delivery id of a webhook run, when the manifest names its header.
    pub delivery_id: Option<String>,
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

/// The source event an event run was started for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunEvent {
    /// `dataset_created`, `dataset_updated` or `data_uploaded`.
    pub kind: String,
    /// The policy path the install's grants are checked on.
    pub path: String,
    /// The document id or object path the guest receives.
    pub resource: String,
    /// The document event or object version.
    pub revision: String,
    /// The event's identity, the same on every node that journaled it.
    pub source: String,
    /// The account whose change the event reports, for metadata events.
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RunPage {
    pub runs: Vec<RunView>,
    /// Pass as `cursor` for the next page; absent on the last page.
    pub next: Option<String>,
}

fn view(run: PluginRun) -> RunView {
    let delivery_id = match &run.trigger {
        RunTrigger::Webhook(hook) => hook.delivery_id.clone(),
        _ => None,
    };
    let (trigger, started_by, input, occurrence_ms, event) = match run.trigger {
        RunTrigger::Manual { by, input } => {
            ("manual", Some(by.to_string()), Some(input), None, None)
        }
        RunTrigger::Timer { occurrence_ms } => ("timer", None, None, Some(occurrence_ms), None),
        RunTrigger::Event(event) => {
            let event = RunEvent {
                kind: event.kind.name().into(),
                path: event.path,
                resource: event.resource,
                revision: event.revision,
                source: event.source,
                actor: event.actor.map(|actor| actor.to_string()),
            };
            ("event", None, None, None, Some(event))
        }
        RunTrigger::Webhook(_) => ("webhook", None, None, None, None),
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
        event,
        delivery_id,
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

/// The install a run call is about, after the caller proved admin of its group.
struct Served {
    auth: AuthContext,
    install_id: Ulid,
    /// `None` once the install was removed; its runs stay on the node that ran them.
    install: Option<PluginInstall>,
}

/// Authorizes the caller as admin of the install's group, then answers 503 while the receiver
/// is lost, relays to the receiver, or serves here. The runs of a removed install stay
/// readable and resolvable on the node that ran them.
async fn serve(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    id: &str,
    run: Option<&str>,
    call: Call,
) -> Result<Served, Response> {
    let failed = IntoResponse::into_response;
    let auth = require_realm_auth(state, auth).map_err(failed)?;
    let install_id = parse_id(id).map_err(failed)?;
    let config = read_config(&state.get_ctx(), state.get_realm_id()).await;
    let config = config.map_err(|error| {
        ServerError::ServiceUnavailableReason(error.to_string()).into_response()
    })?;
    let Some(install) = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .cloned()
    else {
        let group_id = kept_group(state, install_id, run).await?;
        ensure_admin(state, &auth, group_id).await?;
        return Ok(Served {
            auth,
            install_id,
            install: None,
        });
    };
    ensure_admin(state, &auth, install.group_id).await?;
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
    Ok(Served {
        auth,
        install_id,
        install: Some(install),
    })
}

/// The group of a removed install whose runs this node keeps, from the named run or any.
async fn kept_group(
    state: &ServerState,
    install_id: Ulid,
    run: Option<&str>,
) -> Result<GroupId, Response> {
    let ctx = state.get_ctx();
    let kept = match run.map(parse_id) {
        Some(Ok(run_id)) => read_run(&ctx, install_id, run_id).await.ok(),
        Some(Err(error)) => return Err(error.into_response()),
        None => (list_runs(&ctx, install_id, None, 1).await.ok())
            .and_then(|(runs, _)| runs.into_iter().next()),
    };
    kept.map(|run| run.group_id)
        .ok_or_else(|| ServerError::NotFound.into_response())
}

async fn ensure_admin(
    state: &ServerState,
    auth: &AuthContext,
    group_id: GroupId,
) -> Result<(), Response> {
    let path = group_admin_path(state.get_realm_id(), group_id);
    (ensure_permission(state, auth, path, Permission::WRITE).await)
        .map_err(IntoResponse::into_response)
}

fn lost(receiver: NodeId, reason: &str) -> Response {
    let message = format!("the install is blocked: {reason} (node {receiver})");
    ServerError::ServiceUnavailableReason(message).into_response()
}

fn run_error(error: RunError) -> Response {
    match error {
        RunError::NotFound => ServerError::NotFound.into_response(),
        RunError::NotWaiting(_) => ServerError::Conflict(error.to_string()).into_response(),
        RunError::Full(_) => {
            let full = StatusCode::TOO_MANY_REQUESTS;
            let mut refused =
                ServerError::Refused(full, "runs_queued", error.to_string()).into_response();
            let wait = HeaderValue::from(QUEUE_RETRY_SECS);
            refused.headers_mut().insert(header::RETRY_AFTER, wait);
            refused
        }
        RunError::Store(_) => {
            ServerError::ServiceUnavailableReason(error.to_string()).into_response()
        }
    }
}

/// A run just queued by hand.
fn queued_example() -> Value {
    let mut run = run_example(false);
    run["state"] = json!("queued");
    run["attempts"] = json!(0);
    run["finished_ms"] = Value::Null;
    run["output"] = Value::Null;
    run
}

/// A run a person resolved after it needed attention.
fn resolved_example() -> Value {
    let mut run = run_example(false);
    run["message"] = json!("01JBX7Y3S5M3Q8Y1V9K2T4R6P3@realm resolved it as done");
    run["output"] = Value::Null;
    run
}

/// Seconds a caller waits before starting a run again after the queue was full; runs leave the
/// queue as they finish or are resolved.
const QUEUE_RETRY_SECS: u32 = 60;

/// One run as the run routes answer it.
fn run_example(log: bool) -> Value {
    let log = match log {
        true => {
            json!([{"at_ms": 1762700001200u64, "level": "info", "message": "posted the dataset to the catalog"}])
        }
        false => json!([]),
    };
    json!({
        "id": "01JBX7Y3S5M3Q8Y1V9K2T4R6Q1",
        "install": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1",
        "state": "succeeded",
        "handler": "on_manual",
        "trigger": "manual",
        "started_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P3@realm",
        "input": "{\"dataset\":\"01JBX7Y3S5M3Q8Y1V9K2T4R6P4\"}",
        "occurrence_ms": null,
        "event": null,
        "delivery_id": null,
        "revision": 2,
        "attempts": 1,
        "due_ms": 1762700000000u64,
        "created_ms": 1762700000000u64,
        "finished_ms": 1762700001500u64,
        "message": null,
        "output": "c-1",
        "log": log
    })
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
  the receiver and the reason.
- After the install is removed, the node that ran it still lists, reads and resolves its runs."#,
    params(("id" = String, Path, description = "The install id")),
    request_body(content = StartBody, example = json!({"input": {"dataset": "01JBX7Y3S5M3Q8Y1V9K2T4R6P4"}})),
    responses(
        (status = 201, description = "The queued run", body = RunView, example = json!(queued_example())),
        (status = 400, description = "The input is not JSON of at most 64 KiB", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install is not active or has no manual trigger", body = ErrorResponse),
        (status = 429, description = "The install has its limit of unfinished and unresolved runs", body = ErrorResponse,
            headers(("Retry-After" = u32, description = "Seconds before starting a run again"))),
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
    let (auth, install) = match serve(&state, auth, &id, None, call).await {
        Ok(Served {
            auth,
            install: Some(install),
            ..
        }) => (auth, install),
        Ok(_) => return ServerError::NotFound.into_response(),
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
  the receiver and the reason.
- After the install is removed, the node that ran it still lists, reads and resolves its runs."#,
    params(("id" = String, Path, description = "The install id"), RunListQuery),
    responses(
        (status = 200, description = "One page of runs", body = RunPage, example = json!({"runs": [run_example(false)], "next": null})),
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
    let install_id = match serve(&state, auth, &id, None, call).await {
        Ok(served) => served.install_id,
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
    match list_runs(&state.get_ctx(), install_id, after, limit).await {
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
  the receiver and the reason.
- After the install is removed, the node that ran it still lists, reads and resolves its runs."#,
    params(
        ("id" = String, Path, description = "The install id"),
        ("run" = String, Path, description = "The run id")
    ),
    responses(
        (status = 200, description = "The run", body = RunView, example = json!(run_example(true))),
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
    let install_id = match serve(&state, auth, &id, Some(&run), call).await {
        Ok(served) => served.install_id,
        Err(response) => return response,
    };
    let run_id = match parse_id(&run) {
        Ok(run_id) => run_id,
        Err(error) => return error.into_response(),
    };
    match read_run(&state.get_ctx(), install_id, run_id).await {
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
  the receiver and the reason.
- After the install is removed, the node that ran it still lists, reads and resolves its runs."#,
    params(
        ("id" = String, Path, description = "The install id"),
        ("run" = String, Path, description = "The run id")
    ),
    request_body(content = ResolveBody, example = json!({"done": true})),
    responses(
        (status = 200, description = "The resolved run", body = RunView, example = json!(resolved_example())),
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
    let served = match serve(&state, auth, &id, Some(&run), call).await {
        Ok(served) => served,
        Err(response) => return response,
    };
    let (Ok(parsed), Ok(run_id)) = (parsed, parse_id(&run)) else {
        return ServerError::BadRequestReason("the body or run id is not valid".into())
            .into_response();
    };
    let ctx = state.get_ctx();
    let kept = match read_run(&ctx, served.install_id, run_id).await {
        Ok(kept) => kept,
        Err(error) => return run_error(error),
    };
    let actor = served.auth.user_id;
    let target = served.install.map_or(actor, |install| install.account);
    let audit = AdminAuditRecord {
        group_id: Some(kept.group_id),
        plugin: Some(PluginAudit {
            plugin: kept.plugin,
            version: kept.version,
            install_id: served.install_id,
        }),
        ..AdminAuditRecord::new(AdminAuditAction::PluginRunResolved, actor, target)
    };
    match resolve_run(&ctx, served.install_id, run_id, parsed.done, &audit).await {
        Ok(run) => Json(view(run)).into_response(),
        Err(error) => run_error(error),
    }
}

/// Undelivered events of one kind and reason, summed over the nodes that counted them.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GapView {
    /// `dataset_created`, `dataset_updated` or `data_uploaded`.
    pub kind: String,
    /// `full`, `pruned`, `expired`, `waited` or `late`.
    pub reason: String,
    pub count: u64,
    /// Whether a range of events of unknown size was lost as well; `count` is a lower bound.
    pub unknown: bool,
    /// Origin times of the earliest and the latest event counted.
    pub earliest_ms: u64,
    pub latest_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeliveryView {
    /// Events waiting on the receiver for the install record or the account's writes.
    pub waiting: usize,
    /// How long the oldest waiting event has waited, in milliseconds.
    pub oldest_age_ms: Option<u64>,
    /// Events that were not delivered and will not run, by kind and reason.
    pub gaps: Vec<GapView>,
    /// Declared triggers whose events are not delivered yet.
    pub undelivered: Vec<String>,
    /// Why the receiver's last replay started at its earliest kept event. Only the receiver's
    /// own replay is covered; other nodes do not report theirs.
    pub note: Option<String>,
}

#[utoipa::path(
    get,
    path = "/plugin-installs/{id}/delivery",
    tag = "plugins",
    summary = "Show plugin event delivery",
    description = r#"Shows the install's waiting events and the events that were not delivered.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- The install's receiver keeps the waiting events and the gap counts every node reports to it;
  every other node relays the call there. Totals are summed over the reporting nodes.
- While the receiver is drained, gone or no longer a server, the call answers 503 and names
  the receiver and the reason.
- Sync triggers are listed in `undelivered`: they are not delivered yet.
- `note` covers only the receiver's own replay; replays on other nodes are not reported."#,
    params(("id" = String, Path, description = "The install id")),
    responses(
        (status = 200, description = "The install's event delivery", body = DeliveryView, example = json!({"waiting": 1, "oldest_age_ms": 4200, "gaps": [{"kind": "dataset_updated", "reason": "full", "count": 3, "unknown": false, "earliest_ms": 1762700000000u64, "latest_ms": 1762700060000u64}], "undelivered": ["sync_completed"], "note": null})),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 503, description = "The install's receiver is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delivery(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
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
    let install = match serve(&state, auth, &id, None, call).await {
        Ok(Served {
            install: Some(install),
            ..
        }) => install,
        Ok(_) => return ServerError::NotFound.into_response(),
        Err(response) => return response,
    };
    let ctx = state.get_ctx();
    let config = read_config(&ctx, state.get_realm_id()).await.ok();
    let manifest = (config.as_ref())
        .and_then(|config| pick_version(config, &install.plugin, Some(&install.digest)).ok())
        .and_then(|plugin| read_manifest(&plugin).ok());
    let undelivered = manifest.as_ref().map(undelivered).unwrap_or_default();
    let delivery = match event_delivery(&ctx, install.install_id).await {
        Ok(delivery) => delivery,
        Err(error) => {
            return ServerError::ServiceUnavailableReason(error.to_string()).into_response();
        }
    };
    let now = unix_timestamp_millis();
    let gaps = (delivery.gaps.into_iter())
        .map(|row| GapView {
            kind: row.kind.name().to_string(),
            reason: row.reason.as_str().to_string(),
            count: row.count,
            unknown: row.unknown,
            earliest_ms: row.earliest_ms,
            latest_ms: row.latest_ms,
        })
        .collect();
    Json(DeliveryView {
        waiting: delivery.waiting,
        oldest_age_ms: delivery.oldest_ms.map(|at| now.saturating_sub(at)),
        gaps,
        undelivered: undelivered.into_iter().map(str::to_string).collect(),
        note: delivery.note,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    //! The runs of a removed install stay readable and resolvable for the group's admins.
    use super::*;
    use crate::tests::routes::{
        seed_group_docs, seed_realm_auth, seed_realm_config, test_context, test_state,
        test_storage, write_doc,
    };
    use aruna_core::UserId;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::plugin_installs::InstallState;
    use aruna_core::plugin_runs::RunState;
    use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
    use aruna_core::structs::identity::realm::RealmConfigDocument;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::plugins::runs::{admit, save_run};

    fn waiting_run(install_id: Ulid, group_id: GroupId) -> PluginRun {
        PluginRun {
            run_id: Ulid::from_parts(5, 5),
            install_id,
            group_id,
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            revision: 1,
            digest: format!("sha256:{}", "ab".repeat(32)),
            handler: "on_timer".into(),
            settings: "{}".into(),
            trigger: RunTrigger::Timer { occurrence_ms: 1 },
            state: RunState::Queued,
            attempts: 1,
            due_ms: 1,
            created_ms: 1,
            finished_ms: None,
            message: None,
            output: None,
            log: Vec::new(),
        }
    }

    #[test]
    fn full_queue_waits() {
        let refused = run_error(RunError::Full(100));
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        let wait = refused.headers().get(header::RETRY_AFTER);
        assert_eq!(wait.and_then(|value| value.to_str().ok()), Some("60"));
    }

    #[tokio::test]
    async fn removed_install_runs() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let ctx = Arc::new(test_context(storage));
        let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
        let state = Arc::new(test_state(ctx.clone(), realm_id, node_id, capabilities).await);
        let admin = UserId::new(Ulid::from_bytes([3u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: admin,
            realm_id,
        };
        seed_realm_config(&ctx, realm_id, &actor).await;
        seed_realm_auth(&ctx, realm_id, &actor).await;
        let group_id = Ulid::from_bytes([4u8; 16]);
        seed_group_docs(&ctx, realm_id, &actor, group_id, "lab", admin).await;
        // The install is gone from the realm config; its run waits for a person on this node.
        let install_id = Ulid::from_parts(7, 7);
        let mut run = waiting_run(install_id, group_id);
        admit(&ctx, &run).await.unwrap();
        run.finish(
            RunState::NeedsAttention,
            Some("call 0 is unknown".into()),
            2,
        );
        save_run(&ctx, &run).await.unwrap();

        let auth = |user: UserId| AuthContext {
            user_id: user,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let path = format!(
            "/api/v1/plugin-installs/{install_id}/runs/{}/resolve",
            run.run_id
        );
        let resolve_as = |user: UserId| {
            let state = state.clone();
            let path = path.clone();
            async move {
                resolve(
                    State(state),
                    Extension(Some(auth(user))),
                    Path((install_id.to_string(), run.run_id.to_string())),
                    OriginalUri(path.parse().unwrap()),
                    Method::POST,
                    HeaderMap::new(),
                    Bytes::from_static(br#"{"done":true}"#),
                )
                .await
                .status()
            }
        };
        let stranger = UserId::new(Ulid::from_bytes([8u8; 16]), realm_id);
        assert_eq!(resolve_as(stranger).await, StatusCode::FORBIDDEN);
        assert_eq!(resolve_as(admin).await, StatusCode::OK);
        let resolved = read_run(&ctx, install_id, run.run_id).await.unwrap();
        assert_eq!(resolved.state, RunState::Succeeded);
        assert_eq!(resolve_as(admin).await, StatusCode::CONFLICT);
        let missing = Ulid::from_parts(9, 9);
        let unknown = get_run(
            State(state.clone()),
            Extension(Some(auth(admin))),
            Path((missing.to_string(), run.run_id.to_string())),
            OriginalUri(path.parse().unwrap()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delivery_needs_receiver() {
        let (_dir, storage) = test_storage();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let ctx = Arc::new(test_context(storage));
        let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
        let state = Arc::new(test_state(ctx.clone(), realm_id, node_id, capabilities).await);
        let admin = UserId::new(Ulid::from_bytes([3u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: admin,
            realm_id,
        };
        seed_realm_auth(&ctx, realm_id, &actor).await;
        let group_id = Ulid::from_bytes([4u8; 16]);
        seed_group_docs(&ctx, realm_id, &actor, group_id, "lab", admin).await;
        // The install's receiver is not a node of the realm.
        let install_id = Ulid::from_parts(7, 7);
        let digest = format!("sha256:{}", "ab".repeat(32));
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.installs.push(PluginInstall {
            install_id,
            group_id,
            plugin: "announcer".into(),
            version: "0.1.0".into(),
            digest: digest.clone(),
            state: InstallState::Active,
            paused: false,
            settings: "{}".into(),
            grants: Default::default(),
            account: UserId::new(install_id, realm_id),
            role_id: install_id,
            receiver: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
            granted_by: admin,
            request: digest,
            revision: 1,
            active_since_ms: Some(1),
            failed: None,
            prerequisites: Vec::new(),
            activated_by: Some(node_id),
            widened: None,
            secrets: Default::default(),
        });
        let row = config.to_bytes(&actor).unwrap();
        write_doc(
            &ctx,
            REALM_CONFIG_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            row.into(),
        )
        .await;
        let auth = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let path = format!("/api/v1/plugin-installs/{install_id}/delivery");
        let answer = delivery(
            State(state),
            Extension(Some(auth)),
            Path(install_id.to_string()),
            OriginalUri(path.parse().unwrap()),
            Method::GET,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(answer.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
