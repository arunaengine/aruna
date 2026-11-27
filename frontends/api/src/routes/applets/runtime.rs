//! The `/_aruna/` runtime the edge relays for applet pages: the visitor's session, uploads,
//! form submissions and runs of the applet's granted workflow.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::record::{Access, Applet, Serving, guest_pseudonym, pseudonym};
use aruna_core::applet::usage::caps;
use aruna_core::git::StoredObject;
use aruna_ops_work::applets::backends::{SandboxOperation, public_ready};
use aruna_ops_work::applets::intake::{IntakeOperation, Visit, applet_auth};
use aruna_ops_work::applets::reads::LoadAppletOperation;
use aruna_ops_work::applets::runs::{RunInput, VisitRunOperation, run_request, visitor_run};
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_ops_work::workflows::runs::reads::RunSnapshotOperation;
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::{edge_auth, previewer, visitor};
use super::{applet_error, applet_id};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::jobs::rfc3339;
use crate::routes::git::git_node;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(session))
        .routes(routes!(upload))
        .routes(routes!(submit))
        .routes(routes!(start_run))
        .routes(routes!(run_status))
        .routes(routes!(run_results))
}

/// One caller through the edge: the applet as it serves now and who is visiting.
struct Caller {
    applet: Applet,
    visit: Visit,
    user_id: Option<String>,
    /// The visitor may see private versions, as a creator or approver does.
    previewer: bool,
}

/// The guest session id the edge sealed in its host-bound cookie, stored only as a salted hash.
fn guest_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-aruna-guest")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

/// Checks the edge key, what the applet serves and who may use it: a signed-in visitor by the
/// applet's access, else a guest where the node and the applet allow guests.
async fn caller(state: &ServerState, headers: &HeaderMap, id: &str) -> ServerResult<Caller> {
    let settings = edge_auth(state, headers)?.clone();
    let id = applet_id(id)?;
    let load = drive(LoadAppletOperation::new(id), &state.get_ctx()).await;
    let applet = load.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    let visitor = visitor(state, headers, id).await;
    let previewer = previewer(state, &applet, visitor.as_ref()).await;
    let serving = super::serving(state, &applet).await?;
    let allowed = match (serving, applet.access, &visitor) {
        (Serving::Live { .. }, Access::Public, _) => true,
        (Serving::Live { .. }, Access::GroupMembers, Some(visitor)) => {
            let check = AppletAccessOperation::new(visitor, id, Need::Read);
            drive(check, &state.get_ctx()).await.is_ok()
        }
        (Serving::Live { .. } | Serving::Preview { .. }, _, _) => previewer,
        (Serving::Gone, ..) => return Err(ServerError::NotFound),
        _ => {
            let reason = "the applet is paused".to_string();
            return Err(ServerError::ServiceUnavailableReason(reason));
        }
    };
    if !allowed {
        return Err(ServerError::Forbidden);
    }
    let salt = &applet.visitor_salt;
    let (visit, user_id) = match &visitor {
        Some(visitor) => {
            let pseudonym = pseudonym(salt, &visitor.user_id);
            let visit = Visit {
                pseudonym,
                guest: false,
            };
            (visit, Some(visitor.user_id.to_string()))
        }
        None if settings.guests && applet.guests.is_some() => {
            let guest = guest_id(headers).ok_or(ServerError::Unauthorized)?;
            let visit = Visit {
                pseudonym: guest_pseudonym(salt, &guest),
                guest: true,
            };
            (visit, None)
        }
        None => return Err(ServerError::Unauthorized),
    };
    Ok(Caller {
        applet,
        visit,
        user_id,
        previewer,
    })
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletCaps)]
pub struct CapsBody {
    pub hourly: Option<u32>,
    pub daily: Option<u32>,
    pub applet_daily: Option<u32>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletSession, example = json!({
    "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "pseudonym": "4f0c9d1e2b3a495867788990aabbccdd",
    "guest": false, "user_id": "01K8W1A0000000000000000000@realm", "previewer": false,
    "can_submit": true, "can_run": true, "upload_bytes": 104857600,
    "caps": {"hourly": 10, "daily": 50, "applet_daily": 1000}
}))]
pub struct SessionBody {
    pub applet_id: String,
    /// The visitor's folder name; it never reveals who the visitor is.
    pub pseudonym: String,
    pub guest: bool,
    /// The signed-in visitor's own id; never set for a guest.
    pub user_id: Option<String>,
    pub previewer: bool,
    /// Uploads and form submissions are taken.
    pub can_submit: bool,
    /// The granted workflow can be started.
    pub can_run: bool,
    /// The most bytes one upload may hold.
    pub upload_bytes: Option<u64>,
    pub caps: CapsBody,
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/session",
    tag = "applets",
    summary = "Describe the visitor of an applet page",
    description = r#"Answers who visits an applet page and what the page may do for them.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`, plus the visitor's sealed
token in `X-Aruna-Visitor`, or the edge's sealed guest session id in `X-Aruna-Guest` where guests
are allowed.

**Behavior**
- The pseudonym is a keyed hash of the account or guest id; the raw guest id is never stored.
- A group members applet needs a visitor who reads the group's data; a private applet and a
  preview need the creator or an approver."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    responses(
        (status = 200, description = "The visitor and what the page may do", body = SessionBody),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 403, description = "The visitor may not use this applet", body = ErrorResponse),
        (status = 404, description = "No such applet, or taken down", body = ErrorResponse),
        (status = 503, description = "The applet is paused", body = ErrorResponse)
    )
)]
pub async fn session(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> ServerResult<Json<SessionBody>> {
    let caller = caller(&state, &headers, &id).await?;
    let applet = &caller.applet;
    let account = applet_auth(applet, state.get_realm_id()).is_some();
    let can_submit = account && applet.grants.submissions.is_some();
    let caps = caps(applet, caller.visit.guest);
    let upload_bytes = match (&applet.guests, caller.visit.guest) {
        (Some(policy), true) => Some(policy.file_bytes),
        _ => applet.grants.limits.upload_bytes,
    };
    Ok(Json(SessionBody {
        applet_id: applet.applet_id.to_string(),
        pseudonym: caller.visit.pseudonym,
        guest: caller.visit.guest,
        user_id: caller.user_id,
        previewer: caller.previewer,
        can_submit,
        can_run: can_submit && applet.grants.start.is_some(),
        upload_bytes,
        caps: CapsBody {
            hourly: caps.hourly,
            daily: caps.daily,
            applet_daily: caps.applet_daily,
        },
    }))
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UploadQuery {
    /// The file name; only its last path segment is kept.
    pub name: String,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletHandIn, example = json!({
    "bucket": "uploads", "key": "bakta/4f0c9d1e2b3a495867788990aabbccdd/1764000000000-reads.fasta",
    "version_id": "01K8W1C0000000000000000000", "size": 1024
}))]
pub struct HandInBody {
    pub bucket: String,
    /// Name it as an input key of a run.
    pub key: String,
    pub version_id: String,
    pub size: u64,
}

async fn hand_in(
    state: &ServerState,
    caller: Caller,
    (name, bytes): (&str, Bytes),
) -> ServerResult<(StatusCode, Json<HandInBody>)> {
    let node = git_node(state);
    let intake = IntakeOperation::new(&node, &caller.applet, caller.visit, (name, bytes));
    let stored: StoredObject = drive(intake, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    Ok((
        StatusCode::CREATED,
        Json(HandInBody {
            bucket: stored.bucket,
            key: stored.key,
            version_id: stored.version_id.to_string(),
            size: stored.size,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/edge/v1/applets/{applet_id}/uploads",
    tag = "applets",
    summary = "Take in a visitor's file",
    description = r#"Writes the request body as one file below the visitor's folder of the applet's submission folder.

**Authentication**: as for `GET /edge/v1/applets/{applet_id}/session`.

**Behavior**
- The file is written as the applet's own account, which holds WRITE only below the approved
  submission folder.
- Each upload counts against the visitor's hourly and daily caps and the applet's daily cap;
  a guest is held to the guest policy's file size."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        UploadQuery
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 201, description = "The stored file", body = HandInBody),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 403, description = "No submission folder, no approved account, or no access", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 503, description = "The applet is paused", body = ErrorResponse)
    )
)]
pub async fn upload(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<UploadQuery>,
    body: Bytes,
) -> ServerResult<(StatusCode, Json<HandInBody>)> {
    let caller = caller(&state, &headers, &id).await?;
    let name = query.name.rsplit('/').next().unwrap_or_default().trim();
    if name.is_empty() || name == "." || name == ".." {
        return Err(ServerError::BadRequestReason("name a file".to_string()));
    }
    hand_in(&state, caller, (name, body)).await
}

#[utoipa::path(
    post,
    path = "/edge/v1/applets/{applet_id}/submissions",
    tag = "applets",
    summary = "Take in a visitor's form",
    description = r#"Writes one JSON form submission below the visitor's folder of the applet's submission folder.

**Authentication**: as for `GET /edge/v1/applets/{applet_id}/session`.

**Behavior**
- Stored as `<time>-submission.json` by the applet's own account; it counts against the same
  caps as an upload."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body(content = Object, content_type = "application/json",
        example = json!({"sample": "soil-07", "depth_cm": 20})),
    responses(
        (status = 201, description = "The stored submission", body = HandInBody),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 403, description = "No submission folder, no approved account, or no access", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 503, description = "The applet is paused", body = ErrorResponse)
    )
)]
pub async fn submit(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(form): Json<serde_json::Value>,
) -> ServerResult<(StatusCode, Json<HandInBody>)> {
    let caller = caller(&state, &headers, &id).await?;
    let bytes =
        serde_json::to_vec(&form).map_err(|error| ServerError::InternalError(error.into()))?;
    hand_in(&state, caller, ("submission.json", Bytes::from(bytes))).await
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = AppletRunInput)]
pub struct RunInputBody {
    /// The workflow input this file fills.
    pub name: String,
    /// The key of one of the visitor's own uploads.
    pub key: String,
    pub version_id: String,
}

fn empty_values() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(as = AppletRunRequest, example = json!({
    "values": {"genus": "Escherichia"},
    "inputs": [{"name": "reads", "version_id": "01K8W1C0000000000000000000",
        "key": "bakta/4f0c9d1e2b3a495867788990aabbccdd/1764000000000-reads.fasta"}]
}))]
pub struct RunRequestBody {
    #[serde(default = "empty_values")]
    pub values: serde_json::Value,
    #[serde(default)]
    pub inputs: Vec<RunInputBody>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletRun, example = json!({
    "run_id": "01K8W2A0000000000000000000", "state": "running",
    "created_at": "2026-11-24T08:00:00+00:00", "finished_at": null, "error": null
}))]
pub struct RunBody {
    pub run_id: String,
    pub state: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
}

fn run_body(record: &aruna_core::structs::execution::workflow_run::WorkflowRunRecord) -> RunBody {
    RunBody {
        run_id: record.run_id.to_string(),
        state: record.state.as_str().to_string(),
        created_at: rfc3339(record.created_at_ms),
        finished_at: record.finished_at_ms.map(rfc3339),
        error: record.error.clone(),
    }
}

#[utoipa::path(
    post,
    path = "/edge/v1/applets/{applet_id}/runs",
    tag = "applets",
    summary = "Start the applet's workflow for a visitor",
    description = r#"Starts the approved workflow version of the applet with the visitor's values and uploads.

**Authentication**: as for `GET /edge/v1/applets/{applet_id}/session`.

**Behavior**
- The run starts as the applet's own account, restricted to its approved paths; its inputs must
  be the visitor's own uploads and its results go below the visitor's folder.
- Each start counts against the visitor's caps. A guest, or any visitor of a public applet, may
  start work only when every backend of this node declares a kernel sandbox and public work.
- A tool grant cannot start runs yet."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = RunRequestBody,
    responses(
        (status = 202, description = "The started run", body = RunBody),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 403, description = "No granted workflow, another visitor's input, no sandbox, or no access", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "No submission folder, or a tool grant", body = ErrorResponse),
        (status = 503, description = "The applet is paused", body = ErrorResponse)
    )
)]
pub async fn start_run(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<RunRequestBody>,
) -> ServerResult<(StatusCode, Json<RunBody>)> {
    let caller = caller(&state, &headers, &id).await?;
    crate::features::require(
        &state,
        aruna_core::structs::identity::features::Feature::Workflows,
        Some(caller.applet.group_id),
    )
    .await?;
    if caller.visit.guest || caller.applet.access == Access::Public {
        let backends = drive(SandboxOperation, &state.get_ctx())
            .await
            .map_err(applet_error)?;
        if !public_ready(&backends) {
            return Err(ServerError::Forbidden);
        }
    }
    let mut inputs = Vec::with_capacity(body.inputs.len());
    for input in body.inputs {
        let version_id = Ulid::from_string(&input.version_id)
            .map_err(|_| ServerError::BadRequestReason("version_id is not a ULID".to_string()))?;
        inputs.push(RunInput {
            name: input.name,
            key: input.key,
            version_id,
        });
    }
    let request =
        run_request(&caller.applet, &caller.visit, body.values, inputs).map_err(applet_error)?;
    let node = (state.get_node_id(), state.get_realm_id());
    let start = VisitRunOperation::new(&caller.applet, caller.visit, node, request);
    let admitted = drive(start, &state.get_ctx()).await.map_err(applet_error)?;
    Ok((StatusCode::ACCEPTED, Json(run_body(&admitted.record))))
}

/// A run of this applet that the visitor started; any other run reads as missing.
async fn visitor_record(
    state: &ServerState,
    caller: &Caller,
    run: &str,
) -> ServerResult<aruna_core::structs::execution::workflow_run::WorkflowRunRecord> {
    let run_id = Ulid::from_string(run).map_err(|_| ServerError::NotFound)?;
    let auth = applet_auth(&caller.applet, state.get_realm_id()).ok_or(ServerError::NotFound)?;
    let read = RunSnapshotOperation::new(&auth, run_id);
    let snapshot = drive(read, &state.get_ctx())
        .await
        .map_err(|_| ServerError::NotFound)?;
    match visitor_run(&caller.applet, &caller.visit, &snapshot.record) {
        true => Ok(snapshot.record),
        false => Err(ServerError::NotFound),
    }
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/runs/{run_id}",
    tag = "applets",
    summary = "Read a visitor's run",
    description = r#"Answers the state of a run the visitor started.

**Authentication**: as for `GET /edge/v1/applets/{applet_id}/session`.

**Behavior**
- Another visitor's run, and a run of another applet, answers 404."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("run_id" = String, Path, description = "Run id; an unparseable id is 404")
    ),
    responses(
        (status = 200, description = "The run", body = RunBody),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 404, description = "No such run of this visitor", body = ErrorResponse)
    )
)]
pub async fn run_status(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, run)): Path<(String, String)>,
) -> ServerResult<Json<RunBody>> {
    let caller = caller(&state, &headers, &id).await?;
    let record = visitor_record(&state, &caller, &run).await?;
    Ok(Json(run_body(&record)))
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletRunFile)]
pub struct RunFileBody {
    pub name: String,
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub version_id: Option<String>,
    pub size: Option<u64>,
    /// BLAKE3 digest as hex, when known.
    pub digest: Option<String>,
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/runs/{run_id}/results",
    tag = "applets",
    summary = "List the result files of a visitor's run",
    description = r#"Lists the output files of a run the visitor started, each pinned to its exact version.

**Authentication**: as for `GET /edge/v1/applets/{applet_id}/session`.

**Behavior**
- The list is empty until the run produced outputs; every file lies below the visitor's folder."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("run_id" = String, Path, description = "Run id; an unparseable id is 404")
    ),
    responses(
        (status = 200, description = "The result files", body = Vec<RunFileBody>,
            example = json!([{"name": "annotation.gff3", "bucket": "uploads",
                "key": "bakta/4f0c9d1e2b3a495867788990aabbccdd/runs/annotation.gff3",
                "version_id": "01K8W1C0000000000000000000", "size": 2048,
                "digest": "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"}])),
        (status = 401, description = "No valid edge key, or neither visitor nor allowed guest", body = ErrorResponse),
        (status = 404, description = "No such run of this visitor", body = ErrorResponse)
    )
)]
pub async fn run_results(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, run)): Path<(String, String)>,
) -> ServerResult<Json<Vec<RunFileBody>>> {
    let caller = caller(&state, &headers, &id).await?;
    let record = visitor_record(&state, &caller, &run).await?;
    let files = (record.outputs.into_iter())
        .map(|file| RunFileBody {
            name: file.name,
            bucket: file.bucket,
            key: file.key,
            version_id: file.version_id,
            size: file.size,
            digest: file.digest,
        })
        .collect();
    Ok(Json(files))
}
