//! Builds applet sources in the curated builder and manages the drafts the builds produce.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::manifest::LOCKFILE;
use aruna_core::applet::record::{Applet, Draft, PinnedObject};
use aruna_core::applet::settings::BuildingSettings;
use aruna_core::structs::execution::job::family::LogicalJobState;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::local::WorkspaceMode;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_ops_work::applets::build::{StageOperation, build_spec};
use aruna_ops_work::applets::change::{AppletChange, ChangeAppletOperation};
use aruna_ops_work::applets::draft::{DraftAction, DraftOperation};
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::{MAX_MANIFEST, read_bytes};
use super::source::{ReleaseRequest, ReleasedVersion};
use super::{applet_error, applet_id, require_feature};
use crate::auth::{UnrestrictedAuth, ValidatedBearer};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::jobs::{JobRequestError, family_report, map_job_route, rfc3339, submit_external_job};
use crate::routes::git::git_node;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(start_build))
        .routes(routes!(list_drafts, capture_draft))
        .routes(routes!(delete_draft))
        .routes(routes!(keep_draft))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DraftState {
    Building,
    Ready,
    Failed,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDraft, example = json!({
    "build_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "job_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z3",
    "state": "ready", "commit": "9fceb02d0ae598e95dc970b74767f19372d61af8",
    "started_by": "01K8W1A0000000000000000000", "started_at": "2026-11-24T08:00:00+00:00",
    "expires_at": "2026-11-25T08:00:00+00:00", "reason": null
}))]
pub struct DraftBody {
    pub build_id: String,
    /// The builder job; its logs and state are read through the job routes.
    pub job_id: String,
    pub state: DraftState,
    pub commit: String,
    pub started_by: String,
    pub started_at: String,
    /// When the draft and its files go away unless kept.
    pub expires_at: String,
    /// Why the build failed.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDrafts, example = json!({
    "max": 6, "remaining": 5, "ttl_hours": 24, "drafts": []
}))]
pub struct DraftsBody {
    /// Drafts the applet may keep at once, running builds included.
    pub max: usize,
    /// How many more builds may start now.
    pub remaining: usize,
    pub ttl_hours: u64,
    pub drafts: Vec<DraftBody>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"build_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2"}))]
pub struct CaptureRequest {
    pub build_id: String,
}

/// Bytes a build's lockfile may hold.
const MAX_LOCKFILE: usize = 16 << 20;

fn building(state: &ServerState) -> ServerResult<&BuildingSettings> {
    (state.applet_settings().building.as_ref()).ok_or_else(|| {
        ServerError::ServiceUnavailableReason("this node builds no applets".to_string())
    })
}

fn draft_body(draft: &Draft) -> DraftBody {
    DraftBody {
        build_id: draft.build_id.to_string(),
        job_id: draft.job_id.to_string(),
        state: match (&draft.manifest, &draft.failed) {
            (Some(_), _) => DraftState::Ready,
            (None, Some(_)) => DraftState::Failed,
            (None, None) => DraftState::Building,
        },
        commit: draft.commit.clone(),
        started_by: draft.started_by.to_string(),
        started_at: rfc3339(draft.started_at_ms),
        expires_at: rfc3339(draft.expires_at_ms),
        reason: draft.failed.clone(),
    }
}

fn drafts_body(applet: &Applet, settings: &BuildingSettings) -> DraftsBody {
    DraftsBody {
        max: settings.max_drafts,
        remaining: applet.draft_room(settings.max_drafts),
        ttl_hours: settings.draft_ttl_hours,
        drafts: applet.drafts.iter().map(draft_body).collect(),
    }
}

fn build_id(text: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text).map_err(|_| ServerError::NotFound)
}

/// The applet as a caller that may change it reads it, after the feature check.
async fn writable(state: &ServerState, auth: &AuthContext, id: &str) -> ServerResult<Applet> {
    let read = AppletAccessOperation::new(auth, applet_id(id)?, Need::Write);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    require_feature(state, applet.group_id).await?;
    Ok(applet)
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/builds",
    tag = "applets",
    summary = "Build a commit folder into a draft",
    description = r#"Starts the curated builder on one revision folder of the applet repository.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held
applet bucket on the applet's host node and READ on its repository document.

**Behavior**
- The folder's files are pinned in the held bucket and mounted read-only at `/src` of a batch
  job running the node's pinned builder image on its builder backend.
- The job gets no credential and no network. It writes the site, `package-lock.json` and, with
  `ARUNA_PREVIEW_SOURCE_MAP=1`, `aruna-source-map.json` into `/out`.
- The build counts as a draft at once; `GET /applets/{applet_id}/drafts` shows the room left.
  Capture the finished build with `POST /applets/{applet_id}/drafts`.

**Limits**
- At most the node's `max` drafts at once; each lives `ttl_hours` unless kept."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = ReleaseRequest,
    responses(
        (status = 202, description = "The build started", body = DraftBody),
        (status = 400, description = "An empty folder or too many bytes", body = ErrorResponse),
        (status = 403, description = "No WRITE on the held bucket", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "No room for a draft, no repository, taken down, hosted elsewhere, or applets are off", body = ErrorResponse),
        (status = 503, description = "This node builds no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_build(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(id): Path<String>,
    Json(body): Json<ReleaseRequest>,
) -> ServerResult<(StatusCode, Json<DraftBody>)> {
    let settings = building(&state)?.clone();
    let applet = writable(&state, &auth, &id).await?.applet_id;
    let build_id = Ulid::generate();
    let source = (
        body.revision.unwrap_or_else(|| "main".to_string()),
        body.root.unwrap_or_default(),
    );
    let node = git_node(&state);
    let stage = StageOperation::new(
        (&node, &auth),
        (applet, build_id),
        settings.max_drafts,
        source,
    );
    let staged = drive(stage, &state.get_ctx()).await.map_err(applet_error)?;
    let spec = build_spec(&staged, build_id, &settings);
    let token = crate::metadata::forwarded_auth_token(bearer)?;
    let accepted = submit_external_job(
        &state.get_ctx(),
        spec,
        auth.user_id,
        Some(format!("applet-build-{build_id}")),
        WorkspaceMode::None,
        None,
        state.rocrate_limits().artifact_retention_ms,
        token,
    )
    .await
    .map_err(|error| ServerError::from(JobRequestError::from_submit(error)))?;
    let draft = Draft {
        build_id,
        job_id: accepted.job_id.as_ulid(),
        commit: staged.commit,
        started_by: auth.user_id,
        started_at_ms: 0,
        expires_at_ms: 0,
        manifest: None,
        failed: None,
    };
    let change = AppletChange::StartBuild {
        draft: Box::new(draft),
        max: settings.max_drafts,
        ttl_ms: settings.draft_ttl_hours.saturating_mul(3_600_000),
    };
    let write = ChangeAppletOperation::new(&auth, (applet, None), change, state.get_node_id());
    let record = drive(write, &state.get_ctx()).await.map_err(applet_error)?;
    let draft = record.draft(build_id).ok_or(ServerError::NotFound)?;
    Ok((StatusCode::ACCEPTED, Json(draft_body(draft))))
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}/drafts",
    tag = "applets",
    summary = "List an applet's drafts and the room left",
    description = r#"Lists the running and finished builds of an applet with their expiry and how many more may start.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data
on the applet's host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    responses(
        (status = 200, description = "The drafts", body = DraftsBody),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 503, description = "This node builds no applets", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_drafts(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
) -> ServerResult<Json<DraftsBody>> {
    let settings = building(&state)?;
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Write);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(Json(drafts_body(&applet, settings)))
}

/// The lockfile output of a successful build, read so its packages can be listed.
async fn lockfile(
    state: &ServerState,
    applet: &Applet,
    outputs: &[aruna_core::structs::execution::job::local::OutputObject],
) -> ServerResult<Option<Bytes>> {
    let suffix = format!("/out/{LOCKFILE}");
    let Some(output) = outputs.iter().find(|output| output.key.ends_with(&suffix)) else {
        return Ok(None);
    };
    let pinned = PinnedObject {
        node: output.node_id,
        bucket: output.bucket.clone(),
        key: output.key.clone(),
        version_id: output.version_id.to_string(),
        blake3: output.digest.clone().unwrap_or_default(),
    };
    let bytes = read_bytes(state, applet, &pinned, MAX_LOCKFILE).await?;
    Ok(Some(Bytes::from(bytes)))
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/drafts",
    tag = "applets",
    summary = "Capture a finished build as a draft",
    description = r#"Pins the outputs of a finished build as a draft that only previewers see.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held
applet bucket on the applet's host node.

**Behavior**
- A successful build becomes `ready`: its files are pinned exactly as the builder wrote them,
  `aruna-source-map.json` is kept apart from the served files and `package-lock.json` gives the
  package list.
- A failed or cancelled build, and one whose outputs make no site (no root `index.html`, an
  output without a digest, or a changed lockfile), becomes `failed` with its reason. It still
  counts until it is deleted or expires.
- A build that still runs answers 409."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = CaptureRequest,
    responses(
        (status = 200, description = "The captured draft", body = DraftBody),
        (status = 403, description = "No WRITE on the held bucket", body = ErrorResponse),
        (status = 404, description = "No such applet or draft", body = ErrorResponse),
        (status = 409, description = "The build still runs or was captured already", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn capture_draft(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<CaptureRequest>,
) -> ServerResult<Json<DraftBody>> {
    let applet = writable(&state, &auth, &id).await?;
    let build = build_id(&body.build_id)?;
    let draft = applet.draft(build).ok_or(ServerError::NotFound)?;
    let job = JobId::try_from_bytes(draft.job_id.to_bytes()).map_err(|_| ServerError::NotFound)?;
    let report = family_report(&state.get_ctx(), &auth, job)
        .await
        .ok_or(ServerError::NotFound)?
        .map_err(map_job_route)?;
    let action = match report.state {
        LogicalJobState::Succeeded => DraftAction::Capture {
            lockfile: lockfile(&state, &applet, &report.outputs).await?,
            outputs: report.outputs,
        },
        LogicalJobState::Failed | LogicalJobState::Cancelled | LogicalJobState::NotRun => {
            DraftAction::Fail(format!("the build ended {}", report.state.name()))
        }
        _ => return Err(ServerError::Conflict("the build still runs".to_string())),
    };
    let node = git_node(&state);
    let capture = DraftOperation::new((&node, &auth), (applet.applet_id, build), action);
    let record = drive(capture, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    let draft = record.draft(build).ok_or(ServerError::NotFound)?;
    Ok(Json(draft_body(draft)))
}

#[utoipa::path(
    delete,
    path = "/applets/{applet_id}/drafts/{build_id}",
    tag = "applets",
    summary = "Delete a draft",
    description = r#"Drops a draft, running or finished, and frees its room.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data
on the applet's host node.

**Behavior**
- A running build is not cancelled here; cancel its job through the job routes."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("build_id" = String, Path, description = "Build id; an unparseable id is 404")
    ),
    responses(
        (status = 204, description = "The draft is gone"),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet or draft", body = ErrorResponse),
        (status = 409, description = "The applet is hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_draft(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((id, build)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let applet = applet_id(&id)?;
    let change = AppletChange::DropDraft {
        build_id: build_id(&build)?,
    };
    let write = ChangeAppletOperation::new(&auth, (applet, None), change, state.get_node_id());
    drive(write, &state.get_ctx()).await.map_err(applet_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/drafts/{build_id}/keep",
    tag = "applets",
    summary = "Keep a draft as a private version",
    description = r#"Turns a ready draft into the next private version without building again.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held
applet bucket on the applet's host node.

**Behavior**
- The version pins the very objects the draft pinned, source map included, so approvers review
  exactly what the previewers saw.
- The draft is gone afterwards and its room is free."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("build_id" = String, Path, description = "Build id; an unparseable id is 404")
    ),
    responses(
        (status = 201, description = "The new private version", body = ReleasedVersion),
        (status = 403, description = "No WRITE on the held bucket", body = ErrorResponse),
        (status = 404, description = "No such applet or draft", body = ErrorResponse),
        (status = 409, description = "The draft is not ready, the applet is taken down, or the version number moved", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn keep_draft(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((id, build)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<ReleasedVersion>)> {
    let applet = writable(&state, &auth, &id).await?;
    let build = build_id(&build)?;
    let draft = applet.draft(build).ok_or(ServerError::NotFound)?;
    let pinned = (draft.manifest.clone())
        .ok_or_else(|| ServerError::Conflict("the draft is not ready".to_string()))?;
    let bytes = read_bytes(&state, &applet, &pinned, MAX_MANIFEST).await?;
    let node = git_node(&state);
    let action = DraftAction::Keep(Bytes::from(bytes));
    let keep = DraftOperation::new((&node, &auth), (applet.applet_id, build), action);
    let record = drive(keep, &state.get_ctx()).await.map_err(applet_error)?;
    let entry = (record.versions.iter())
        .max_by_key(|entry| entry.version)
        .ok_or(ServerError::NotFound)?;
    Ok((
        StatusCode::CREATED,
        Json(ReleasedVersion {
            version: entry.version,
            commit: entry.commit.clone(),
            state: super::wire::VersionStateBody::Private,
        }),
    ))
}
