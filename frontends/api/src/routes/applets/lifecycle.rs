//! Moves an applet through its lifecycle and changes its embedding and guest settings.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::hosts::embedding_origins;
use aruna_core::applet::record::Applet;
use aruna_ops_work::applets::change::{AppletChange, ChangeAppletOperation};
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_ops_work::automations::applet_link::LinkAppletOperation;
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::wire::{
    AppletAutomationsRequest, AppletBody, ApprovalRequestBody, EmbeddingRequest, GuestsRequest,
    RevisionBody, TakedownRequest, VersionAction,
};
use super::{applet_error, applet_id, effective_hosting, require_feature, serving};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::groups::ensure_group_admin;
use crate::routes::execution::automations::auto_error;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(request_approval))
        .routes(routes!(approve))
        .routes(routes!(reject))
        .routes(routes!(rollback))
        .routes(routes!(pause))
        .routes(routes!(resume))
        .routes(routes!(takedown))
        .routes(routes!(set_embedding))
        .routes(routes!(set_guests))
        .routes(routes!(set_automations))
}

/// Applies `change` as `auth` and answers the changed applet.
async fn change(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    (applet, expected): (ulid::Ulid, Option<u64>),
    change: AppletChange,
) -> ServerResult<Json<AppletBody>> {
    let operation =
        ChangeAppletOperation::new(auth, (applet, expected), change, state.get_node_id());
    let applet = drive(operation, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    Ok(Json(AppletBody::new(
        &applet,
        serving(state, &applet).await?,
    )))
}

/// The applet as a caller that may change it reads it, for checks before a change.
async fn writable(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    id: &str,
) -> ServerResult<Applet> {
    let read = AppletAccessOperation::new(auth, applet_id(id)?, Need::Write);
    drive(read, &state.get_ctx()).await.map_err(applet_error)
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/request-approval",
    tag = "applets",
    summary = "Ask for approval of a version",
    description = r#"Queues a version with the access, grants and embedding origins it should go live with.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on
the applet's host node.

**Behavior**
- An applet that never went live waits for approval as a whole; a live applet keeps serving its
  live version meanwhile.
- A new request replaces an older pending one. Agents and workflows can ask but never approve.
- Read patterns must lie inside the applet's group; origins are exact `https://` origins."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body(content = ApprovalRequestBody, example = json!({
        "version": 2, "access": "public", "origins": ["https://lab.example.org"],
        "grants": {"read": ["/realm/g/group/data/node/results/**"]}, "expected_revision": 4
    })),
    responses(
        (status = 200, description = "The applet with its pending approval", body = AppletBody, example = json!({
            "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.net", "repository": null,
            "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "lifecycle": "waiting_for_approval", "access": "private", "serving": "preview", "live_version": null,
            "versions": [], "grants": {"read": [], "references": [], "start": null, "submissions": null,
                "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
                "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
            "embedding": {"enabled": false, "origins": []}, "guests": null, "domains": [],
            "pending": {"version": 2, "access": "public", "origins": ["https://lab.example.org"],
                "grants": {"read": [], "references": [], "start": null, "submissions": null,
                "limits": {"cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
                "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
                "requested_by": "01K8W1A0000000000000000000", "requested_at": "2026-11-24T08:00:00+00:00"},
            "account": null, "created_by": "01K8W1A0000000000000000000",
            "created_at": "2026-11-24T08:00:00+00:00", "revision": 5
        })),
        (status = 400, description = "A grant outside the group or an invalid origin", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's data", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, an unknown version, the applet is taken down, is hosted elsewhere, or applets are off", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn request_approval(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<ApprovalRequestBody>,
) -> ServerResult<Json<AppletBody>> {
    let applet = writable(&state, &auth, &id).await?;
    require_feature(&state, applet.group_id).await?;
    let limit = effective_hosting(&state).await?.max_origins;
    let mut own: Vec<&str> = vec![applet.host.as_str()];
    own.extend(applet.domains.iter().map(|domain| domain.host.as_str()));
    let origins = embedding_origins(&body.origins, &own, limit)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let grants = body
        .grants
        .grants(auth.realm_id, applet.group_id, applet.host_node)?;
    let request = AppletChange::RequestApproval {
        version: body.version,
        access: body.access.into(),
        grants,
        origins,
    };
    change(
        &state,
        &auth,
        (applet.applet_id, body.expected_revision),
        request,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/approve",
    tag = "applets",
    summary = "Approve a pending version",
    description = r#"Puts the pending version live with the access, grants and origins it was requested with.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held applet bucket on
the host node: the right to publish every file of the version. Approving grants also needs a
group admin, because it makes the applet's own account.

**Behavior**
- With grants, the applet gets its own service account and one role holding exactly the
  approved paths. Pausing or taking the applet down removes the role and deactivates the
  account; resuming restores them. A rollback keeps the approved grants."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = VersionAction,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn approve(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<VersionAction>,
) -> ServerResult<Json<AppletBody>> {
    let applet = applet_id(&id)?;
    let read = AppletAccessOperation::new(&auth, applet, Need::Publish);
    let record = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    require_feature(&state, record.group_id).await?;
    // Grants make the applet's own account, which only a group admin may create.
    if (record.pending.as_ref()).is_some_and(|pending| !pending.grants.is_empty()) {
        ensure_group_admin(&state, &auth, record.group_id).await?;
    }
    let version = body.version;
    change(
        &state,
        &auth,
        (applet, body.expected_revision),
        AppletChange::Approve { version },
    )
    .await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/reject",
    tag = "applets",
    summary = "Reject the pending approval",
    description = r#"Drops the pending approval; an applet that never went live is private again.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn reject(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AppletBody>> {
    let key = (applet_id(&id)?, body.expected_revision);
    change(&state, &auth, key, AppletChange::Reject).await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/rollback",
    tag = "applets",
    summary = "Serve an earlier approved version",
    description = r#"Serves an earlier approved version that was not taken down.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = VersionAction,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn rollback(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<VersionAction>,
) -> ServerResult<Json<AppletBody>> {
    let key = (applet_id(&id)?, body.expected_revision);
    change(
        &state,
        &auth,
        key,
        AppletChange::Rollback {
            version: body.version,
        },
    )
    .await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/pause",
    tag = "applets",
    summary = "Pause a live applet",
    description = r#"Pauses a live applet until its owner resumes it; the feature switch never resumes it.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn pause(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AppletBody>> {
    let key = (applet_id(&id)?, body.expected_revision);
    change(&state, &auth, key, AppletChange::Pause).await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/resume",
    tag = "applets",
    summary = "Resume a paused applet",
    description = r#"Serves a paused applet's live version again.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = RevisionBody,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resume(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<RevisionBody>,
) -> ServerResult<Json<AppletBody>> {
    let key = (applet_id(&id)?, body.expected_revision);
    change(&state, &auth, key, AppletChange::Resume).await
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/takedown",
    tag = "applets",
    summary = "Take an applet or version down",
    description = r#"Takes one version down, or the whole applet for good when no version is named.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = TakedownRequest,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn takedown(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<TakedownRequest>,
) -> ServerResult<Json<AppletBody>> {
    let key = (applet_id(&id)?, body.expected_revision);
    change(
        &state,
        &auth,
        key,
        AppletChange::TakeDown {
            version: body.version,
        },
    )
    .await
}

#[utoipa::path(
    put,
    path = "/applets/{applet_id}/embedding",
    tag = "applets",
    summary = "Set the applet's embedding",
    description = r#"Switches embedding and narrows its origins at once; adding an origin needs approval.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's
host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = EmbeddingRequest,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_embedding(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<EmbeddingRequest>,
) -> ServerResult<Json<AppletBody>> {
    let applet = writable(&state, &auth, &id).await?;
    if body.enabled && !effective_hosting(&state).await?.embedding {
        return Err(ServerError::Conflict(
            "this node does not allow embedding".to_string(),
        ));
    }
    let mut own: Vec<&str> = vec![applet.host.as_str()];
    own.extend(applet.domains.iter().map(|domain| domain.host.as_str()));
    let origins = embedding_origins(&body.origins, &own, usize::MAX)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let change_body = AppletChange::Embedding {
        enabled: body.enabled,
        origins,
    };
    change(
        &state,
        &auth,
        (applet.applet_id, body.expected_revision),
        change_body,
    )
    .await
}

#[utoipa::path(
    put,
    path = "/applets/{applet_id}/guests",
    tag = "applets",
    summary = "Set guest submissions",
    description = r#"Switches submissions without an account on or off with their fixed caps.

**Authentication**: realm bearer token without path restrictions of a group admin of the applet's group."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = GuestsRequest,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "A malformed body", body = ErrorResponse),
        (status = 403, description = "The caller lacks the right the change needs", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "A stale revision, a change the lifecycle refuses, or an applet hosted elsewhere", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_guests(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<GuestsRequest>,
) -> ServerResult<Json<AppletBody>> {
    let applet = writable(&state, &auth, &id).await?;
    ensure_group_admin(&state, &auth, applet.group_id).await?;
    if body.guests.is_some() && !effective_hosting(&state).await?.guests {
        return Err(ServerError::Conflict(
            "this node does not allow guest submissions".to_string(),
        ));
    }
    let policy = body.guests.map(|guests| guests.policy(auth.user_id));
    let key = (applet.applet_id, body.expected_revision);
    change(&state, &auth, key, AppletChange::Guests(policy)).await
}

#[utoipa::path(
    put,
    path = "/applets/{applet_id}/automations",
    tag = "applets",
    summary = "Set the automations started by an applet",
    description = r#"Sets which workflow automations in the applet's group are started when visitors submit this applet.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's data on the applet's host node."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = AppletAutomationsRequest,
    responses(
        (status = 200, description = "The changed applet", body = AppletBody),
        (status = 400, description = "Invalid automation id or automation belongs to another group", body = ErrorResponse),
        (status = 403, description = "Permission denied or feature disabled", body = ErrorResponse),
        (status = 404, description = "No such applet or automation", body = ErrorResponse),
        (status = 409, description = "Stale revision or applet taken down", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_automations(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<AppletAutomationsRequest>,
) -> ServerResult<Json<AppletBody>> {
    let target = applet_id(&id)?;
    let ids = body
        .automation_ids
        .iter()
        .map(|raw| {
            Ulid::from_string(raw).map_err(|_| {
                ServerError::BadRequestReason(format!("automation_id `{raw}` is not a ULID"))
            })
        })
        .collect::<ServerResult<Vec<_>>>()?;
    let operation = LinkAppletOperation::new(
        &auth,
        (target, body.expected_revision),
        ids,
        state.get_node_id(),
    )
    .with_disabled_features(state.disabled_features());
    let applet = drive(operation, &state.get_ctx())
        .await
        .map_err(auto_error)?;
    Ok(Json(AppletBody::new(
        &applet,
        serving(&state, &applet).await?,
    )))
}
