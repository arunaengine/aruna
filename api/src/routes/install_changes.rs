//! Serves the changes of a plugin install after it is active: grants, version, pause, resume.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::plugins::lifecycle::{
    pause_install, resume_install, set_grants, update_install,
};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::plugin_installs::{InstallView, actor, admin_call, map_error, view, view_example};
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
pub struct ChangesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ChangesApiDoc::openapi())
        .routes(routes!(put_grants))
        .routes(routes!(put_version))
        .routes(routes!(pause))
        .routes(routes!(resume))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct GrantsBody {
    /// Narrowed resource patterns by permission, as for an install.
    #[serde(default)]
    pub grants: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct VersionBody {
    /// The published digest to move to.
    pub version: String,
    /// New setting values; the current ones when absent.
    #[schema(value_type = Option<Object>)]
    pub settings: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateView {
    pub install: InstallView,
    /// What the new version requires that nobody granted yet; the install stays blocked
    /// until a group admin grants it.
    pub missing: Vec<String>,
}

#[utoipa::path(
    put,
    path = "/plugin-installs/{id}/grants",
    tag = "plugins",
    summary = "Replace plugin grants",
    description = r#"Replaces the grants of a plugin install and recomputes its account role.

**Authentication**: realm bearer token of an admin of the install's group. A management node
serves the call and every other node relays it to one.

**Behavior**
- A permission left out gets all of the group, an empty list removes an optional one; required
  permissions and connections cannot be removed. The caller must hold every granted right now.
- A `blocked` install becomes `active`, since the admin approved what it needs."#,
    params(("id" = String, Path, description = "The install id")),
    request_body(content = GrantsBody, example = json!({"grants": {"metadata.write": ["/realm/g/01JBX7Y3S5M3Q8Y1V9K2T4R6P2/meta/runs/**"]}})),
    responses(
        (status = 200, description = "The changed install", body = InstallView, example = json!(view_example())),
        (status = 400, description = "A grant is invalid", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin or lacks a granted right", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install changed meanwhile or is not settled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_grants(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Json(body): Json<GrantsBody>,
) -> ServerResult<Json<InstallView>> {
    let (auth, install_id) = admin_call(&state, auth, &id).await?;
    let changed = set_grants(
        &state.get_ctx(),
        &actor(&state, &auth),
        &auth,
        install_id,
        &body.grants,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(view(&state, changed).await?))
}

#[utoipa::path(
    put,
    path = "/plugin-installs/{id}/version",
    tag = "plugins",
    summary = "Update a plugin install",
    description = r#"Moves a plugin install to another published version of the same plugin.

**Authentication**: realm bearer token of an admin of the install's group. A management node
serves the call and every other node relays it to one.

**Behavior**
- Keeps the grants the new version still requests, drops the others, and grants nothing new.
- While the new version requires a permission or connection nobody granted, the install is
  `blocked` and `missing` lists what needs approval through the grants route."#,
    params(("id" = String, Path, description = "The install id")),
    request_body(content = VersionBody, example = json!({"version": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1"})),
    responses(
        (status = 200, description = "The updated install", body = UpdateView, example = json!({"install": view_example(), "missing": []})),
        (status = 400, description = "The settings do not fit the new version, or it cannot be installed yet", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install or published version matches", body = ErrorResponse),
        (status = 409, description = "The install changed meanwhile or is not settled", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_version(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Json(body): Json<VersionBody>,
) -> ServerResult<Json<UpdateView>> {
    let (auth, install_id) = admin_call(&state, auth, &id).await?;
    let actor = actor(&state, &auth);
    let settings = body.settings.as_ref();
    let (updated, missing) = update_install(
        &state.get_ctx(),
        &actor,
        &auth,
        install_id,
        &body.version,
        settings,
    )
    .await
    .map_err(map_error)?;
    let install = view(&state, updated).await?;
    Ok(Json(UpdateView { install, missing }))
}

#[utoipa::path(
    post,
    path = "/plugin-installs/{id}/pause",
    tag = "plugins",
    summary = "Pause a plugin install",
    description = r#"Pauses an active plugin install, which then runs nothing and passes no call check.

**Authentication**: realm bearer token of an admin of the install's group. A management node
serves the call and every other node relays it to one.

**Behavior**
- Keeps the account, the role and the grants."#,
    params(("id" = String, Path, description = "The install id")),
    responses(
        (status = 200, description = "The paused install", body = InstallView, example = json!(view_example())),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install is not active", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn pause(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<InstallView>> {
    let (auth, install_id) = admin_call(&state, auth, &id).await?;
    let paused = pause_install(&state.get_ctx(), &actor(&state, &auth), &auth, install_id)
        .await
        .map_err(map_error)?;
    Ok(Json(view(&state, paused).await?))
}

#[utoipa::path(
    post,
    path = "/plugin-installs/{id}/resume",
    tag = "plugins",
    summary = "Resume a plugin install",
    description = r#"Makes a paused plugin install active again.

**Authentication**: realm bearer token of an admin of the install's group. A management node
serves the call and every other node relays it to one.

**Behavior**
- Events from the pause are not replayed: the install is active from now on."#,
    params(("id" = String, Path, description = "The install id")),
    responses(
        (status = 200, description = "The active install", body = InstallView, example = json!(view_example())),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install is not paused", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resume(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<InstallView>> {
    let (auth, install_id) = admin_call(&state, auth, &id).await?;
    let resumed = resume_install(&state.get_ctx(), &actor(&state, &auth), &auth, install_id)
        .await
        .map_err(map_error)?;
    Ok(Json(view(&state, resumed).await?))
}
