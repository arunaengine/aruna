//! Serves plugin installs: group admins install a published plugin into their group, narrow its
//! grants, update, pause, resume and remove it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_operations::plugins::grants::{GrantError, InstallScope, parse_grants};
use aruna_operations::plugins::install::{
    InstallError, InstallRequest, install_plugin, pick_version, prepare_install, read_config,
    read_manifest,
};
use aruna_operations::plugins::lifecycle::remove_install;
use aruna_operations::plugins::records::RecordError;
use aruna_operations::users::service_account::group_admin_path;
use aruna_plugins::manifest::Manifest;
use aruna_plugins::permission::Permission as Grant;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

#[derive(OpenApi)]
pub struct InstallsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(InstallsApiDoc::openapi())
        .routes(routes!(install))
        .routes(routes!(list_installs))
        .routes(routes!(get_install, remove))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct InstallBody {
    /// The group the plugin acts in.
    pub group: String,
    /// The published digest to pin; the newest version when absent.
    pub version: Option<String>,
    /// Setting values by name; a setting left out takes its default.
    #[serde(default)]
    #[schema(value_type = Object)]
    pub settings: Map<String, Value>,
    /// Narrowed resource patterns by permission. A permission left out gets all of the group,
    /// an empty list removes an optional one; `connections.send` lists connection names.
    #[serde(default)]
    pub grants: BTreeMap<String, Vec<String>>,
    /// The id a preview answered; sending the same request with it resumes that install.
    pub id: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct InstallQuery {
    /// Checks the request and answers its summary without writing anything.
    pub preview: Option<bool>,
}

/// One permission line of the summary: what the manifest requests and what is granted.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PermissionLine {
    pub permission: String,
    pub required: bool,
    /// Granted resource patterns; empty when not granted.
    pub granted: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectionLine {
    pub name: String,
    pub url: String,
    pub required: bool,
    pub granted: bool,
}

/// What an admin approves: the version, the permissions and connections, and the triggers.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InstallSummary {
    pub id: String,
    pub plugin: String,
    pub title: Option<String>,
    pub version: String,
    pub digest: String,
    pub group: String,
    pub permissions: Vec<PermissionLine>,
    pub connections: Vec<ConnectionLine>,
    /// Trigger kinds, such as `manual` or `dataset_created`.
    pub triggers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InstallView {
    pub id: String,
    pub plugin: String,
    pub version: String,
    pub digest: String,
    pub group: String,
    /// The service account the plugin acts as.
    pub account: String,
    /// `preparing`, `active`, `paused`, `blocked` or `removing`.
    pub status: String,
    #[schema(value_type = Object)]
    pub settings: Value,
    pub granted_by: String,
    pub revision: u64,
    pub active_since_ms: Option<u64>,
    /// The preparation step that failed last, until a retry finishes.
    pub failed: Option<String>,
    pub summary: InstallSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct InstallPage {
    pub installs: Vec<InstallView>,
    /// Pass as `cursor` for the next page; absent on the last page.
    pub next: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct InstallListQuery {
    /// The group whose installs to list.
    pub group: String,
    /// The id of the last install of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; 100 by default.
    pub limit: Option<usize>,
}

const VIEW_EXAMPLE: &str = r#"{"id": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1", "plugin": "dataset-announcer", "version": "0.1.0", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "account": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "status": "active", "settings": {"catalog_url": "https://catalog.example.org"}, "granted_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P3@realm", "revision": 2, "active_since_ms": 1762700000000, "failed": null, "summary": {"id": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1", "plugin": "dataset-announcer", "title": "Announce new datasets", "version": "0.1.0", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "permissions": [{"permission": "metadata.read", "required": true, "granted": ["/realm/g/01JBX7Y3S5M3Q8Y1V9K2T4R6P2/meta/**"]}], "connections": [{"name": "catalog", "url": "{catalog_url}/api/records/", "required": true, "granted": true}], "triggers": ["dataset_created", "manual"]}}"#;

pub(crate) fn view_example() -> Value {
    serde_json::from_str(VIEW_EXAMPLE).unwrap_or(Value::Null)
}

#[utoipa::path(
    post,
    path = "/plugins/{name}/installs",
    tag = "plugins",
    summary = "Install a plugin into a group",
    description = r#"Installs a published plugin into a group, where it acts as its own service account.

**Authentication**: realm bearer token of a group admin. A management node serves the call and
every other node relays it to one.

**Behavior**
- Checks the version, the settings and the grants. Grants default to everything the manifest
  requests on the whole group; the caller must hold every granted right now.
- With `preview=true` the call answers only the summary and an install id, and writes nothing.
  Sending the same request with that `id` installs it; a retry with the id resumes the same
  install, another request with the id is refused.
- The install is written `preparing`, its account and role are made right, and it becomes
  `active` as the last write. A failed step leaves it `preparing` with the step in `failed`.
- Plugins with secret slots or webhooks cannot be installed yet.

**Errors**
- 400 when a setting or grant is invalid, or the plugin cannot be installed yet.
- 403 when the caller is not a group admin or does not hold a granted right.
- 404 when no published version matches.
- 409 when the id belongs to another request or a step failed."#,
    params(("name" = String, Path, description = "The plugin name"), InstallQuery),
    request_body(content = InstallBody, example = json!({"group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "settings": {"catalog_url": "https://catalog.example.org"}, "grants": {"metadata.write": ["/realm/g/01JBX7Y3S5M3Q8Y1V9K2T4R6P2/meta/runs/**"]}})),
    responses(
        (status = 200, description = "The summary of a preview", body = InstallSummary, example = json!(view_example()["summary"])),
        (status = 201, description = "The active install", body = InstallView, example = json!(view_example())),
        (status = 400, description = "Invalid settings or grants, or the plugin cannot be installed yet", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin or lacks a granted right", body = ErrorResponse),
        (status = 404, description = "No published version matches", body = ErrorResponse),
        (status = 409, description = "The id belongs to another request, or a step failed", body = ErrorResponse),
        (status = 503, description = "No management node was reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn install(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    Query(query): Query<InstallQuery>,
    Json(body): Json<InstallBody>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_id(&body.group)?;
    ensure_admin(&state, &auth, group_id).await?;
    let request = InstallRequest {
        plugin: name,
        group_id,
        version: body.version,
        settings: body.settings,
        grants: body.grants,
        install_id: body.id.as_deref().map(parse_id).transpose()?,
    };
    let ctx = state.get_ctx();
    if query.preview.unwrap_or(false) {
        let prepared = prepare_install(&ctx, &auth, state.get_realm_id(), &request)
            .await
            .map_err(map_error)?;
        let grants = aruna_operations::plugins::grants::stored_grants(&prepared.grants);
        let summary = summary(
            &prepared.manifest,
            &prepared.plugin.digest,
            &prepared.scope,
            &grants,
        );
        return Ok(Json(summary).into_response());
    }
    let installed = install_plugin(&ctx, &actor(&state, &auth), &auth, &request)
        .await
        .map_err(map_error)?;
    let view = view(&state, installed).await?;
    Ok((StatusCode::CREATED, Json(view)).into_response())
}

#[utoipa::path(
    get,
    path = "/plugin-installs",
    tag = "plugins",
    summary = "List plugin installs of a group",
    description = r#"Lists the plugin installs of one group, sorted by install id.

**Authentication**: realm bearer token of a group admin.

**Behavior**
- Answers from this node's copy of the realm configuration, so a change made a moment ago on
  another node may appear a little later.
- Pages with `cursor` and `limit`."#,
    params(InstallListQuery),
    responses(
        (status = 200, description = "One page of installs", body = InstallPage, example = json!({"installs": [view_example()], "next": null})),
        (status = 400, description = "The group id or the limit is invalid", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_installs(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<InstallListQuery>,
) -> ServerResult<Json<InstallPage>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_id(&query.group)?;
    ensure_admin(&state, &auth, group_id).await?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(ServerError::BadRequestReason(
            "limit must lie between 1 and 1000".into(),
        ));
    }
    let after = query.cursor.as_deref().map(parse_id).transpose()?;
    let config = load_config(&state).await?;
    let mut selected: Vec<PluginInstall> = (config.installs.iter())
        .filter(|install| install.group_id == group_id)
        .filter(|install| after.is_none_or(|after| install.install_id > after))
        .take(limit + 1)
        .cloned()
        .collect();
    let more = selected.len() > limit;
    selected.truncate(limit);
    let next = more
        .then(|| {
            selected
                .last()
                .map(|install| install.install_id.to_string())
        })
        .flatten();
    let installs = selected
        .into_iter()
        .map(|install| view_in(&config, install))
        .collect();
    Ok(Json(InstallPage { installs, next }))
}

#[utoipa::path(
    get,
    path = "/plugin-installs/{id}",
    tag = "plugins",
    summary = "Read a plugin install",
    description = r#"Reads one plugin install with its summary.

**Authentication**: realm bearer token of an admin of the install's group.

**Behavior**
- Answers from this node's copy of the realm configuration."#,
    params(("id" = String, Path, description = "The install id")),
    responses(
        (status = 200, description = "The install", body = InstallView, example = json!(view_example())),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_install(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<Json<InstallView>> {
    let auth = require_realm_auth(&state, auth)?;
    let install_id = parse_id(&id)?;
    let config = load_config(&state).await?;
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .cloned()
        .ok_or(ServerError::NotFound)?;
    ensure_admin(&state, &auth, install.group_id).await?;
    Ok(Json(view_in(&config, install)))
}

#[utoipa::path(
    delete,
    path = "/plugin-installs/{id}",
    tag = "plugins",
    summary = "Remove a plugin install",
    description = r#"Removes a plugin install, deactivates its account and removes its role.

**Authentication**: realm bearer token of an admin of the install's group. A management node
serves the call and every other node relays it to one.

**Behavior**
- The install is marked `removing` first, so a retry after a failure finishes the removal.
- The account id stays in audit rows."#,
    params(("id" = String, Path, description = "The install id")),
    responses(
        (status = 204, description = "The install is removed"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a group admin", body = ErrorResponse),
        (status = 404, description = "No install has this id", body = ErrorResponse),
        (status = 409, description = "The install changed meanwhile, or a step failed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let (auth, install_id) = admin_call(&state, auth, &id).await?;
    remove_install(&state.get_ctx(), &actor(&state, &auth), &auth, install_id)
        .await
        .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn actor(state: &ServerState, auth: &AuthContext) -> Actor {
    Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    }
}

pub(crate) fn parse_id(id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(id).map_err(|_| ServerError::BadRequestReason(format!("`{id}` is not an id")))
}

/// Request policies live at this boundary; the operations check roles.
async fn ensure_admin(
    state: &ServerState,
    auth: &AuthContext,
    group_id: GroupId,
) -> ServerResult<()> {
    let path = group_admin_path(state.get_realm_id(), group_id);
    ensure_permission(state, auth, path, Permission::WRITE).await
}

/// The caller and install of a change, after the caller proved admin of the install's group.
pub(crate) async fn admin_call(
    state: &ServerState,
    auth: Option<AuthContext>,
    id: &str,
) -> ServerResult<(AuthContext, Ulid)> {
    let auth = require_realm_auth(state, auth)?;
    let install_id = parse_id(id)?;
    let config = load_config(state).await?;
    let install = (config.installs.iter())
        .find(|install| install.install_id == install_id)
        .ok_or(ServerError::NotFound)?;
    ensure_admin(state, &auth, install.group_id).await?;
    Ok((auth, install_id))
}

async fn load_config(state: &ServerState) -> ServerResult<RealmConfigDocument> {
    read_config(&state.get_ctx(), state.get_realm_id())
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))
}

pub(crate) async fn view(state: &ServerState, install: PluginInstall) -> ServerResult<InstallView> {
    Ok(view_in(&load_config(state).await?, install))
}

fn view_in(config: &RealmConfigDocument, install: PluginInstall) -> InstallView {
    let manifest = pick_version(config, &install.plugin, Some(&install.digest))
        .ok()
        .and_then(|plugin| read_manifest(&plugin).ok());
    let scope = InstallScope {
        realm_id: config.realm_id,
        group_id: install.group_id,
        install_id: install.install_id,
    };
    let summary = match manifest {
        Some(manifest) => summary(&manifest, &install.digest, &scope, &install.grants),
        None => bare_summary(&install),
    };
    InstallView {
        id: install.install_id.to_string(),
        plugin: install.plugin,
        version: install.version,
        digest: install.digest,
        group: install.group_id.to_string(),
        account: install.account.to_string(),
        status: install.state.as_str().to_string(),
        settings: serde_json::from_str(&install.settings).unwrap_or(Value::Null),
        granted_by: install.granted_by.to_string(),
        revision: install.revision,
        active_since_ms: install.active_since_ms,
        failed: install.failed,
        summary,
    }
}

/// The summary of `grants` against the manifest's requests.
fn summary(
    manifest: &Manifest,
    digest: &str,
    scope: &InstallScope,
    grants: &BTreeMap<String, Vec<String>>,
) -> InstallSummary {
    let granted = parse_grants(grants);
    let required = &manifest.permissions.required;
    let permissions = (manifest.requested().into_iter())
        .map(|permission| PermissionLine {
            permission: permission.to_string(),
            required: required.contains(&permission),
            granted: (granted.get(&permission).into_iter().flatten())
                .cloned()
                .collect(),
        })
        .collect();
    let sends = granted.get(&Grant::ConnectionsSend);
    let connections = (manifest.connections.iter())
        .map(|(name, connection)| ConnectionLine {
            name: name.clone(),
            url: connection.url.clone(),
            required: !connection.optional,
            granted: sends.is_some_and(|paths| paths.contains(&scope.connection(name))),
        })
        .collect();
    InstallSummary {
        id: scope.install_id.to_string(),
        plugin: manifest.name.clone(),
        title: manifest.title.clone(),
        version: manifest.version.clone(),
        digest: digest.to_string(),
        group: scope.group_id.to_string(),
        permissions,
        connections,
        triggers: manifest
            .triggers
            .keys()
            .map(|kind| kind.as_str().to_string())
            .collect(),
    }
}

/// A summary from the record alone, when the pinned version's manifest is not here yet.
fn bare_summary(install: &PluginInstall) -> InstallSummary {
    let permissions = (install.grants.iter())
        .map(|(permission, granted)| PermissionLine {
            permission: permission.clone(),
            required: false,
            granted: granted.clone(),
        })
        .collect();
    InstallSummary {
        id: install.install_id.to_string(),
        plugin: install.plugin.clone(),
        title: None,
        version: install.version.clone(),
        digest: install.digest.clone(),
        group: install.group_id.to_string(),
        permissions,
        connections: Vec::new(),
        triggers: Vec::new(),
    }
}

pub(crate) fn map_error(error: InstallError) -> ServerError {
    let message = error.to_string();
    match error {
        InstallError::Unauthorized | InstallError::NotHeld(_) => ServerError::Forbidden,
        InstallError::NotPublished | InstallError::NotFound => ServerError::NotFound,
        InstallError::Grant(GrantError::Unsupported(_))
        | InstallError::Grant(_)
        | InstallError::Setting(_) => ServerError::BadRequestReason(message),
        InstallError::Changed | InstallError::State(_) | InstallError::Step { .. } => {
            ServerError::Conflict(message)
        }
        InstallError::Record(RecordError::Unauthorized | RecordError::NotManagementNode) => {
            ServerError::Forbidden
        }
        InstallError::Record(
            RecordError::Stale(_) | RecordError::StorageError(StorageError::TransactionConflict),
        ) => ServerError::Conflict(message),
        InstallError::Unavailable(_) => ServerError::ServiceUnavailableReason(message),
        InstallError::Manifest(_) | InstallError::Record(_) => ServerError::InternalError(message),
    }
}
