//! Serves plugin installs: group admins install a published plugin into their group, narrow its
//! grants, update, pause, resume and remove it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::errors::StorageError;
use aruna_core::plugin_installs::{BoundScope, PluginInstall, SecretBinding};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_operations::plugins::account::plugin_account;
use aruna_operations::plugins::bound::vault_kind;
use aruna_operations::plugins::grants::{GrantError, InstallScope, parse_grants};
use aruna_operations::plugins::install::{
    InstallError, InstallRequest, install_plugin, pick_version, prepare_install, read_config,
    read_manifest,
};
use aruna_operations::plugins::lifecycle::remove_install;
use aruna_operations::plugins::matcher::{SYNC_WARNING, undelivered};
use aruna_operations::plugins::receiver::lost_reason;
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

use crate::auth::{ensure_permission, require_realm_auth, require_unrestricted_auth};
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
    /// The secret of each slot as `group:<name>`, `realm:<name>` or `node:<name>` (the
    /// receiver's node vault); a slot left out uses the group secret of the slot's name.
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
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
    /// The code reads whole answers, headers and body, as far as the secret's rule allows.
    pub full_answer: bool,
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
    /// The secret each slot uses.
    pub secrets: Vec<SecretLine>,
    /// What the admin should know before approving, such as triggers not delivered yet.
    pub warnings: Vec<String>,
}

/// One secret slot of the summary: its kind and the secret it uses.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SecretLine {
    pub slot: String,
    /// `bearer`, `basic`, `header` or `hmac`.
    pub kind: String,
    /// `group:<name>`, `realm:<name>` or `node:<name>`.
    pub secret: String,
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
    /// The node that runs this install's runs.
    pub receiver: String,
    /// `preparing`, `active`, `paused`, `blocked` or `removing`.
    pub status: String,
    /// Why the install is blocked while the node that runs it cannot run it.
    pub blocked_reason: Option<String>,
    /// An admin paused it; it stays paused through updates and grant changes until resumed.
    pub paused: bool,
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

const VIEW_EXAMPLE: &str = r#"{"id": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1", "plugin": "dataset-announcer", "version": "0.1.0", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "account": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "receiver": "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6", "status": "active", "blocked_reason": null, "paused": false, "settings": {"catalog_url": "https://catalog.example.org"}, "granted_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P3@realm", "revision": 2, "active_since_ms": 1762700000000, "failed": null, "summary": {"id": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1", "plugin": "dataset-announcer", "title": "Announce new datasets", "version": "0.1.0", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "permissions": [{"permission": "metadata.read", "required": true, "granted": ["/realm/g/01JBX7Y3S5M3Q8Y1V9K2T4R6P2/meta/**"]}], "connections": [{"name": "catalog", "url": "{catalog_url}/api/records/", "required": true, "granted": true, "full_answer": false}], "triggers": ["dataset_created", "manual"], "secrets": [{"slot": "catalog_key", "kind": "bearer", "secret": "group:catalog-key"}], "warnings": []}}"#;

pub(crate) fn view_example() -> Value {
    serde_json::from_str(VIEW_EXAMPLE).unwrap_or(Value::Null)
}

#[utoipa::path(
    post,
    path = "/plugins/{name}/installs",
    tag = "plugins",
    summary = "Install a plugin into a group",
    description = r#"Installs a published plugin into a group, where it acts as its own service account.

**Authentication**: realm bearer token of a group admin; a session without path restrictions
when the manifest declares secret slots. A management node serves the call and every other node
relays it to one.

**Behavior**
- Checks the version, the settings and the grants. Grants default to everything the manifest
  requests on the whole group; the caller must hold every granted right now.
- With `preview=true` the call answers only the summary and an install id, and writes nothing.
  Sending the same request with that `id` and the summary's digest as `version` installs exactly
  what the summary showed; a retry with the id resumes the same install, another request with
  the id is refused. A retry without `version` keeps the existing install's version.
- The install is written `preparing`, its account and role are made right, and it becomes
  `active` as the last write. A failed step leaves it `preparing` with the step in `failed`.
- Each secret slot binds a secret by `secrets`, the group secret of the slot's name by default.
  A group or realm secret is checked here, a node secret on the receiver; it must exist with the
  slot's kind. A realm or node secret needs its use rule from a realm admin; the summary warns.
- Plugins with secret slots or webhooks cannot be installed yet.
- While any enabled realm or group policy reads the request path, a grant on a subtree is
  refused, even when the policy's guard leaves plugin operations out; exact paths stay usable.

**Errors**
- 400 when a setting or grant is invalid, or the plugin cannot be installed yet.
- 403 when the caller is not a group admin or does not hold a granted right.
- 404 when no published version matches.
- 409 when the id belongs to another request or a step failed."#,
    params(("name" = String, Path, description = "The plugin name"), InstallQuery),
    request_body(content = InstallBody, example = json!({"group": "01JBX7Y3S5M3Q8Y1V9K2T4R6P2", "settings": {"catalog_url": "https://catalog.example.org"}, "grants": {"metadata.write": ["/realm/g/01JBX7Y3S5M3Q8Y1V9K2T4R6P2/meta/runs/**"]}, "secrets": {"catalog_key": "group:catalog-key"}})),
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
        secrets: body.secrets,
    };
    let ctx = state.get_ctx();
    let preview = query.preview.unwrap_or(false);
    if !preview && !request.secrets.is_empty() {
        require_unrestricted_auth(&state, Some(auth.clone()))?;
    }
    if !preview {
        let config = load_config(&state).await?;
        let chosen = pick_version(&config, &request.plugin, request.version.as_deref()).ok();
        let digest = chosen.map(|plugin| plugin.digest);
        secret_session(&state, &auth, (request.install_id, digest.as_deref())).await?;
    }
    if preview {
        let prepared = prepare_install(&ctx, &auth, state.get_realm_id(), &request)
            .await
            .map_err(map_error)?;
        let grants = aruna_operations::plugins::grants::stored_grants(&prepared.grants);
        let granted = (&grants, &prepared.secrets);
        let summary = summary(
            &prepared.manifest,
            &prepared.plugin.digest,
            &prepared.scope,
            granted,
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

**Authentication**: realm bearer token of an admin of the install's group; a session without
path restrictions when the install's manifest declares secret slots. A management node serves
the call and every other node relays it to one.

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
    secret_session(&state, &auth, (Some(install_id), None)).await?;
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

/// A change to an install whose manifest, now or after the change, declares secret slots adds,
/// changes or removes use rules; like vault management, it needs an unrestricted session.
pub(crate) async fn secret_session(
    state: &ServerState,
    auth: &AuthContext,
    (install_id, digest): (Option<Ulid>, Option<&str>),
) -> ServerResult<()> {
    let config = load_config(state).await?;
    let install = install_id.and_then(|id| config.installs.iter().find(|i| i.install_id == id));
    let pinned = install.map(|install| (install.plugin.as_str(), install.digest.as_str()));
    let named = digest.and_then(|digest| {
        let plugin = config
            .plugins
            .iter()
            .find(|plugin| plugin.digest == digest)?;
        Some((plugin.name.as_str(), digest))
    });
    let slots = [pinned, named].into_iter().flatten().any(|(name, digest)| {
        (pick_version(&config, name, Some(digest)).ok())
            .and_then(|plugin| read_manifest(&plugin).ok())
            .is_some_and(|manifest| !manifest.secrets.is_empty())
    });
    match slots {
        true => require_unrestricted_auth(state, Some(auth.clone())).map(drop),
        false => Ok(()),
    }
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
        Some(manifest) => {
            let granted = (&install.grants, &install.secrets);
            summary(&manifest, &install.digest, &scope, granted)
        }
        None => bare_summary(&install),
    };
    // A lost receiver blocks the install here; its stored state stays as it is.
    let lost = lost_reason(config, install.receiver);
    InstallView {
        id: install.install_id.to_string(),
        plugin: install.plugin,
        version: install.version,
        digest: install.digest,
        group: install.group_id.to_string(),
        account: install.account.to_string(),
        receiver: install.receiver.to_string(),
        status: match lost {
            Some(_) => "blocked".to_string(),
            None => install.state.as_str().to_string(),
        },
        blocked_reason: lost.map(str::to_string),
        paused: install.paused,
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
    (grants, bound): (
        &BTreeMap<String, Vec<String>>,
        &BTreeMap<String, SecretBinding>,
    ),
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
            full_answer: connection.response.full(),
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
        secrets: (bound.iter())
            .map(|(slot, binding)| SecretLine {
                slot: slot.clone(),
                kind: (manifest.secrets.get(slot))
                    .and_then(|declared| vault_kind(declared.kind))
                    .map_or_else(String::new, |kind| kind.as_str().to_string()),
                secret: binding.to_string(),
            })
            .collect(),
        warnings: (undelivered(manifest)
            .first()
            .map(|_| SYNC_WARNING.to_string()))
        .into_iter()
        .chain(bound.iter().filter_map(|(slot, binding)| {
            let shared = matches!(binding.scope, BoundScope::Realm | BoundScope::Node);
            shared.then(|| format!("a realm admin adds the use rule of `{binding}` for `{slot}`"))
        }))
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
        secrets: Vec::new(),
        warnings: Vec::new(),
    }
}

pub(crate) fn map_error(error: InstallError) -> ServerError {
    let message = error.to_string();
    match error {
        InstallError::Unauthorized | InstallError::NotHeld(_) => ServerError::Forbidden,
        InstallError::NotPublished | InstallError::NotFound => ServerError::NotFound,
        InstallError::Grant(GrantError::Unsupported(_))
        | InstallError::Grant(_)
        | InstallError::Setting(_)
        | InstallError::Binding(_) => ServerError::BadRequestReason(message),
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

/// Refuses member, role, admin and token changes of a plugin account; its install manages it.
pub(crate) async fn refuse_plugin_account(
    state: &ServerState,
    user_id: UserId,
) -> ServerResult<()> {
    match plugin_account(&state.get_ctx(), user_id).await {
        Ok(false) => Ok(()),
        Ok(true) => Err(ServerError::Conflict(
            "the account acts for a plugin install; change the install instead".into(),
        )),
        Err(error) => Err(ServerError::InternalError(error)),
    }
}

/// Refuses deleting the role a plugin install computed for its account.
pub(crate) async fn refuse_plugin_role(state: &ServerState, role_id: Ulid) -> ServerResult<()> {
    let config = load_config(state).await?;
    match config
        .installs
        .iter()
        .any(|install| install.role_id == role_id)
    {
        false => Ok(()),
        true => Err(ServerError::Conflict(
            "the role belongs to a plugin install; change its grants instead".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::install_changes::{GrantsBody, put_grants};
    use crate::tests::routes::{
        seed_group_docs, seed_realm_auth, test_context, test_state, test_storage, write_doc,
    };
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::plugin_installs::InstallState;
    use aruna_core::plugins::PublishedPlugin;
    use aruna_core::structs::identity::auth::{NodeCapabilities, PathRestriction};
    use aruna_core::structs::identity::realm::RealmId;

    #[tokio::test]
    async fn secret_changes_unrestricted() {
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
        let digest = format!("sha256:{}", "ab".repeat(32));
        let manifest = serde_json::json!({
            "name": "announcer", "version": "0.1.0",
            "secrets": { "api_key": { "title": "API key" } },
        });
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.plugins.push(PublishedPlugin {
            name: "announcer".into(),
            version: "0.1.0".into(),
            digest: digest.clone(),
            size: 10,
            manifest: manifest.to_string(),
            published_by: admin,
            published_at_ms: 1,
        });
        let install_id = Ulid::from_parts(7, 7);
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
            receiver: node_id,
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
        // A session restricted to the group's administration passes the admin check only.
        let restricted = AuthContext {
            user_id: admin,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: format!("/{realm_id}/g/{group_id}/**"),
                permission: Permission::WRITE,
            }]),
            session: None,
        };
        let id = Path(install_id.to_string());
        let body = Json(GrantsBody {
            grants: BTreeMap::new(),
        });
        let refused = put_grants(
            State(state.clone()),
            Extension(Some(restricted.clone())),
            id,
            body,
        );
        assert!(matches!(refused.await, Err(ServerError::Forbidden)));
        let after = read_config(&ctx, realm_id).await.unwrap();
        assert_eq!(after.installs[0].revision, 1);
        // The rule check itself: slots need an unrestricted session, no slots need none.
        let target = (Some(install_id), None);
        let checked = secret_session(&state, &restricted, target).await;
        assert!(matches!(checked, Err(ServerError::Forbidden)));
        let open = AuthContext {
            path_restrictions: None,
            ..restricted.clone()
        };
        assert!(secret_session(&state, &open, target).await.is_ok());
        assert!(
            secret_session(&state, &restricted, (None, None))
                .await
                .is_ok()
        );
    }

    #[test]
    fn sync_warnings() {
        let scope = InstallScope {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1u8; 32]),
            group_id: Ulid::from_parts(1, 1),
            install_id: Ulid::from_parts(2, 2),
        };
        let warned = |triggers: Value| {
            let manifest =
                serde_json::json!({"name": "p", "version": "0.1.0", "triggers": triggers});
            let manifest: Manifest = serde_json::from_value(manifest).unwrap();
            let none = (&BTreeMap::new(), &BTreeMap::new());
            summary(&manifest, "sha256:ab", &scope, none).warnings
        };
        let manifest = serde_json::json!({
            "name": "p", "version": "0.1.0",
            "settings": { "base": { "title": "Base", "type": "url" } },
            "connections": { "receipts": { "url": "{base}/x", "response": "full" } },
        });
        let manifest: Manifest = serde_json::from_value(manifest).unwrap();
        let none = (&BTreeMap::new(), &BTreeMap::new());
        let lines = summary(&manifest, "sha256:ab", &scope, none).connections;
        assert!(lines[0].full_answer);
        let sync_only = warned(serde_json::json!({"sync_failed": {}}));
        let mixed = warned(serde_json::json!({"sync_completed": {}, "dataset_created": {}}));
        assert_eq!(
            (sync_only, mixed),
            (
                vec![SYNC_WARNING.to_string()],
                vec![SYNC_WARNING.to_string()]
            )
        );
        assert!(warned(serde_json::json!({"manual": {}, "data_uploaded": {}})).is_empty());
    }
}
