//! Routes for named realm secrets, which realm admins store in the realm vault through a
//! management node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::realm_vault::RealmVaultKey;
use aruna_core::secrets::SecretRecord;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::secrets::SecretChange;
use aruna_operations::secrets::realm::{RealmChange, RealmSecretError, RealmSecretOperation};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{
    CreateSecretRequest, ReplaceSecretRequest, RotateSecretRequest, SecretListQuery, SecretPage,
    SecretView, change_error, create_change, expected, page, parse_name, replace_change,
    rotate_change, view,
};
use crate::auth::{ensure_permission, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
pub struct RealmSecretsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(RealmSecretsDoc::openapi())
        .routes(routes!(list_realm_secrets, create_realm_secret))
        .routes(routes!(
            get_realm_secret,
            replace_realm_secret,
            delete_realm_secret
        ))
        .routes(routes!(rotate_realm_secret))
}

async fn manager(state: &Arc<ServerState>, auth: Option<AuthContext>) -> ServerResult<AuthContext> {
    let auth = require_unrestricted_auth(state, auth)?;
    let path = policy_admin_path(state.get_realm_id());
    ensure_permission(state, &auth, path, Permission::WRITE).await?;
    Ok(auth)
}

/// This node's copy of the realm secret records, sorted by name.
async fn records(state: &Arc<ServerState>) -> ServerResult<Vec<SecretRecord>> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        GetConfigError::DocumentNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(config.secrets)
}

async fn change(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    change: SecretChange,
) -> ServerResult<Option<SecretRecord>> {
    // The relay sends writes to a management node; only it holds the realm signing key.
    let NodeCapabilities::Management {
        realm_signing_key, ..
    } = state.node_capabilities()
    else {
        return Err(ServerError::Forbidden);
    };
    let input = RealmChange {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: state.get_realm_id(),
        },
        vault_key: RealmVaultKey::derive(realm_signing_key),
        change,
        now_ms: unix_timestamp_millis(),
        write_id: Ulid::generate(),
    };
    drive(RealmSecretOperation::new(input), &state.get_ctx())
        .await
        .map_err(realm_error)
}

fn realm_error(error: RealmSecretError) -> ServerError {
    match error {
        RealmSecretError::Change(error) => change_error(error),
        RealmSecretError::ConfigMissing => ServerError::NotFound,
        RealmSecretError::NotManagementNode => ServerError::Forbidden,
        RealmSecretError::Storage(StorageError::TransactionConflict) => ServerError::Conflict(
            "concurrent realm configuration update conflict; retry".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    }
}

const SECRET_EXAMPLE: &str = r#"{"name": "catalog-key", "kind": "bearer", "description": "Catalog API token",
"settings": {}, "fields": ["token"], "uses": [{"consumer": {"plugin": {"plugin_id": "dataset-announcer",
"install_id": "01JINSTALL0123456789ABCDEF"}}, "delivery": "inject", "origins": ["https://catalog.example.org"],
"paths": [], "methods": [], "response": "status", "groups": ["01JABCDEF0123456789ABCDEFG"]}],
"readable": false, "incomplete": false, "revision": "01JREVIS00123456789ABCDEFG.01JVALUE00123456789ABCDEFG",
"created_by": "01JUSER00123456789ABCDEFGH@01JREALM0123456789ABCDEFG", "created_at_ms": 1762160400000,
"updated_by": "01JUSER00123456789ABCDEFGH@01JREALM0123456789ABCDEFG", "updated_at_ms": 1762160400000}"#;

#[utoipa::path(
    get,
    path = "/system/realm/secrets",
    tag = "system/secrets",
    summary = "List the named realm secrets",
    description = r#"Lists the realm secrets by name, one page at a time, from this node's copy of the realm config.

**Authentication**: realm bearer token without path restrictions and WRITE on
`/{realm}/admin/config`.

**Behavior**
- Returns names, kinds, settings, field names, use rules and who changed them when. Field values
  are never returned.
- Changes reach nodes as the realm configuration replicates. Two admins writing one name at
  the same time leave it absent until an admin writes it again."#,
    params(SecretListQuery),
    responses(
        (status = 200, description = "One page of realm secrets", body = SecretPage,
            example = json!({"secrets": [serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default()]})),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_realm_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<SecretListQuery>,
) -> ServerResult<Json<SecretPage>> {
    manager(&state, auth).await?;
    let (cursor, limit) = query.page()?;
    let mut infos: Vec<_> = (records(&state).await?.into_iter())
        .filter(|info| cursor.as_ref().is_none_or(|cursor| info.name > *cursor))
        .take(limit + 1)
        .collect();
    let more = infos.len() > limit;
    infos.truncate(limit);
    Ok(Json(page(infos, more)))
}

#[utoipa::path(
    get,
    path = "/system/realm/secrets/{name}",
    tag = "system/secrets",
    summary = "Read one named realm secret",
    description = r#"Returns one realm secret without its field values, from this node's copy of the realm config.

**Authentication**: as for listing realm secrets."#,
    params(("name" = String, Path, description = "Secret name")),
    responses(
        (status = 200, description = "The secret", body = SecretView,
            example = json!(serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default())),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No realm secret with this name", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
) -> ServerResult<Json<SecretView>> {
    manager(&state, auth).await?;
    let name = parse_name(&name)?;
    let info = records(&state)
        .await?
        .into_iter()
        .find(|info| info.name == name);
    info.map(|info| Json(view(info)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    post,
    path = "/system/realm/secrets",
    tag = "system/secrets",
    summary = "Store a named realm secret",
    description = r#"Stores a secret in the realm vault, sealed with a key every holder of the realm signing key derives.

**Authentication**: realm bearer token without path restrictions and WRITE on
`/{realm}/admin/config`. A management node serves the call and every other node relays it to one.

**Behavior**
- The record goes into the realm configuration by name.
- Kinds and use rules as for node secrets; a rule lists the groups whose installs may use it.
- Field values are never returned. Only management nodes open the value; other nodes never fetch
  or cache it. `readable` is refused.
- `If-Match` on later changes compares with the serving node's copy only.

**Errors**
- 400 for an invalid name, kind, field set, setting or use rule.
- 409 when the name exists, or another realm configuration update won the race."#,
    request_body(content = CreateSecretRequest, example = json!({
        "name": "catalog-key", "kind": "bearer", "description": "Catalog API token",
        "fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}
    })),
    responses(
        (status = 201, description = "The stored secret, without field values", body = SecretView,
            example = json!(serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default())),
        (status = 400, description = "Invalid name, kind, fields, settings or use rules", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 409, description = "The name exists, or a concurrent realm configuration update; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_realm_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateSecretRequest>,
) -> ServerResult<(StatusCode, Json<SecretView>)> {
    let auth = manager(&state, auth).await?;
    let stored = change(&state, &auth, create_change(request)?).await?;
    let info = stored.ok_or_else(|| ServerError::InternalError("nothing stored".to_string()))?;
    Ok((StatusCode::CREATED, Json(view(info))))
}

#[utoipa::path(
    put,
    path = "/system/realm/secrets/{name}",
    tag = "system/secrets",
    summary = "Replace the rules of a realm secret",
    description = r#"Replaces everything but the value; the kind stays.

**Authentication**: as for storing a realm secret; relayed to a management node."#,
    params(
        ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to")
    ),
    request_body(content = ReplaceSecretRequest, example = json!({
        "description": "Catalog API token", "settings": {}, "uses": []
    })),
    responses(
        (status = 200, description = "The changed secret", body = SecretView,
            example = json!(serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default())),
        (status = 400, description = "Invalid name, settings or use rules", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No realm secret with this name", body = ErrorResponse),
        (status = 409, description = "A concurrent realm configuration update; retry", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_realm_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReplaceSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let auth = manager(&state, auth).await?;
    let change_request = replace_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = change(&state, &auth, change_request).await?;
    stored
        .map(|info| Json(view(info)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    post,
    path = "/system/realm/secrets/{name}/rotate",
    tag = "system/secrets",
    summary = "Set a new value for a realm secret",
    description = r#"Seals new field values under a new value id and retires the old value in one admin document.

**Authentication**: as for storing a realm secret; relayed to a management node.

**Behavior**
- Description, settings and use rules stay.
- A node that has not received the change yet still uses the old value until it does. Rotate the
  credential at its provider too."#,
    params(
        ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to")
    ),
    request_body(content = RotateSecretRequest, example = json!({
        "fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}
    })),
    responses(
        (status = 200, description = "The rotated secret, without field values", body = SecretView,
            example = json!(serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default())),
        (status = 400, description = "Invalid name or fields", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No realm secret with this name", body = ErrorResponse),
        (status = 409, description = "A concurrent realm configuration update; retry", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn rotate_realm_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RotateSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let auth = manager(&state, auth).await?;
    let change_request = rotate_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = change(&state, &auth, change_request).await?;
    stored
        .map(|info| Json(view(info)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    delete,
    path = "/system/realm/secrets/{name}",
    tag = "system/secrets",
    summary = "Delete a realm secret",
    description = r#"Removes the record and retires the sealed value from the realm configuration.

**Authentication**: as for storing a realm secret; relayed to a management node.

**Behavior**
- Older sealed values stay in replicated history; holders of the realm signing key can still
  open them. Rotate the credential at its provider to revoke it."#,
    params(
        ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to")
    ),
    responses(
        (status = 204, description = "The secret is gone"),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No realm secret with this name", body = ErrorResponse),
        (status = 409, description = "A concurrent realm configuration update; retry", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_realm_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> ServerResult<StatusCode> {
    let auth = manager(&state, auth).await?;
    let delete = SecretChange::Delete {
        name: parse_name(&name)?,
        expected: expected(&headers)?,
    };
    change(&state, &auth, delete).await?;
    Ok(StatusCode::NO_CONTENT)
}
