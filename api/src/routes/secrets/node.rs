//! Routes for the named secrets of the node that serves the request; realm admins manage them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::secrets::SecretRecord;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::secrets::SecretChange;
use aruna_operations::secrets::node::{
    NodeChange, NodeSecretError, NodeSecretOperation, NodeSecretsOperation,
};
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
#[openapi(tags((name = "system/secrets", description = "Named node, realm, group and user secrets")))]
pub struct NodeSecretsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(NodeSecretsDoc::openapi())
        .routes(routes!(list_node_secrets, create_node_secret))
        .routes(routes!(
            get_node_secret,
            replace_node_secret,
            delete_node_secret
        ))
        .routes(routes!(rotate_node_secret))
}

/// Realm admins manage the secrets of every node, each on the node it lives on.
async fn manager(state: &Arc<ServerState>, auth: Option<AuthContext>) -> ServerResult<AuthContext> {
    let auth = require_unrestricted_auth(state, auth)?;
    let path = policy_admin_path(state.get_realm_id());
    ensure_permission(state, &auth, path, Permission::WRITE).await?;
    Ok(auth)
}

async fn change(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    change: SecretChange,
) -> ServerResult<Option<SecretRecord>> {
    let input = NodeChange {
        actor: auth.user_id,
        change,
        now_ms: unix_timestamp_millis(),
        write_id: Ulid::generate(),
    };
    drive(NodeSecretOperation::new(input), &state.get_ctx())
        .await
        .map_err(node_error)
}

fn node_error(error: NodeSecretError) -> ServerError {
    match error {
        NodeSecretError::Change(error) => change_error(error),
        other => ServerError::InternalError(other.to_string()),
    }
}

const SECRET_EXAMPLE: &str = r#"{"name": "catalog-key", "kind": "bearer", "description": "Catalog API token",
"settings": {}, "fields": ["token"], "uses": [{"consumer": {"plugin": {"plugin_id": "dataset-announcer",
"install_id": "01JINSTALL0123456789ABCDEF"}}, "delivery": "inject", "origins": ["https://catalog.example.org"],
"paths": ["/api/records"], "methods": ["POST"], "response": "status", "groups": ["01JABCDEF0123456789ABCDEFG"]}],
"readable": false, "incomplete": false, "revision": "01JREVIS00123456789ABCDEFG.01JVALUE00123456789ABCDEFG",
"created_by": "01JUSER00123456789ABCDEFGH@01JREALM0123456789ABCDEFG", "created_at_ms": 1762160400000,
"updated_by": "01JUSER00123456789ABCDEFGH@01JREALM0123456789ABCDEFG", "updated_at_ms": 1762160400000}"#;

#[utoipa::path(
    get,
    path = "/system/node/secrets",
    tag = "system/secrets",
    summary = "List the named secrets of this node",
    description = r#"Lists the secrets stored on the node that serves the request, by name, one page at a time.

**Authentication**: realm bearer token without path restrictions and WRITE on
`/{realm}/admin/config`. Every realm admin manages the secrets of every node.

**Behavior**
- Returns names, kinds, settings, field names, use rules and who changed them when. Field values
  are never returned.
- Node secrets never leave their node; other nodes list their own."#,
    params(SecretListQuery),
    responses(
        (status = 200, description = "One page of node secrets", body = SecretPage,
            example = json!({"secrets": [serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default()], "next_cursor": "catalog-key"})),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_node_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<SecretListQuery>,
) -> ServerResult<Json<SecretPage>> {
    manager(&state, auth).await?;
    let (cursor, limit) = query.page()?;
    let operation = NodeSecretsOperation::new(cursor.as_ref(), limit);
    let (infos, more) = (drive(operation, &state.get_ctx()).await).map_err(node_error)?;
    Ok(Json(page(infos, more)))
}

#[utoipa::path(
    get,
    path = "/system/node/secrets/{name}",
    tag = "system/secrets",
    summary = "Read one named secret of this node",
    description = r#"Returns one secret of the serving node without its field values.

**Authentication**: as for listing node secrets."#,
    params(("name" = String, Path, description = "Secret name")),
    responses(
        (status = 200, description = "The secret", body = SecretView,
            example = json!(serde_json::from_str::<serde_json::Value>(SECRET_EXAMPLE).unwrap_or_default())),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No secret with this name on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_node_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
) -> ServerResult<Json<SecretView>> {
    manager(&state, auth).await?;
    let name = parse_name(&name)?;
    let operation = NodeSecretsOperation::from_name(&name);
    let (infos, _) = (drive(operation, &state.get_ctx()).await).map_err(node_error)?;
    let info = infos.into_iter().find(|info| info.name == name);
    info.map(|info| Json(view(info)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(
    post,
    path = "/system/node/secrets",
    tag = "system/secrets",
    summary = "Store a named secret on this node",
    description = r#"Stores a secret in the node vault of the serving node, sealed with a key only that node derives.

**Authentication**: as for listing node secrets.

**Behavior**
- The record, the sealed value and the audit row commit together; nothing replicates.
- Kinds: `bearer` (`token`), `basic` (`username`, `password`), `header` (`value`, setting
  `header`), `hmac` (`key`, settings `header` and optional `prefix`) and `fields` (any fields).
- Use rules name a plugin install, a delivery (`inject`, `verify` or `value`), and for `inject`
  the HTTPS origins, optional path prefixes, methods and the response exposure (`status`,
  `{"fields": {"pointers": [...], "headers": [...]}}` or `full`). Node secrets list the groups
  whose installs may use them in `groups`.
- Field values are never returned. `readable` is refused: only user secrets read back.

**Errors**
- 400 for an invalid name, kind, field set, setting or use rule.
- 409 when a secret with this name exists on this node."#,
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
        (status = 409, description = "A secret with this name exists on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_node_secret(
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
    path = "/system/node/secrets/{name}",
    tag = "system/secrets",
    summary = "Replace the rules of a node secret",
    description = r#"Replaces everything but the value; the kind stays.

**Authentication**: as for listing node secrets.

**Behavior**
- With `If-Match: <revision>` the change applies only to that state of the secret."#,
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
        (status = 404, description = "No secret with this name on this node", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_node_secret(
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
    path = "/system/node/secrets/{name}/rotate",
    tag = "system/secrets",
    summary = "Set a new value for a node secret",
    description = r#"Seals new field values under a new value id and removes the old value in one transaction.

**Authentication**: as for listing node secrets.

**Behavior**
- Description, settings and use rules stay.
- Rotate the credential at its provider too: anything that used the old value keeps it."#,
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
        (status = 404, description = "No secret with this name on this node", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn rotate_node_secret(
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
    path = "/system/node/secrets/{name}",
    tag = "system/secrets",
    summary = "Delete a node secret",
    description = r#"Removes the record and the sealed value from this node.

**Authentication**: as for listing node secrets."#,
    params(
        ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to")
    ),
    responses(
        (status = 204, description = "The secret is gone"),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or not a realm admin", body = ErrorResponse),
        (status = 404, description = "No secret with this name on this node", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_node_secret(
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
