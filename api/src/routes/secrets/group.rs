//! Routes for group secrets, which group admins manage and which replicate as last-writer-wins
//! registers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::secrets::SecretChange;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{
    CreateSecretRequest, ReplaceSecretRequest, RotateSecretRequest, SecretListQuery, SecretPage,
    SecretView, entry_example, entry_view, expected, owner_change, owner_create, owner_for,
    owner_list, owner_read, parse_name, replace_change, rotate_change,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
pub struct GroupSecretsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(GroupSecretsDoc::openapi())
        .routes(routes!(list_group_secrets, create_group_secret))
        .routes(routes!(
            get_group_secret,
            replace_group_secret,
            delete_group_secret
        ))
        .routes(routes!(rotate_group_secret))
}

#[utoipa::path(get, path = "/data/groups/{group_id}/secrets", tag = "system/secrets",
    summary = "List the secrets of a group",
    description = r#"Lists the group's secrets by name, one page at a time, without field values.

**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call."#,
    params(("group_id" = String, Path, description = "Group id"), SecretListQuery),
    responses(
        (status = 200, description = "One page of group secrets", body = SecretPage, example = json!({"secrets": [entry_example()]})),
        (status = 400, description = "Invalid group id or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn list_group_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Query(query): Query<SecretListQuery>,
) -> ServerResult<Json<SecretPage>> {
    let (_, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    Ok(Json(owner_list(&state, secret_owner, &query).await?))
}

#[utoipa::path(get, path = "/data/groups/{group_id}/secrets/{name}", tag = "system/secrets",
    summary = "Read one secret of a group",
    description = r#"Returns one group secret from this node's view, without field values.

**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call."#,
    params(("group_id" = String, Path, description = "Group id"), ("name" = String, Path, description = "Secret name")),
    responses(
        (status = 200, description = "The secret", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid group id or name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn get_group_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, name)): Path<(String, String)>,
) -> ServerResult<Json<SecretView>> {
    let (_, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    Ok(Json(owner_read(&state, secret_owner, &name).await?))
}

#[utoipa::path(post, path = "/data/groups/{group_id}/secrets", tag = "system/secrets",
    summary = "Store a secret of a group",
    description = concat!("Stores a group secret, sealed to the realm key.", "\n\n", r#"**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call.

**Behavior**
- Use rules name the group's plugin installs; a group secret never names other groups.
  `readable` is refused.
- Every infrastructure node holds the sealed documents; only management nodes open values. A
  write applies on the serving node at once and reaches other nodes through document sync.
- Writes are last-writer-wins; `If-Match` compares with the serving node's view only.

**Errors**
- 400 for an invalid name, kind, field set, setting or use rule.
- 409 when the name exists in this node's view."#),
    params(("group_id" = String, Path, description = "Group id")),
    request_body(content = CreateSecretRequest, example = json!({"name": "catalog-key", "kind": "bearer", "fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}})),
    responses(
        (status = 201, description = "The stored secret, without field values", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse),
        (status = 409, description = "The name exists in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn create_group_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateSecretRequest>,
) -> ServerResult<(StatusCode, Json<SecretView>)> {
    let (auth, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    if request.readable {
        return Err(ServerError::BadRequestReason(
            "only user secrets can be readable".to_string(),
        ));
    }
    let (create, _) = owner_create(request)?;
    let stored = owner_change(&state, &auth, secret_owner, create, false).await?;
    let entry = stored.ok_or_else(|| ServerError::InternalError("nothing stored".to_string()))?;
    Ok((StatusCode::CREATED, Json(entry_view(entry))))
}

#[utoipa::path(put, path = "/data/groups/{group_id}/secrets/{name}", tag = "system/secrets",
    summary = "Replace the rules of a group secret",
    description = r#"Writes a new description, settings and use rules; the value and kind stay.

**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call."#,
    params(("group_id" = String, Path, description = "Group id"), ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    request_body(content = ReplaceSecretRequest, example = json!({"description": "Catalog API token", "settings": {}, "uses": []})),
    responses(
        (status = 200, description = "The changed secret", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn replace_group_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, name)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<ReplaceSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let (auth, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    let replace = replace_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = owner_change(&state, &auth, secret_owner, replace, false).await?;
    stored
        .map(|entry| Json(entry_view(entry)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(post, path = "/data/groups/{group_id}/secrets/{name}/rotate", tag = "system/secrets",
    summary = "Set a new value for a group secret",
    description = r#"Writes a new value; description, settings and use rules stay.

**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call.

**Behavior**
- A node that has not received the new value still uses the old one until it does."#,
    params(("group_id" = String, Path, description = "Group id"), ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    request_body(content = RotateSecretRequest, example = json!({"fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}})),
    responses(
        (status = 200, description = "The rotated secret, without field values", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn rotate_group_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, name)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<RotateSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let (auth, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    let rotate = rotate_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = owner_change(&state, &auth, secret_owner, rotate, false).await?;
    stored
        .map(|entry| Json(entry_view(entry)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(delete, path = "/data/groups/{group_id}/secrets/{name}", tag = "system/secrets",
    summary = "Delete a group secret",
    description = r#"Deletes the secret by writing a tombstone.

**Authentication**: realm bearer token without path restrictions from a group admin or a realm
groups administrator. User devices refuse the call.

**Behavior**
- Old sealed values stay in replicated history; rotate the credential at its provider to revoke
  it."#,
    params(("group_id" = String, Path, description = "Group id"), ("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    responses(
        (status = 204, description = "The secret is deleted in this node's view"),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Not a group admin, a path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn delete_group_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> ServerResult<StatusCode> {
    let (auth, secret_owner) = owner_for(&state, auth, Some(&group_id)).await?;
    let delete = SecretChange::Delete {
        name: parse_name(&name)?,
        expected: expected(&headers)?,
    };
    owner_change(&state, &auth, secret_owner, delete, false).await?;
    Ok(StatusCode::NO_CONTENT)
}
