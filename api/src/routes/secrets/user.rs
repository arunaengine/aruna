//! Routes for the caller's own user secrets and the owner read-back of readable ones on a
//! management node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::realm_vault::RealmSealKey;
use aruna_core::registers::SecretOwner;
use aruna_core::secrets::SecretFields;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_operations::driver::drive;
use aruna_operations::secrets::SecretChange;
use aruna_operations::secrets::owner::{OwnerSecretError, ReadbackOperation, open_value};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{
    CreateSecretRequest, ReplaceSecretRequest, RotateSecretRequest, SecretListQuery, SecretPage,
    SecretView, change_error, entry_example, entry_view, expected, owner_change, owner_create,
    owner_error, owner_for, owner_list, owner_read, parse_name, replace_change, rotate_change,
};
use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
pub struct UserSecretsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(UserSecretsDoc::openapi())
        .routes(routes!(list_user_secrets, create_user_secret))
        .routes(routes!(
            get_user_secret,
            replace_user_secret,
            delete_user_secret
        ))
        .routes(routes!(rotate_user_secret))
        .routes(routes!(read_user_secret))
}

/// The field values of a readable user secret.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct SecretValueView {
    pub fields: BTreeMap<String, String>,
    pub revision: String,
}

impl std::fmt::Debug for SecretValueView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretValueView")
            .field("fields", &self.fields.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

#[utoipa::path(get, path = "/access/users/me/secrets", tag = "system/secrets",
    summary = "List the caller's own secrets",
    description = r#"Lists the caller's user secrets by name, one page at a time, without field values.

**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call."#,
    params(SecretListQuery),
    responses(
        (status = 200, description = "One page of user secrets", body = SecretPage, example = json!({"secrets": [entry_example()]})),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn list_user_secrets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<SecretListQuery>,
) -> ServerResult<Json<SecretPage>> {
    let (_, secret_owner) = owner_for(&state, auth, None).await?;
    Ok(Json(owner_list(&state, secret_owner, &query).await?))
}

#[utoipa::path(get, path = "/access/users/me/secrets/{name}", tag = "system/secrets",
    summary = "Read one of the caller's secrets",
    description = r#"Returns one of the caller's secrets from this node's view, without field values.

**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call."#,
    params(("name" = String, Path, description = "Secret name")),
    responses(
        (status = 200, description = "The secret", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn get_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
) -> ServerResult<Json<SecretView>> {
    let (_, secret_owner) = owner_for(&state, auth, None).await?;
    Ok(Json(owner_read(&state, secret_owner, &name).await?))
}

#[utoipa::path(post, path = "/access/users/me/secrets", tag = "system/secrets",
    summary = "Store one of the caller's secrets",
    description = concat!("Stores a user secret of the caller, sealed to the realm key.", "\n\n", r#"**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call.

**Behavior**
- Plugins never use user secrets, so a user secret takes no use rules.
- `readable: true` lets the owner read the value back later; it is set at create only.
- Every infrastructure node holds the sealed documents; only management nodes open values.
- Writes are last-writer-wins; `If-Match` compares with the serving node's view only.

**Errors**
- 400 for an invalid name, kind, field set, setting or any use rule.
- 409 when the name exists in this node's view."#),
    request_body(content = CreateSecretRequest, example = json!({"name": "orcid-token", "kind": "bearer", "fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}, "readable": true})),
    responses(
        (status = 201, description = "The stored secret, without field values", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse),
        (status = 409, description = "The name exists in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn create_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateSecretRequest>,
) -> ServerResult<(StatusCode, Json<SecretView>)> {
    let (auth, secret_owner) = owner_for(&state, auth, None).await?;
    let (create, readable) = owner_create(request)?;
    let stored = owner_change(&state, &auth, secret_owner, create, readable).await?;
    let entry = stored.ok_or_else(|| ServerError::InternalError("nothing stored".to_string()))?;
    Ok((StatusCode::CREATED, Json(entry_view(entry))))
}

#[utoipa::path(put, path = "/access/users/me/secrets/{name}", tag = "system/secrets",
    summary = "Replace the settings of a user secret",
    description = r#"Writes a new description and settings; the value and kind stay.

**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call."#,
    params(("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    request_body(content = ReplaceSecretRequest, example = json!({"description": "ORCID token", "settings": {}, "uses": []})),
    responses(
        (status = 200, description = "The changed secret", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn replace_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ReplaceSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let (auth, secret_owner) = owner_for(&state, auth, None).await?;
    let replace = replace_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = owner_change(&state, &auth, secret_owner, replace, false).await?;
    stored
        .map(|entry| Json(entry_view(entry)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(post, path = "/access/users/me/secrets/{name}/rotate", tag = "system/secrets",
    summary = "Set a new value for a user secret",
    description = r#"Writes a new value; the description and settings stay.

**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call."#,
    params(("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    request_body(content = RotateSecretRequest, example = json!({"fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}})),
    responses(
        (status = 200, description = "The rotated secret, without field values", body = SecretView, example = json!(entry_example())),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn rotate_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<RotateSecretRequest>,
) -> ServerResult<Json<SecretView>> {
    let (auth, secret_owner) = owner_for(&state, auth, None).await?;
    let rotate = rotate_change(parse_name(&name)?, request, expected(&headers)?);
    let stored = owner_change(&state, &auth, secret_owner, rotate, false).await?;
    stored
        .map(|entry| Json(entry_view(entry)))
        .ok_or(ServerError::NotFound)
}

#[utoipa::path(delete, path = "/access/users/me/secrets/{name}", tag = "system/secrets",
    summary = "Delete one of the caller's secrets",
    description = r#"Deletes one of the caller's secrets by writing a tombstone.

**Authentication**: the owner's realm bearer token without path restrictions. User devices refuse
the call.

**Behavior**
- Old sealed values stay in replicated history."#,
    params(("name" = String, Path, description = "Secret name"),
        ("If-Match" = Option<String>, Header, description = "The revision the change applies to, in this node's view")),
    responses(
        (status = 204, description = "The secret is deleted in this node's view"),
        (status = 400, description = "Invalid input", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, or a user device", body = ErrorResponse),
        (status = 404, description = "No secret with this name in this node's view", body = ErrorResponse),
        (status = 412, description = "The secret changed since the given revision in this node's view", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn delete_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> ServerResult<StatusCode> {
    let (auth, secret_owner) = owner_for(&state, auth, None).await?;
    let delete = SecretChange::Delete {
        name: parse_name(&name)?,
        expected: expected(&headers)?,
    };
    owner_change(&state, &auth, secret_owner, delete, false).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/access/users/me/secrets/{name}/value", tag = "system/secrets",
    summary = "Read back a readable user secret",
    description = r#"Returns the field values of a user secret created with `readable: true`.

**Authentication**: the owner's realm bearer token without path restrictions. A management node
serves the call and checks the token itself; every other node relays it to one.

**Behavior**
- Uses the serving management node's current view of the secret.
- An audit row `secret_read` is written before the value is opened; if it cannot be written,
  no value is returned."#,
    params(("name" = String, Path, description = "Secret name")),
    responses(
        (status = 200, description = "The field values", body = SecretValueView, example = json!({"fields": {"token": "EXAMPLE-TOKEN-PLACEHOLDER"}, "revision": "01JLIFE000123456789ABCDEFG.01JINFO000123456789ABCDEFG.01JVALUE00123456789ABCDEFG"})),
        (status = 400, description = "Invalid name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token, a secret that is not readable, or a node that cannot open values", body = ErrorResponse),
        (status = 404, description = "No complete secret with this name in this node's view", body = ErrorResponse),
        (status = 503, description = "The audit row could not be written, or no management node was reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = [])))]
pub async fn read_user_secret(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(name): Path<String>,
) -> ServerResult<Json<SecretValueView>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let NodeCapabilities::Management {
        realm_signing_key, ..
    } = state.node_capabilities()
    else {
        return Err(ServerError::Forbidden);
    };
    let operation = ReadbackOperation::new(
        state.get_realm_id(),
        SecretOwner::User(auth.user_id),
        parse_name(&name)?,
        auth.user_id,
    );
    let entry = drive(operation, &state.get_ctx())
        .await
        .map_err(|error| match error {
            OwnerSecretError::Change(error) => change_error(error),
            OwnerSecretError::NotReadable => ServerError::Forbidden,
            OwnerSecretError::Storage(_) => {
                ServerError::ServiceUnavailableReason("the audit row was not written".to_string())
            }
            other => ServerError::InternalError(other.to_string()),
        })?;
    let key = RealmSealKey::derive(realm_signing_key);
    let bytes = open_value(&key, &state.get_realm_id(), &entry).map_err(owner_error)?;
    let fields = SecretFields::decode(&bytes)
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(Json(SecretValueView {
        fields: (fields.names().zip(fields.values()))
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        revision: entry.revision(),
    }))
}
