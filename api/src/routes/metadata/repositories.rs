//! Serves the group routes that register Invenio and OAI-PMH repository connectors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_operations::driver::drive;
use aruna_operations::harvest::create_connector::{
    CreateConnectorError, CreateConnectorInput, CreateConnectorOperation, INVENIO_COMMUNITY,
};
use aruna_operations::harvest::delete_connector::DeleteRepositoryOperation;
use aruna_operations::harvest::read_connector::{
    ConnectorView, GetRepositoryOperation, ListRepositoryOperation, ReadConnectorError,
};
use aruna_operations::harvest::update_connector::{
    UpdateConnectorError, UpdateConnectorInput, UpdateRepositoryOperation,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::ensure_metadata_scope;
use crate::server::state::ServerState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiRepositoryKind {
    Invenio,
    OaiPmh,
}

impl From<ApiRepositoryKind> for RepositoryConnectorKind {
    fn from(value: ApiRepositoryKind) -> Self {
        match value {
            ApiRepositoryKind::Invenio => Self::Invenio,
            ApiRepositoryKind::OaiPmh => Self::OaiPmh,
        }
    }
}

impl From<RepositoryConnectorKind> for ApiRepositoryKind {
    fn from(value: RepositoryConnectorKind) -> Self {
        match value {
            RepositoryConnectorKind::Invenio => Self::Invenio,
            RepositoryConnectorKind::OaiPmh => Self::OaiPmh,
        }
    }
}

/// Body of both create and replace; only `token` is accepted as a secret.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RepositoryRequest {
    pub name: String,
    pub kind: ApiRepositoryKind,
    pub endpoint: String,
    #[serde(default)]
    pub community: Option<String>,
    /// Omitted on replace keeps the stored secret; an empty object removes it.
    #[serde(default)]
    pub secret_config: Option<HashMap<String, String>>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for RepositoryRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepositoryRequest")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("endpoint", &self.endpoint)
            .field("community", &self.community)
            .field(
                "secret_keys",
                &self
                    .secret_config
                    .as_ref()
                    .map(|config| config.keys().collect::<Vec<_>>()),
            )
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RepositoryResponse {
    pub connector_id: String,
    pub group_id: String,
    pub name: String,
    pub kind: ApiRepositoryKind,
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub community: Option<String>,
    pub has_secret_config: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RepositoryList {
    pub connectors: Vec<RepositoryResponse>,
}

#[utoipa::path(
    post,
    path = "/metadata/groups/{group_id}/repositories",
    tag = "metadata/repositories",
    summary = "Register a repository connector",
    description = r#"Registers an Invenio or OAI-PMH repository connector for a group.

**Authentication**: realm bearer token with WRITE on the group's metadata path.

**Behavior**
- An Invenio endpoint is the REST API root, such as `https://zenodo.org/api/`, and must use https
  unless it points to a loopback host. `community` names the community new records target.
- The optional `secret_config.token` is used only to read private records during imports. It is
  never returned; `has_secret_config` states whether one is stored.

**Errors**
- 400 names the rejected field."#,
    params(("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID")),
    request_body(content = RepositoryRequest, example = json!({
        "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/",
        "community": "aruna", "secret_config": {"token": "<personal-access-token>"}
    })),
    responses(
        (status = 201, description = "The stored connector, secrets excluded", body = RepositoryResponse, example = json!({
            "connector_id": "01JCNCTR0123456789ABCDEFGH", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/",
            "community": "aruna", "has_secret_config": true,
            "created_at": "2026-09-23T10:00:00+00:00", "updated_at": "2026-09-23T10:00:00+00:00"
        })),
        (status = 400, description = "Invalid group id, name, endpoint or configuration", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's metadata path", body = ErrorResponse),
        (status = 409, description = "The group is being deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<RepositoryRequest>,
) -> ServerResult<(StatusCode, Json<RepositoryResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::WRITE).await?;
    let result = drive(
        CreateConnectorOperation::new(CreateConnectorInput {
            group_id,
            created_by: auth.user_id,
            name: request.name,
            kind: request.kind.into(),
            endpoint: request.endpoint,
            public_config: public_config(request.community),
            secret_config: request.secret_config.unwrap_or_default(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(create_error)?;
    Ok((
        StatusCode::CREATED,
        Json(response(ConnectorView {
            connector: result.connector,
            has_secret_config: result.has_secret_config,
        })),
    ))
}

#[utoipa::path(
    get,
    path = "/metadata/groups/{group_id}/repositories",
    tag = "metadata/repositories",
    summary = "List a group's repository connectors",
    description = r#"Returns every repository connector the group registered on this node.

**Authentication**: realm bearer token with READ on the group's metadata path.

**Behavior**
- Connectors are ordered by connector id and returned in one response without paging.
- Secrets are never included; `has_secret_config` states whether one is stored."#,
    params(("group_id" = String, Path, description = "Group whose connectors are listed, as a 26-character ULID")),
    responses(
        (status = 200, description = "The group's connectors, secrets excluded", body = RepositoryList, example = json!({
            "connectors": [{
                "connector_id": "01JCNCTR0123456789ABCDEFGH", "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/",
                "has_secret_config": false,
                "created_at": "2026-09-23T10:00:00+00:00", "updated_at": "2026-09-23T10:00:00+00:00"
            }]
        })),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the group's metadata path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_repositories(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<RepositoryList>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::READ).await?;
    let views = drive(ListRepositoryOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(read_error)?;
    Ok(Json(RepositoryList {
        connectors: views.into_iter().map(response).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/metadata/groups/{group_id}/repositories/{connector_id}",
    tag = "metadata/repositories",
    summary = "Read one repository connector",
    description = r#"Returns one repository connector of the group.

**Authentication**: realm bearer token with READ on the group's metadata path.

**Behavior**
- A connector id of another group reads as not found.
- Secrets are never returned."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to read, as a 26-character ULID")
    ),
    responses(
        (status = 200, description = "The stored connector, secrets excluded", body = RepositoryResponse, example = json!({
            "connector_id": "01JCNCTR0123456789ABCDEFGH", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/",
            "has_secret_config": false,
            "created_at": "2026-09-23T10:00:00+00:00", "updated_at": "2026-09-23T10:00:00+00:00"
        })),
        (status = 400, description = "An id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the group's metadata path", body = ErrorResponse),
        (status = 404, description = "No such connector in this group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<Json<RepositoryResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector(&connector_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::READ).await?;
    let view = drive(
        GetRepositoryOperation::new(group_id, connector_id),
        &state.get_ctx(),
    )
    .await
    .map_err(read_error)?;
    Ok(Json(response(view)))
}

#[utoipa::path(
    put,
    path = "/metadata/groups/{group_id}/repositories/{connector_id}",
    tag = "metadata/repositories",
    summary = "Replace a repository connector",
    description = r#"Replaces a connector's name, kind, endpoint and community.

**Authentication**: realm bearer token with WRITE on the group's metadata path.

**Behavior**
- An omitted `secret_config` keeps the stored token, an empty object removes it and a new
  token replaces it.
- A changed endpoint needs a new token or an explicit removal, so a stored token never follows
  the connector to another host.
- The id, group, kind, creator and creation time are kept; a different kind answers 400."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to replace, as a 26-character ULID")
    ),
    request_body(content = RepositoryRequest, example = json!({
        "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/", "community": "aruna"
    })),
    responses(
        (status = 200, description = "The connector after the replacement, secrets excluded", body = RepositoryResponse, example = json!({
            "connector_id": "01JCNCTR0123456789ABCDEFGH", "group_id": "01JABCDEF0123456789ABCDEFG",
            "name": "zenodo", "kind": "invenio", "endpoint": "https://zenodo.org/api/",
            "community": "aruna", "has_secret_config": true,
            "created_at": "2026-09-23T10:00:00+00:00", "updated_at": "2026-09-23T11:00:00+00:00"
        })),
        (status = 400, description = "Invalid id or configuration, or an endpoint change that keeps the token", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's metadata path", body = ErrorResponse),
        (status = 404, description = "No such connector in this group", body = ErrorResponse),
        (status = 409, description = "The group is being deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Json(request): Json<RepositoryRequest>,
) -> ServerResult<Json<RepositoryResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector(&connector_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::WRITE).await?;
    let view = drive(
        UpdateRepositoryOperation::new(UpdateConnectorInput {
            group_id,
            connector_id,
            name: request.name,
            kind: request.kind.into(),
            endpoint: request.endpoint,
            public_config: public_config(request.community),
            secret_config: request.secret_config,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(update_error)?;
    Ok(Json(response(view)))
}

#[utoipa::path(
    delete,
    path = "/metadata/groups/{group_id}/repositories/{connector_id}",
    tag = "metadata/repositories",
    summary = "Delete a repository connector",
    description = r#"Removes a repository connector and its stored token together.

**Authentication**: realm bearer token with WRITE on the group's metadata path.

**Behavior**
- Repeating the call answers 404.
- A connector that repository links on this node still use is kept and answers 409.
- Existing reference imports keep their repository file URLs and read them without the removed
  token from then on."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to delete, as a 26-character ULID")
    ),
    responses(
        (status = 204, description = "Connector and token deleted"),
        (status = 400, description = "An id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's metadata path", body = ErrorResponse),
        (status = 404, description = "No such connector in this group", body = ErrorResponse),
        (status = 409, description = "Repository links still use the connector", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector(&connector_id)?;
    ensure_metadata_scope(&state, &auth, group_id, Permission::WRITE).await?;
    drive(
        DeleteRepositoryOperation::new(group_id, connector_id),
        &state.get_ctx(),
    )
    .await
    .map_err(update_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn public_config(community: Option<String>) -> HashMap<String, String> {
    community
        .map(|community| HashMap::from([(INVENIO_COMMUNITY.to_string(), community)]))
        .unwrap_or_default()
}

fn parse_connector(connector_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(connector_id).map_err(|_| ServerError::BadRequest)
}

fn response(view: ConnectorView) -> RepositoryResponse {
    let mut connector = view.connector;
    RepositoryResponse {
        connector_id: connector.connector_id.to_string(),
        group_id: connector.group_id.to_string(),
        name: connector.name,
        kind: connector.kind.into(),
        endpoint: connector.endpoint,
        community: connector.public_config.remove(INVENIO_COMMUNITY),
        has_secret_config: view.has_secret_config,
        created_at: timestamp(connector.created_at),
        updated_at: timestamp(connector.updated_at),
    }
}

fn timestamp(value: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

fn create_error(error: CreateConnectorError) -> ServerError {
    match error {
        CreateConnectorError::GroupWrite(GroupWriteError::Frozen | GroupWriteError::Deleted) => {
            ServerError::Conflict(error.to_string())
        }
        CreateConnectorError::GroupWrite(_)
        | CreateConnectorError::Storage(_)
        | CreateConnectorError::Conversion(_)
        | CreateConnectorError::Failed
        | CreateConnectorError::InvalidStateEvent { .. } => {
            ServerError::InternalError(error.to_string())
        }
        CreateConnectorError::EmptyName
        | CreateConnectorError::EmptyEndpoint
        | CreateConnectorError::AmbiguousEndpoint(_)
        | CreateConnectorError::InsecureEndpoint
        | CreateConnectorError::UnknownPublicKey(_)
        | CreateConnectorError::UnknownSecretKey(_)
        | CreateConnectorError::EmptyValue(_) => ServerError::BadRequestReason(error.to_string()),
    }
}

fn update_error(error: UpdateConnectorError) -> ServerError {
    match error {
        UpdateConnectorError::Invalid(error) => create_error(error),
        UpdateConnectorError::GroupWrite(GroupWriteError::Frozen | GroupWriteError::Deleted) => {
            ServerError::Conflict(error.to_string())
        }
        UpdateConnectorError::NotFound => ServerError::NotFound,
        UpdateConnectorError::InUse => ServerError::Conflict(error.to_string()),
        UpdateConnectorError::SecretEndpoint | UpdateConnectorError::KindChanged => {
            ServerError::BadRequestReason(error.to_string())
        }
        UpdateConnectorError::GroupWrite(_)
        | UpdateConnectorError::Storage(_)
        | UpdateConnectorError::Unexpected => ServerError::InternalError(error.to_string()),
    }
}

fn read_error(error: ReadConnectorError) -> ServerError {
    match error {
        ReadConnectorError::NotFound => ServerError::NotFound,
        ReadConnectorError::Storage(_) | ReadConnectorError::Unexpected => {
            ServerError::InternalError(error.to_string())
        }
    }
}

#[cfg(test)]
#[path = "repositories_tests.rs"]
mod tests;
