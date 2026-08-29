use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::SourceConnectorResolutionError;
use aruna_core::structs::{
    AuthContext, Permission, ResolvedSourceAccess, SourceConnector, SourceConnectorKind,
    SourceEntryKind,
};
use aruna_operations::connectors::create_source_connector::{
    CreateSourceConnectorError, CreateSourceConnectorInput, CreateSourceConnectorOperation,
};
use aruna_operations::connectors::delete_source_connector::{
    DeleteSourceConnectorError, DeleteSourceConnectorInput, DeleteSourceConnectorOperation,
};
use aruna_operations::connectors::get_source_connector::{
    GetSourceConnectorError, GetSourceConnectorInput, GetSourceConnectorOperation,
};
use aruna_operations::connectors::has_secret_config::{
    ConnectorHasSecretConfigError, ConnectorHasSecretConfigOperation,
};
use aruna_operations::connectors::list_source_connectors::{
    ListSourceConnectorsError, ListSourceConnectorsInput, ListSourceConnectorsOperation,
};
use aruna_operations::connectors::replace_source_connector::{
    ReplaceSourceConnectorError, ReplaceSourceConnectorInput, ReplaceSourceConnectorOperation,
};
use aruna_operations::connectors::resolver::{resolve_inline_access, validate_source_path};
use aruna_operations::connectors::validation::validate_connector_input;
use aruna_operations::connectors::{ResolveSourceConnectorInput, ResolveSourceConnectorOperation};
use aruna_operations::driver::drive;
use aruna_operations::staging::check_source::CheckStagingSourceOperation;
use aruna_operations::staging::list_source::{
    ListStagingSourceError, ListStagingSourceInput, ListStagingSourceOperation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, UNIX_EPOCH};
use tokio::time::timeout;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const CONNECTOR_CHECK_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_ENTRY_LIMIT: usize = 200;
const MAX_ENTRY_LIMIT: usize = 1000;

#[derive(OpenApi)]
#[openapi(
    tags((name = "connectors", description = "External source connector registration"))
)]
pub struct ConnectorsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ConnectorsApiDoc::openapi())
        .routes(routes!(check_source_connector))
        .routes(routes!(create_source_connector, list_source_connectors))
        .routes(routes!(
            get_source_connector,
            replace_source_connector,
            delete_source_connector
        ))
        .routes(routes!(check_stored_connector))
        .routes(routes!(list_connector_entries))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiSourceConnectorKind {
    Http,
    S3,
    Webdav,
    /// Rejected on registration and refused at use; readable on stored records only.
    Ftp,
    /// Rejected on registration and refused at use; readable on stored records only.
    ArunaNative,
    /// A directory offered by a device, registered there and never here;
    /// readable on stored records only.
    LocalDirectory,
}

impl From<ApiSourceConnectorKind> for SourceConnectorKind {
    fn from(value: ApiSourceConnectorKind) -> Self {
        match value {
            ApiSourceConnectorKind::Http => SourceConnectorKind::Http,
            ApiSourceConnectorKind::S3 => SourceConnectorKind::S3,
            ApiSourceConnectorKind::Webdav => SourceConnectorKind::Webdav,
            ApiSourceConnectorKind::Ftp => SourceConnectorKind::Ftp,
            ApiSourceConnectorKind::ArunaNative => SourceConnectorKind::ArunaNative,
            ApiSourceConnectorKind::LocalDirectory => SourceConnectorKind::LocalDirectory,
        }
    }
}

impl From<SourceConnectorKind> for ApiSourceConnectorKind {
    fn from(value: SourceConnectorKind) -> Self {
        match value {
            SourceConnectorKind::Http => ApiSourceConnectorKind::Http,
            SourceConnectorKind::S3 => ApiSourceConnectorKind::S3,
            SourceConnectorKind::Webdav => ApiSourceConnectorKind::Webdav,
            SourceConnectorKind::Ftp => ApiSourceConnectorKind::Ftp,
            SourceConnectorKind::ArunaNative => ApiSourceConnectorKind::ArunaNative,
            SourceConnectorKind::LocalDirectory => ApiSourceConnectorKind::LocalDirectory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceSourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceConnectorRequest {
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorCheckSuccess {
    pub ok: bool,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorCheckFailure {
    pub ok: bool,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(untagged)]
pub enum ConnectorCheckResponse {
    Success(ConnectorCheckSuccess),
    Failure(ConnectorCheckFailure),
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorEntriesQuery {
    #[serde(default)]
    pub path: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorEntryKind {
    File,
    Dir,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorEntryResponse {
    pub name: String,
    pub path: String,
    pub kind: ConnectorEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ConnectorEntriesResponse {
    pub entries: Vec<ConnectorEntryResponse>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SourceConnectorResponse {
    pub connector_id: String,
    pub group_id: String,
    pub name: String,
    pub kind: ApiSourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub created_at: String,
    pub updated_at: String,
    pub created_by: String,
    pub has_secret_config: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ListSourceConnectorsResponse {
    pub connectors: Vec<SourceConnectorResponse>,
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors",
    tag = "connectors",
    summary = "Register a source connector for a group",
    description = r#"Registers an external source connector for a group and stores its credentials write-only.

**Authentication**: realm bearer token with WRITE on the group's data path; a caller lacking that
permission and a caller naming a group that does not exist both receive 403.

**Behavior**
- `secret_config` is write-only: the credentials are stored apart from the connector record and are
  never returned by any endpoint, so a reader only learns from `has_secret_config` whether any are
  stored.
- Nothing is contacted here, so a successful registration is no evidence that the credentials work
  or that the host is reachable, and an endpoint resolving to a blocked address is only refused when
  the connector is used.
- The record is written on the node that serves the request and is not replicated to the realm's
  other nodes.

**Limits** (the accepted keys depend on `kind`)
- `http` and `webdav` require the public key `endpoint` and also accept `root`, with `username`,
  `password` or `token` as secrets.
- `s3` requires `bucket` and `endpoint`, also accepts `region`, `root` and `skip_signature`, and
  requires the secrets `access_key_id` and `secret_access_key` unless `skip_signature` is `true`,
  which in turn forbids any secret.
- `ftp` and `aruna_native` are refused outright.
- An unknown or empty config key, an endpoint the HTTP client would parse differently than it is
  written, and a bucket containing a path or authority separator are refused as well.

**Errors**: the 400 body states only that the request was bad, without the reason; the connector
check operation reports the reason."#,
    params(("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID")),
    request_body(
        content = CreateSourceConnectorRequest,
        description = "Connector name, kind, the public configuration for that kind, and the write-only credentials; `secret_config` may be omitted for a source that needs none",
        example = json!({
            "name": "reference-data",
            "kind": "s3",
            "public_config": {
                "bucket": "reference-data",
                "endpoint": "https://s3.example.test",
                "region": "eu-central-1"
            },
            "secret_config": {
                "access_key_id": "EXAMPLE-KEY-ID-PLACEHOLDER",
                "secret_access_key": "EXAMPLE-SECRET-PLACEHOLDER"
            }
        })
    ),
    responses(
        (
            status = 201,
            description = "Connector registered; the stored credentials are not echoed back",
            body = SourceConnectorResponse,
            example = json!({
                "connector_id": "01JCNCTR0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "reference-data",
                "kind": "s3",
                "public_config": {
                    "bucket": "reference-data",
                    "endpoint": "https://s3.example.test",
                    "region": "eu-central-1"
                },
                "created_at": "2026-04-09T14:23:11.123456789+00:00",
                "updated_at": "2026-04-09T14:23:11.123456789+00:00",
                "created_by": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "has_secret_config": true
            })
        ),
        (
            status = 400,
            description = "The group id is not a ULID, or the name, kind or configuration failed validation",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no WRITE on the group's data path, including when the group does not exist",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateSourceConnectorRequest>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let result = drive(
        CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
            group_id,
            created_by: auth.user_id,
            name: request.name,
            kind: request.kind.into(),
            public_config: request.public_config,
            secret_config: request.secret_config,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_connector_error)?;

    Ok((
        StatusCode::CREATED,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors",
    tag = "connectors",
    summary = "List a group's source connectors",
    description = r#"Returns every source connector the group has registered on this node.

**Authentication**: realm bearer token with READ on the group's data path; a caller lacking that
permission and a caller naming a group that does not exist both receive 403.

**Behavior**
- Connectors are ordered by connector id and therefore by registration time, and are returned in a
  single response: there is no paging, no cursor and no limit.
- Credentials are never included; `has_secret_config` is resolved per connector and only states
  whether any are stored.
- This is the node's own view, so a connector registered against another node of the realm is not
  listed here."#,
    params(("group_id" = String, Path, description = "Group whose connectors are listed, as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Every connector the group has registered on this node, credentials excluded",
            body = ListSourceConnectorsResponse,
            example = json!({
                "connectors": [
                    {
                        "connector_id": "01JCNCTR0123456789ABCDEFGH",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "reference-data",
                        "kind": "s3",
                        "public_config": {
                            "bucket": "reference-data",
                            "endpoint": "https://s3.example.test"
                        },
                        "created_at": "2026-04-09T14:23:11.123456789+00:00",
                        "updated_at": "2026-04-09T14:23:11.123456789+00:00",
                        "created_by": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "has_secret_config": true
                    }
                ]
            })
        ),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no READ on the group's data path, including when the group does not exist",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_source_connectors(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ListSourceConnectorsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let result = drive(
        ListSourceConnectorsOperation::new(ListSourceConnectorsInput { group_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_list_connector_error)?;

    let mut connectors = Vec::with_capacity(result.connectors.len());
    for connector in result.connectors {
        let has_secret_config =
            connector_has_secret_config(state.as_ref(), connector.connector_id).await?;
        connectors.push(map_connector_response(connector, has_secret_config));
    }

    Ok((
        StatusCode::OK,
        Json(ListSourceConnectorsResponse { connectors }),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    summary = "Read one source connector",
    description = r#"Returns one stored connector with its name, kind and public configuration.

**Authentication**: realm bearer token with READ on the group's data path.

**Behavior**
- The connector is looked up under the group in the path, so a connector id that belongs to a
  different group reads as not found rather than as forbidden.
- The credentials are never returned and `has_secret_config` is the only thing a reader learns about
  them.
- Reads this node's own records, so a connector registered against another node of the realm is not
  found here."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to read, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The stored connector, credentials excluded",
            body = SourceConnectorResponse,
            example = json!({
                "connector_id": "01JCNCTR0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "reference-data",
                "kind": "s3",
                "public_config": {
                    "bucket": "reference-data",
                    "endpoint": "https://s3.example.test",
                    "region": "eu-central-1"
                },
                "created_at": "2026-04-09T14:23:11.123456789+00:00",
                "updated_at": "2026-04-09T14:25:02.987654321+00:00",
                "created_by": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "has_secret_config": true
            })
        ),
        (
            status = 400,
            description = "The group id or the connector id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no READ on the group's data path, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No connector with that id is registered for this group on this node",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let result = drive(
        GetSourceConnectorOperation::new(GetSourceConnectorInput {
            group_id,
            connector_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_get_connector_error)?;

    Ok((
        StatusCode::OK,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    put,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    summary = "Replace a source connector's settings and credentials",
    description = r#"Replaces a stored connector's name, kind, public configuration and credentials in full.

**Authentication**: realm bearer token with WRITE on the group's data path.

**Behavior**
- This is a full replacement, not a patch: name, kind, public configuration and credentials are all
  taken from the request, so credentials that are not sent again are deleted, and after a request
  without `secret_config` the connector has none and `has_secret_config` reads false.
- The connector id, the owning group, the creation time and the creator are preserved and the update
  time is refreshed.
- Changing only the name or the public configuration is allowed even while stored object versions
  still reference the credentials.
- Nothing is contacted, so a successful replace does not prove that the new credentials work.

**Limits**
- The same validation rules as registration apply.
- Changing the credentials is refused while any stored object version still references them, so that
  a reference can always be resolved with the credentials it was created against.

**Errors**: the 400 body again states only that the request was bad, without the reason. The refusal
to change referenced credentials is a 409 and stays one until the referencing versions are gone, so
it is not worth retrying unchanged."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to replace, as a 26-character ULID")
    ),
    request_body(
        content = ReplaceSourceConnectorRequest,
        description = "The complete new connector definition; every field replaces the stored one, and an omitted or empty `secret_config` clears the stored credentials",
        example = json!({
            "name": "reference-data",
            "kind": "s3",
            "public_config": {
                "bucket": "reference-data-v2",
                "endpoint": "https://s3.example.test",
                "region": "eu-central-1"
            },
            "secret_config": {
                "access_key_id": "EXAMPLE-KEY-ID-PLACEHOLDER",
                "secret_access_key": "EXAMPLE-ROTATED-SECRET-PLACEHOLDER"
            }
        })
    ),
    responses(
        (
            status = 200,
            description = "The connector as stored after the replacement, credentials excluded",
            body = SourceConnectorResponse,
            example = json!({
                "connector_id": "01JCNCTR0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "reference-data",
                "kind": "s3",
                "public_config": {
                    "bucket": "reference-data-v2",
                    "endpoint": "https://s3.example.test",
                    "region": "eu-central-1"
                },
                "created_at": "2026-04-09T14:23:11.123456789+00:00",
                "updated_at": "2026-04-10T09:02:44.500000000+00:00",
                "created_by": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "has_secret_config": true
            })
        ),
        (
            status = 400,
            description = "An id is not a ULID, or the new name, kind or configuration failed validation",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no WRITE on the group's data path, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No connector with that id is registered for this group on this node",
            body = ErrorResponse
        ),
        (
            status = 409,
            description = "The credentials are still referenced by a stored object version",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Json(request): Json<ReplaceSourceConnectorRequest>,
) -> ServerResult<(StatusCode, Json<SourceConnectorResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let result = drive(
        ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
            group_id,
            connector_id,
            name: request.name,
            kind: request.kind.into(),
            public_config: request.public_config,
            secret_config: request.secret_config,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_replace_connector_error)?;

    Ok((
        StatusCode::OK,
        Json(map_connector_response(
            result.connector,
            result.has_secret_config,
        )),
    ))
}

#[utoipa::path(
    delete,
    path = "/groups/{group_id}/connectors/{connector_id}",
    tag = "connectors",
    summary = "Delete a source connector",
    description = r#"Removes a source connector record and its stored credentials together.

**Authentication**: realm bearer token with WRITE on the group's data path.

**Behavior**
- After a 204 nothing on this node can resolve the connector any more, and a staged reference that
  would have used it fails from then on.
- Deleting a connector that is not registered for this group is a 404, not a silent success, so the
  call is not idempotent once it has succeeded.

**Limits**
- The deletion is refused while any stored object version still references the connector's
  credentials, so a referenced connector must be detached before it can be removed.

**Errors**: the refusal to delete a referenced connector is a 409 and stays one until the
referencing versions are gone, so it is not worth retrying unchanged."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to delete, as a 26-character ULID")
    ),
    responses(
        (
            status = 204,
            description = "Connector and credentials deleted; the response has no body and no content type"
        ),
        (
            status = 400,
            description = "The group id or the connector id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no WRITE on the group's data path, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No connector with that id is registered for this group on this node",
            body = ErrorResponse
        ),
        (
            status = 409,
            description = "The credentials are still referenced by a stored object version",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    drive(
        DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
            group_id,
            connector_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_delete_connector_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors/check",
    tag = "connectors",
    summary = "Test connector settings without registering them",
    description = r#"Probes a candidate connector definition and reports whether the source answered.

**Authentication**: realm bearer token with WRITE on the group's data path, the same permission as
registration, because the request carries credentials and makes the node open a connection to the
endpoint they name.

**Behavior**
- The body is validated with exactly the registration rules, so a malformed body is rejected with
  400 before anything is contacted, and here the 400 carries the validation reason.
- Otherwise the node opens the source and reports the outcome inside a 200 response:
  `{"ok": true, "latency_ms": ...}` when the source answered, `{"ok": false, "error": ...}` when it
  did not. A failed check is never an HTTP error status.
- An `http` source counts as reachable as soon as the endpoint answers, including with a 404 for the
  probe path.
- Nothing is stored: the credentials in the body are used for this request only and are not written
  anywhere.

**Errors**: the `error` text comes from a fixed set, since a kind that cannot be staged at all is
  already refused with 400 before the probe.
- `connector configuration is invalid`: the settings cannot be turned into a client and the
  configuration must be corrected.
- `connector is unreachable`: the endpoint refused, denied, or its address was blocked by the egress
  policy, which is worth retrying only if the remote is expected to recover.
- `connector check is unavailable`: this node cannot run source checks at the moment and the call
  may be retried as is.
- `connector check timed out`: the check exceeded its five-second budget and may be retried."#,
    params(("group_id" = String, Path, description = "Group whose data permission gates the check, as a 26-character ULID")),
    request_body(
        content = SourceConnectorRequest,
        description = "A candidate connector definition in the same shape as registration; the credentials are used for this one probe and never stored",
        example = json!({
            "name": "reference-data",
            "kind": "s3",
            "public_config": {
                "bucket": "reference-data",
                "endpoint": "https://s3.example.test"
            },
            "secret_config": {
                "access_key_id": "EXAMPLE-KEY-ID-PLACEHOLDER",
                "secret_access_key": "EXAMPLE-SECRET-PLACEHOLDER"
            }
        })
    ),
    responses(
        (
            status = 200,
            description = "The probe ran; `ok` says whether the source answered and a failure carries a fixed reason text",
            body = ConnectorCheckResponse,
            examples(
                ("Reachable" = (
                    summary = "The source answered within the budget",
                    value = json!({
                        "ok": true,
                        "latency_ms": 87
                    })
                )),
                ("Unreachable" = (
                    summary = "The endpoint did not answer or was blocked",
                    value = json!({
                        "ok": false,
                        "error": "connector is unreachable"
                    })
                ))
            )
        ),
        (
            status = 400,
            description = "The group id is not a ULID, or the candidate configuration failed validation; the message names the offending field",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no WRITE on the group's data path, including when the group does not exist",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn check_source_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<SourceConnectorRequest>,
) -> ServerResult<Json<ConnectorCheckResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::WRITE).await?;

    let kind: SourceConnectorKind = request.kind.into();
    validate_connector_input(
        &request.name,
        kind,
        &request.public_config,
        &request.secret_config,
    )
    .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;

    let access = resolve_inline_access(kind, &request.public_config, request.secret_config)
        .map_err(map_resolution_error)?;
    Ok(Json(run_connector_check(&state, access).await))
}

#[utoipa::path(
    post,
    path = "/groups/{group_id}/connectors/{connector_id}/check",
    tag = "connectors",
    summary = "Test a registered connector's stored settings",
    description = r#"Probes a registered connector with its stored credentials and reports the outcome.

**Authentication**: realm bearer token with READ on the group's data path. READ is enough here,
unlike the check that takes settings in the body, because the caller supplies no credentials.

**Behavior**
- The node resolves the stored record and its stored credentials and probes the source root with
  them, which tells the caller whether the credentials still work without exposing them.
- The outcome is reported inside a 200 response exactly as for the inline check:
  `{"ok": true, "latency_ms": ...}` or `{"ok": false, "error": ...}`, with the same fixed reason
  texts and the same five-second budget.
- A stored `ftp` connector resolves but always reports `connector kind is not supported`.
- Nothing is written and the record is left untouched whatever the outcome.

**Errors**: `connector check is unavailable` and `connector check timed out` are retryable, while
`connector configuration is invalid` and `connector kind is not supported` mean the stored record
must be replaced."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to probe, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The probe ran with the stored credentials; `ok` says whether the source answered",
            body = ConnectorCheckResponse,
            examples(
                ("Reachable" = (
                    summary = "The stored credentials still open the source",
                    value = json!({
                        "ok": true,
                        "latency_ms": 142
                    })
                )),
                ("Unavailable" = (
                    summary = "This node cannot run source checks at the moment",
                    value = json!({
                        "ok": false,
                        "error": "connector check is unavailable"
                    })
                ))
            )
        ),
        (
            status = 400,
            description = "An id is not a ULID, or the stored record names a kind that cannot be probed at all",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no READ on the group's data path, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No connector with that id is registered for this group on this node",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn check_stored_connector(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
) -> ServerResult<Json<ConnectorCheckResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;

    let resolved = drive(
        ResolveSourceConnectorOperation::new(ResolveSourceConnectorInput {
            group_id,
            connector_id,
            source_path: String::new(),
            allow_root: true,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_resolution_error)?;

    Ok(Json(run_connector_check(&state, resolved.access).await))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/connectors/{connector_id}/entries",
    tag = "connectors",
    summary = "Browse the entries under a connector path",
    description = r#"Lists one level below a path at the connector's source, using its stored credentials.

**Authentication**: realm bearer token with READ on the group's data path.

**Behavior**
- The listing never descends into subdirectories: directories are returned as entries of kind `dir`
  and must be browsed with a follow-up request.
- `size` and `modified_ms` are omitted for entries whose source reports neither, and `modified_ms`
  is milliseconds since the Unix epoch.
- An object source that simply holds nothing below the path answers 200 with an empty list, while a
  source that reports the path itself as gone is a 404.
- An `http` connector has no listing protocol, so its entries are parsed out of the server's HTML
  directory index and a server that does not serve one fails with 502.

**Limits**
- There is no cursor: `truncated` only says that the source held more entries than the limit
  allowed, and the only way to see more is a larger `limit`, up to the cap, or a narrower `path`.

**Errors**: a source error, a blocked address or an unparsable index all read as 502 and carry the
source's own message; retry only when the source is expected to recover, since the same 502 also
reports a configuration that can never work."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the connector, as a 26-character ULID"),
        ("connector_id" = String, Path, description = "Connector to browse, as a 26-character ULID"),
        (
            "path" = Option<String>,
            Query,
            description = "Directory to list, relative to the connector root and without a leading slash; absolute paths and `.` or `..` components are rejected, and omitting it or passing an empty value lists the root"
        ),
        (
            "limit" = Option<usize>,
            Query,
            description = "Maximum number of entries to return; defaults to 200, must be greater than zero and is capped at 1000"
        )
    ),
    responses(
        (
            status = 200,
            description = "The entries directly below the requested path, with `truncated` set when the limit cut the listing short",
            body = ConnectorEntriesResponse,
            example = json!({
                "entries": [
                    {
                        "name": "run-1",
                        "path": "datasets/run-1/",
                        "kind": "dir"
                    },
                    {
                        "name": "manifest.tsv",
                        "path": "datasets/manifest.tsv",
                        "kind": "file",
                        "size": 20480,
                        "modified_ms": 1775744591123_i64
                    }
                ],
                "truncated": false
            })
        ),
        (
            status = 400,
            description = "An id is not a ULID, the path escapes the connector root, or the limit is zero",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller has no READ on the group's data path, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No connector with that id on this node, or the source reported the path as gone",
            body = ErrorResponse
        ),
        (
            status = 502,
            description = "The source refused the listing, was blocked, or returned no directory listing; the message repeats the source's reason",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_connector_entries(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, connector_id)): Path<(String, String)>,
    Query(query): Query<ConnectorEntriesQuery>,
) -> ServerResult<Json<ConnectorEntriesResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let connector_id = parse_connector_id(&connector_id)?;
    ensure_group_data_permission(&state, &auth, group_id, Permission::READ).await?;
    let source_path = normalize_browse_path(&query.path)?;
    let limit = query.limit.unwrap_or(DEFAULT_ENTRY_LIMIT);
    if limit == 0 {
        return Err(ServerError::BadRequestReason(
            "limit must be greater than zero".to_string(),
        ));
    }

    let result = drive(
        ListStagingSourceOperation::new(ListStagingSourceInput {
            group_id,
            connector_id,
            source_path,
            offset: 0,
            limit: limit.min(MAX_ENTRY_LIMIT),
            recursive: false,
            files_only: false,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_list_error)?;

    Ok(Json(ConnectorEntriesResponse {
        entries: result
            .entries
            .into_iter()
            .map(|entry| ConnectorEntryResponse {
                name: entry.name,
                path: entry.path,
                kind: match entry.kind {
                    SourceEntryKind::File => ConnectorEntryKind::File,
                    SourceEntryKind::Directory => ConnectorEntryKind::Dir,
                },
                size: entry.size,
                modified_ms: entry.modified.and_then(|modified| {
                    modified
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                }),
            })
            .collect(),
        truncated: result.truncated,
    }))
}

async fn run_connector_check(
    state: &ServerState,
    access: ResolvedSourceAccess,
) -> ConnectorCheckResponse {
    let started = Instant::now();
    match timeout(
        CONNECTOR_CHECK_TIMEOUT,
        drive(CheckStagingSourceOperation::new(access), &state.get_ctx()),
    )
    .await
    {
        Ok(Ok(())) => ConnectorCheckResponse::Success(ConnectorCheckSuccess {
            ok: true,
            latency_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }),
        Ok(Err(error)) => ConnectorCheckResponse::Failure(ConnectorCheckFailure {
            ok: false,
            error: check_error_message(&error),
        }),
        Err(_) => ConnectorCheckResponse::Failure(ConnectorCheckFailure {
            ok: false,
            error: "connector check timed out".to_string(),
        }),
    }
}

fn check_error_message(
    error: &aruna_operations::staging::check_source::CheckStagingSourceError,
) -> String {
    use aruna_core::errors::StagingSourceError;
    use aruna_operations::staging::check_source::CheckStagingSourceError;

    match error {
        CheckStagingSourceError::Staging(StagingSourceError::OperatorCreationFailed(_)) => {
            "connector configuration is invalid".to_string()
        }
        CheckStagingSourceError::Staging(StagingSourceError::UnsupportedKind(_)) => {
            "connector kind is not supported".to_string()
        }
        CheckStagingSourceError::Staging(StagingSourceError::HandleMissing)
        | CheckStagingSourceError::Staging(StagingSourceError::ChannelClosed) => {
            "connector check is unavailable".to_string()
        }
        _ => "connector is unreachable".to_string(),
    }
}

fn normalize_browse_path(path: &str) -> ServerResult<String> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }
    validate_source_path(path, false).map_err(map_resolution_error)?;
    Ok(format!("{}/", path.trim().trim_end_matches('/')))
}

fn parse_connector_id(connector_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(connector_id).map_err(|_| ServerError::BadRequest)
}

pub(crate) async fn ensure_group_data_permission(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    required_permission: Permission,
) -> ServerResult<()> {
    crate::auth::ensure_permission(
        state,
        auth,
        format!("/{}/g/{group_id}/data/**", state.get_realm_id()),
        required_permission,
    )
    .await
}

async fn connector_has_secret_config(
    state: &ServerState,
    connector_id: Ulid,
) -> ServerResult<bool> {
    drive(
        ConnectorHasSecretConfigOperation::new(connector_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_connector_secret_config_error)
}

fn map_connector_secret_config_error(error: ConnectorHasSecretConfigError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn map_connector_response(
    connector: SourceConnector,
    has_secret_config: bool,
) -> SourceConnectorResponse {
    SourceConnectorResponse {
        connector_id: connector.connector_id.to_string(),
        group_id: connector.group_id.to_string(),
        name: connector.name,
        kind: connector.kind.into(),
        public_config: connector.public_config,
        created_at: format_system_time(connector.created_at),
        updated_at: format_system_time(connector.updated_at),
        created_by: connector.created_by.to_string(),
        has_secret_config,
    }
}

fn format_system_time(value: std::time::SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(value).to_rfc3339()
}

fn map_create_connector_error(error: CreateSourceConnectorError) -> ServerError {
    match error {
        CreateSourceConnectorError::ValidationError(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_list_connector_error(error: ListSourceConnectorsError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn map_get_connector_error(
    error: aruna_operations::connectors::get_source_connector::GetSourceConnectorError,
) -> ServerError {
    match error {
        GetSourceConnectorError::NotFound => ServerError::NotFound,
        GetSourceConnectorError::StorageError(_)
        | GetSourceConnectorError::ConversionError(_)
        | GetSourceConnectorError::GetSourceConnectorFailed => {
            ServerError::InternalError(error.to_string())
        }
    }
}

fn map_replace_connector_error(error: ReplaceSourceConnectorError) -> ServerError {
    match error {
        ReplaceSourceConnectorError::ValidationError(_) => ServerError::BadRequest,
        ReplaceSourceConnectorError::NotFound => ServerError::NotFound,
        ReplaceSourceConnectorError::ReferencedByObjectVersion => {
            ServerError::Conflict(error.to_string())
        }
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_delete_connector_error(error: DeleteSourceConnectorError) -> ServerError {
    match error {
        DeleteSourceConnectorError::NotFound => ServerError::NotFound,
        DeleteSourceConnectorError::ReferencedByObjectVersion => {
            ServerError::Conflict(error.to_string())
        }
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_resolution_error(error: SourceConnectorResolutionError) -> ServerError {
    match error {
        SourceConnectorResolutionError::NotFound => ServerError::NotFound,
        SourceConnectorResolutionError::InvalidSourcePath
        | SourceConnectorResolutionError::UnsupportedConnectorKind(_) => ServerError::BadRequest,
        _ => ServerError::InternalError(error.to_string()),
    }
}

fn map_list_error(error: ListStagingSourceError) -> ServerError {
    match error {
        ListStagingSourceError::Resolve(error) => map_resolution_error(error),
        ListStagingSourceError::Staging(aruna_core::errors::StagingSourceError::NotFound) => {
            ServerError::NotFound
        }
        ListStagingSourceError::Staging(error) => ServerError::BadGatewayReason(error.to_string()),
        _ => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openapi::ApiDoc;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
        RealmConfigDocument,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use serde_json::json;
    use tempfile::TempDir;

    struct TestState {
        _storage_dir: TempDir,
        auth: AuthContext,
        other_auth: AuthContext,
        group_id: Ulid,
        state: Arc<ServerState>,
    }

    #[tokio::test]
    async fn connector_routes_crud_and_redact_secret_config() {
        let test = setup_state().await;

        let (_, Json(created)) = create_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "refdata".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("access_key_id".to_string(), "AKIA".to_string()),
                    ("secret_access_key".to_string(), "super-secret".to_string()),
                ]),
            }),
        )
        .await
        .unwrap();

        assert_eq!(created.name, "refdata");
        assert!(created.has_secret_config);
        assert!(!created.public_config.contains_key("access_key_id"));

        let (_, Json(listed)) = list_source_connectors(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(listed.connectors.len(), 1);
        assert!(listed.connectors[0].has_secret_config);

        let (_, Json(fetched)) = get_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
        )
        .await
        .unwrap();

        assert_eq!(fetched, created);

        let (_, Json(replaced)) = replace_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
            Json(ReplaceSourceConnectorRequest {
                name: "refdata-updated".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads-v2".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                    ("skip_signature".to_string(), "true".to_string()),
                ]),
                secret_config: HashMap::new(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(replaced.name, "refdata-updated");
        assert!(!replaced.has_secret_config);

        let delete_status = delete_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), created.connector_id.clone())),
        )
        .await
        .unwrap();

        assert_eq!(delete_status, StatusCode::NO_CONTENT);

        let get_result = get_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), created.connector_id)),
        )
        .await;
        assert!(matches!(get_result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn connector_routes_require_group_data_permission() {
        let test = setup_state().await;

        let result = create_source_connector(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "forbidden".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn check_requires_permission() {
        let test = setup_state().await;

        let result = check_source_connector(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path(test.group_id.to_string()),
            Json(SourceConnectorRequest {
                name: "source".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn check_returns_failure() {
        let test = setup_state().await;

        let Json(result) = check_source_connector(
            State(test.state),
            Extension(Some(test.auth)),
            Path(test.group_id.to_string()),
            Json(SourceConnectorRequest {
                name: "source".to_string(),
                kind: ApiSourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
        )
        .await
        .unwrap();

        assert!(matches!(
            result,
            ConnectorCheckResponse::Failure(ConnectorCheckFailure { ok: false, .. })
        ));
    }

    #[test]
    fn check_maps_unreachable() {
        let error = aruna_operations::staging::check_source::CheckStagingSourceError::Staging(
            aruna_core::errors::StagingSourceError::CheckError("connection refused".to_string()),
        );

        assert_eq!(check_error_message(&error), "connector is unreachable");
    }

    #[tokio::test]
    async fn stored_check_resolves() {
        let test = setup_state().await;
        let (_, Json(connector)) = create_source_connector(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(CreateSourceConnectorRequest {
                name: "stored-source".to_string(),
                kind: ApiSourceConnectorKind::S3,
                public_config: HashMap::from([
                    ("bucket".to_string(), "reads".to_string()),
                    ("endpoint".to_string(), "https://s3.example.org".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("access_key_id".to_string(), "AKIA".to_string()),
                    ("secret_access_key".to_string(), "stored-secret".to_string()),
                ]),
            }),
        )
        .await
        .unwrap();

        let Json(result) = check_stored_connector(
            State(test.state),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), connector.connector_id)),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            ConnectorCheckResponse::Failure(ConnectorCheckFailure {
                ok: false,
                error: "connector check is unavailable".to_string(),
            })
        );
    }

    #[test]
    fn check_serializes_success() {
        let result = ConnectorCheckResponse::Success(ConnectorCheckSuccess {
            ok: true,
            latency_ms: 12,
        });

        assert_eq!(
            serde_json::to_value(result).unwrap(),
            json!({"ok": true, "latency_ms": 12})
        );
    }

    #[tokio::test]
    async fn entries_reject_traversal() {
        let test = setup_state().await;

        let result = list_connector_entries(
            State(test.state),
            Extension(Some(test.auth)),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
            Query(ConnectorEntriesQuery {
                path: "../secret".to_string(),
                limit: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn entries_require_permission() {
        let test = setup_state().await;

        let result = list_connector_entries(
            State(test.state),
            Extension(Some(test.other_auth)),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
            Query(ConnectorEntriesQuery {
                path: String::new(),
                limit: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[test]
    fn entries_normalize_prefix() {
        assert_eq!(normalize_browse_path("prefix").unwrap(), "prefix/");
        assert_eq!(normalize_browse_path("prefix/").unwrap(), "prefix/");
        assert_eq!(normalize_browse_path("").unwrap(), "");
    }

    #[test]
    fn list_preserves_reason() {
        let error = map_list_error(ListStagingSourceError::Staging(
            aruna_core::errors::StagingSourceError::ListError("not an index".to_string()),
        ));

        assert!(matches!(
            error,
            ServerError::BadGatewayReason(message) if message == "List error: not an index"
        ));
    }

    #[test]
    fn referenced_connector_conflicts() {
        // A still-referenced credential is a policy refusal, not an internal error.
        assert!(matches!(
            map_replace_connector_error(ReplaceSourceConnectorError::ReferencedByObjectVersion),
            ServerError::Conflict(_)
        ));
        assert!(matches!(
            map_delete_connector_error(DeleteSourceConnectorError::ReferencedByObjectVersion),
            ServerError::Conflict(_)
        ));
    }

    #[test]
    fn openapi_includes_connector_paths() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/check")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}/check")
                .is_some()
        );
        assert!(
            openapi["paths"]
                .get("/groups/{group_id}/connectors/{connector_id}/entries")
                .is_some()
        );
        assert_eq!(
            openapi["components"]["schemas"]["ApiSourceConnectorKind"]["type"],
            json!("string")
        );
    }

    async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = aruna_core::structs::RealmId([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let other_user_id = UserId::local(Ulid::generate(), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let group_id = Ulid::generate();
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "connector-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);

        // Request-policy loading fails closed without the realm config document.
        write_doc(
            &driver_ctx,
            REALM_CONFIG_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap()
                .into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        TestState {
            _storage_dir: storage_dir,
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            other_auth: AuthContext {
                user_id: other_user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id,
            state,
        }
    }

    async fn write_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
        value: byteview::ByteView,
    ) {
        let event = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}
