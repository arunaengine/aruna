use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage_routing::ensure_group_admin;
use crate::server_state::ServerState;
use aruna_core::structs::{
    AuthContext, BackendRef, CleanupStrategy, GroupBackendKind, GroupStorageBackend,
};
use aruna_operations::blob::reclaim::backend_status;
use aruna_operations::driver::drive;
use aruna_operations::group_backends::create::{
    CreateGroupBackendError, CreateGroupBackendInput, CreateGroupBackendOperation,
};
use aruna_operations::group_backends::disable::{SetDisabledError, SetDisabledOperation};
use aruna_operations::group_backends::query::{
    GetGroupBackendOperation, ListGroupBackendsOperation,
};
use aruna_operations::group_backends::replace::ReplaceGroupBackendOperation;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((
        name = "data/storage",
        description = "Tenant-registered write backends on a group's own object storage"
    ))
)]
pub struct GroupBackendsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(GroupBackendsApiDoc::openapi())
        .routes(routes!(create_group_backend, list_group_backends))
        .routes(routes!(
            get_group_backend,
            replace_group_backend,
            delete_group_backend
        ))
        .routes(routes!(enable_group_backend))
        .routes(routes!(backend_reclaim_status))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CreateGroupBackendRequest {
    pub name: String,
    /// One of `s3`, `gcs`, `azblob`, `azdls`, `b2`.
    pub kind: String,
    #[serde(default)]
    pub public_config: HashMap<String, String>,
    /// Credentials. Stored separately and never returned.
    #[serde(default)]
    pub secret_config: HashMap<String, String>,
    /// Omitted means `retain`: tenant storage is never reclaimed by default.
    #[serde(default)]
    pub cleanup: Option<CleanupPolicy>,
}

/// Wire form of the cleanup strategy. Durations cross the API as seconds so no
/// client has to parse a duration syntax.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CleanupPolicy {
    /// `retain` or `reclaim`.
    pub mode: String,
    /// Grace before an unreferenced copy is deleted. Reclaim only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_secs: Option<u64>,
}

impl CleanupPolicy {
    fn resolve(policy: Option<Self>) -> ServerResult<CleanupStrategy> {
        let Some(policy) = policy else {
            return Ok(CleanupStrategy::Retain);
        };
        match (policy.mode.as_str(), policy.after_secs) {
            ("retain", None) => Ok(CleanupStrategy::Retain),
            ("reclaim", None) => Ok(CleanupStrategy::Reclaim {
                after: CleanupStrategy::DEFAULT_RECLAIM_AFTER,
            }),
            ("reclaim", Some(after)) if after > 0 => Ok(CleanupStrategy::Reclaim {
                after: Duration::from_secs(after),
            }),
            _ => Err(ServerError::BadRequestMessage(
                "cleanup must be `retain`, or `reclaim` with a positive after_secs".to_string(),
            )),
        }
    }
}

impl From<CleanupStrategy> for CleanupPolicy {
    fn from(value: CleanupStrategy) -> Self {
        match value {
            CleanupStrategy::Retain => Self {
                mode: "retain".to_string(),
                after_secs: None,
            },
            CleanupStrategy::Reclaim { after } => Self {
                mode: "reclaim".to_string(),
                after_secs: Some(after.as_secs()),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupBackendResponse {
    pub backend_id: String,
    pub group_id: String,
    pub name: String,
    pub kind: String,
    pub public_config: HashMap<String, String>,
    /// Writes are refused while this is set; reads keep working.
    pub disabled: bool,
    pub cleanup: CleanupPolicy,
}

/// Reclaim queue depth for one backend, computed from the queues on each call.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ReclaimStatusResponse {
    pub pending_candidates: usize,
    /// Physical deletes still owed to this backend. The drain runs on its own
    /// timer, so a non-zero count is normal; reclaim is blocked when
    /// `oldest_enqueued_at` stops moving forward.
    pub queued_cleanups: usize,
    /// When the oldest item in either queue was enqueued, so a stalled physical
    /// delete still dates itself once its candidate row is gone.
    pub oldest_enqueued_at: Option<String>,
    /// A scan hit its cap, so the counts are lower bounds.
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ListGroupBackendsResponse {
    pub backends: Vec<GroupBackendResponse>,
}

impl From<GroupStorageBackend> for GroupBackendResponse {
    fn from(value: GroupStorageBackend) -> Self {
        Self {
            backend_id: value.backend_id.to_string(),
            group_id: value.group_id.to_string(),
            name: value.name,
            kind: value.kind.to_string(),
            public_config: value.public_config,
            disabled: value.disabled,
            cleanup: value.cleanup.into(),
        }
    }
}

fn map_create_error(error: CreateGroupBackendError) -> ServerError {
    match error {
        CreateGroupBackendError::NotFound => ServerError::NotFound,
        CreateGroupBackendError::Invalid(error) => {
            ServerError::BadRequestMessage(error.to_string())
        }
        CreateGroupBackendError::Unreachable(error) => {
            ServerError::BadRequestMessage(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn admin_of_group(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<(Ulid, AuthContext)> {
    let auth = require_realm_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    ensure_group_admin(state, &auth, group_id).await?;
    Ok((group_id, auth))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/backends",
    tag = "data/storage",
    summary = "Register a storage backend for a group",
    description = r#"Registers a write backend on the group's own object storage after proving its credentials.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path. A backend receives the group's data, so group write rights are deliberately not enough: a
caller who is not a group administrator, as well as a caller naming a group that does not exist,
receives 403.

**Behavior**
- Before anything is stored the node proves the credentials by writing a probe object to the
  backend and deleting it again, so delete rights are required and not just write rights.
- A failed probe registers nothing.
- `secret_config` is write-only: the credentials are stored apart from the record, are never
  returned by any endpoint, and no response field reports whether they are set.
- `cleanup` defaults to `retain`, which never reclaims tenant storage.
- The backend is registered enabled, and its record lives on the node that serves the request
  rather than being replicated to the realm's other nodes.

**Limits**
- `kind` is one of `s3`, `gcs`, `azblob`, `azdls` and `b2`, matched case-insensitively;
  source-only kinds such as WebDAV are not write backends and are refused.
- Config keys are a closed allowlist per kind, are lowercased before matching and may not be
  given twice.
- `s3` requires `endpoint` and `bucket`, also accepts `region`, `root` and `force_path_style`, and
  requires the secrets `access_key_id` and `secret_access_key`.
- `gcs` requires `bucket`, also accepts `root` and `endpoint`, and requires the secret
  `credential`.
- `azblob` requires `endpoint`, `container` and `account_name`, `azdls` requires `endpoint`,
  `filesystem` and `account_name`; both accept `root` and both require either `account_key` or
  `sas_token`.
- `b2` requires `bucket` and `bucket_id` and the secrets `application_key_id` and
  `application_key`.
- An endpoint must be an `https` URL spelled the way the HTTP client parses it, and `root` must be
  a relative path that stays below itself.

**Errors**: a failed probe is reported as 400 with the reason, including when the real cause is the
remote store being unreachable rather than the request being wrong."#,
    params(("group_id" = String, Path, description = "Group that will own the backend, as a 26-character ULID")),
    request_body(
        content = CreateGroupBackendRequest,
        description = "Backend name, kind, the public configuration for that kind, the write-only credentials and an optional cleanup policy",
        example = json!({
            "name": "institute-archive",
            "kind": "s3",
            "public_config": {
                "endpoint": "https://s3.example.test",
                "bucket": "institute-archive",
                "region": "eu-central-1",
                "root": "aruna/"
            },
            "secret_config": {
                "access_key_id": "EXAMPLE-KEY-ID-PLACEHOLDER",
                "secret_access_key": "EXAMPLE-SECRET-PLACEHOLDER"
            },
            "cleanup": {
                "mode": "reclaim",
                "after_secs": 86400
            }
        })
    ),
    responses(
        (
            status = 201,
            description = "Backend registered and proved writable; the credentials are stored but never echoed back",
            body = GroupBackendResponse,
            example = json!({
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "institute-archive",
                "kind": "s3",
                "public_config": {
                    "endpoint": "https://s3.example.test",
                    "bucket": "institute-archive",
                    "region": "eu-central-1",
                    "root": "aruna/"
                },
                "disabled": false,
                "cleanup": {
                    "mode": "reclaim",
                    "after_secs": 86400
                }
            })
        ),
        (
            status = 400,
            description = "Invalid group id, unknown kind, rejected configuration or cleanup policy, or a failed endpoint probe",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateGroupBackendRequest>,
) -> ServerResult<(StatusCode, Json<GroupBackendResponse>)> {
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let kind = GroupBackendKind::from_str(&request.kind)
        .map_err(|error| ServerError::BadRequestMessage(error.to_string()))?;
    let cleanup = CleanupPolicy::resolve(request.cleanup)?;

    let record = drive(
        CreateGroupBackendOperation::new(CreateGroupBackendInput {
            group_id,
            created_by: auth.user_id,
            name: request.name,
            kind,
            public_config: request.public_config,
            secret_config: request.secret_config,
            cleanup,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;

    Ok((StatusCode::CREATED, Json(record.into())))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/backends",
    tag = "data/storage",
    summary = "List a group's storage backends",
    description = r#"Lists every storage backend the group has registered on this node.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path, the same group administrator right that registration takes; a caller without it, and a
caller naming a group that does not exist, receives 403.

**Behavior**
- Disabled backends are included, ordered by backend id and therefore by registration time, in a
  single response without paging or a cursor.
- Credentials are never included and no field states whether any are stored.
- A `disabled` backend still appears and still serves reads of the copies it already holds; only
  new writes are refused.
- This is the node's own view: a backend registered against another node of the realm is not
  listed here."#,
    params(("group_id" = String, Path, description = "Group whose backends are listed, as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Every backend the group has registered on this node, enabled and disabled alike, credentials excluded",
            body = ListGroupBackendsResponse,
            example = json!({
                "backends": [
                    {
                        "backend_id": "01JBCKND0123456789ABCDEFGH",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "institute-archive",
                        "kind": "s3",
                        "public_config": {
                            "endpoint": "https://s3.example.test",
                            "bucket": "institute-archive",
                            "root": "aruna/"
                        },
                        "disabled": false,
                        "cleanup": {
                            "mode": "retain"
                        }
                    }
                ]
            })
        ),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_backends(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<ListGroupBackendsResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;

    let backends = drive(ListGroupBackendsOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;

    Ok(Json(ListGroupBackendsResponse {
        backends: backends.into_iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/backends/{backend_id}",
    tag = "data/storage",
    summary = "Read one registered storage backend",
    description = r#"Returns one storage backend registration as stored on this node.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path.

**Behavior**
- Returns the name, kind, public configuration, the `disabled` flag and the cleanup policy as
  stored on this node.
- The credentials are never returned and no field reports whether they are set.

**Errors**: the record is fetched by backend id and then checked against the group in the path, so
a backend belonging to a different group reads as not found rather than as forbidden."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the backend, as a 26-character ULID"),
        ("backend_id" = String, Path, description = "Backend to read, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The stored backend registration, credentials excluded",
            body = GroupBackendResponse,
            example = json!({
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "institute-archive",
                "kind": "s3",
                "public_config": {
                    "endpoint": "https://s3.example.test",
                    "bucket": "institute-archive",
                    "region": "eu-central-1",
                    "root": "aruna/"
                },
                "disabled": false,
                "cleanup": {"mode": "reclaim", "after_secs": 86400}
            })
        ),
        (
            status = 400,
            description = "The group id or the backend id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No backend with that id belongs to this group on this node",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
) -> ServerResult<Json<GroupBackendResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let backend_id = Ulid::from_str(&backend_id).map_err(|_| ServerError::BadRequest)?;

    let record = drive(GetGroupBackendOperation::new(backend_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .filter(|record| record.group_id == group_id)
        .ok_or(ServerError::NotFound)?;

    Ok(Json(record.into()))
}

#[utoipa::path(
    put,
    path = "/data/groups/{group_id}/storage/backends/{backend_id}",
    tag = "data/storage",
    summary = "Replace a storage backend registration",
    description = r#"Replaces a backend record whole, credentials included, after proving the new credentials.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path.

**Behavior**
- Every field must be sent again, including the credentials: they are replaced wholesale and are
  not carried over from the stored ones.
- The name, the remaining public keys, the credentials and the cleanup policy may change; an
  omitted `cleanup` falls back to `retain` rather than keeping the stored one.
- The `disabled` flag is preserved, and a replacement is deliberately allowed while the backend is
  disabled so a leaked credential can be rotated without opening the backend for writes first.
- The new credentials are proved with the same write-and-delete probe as registration before the
  record is updated, and a failed probe leaves the stored record untouched.

**Limits**
- The physical store is immutable, because stored copies record only the path below the backend's
  root.
- The kind and the keys that name the store, that is `endpoint`, `bucket`, `container`,
  `filesystem`, `account_name`, `bucket_id` and `root` as they apply to the kind, must be repeated
  exactly as registered; to move the data a new backend has to be registered instead.

**Errors**: a request that would move the backend to another store is refused with 400, as is a
failed probe, even when the cause is the store being unreachable."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the backend, as a 26-character ULID"),
        ("backend_id" = String, Path, description = "Backend to replace, as a 26-character ULID")
    ),
    request_body(
        content = CreateGroupBackendRequest,
        description = "The complete new definition; the kind and the store-naming keys must match the registered ones, and an omitted `cleanup` resets the policy to `retain`",
        example = json!({
            "name": "institute-archive",
            "kind": "s3",
            "public_config": {
                "endpoint": "https://s3.example.test",
                "bucket": "institute-archive",
                "region": "eu-central-1",
                "root": "aruna/"
            },
            "secret_config": {
                "access_key_id": "EXAMPLE-ROTATED-KEY-ID-PLACEHOLDER",
                "secret_access_key": "EXAMPLE-ROTATED-SECRET-PLACEHOLDER"
            },
            "cleanup": {
                "mode": "retain"
            }
        })
    ),
    responses(
        (
            status = 200,
            description = "The backend as stored after the replacement, credentials excluded",
            body = GroupBackendResponse,
            example = json!({
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "institute-archive",
                "kind": "s3",
                "public_config": {
                    "endpoint": "https://s3.example.test",
                    "bucket": "institute-archive",
                    "region": "eu-central-1",
                    "root": "aruna/"
                },
                "disabled": false,
                "cleanup": {
                    "mode": "retain"
                }
            })
        ),
        (
            status = 400,
            description = "An id is not a ULID, the configuration or cleanup policy failed validation, the request would move the backend to another store, or the probe against the endpoint failed",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No backend with that id belongs to this group on this node",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn replace_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
    Json(request): Json<CreateGroupBackendRequest>,
) -> ServerResult<Json<GroupBackendResponse>> {
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let backend_id = Ulid::from_str(&backend_id).map_err(|_| ServerError::BadRequest)?;
    let kind = GroupBackendKind::from_str(&request.kind)
        .map_err(|error| ServerError::BadRequestMessage(error.to_string()))?;
    let cleanup = CleanupPolicy::resolve(request.cleanup)?;

    let record = drive(
        ReplaceGroupBackendOperation::new(
            backend_id,
            CreateGroupBackendInput {
                group_id,
                created_by: auth.user_id,
                name: request.name,
                kind,
                public_config: request.public_config,
                secret_config: request.secret_config,
                cleanup,
            },
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;

    Ok(Json(record.into()))
}

#[utoipa::path(
    delete,
    path = "/data/groups/{group_id}/storage/backends/{backend_id}",
    tag = "data/storage",
    summary = "Disable a group's storage backend",
    description = r#"Marks a group's storage backend disabled for new writes.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path.

**Behavior**
- Despite the method this deletes neither the data nor the registration: it marks the backend
  disabled, and that is all a 204 promises.
- Write routing no longer chooses the backend, and a writer that resolved it just before the call
  is fenced by the same record and loses its commit rather than landing bytes afterwards.
- Reads of the copies already on the backend keep working, and the record stays visible in the
  listing with `disabled` set.
- Disabling a backend that is already disabled commits nothing and still answers 204, so the call
  may be repeated.
- Under `retain` the copies and the registration stay indefinitely.
- Under `reclaim` unreferenced copies are queued and physically deleted once their grace has
  passed; when the backend finally holds nothing and nothing holds it, a background sweep on this
  node deletes the record together with its credentials, after which the id reads as not found.
- Until that happens the backend can be enabled again, and its progress can be followed through
  the reclaim status of the same backend."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the backend, as a 26-character ULID"),
        ("backend_id" = String, Path, description = "Backend to disable, as a 26-character ULID")
    ),
    responses(
        (
            status = 204,
            description = "The backend is disabled for new writes; the response has no body and no content type"
        ),
        (
            status = 400,
            description = "The group id or the backend id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No backend with that id belongs to this group on this node, including one a previous disable has already drained and removed",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    set_disabled(&state, auth, &group_id, &backend_id, true).await?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/backends/{backend_id}/enable",
    tag = "data/storage",
    summary = "Re-enable a disabled storage backend",
    description = r#"Clears a backend's `disabled` flag so write routing may choose it again.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path.

**Behavior**
- The sweep that removes drained backends no longer considers the backend once it is enabled.
- Enabling a backend that is already enabled commits nothing and returns the stored record
  unchanged, so the call may be repeated.
- Nothing is contacted: unlike registration and replacement this does not probe the endpoint, so a
  backend whose credentials expired while it was disabled enables successfully and only fails at
  the next write; replace it to rotate them.
- A 200 says the flag is cleared on this node, not that any copy already reclaimed under a
  `reclaim` policy has come back.
- Once the removal sweep has deleted a drained backend there is nothing left to enable and the id
  reads as not found."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the backend, as a 26-character ULID"),
        ("backend_id" = String, Path, description = "Backend to enable, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The backend as stored after the call, with `disabled` cleared; credentials excluded",
            body = GroupBackendResponse,
            example = json!({
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "name": "institute-archive",
                "kind": "s3",
                "public_config": {
                    "endpoint": "https://s3.example.test",
                    "bucket": "institute-archive",
                    "root": "aruna/"
                },
                "disabled": false,
                "cleanup": {
                    "mode": "retain"
                }
            })
        ),
        (
            status = 400,
            description = "The group id or the backend id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No backend with that id belongs to this group on this node, including one that has already been drained and removed",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn enable_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
) -> ServerResult<Json<GroupBackendResponse>> {
    let record = set_disabled(&state, auth, &group_id, &backend_id, false).await?;

    Ok(Json(record.into()))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/backends/{backend_id}/reclaim/status",
    tag = "data/storage",
    summary = "Read a backend's pending reclaim work",
    description = r#"Counts the reclaim and cleanup work this node still owes one storage backend.

**Authentication**: bearer token issued for this realm with WRITE on the group's administrative
path; the backend must exist and belong to the group before anything is counted, otherwise the
answer is 404.

**Behavior**
- The counts are taken from the queues of the node serving the request at the moment of the call
  and describe that node only.
- `pending_candidates` is how many copies are waiting for the reclaim sweep to judge them and
  `queued_cleanups` how many physical deletes are still owed to this backend.
- `oldest_enqueued_at` is when the oldest item in either queue was enqueued, as an RFC 3339
  timestamp, absent when both queues are empty for this backend.
- Both sweeps run on their own timers, so non-zero counts are normal and mean work is pending
  rather than stuck; reclaim is only blocked when `oldest_enqueued_at` stops moving forward across
  calls.
- Candidates are queued whatever the cleanup policy says, so a backend on `retain` can report
  pending candidates too, and the sweep will drop them without deleting anything.
- A backend that reports zeroes and holds no data is the one the background sweep is free to
  remove.

**Limits**
- A scan stops at ten thousand candidates or one thousand cleanup rows; `truncated` then reports
  that both counts are lower bounds."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the backend, as a 26-character ULID"),
        ("backend_id" = String, Path, description = "Backend whose reclaim queues are counted, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "A point-in-time count of this node's reclaim and cleanup queues for the backend",
            body = ReclaimStatusResponse,
            example = json!({
                "pending_candidates": 3,
                "queued_cleanups": 1,
                "oldest_enqueued_at": "2026-04-09T14:23:11.123456789+00:00",
                "truncated": false
            })
        ),
        (
            status = 400,
            description = "The group id or the backend id is not a ULID",
            body = ErrorResponse
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (
            status = 403,
            description = "The token belongs to another realm, or the caller is not an administrator of the group, including when the group does not exist",
            body = ErrorResponse
        ),
        (
            status = 404,
            description = "No backend with that id belongs to this group on this node",
            body = ErrorResponse
        )
    ),
    security(("bearer_auth" = []))
)]
pub async fn backend_reclaim_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
) -> ServerResult<Json<ReclaimStatusResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let backend_id = Ulid::from_str(&backend_id).map_err(|_| ServerError::BadRequest)?;
    let context = state.get_ctx();

    drive(GetGroupBackendOperation::new(backend_id), &context)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .filter(|record| record.group_id == group_id)
        .ok_or(ServerError::NotFound)?;
    let status = backend_status(&context, &BackendRef::Group(backend_id))
        .await
        .map_err(ServerError::InternalError)?;

    Ok(Json(ReclaimStatusResponse {
        pending_candidates: status.pending_candidates,
        queued_cleanups: status.queued_cleanups,
        oldest_enqueued_at: status
            .oldest_enqueued_at
            .map(|time| chrono::DateTime::<chrono::Utc>::from(time).to_rfc3339()),
        truncated: status.truncated,
    }))
}

async fn set_disabled(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    group_id: &str,
    backend_id: &str,
    disabled: bool,
) -> ServerResult<GroupStorageBackend> {
    let (group_id, _) = admin_of_group(state, auth, group_id).await?;
    let backend_id = Ulid::from_str(backend_id).map_err(|_| ServerError::BadRequest)?;

    drive(
        SetDisabledOperation::new(group_id, backend_id, disabled),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        SetDisabledError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CleanupPolicy, CleanupStrategy, CreateGroupBackendRequest, backend_reclaim_status,
        create_group_backend, delete_group_backend, enable_group_backend, list_group_backends,
    };
    use crate::error::ServerError;
    use crate::routes::storage_routing::tests::setup_state;
    use axum::extract::{Path, State};
    use axum::{Extension, Json};
    use std::collections::HashMap;
    use ulid::Ulid;

    #[tokio::test]
    async fn requires_group_admin() {
        // Write rights are not enough: a backend receives the group's data.
        let test = setup_state().await;

        let result = list_group_backends(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));

        let disabled = delete_group_backend(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
        )
        .await;

        assert!(matches!(disabled, Err(ServerError::Forbidden)));

        let enabled = enable_group_backend(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
        )
        .await;

        assert!(matches!(enabled, Err(ServerError::Forbidden)));

        let status = backend_reclaim_status(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
        )
        .await;

        assert!(matches!(status, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn openapi_lists_enable() {
        let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();

        for path in ["enable", "reclaim-status"] {
            assert!(
                openapi["paths"]
                    .get(format!(
                        "/groups/{{group_id}}/storage-backends/{{backend_id}}/{path}"
                    ))
                    .is_some(),
                "{path}"
            );
        }
        for field in ["disabled", "cleanup"] {
            assert!(
                openapi["components"]["schemas"]["GroupBackendResponse"]["properties"]
                    .get(field)
                    .is_some()
            );
        }
    }

    #[test]
    fn policy_round_trips() {
        // Omitted means retain, and a reclaim without a grace takes the default.
        assert_eq!(
            CleanupPolicy::resolve(None).unwrap(),
            CleanupStrategy::Retain
        );
        assert_eq!(
            CleanupPolicy::resolve(Some(CleanupPolicy {
                mode: "reclaim".to_string(),
                after_secs: None,
            }))
            .unwrap(),
            CleanupStrategy::Reclaim {
                after: CleanupStrategy::DEFAULT_RECLAIM_AFTER
            }
        );
        assert_eq!(
            CleanupPolicy::from(CleanupStrategy::Reclaim {
                after: std::time::Duration::from_secs(60)
            })
            .after_secs,
            Some(60)
        );
        for bad in [("reclaim", Some(0)), ("retain", Some(60)), ("purge", None)] {
            assert!(
                CleanupPolicy::resolve(Some(CleanupPolicy {
                    mode: bad.0.to_string(),
                    after_secs: bad.1,
                }))
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn status_needs_backend() {
        // The record must exist and belong to the group before any queue scan.
        let test = setup_state().await;

        let result = backend_reclaim_status(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
        )
        .await;

        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn rejects_unknown_kind() {
        // WebDAV is deliberately not a write backend.
        let test = setup_state().await;

        let result = create_group_backend(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(CreateGroupBackendRequest {
                name: "tenant".to_string(),
                kind: "webdav".to_string(),
                public_config: HashMap::new(),
                secret_config: HashMap::new(),
                cleanup: None,
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
    }

    #[tokio::test]
    async fn lists_empty_backends() {
        let test = setup_state().await;

        let Json(listed) = list_group_backends(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();

        assert!(listed.backends.is_empty());
    }
}
