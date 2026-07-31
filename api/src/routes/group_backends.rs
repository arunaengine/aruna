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
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((
        name = "storage-backends",
        description = "Tenant-registered write backends on a group's own object storage"
    )),
    paths(
        create_group_backend,
        list_group_backends,
        get_group_backend,
        replace_group_backend,
        delete_group_backend,
        enable_group_backend,
        group_backend_reclaim_status
    )
)]
pub struct GroupBackendsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/groups/{group_id}/storage-backends",
            post(create_group_backend).get(list_group_backends),
        )
        .route(
            "/groups/{group_id}/storage-backends/{backend_id}",
            get(get_group_backend)
                .put(replace_group_backend)
                .delete(delete_group_backend),
        )
        .route(
            "/groups/{group_id}/storage-backends/{backend_id}/enable",
            post(enable_group_backend),
        )
        .route(
            "/groups/{group_id}/storage-backends/{backend_id}/reclaim-status",
            get(group_backend_reclaim_status),
        )
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
    /// When the oldest queued candidate was enqueued.
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
    path = "/groups/{group_id}/storage-backends",
    tag = "storage-backends",
    params(("group_id" = String, Path, description = "Group id")),
    request_body = CreateGroupBackendRequest,
    responses(
        (status = 201, description = "Backend registered", body = GroupBackendResponse),
        (status = 400, description = "Invalid or unreachable backend", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends",
    tag = "storage-backends",
    params(("group_id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Registered backends", body = ListGroupBackendsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends/{backend_id}",
    tag = "storage-backends",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("backend_id" = String, Path, description = "Backend id")
    ),
    responses(
        (status = 200, description = "Registered backend", body = GroupBackendResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends/{backend_id}",
    tag = "storage-backends",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("backend_id" = String, Path, description = "Backend id")
    ),
    request_body = CreateGroupBackendRequest,
    responses(
        (status = 200, description = "Backend updated", body = GroupBackendResponse),
        (
            status = 400,
            description = "Invalid, unreachable, or store-changing backend",
            body = ErrorResponse
        ),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends/{backend_id}",
    tag = "storage-backends",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("backend_id" = String, Path, description = "Backend id")
    ),
    responses(
        (status = 204, description = "Backend disabled"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends/{backend_id}/enable",
    tag = "storage-backends",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("backend_id" = String, Path, description = "Backend id")
    ),
    responses(
        (status = 200, description = "Backend enabled", body = GroupBackendResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse)
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
    path = "/groups/{group_id}/storage-backends/{backend_id}/reclaim-status",
    tag = "storage-backends",
    params(
        ("group_id" = String, Path, description = "Group id"),
        ("backend_id" = String, Path, description = "Backend id")
    ),
    responses(
        (status = 200, description = "Reclaim queue depth", body = ReclaimStatusResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn group_backend_reclaim_status(
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
        CleanupPolicy, CleanupStrategy, CreateGroupBackendRequest, create_group_backend,
        delete_group_backend, enable_group_backend, group_backend_reclaim_status,
        list_group_backends,
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

        let status = group_backend_reclaim_status(
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
    fn cleanup_policy_round_trips() {
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
    async fn reclaim_status_needs_backend() {
        // The record must exist and belong to the group before any queue scan.
        let test = setup_state().await;

        let result = group_backend_reclaim_status(
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
