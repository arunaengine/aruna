use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage_routing::ensure_group_admin;
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, GroupBackendKind, GroupStorageBackend};
use aruna_operations::driver::drive;
use aruna_operations::group_backends::create::{
    CreateGroupBackendError, CreateGroupBackendInput, CreateGroupBackendOperation,
};
use aruna_operations::group_backends::delete::{
    DeleteGroupBackendError, DeleteGroupBackendOperation,
};
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
        delete_group_backend
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupBackendResponse {
    pub backend_id: String,
    pub group_id: String,
    pub name: String,
    pub kind: String,
    pub public_config: HashMap<String, String>,
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

    let record = drive(
        CreateGroupBackendOperation::new(CreateGroupBackendInput {
            group_id,
            created_by: auth.user_id,
            name: request.name,
            kind,
            public_config: request.public_config,
            secret_config: request.secret_config,
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
        (status = 200, description = "Credentials rotated", body = GroupBackendResponse),
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
        (status = 204, description = "Backend removed"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Backend not found", body = ErrorResponse),
        (status = 409, description = "Backend still holds object data", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_group_backend(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, backend_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let backend_id = Ulid::from_str(&backend_id).map_err(|_| ServerError::BadRequest)?;

    drive(
        DeleteGroupBackendOperation::new(group_id, backend_id),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        DeleteGroupBackendError::NotFound => ServerError::NotFound,
        DeleteGroupBackendError::StillReferenced => ServerError::Conflict(
            "storage backend still holds object data and cannot be removed".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::{
        CreateGroupBackendRequest, create_group_backend, delete_group_backend, list_group_backends,
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

        let removed = delete_group_backend(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path((test.group_id.to_string(), Ulid::generate().to_string())),
        )
        .await;

        assert!(matches!(removed, Err(ServerError::Forbidden)));
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
