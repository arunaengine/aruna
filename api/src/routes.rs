use std::sync::Arc;
use axum::extract::{Path, Query, State};
use axum::{Extension, Json, Router};
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::{AuthContext, ServerState};

/// Build the group routes.
pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/groups/{id}", get(get_group))
        .route("/groups", post(create_group))
        .route("/groups", get(list_groups))
        .layer(Extension(Some(AuthContext{})))
        .with_state(state)
}

/// Request to create a new group.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRequest {
    /// Human-readable name for the group.
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupResponse {}


/// Create a new group.
///
/// POST /api/v1/groups
///
/// Per spec Section 2.1.2: Users MUST NOT create groups in foreign realms.
/// Group creation is restricted to users whose home realm matches the group's realm.
#[utoipa::path(
    post,
    path = "/groups",
    tag = "groups",
    request_body = CreateGroupRequest,
    responses(
        (status = 201, description = "Group created successfully", body = CreateGroupResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden - cannot create groups in foreign realms", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateGroupRequest>,
) -> ServerResult<(StatusCode, Json<CreateGroupResponse>)> {
    todo!()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiGroup {

}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListGroupsResponse {
    pub groups: Vec<ApiGroup>,
}

/// Pagination parameters for list endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct PaginationParams {
    /// Maximum number of items to return.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Number of items to skip.
    #[serde(default)]
    pub offset: Option<u32>,
}

impl PaginationParams {
    /// Get the limit with a default value.
    #[inline]
    #[must_use]
    pub fn limit_or(&self, default: u32) -> u32 {
        self.limit.unwrap_or(default)
    }

    /// Get the offset with a default value.
    #[inline]
    #[must_use]
    pub fn offset_or(&self, default: u32) -> u32 {
        self.offset.unwrap_or(default)
    }
}

/// List groups.
///
/// GET /api/v1/groups
#[utoipa::path(
    get,
    path = "/groups",
    tag = "groups",
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip")
    ),
    responses(
        (status = 200, description = "List of groups", body = ListGroupsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_groups(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(pagination): Query<PaginationParams>,
) -> ServerResult<(StatusCode, Json<ListGroupsResponse>)> {
    todo!()
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupInfoResponse {
    group: ApiGroup
}

/// Get group information.
///
/// GET /api/v1/groups/:id
#[utoipa::path(
    get,
    path = "/groups/{id}",
    tag = "groups",
    params(
        ("id" = String, Path, description = "Group ID (hex-encoded)")
    ),
    responses(
        (status = 200, description = "Group information", body = GroupInfoResponse),
        (status = 400, description = "Invalid group ID", body = ErrorResponse),
        (status = 404, description = "Group not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group(
    State(state): State<Arc<ServerState>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<GroupInfoResponse>> {
    todo!()
}
