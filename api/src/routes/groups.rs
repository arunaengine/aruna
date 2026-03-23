use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, Permission,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::list_groups::ListGroupOperation;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "groups", description = "Group management operations")),
    paths(create_group, list_groups, get_group)
)]
pub struct GroupsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/groups", post(create_group))
        .route("/groups", get(list_groups))
        .route("/groups/{id}", get(get_group))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupResponse {
    pub display_name: String,
    pub group_id: String,
    pub realm_id: String,
    pub roles: Vec<RoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RoleResponse {
    pub role_id: String,
    pub name: String,
    pub permissions: HashMap<String, String>,
    pub assigned_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiGroup {
    pub display_name: String,
    pub group_id: String,
    pub realm_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListGroupsResponse {
    pub groups: Vec<ApiGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct PaginationParams {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u32>,
}

impl PaginationParams {
    pub fn limit_or(&self, default: u32) -> u32 {
        self.limit.unwrap_or(default)
    }

    pub fn offset_or(&self, default: u32) -> u32 {
        self.offset.unwrap_or(default)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupInfoResponse {
    pub display_name: String,
    pub group_id: String,
    pub realm_id: String,
    pub roles: Vec<RoleResponse>,
}

fn map_roles(auth: GroupAuthorizationDocument) -> Vec<RoleResponse> {
    auth.roles
        .into_iter()
        .map(|(role_id, role)| RoleResponse {
            role_id: role_id.to_string(),
            name: role.name,
            permissions: role
                .permissions
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
            assigned_users: role.assigned_users.iter().map(|u| u.to_string()).collect(),
        })
        .collect()
}

impl From<(Group, GroupAuthorizationDocument)> for CreateGroupResponse {
    fn from((group, auth): (Group, GroupAuthorizationDocument)) -> Self {
        Self {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth),
        }
    }
}

impl From<(Group, GroupAuthorizationDocument)> for GroupInfoResponse {
    fn from((group, auth): (Group, GroupAuthorizationDocument)) -> Self {
        Self {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth),
        }
    }
}

#[utoipa::path(
    post,
    path = "/groups",
    tag = "groups",
    request_body = CreateGroupRequest,
    responses(
        (status = 201, description = "Group created", body = CreateGroupResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateGroupRequest>,
) -> ServerResult<(StatusCode, Json<CreateGroupResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{realm_id}/admin/groups"),
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::AuthDocNotFound => ServerError::Forbidden,
        _ => ServerError::InternalError(err.to_string()),
    })?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }

    let result = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            display_name: request.name,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((StatusCode::CREATED, Json(result.into())))
}

#[utoipa::path(
    get,
    path = "/groups",
    tag = "groups",
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip")
    ),
    responses(
        (status = 200, description = "List groups", body = ListGroupsResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_groups(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(pagination): Query<PaginationParams>,
) -> ServerResult<(StatusCode, Json<ListGroupsResponse>)> {
    let _auth = auth.ok_or(ServerError::Unauthorized)?;
    let limit = pagination.limit_or(100).clamp(1, 1_000);
    let offset = pagination.offset_or(0);
    let result = drive(
        ListGroupOperation::with_pagination(limit as usize, offset as usize),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok((
        StatusCode::OK,
        Json(ListGroupsResponse {
            groups: result
                .iter()
                .map(|g| ApiGroup {
                    display_name: g.display_name.clone(),
                    group_id: g.group_id.to_string(),
                    realm_id: g.realm_id.to_string(),
                })
                .collect(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/groups/{id}",
    tag = "groups",
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Group info", body = GroupInfoResponse),
        (status = 400, description = "Invalid group id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "Group not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupInfoResponse>)> {
    let _auth = auth.ok_or(ServerError::Unauthorized)?;
    let group_id = Ulid::from_string(&group_id).map_err(|_| ServerError::BadRequest)?;
    let result = drive(GetGroupOperation::new(GetGroupConfig { group_id }), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok((StatusCode::OK, Json(result.into())))
}
