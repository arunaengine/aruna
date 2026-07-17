use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmId, Role,
    usage_group_key,
};
use aruna_core::types::RoleId;
use aruna_operations::add_group_role::{
    AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation,
};
use aruna_operations::add_user_to_group::{
    AddUserToGroupError, AddUserToGroupInput, AddUserToGroupOperation,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupError, CreateGroupOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::remove_group_role::{
    RemoveGroupRoleConfig, RemoveGroupRoleError, RemoveGroupRoleOperation,
};
use aruna_operations::remove_user_from_group::{
    RemoveUserFromGroupError, RemoveUserFromGroupInput, RemoveUserFromGroupOperation,
};
use aruna_operations::resolve_users::{ResolveUsersInput, ResolveUsersOperation};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{Instrument, Span, field, info_span, trace};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "groups", description = "Group management operations")),
    paths(
        create_group,
        list_groups,
        get_group,
        get_group_usage,
        list_group_members,
        add_group_member,
        remove_group_member,
        leave_group,
        create_group_role,
        delete_group_role,
    )
)]
pub struct GroupsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/groups", post(create_group))
        .route("/groups", get(list_groups))
        .route("/groups/{id}", get(get_group))
        .route("/groups/{id}/usage", get(get_group_usage))
        .route(
            "/groups/{id}/members",
            get(list_group_members).post(add_group_member),
        )
        .route(
            "/groups/{id}/members/{user_id}",
            delete(remove_group_member),
        )
        .route("/groups/{id}/leave", post(leave_group))
        .route("/groups/{id}/roles", post(create_group_role))
        .route("/groups/{id}/roles/{role_id}", delete(delete_group_role))
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
    /// Only present when the caller is a member of the group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_users: Option<Vec<String>>,
    /// True when the role applies to every principal, including anonymous
    /// requests (it is assigned to the Everyone principal).
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddGroupMemberRequest {
    pub user_id: String,
    /// Role ids to assign; defaults to the role named "user" when omitted.
    #[serde(default)]
    pub role_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupRolesResponse {
    pub roles: Vec<RoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct RemoveGroupMemberQuery {
    /// Revoke only this role; all roles when omitted.
    #[serde(default)]
    pub role_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRoleRequest {
    pub name: String,
    /// Permission path -> "read" | "write" | "deny". Every path must stay
    /// inside the group.
    pub permissions: HashMap<String, String>,
    #[serde(default)]
    pub assigned_users: Vec<String>,
    /// Public roles apply to every principal — including anonymous requests —
    /// by assigning the Everyone principal (the nil user id).
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupMemberRoleResponse {
    pub role_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupMemberResponse {
    pub user_id: String,
    /// Display name from the user directory; None when the user is unresolvable.
    pub name: Option<String>,
    pub roles: Vec<GroupMemberRoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupMembersResponse {
    pub members: Vec<GroupMemberResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiGroup {
    pub display_name: String,
    pub group_id: String,
    pub realm_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<RoleResponse>>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
pub struct ListGroupsQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u32>,
    #[serde(default)]
    pub include: Option<String>,
}

impl PaginationParams {
    pub fn limit_or(&self, default: u32) -> u32 {
        self.limit.unwrap_or(default)
    }

    pub fn offset_or(&self, default: u32) -> u32 {
        self.offset.unwrap_or(default)
    }
}

impl ListGroupsQuery {
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

fn map_roles(auth: GroupAuthorizationDocument, realm_id: RealmId) -> Vec<RoleResponse> {
    map_roles_with_visibility(auth, realm_id, true)
}

/// Member lists are only visible to group members; open endpoints get the
/// roles without `assigned_users`.
fn map_roles_with_visibility(
    auth: GroupAuthorizationDocument,
    realm_id: RealmId,
    include_members: bool,
) -> Vec<RoleResponse> {
    auth.roles
        .into_iter()
        .map(|(role_id, role)| RoleResponse {
            role_id: role_id.to_string(),
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
            public: role.is_public(realm_id),
            // The Everyone principal is surfaced via `public`, not as a member.
            assigned_users: include_members.then(|| {
                role.assigned_users
                    .iter()
                    .filter(|u| !u.is_nil())
                    .map(|u| u.to_string())
                    .collect()
            }),
        })
        .collect()
}

fn is_group_member(auth_doc: &GroupAuthorizationDocument, user_id: UserId) -> bool {
    if user_id.is_nil() {
        return false;
    }

    auth_doc
        .roles
        .values()
        .any(|role| role.assigned_users.contains(&user_id))
}

fn parse_group_id(group_id: &str) -> ServerResult<Ulid> {
    Ulid::from_string(group_id).map_err(|_| ServerError::BadRequest)
}

fn parse_role_id(role_id: &str) -> ServerResult<RoleId> {
    Ulid::from_string(role_id).map_err(|_| ServerError::BadRequest)
}

fn parse_user_id(user_id: &str) -> ServerResult<UserId> {
    UserId::from_string(user_id).map_err(|_| ServerError::BadRequest)
}

fn parse_member_user_id(user_id: &str) -> ServerResult<UserId> {
    let user_id = parse_user_id(user_id)?;
    if user_id.is_nil() {
        return Err(ServerError::BadRequest);
    }
    Ok(user_id)
}

/// Write endpoints mint their permission checks from the caller identity, so
/// path-restricted (delegated) tokens must not reach them.
fn require_unrestricted(auth: Option<AuthContext>) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

fn actor_for(state: &ServerState, auth: &AuthContext) -> Actor {
    Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    }
}

async fn load_group(
    state: &ServerState,
    group_id: Ulid,
) -> ServerResult<(Group, GroupAuthorizationDocument)> {
    drive(
        GetGroupOperation::new(GetGroupConfig { group_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_get_group_error)
}

fn map_get_group_error(error: GetGroupError) -> ServerError {
    match error {
        GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

impl From<(Group, GroupAuthorizationDocument)> for CreateGroupResponse {
    fn from((group, auth): (Group, GroupAuthorizationDocument)) -> Self {
        Self {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth, group.realm_id),
        }
    }
}

impl From<(Group, GroupAuthorizationDocument)> for GroupInfoResponse {
    fn from((group, auth): (Group, GroupAuthorizationDocument)) -> Self {
        Self {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth, group.realm_id),
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
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 409, description = "Group creation conflict", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateGroupRequest>,
) -> ServerResult<(StatusCode, Json<CreateGroupResponse>)> {
    let auth = require_unrestricted(auth)?;
    let realm_id = state.get_realm_id();
    let request_span = Span::current();
    request_span.record("group_name", field::display(&request.name));
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let is_realm_admin = drive(
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

    // Self-service path: any unrestricted same-realm token subject may create
    // groups, capped by the realm quota config; realm admins are exempt.
    let owner_cap = if is_realm_admin {
        None
    } else {
        let realm_config = drive(GetRealmConfigOperation::new(realm_id), &state.get_ctx())
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?;
        realm_config.quota.max_groups_for(&auth.user_id)
    };

    trace!(
        event = "request.group.create.authorized",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        group_name = %request.name,
        "Authorized group creation request"
    );

    let create_span = info_span!(
        "group.create",
        "otel.kind" = "internal",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        group_name = %request.name,
        group_id = field::Empty,
    );
    let result = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            display_name: request.name,
            owner_cap,
        }),
        &state.get_ctx(),
    )
    .instrument(create_span.clone())
    .await
    .map_err(|err| match err {
        CreateGroupError::OwnedGroupLimitReached { limit } => {
            ServerError::Conflict(format!("owned group limit reached ({limit})"))
        }
        CreateGroupError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent group creation conflict; retry".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    create_span.record("group_id", field::display(result.0.group_id));
    request_span.record("group_id", field::display(result.0.group_id));

    trace!(
        event = "request.group.create.completed",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        group_id = %result.0.group_id,
        "Completed group creation request"
    );

    Ok((StatusCode::CREATED, Json(result.into())))
}

#[utoipa::path(
    get,
    path = "/groups",
    tag = "groups",
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip"),
        ("include" = Option<String>, Query, description = "Comma-separated includes. Currently supports roles")
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
    Query(query): Query<ListGroupsQuery>,
) -> ServerResult<(StatusCode, Json<ListGroupsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let include_roles = parse_list_groups_include(query.include.as_deref())?;
    let limit = query.limit_or(100).clamp(1, 1_000);
    let offset = query.offset_or(0);
    let result = drive(
        ListGroupOperation::with_pagination(limit as usize, offset as usize),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok((
        StatusCode::OK,
        Json(ListGroupsResponse {
            groups: build_api_groups(&state, result, include_roles, auth.user_id).await?,
        }),
    ))
}

fn parse_list_groups_include(include: Option<&str>) -> ServerResult<bool> {
    let Some(include) = include else {
        return Ok(false);
    };
    let mut include_roles = false;
    for value in include.split(',').map(str::trim) {
        if value.is_empty() {
            continue;
        }
        match value {
            "roles" => include_roles = true,
            _ => return Err(ServerError::BadRequest),
        }
    }
    Ok(include_roles)
}

async fn build_api_groups(
    state: &ServerState,
    groups: Vec<aruna_core::structs::Group>,
    include_roles: bool,
    caller: UserId,
) -> ServerResult<Vec<ApiGroup>> {
    let mut response = Vec::with_capacity(groups.len());
    for group in groups {
        let roles = if include_roles {
            let (_, auth_doc) = drive(
                GetGroupOperation::new(GetGroupConfig {
                    group_id: group.group_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?;
            let is_member = is_group_member(&auth_doc, caller);
            Some(map_roles_with_visibility(
                auth_doc,
                group.realm_id,
                is_member,
            ))
        } else {
            None
        };
        response.push(ApiGroup {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles,
        });
    }
    Ok(response)
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
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let (group, auth_doc) = load_group(&state, group_id).await?;
    let is_member = is_group_member(&auth_doc, auth.user_id);
    Ok((
        StatusCode::OK,
        Json(GroupInfoResponse {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles_with_visibility(auth_doc, group.realm_id, is_member),
        }),
    ))
}

fn map_add_member_error(error: AddUserToGroupError) -> ServerError {
    match error {
        AddUserToGroupError::Unauthorized => ServerError::Forbidden,
        AddUserToGroupError::InvalidUserId => ServerError::BadRequest,
        AddUserToGroupError::RoleNotFound | AddUserToGroupError::AuthDocNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_add_role_error(error: AddGroupRoleError) -> ServerError {
    match error {
        AddGroupRoleError::Unauthorized => ServerError::Forbidden,
        AddGroupRoleError::InvalidPublicRole
        | AddGroupRoleError::InvalidAssignedUser
        | AddGroupRoleError::ReservedRoleName => ServerError::BadRequest,
        AddGroupRoleError::GroupNotFound => ServerError::NotFound,
        AddGroupRoleError::CheckPermissionsError(
            AuthorizationError::GroupNotFound | AuthorizationError::AuthDocNotFound,
        ) => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_remove_member_error(error: RemoveUserFromGroupError) -> ServerError {
    match error {
        RemoveUserFromGroupError::Unauthorized => ServerError::Forbidden,
        RemoveUserFromGroupError::InvalidUserId => ServerError::BadRequest,
        RemoveUserFromGroupError::RoleNotFound | RemoveUserFromGroupError::AuthDocNotFound => {
            ServerError::NotFound
        }
        RemoveUserFromGroupError::LastAdmin => {
            ServerError::Conflict("the last admin of a group cannot be removed".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/groups/{id}/usage",
    tag = "groups",
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Group storage usage", body = crate::routes::info::UsageResponse),
        (status = 400, description = "Invalid group id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Group not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<crate::routes::info::UsageResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let (_, auth_doc) = load_group(&state, group_id).await?;
    if !is_group_member(&auth_doc, auth.user_id) {
        return Err(ServerError::Forbidden);
    }

    let local = crate::routes::info::load_usage_counters(&state, usage_group_key(group_id)).await?;
    let realm = crate::routes::info::load_realm_usage(
        &state,
        aruna_operations::usage_stats::RealmUsageScope::Group(group_id),
    )
    .await?;

    // The QuotaGate enforces against the group's realm-wide logical_bytes, so the
    // warning threshold is evaluated against the same counter.
    let realm_group_logical_bytes = realm.logical_bytes;
    let mut response = crate::routes::info::UsageResponse::new(local, realm);
    // Best effort: omit the quota block rather than failing the request if the
    // realm config is unavailable.
    if let Ok(config) = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        response.quota = Some(crate::routes::info::GroupQuotaStatus::resolve(
            &config.quota,
            &group_id,
            realm_group_logical_bytes,
        ));
    }
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/groups/{id}/members",
    tag = "groups",
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 200, description = "Group members", body = GroupMembersResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_members(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupMembersResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let (_, auth_doc) = load_group(&state, group_id).await?;
    if !is_group_member(&auth_doc, auth.user_id) {
        return Err(ServerError::Forbidden);
    }

    let mut roles_by_user: HashMap<UserId, Vec<GroupMemberRoleResponse>> = HashMap::new();
    for (role_id, role) in &auth_doc.roles {
        for user in &role.assigned_users {
            if user.is_nil() {
                continue;
            }
            roles_by_user
                .entry(*user)
                .or_default()
                .push(GroupMemberRoleResponse {
                    role_id: role_id.to_string(),
                    name: role.name.clone(),
                });
        }
    }

    let names = resolve_member_names(&state, roles_by_user.keys().copied().collect()).await;
    let mut members: Vec<GroupMemberResponse> = roles_by_user
        .into_iter()
        .map(|(user_id, mut roles)| {
            roles.sort_by(|a, b| a.name.cmp(&b.name));
            GroupMemberResponse {
                name: names.get(&user_id).cloned(),
                user_id: user_id.to_string(),
                roles,
            }
        })
        .collect();
    members.sort_by(|a, b| a.user_id.cmp(&b.user_id));

    Ok((StatusCode::OK, Json(GroupMembersResponse { members })))
}

/// Best-effort name lookup: a resolve failure leaves every member unnamed
/// rather than failing the members listing.
async fn resolve_member_names(
    state: &ServerState,
    user_ids: Vec<UserId>,
) -> HashMap<UserId, String> {
    match drive(
        ResolveUsersOperation::new(ResolveUsersInput {
            realm_id: state.get_realm_id(),
            user_ids,
        }),
        &state.get_ctx(),
    )
    .await
    {
        Ok(output) => output
            .users
            .into_iter()
            .map(|user| (user.user_id, user.name))
            .collect(),
        Err(error) => {
            trace!(event = "group.members.resolve_failed", error = %error);
            HashMap::new()
        }
    }
}

#[utoipa::path(
    post,
    path = "/groups/{id}/members",
    tag = "groups",
    request_body = AddGroupMemberRequest,
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 201, description = "Member added", body = GroupRolesResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Group or role not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_group_member(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<AddGroupMemberRequest>,
) -> ServerResult<(StatusCode, Json<GroupRolesResponse>)> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let user_id = parse_member_user_id(&request.user_id)?;

    let role_ids: HashSet<Ulid> = match &request.role_ids {
        Some(role_ids) if !role_ids.is_empty() => role_ids
            .iter()
            .map(|role_id| parse_role_id(role_id))
            .collect::<ServerResult<_>>()?,
        _ => {
            let (_, auth_doc) = load_group(&state, group_id).await?;
            let role_ids = auth_doc
                .roles
                .iter()
                .filter_map(|(role_id, role)| (role.name == "user").then_some(*role_id))
                .collect::<HashSet<_>>();
            if role_ids.len() != 1 {
                return Err(ServerError::BadRequest);
            }
            role_ids
        }
    };
    if role_ids.is_empty() {
        return Err(ServerError::BadRequest);
    }

    let auth_doc = drive(
        AddUserToGroupOperation::new(AddUserToGroupInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id,
            role_ids,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_add_member_error)?;

    Ok((
        StatusCode::CREATED,
        Json(GroupRolesResponse {
            roles: map_roles(auth_doc, state.get_realm_id()),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/groups/{id}/members/{user_id}",
    tag = "groups",
    params(
        ("id" = String, Path, description = "Group id"),
        ("user_id" = String, Path, description = "User id to remove"),
        ("role_id" = Option<String>, Query, description = "Revoke only this role")
    ),
    responses(
        (status = 204, description = "Member removed"),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 409, description = "Last admin cannot be removed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_group_member(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, user_id)): Path<(String, String)>,
    Query(query): Query<RemoveGroupMemberQuery>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let user_id = parse_member_user_id(&user_id)?;
    let role_ids = query
        .role_id
        .as_deref()
        .map(|role_id| parse_role_id(role_id).map(|role_id| HashSet::from([role_id])))
        .transpose()?;

    drive(
        RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id,
            role_ids,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_remove_member_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/groups/{id}/leave",
    tag = "groups",
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 204, description = "Left the group"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 409, description = "Last admin cannot leave", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn leave_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;

    drive(
        RemoveUserFromGroupOperation::new(RemoveUserFromGroupInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id: auth.user_id,
            role_ids: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_remove_member_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/groups/{id}/roles",
    tag = "groups",
    request_body = CreateGroupRoleRequest,
    params(("id" = String, Path, description = "Group id")),
    responses(
        (status = 201, description = "Role created", body = RoleResponse),
        (status = 400, description = "Invalid request or foreign permission path", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Group not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group_role(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateGroupRoleRequest>,
) -> ServerResult<(StatusCode, Json<RoleResponse>)> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let realm_id = state.get_realm_id();

    let name = request.name.trim().to_string();
    if name.is_empty() || matches!(name.as_str(), "admin" | "user") {
        return Err(ServerError::BadRequest);
    }

    // A group admin must not be able to mint a role granting paths outside
    // their own group (privilege escalation).
    let group_prefix = format!("/{realm_id}/g/{group_id}/");
    let mut permissions = HashMap::with_capacity(request.permissions.len());
    for (path, permission) in &request.permissions {
        if !path.starts_with(&group_prefix) {
            return Err(ServerError::BadRequest);
        }
        let permission = match permission.to_ascii_lowercase().as_str() {
            "read" => Permission::READ,
            "write" => Permission::WRITE,
            "deny" => Permission::DENY,
            _ => return Err(ServerError::BadRequest),
        };
        permissions.insert(path.clone(), permission);
    }
    if request.public
        && permissions
            .values()
            .any(|permission| permission != &Permission::READ)
    {
        return Err(ServerError::BadRequest);
    }

    let mut assigned_users = request
        .assigned_users
        .iter()
        .map(|user_id| parse_member_user_id(user_id))
        .collect::<ServerResult<HashSet<UserId>>>()?;
    if request.public {
        assigned_users.insert(UserId::nil(realm_id));
    }

    let role_id = Ulid::r#gen();
    let (_, auth_doc) = drive(
        AddGroupRoleOperation::new(AddGroupRoleConfig {
            auth_context: auth.clone(),
            actor: actor_for(&state, &auth),
            realm_id,
            group_id,
            role: Role {
                role_id,
                name,
                permissions,
                assigned_users,
            },
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_add_role_error)?;

    let role = map_roles(auth_doc, realm_id)
        .into_iter()
        .find(|role| role.role_id == role_id.to_string())
        .ok_or_else(|| ServerError::InternalError("created role missing".to_string()))?;

    Ok((StatusCode::CREATED, Json(role)))
}

#[utoipa::path(
    delete,
    path = "/groups/{id}/roles/{role_id}",
    tag = "groups",
    params(
        ("id" = String, Path, description = "Group id"),
        ("role_id" = String, Path, description = "Role id to delete")
    ),
    responses(
        (status = 204, description = "Role deleted"),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Role not found", body = ErrorResponse),
        (status = 409, description = "Admin role cannot be deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_group_role(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, role_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let role_id = parse_role_id(&role_id)?;

    drive(
        RemoveGroupRoleOperation::new(RemoveGroupRoleConfig {
            auth_context: auth.clone(),
            actor: actor_for(&state, &auth),
            realm_id: state.get_realm_id(),
            group_id,
            role_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        RemoveGroupRoleError::Unauthorized => ServerError::Forbidden,
        RemoveGroupRoleError::RoleNotFound | RemoveGroupRoleError::AuthDocNotFound => {
            ServerError::NotFound
        }
        RemoveGroupRoleError::AdminRoleUndeletable => {
            ServerError::Conflict("the admin role cannot be deleted".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::{ListGroupsQuery, get_group, get_group_usage, list_group_members, list_groups};
    use crate::error::ServerError;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, USER_KEYSPACE};
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, NodeCapabilities, RealmId, User,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use axum::extract::{Path, Query, State};
    use axum::{Extension, Json};
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn store_bytes(state: &ServerState, keyspace: &str, key: Vec<u8>, value: Vec<u8>) {
        match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: keyspace.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write result: {other:?}"),
        }
    }

    async fn seed_group(state: &ServerState, owner: UserId) -> Ulid {
        let realm_id = state.get_realm_id();
        let group_id = Ulid::r#gen();
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "Test".to_string(),
            group_id,
            realm_id,
            roles: auth_doc.roles.keys().copied().collect(),
            owner,
        };
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: owner,
            realm_id,
        };
        store_bytes(
            state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
        store_bytes(
            state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(&actor).unwrap(),
        )
        .await;
        group_id
    }

    async fn store_user(state: &ServerState, user_id: UserId, name: &str) {
        let user = User {
            user_id,
            name: name.to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id,
            realm_id: user_id.realm_id,
        };
        store_bytes(
            state,
            USER_KEYSPACE,
            user_id.to_bytes(),
            user.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    fn member_auth(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: user_id.realm_id,
            path_restrictions: None,
        }
    }

    async fn setup_state() -> (Arc<ServerState>, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_signing_key =
            SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                iroh::SecretKey::generate().public(),
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, tempdir)
    }

    fn foreign_auth() -> AuthContext {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        AuthContext {
            user_id: UserId::local(Ulid::r#gen(), realm_id),
            realm_id,
            path_restrictions: None,
        }
    }

    /// Anonymous callers get 401, foreign-realm tokens 403: neither may
    /// enumerate the local group or usage directory.
    #[tokio::test]
    async fn group_directory_requires_realm() {
        let (state, _tempdir) = setup_state().await;
        let group_id = Ulid::r#gen().to_string();

        assert!(matches!(
            list_groups(
                State(state.clone()),
                Extension(None),
                Query(ListGroupsQuery::default())
            )
            .await,
            Err(ServerError::Unauthorized)
        ));
        assert!(matches!(
            list_groups(
                State(state.clone()),
                Extension(Some(foreign_auth())),
                Query(ListGroupsQuery::default())
            )
            .await,
            Err(ServerError::Forbidden)
        ));

        for auth in [None, Some(foreign_auth())] {
            let expected = if auth.is_none() {
                ServerError::Unauthorized
            } else {
                ServerError::Forbidden
            };
            for result in [
                get_group(
                    State(state.clone()),
                    Extension(auth.clone()),
                    Path(group_id.clone()),
                )
                .await
                .map(|_| ()),
                get_group_usage(
                    State(state.clone()),
                    Extension(auth.clone()),
                    Path(group_id.clone()),
                )
                .await
                .map(|_| ()),
                list_group_members(
                    State(state.clone()),
                    Extension(auth.clone()),
                    Path(group_id.clone()),
                )
                .await
                .map(|_| ()),
            ] {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    expected.to_string(),
                    "group route leaked to {auth:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn joins_member_names() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::r#gen(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        store_user(&state, owner, "Owner").await;

        let (status, Json(body)) = list_group_members(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(status, axum::http::StatusCode::OK);
        let member = body
            .members
            .iter()
            .find(|member| member.user_id == owner.to_string())
            .unwrap();
        assert_eq!(member.name.as_deref(), Some("Owner"));
    }

    #[tokio::test]
    async fn unresolved_member_none() {
        // A member without a stored user record still lists, with name None.
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::r#gen(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;

        let (status, Json(body)) = list_group_members(
            State(state),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(status, axum::http::StatusCode::OK);
        let member = body
            .members
            .iter()
            .find(|member| member.user_id == owner.to_string())
            .unwrap();
        assert_eq!(member.name, None);
    }
}
