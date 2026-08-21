use crate::auth::{ensure_permission, permission_granted, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmId, Role,
    blob_bucket_permission_path, blob_group_permission_path, blob_object_permission_path,
    usage_group_key,
};
use aruna_core::types::RoleId;
use aruna_operations::add_group_role::{
    AddGroupRoleConfig, AddGroupRoleError, AddGroupRoleOperation,
};
use aruna_operations::add_user_to_group::{
    AddUserToGroupError, AddUserToGroupInput, AddUserToGroupOperation,
};
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
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::list_buckets::{ListBucketsInput, ListBucketsOperation};
use aruna_operations::s3::list_objects_v2::{
    ListObjectsV2ContinuationToken, ListObjectsV2Input, ListObjectsV2Operation,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{Instrument, Span, field, info_span, trace};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "groups", description = "Group management operations"))
)]
pub struct GroupsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(GroupsApiDoc::openapi())
        .routes(routes!(create_group, list_groups))
        .routes(routes!(get_group))
        .routes(routes!(get_group_usage))
        .routes(routes!(list_data_paths))
        .routes(routes!(list_group_members, add_group_member))
        .routes(routes!(remove_group_member))
        .routes(routes!(leave_group))
        .routes(routes!(create_group_role))
        .routes(routes!(delete_group_role))
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
    summary = "Create a group in this realm",
    description = "Requires a bearer token issued for this realm; a token confined to a path subset is refused. Group creation is self-service: any unrestricted realm member may create groups up to the realm's per-user group quota, and a caller holding WRITE on the realm group-admin path is exempt from that cap. The caller becomes the owner and the only member of the new group's admin role, next to the default user and viewer roles. The write commits on this node and replicates to the rest of the realm afterwards, so another node may not list the group immediately.",
    request_body(
        content = CreateGroupRequest,
        description = "Display name for the new group. It is stored as given and need not be unique.",
        example = json!({"name": "Proteomics Lab"})
    ),
    responses(
        (
            status = 201,
            description = "Group created on this node, with its initial roles. Assigned users are listed because the caller is a member.",
            body = CreateGroupResponse,
            example = json!({
                "display_name": "Proteomics Lab",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "roles": [
                    {
                        "role_id": "01JROLEADMIN0123456789ABCD",
                        "name": "admin",
                        "permissions": {
                            "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/**": "Write"
                        },
                        "assigned_users": [
                            "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
                        ],
                        "public": false
                    },
                    {
                        "role_id": "01JROLEUSER00123456789ABCD",
                        "name": "user",
                        "permissions": {
                            "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/**": "Write"
                        },
                        "assigned_users": [],
                        "public": false
                    }
                ]
            })
        ),
        (status = 400, description = "Request body is not a valid create-group document", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or is confined to a path subset", body = ErrorResponse),
        (status = 409, description = "The caller's group quota is exhausted, or a concurrent create conflicted; the latter may be retried unchanged", body = ErrorResponse)
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

    let is_realm_admin = permission_granted(
        &state,
        &auth,
        format!("/{realm_id}/admin/groups"),
        Permission::WRITE,
    )
    .await?;

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
    summary = "List the groups of this realm",
    description = "Requires a bearer token issued for this realm: an anonymous caller is rejected and a token from another realm is forbidden. Reads the group directory as replicated to this node, so a group created elsewhere can be missing until it arrives here. Every realm member sees each group's id, realm and display name. `include=roles` additionally returns each group's roles and their permission paths, but the users assigned to a role are only included for groups the caller is a member of; for every other group the `assigned_users` field is omitted and a role that applies to everyone is visible only through its `public` flag.",
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return; defaults to 100 and is clamped to the range 1-1000"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip from the start of the directory; defaults to 0"),
        ("include" = Option<String>, Query, description = "Comma-separated extras. Currently supports roles; blank entries are ignored and any other value is rejected as a bad request")
    ),
    responses(
        (
            status = 200,
            description = "Groups visible to the caller, with member-only fields hidden for groups the caller does not belong to",
            body = ListGroupsResponse,
            example = json!({
                "groups": [
                    {
                        "display_name": "Proteomics Lab",
                        "group_id": "01JABCDEF0123456789ABCDEFG",
                        "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "roles": [
                            {
                                "role_id": "01JROLEUSER00123456789ABCD",
                                "name": "user",
                                "permissions": {
                                    "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/**": "Write"
                                },
                                "public": false
                            }
                        ]
                    }
                ]
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse)
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
    summary = "Read one group's directory entry",
    description = "Requires a bearer token issued for this realm; no group membership is needed, because every realm member may look up any group in the realm. Reads the copy replicated to this node. Members receive the full role list including the users assigned to each role; a non-member receives the same roles and permission paths with the `assigned_users` field omitted, and learns only from the `public` flag that a role applies to everyone. A group whose record or authorization document is not present on this node reads as not found.",
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The group and its roles, with member lists included only for a caller who is a member",
            body = GroupInfoResponse,
            example = json!({
                "display_name": "Proteomics Lab",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                "roles": [
                    {
                        "role_id": "01JROLEADMIN0123456789ABCD",
                        "name": "admin",
                        "permissions": {
                            "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/**": "Write"
                        },
                        "assigned_users": [
                            "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
                        ],
                        "public": false
                    }
                ]
            })
        ),
        (status = 400, description = "The path segment is not a valid ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
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
        | AddGroupRoleError::UnconfinedRolePath
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
    summary = "Read a group's storage usage",
    description = "Requires a bearer token issued for this realm and membership in the group: membership is the only check, so a realm administrator who is not a member is forbidden. The flat counters report what this node stores for the group, while `realm` reports the realm-wide totals aggregated from the usage summaries the realm's nodes publish, which trail recent writes. `quota` restates the realm quota configuration for this group together with a warning flag evaluated against the group's realm-wide logical bytes; it is omitted when the realm configuration cannot be read, and the document count reported by the realm-wide usage endpoint is never included here.",
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Local counters for this node, realm-wide totals, and the group's quota status when it is available",
            body = crate::routes::info::UsageResponse,
            example = json!({
                "buckets": 3,
                "objects": 1284,
                "stored_blobs": 1190,
                "stored_bytes": 87412338176_i64,
                "logical_bytes": 91002113024_i64,
                "referenced_bytes": 91002113024_i64,
                "realm": {
                    "buckets": 5,
                    "objects": 2048,
                    "stored_blobs": 1902,
                    "stored_bytes": 140733193388_i64,
                    "logical_bytes": 152882105100_i64,
                    "referenced_bytes": 152882105100_i64
                },
                "quota": {
                    "quota_bytes": 214748364800_i64,
                    "ceiling_bytes": 236223201280_i64,
                    "warn_threshold_percent": 80,
                    "warning": false
                }
            })
        ),
        (status = 400, description = "The path segment is not a valid ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller is not a member of the group", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
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
    summary = "List the members of a group",
    description = "Requires a bearer token issued for this realm and membership in the group; the member list is never exposed to outsiders, so a non-member is forbidden. Returns every member in one response, sorted by user id, each with the roles that assign them, sorted by role name. A role that applies to everyone contributes no member here, since the principal standing for everyone is not a user. Display names are resolved from the realm's user directory as a best effort: a member without a resolvable record is returned with a null name instead of failing the listing.",
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Every member of the group with their roles; names may be null when the user directory cannot resolve them",
            body = GroupMembersResponse,
            example = json!({
                "members": [
                    {
                        "user_id": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "name": "Ada Lovelace",
                        "roles": [
                            {"role_id": "01JROLEADMIN0123456789ABCD", "name": "admin"}
                        ]
                    },
                    {
                        "user_id": "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "name": null,
                        "roles": [
                            {"role_id": "01JROLEUSER00123456789ABCD", "name": "user"}
                        ]
                    }
                ]
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller is not a member of the group", body = ErrorResponse)
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
    summary = "Add a user to a group",
    description = "Requires an unrestricted bearer token issued for this realm and WRITE on the group's administrative path for the user being added, so authority can be delegated per member. When `role_ids` is omitted or empty the user is assigned the group's role named user, and the request is rejected when that role is missing or ambiguous. Adding a user who already holds the roles is accepted and changes nothing. The response is the group's complete role list after the change, including the users assigned to each role. The change commits on this node and replicates to the rest of the realm afterwards.",
    request_body(
        content = AddGroupMemberRequest,
        description = "User to add, and optionally the exact roles to assign instead of the default user role.",
        example = json!({
            "user_id": "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
            "role_ids": ["01JROLEUSER00123456789ABCD"]
        })
    ),
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 201,
            description = "The group's roles after the assignment",
            body = GroupRolesResponse,
            example = json!({
                "roles": [
                    {
                        "role_id": "01JROLEUSER00123456789ABCD",
                        "name": "user",
                        "permissions": {
                            "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/**": "Write"
                        },
                        "assigned_users": [
                            "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
                        ],
                        "public": false
                    }
                ]
            })
        ),
        (status = 400, description = "Malformed ids, a user id standing for everyone, or no default user role to fall back on", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token is confined to a path subset, or the caller lacks write access to this member of the group", body = ErrorResponse),
        (status = 404, description = "No such group on this node, or one of the requested roles does not exist", body = ErrorResponse)
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

    ensure_permission(
        &state,
        &auth,
        format!(
            "/{}/g/{}/admin/users/{}",
            state.get_realm_id(),
            group_id,
            user_id
        ),
        Permission::WRITE,
    )
    .await?;

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
    summary = "Remove a member from a group or revoke one of their roles",
    description = "Requires an unrestricted bearer token issued for this realm. Removing yourself needs no group permission; removing anyone else requires WRITE on the group's administrative path for that user. Without `role_id` the user loses every role in the group, with it only that one assignment is revoked and the rest are kept. A group must keep at least one administrator, so the request is refused when it would strip the last one. The change commits on this node and replicates to the rest of the realm afterwards.",
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("user_id" = String, Path, description = "Member to remove, in the realm-qualified user id form `<user ULID>@<realm id>`"),
        ("role_id" = Option<String>, Query, description = "Revoke only this role, given as a 26-character ULID; when omitted every role of the user in this group is revoked")
    ),
    responses(
        (status = 204, description = "The membership or role assignment is gone; no response body is returned"),
        (status = 400, description = "Malformed group, user or role id, or a user id standing for everyone", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token is confined to a path subset, or the caller lacks write access to this member of the group", body = ErrorResponse),
        (status = 409, description = "The removal would leave the group without an administrator", body = ErrorResponse)
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

    // Self-leave via this endpoint needs no admin permission, matching the operation.
    if user_id != auth.user_id {
        ensure_permission(
            &state,
            &auth,
            format!(
                "/{}/g/{}/admin/users/{}",
                state.get_realm_id(),
                group_id,
                user_id
            ),
            Permission::WRITE,
        )
        .await?;
    }

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
    summary = "Leave a group",
    description = "Self-scoped: the caller drops every role they hold in the group, and no group permission is required because the token's own subject is the only user affected. Requires an unrestricted bearer token issued for this realm; a token confined to a path subset is refused. A group must keep at least one administrator, so the last one cannot leave and must hand the role over first. Leaving a group the caller does not belong to changes nothing. The change commits on this node and replicates to the rest of the realm afterwards.",
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (status = 204, description = "The caller no longer holds any role in the group; no response body is returned"),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 409, description = "The caller is the group's last administrator", body = ErrorResponse)
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
    summary = "Create a role in a group",
    description = "Requires an unrestricted bearer token issued for this realm and WRITE on the group's administrative path. The name is trimmed, must not be empty and must not be admin or user, which are reserved for the built-in roles. Every permission path must lie inside the group's own path, so a group administrator cannot mint authority over anything else, and each is granted as read, write or deny (accepted case-insensitively, reported capitalised). A public role applies to every principal including anonymous callers and may therefore only carry read grants. The role commits on this node and replicates to the rest of the realm afterwards.",
    request_body(
        content = CreateGroupRoleRequest,
        description = "Role name, the permission paths it grants inside the group, and the users it is assigned to.",
        example = json!({
            "name": "readers",
            "permissions": {
                "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/**": "read"
            },
            "assigned_users": [
                "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
            ],
            "public": false
        })
    ),
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 201,
            description = "The created role as stored, with its generated id",
            body = RoleResponse,
            example = json!({
                "role_id": "01JROLEREADERS123456789ABC",
                "name": "readers",
                "permissions": {
                    "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/**": "Read"
                },
                "assigned_users": [
                    "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
                ],
                "public": false
            })
        ),
        (status = 400, description = "Reserved or empty name, an unknown grant value, a permission path outside the group, a malformed assigned user, or a public role asking for more than read", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token is confined to a path subset, or the caller does not administer the group", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
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

    ensure_permission(
        &state,
        &auth,
        format!("/{realm_id}/g/{group_id}/admin"),
        Permission::WRITE,
    )
    .await?;

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

    let role_id = Ulid::generate();
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
    summary = "Delete a role from a group",
    description = "Requires an unrestricted bearer token issued for this realm and WRITE on the group's administrative path. Deleting a role revokes it from every user holding it, which can leave a user with no role in the group at all. The built-in admin role is permanent and cannot be deleted, so a group never loses its administrative path. The change commits on this node and replicates to the rest of the realm afterwards.",
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("role_id" = String, Path, description = "Role to delete, as a 26-character ULID")
    ),
    responses(
        (status = 204, description = "The role and all of its assignments are gone; no response body is returned"),
        (status = 400, description = "Malformed group or role id", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token is confined to a path subset, or the caller does not administer the group", body = ErrorResponse),
        (status = 404, description = "No such role in this group, or the group is not present on this node", body = ErrorResponse),
        (status = 409, description = "The built-in admin role cannot be deleted", body = ErrorResponse)
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

    ensure_permission(
        &state,
        &auth,
        format!("/{}/g/{}/admin", state.get_realm_id(), group_id),
        Permission::WRITE,
    )
    .await?;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DataPathKind {
    Folder,
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataPathEntry {
    /// Node-scoped data permission path as consumed by role permissions.
    pub permission_path: String,
    pub kind: DataPathKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataPathsResponse {
    pub entries: Vec<DataPathEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct DataPathsQuery {
    /// Data permission path to browse under; empty lists the group's buckets, a
    /// bucket path lists its contents, any other bare segment filters bucket
    /// names by that prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub delimiter: Option<String>,
    #[serde(default)]
    pub continuation_token: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[utoipa::path(
    get,
    path = "/groups/{id}/data-paths",
    tag = "groups",
    summary = "Browse the data permission paths of a group",
    description = "Requires a bearer token issued for this realm and membership in the group, and every page is additionally authorized as a data read: browsing the bucket level needs READ on the group's data root, browsing inside a bucket needs READ on that bucket or on the prefix being listed, so a token confined to a narrower path sees only what it may read. The entries are the permission paths that role grants are written against, folders ending at the delimiter and objects as leaves, and they are scoped to this node, so a prefix belonging to another node is rejected. Bucket names are globally unique: naming a bucket owned by another group returns an empty page instead of an error. Paging is forward-only through an opaque token that must be echoed back unchanged; a response without one is the last page.",
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("prefix" = Option<String>, Query, description = "Data permission path to browse under; empty or the group data path lists buckets; a bucket path lists that bucket's contents; any other bare segment filters bucket names by that prefix"),
        ("delimiter" = Option<String>, Query, description = "Folder delimiter that collapses keys sharing a prefix into one folder entry, typically '/'; when omitted every matching object is listed individually"),
        ("continuation_token" = Option<String>, Query, description = "Opaque base64 token copied from the previous page; omit it to start at the beginning, and pass back the exact value received"),
        ("limit" = Option<u32>, Query, description = "Maximum entries per page; defaults to 1000 and is clamped to the range 1-1000")
    ),
    responses(
        (
            status = 200,
            description = "One page of browsable data permission paths on this node, with a continuation token when more entries remain",
            body = DataPathsResponse,
            example = json!({
                "entries": [
                    {
                        "permission_path": "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/proteomics/runs/",
                        "kind": "folder"
                    },
                    {
                        "permission_path": "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978/proteomics/README.md",
                        "kind": "object"
                    }
                ],
                "continuation_token": "cHJvdGVvbWljcy9SRUFETUUubWQ="
            })
        ),
        (status = 400, description = "Malformed group id, a prefix outside this node's group data path, or an unreadable continuation token", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, the caller is not a member of the group, or the caller may not read the browsed path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_data_paths(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Query(query): Query<DataPathsQuery>,
) -> ServerResult<(StatusCode, Json<DataPathsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let (_, auth_doc) = load_group(&state, group_id).await?;
    if !is_group_member(&auth_doc, auth.user_id) {
        return Err(ServerError::Forbidden);
    }

    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let limit = query.limit.unwrap_or(1_000).clamp(1, 1_000) as usize;

    // Permission paths are node-scoped; only paths under this node's group data
    // root are browsable, so a foreign prefix is rejected outright.
    let group_path = blob_group_permission_path(realm_id, group_id, node_id);
    let remainder = match query.prefix.as_deref().filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => {
            let rest = prefix
                .strip_prefix(group_path.as_str())
                .ok_or(ServerError::BadRequest)?;
            rest.strip_prefix('/').unwrap_or(rest).to_string()
        }
        None => String::new(),
    };

    // A bare segment that names an existing bucket browses into that bucket's
    // root so a returned bucket path round-trips; any other bare segment stays a
    // bucket-name filter.
    let bucket_target = match remainder.split_once('/') {
        Some((bucket, key_prefix)) => Some((bucket.to_string(), key_prefix.to_string())),
        None => {
            if !remainder.is_empty()
                && get_bucket_group(&state, &remainder).await? == Some(group_id)
            {
                Some((remainder.clone(), String::new()))
            } else {
                None
            }
        }
    };

    let response = match bucket_target {
        Some((bucket, key_prefix)) => {
            // Listing inside a bucket requires READ on the bucket, or the prefix
            // being browsed, so path-restricted tokens see only what they may read.
            let listing_path = if key_prefix.is_empty() {
                blob_bucket_permission_path(realm_id, group_id, node_id, &bucket)
            } else {
                blob_object_permission_path(realm_id, group_id, node_id, &bucket, &key_prefix)
            };
            require_data_read(&state, &auth, listing_path).await?;
            list_bucket_objects(
                &state,
                group_id,
                &bucket,
                &key_prefix,
                query.delimiter.as_deref(),
                query.continuation_token.as_deref(),
                limit,
            )
            .await?
        }
        None => {
            // Browsing the bucket level requires READ on the group data root.
            require_data_read(&state, &auth, group_path).await?;
            let name_filter = (!remainder.is_empty()).then_some(remainder.as_str());
            list_group_buckets(
                &state,
                group_id,
                name_filter,
                query.continuation_token.as_deref(),
                limit,
            )
            .await?
        }
    };

    Ok((StatusCode::OK, Json(response)))
}

/// Authorizes READ on a data permission path via the shared CheckPermissions
/// flow, matching the S3 surface: a caller without READ (empty role, DENY, or a
/// path restriction that excludes the path) is forbidden.
async fn require_data_read(
    state: &ServerState,
    auth: &AuthContext,
    path: String,
) -> ServerResult<()> {
    crate::auth::ensure_permission(state, auth, path, Permission::READ).await
}

async fn list_group_buckets(
    state: &ServerState,
    group_id: Ulid,
    name_filter: Option<&str>,
    continuation_token: Option<&str>,
    limit: usize,
) -> ServerResult<DataPathsResponse> {
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    let continuation_token = decode_bucket_token(continuation_token)?;
    let result = drive(
        ListBucketsOperation::new(ListBucketsInput {
            group_id,
            prefix: name_filter.map(str::to_string),
            continuation_token,
            max_buckets: Some(limit),
        }),
        &state.get_ctx(),
    )
    .await
    .and_then(|output| output.transpose())
    .map_err(|err| ServerError::InternalError(err.to_string()))?
    .ok_or_else(|| ServerError::InternalError("bucket listing produced no result".to_string()))?;

    let entries = result
        .buckets
        .into_iter()
        .map(|(bucket, _info)| DataPathEntry {
            permission_path: blob_bucket_permission_path(realm_id, group_id, node_id, &bucket),
            kind: DataPathKind::Folder,
        })
        .collect();
    Ok(DataPathsResponse {
        entries,
        continuation_token: result.continuation_token.map(encode_bucket_token),
    })
}

async fn list_bucket_objects(
    state: &ServerState,
    group_id: Ulid,
    bucket: &str,
    key_prefix: &str,
    delimiter: Option<&str>,
    continuation_token: Option<&str>,
    limit: usize,
) -> ServerResult<DataPathsResponse> {
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();
    // Bucket names are globally unique; refuse to enumerate a bucket owned by
    // another group to avoid leaking its keys under this group's path.
    if get_bucket_group(state, bucket).await? != Some(group_id) {
        return Ok(DataPathsResponse {
            entries: Vec::new(),
            continuation_token: None,
        });
    }

    let continuation_token = decode_object_token(continuation_token)?;
    let result = drive(
        ListObjectsV2Operation::new(ListObjectsV2Input {
            bucket: bucket.to_string(),
            group_id,
            continuation_token,
            max_keys: Some(limit),
            prefix: (!key_prefix.is_empty()).then(|| key_prefix.to_string()),
            delimiter: delimiter.map(str::to_string),
            start_after: None,
        }),
        &state.get_ctx(),
    )
    .await
    .and_then(|output| output.transpose())
    .map_err(|err| ServerError::InternalError(err.to_string()))?
    .ok_or_else(|| ServerError::InternalError("object listing produced no result".to_string()))?;

    let mut entries = Vec::with_capacity(result.objects.len() + result.common_prefixes.len());
    for prefix in result.common_prefixes {
        entries.push(DataPathEntry {
            permission_path: blob_object_permission_path(
                realm_id, group_id, node_id, bucket, &prefix,
            ),
            kind: DataPathKind::Folder,
        });
    }
    for object in result.objects {
        entries.push(DataPathEntry {
            permission_path: blob_object_permission_path(
                realm_id,
                group_id,
                node_id,
                bucket,
                &object.head.key,
            ),
            kind: DataPathKind::Object,
        });
    }
    Ok(DataPathsResponse {
        entries,
        continuation_token: result
            .continuation_token
            .map(encode_object_token)
            .transpose()?,
    })
}

async fn get_bucket_group(state: &ServerState, bucket: &str) -> ServerResult<Option<Ulid>> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    .and_then(|output| output.transpose())
    {
        Ok(Some(info)) => Ok(Some(info.group_id)),
        Ok(None) | Err(GetBucketInfoError::NotFound) => Ok(None),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

fn decode_bucket_token(token: Option<&str>) -> ServerResult<Option<String>> {
    token
        .map(|token| {
            let bytes = STANDARD
                .decode(token)
                .map_err(|_| ServerError::BadRequest)?;
            String::from_utf8(bytes).map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn encode_bucket_token(bucket: String) -> String {
    STANDARD.encode(bucket.as_bytes())
}

fn decode_object_token(
    token: Option<&str>,
) -> ServerResult<Option<ListObjectsV2ContinuationToken>> {
    token
        .map(|token| {
            let bytes = STANDARD
                .decode(token)
                .map_err(|_| ServerError::BadRequest)?;
            ListObjectsV2ContinuationToken::from_bytes(&bytes).map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn encode_object_token(token: ListObjectsV2ContinuationToken) -> ServerResult<String> {
    token
        .to_bytes()
        .map(|bytes| STANDARD.encode(bytes))
        .map_err(|err| ServerError::InternalError(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{
        CreateGroupRequest, DataPathKind, DataPathsQuery, ListGroupsQuery, create_group, get_group,
        get_group_usage, list_data_paths, list_group_members, list_groups,
    };
    use crate::error::{ServerError, ServerResult};
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
        GROUP_KEYSPACE, S3_BUCKET_KEYSPACE, USER_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, AuthContext, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
        BucketInfo, CurrentVersionPointer, Group, GroupAuthorizationDocument, NodeCapabilities,
        RealmAuthorizationDocument, RealmId, Role, User, VersionKey, blob_bucket_permission_path,
        blob_object_permission_path,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::driver::drive;
    use aruna_storage::storage;
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::{Extension, Json};
    use byteview::ByteView;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
        let group_id = Ulid::generate();
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
        let realm_signing_key = generate_signing_key();
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

        // The data permission check reads the realm auth doc; seed an empty one
        // so authority comes solely from group roles.
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        store_bytes(
            &state,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        // Policy loading fails closed without the realm config document.
        store_bytes(
            &state,
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            aruna_core::structs::RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;

        (state, tempdir)
    }

    fn foreign_auth() -> AuthContext {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
        }
    }

    /// A realm whose initial admin is claimed, so the realm admin path grants.
    async fn setup_admin_state() -> (Arc<ServerState>, UserId, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(aruna_tasks::TaskHandle::new()),
            compute_handle: None,
        });
        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();
        let admin = UserId::local(Ulid::generate(), realm_id);
        let actor = Actor {
            node_id,
            user_id: admin,
            realm_id,
        };
        drive(
            aruna_operations::create_realm::CreateRealmOperation::new(
                aruna_operations::create_realm::CreateRealmConfig {
                    actor: actor.clone(),
                    realm_description: "groups".to_string(),
                    oidc_providers: Vec::new(),
                    node_location: None,
                    node_weight: None,
                    node_labels: Default::default(),
                },
            ),
            &driver_ctx,
        )
        .await
        .unwrap();
        drive(
            aruna_operations::claim_initial_realm_admin::ClaimInitialRealmAdminOperation::new(
                aruna_operations::claim_initial_realm_admin::ClaimInitialRealmAdminInput { actor },
            ),
            &driver_ctx,
        )
        .await
        .unwrap();
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::local_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        (state, admin, tempdir)
    }

    async fn update_config(
        state: &ServerState,
        mutate: impl FnOnce(&mut aruna_core::structs::RealmConfigDocument),
    ) {
        let realm_id = state.get_realm_id();
        let mut config = drive(
            aruna_operations::get_realm_config::GetRealmConfigOperation::new(realm_id),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        mutate(&mut config);
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        store_bytes(
            state,
            aruna_core::keyspaces::REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            config.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn new_group(state: &Arc<ServerState>, user_id: UserId, name: &str) -> ServerResult<()> {
        create_group(
            State(state.clone()),
            Extension(Some(member_auth(user_id))),
            Json(CreateGroupRequest {
                name: name.to_string(),
            }),
        )
        .await
        .map(|(status, _)| assert_eq!(status, StatusCode::CREATED))
    }

    #[tokio::test]
    async fn policy_drops_exemption() {
        // A realm deny policy on the admin path must grade the caller as an
        // ordinary user, without breaking capped self-service creation.
        let (state, admin, _tempdir) = setup_admin_state().await;
        let realm_id = state.get_realm_id();
        update_config(&state, |config| {
            config.quota.max_groups_per_user = Some(1);
        })
        .await;

        new_group(&state, admin, "first").await.unwrap();
        new_group(&state, admin, "second").await.unwrap();

        let denied = format!("/{realm_id}/admin/groups");
        update_config(&state, |config| {
            config
                .request_policies
                .push(aruna_core::request_policy::RequestPolicy {
                    policy_id: Ulid::generate(),
                    name: "deny-group-admin".to_string(),
                    kind: aruna_core::request_policy::PolicyKind::Deny,
                    when: None,
                    expression: format!("path == '{denied}'"),
                    enabled: true,
                });
        })
        .await;

        let error = new_group(&state, admin, "third").await.unwrap_err();
        assert!(matches!(error, ServerError::Conflict(_)));

        let member = UserId::local(Ulid::generate(), realm_id);
        new_group(&state, member, "mine").await.unwrap();
    }

    /// Anonymous callers get 401, foreign-realm tokens 403: neither may
    /// enumerate the local group or usage directory.
    #[tokio::test]
    async fn group_directory_requires_realm() {
        let (state, _tempdir) = setup_state().await;
        let group_id = Ulid::generate().to_string();

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
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
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
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
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

    async fn seed_bucket(state: &ServerState, bucket: &str, group_id: Ulid) {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        store_bytes(
            state,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            info.to_bytes().unwrap(),
        )
        .await;
    }

    async fn seed_object(state: &ServerState, bucket: &str, key: &str, owner: UserId, tag: u8) {
        let version_id = Ulid::generate();
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let hash = [tag; 32];
        store_bytes(
            state,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        )
        .await;
        store_bytes(
            state,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
            BlobVersion::materialized(hash, BackendRef::node_default(), created_at, owner, None)
                .to_bytes()
                .unwrap(),
        )
        .await;
        store_bytes(
            state,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new(hash, BackendRef::node_default()).to_bytes(),
            BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/tmp".to_string(),
                storage_bucket: "objects".to_string(),
                backend_path: format!("path/{key}"),
                ulid: Ulid::generate(),
                compressed: false,
                encrypted: false,
                created_by: owner,
                created_at,
                staging: false,
                partial: false,
                blob_size: 42,
                hashes: HashMap::new(),
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
    }

    fn browse(prefix: Option<String>) -> DataPathsQuery {
        DataPathsQuery {
            prefix,
            delimiter: Some("/".to_string()),
            continuation_token: None,
            limit: None,
        }
    }

    #[tokio::test]
    async fn folds_group_buckets() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "alpha", group_id).await;
        seed_bucket(&state, "beta", group_id).await;
        seed_bucket(&state, "foreign", Ulid::generate()).await;

        let (status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(DataPathsQuery::default()),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::OK);
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let mut paths: Vec<_> = body
            .entries
            .iter()
            .map(|entry| {
                assert_eq!(entry.kind, DataPathKind::Folder);
                entry.permission_path.clone()
            })
            .collect();
        paths.sort();
        assert_eq!(
            paths,
            vec![
                blob_bucket_permission_path(realm_id, group_id, node_id, "alpha"),
                blob_bucket_permission_path(realm_id, group_id, node_id, "beta"),
            ]
        );
    }

    #[tokio::test]
    async fn bucket_folds_objects() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "data", group_id).await;
        for (index, key) in ["a.txt", "dir/1", "dir/2", "z.txt"].iter().enumerate() {
            seed_object(&state, "data", key, owner, index as u8 + 1).await;
        }
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let prefix = blob_bucket_permission_path(realm_id, group_id, node_id, "data") + "/";

        let (status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(browse(Some(prefix))),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::OK);
        let folders: Vec<_> = body
            .entries
            .iter()
            .filter(|entry| entry.kind == DataPathKind::Folder)
            .map(|entry| entry.permission_path.clone())
            .collect();
        let mut objects: Vec<_> = body
            .entries
            .iter()
            .filter(|entry| entry.kind == DataPathKind::Object)
            .map(|entry| entry.permission_path.clone())
            .collect();
        objects.sort();

        assert_eq!(
            folders,
            vec![blob_object_permission_path(
                realm_id, group_id, node_id, "data", "dir/"
            )]
        );
        assert_eq!(
            objects,
            vec![
                blob_object_permission_path(realm_id, group_id, node_id, "data", "a.txt"),
                blob_object_permission_path(realm_id, group_id, node_id, "data", "z.txt"),
            ]
        );
        assert!(body.continuation_token.is_none());
    }

    #[tokio::test]
    async fn round_trips_bucket() {
        // A returned bucket folder path must list the bucket's children verbatim.
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "data", group_id).await;
        for (index, key) in ["a.txt", "dir/1"].iter().enumerate() {
            seed_object(&state, "data", key, owner, index as u8 + 1).await;
        }
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();

        let (_status, Json(listing)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(DataPathsQuery::default()),
        )
        .await
        .unwrap();
        let bucket_path = listing.entries[0].permission_path.clone();
        assert_eq!(
            bucket_path,
            blob_bucket_permission_path(realm_id, group_id, node_id, "data")
        );

        let (_status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(browse(Some(bucket_path.clone()))),
        )
        .await
        .unwrap();

        assert!(
            body.entries
                .iter()
                .all(|entry| entry.permission_path != bucket_path)
        );
        let paths: Vec<_> = body
            .entries
            .iter()
            .map(|entry| entry.permission_path.clone())
            .collect();
        assert!(paths.contains(&blob_object_permission_path(
            realm_id, group_id, node_id, "data", "a.txt"
        )));
        assert!(paths.contains(&blob_object_permission_path(
            realm_id, group_id, node_id, "data", "dir/"
        )));
    }

    #[tokio::test]
    async fn paginates_object_pages() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "data", group_id).await;
        for (index, key) in ["a", "b", "c", "d"].iter().enumerate() {
            seed_object(&state, "data", key, owner, index as u8 + 1).await;
        }
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let prefix = blob_bucket_permission_path(realm_id, group_id, node_id, "data") + "/";

        let mut token = None;
        let mut collected = Vec::new();
        let mut pages = 0;
        loop {
            let (_status, Json(body)) = list_data_paths(
                State(state.clone()),
                Extension(Some(member_auth(owner))),
                Path(group_id.to_string()),
                Query(DataPathsQuery {
                    prefix: Some(prefix.clone()),
                    delimiter: Some("/".to_string()),
                    continuation_token: token.take(),
                    limit: Some(2),
                }),
            )
            .await
            .unwrap();
            collected.extend(body.entries.into_iter().map(|entry| entry.permission_path));
            pages += 1;
            assert!(pages <= 5);
            token = body.continuation_token;
            if token.is_none() {
                break;
            }
        }

        collected.sort();
        let expected: Vec<_> = ["a", "b", "c", "d"]
            .iter()
            .map(|key| blob_object_permission_path(realm_id, group_id, node_id, "data", key))
            .collect();
        assert_eq!(collected, expected);
        assert!(pages >= 2);
    }

    #[tokio::test]
    async fn path_matches_helper() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "data", group_id).await;
        seed_object(&state, "data", "reports/q1.csv", owner, 9).await;
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let prefix = blob_bucket_permission_path(realm_id, group_id, node_id, "data") + "/reports/";

        let (_status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(browse(Some(prefix))),
        )
        .await
        .unwrap();

        let object = body
            .entries
            .iter()
            .find(|entry| entry.kind == DataPathKind::Object)
            .unwrap();
        assert_eq!(
            object.permission_path,
            blob_object_permission_path(realm_id, group_id, node_id, "data", "reports/q1.csv")
        );
    }

    #[tokio::test]
    async fn hides_foreign_bucket() {
        // A crafted path naming another group's bucket must not leak its keys.
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        seed_bucket(&state, "secret", Ulid::generate()).await;
        seed_object(&state, "secret", "k", owner, 1).await;
        let realm_id = state.get_realm_id();
        let node_id = state.get_node_id();
        let prefix = blob_bucket_permission_path(realm_id, group_id, node_id, "secret") + "/";

        let (_status, Json(body)) = list_data_paths(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Query(browse(Some(prefix))),
        )
        .await
        .unwrap();

        assert!(body.entries.is_empty());
    }

    #[tokio::test]
    async fn non_member_forbidden() {
        let (state, _tempdir) = setup_state().await;
        let owner = UserId::local(Ulid::generate(), state.get_realm_id());
        let group_id = seed_group(&state, owner).await;
        let outsider = member_auth(UserId::local(Ulid::generate(), state.get_realm_id()));

        let result = list_data_paths(
            State(state),
            Extension(Some(outsider)),
            Path(group_id.to_string()),
            Query(DataPathsQuery::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn empty_role_forbidden() {
        // Membership alone is not enough: a role granting no READ is forbidden.
        let (state, _tempdir) = setup_state().await;
        let realm_id = state.get_realm_id();
        let owner = UserId::local(Ulid::generate(), realm_id);
        let group_id = seed_group(&state, owner).await;
        let limited = UserId::local(Ulid::generate(), realm_id);

        let mut auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        let role_id = Ulid::generate();
        auth_doc.roles.insert(
            role_id,
            Role {
                role_id,
                name: "empty".to_string(),
                permissions: HashMap::new(),
                assigned_users: HashSet::from([limited]),
            },
        );
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: owner,
            realm_id,
        };
        store_bytes(
            &state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(&actor).unwrap(),
        )
        .await;

        let result = list_data_paths(
            State(state),
            Extension(Some(member_auth(limited))),
            Path(group_id.to_string()),
            Query(DataPathsQuery::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn rejects_anonymous_caller() {
        let (state, _tempdir) = setup_state().await;
        let result = list_data_paths(
            State(state),
            Extension(None),
            Path(Ulid::generate().to_string()),
            Query(DataPathsQuery::default()),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));
    }

    #[tokio::test]
    async fn policy_denies_role() {
        // A group deny policy on the admin path blocks role creation with 403
        // and emits no role, even for the group owner.
        let (state, _tempdir) = setup_state().await;
        let realm_id = state.get_realm_id();
        let owner = UserId::local(Ulid::generate(), realm_id);
        store_user(&state, owner, "Owner").await;
        let group_id = Ulid::generate();

        let mut auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        auth_doc.policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "no-writes".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }];
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
            &state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
        store_bytes(
            &state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(&actor).unwrap(),
        )
        .await;

        let result = super::create_group_role(
            State(state.clone()),
            Extension(Some(member_auth(owner))),
            Path(group_id.to_string()),
            Json(super::CreateGroupRoleRequest {
                name: "readers".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/g/{group_id}/data/**"),
                    "read".to_string(),
                )]),
                assigned_users: Vec::new(),
                public: false,
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));

        let value = match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: AUTH_KEYSPACE.to_string(),
                key: ByteView::from(group_id.to_bytes().to_vec()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.unwrap(),
            other => panic!("unexpected read result: {other:?}"),
        };
        let stored = GroupAuthorizationDocument::from_bytes(&value).unwrap();
        assert_eq!(stored.roles.len(), auth_doc.roles.len());
    }
}
