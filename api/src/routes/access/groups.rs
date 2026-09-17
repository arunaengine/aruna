use crate::auth::{
    ValidatedBearer, ensure_permission, permission_granted, require_realm_auth,
    require_unrestricted_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::map_api_error;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::errors::{AuthorizationError, StorageError};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission, Role};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    bucket_permission_path, group_permission_path, object_permission_path,
};
use aruna_core::structs::storage::usage::usage_group_key;
use aruna_core::types::RoleId;
use aruna_operations::device::realm_documents::install_group_docs;
use aruna_operations::driver::drive;
use aruna_operations::forward::routing::is_user_origin;
use aruna_operations::groups::add_member::{AddUserError, AddUserInput, AddUserOperation};
use aruna_operations::groups::add_role::{AddRoleConfig, AddRoleError, AddRoleOperation};
use aruna_operations::groups::create_group::{
    CreateGroupConfig, CreateGroupError, CreateGroupOperation,
};
use aruna_operations::groups::forward::{ForwardGroupError, forward_group_create};
use aruna_operations::groups::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use aruna_operations::groups::list_groups::ListGroupOperation;
use aruna_operations::groups::remove_member::{
    RemoveFromError, RemoveFromInput, RemoveFromOperation,
};
use aruna_operations::groups::remove_role::{
    RemoveGroupConfig, RemoveGroupError, RemoveGroupOperation,
};
use aruna_operations::groups::update_group::{
    UpdateGroupConfig, UpdateGroupError, UpdateGroupOperation, normalize_group_name,
};
use aruna_operations::metadata::api::forwarded_bearer;
use aruna_operations::metadata::stats::count_group_purpose;
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::list::{ListBucketsInput, ListBucketsOperation};
use aruna_operations::s3::object::list::{
    ListBucketInput, ListBucketOperation, ListContinuationToken,
};
use aruna_operations::users::resolve_users::{ResolveUsersInput, ResolveUsersOperation};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{Instrument, Span, field, info_span, trace, warn};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "access/groups", description = "Group management operations"))
)]
pub struct GroupsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(GroupsApiDoc::openapi())
        .routes(routes!(create_group, list_groups))
        .routes(routes!(get_group, update_group))
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
pub struct UpdateGroupRequest {
    pub display_name: String,
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
#[schema(as = AddGroupMemberRequest)]
pub struct AddMemberRequest {
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
#[schema(as = RemoveGroupMemberQuery)]
pub struct RemoveMemberQuery {
    /// Revoke only this role; all roles when omitted.
    #[serde(default)]
    pub role_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = CreateGroupRoleRequest)]
pub struct CreateRoleRequest {
    pub name: String,
    /// Permission path -> "read" | "write" | "deny". Every path must stay
    /// inside the group.
    pub permissions: HashMap<String, String>,
    #[serde(default)]
    pub assigned_users: Vec<String>,
    /// Public roles apply to every principal, including anonymous requests,
    /// by assigning the Everyone principal (the nil user id).
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = GroupMemberRoleResponse)]
pub struct GroupRoleResponse {
    pub role_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupMemberResponse {
    pub user_id: String,
    /// Display name from the user directory; None when the user is unresolvable.
    pub name: Option<String>,
    pub roles: Vec<GroupRoleResponse>,
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
    map_visible_roles(auth, realm_id, true)
}

/// Member lists are only visible to group members; open endpoints get the
/// roles without `assigned_users`.
fn map_visible_roles(
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

fn parse_member_id(user_id: &str) -> ServerResult<UserId> {
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

/// Refuses a group-document edit on a device. Every realm holder rejects an
/// admin envelope a device originated, so applying it locally would answer the
/// caller with success and then diverge; the action journal replaces this.
pub(crate) async fn refuse_group_edit(state: &ServerState) -> ServerResult<()> {
    let device = is_user_origin(&state.get_ctx(), state.get_realm_id(), state.get_node_id())
        .await
        .map_err(map_api_error)?;
    match device {
        true => Err(ServerError::Conflict(
            "group changes are made through the realm".to_string(),
        )),
        false => Ok(()),
    }
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
    .map_err(map_group_error)
}

fn map_group_error(error: GetGroupError) -> ServerError {
    match error {
        GetGroupError::GroupNotFound | GetGroupError::DocNotFound => ServerError::NotFound,
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
    path = "/access/groups",
    tag = "access/groups",
    summary = "Create a group in this realm",
    description = r#"Creates a group in this realm and makes the caller its sole administrator.

**Authentication**: realm bearer token without path restrictions. Creation is self-service up to the
realm's per-user group quota, from which WRITE on the realm group-admin path exempts the caller.

**Behavior**
- The caller becomes the only member of the new group's `admin` role, next to the default `user` and
  `viewer` roles.
- A user node forwards the create to a management or server peer under the caller's own token, which
  is where the quota and the permission are checked.
- The write commits here and reaches the rest of the realm through document sync, so another node
  may not list the group immediately.

**Limits**
- The name is trimmed and must be 1 to 256 bytes.
- Names need not be unique."#,
    request_body(
        content = CreateGroupRequest,
        description = "Display name for the new group. It is stored as given and need not be unique.",
        example = json!({
            "name": "Proteomics Lab"
        })
    ),
    responses(
        (
            status = 201,
            description = "The group as created, with its initial roles; assigned users are listed because the caller is a member",
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
        (status = 400, description = "Malformed request body, or a name that is empty, blank or longer than 256 bytes once trimmed; the name is checked here, so a user node refuses it without forwarding", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or is path-restricted", body = ErrorResponse),
        (status = 409, description = "The caller's group quota is exhausted, or a concurrent create conflicted; the latter is retryable unchanged", body = ErrorResponse),
        (status = 503, description = "No eligible realm peer accepted the create forwarded from a user node; retryable, the response carries Retry-After", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<CreateGroupRequest>,
) -> ServerResult<(StatusCode, Json<CreateGroupResponse>)> {
    let auth = require_unrestricted(auth)?;
    let realm_id = state.get_realm_id();
    let request_span = Span::current();
    request_span.record("group_name", field::display(&request.name));
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }
    // Checked before the forwarding branch so a user node answers the same 400
    // a management node does, instead of a forwarding failure.
    let name = normalize_group_name(&request.name).ok_or_else(|| {
        ServerError::BadRequestReason(CreateGroupError::InvalidDisplayName.to_string())
    })?;

    let ctx = state.get_ctx();
    // A device originates no realm administration, so the create travels to an
    // ingress, which decides quota and permission under the caller's own token.
    if is_user_origin(&ctx, realm_id, state.get_node_id())
        .await
        .map_err(map_api_error)?
    {
        let caller_token = bearer_token.as_ref().ok_or(ServerError::Unauthorized)?;
        let auth_token = forwarded_bearer(Some(caller_token.as_str()))
            .map_err(map_api_error)?
            .ok_or(ServerError::Unauthorized)?;
        let forwarded = forward_group_create(&ctx, realm_id, auth_token, name)
            .await
            .map_err(|err| match err {
                ForwardGroupError::Conflict(reason) => ServerError::Conflict(reason),
                ForwardGroupError::Api(err) => map_api_error(err),
            })?;
        request_span.record("group_id", field::display(forwarded.0.group_id));
        // Cached here so the new group answers on the next read instead of
        // after the device's next realm-document fetch.
        if !install_group_docs(&ctx, &actor_for(&state, &auth), &forwarded.0, &forwarded.1).await {
            warn!(group_id = %forwarded.0.group_id, "Failed to cache a forwarded group locally");
        }
        return Ok((StatusCode::CREATED, Json(forwarded.into())));
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
        let realm_config = drive(GetConfigOperation::new(realm_id), &state.get_ctx())
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?;
        realm_config.quota.max_groups_for(&auth.user_id)
    };

    trace!(
        event = "request.group.create.authorized",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        group_name = %name,
        "Authorized group creation request"
    );

    let create_span = info_span!(
        "group.create",
        "otel.kind" = "internal",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        group_name = %name,
        group_id = field::Empty,
    );
    let result = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            display_name: name,
            owner_cap,
        }),
        &state.get_ctx(),
    )
    .instrument(create_span.clone())
    .await
    .map_err(|err| match err {
        CreateGroupError::GroupLimitReached { limit } => {
            ServerError::Conflict(format!("owned group limit reached ({limit})"))
        }
        CreateGroupError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent group creation conflict; retry".to_string())
        }
        CreateGroupError::InvalidDisplayName => ServerError::BadRequest,
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
    path = "/access/groups",
    tag = "access/groups",
    summary = "List the groups of this realm",
    description = r#"Lists this realm's groups, optionally with their roles and permission paths.

**Authentication**: realm bearer token; no group membership is needed, because every realm member
sees each group's id, realm and display name.

**Behavior**
- This is a node-local read of the replicated group directory, so a group created elsewhere can be
  missing until it arrives here.
- `include=roles` adds each group's roles and their permission paths, but `assigned_users` is
  included only for groups the caller belongs to; elsewhere a role that applies to everyone is
  visible only through its `public` flag."#,
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return; defaults to 100 and is clamped to the range 1-1000"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip from the start of the directory; defaults to 0"),
        ("include" = Option<String>, Query, description = "Comma-separated extras; only `roles` is supported, blank entries are ignored and any other value is rejected")
    ),
    responses(
        (
            status = 200,
            description = "This realm's groups, with member-only fields hidden for groups the caller does not belong to",
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
        (status = 400, description = "The `include` parameter names an unsupported extra", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_groups(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListGroupsQuery>,
) -> ServerResult<(StatusCode, Json<ListGroupsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let include_roles = parse_group_include(query.include.as_deref())?;
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

fn parse_group_include(include: Option<&str>) -> ServerResult<bool> {
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
    groups: Vec<aruna_core::structs::identity::group::Group>,
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
            Some(map_visible_roles(auth_doc, group.realm_id, is_member))
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
    path = "/access/groups/{id}",
    tag = "access/groups",
    summary = "Read one group's directory entry",
    description = r#"Returns one group's directory entry together with its roles.

**Authentication**: realm bearer token; no group membership is needed, because every realm member
may look up any group in the realm.

**Behavior**
- A member receives the full role list including the users assigned to each role; a non-member
  receives the same roles and permission paths with `assigned_users` omitted, and learns only from
  the `public` flag that a role applies to everyone.
- This is a node-local read, so a group whose record or authorization document has not arrived here
  reads as not found."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The group and its roles, with `assigned_users` included only for a caller who is a member",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupInfoResponse>)> {
    Ok((
        StatusCode::OK,
        Json(run_get_group(&state, auth, &group_id).await?),
    ))
}

pub(crate) async fn run_get_group(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<GroupInfoResponse> {
    let auth = require_realm_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    let (group, auth_doc) = load_group(state, group_id).await?;
    let is_member = is_group_member(&auth_doc, auth.user_id);
    Ok(GroupInfoResponse {
        display_name: group.display_name,
        group_id: group.group_id.to_string(),
        realm_id: group.realm_id.to_string(),
        roles: map_visible_roles(auth_doc, group.realm_id, is_member),
    })
}

#[utoipa::path(
    patch,
    path = "/access/groups/{id}",
    tag = "access/groups",
    summary = "Rename a group",
    description = r#"Changes a group's display name and returns its refreshed directory entry.

**Authentication**: realm bearer token without path restrictions, carrying WRITE on the group's
administrative path or on the realm group-administration path.

**Behavior**
- Only the label changes: the group id and every permission path, bucket, dataset and usage counter
  stay as they are.
- The rename commits here and reaches the rest of the realm through document sync, so another node
  may still answer with the previous name for a moment.
- Two concurrent renames converge on the later one; a receiver that cannot order them keeps the name
  it already stored.

**Limits**
- The name is trimmed and must be 1 to 256 characters.
- Names need not be unique."#,
    request_body(
        content = UpdateGroupRequest,
        description = "The new display name. It is stored as given, trimmed, and need not be unique.",
        example = json!({
            "display_name": "Proteomics Lab"
        })
    ),
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The group after the rename, in the same shape as reading it",
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
        (status = 400, description = "The path segment is not a valid ULID, or the name is empty or longer than 256 characters", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller administers neither the group nor the realm's groups", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse),
        (status = 409, description = "This node is a device, a concurrent write conflicted, or the group's bucket cut over; the latter two are retryable unchanged", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn update_group(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<UpdateGroupRequest>,
) -> ServerResult<(StatusCode, Json<GroupInfoResponse>)> {
    let auth = require_unrestricted(auth)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }
    let group_id = parse_group_id(&group_id)?;
    refuse_group_edit(&state).await?;
    // The operation decides again; the boundary refuses early for anyone who is
    // neither a group admin nor a realm groups administrator.
    let group_admin = format!("/{realm_id}/g/{group_id}/admin");
    if !crate::auth::permission_granted(
        &state,
        &auth,
        group_admin,
        aruna_core::structs::identity::auth::Permission::WRITE,
    )
    .await?
    {
        crate::auth::ensure_permission(
            &state,
            &auth,
            format!("/{realm_id}/admin/groups"),
            aruna_core::structs::identity::auth::Permission::WRITE,
        )
        .await?;
    }

    drive(
        UpdateGroupOperation::new(UpdateGroupConfig {
            actor: actor_for(&state, &auth),
            auth_context: auth.clone(),
            group_id,
            display_name: request.display_name,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_rename_error)?;

    Ok((
        StatusCode::OK,
        Json(run_get_group(&state, Some(auth), &group_id.to_string()).await?),
    ))
}

fn map_rename_error(error: UpdateGroupError) -> ServerError {
    match error {
        UpdateGroupError::Unauthorized => ServerError::Forbidden,
        UpdateGroupError::GroupNotFound => ServerError::NotFound,
        UpdateGroupError::InvalidDisplayName | UpdateGroupError::ConversionError(_) => {
            ServerError::BadRequest
        }
        UpdateGroupError::PlacementFenced => {
            ServerError::Conflict("the group moved to a new holder set; retry".to_string())
        }
        UpdateGroupError::StorageError(StorageError::TransactionConflict) => {
            ServerError::Conflict("concurrent group update conflict; retry".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_member_error(error: AddUserError) -> ServerError {
    match error {
        AddUserError::Unauthorized => ServerError::Forbidden,
        AddUserError::InvalidUserId => ServerError::BadRequest,
        AddUserError::RoleNotFound | AddUserError::DocNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_role_error(error: AddRoleError) -> ServerError {
    match error {
        AddRoleError::Unauthorized => ServerError::Forbidden,
        AddRoleError::InvalidPublicRole
        | AddRoleError::InvalidAssignedUser
        | AddRoleError::UnconfinedRolePath
        | AddRoleError::ReservedRoleName => ServerError::BadRequest,
        AddRoleError::GroupNotFound => ServerError::NotFound,
        AddRoleError::CheckPermissionsError(
            AuthorizationError::GroupNotFound | AuthorizationError::DocNotFound,
        ) => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_removal_error(error: RemoveFromError) -> ServerError {
    match error {
        RemoveFromError::Unauthorized => ServerError::Forbidden,
        RemoveFromError::InvalidUserId => ServerError::BadRequest,
        RemoveFromError::RoleNotFound | RemoveFromError::DocNotFound => ServerError::NotFound,
        RemoveFromError::LastAdmin => {
            ServerError::Conflict("the last admin of a group cannot be removed".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[utoipa::path(
    get,
    path = "/access/groups/{id}/usage",
    tag = "access/groups",
    summary = "Read a group's storage usage",
    description = r#"Reports what this node stores for a group next to the realm-wide totals.

**Authentication**: realm bearer token without path restrictions, and membership in the group.
Membership is the only permission check, so a realm administrator who is not a member is refused.

**Behavior**
- The flat counters report what this node stores for the group, while `realm` reports the totals
  aggregated from the usage summaries the realm's nodes publish, which trail recent writes.
- `stored_blobs` and `stored_bytes` are omitted from both: a blob copy is content-addressed and
  shared by every group referencing it, so it is counted per node and per storage backend only, and
  `/system/usage` reports those node-wide figures.
- `dataset_count`, `profile_count` and `process_run_count` are exact lifecycle-live counts when this
  node's metadata subsystem can answer, and all three are omitted together when it cannot.
- Classification reads each live registry record's root-only RO-Crate summary: a root whose `@type`
  contains `http://www.w3.org/ns/dx/prof/Profile` is a Profile, one with exact `conformsTo`
  `https://w3id.org/ro/wfrun/process/0.5` is a Process Run, and every other root is a Dataset.
- `quota` restates the realm quota configuration for this group with a warning flag evaluated
  against the group's realm-wide logical bytes; it is omitted when the realm configuration cannot be
  read.

**Limits**
- The candidate set is bounded by the metadata registry limit and root-summary reads have at most
  eight in flight, so an over-limit or unreadable set is reported as unavailable rather than
  partially counted.
- Full crates are never scanned."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "This node's counters for the group, the realm-wide totals, and the quota status when it is available",
            body = crate::routes::info::UsageResponse,
            example = json!({
                "buckets": 3,
                "objects": 1284,
                "logical_bytes": 91002113024_i64,
                "referenced_bytes": 91002113024_i64,
                "dataset_count": 37,
                "profile_count": 4,
                "process_run_count": 19,
                "realm": {
                    "buckets": 5,
                    "objects": 2048,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller is not a member of the group", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<crate::routes::info::UsageResponse>)> {
    Ok((
        StatusCode::OK,
        Json(run_group_usage(&state, auth, &group_id).await?),
    ))
}

pub(crate) async fn run_group_usage(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<crate::routes::info::UsageResponse> {
    let auth = require_unrestricted_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    let (_, auth_doc) = load_group(state, group_id).await?;
    if !is_group_member(&auth_doc, auth.user_id) {
        return Err(ServerError::Forbidden);
    }

    let local = crate::routes::info::load_usage_counters(state, usage_group_key(group_id)).await?;
    let realm = crate::routes::info::load_realm_usage(
        state,
        aruna_operations::node::usage_stats::RealmUsageScope::Group(group_id),
    )
    .await?;

    // The QuotaGate enforces against the group's realm-wide logical_bytes, so the
    // warning threshold is evaluated against the same counter.
    let group_logical_bytes = realm.logical_bytes;
    let mut response = crate::routes::info::UsageResponse::for_group(local, realm);
    match count_group_purpose(&state.get_ctx(), state.get_realm_id(), group_id).await {
        Ok(Some(counts)) => {
            response.dataset_count = Some(counts.dataset_count);
            response.profile_count = Some(counts.profile_count);
            response.process_run_count = Some(counts.process_run_count);
        }
        Ok(None) => {}
        Err(error) => {
            warn!(group_id = %group_id, error = %error, "metadata purpose counts unavailable for group usage response");
        }
    }
    // Best effort: omit the quota block rather than failing the request if the
    // realm config is unavailable.
    if let Ok(config) = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    {
        response.quota = Some(crate::routes::info::GroupQuotaStatus::resolve(
            &config.quota,
            &group_id,
            group_logical_bytes,
        ));
    }
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/access/groups/{id}/members",
    tag = "access/groups",
    summary = "List the members of a group",
    description = r#"Returns every member of a group with the roles that assign them.

**Authentication**: realm bearer token without path restrictions, and membership in the group; the
member list is never exposed to a non-member.

**Behavior**
- The whole membership comes back in one response, sorted by user id, with each member's roles
  sorted by role name.
- A role that applies to everyone contributes no member here, since the principal standing for
  everyone is not a user.
- Display names are resolved from the realm's user directory as a best effort: a member without a
  resolvable record is returned with a null name instead of failing the listing."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Every member of the group with their roles; a name is null when the user directory cannot resolve it",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller is not a member of the group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_members(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupMembersResponse>)> {
    Ok((
        StatusCode::OK,
        Json(run_group_members(&state, auth, &group_id).await?),
    ))
}

pub(crate) async fn run_group_members(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<GroupMembersResponse> {
    let auth = require_unrestricted_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    let (_, auth_doc) = load_group(state, group_id).await?;
    if !is_group_member(&auth_doc, auth.user_id) {
        return Err(ServerError::Forbidden);
    }

    let mut roles_by_user: HashMap<UserId, Vec<GroupRoleResponse>> = HashMap::new();
    for (role_id, role) in &auth_doc.roles {
        for user in &role.assigned_users {
            if user.is_nil() {
                continue;
            }
            roles_by_user
                .entry(*user)
                .or_default()
                .push(GroupRoleResponse {
                    role_id: role_id.to_string(),
                    name: role.name.clone(),
                });
        }
    }

    let names = resolve_member_names(state, roles_by_user.keys().copied().collect()).await;
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

    Ok(GroupMembersResponse { members })
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
    path = "/access/groups/{id}/members",
    tag = "access/groups",
    summary = "Add a user to a group",
    description = r#"Assigns a user one or more roles in a group.

**Authentication**: realm bearer token without path restrictions, carrying WRITE on the group's
administrative path for the user being added, so authority can be granted per member.

**Behavior**
- When `role_ids` is omitted or empty the user is assigned the group's `user` role, and the request
  is rejected when that role is missing or ambiguous.
- Adding a user who already holds the roles is accepted and changes nothing.
- The change commits here and reaches the rest of the realm through document sync."#,
    request_body(
        content = AddMemberRequest,
        description = "User to add, and optionally the exact roles to assign instead of the default `user` role.",
        example = json!({
            "user_id": "01JUSER02ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
            "role_ids": ["01JROLEUSER00123456789ABCD"]
        })
    ),
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 201,
            description = "The group's complete role list after the assignment, with the users assigned to each",
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
        (status = 400, description = "Malformed ids, a user id standing for everyone, or no default `user` role to fall back on", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or the caller lacks WRITE on the group's administrative path for this member", body = ErrorResponse),
        (status = 404, description = "No such group on this node, or one of the requested roles does not exist", body = ErrorResponse),
        (status = 409, description = "This node is a device; group changes are made through the realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn add_group_member(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<AddMemberRequest>,
) -> ServerResult<(StatusCode, Json<GroupRolesResponse>)> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let user_id = parse_member_id(&request.user_id)?;
    refuse_group_edit(&state).await?;

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
        AddUserOperation::new(AddUserInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id,
            role_ids,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_member_error)?;

    Ok((
        StatusCode::CREATED,
        Json(GroupRolesResponse {
            roles: map_roles(auth_doc, state.get_realm_id()),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/access/groups/{id}/members/{user_id}",
    tag = "access/groups",
    summary = "Remove a group member or revoke one role",
    description = r#"Revokes a user's roles in a group, or only the one named by `role_id`.

**Authentication**: realm bearer token without path restrictions. Removing yourself needs no group
permission; removing anyone else requires WRITE on the group's administrative path for that user.

**Behavior**
- Without `role_id` the user loses every role in the group; with it only that one assignment is
  revoked and the rest are kept.
- The change commits here and reaches the rest of the realm through document sync.

**Limits**
- A group must keep at least one administrator, so a request that would strip the last one is
  refused."#,
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("user_id" = String, Path, description = "Member to remove, in the form `<ulid>@<realm>`"),
        ("role_id" = Option<String>, Query, description = "Revoke only this role, given as a 26-character ULID; when omitted every role of the user in this group is revoked")
    ),
    responses(
        (status = 204, description = "The membership or role assignment is gone"),
        (status = 400, description = "Malformed group, user or role id, or a user id standing for everyone", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or the caller lacks WRITE on the group's administrative path for this member", body = ErrorResponse),
        (status = 409, description = "The removal would leave the group without an administrator, or this node is a device where group changes are made through the realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn remove_group_member(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, user_id)): Path<(String, String)>,
    Query(query): Query<RemoveMemberQuery>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let user_id = parse_member_id(&user_id)?;
    let role_ids = query
        .role_id
        .as_deref()
        .map(|role_id| parse_role_id(role_id).map(|role_id| HashSet::from([role_id])))
        .transpose()?;
    refuse_group_edit(&state).await?;

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
        RemoveFromOperation::new(RemoveFromInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id,
            role_ids,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_removal_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/access/groups/{id}/leave",
    tag = "access/groups",
    summary = "Leave a group",
    description = r#"Drops every role the calling user holds in the group.

**Authentication**: realm bearer token without path restrictions. No group permission is required,
because the caller is the only user affected.

**Behavior**
- Leaving a group the caller does not belong to changes nothing.
- The change commits here and reaches the rest of the realm through document sync.

**Limits**
- A group must keep at least one administrator, so the last one cannot leave and must hand the role
  over first."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (status = 204, description = "The caller no longer holds any role in the group"),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted", body = ErrorResponse),
        (status = 409, description = "The caller is the group's last administrator, or this node is a device where group changes are made through the realm", body = ErrorResponse)
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
    refuse_group_edit(&state).await?;

    drive(
        RemoveFromOperation::new(RemoveFromInput {
            actor: actor_for(&state, &auth),
            group_id,
            user_id: auth.user_id,
            role_ids: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_removal_error)?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/access/groups/{id}/roles",
    tag = "access/groups",
    summary = "Create a role in a group",
    description = r#"Creates a role in a group from permission paths confined to that group.

**Authentication**: realm bearer token without path restrictions, carrying WRITE on the group's
administrative path.

**Behavior**
- Each permission path is granted as `READ`, `WRITE` or `DENY`, accepted case-insensitively and
  reported capitalised.
- The role commits here and reaches the rest of the realm through document sync.

**Limits**
- The name is trimmed, must not be empty, and must not be `admin` or `user`, which are reserved for
  the built-in roles.
- Every permission path must lie inside the group's own path, so a group administrator cannot mint
  authority over anything else.
- A public role applies to every principal including anonymous callers, so it may only carry `READ`
  grants."#,
    request_body(
        content = CreateRoleRequest,
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
        (status = 400, description = "Reserved or empty name, an unknown grant value, a permission path outside the group, a malformed assigned user, or a public role asking for more than `READ`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or the caller lacks WRITE on the group's administrative path", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse),
        (status = 409, description = "This node is a device; group changes are made through the realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_group_role(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateRoleRequest>,
) -> ServerResult<(StatusCode, Json<RoleResponse>)> {
    let auth = require_unrestricted(auth)?;
    let group_id = parse_group_id(&group_id)?;
    let realm_id = state.get_realm_id();
    refuse_group_edit(&state).await?;

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
        .map(|user_id| parse_member_id(user_id))
        .collect::<ServerResult<HashSet<UserId>>>()?;
    if request.public {
        assigned_users.insert(UserId::nil(realm_id));
    }

    let role_id = Ulid::generate();
    let (_, auth_doc) = drive(
        AddRoleOperation::new(AddRoleConfig {
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
    .map_err(map_role_error)?;

    let role = map_roles(auth_doc, realm_id)
        .into_iter()
        .find(|role| role.role_id == role_id.to_string())
        .ok_or_else(|| ServerError::InternalError("created role missing".to_string()))?;

    Ok((StatusCode::CREATED, Json(role)))
}

#[utoipa::path(
    delete,
    path = "/access/groups/{id}/roles/{role_id}",
    tag = "access/groups",
    summary = "Delete a role from a group",
    description = r#"Deletes a role from a group and revokes it from everyone holding it.

**Authentication**: realm bearer token without path restrictions, carrying WRITE on the group's
administrative path.

**Behavior**
- Deletion revokes the role from every user holding it, which can leave a user with no role in the
  group at all.
- The change commits here and reaches the rest of the realm through document sync.

**Limits**
- The built-in `admin` role is permanent, so a group never loses its administrative path."#,
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("role_id" = String, Path, description = "Role to delete, as a 26-character ULID")
    ),
    responses(
        (status = 204, description = "The role and all of its assignments are gone"),
        (status = 400, description = "Malformed group or role id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or the caller lacks WRITE on the group's administrative path", body = ErrorResponse),
        (status = 404, description = "No such role in this group, or the group is not present on this node", body = ErrorResponse),
        (status = 409, description = "The built-in `admin` role cannot be deleted, or this node is a device where group changes are made through the realm", body = ErrorResponse)
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
    refuse_group_edit(&state).await?;

    ensure_permission(
        &state,
        &auth,
        format!("/{}/g/{}/admin", state.get_realm_id(), group_id),
        Permission::WRITE,
    )
    .await?;

    drive(
        RemoveGroupOperation::new(RemoveGroupConfig {
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
        RemoveGroupError::Unauthorized => ServerError::Forbidden,
        RemoveGroupError::RoleNotFound | RemoveGroupError::DocNotFound => ServerError::NotFound,
        RemoveGroupError::AdminRoleUndeletable => {
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
    path = "/access/groups/{id}/data/paths",
    tag = "access/groups",
    summary = "Browse the data permission paths of a group",
    description = r#"Returns one page of the data permission paths a group's role grants are written against.

**Authentication**: realm bearer token and membership in the group. Every page is additionally
authorized as a data read, needing READ on the group's data root at the bucket level and READ on the
bucket or listed prefix inside one, so a path-restricted token sees only what it may read.

**Behavior**
- Entries are folders ending at the delimiter and objects as leaves, scoped to this node, so a
  prefix belonging to another node is rejected.
- Bucket names are globally unique: naming a bucket owned by another group returns an empty page
  instead of an error.
- Paging is forward-only through an opaque token that must be echoed back unchanged; a response
  without one is the last page."#,
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
            description = "One page of data permission paths on this node, with a continuation token when more entries remain",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, the caller is not a member of the group, or the caller lacks READ on the browsed path", body = ErrorResponse)
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
    let group_path = group_permission_path(realm_id, group_id, node_id);
    let remainder = match query.prefix.as_deref().filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => {
            let rest = prefix
                .strip_prefix(group_path.as_str())
                .ok_or(ServerError::BadRequest)?;
            rest.strip_prefix('/').unwrap_or(rest).to_string()
        }
        None => String::new(),
    };

    // An existing bare bucket browses its root; other bare segments remain name filters.
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
                bucket_permission_path(realm_id, group_id, node_id, &bucket)
            } else {
                object_permission_path(realm_id, group_id, node_id, &bucket, &key_prefix)
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
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let entries = result
        .buckets
        .into_iter()
        .map(|(bucket, _info)| DataPathEntry {
            permission_path: bucket_permission_path(realm_id, group_id, node_id, &bucket),
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
        ListBucketOperation::new(ListBucketInput {
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
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let mut entries = Vec::with_capacity(result.objects.len() + result.common_prefixes.len());
    for prefix in result.common_prefixes {
        entries.push(DataPathEntry {
            permission_path: object_permission_path(realm_id, group_id, node_id, bucket, &prefix),
            kind: DataPathKind::Folder,
        });
    }
    for object in result.objects {
        entries.push(DataPathEntry {
            permission_path: object_permission_path(
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

pub(crate) async fn get_bucket_group(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<Option<Ulid>> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => Ok(Some(info.group_id)),
        Err(GetBucketError::NotFound) => Ok(None),
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

fn decode_object_token(token: Option<&str>) -> ServerResult<Option<ListContinuationToken>> {
    token
        .map(|token| {
            let bytes = STANDARD
                .decode(token)
                .map_err(|_| ServerError::BadRequest)?;
            ListContinuationToken::from_bytes(&bytes).map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn encode_object_token(token: ListContinuationToken) -> ServerResult<String> {
    token
        .to_bytes()
        .map(|bytes| STANDARD.encode(bytes))
        .map_err(|err| ServerError::InternalError(err.to_string()))
}

#[cfg(test)]
#[path = "groups_tests.rs"]
mod tests;
