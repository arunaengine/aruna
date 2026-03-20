use crate::auth::auth_middleware;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::onboarding;
use crate::server_state::ServerState;
use aruna_core::NodeId;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, PathRestriction, Permission,
    UserIdentity,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::replication::outgoing_bao::OutgoingBaoOperation;
use aruna_operations::s3::create_user_access::{
    CreateUserAccessConfig, CreateUserAccessOperation, DEFAULT_CREDENTIAL_TTL,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::revoke_user_access::RevokeUserAccessError;
use aruna_operations::s3::revoke_user_access::RevokeUserAccessOperation;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use ulid::Ulid;
use utoipa::ToSchema;

/// Build the group routes.
pub fn rest_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route(
            "/onboarding/bootstrap",
            post(onboarding::bootstrap_onboarding),
        )
        .route("/admin/onboarding/secrets", post(onboarding::create_onboarding_secret))
        .route("/admin/onboarding/secrets", get(onboarding::list_onboarding_secrets))
        .route(
            "/admin/onboarding/secrets/{id}",
            delete(onboarding::revoke_onboarding_secret),
        )
        .route("/users/me/credentials", post(create_s3_credentials))
        .route(
            "/users/me/credentials/{access_key_id}",
            delete(revoke_s3_credentials),
        )
        .route("/users/credentials", post(create_s3_credentials))
        .route("/blobs/replicate", post(replicate_blob))
        .route("/groups/{id}", get(get_group))
        .route("/groups", post(create_group))
        .route("/groups", get(list_groups))
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .with_state(state)
}

/// Request to create S3 credentials
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3PathRestriction {
    pub pattern: String,
    pub permission: String,
}

/// Request to create S3 credentials
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsRequest {
    /// S3 credentials are group specific
    pub group_id: String,
    /// Optional credential TTL in seconds
    pub expires_in_seconds: Option<u64>,
    /// Optional path restrictions relative to the group data root or as absolute paths
    pub path_restrictions: Option<Vec<CreateS3PathRestriction>>,
}

/// Request to create S3 credentials
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsResponse {
    /// S3 credentials
    pub access_key_id: String,
    pub access_secret: String,
}

/// Request to replicate a blob.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplicateBlobRequest {
    /// Hex encoded Blake3 hash of the blob.
    pub hash: String,
    /// Recipient node of the replication.
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplicateBlobResponse {
    pub success: bool,
}

/// Request to create a new group.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateGroupRequest {
    /// Human-readable name for the group.
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
        CreateGroupResponse {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth),
        }
    }
}

/// Replicate a blob.
///
/// POST /api/v1/blobs/replicate
///
#[utoipa::path(
    post,
    path = "/blobs/replicate",
    tag = "blobs",
    request_body = ReplicateBlobRequest,
    responses(
        (status = 201, description = "Blob replicated successfully", body = ReplicateBlobResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden - cannot replicate blobs in foreign realms", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn replicate_blob(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ReplicateBlobRequest>,
) -> ServerResult<(StatusCode, Json<ReplicateBlobResponse>)> {
    let _auth = auth.ok_or(ServerError::Unauthorized)?;

    // Evaluate request input
    let node_id = NodeId::from_str(&request.node_id).map_err(|_e| ServerError::BadRequest)?;
    let hash = blake3::Hash::from_hex(&request.hash).map_err(|_e| ServerError::BadRequest)?;
    let op = OutgoingBaoOperation::new(node_id, *hash.as_bytes());

    // Execute operation
    let result = drive(op, &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let response = ReplicateBlobResponse {
        success: result.is_ok(),
    };

    Ok((StatusCode::OK, response.into()))
}

/// Create S3 credentials.
///
/// POST /api/v1/users/me/credentials
///
#[utoipa::path(
    post,
    path = "/users/me/credentials",
    tag = "users",
    request_body = CreateS3CredentialsRequest,
    responses(
        (status = 201, description = "Credentials created successfully", body = CreateS3CredentialsResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden - cannot create credentials in foreign realms", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<CreateS3CredentialsRequest>,
) -> ServerResult<(StatusCode, Json<CreateS3CredentialsResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    let node_id = state.get_node_id();

    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let user_identity = UserIdentity {
        user_id: auth.user_id,
        realm_key: auth.realm_id.clone(),
    };

    // Evaluate request input
    let group_id = Ulid::from_str(&request.group_id).map_err(|_e| ServerError::BadRequest)?;
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!("/{realm_id}/g/{group_id}/data/{}", state.get_node_id()),
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

    let expires_in_seconds = request.expires_in_seconds;
    let requested_restrictions = request.path_restrictions.clone();
    let path_restrictions =
        build_credential_restrictions(&auth, &state, group_id, requested_restrictions).await?;
    let expiry = credential_expiry(SystemTime::now(), expires_in_seconds)?;

    let op = CreateUserAccessOperation::new(CreateUserAccessConfig {
        user_identity,
        group_id,
        expiry,
        path_restrictions,
        issued_by: *node_id.as_bytes(),
    });

    // Execute operation
    let result = drive(op, &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

    match result {
        Ok((access_key_id, access_secret)) => {
            let response = CreateS3CredentialsResponse {
                access_key_id,
                access_secret: access_secret.secret,
            };
            Ok((StatusCode::CREATED, response.into()))
        }
        Err(err) => {
            //TODO: Differentiate return type based on error
            Err(ServerError::InternalError(err.to_string()))
        }
    }
}

#[utoipa::path(
    delete,
    path = "/users/me/credentials/{access_key_id}",
    tag = "users",
    params(("access_key_id" = String, Path, description = "S3 access key to revoke")),
    responses(
        (status = 204, description = "Credential revoked successfully"),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Credential not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_s3_credentials(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;

    let credential = match drive(
        GetUserAccessOperation::new(access_key_id.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(credential))) => credential,
        Ok(None)
        | Ok(Some(Err(GetUserAccessError::NotFound)))
        | Err(GetUserAccessError::NotFound) => return Err(ServerError::NotFound),
        Ok(Some(Err(err))) | Err(err) => return Err(ServerError::InternalError(err.to_string())),
    };

    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: format!(
                "/{}/g/{}/data/{}",
                state.get_realm_id(),
                credential.group_id,
                state.get_node_id()
            ),
            required_permission: Permission::WRITE,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    if !allowed {
        return Err(ServerError::Forbidden);
    }

    match drive(
        RevokeUserAccessOperation::new(access_key_id),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(_))) => {}
        Ok(None)
        | Ok(Some(Err(RevokeUserAccessError::NotFound)))
        | Err(RevokeUserAccessError::NotFound) => {
            return Err(ServerError::NotFound);
        }
        Ok(Some(Err(err))) | Err(err) => return Err(ServerError::InternalError(err.to_string())),
    }

    Ok(StatusCode::NO_CONTENT)
}

fn credential_expiry(now: SystemTime, expires_in_seconds: Option<u64>) -> ServerResult<SystemTime> {
    const MIN_TTL: u64 = 60;
    const MAX_TTL: u64 = DEFAULT_CREDENTIAL_TTL.as_secs();

    let ttl = expires_in_seconds.unwrap_or(MAX_TTL);
    if !(MIN_TTL..=MAX_TTL).contains(&ttl) {
        return Err(ServerError::BadRequest);
    }

    now.checked_add(Duration::from_secs(ttl))
        .ok_or(ServerError::BadRequest)
}

async fn build_credential_restrictions(
    auth: &AuthContext,
    state: &ServerState,
    group_id: Ulid,
    requested_restrictions: Option<Vec<CreateS3PathRestriction>>,
) -> ServerResult<Option<Vec<PathRestriction>>> {
    let Some(requested_restrictions) = requested_restrictions else {
        return Ok(None);
    };

    let group_root = format!(
        "/{}/g/{}/data/{}",
        state.get_realm_id(),
        group_id,
        state.get_node_id()
    );
    let mut restrictions = Vec::with_capacity(requested_restrictions.len());
    for restriction in requested_restrictions {
        let permission = parse_permission(&restriction.permission)?;
        let pattern = if restriction.pattern.starts_with('/') {
            restriction.pattern
        } else if restriction.pattern.is_empty() {
            group_root.clone()
        } else {
            format!(
                "{group_root}/{}",
                restriction.pattern.trim_start_matches('/')
            )
        };

        if !pattern.starts_with(&group_root) {
            return Err(ServerError::Forbidden);
        }

        restrictions.push(PathRestriction {
            pattern,
            permission,
        });
    }

    for restriction in &restrictions {
        let allowed = drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path: restriction.pattern.clone(),
                required_permission: restriction.permission.clone(),
            }),
            &state.get_ctx(),
        )
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;

        if !allowed {
            return Err(ServerError::Forbidden);
        }
    }

    Ok(Some(restrictions))
}

fn parse_permission(permission: &str) -> ServerResult<Permission> {
    match permission.to_ascii_uppercase().as_str() {
        "READ" => Ok(Permission::READ),
        "WRITE" => Ok(Permission::WRITE),
        "DENY" => Ok(Permission::DENY),
        _ => Err(ServerError::BadRequest),
    }
}

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
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    if !allowed {
        return Err(ServerError::Forbidden);
    }

    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id,
    };

    let config = CreateGroupConfig {
        actor,
        display_name: request.name,
    };
    let result = drive(CreateGroupOperation::new(config), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let response: CreateGroupResponse = result.into();

    Ok((StatusCode::CREATED, response.into()))
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
    let _auth = auth.ok_or(ServerError::Unauthorized)?;

    let limit = pagination.limit_or(100).clamp(1, 1_000);
    let offset = pagination.offset_or(0);

    let result = drive(
        ListGroupOperation::with_pagination(limit as usize, offset as usize),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let response = ListGroupsResponse {
        groups: result
            .iter()
            .map(|g| ApiGroup {
                display_name: g.display_name.clone(),
                group_id: g.group_id.to_string(),
                realm_id: g.realm_id.to_string(),
            })
            .collect(),
    };

    Ok((StatusCode::OK, response.into()))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupInfoResponse {
    pub display_name: String,
    pub group_id: String,
    pub realm_id: String,
    pub roles: Vec<RoleResponse>,
}

impl From<(Group, GroupAuthorizationDocument)> for GroupInfoResponse {
    fn from((group, auth): (Group, GroupAuthorizationDocument)) -> Self {
        GroupInfoResponse {
            display_name: group.display_name,
            group_id: group.group_id.to_string(),
            realm_id: group.realm_id.to_string(),
            roles: map_roles(auth),
        }
    }
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
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 400, description = "Invalid group ID", body = ErrorResponse),
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

    let group_id = Ulid::from_string(&group_id).map_err(|_e| ServerError::BadRequest)?;
    let config = GetGroupConfig { group_id };
    let result = drive(GetGroupOperation::new(config), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let response: GroupInfoResponse = result.into();

    Ok((StatusCode::OK, response.into()))
}
