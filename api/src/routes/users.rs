use crate::auth::{OidcIdentity, ValidatedArunaBearerTokenCarrier, bearer_token};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, RealmAuthorizationDocument, Role, User,
};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::get_oidc_user::{GetOidcUserInput, GetOidcUserOperation};
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::list_users::{ListUsersInput, ListUsersOperation};
use aruna_operations::read_realm_authorization::{
    ReadRealmAuthorizationError, ReadRealmAuthorizationOperation,
};
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use aruna_operations::routing::dispatch::dispatch_holder_call;
use aruna_operations::routing::protocol::{ProxiedCall, ProxiedReply, UserCall, UserReply};
use aruna_operations::search_users::{SearchUsersInput, SearchUsersOperation};
use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "users", description = "User operations")),
    paths(
        register_user,
        get_token,
        get_user_info,
        patch_user_info,
        list_users,
        search_users,
        get_user,
        update_user,
    )
)]
pub struct UsersApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/users/register", post(register_user))
        .route("/users/token", get(get_token))
        .route("/users/info", get(get_user_info).patch(patch_user_info))
        .route("/users", get(list_users))
        .route("/users/search", get(search_users))
        .route("/users/{id}", get(get_user).patch(update_user))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserRequest {
    pub onboarding_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserResponse {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetTokenResponse {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetUserResponse {
    pub user_id: String,
    pub name: String,
    pub subject_ids: Vec<String>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListUsersQuery {
    pub limit: Option<usize>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListUsersResponse {
    pub users: Vec<GetUserResponse>,
    pub next_start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchUsersQuery {
    pub q: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchUserResult {
    pub user_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchUsersResponse {
    pub users: Vec<SearchUserResult>,
    pub next_start_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserInfoRoleResponse {
    pub role_id: String,
    pub name: String,
    pub permissions: HashMap<String, String>,
    pub assigned_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserInfoRealmResponse {
    pub realm_id: String,
    pub roles: Vec<UserInfoRoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserInfoGroupResponse {
    pub group_id: String,
    pub display_name: String,
    pub roles: Vec<UserInfoRoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserInfoPreferencesResponse {
    pub preferred_profile_path: Option<String>,
    pub favourite_metadata_ids: Vec<String>,
    pub theme: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetUserInfoResponse {
    pub user: GetUserResponse,
    pub realm: UserInfoRealmResponse,
    pub groups: Vec<UserInfoGroupResponse>,
    pub preferences: UserInfoPreferencesResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateUserRequest {
    pub name: Option<String>,
    #[serde(default)]
    pub set_attributes: HashMap<String, String>,
    #[serde(default)]
    pub remove_attributes: Vec<String>,
}

pub type PatchUserInfoRequest = UpdateUserRequest;

const DEFAULT_LIST_USERS_LIMIT: usize = 100;
const MAX_LIST_USERS_LIMIT: usize = 1_000;
const MIN_SEARCH_QUERY_CHARS: usize = 2;
const MAX_SEARCH_USERS_LIMIT: usize = 20;

impl From<User> for GetUserResponse {
    fn from(value: User) -> Self {
        GetUserResponse {
            name: value.name,
            user_id: value.user_id.to_string(),
            subject_ids: value.subject_ids,
            attributes: value.attributes,
        }
    }
}

fn map_user_info_role(role_id: Ulid, role: Role) -> UserInfoRoleResponse {
    UserInfoRoleResponse {
        role_id: role_id.to_string(),
        name: role.name,
        permissions: role
            .permissions
            .iter()
            .map(|(path, permission)| (path.clone(), permission.to_string()))
            .collect(),
        assigned_users: role
            .assigned_users
            .iter()
            .map(|user| user.to_string())
            .collect(),
    }
}

fn user_preferences_from_attributes(
    attributes: &HashMap<String, String>,
) -> UserInfoPreferencesResponse {
    UserInfoPreferencesResponse {
        preferred_profile_path: attributes.get("ui.preferred_profile_path").cloned(),
        favourite_metadata_ids: attributes
            .get("ui.favourite_metadata_ids")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        theme: attributes.get("ui.theme").cloned(),
    }
}

impl From<User> for RegisterUserResponse {
    fn from(value: User) -> Self {
        RegisterUserResponse {
            name: value.name,
            id: value.user_id.to_string(),
        }
    }
}

fn now_timestamp() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn map_consume_onboarding_error(error: ConsumeOnboardingSecretError) -> ServerError {
    match error {
        ConsumeOnboardingSecretError::NotFound
        | ConsumeOnboardingSecretError::Expired
        | ConsumeOnboardingSecretError::AlreadyClaimed
        | ConsumeOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_inspect_onboarding_error(error: InspectOnboardingSecretError) -> ServerError {
    match error {
        InspectOnboardingSecretError::NotFound
        | InspectOnboardingSecretError::Expired
        | InspectOnboardingSecretError::AlreadyClaimed
        | InspectOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

const USER_TOKEN_EXPIRY_SECONDS: u64 = 24 * 60 * 60;

async fn issue_user_token(
    state: &Arc<ServerState>,
    user_id: UserId,
    expiry: Option<u64>,
) -> ServerResult<String> {
    drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: now_timestamp(),
            expiry,
            user_id,
            realm_id: state.get_realm_id(),
            node_capabilities: state.node_capabilities().clone(),
        })
        .map_err(|err| ServerError::InternalError(err.to_string()))?,
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))
}

fn bearer_str(bearer: &Option<ValidatedArunaBearerTokenCarrier>) -> Option<&str> {
    bearer
        .as_ref()
        .map(ValidatedArunaBearerTokenCarrier::as_str)
}

/// Routes a user-scoped call to a current holder of the user's subject shard.
/// The holder re-derives the acting identity from the forwarded bearer.
async fn route_user_call(
    state: &ServerState,
    bearer: &Option<ValidatedArunaBearerTokenCarrier>,
    call: UserCall,
) -> ServerResult<UserReply> {
    let reply = dispatch_holder_call(
        state.get_ctx().as_ref(),
        state.get_node_id(),
        bearer_str(bearer),
        ProxiedCall::User(call),
    )
    .await?;
    match reply {
        ProxiedReply::User(user_reply) => Ok(*user_reply),
        ProxiedReply::Group(_) | ProxiedReply::Metadata(_) => Err(ServerError::InternalError(
            "holder returned a non-user reply".to_string(),
        )),
    }
}

fn expect_user(reply: UserReply) -> ServerResult<User> {
    match reply {
        UserReply::User(user) => Ok(user),
        UserReply::TokenSubjectEnsured => Err(ServerError::InternalError(
            "holder returned an unexpected user reply".to_string(),
        )),
    }
}

async fn ensure_canonical_user_token_subject(
    state: &ServerState,
    bearer: &Option<ValidatedArunaBearerTokenCarrier>,
    user_id: UserId,
) -> ServerResult<()> {
    match route_user_call(
        state,
        bearer,
        UserCall::EnsureCanonicalTokenSubject { user_id },
    )
    .await?
    {
        UserReply::TokenSubjectEnsured => Ok(()),
        UserReply::User(_) => Err(ServerError::InternalError(
            "holder returned an unexpected user reply".to_string(),
        )),
    }
}

async fn read_current_user(
    state: &ServerState,
    bearer: &Option<ValidatedArunaBearerTokenCarrier>,
    user_id: UserId,
) -> ServerResult<User> {
    let reply = route_user_call(state, bearer, UserCall::ReadDocument { user_id }).await?;
    expect_user(reply)
}

async fn read_realm_authorization(
    state: &ServerState,
) -> ServerResult<Option<RealmAuthorizationDocument>> {
    drive(
        ReadRealmAuthorizationOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_read_realm_authorization_error)
}

fn map_read_realm_authorization_error(error: ReadRealmAuthorizationError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn collect_user_realm_roles(
    auth_doc: Option<RealmAuthorizationDocument>,
    user_id: UserId,
) -> Vec<UserInfoRoleResponse> {
    auth_doc
        .into_iter()
        .flat_map(|document| document.roles)
        .filter(|(_, role)| role.assigned_users.contains(&user_id))
        .map(|(role_id, role)| map_user_info_role(role_id, role))
        .collect()
}

fn collect_assigned_group_roles(
    auth_doc: GroupAuthorizationDocument,
    user_id: UserId,
) -> Vec<UserInfoRoleResponse> {
    auth_doc
        .roles
        .into_iter()
        .filter(|(_, role)| role.assigned_users.contains(&user_id))
        .map(|(role_id, role)| map_user_info_role(role_id, role))
        .collect()
}

async fn collect_user_group_memberships(
    state: &ServerState,
    user_id: UserId,
) -> ServerResult<Vec<UserInfoGroupResponse>> {
    let groups = drive(ListGroupOperation::new(), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let mut memberships = Vec::new();
    for Group { group_id, .. } in groups {
        let (group, auth_doc) = drive(
            GetGroupOperation::new(GetGroupConfig { group_id }),
            &state.get_ctx(),
        )
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
        let roles = collect_assigned_group_roles(auth_doc, user_id);
        if roles.is_empty() {
            continue;
        }
        memberships.push(UserInfoGroupResponse {
            group_id: group.group_id.to_string(),
            display_name: group.display_name,
            roles,
        });
    }
    Ok(memberships)
}

async fn build_user_info_response(
    state: &ServerState,
    bearer: &Option<ValidatedArunaBearerTokenCarrier>,
    auth: AuthContext,
) -> ServerResult<GetUserInfoResponse> {
    if auth.realm_id != state.get_realm_id() || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let user = read_current_user(state, bearer, auth.user_id).await?;
    let preferences = user_preferences_from_attributes(&user.attributes);
    let realm_roles =
        collect_user_realm_roles(read_realm_authorization(state).await?, auth.user_id);
    let groups = collect_user_group_memberships(state, auth.user_id).await?;

    Ok(GetUserInfoResponse {
        user: user.into(),
        realm: UserInfoRealmResponse {
            realm_id: state.get_realm_id().to_string(),
            roles: realm_roles,
        },
        groups,
        preferences,
    })
}

async fn try_claim_initial_admin(state: &Arc<ServerState>, user_id: UserId) {
    let auth_context = AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
    };
    if let Err(error) = state.claim_initial_realm_admin(&auth_context).await {
        error!(error = %error, "Failed to claim initial realm admin after user registration");
    }
}

async fn validate_oidc_token(
    state: &Arc<ServerState>,
    token: &str,
) -> Result<OidcIdentity, ServerError> {
    let validator = state
        .oidc_validator()
        .map_err(|_| ServerError::Unauthorized)?;
    let selector = validator
        .token_selector(token)
        .map_err(|_| ServerError::Unauthorized)?;
    let provider = state
        .get_oidc_provider_by_token(&selector)
        .await
        .map_err(|_| ServerError::Unauthorized)?;
    let oidc_identity = validator
        .validate(&provider, token)
        .await
        .map_err(|_| ServerError::Unauthorized)?;
    Ok(oidc_identity)
}

async fn register_admin(
    state: &Arc<ServerState>,
    onboarding_secret: String,
    oidc_identity: OidcIdentity,
    user_id: UserId,
    name: String,
) -> Result<User, ServerError> {
    let onboarding_secret =
        OnboardingSecret::decode(&onboarding_secret).map_err(|_| ServerError::Unauthorized)?;
    let secret_hash = onboarding_secret.secret_hash();
    let inspected = drive(
        InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: secret_hash.clone(),
            node_id: user_id.to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_inspect_onboarding_error)?;
    if inspected.mode != OnboardingMode::Local {
        return Err(ServerError::Forbidden);
    }

    drive(
        ConsumeOnboardingSecretOperation::new(ConsumeOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash,
            node_id: user_id.to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_consume_onboarding_error)?;

    let user = drive(
        RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id,
                realm_id: state.get_realm_id(),
            },
            issuer: oidc_identity.issuer,
            subject_id: oidc_identity.subject_id,
            name,
            user_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    try_claim_initial_admin(state, user.user_id).await;
    Ok(user)
}

#[utoipa::path(
    post,
    path = "/users/register",
    tag = "users",
    request_body = RegisterUserRequest,
    responses(
        (status = 201, description = "User registered", body = RegisterUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    )
)]
async fn register_user(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RegisterUserRequest>,
) -> ServerResult<(StatusCode, Json<RegisterUserResponse>)> {
    let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
    let oidc_identity = validate_oidc_token(&state, token).await?;

    let user_id = UserId::local(Ulid::new(), state.get_realm_id());
    let name = oidc_identity
        .display_name
        .clone()
        .unwrap_or(user_id.to_string());

    let user = match request.onboarding_secret {
        Some(onboarding_secret) => {
            register_admin(&state, onboarding_secret, oidc_identity, user_id, name).await?
        }
        None => {
            let realm_id = state.get_realm_id();
            drive(
                RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
                    actor: Actor {
                        node_id: state.get_node_id(),
                        user_id,
                        realm_id,
                    },
                    issuer: oidc_identity.issuer,
                    subject_id: oidc_identity.subject_id,
                    name,
                    user_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?
        }
    };
    Ok((
        StatusCode::CREATED,
        Json(RegisterUserResponse {
            name: user.name,
            id: user.user_id.to_string(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/users/token",
    tag = "users",
    responses(
        (status = 200, description = "Token issued", body = GetTokenResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_token(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
) -> ServerResult<(StatusCode, Json<GetTokenResponse>)> {
    let user_id = match auth {
        Some(aruna_ctx) => {
            if aruna_ctx.path_restrictions.is_some() {
                return Err(ServerError::Forbidden);
            }
            ensure_canonical_user_token_subject(&state, &bearer, aruna_ctx.user_id).await?;
            aruna_ctx.user_id
        }
        None => {
            let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
            let oidc_identity = validate_oidc_token(&state, token).await?;
            let user = drive(
                GetOidcUserOperation::new(GetOidcUserInput {
                    issuer: oidc_identity.issuer,
                    subject_id: oidc_identity.subject_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?;
            user.user_id
        }
    };

    let expiry = Some(now_timestamp() + USER_TOKEN_EXPIRY_SECONDS);
    let token = issue_user_token(&state, user_id, expiry).await?;

    Ok((StatusCode::OK, Json(GetTokenResponse { token })))
}

#[utoipa::path(
    get,
    path = "/users/info",
    tag = "users",
    responses(
        (status = 200, description = "Current user information", body = GetUserInfoResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
) -> ServerResult<(StatusCode, Json<GetUserInfoResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    Ok((
        StatusCode::OK,
        Json(build_user_info_response(&state, &bearer, auth).await?),
    ))
}

#[utoipa::path(
    patch,
    path = "/users/info",
    tag = "users",
    request_body = PatchUserInfoRequest,
    responses(
        (status = 200, description = "Current user updated", body = GetUserInfoResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn patch_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<PatchUserInfoRequest>,
) -> ServerResult<(StatusCode, Json<GetUserInfoResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }

    let reply = route_user_call(
        &state,
        &bearer,
        UserCall::Update {
            user_id: auth.user_id.to_string(),
            name: request.name,
            set_attributes: request.set_attributes,
            remove_attributes: request.remove_attributes,
        },
    )
    .await?;
    expect_user(reply)?;

    Ok((
        StatusCode::OK,
        Json(build_user_info_response(&state, &bearer, auth).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/users",
    tag = "users",
    params(
        ("limit" = Option<usize>, Query, description = "Maximum users to return"),
        ("start_after" = Option<String>, Query, description = "Exclusive user id cursor")
    ),
    responses(
        (status = 200, description = "Users listed", body = ListUsersResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_users(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<ListUsersQuery>,
) -> ServerResult<(StatusCode, Json<ListUsersResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let limit = query
        .limit
        .unwrap_or(DEFAULT_LIST_USERS_LIMIT)
        .clamp(1, MAX_LIST_USERS_LIMIT);
    let output = drive(
        ListUsersOperation::new(ListUsersInput {
            auth_context: auth,
            self_realm_id: realm_id,
            limit,
            start_after: query.start_after,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        aruna_operations::list_users::ListUsersError::Unauthorized => ServerError::Forbidden,
        aruna_operations::list_users::ListUsersError::ConversionError(_) => ServerError::BadRequest,
        aruna_operations::list_users::ListUsersError::AuthorizationError(_) => {
            ServerError::Forbidden
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((
        StatusCode::OK,
        Json(ListUsersResponse {
            users: output.users.into_iter().map(Into::into).collect(),
            next_start_after: output.next_start_after,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/users/search",
    tag = "users",
    params(
        ("q" = String, Query, description = "Substring to match against user name or email; minimum 2 characters"),
        ("limit" = Option<usize>, Query, description = "Maximum results, capped at 20"),
        ("start_after" = Option<String>, Query, description = "Pagination cursor")
    ),
    responses(
        (status = 200, description = "Matching users", body = SearchUsersResponse),
        (status = 400, description = "Query too short", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn search_users(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<SearchUsersQuery>,
) -> ServerResult<(StatusCode, Json<SearchUsersResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }
    let q = query.q.trim().to_string();
    if q.chars().count() < MIN_SEARCH_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let limit = query
        .limit
        .unwrap_or(MAX_SEARCH_USERS_LIMIT)
        .clamp(1, MAX_SEARCH_USERS_LIMIT);
    if let Some(start_after) = &query.start_after {
        UserId::from_string(start_after).map_err(|_| ServerError::BadRequest)?;
    }

    let output = drive(
        SearchUsersOperation::new(SearchUsersInput {
            realm_id,
            query: q,
            limit,
            start_after: query.start_after,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(SearchUsersResponse {
            users: output
                .users
                .into_iter()
                .map(|user| SearchUserResult {
                    user_id: user.user_id.to_string(),
                    name: user.name,
                })
                .collect(),
            next_start_after: output.next_start_after,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/users/{id}",
    tag = "users",
    params(("id" = String, Path, description = "User id")),
    responses(
        (status = 200, description = "User fetched", body = GetUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(user_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GetUserResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        // TODO: Forwarding for foreign realm users
        return Err(ServerError::Unimplemented);
    }
    let reply = route_user_call(&state, &bearer, UserCall::Get { user_id }).await?;
    let user = expect_user(reply)?;

    Ok((StatusCode::OK, Json(user.into())))
}

#[utoipa::path(
    patch,
    path = "/users/{id}",
    tag = "users",
    params(("id" = String, Path, description = "User id")),
    request_body = UpdateUserRequest,
    responses(
        (status = 200, description = "User updated", body = GetUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn update_user(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(user_id): Path<String>,
    Json(request): Json<UpdateUserRequest>,
) -> ServerResult<(StatusCode, Json<GetUserResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let reply = route_user_call(
        &state,
        &bearer,
        UserCall::Update {
            user_id,
            name: request.name,
            set_attributes: request.set_attributes,
            remove_attributes: request.remove_attributes,
        },
    )
    .await?;
    let user = expect_user(reply)?;

    Ok((StatusCode::OK, Json(user.into())))
}

#[cfg(test)]
mod tests {
    use super::{GetTokenResponse, RegisterUserRequest, RegisterUserResponse};
    use crate::auth::OidcValidator;
    use crate::error::ErrorResponse;
    use crate::server::Server;
    use crate::server::ServerConfig;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
    use aruna_core::onboarding::{OnboardingMode, OnboardingSecret, OnboardingSecretRecord};
    use aruna_core::structs::{
        Actor, NodeCapabilities, OidcProviderConfig, PathRestriction, Permission,
        RealmConfigDocument, RealmId, TokenClaims, User, oidc_subject_key,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::announce_realm_presence::{
        AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
    };
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::incoming::initialize_net_incoming;
    use aruna_operations::task_incoming::initialize_task_incoming;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;
    use base64::Engine;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use ulid::Ulid;

    #[derive(Clone)]
    struct OidcProviderState {
        issuer: String,
        jwks_uri: String,
        jwks: serde_json::Value,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct TestOidcClaims {
        sub: String,
        iss: String,
        aud: String,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    }

    struct TestNode {
        _temp_dir: TempDir,
        context: Arc<DriverContext>,
        state: Arc<ServerState>,
        base_url: String,
        realm_id: RealmId,
        realm_admin_id: UserId,
        net: NetHandle,
        server_task: JoinHandle<()>,
    }

    async fn oidc_discovery(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "issuer": state.issuer,
            "jwks_uri": state.jwks_uri,
        }))
    }

    async fn oidc_jwks(State(state): State<OidcProviderState>) -> Json<serde_json::Value> {
        Json(state.jwks)
    }

    async fn spawn_oidc_provider(
        issuer: &str,
        kid: &str,
        signing_key: &SigningKey,
    ) -> (OidcProviderConfig, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let jwks_uri = format!("http://{addr}/jwks.json");
        let discovery_url = format!("http://{addr}/.well-known/openid-configuration");
        let jwks = serde_json::json!({
            "keys": [{
                "kty": "OKP",
                "alg": "EdDSA",
                "use": "sig",
                "kid": kid,
                "crv": "Ed25519",
                "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
            }]
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(oidc_discovery))
            .route("/jwks.json", get(oidc_jwks))
            .with_state(OidcProviderState {
                issuer: issuer.to_string(),
                jwks_uri: jwks_uri.clone(),
                jwks,
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        (
            OidcProviderConfig {
                id: "main".to_string(),
                issuer: issuer.to_string(),
                audience: "aruna-api".to_string(),
                discovery_url,
            },
            task,
        )
    }

    fn sign_oidc_token(
        issuer: &str,
        kid: &str,
        signing_key: &SigningKey,
        subject: &str,
        name: Option<&str>,
    ) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        let claims = TestOidcClaims {
            sub: subject.to_string(),
            iss: issuer.to_string(),
            aud: "aruna-api".to_string(),
            exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
            name: name.map(str::to_string),
        };
        let key_pem = signing_key
            .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .unwrap();
        encode(
            &header,
            &claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn sign_scoped_aruna_token(node: &TestNode, user_id: UserId) -> String {
        sign_aruna_token(
            node,
            user_id,
            Some(vec![PathRestriction {
                pattern: format!("/{}/admin/u/**", node.realm_id),
                permission: Permission::READ,
            }]),
        )
    }

    fn sign_aruna_token(
        node: &TestNode,
        user_id: UserId,
        restrictions: Option<Vec<PathRestriction>>,
    ) -> String {
        let now = super::now_timestamp();
        let claims = TokenClaims {
            sub: user_id.to_string(),
            iss: node.realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::new().to_string(),
            restrictions,
            issuer_pubkey: None,
            delegation_signature: None,
        };
        let NodeCapabilities::Management {
            realm_encoding_key, ..
        } = node.state.node_capabilities()
        else {
            panic!("test node must use management capabilities");
        };
        encode(
            &Header::new(Algorithm::EdDSA),
            &claims,
            &EncodingKey::from_ed_pem(realm_encoding_key).unwrap(),
        )
        .unwrap()
    }

    async fn read_realm_config(
        driver_ctx: &DriverContext,
        realm_id: &RealmId,
    ) -> RealmConfigDocument {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmConfigDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected realm config read result: {other:?}"),
        }
    }

    async fn spawn_test_node(provider: OidcProviderConfig, claim_initial_admin: bool) -> TestNode {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage_handle = FjallStorage::open(temp_dir.path().to_str().unwrap()).unwrap();
        let realm_signing_key =
            SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(task_handle.clone()),
        });
        initialize_net_incoming(driver_ctx.clone());
        initialize_task_incoming(driver_ctx.clone(), task_handle).await;

        let node_id = net_handle.node_id();
        let realm_admin_id = UserId::local(Ulid::new(), realm_id);

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: realm_admin_id,
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();

        if claim_initial_admin {
            drive(
                ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                    actor: Actor {
                        node_id,
                        user_id: realm_admin_id,
                        realm_id,
                    },
                }),
                driver_ctx.as_ref(),
            )
            .await
            .unwrap();
        }

        drive(
            AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
                realm_id,
                node_id,
                schedule_refresh: false,
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();

        let mut config = read_realm_config(driver_ctx.as_ref(), &realm_id).await;
        config.oidc_providers.push(provider);
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                value: ByteView::from(
                    config
                        .to_bytes(&Actor {
                            node_id,
                            user_id: UserId::nil(realm_id),
                            realm_id,
                        })
                        .unwrap(),
                ),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected realm config write result: {other:?}"),
        }

        let state = Arc::new(
            ServerState::new(
                driver_ctx.clone(),
                realm_id,
                node_id,
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                false,
                Some(Arc::new(OidcValidator::new().unwrap())),
            )
            .await,
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Server::new(
            state.clone(),
            ServerConfig {
                http_addr: addr,
                max_http_body_size: crate::server::DEFAULT_MAX_HTTP_BODY_SIZE,
                cors: crate::cors::CorsConfig::default(),
            },
        )
        .build_router();
        let server_task = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });

        TestNode {
            _temp_dir: temp_dir,
            context: driver_ctx,
            state,
            base_url: format!("http://{addr}"),
            realm_id,
            realm_admin_id,
            net: net_handle,
            server_task,
        }
    }

    async fn register_via_oidc(
        node: &TestNode,
        issuer: &str,
        kid: &str,
        signing_key: &SigningKey,
        subject: &str,
        name: &str,
        onboarding_secret: Option<String>,
    ) -> (RegisterUserResponse, String) {
        let oidc_token = sign_oidc_token(issuer, kid, signing_key, subject, Some(name));
        let register = reqwest::Client::new()
            .post(format!("{}/api/v1/users/register", node.base_url))
            .bearer_auth(&oidc_token)
            .json(&RegisterUserRequest { onboarding_secret })
            .send()
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::CREATED);
        let registered: RegisterUserResponse = register.json().await.unwrap();

        let token_response = reqwest::Client::new()
            .get(format!("{}/api/v1/users/token", node.base_url))
            .bearer_auth(&oidc_token)
            .send()
            .await
            .unwrap();
        assert_eq!(token_response.status(), StatusCode::OK);
        let token: GetTokenResponse = token_response.json().await.unwrap();

        (registered, token.token)
    }

    async fn create_local_onboarding_secret(node: &TestNode) -> String {
        let onboarding_secret = OnboardingSecret {
            seed_url: node.base_url.clone(),
            enrollment_id: Ulid::new(),
            secret: [7u8; 32],
            mode: OnboardingMode::Local,
        };
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: onboarding_secret.enrollment_id,
                    secret_hash: onboarding_secret.secret_hash(),
                    mode: OnboardingMode::Local,
                    expires_at: u64::MAX,
                    claimed_node_id: None,
                },
            }),
            node.context.as_ref(),
        )
        .await
        .unwrap();
        onboarding_secret.encode().unwrap()
    }

    #[tokio::test]
    async fn get_user_for_regular_registered_user_returns_forbidden() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (registered, aruna_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-123",
            "Alice",
            None,
        )
        .await;

        let response = reqwest::Client::new()
            .get(format!("{}/api/v1/users/{}", node.base_url, registered.id))
            .bearer_auth(&aruna_token)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: ErrorResponse = response.json().await.unwrap();
        assert_eq!(body.error, "Forbidden");

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn issued_user_token_carries_bounded_expiry() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (_registered, aruna_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "expiry-subject",
            "Expiry Alice",
            None,
        )
        .await;

        let payload = aruna_token.split('.').nth(1).unwrap();
        let claims: TokenClaims = serde_json::from_slice(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(payload)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(claims.exp, claims.iat + super::USER_TOKEN_EXPIRY_SECONDS);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn bootstrap_registration_consumes_secret() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, false).await;

        let onboarding_secret = create_local_onboarding_secret(&node).await;
        let (body, _token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "bootstrap-subject",
            "Admin Alice",
            Some(onboarding_secret.clone()),
        )
        .await;
        assert_eq!(body.name, "Admin Alice");

        let second_response = reqwest::Client::new()
            .post(format!("{}/api/v1/users/register", node.base_url))
            .bearer_auth(sign_oidc_token(
                issuer,
                kid,
                &signing_key,
                "bootstrap-subject-2",
                Some("Other Admin"),
            ))
            .json(&RegisterUserRequest {
                onboarding_secret: Some(onboarding_secret),
            })
            .send()
            .await
            .unwrap();

        assert_eq!(second_response.status(), StatusCode::UNAUTHORIZED);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_user_for_missing_user_without_admin_permissions_returns_forbidden() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (_admin, admin_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-404",
            "Admin Alice",
            None,
        )
        .await;

        let missing_user_id = UserId::local(Ulid::new(), node.realm_id);
        let response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/users/{}",
                node.base_url, missing_user_id
            ))
            .bearer_auth(&admin_token)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_user_requires_authentication() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let missing_user_id = UserId::local(Ulid::new(), node.realm_id);
        let response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/users/{}",
                node.base_url, missing_user_id
            ))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_user_returns_unimplemented_for_foreign_realm_token() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let foreign_realm_signing_key =
            SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let foreign_realm_id =
            RealmId::from_bytes(foreign_realm_signing_key.verifying_key().to_bytes());
        node.state.add_trusted_realm(foreign_realm_id).await;
        let foreign_user_id = UserId::local(Ulid::new(), foreign_realm_id);
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: USER_KEYSPACE.to_string(),
                key: ByteView::from(foreign_user_id.to_bytes()),
                value: ByteView::from(
                    User {
                        user_id: foreign_user_id,
                        name: "Foreign User".to_string(),
                        subject_ids: Vec::new(),
                        alias_user_ids: Default::default(),
                        attributes: Default::default(),
                    }
                    .to_bytes(&Actor {
                        node_id: node.net.node_id(),
                        user_id: node.realm_admin_id,
                        realm_id: node.realm_id,
                    })
                    .unwrap(),
                ),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected foreign user write result: {other:?}"),
        }
        let token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time: super::now_timestamp(),
                expiry: None,
                user_id: foreign_user_id,
                realm_id: foreign_realm_id,
                node_capabilities: NodeCapabilities::management_node(foreign_realm_signing_key)
                    .unwrap(),
            })
            .unwrap(),
            node.context.as_ref(),
        )
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .get(format!(
                "{}/api/v1/users/{}",
                node.base_url, node.realm_admin_id
            ))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_token_returns_aruna_token_for_registered_oidc_user() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let oidc_token = sign_oidc_token(issuer, kid, &signing_key, "subject-123", Some("Alice"));
        let register = reqwest::Client::new()
            .post(format!("{}/api/v1/users/register", node.base_url))
            .bearer_auth(&oidc_token)
            .json(&RegisterUserRequest {
                onboarding_secret: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(register.status(), StatusCode::CREATED);

        let token_response = reqwest::Client::new()
            .get(format!("{}/api/v1/users/token", node.base_url))
            .bearer_auth(&oidc_token)
            .send()
            .await
            .unwrap();

        assert_eq!(token_response.status(), StatusCode::OK);
        let body: GetTokenResponse = token_response.json().await.unwrap();
        assert!(!body.token.is_empty());

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_token_rejects_scoped_aruna_token() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (registered, _aruna_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-123",
            "Alice",
            None,
        )
        .await;
        let scoped_token =
            sign_scoped_aruna_token(&node, UserId::from_string(&registered.id).unwrap());

        let token_response = reqwest::Client::new()
            .get(format!("{}/api/v1/users/token", node.base_url))
            .bearer_auth(&scoped_token)
            .send()
            .await
            .unwrap();

        assert_eq!(token_response.status(), StatusCode::FORBIDDEN);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_token_rejects_alias_aruna_token() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (registered, _aruna_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-123",
            "Alice",
            None,
        )
        .await;
        let canonical_user_id = UserId::from_string(&registered.id).unwrap();
        let alias_user_id = UserId::local(Ulid::from_bytes([9u8; 16]), node.realm_id);
        let subject_key = oidc_subject_key(issuer, "subject-123").unwrap();
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: USER_KEYSPACE.to_string(),
                key: ByteView::from(alias_user_id.to_bytes()),
                value: ByteView::from(
                    User {
                        user_id: alias_user_id,
                        name: "Alias Alice".to_string(),
                        subject_ids: vec![subject_key],
                        alias_user_ids: Default::default(),
                        attributes: Default::default(),
                    }
                    .to_bytes(&Actor {
                        node_id: node.net.node_id(),
                        user_id: canonical_user_id,
                        realm_id: node.realm_id,
                    })
                    .unwrap(),
                ),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected alias user write result: {other:?}"),
        }
        let alias_token = sign_aruna_token(&node, alias_user_id, None);

        let token_response = reqwest::Client::new()
            .get(format!("{}/api/v1/users/token", node.base_url))
            .bearer_auth(&alias_token)
            .send()
            .await
            .unwrap();

        assert_eq!(token_response.status(), StatusCode::FORBIDDEN);

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }
}
