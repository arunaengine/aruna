#[path = "users_vault.rs"]
mod vault;

use crate::auth::{OidcIdentity, bearer_token, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::onboarding::authorize_onboarding_admin;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::onboarding::{OnboardingPurpose, OnboardingSecret};
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, Permission, RealmAuthorizationDocument,
    Role, SessionKind, User,
};
use aruna_core::time::unix_timestamp_secs as now_timestamp;
use aruna_operations::auth::token_subject::{SubjectCheckError, SubjectCheckOperation};
use aruna_operations::device::remove_node::{
    DeviceEvictionScope, RemoveNodeConfig, RemoveNodeError, RemoveNodeOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::groups::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::groups::list_groups::ListGroupOperation;
use aruna_operations::onboarding::consume_secret::{
    ConsumeSecretError, ConsumeSecretInput, ConsumeSecretOperation,
};
use aruna_operations::onboarding::delete_secret::{
    DeleteSecretError, DeleteSecretInput, DeleteSecretOperation,
};
use aruna_operations::onboarding::inspect_secret::{
    InspectSecretError, InspectSecretInput, InspectSecretOperation,
};
use aruna_operations::onboarding::list_secrets::ListSecretsOperation;
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::realm::read_authorization::{
    ReadAuthorizationError, ReadAuthorizationOperation,
};
use aruna_operations::session::{CreateSessionConfig, CreateSessionError, CreateSessionOperation};
use aruna_operations::users::get_oidc::{GetOidcInput, GetOidcOperation};
use aruna_operations::users::get_user::{GetUserInput, GetUserOperation};
use aruna_operations::users::list_users::{ListUsersInput, ListUsersOperation};
use aruna_operations::users::oidc_user::{ResolveOidcInput, ResolveOidcOperation};
use aruna_operations::users::read_document::{ReadUserError, ReadUserOperation};
use aruna_operations::users::resolve_users::{ResolveUsersInput, ResolveUsersOperation};
use aruna_operations::users::search_users::{SearchUsersInput, SearchUsersOperation};
use aruna_operations::users::update_user::{UpdateUserInput, UpdateUserOperation};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "access/users", description = "User operations"),
        (name = "access/devices", description = "User device administration")
    )
)]
pub struct UsersApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(UsersApiDoc::openapi())
        .routes(routes!(register_user))
        .routes(routes!(get_token))
        .routes(routes!(get_user_info, patch_user_info))
        .routes(routes!(list_users))
        .routes(routes!(search_users))
        .routes(routes!(resolve_users))
        .routes(routes!(get_user, update_user))
        .routes(routes!(list_user_devices))
        .routes(routes!(revoke_user_device))
        .routes(routes!(evict_device))
        .merge(vault::router())
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
pub struct ResolveUsersRequest {
    pub user_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ResolveUserResult {
    pub user_id: String,
    pub name: String,
    /// Only attributes the user explicitly marks public.
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = UserInfoRoleResponse)]
pub struct UserRoleResponse {
    pub role_id: String,
    pub name: String,
    pub permissions: HashMap<String, String>,
    pub assigned_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = UserInfoRealmResponse)]
pub struct UserRealmResponse {
    pub realm_id: String,
    pub roles: Vec<UserRoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = UserInfoGroupResponse)]
pub struct UserGroupResponse {
    pub group_id: String,
    pub display_name: String,
    pub roles: Vec<UserRoleResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = UserInfoPreferencesResponse)]
pub struct UserPreferencesResponse {
    pub preferred_profile_path: Option<String>,
    pub favourite_metadata_ids: Vec<String>,
    pub theme: Option<String>,
    /// Which dashboard section leads: `personal` or `realm`. Absent when the
    /// attribute is unset or carries an unknown value.
    pub dashboard_scope: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = GetUserInfoResponse)]
pub struct UserInfoResponse {
    pub user: GetUserResponse,
    pub realm: UserRealmResponse,
    pub groups: Vec<UserGroupResponse>,
    pub preferences: UserPreferencesResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserDevicesResponse {
    pub devices: Vec<UserDeviceResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserDeviceResponse {
    /// Identifier this device is addressed by: its node id once enrolled, its
    /// enrollment id while the secret is still outstanding.
    pub id: String,
    /// Node id, set once the device has joined the realm configuration.
    pub node_id: Option<String>,
    /// Enrollment id, set while an enrollment secret is still outstanding.
    pub enrollment_id: Option<String>,
    /// `enrolled`, `claimed`, `pending` or `expired`.
    pub status: String,
    /// Expiry of an outstanding enrollment secret, in Unix seconds.
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateUserRequest {
    pub name: Option<String>,
    #[serde(default)]
    pub set_attributes: HashMap<String, String>,
    #[serde(default)]
    pub remove_attributes: Vec<String>,
}

pub type PatchUserRequest = UpdateUserRequest;

const DEFAULT_LIST_USERS_LIMIT: usize = 100;
const MAX_LIST_USERS_LIMIT: usize = 1_000;
pub(crate) const MIN_SEARCH_QUERY_CHARS: usize = 2;
const MAX_SEARCH_USERS_LIMIT: usize = 20;
const MAX_RESOLVE_USER_IDS: usize = 100;

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

fn map_user_role(role_id: Ulid, role: Role) -> UserRoleResponse {
    UserRoleResponse {
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

fn preferences_from_attributes(attributes: &HashMap<String, String>) -> UserPreferencesResponse {
    UserPreferencesResponse {
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
        dashboard_scope: attributes
            .get("ui.dashboard_scope")
            .map(|value| value.trim())
            .filter(|value| matches!(*value, "personal" | "realm"))
            .map(ToOwned::to_owned),
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

fn map_consume_error(error: ConsumeSecretError) -> ServerError {
    match error {
        ConsumeSecretError::NotFound
        | ConsumeSecretError::Expired
        | ConsumeSecretError::AlreadyClaimed
        | ConsumeSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_inspect_error(error: InspectSecretError) -> ServerError {
    match error {
        InspectSecretError::NotFound
        | InspectSecretError::Expired
        | InspectSecretError::AlreadyClaimed
        | InspectSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

const USER_TOKEN_EXPIRY_SECONDS: u64 = 24 * 60 * 60;

async fn issue_user_session(
    state: &Arc<ServerState>,
    user_id: UserId,
    expiry: u64,
    kind: SessionKind,
) -> ServerResult<String> {
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now_timestamp(),
            expiry,
            user_id,
            realm_id: state.get_realm_id(),
            node_capabilities: state.node_capabilities().clone(),
            kind,
            label: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        CreateSessionError::LimitReached => {
            ServerError::Conflict("active session limit reached".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    })?;
    Ok(created.token.expose().to_string())
}

async fn ensure_token_subject(state: &Arc<ServerState>, user_id: UserId) -> ServerResult<()> {
    drive(SubjectCheckOperation::new(user_id), &state.get_ctx())
        .await
        .map_err(map_subject_error)
}

async fn read_current_user(state: &ServerState, user_id: UserId) -> ServerResult<User> {
    drive(ReadUserOperation::new(user_id), &state.get_ctx())
        .await
        .map_err(map_user_error)
}

async fn read_realm_authorization(
    state: &ServerState,
) -> ServerResult<Option<RealmAuthorizationDocument>> {
    drive(
        ReadAuthorizationOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_realm_error)
}

fn map_subject_error(error: SubjectCheckError) -> ServerError {
    match error {
        SubjectCheckError::Unauthorized => ServerError::Unauthorized,
        SubjectCheckError::Forbidden => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_user_error(error: ReadUserError) -> ServerError {
    match error {
        ReadUserError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_realm_error(error: ReadAuthorizationError) -> ServerError {
    ServerError::InternalError(error.to_string())
}

fn collect_realm_roles(
    auth_doc: Option<RealmAuthorizationDocument>,
    user_id: UserId,
) -> Vec<UserRoleResponse> {
    auth_doc
        .into_iter()
        .flat_map(|document| document.roles)
        .filter(|(_, role)| role.assigned_users.contains(&user_id))
        .map(|(role_id, role)| map_user_role(role_id, role))
        .collect()
}

fn collect_group_roles(
    auth_doc: GroupAuthorizationDocument,
    user_id: UserId,
) -> Vec<UserRoleResponse> {
    auth_doc
        .roles
        .into_iter()
        .filter(|(_, role)| role.assigned_users.contains(&user_id))
        .map(|(role_id, role)| map_user_role(role_id, role))
        .collect()
}

async fn collect_group_memberships(
    state: &ServerState,
    user_id: UserId,
) -> ServerResult<Vec<UserGroupResponse>> {
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
        let roles = collect_group_roles(auth_doc, user_id);
        if roles.is_empty() {
            continue;
        }
        memberships.push(UserGroupResponse {
            group_id: group.group_id.to_string(),
            display_name: group.display_name,
            roles,
        });
    }
    Ok(memberships)
}

async fn build_user_response(
    state: &ServerState,
    auth: AuthContext,
) -> ServerResult<UserInfoResponse> {
    if auth.realm_id != state.get_realm_id() || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let user = read_current_user(state, auth.user_id).await?;
    let preferences = preferences_from_attributes(&user.attributes);
    let realm_roles = collect_realm_roles(read_realm_authorization(state).await?, auth.user_id);
    let groups = collect_group_memberships(state, auth.user_id).await?;

    Ok(UserInfoResponse {
        user: user.into(),
        realm: UserRealmResponse {
            realm_id: state.get_realm_id().to_string(),
            roles: realm_roles,
        },
        groups,
        preferences,
    })
}

async fn claim_initial_admin(state: &Arc<ServerState>, user_id: UserId) {
    let auth_context = AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    };
    if let Err(error) = state.claim_initial_admin(&auth_context).await {
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
        .get_oidc_provider(&selector)
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
    if onboarding_secret.realm_id != state.get_realm_id() {
        return Err(ServerError::Unauthorized);
    }
    let secret_hash = onboarding_secret.secret_hash();
    let inspected = drive(
        InspectSecretOperation::new(InspectSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: secret_hash.clone(),
            node_id: user_id.to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_inspect_error)?;
    if inspected.purpose != OnboardingPurpose::InitialAdministrator {
        return Err(ServerError::Forbidden);
    }

    drive(
        ConsumeSecretOperation::new(ConsumeSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash,
            node_id: user_id.to_string(),
            now: now_timestamp(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_consume_error)?;

    let user = drive(
        ResolveOidcOperation::new(ResolveOidcInput {
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

    claim_initial_admin(state, user.user_id).await;
    Ok(user)
}

#[utoipa::path(
    post,
    path = "/access/users/register",
    tag = "access/users",
    summary = "Register the caller as a realm user",
    description = r#"Registers the subject of the presented OIDC token as a user of this realm.

**Authentication**: an OIDC bearer token from a configured issuer, not a realm bearer token. Any
holder of a valid one registers themselves; an onboarding secret in the body additionally claims the
initial realm administrator role.

**Behavior**
- Without an onboarding secret this is get or create: a subject that already has a user gets the
  existing one back unchanged, still with 201.
- The onboarding secret must have been issued for this realm with the initial administrator purpose
  and is consumed single-use.
- The registered user is always the subject of the presented token, never a user id chosen by the
  caller, and the display name comes from the token.
- The user document is written here and reaches the other realm nodes through document sync, so it
  may not be visible elsewhere immediately.
- No access token is returned: exchange the same OIDC token at `GET /access/token`."#,
    request_body(
        content = RegisterUserRequest,
        description = "Optional onboarding secret. Omit it, or send null, for ordinary self service registration; send the secret handed out by the node operator only to claim the initial realm administrator.",
        examples(
            (
                "SelfService" = (
                    summary = "Register the OIDC subject with no special role",
                    value = json!({
                        "onboarding_secret": null
                    })
                )
            ),
            (
                "InitialAdministrator" = (
                    summary = "Consume an onboarding secret and claim the realm administrator role",
                    value = json!({
                        "onboarding_secret": "<onboarding-secret-issued-by-the-node-operator>"
                    })
                )
            )
        )
    ),
    responses(
        (
            status = 201,
            description = "The user of this OIDC subject, newly created or already present from an earlier registration",
            body = RegisterUserResponse,
            example = json!({
                "id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Alice Example"
            })
        ),
        (status = 400, description = "Malformed request body", body = ErrorResponse),
        (status = 401, description = "Missing or invalid OIDC bearer token, or an onboarding secret that is unknown, expired, already claimed or issued for another realm", body = ErrorResponse),
        (status = 403, description = "The onboarding secret was issued for another purpose than the initial realm administrator", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn register_user(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(request): Json<RegisterUserRequest>,
) -> ServerResult<(StatusCode, Json<RegisterUserResponse>)> {
    let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
    let oidc_identity = validate_oidc_token(&state, token).await?;

    let user_id = UserId::local(Ulid::generate(), state.get_realm_id());
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
                ResolveOidcOperation::new(ResolveOidcInput {
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
    path = "/access/token",
    tag = "access/tokens",
    summary = "Issue a realm bearer token",
    description = r#"Mints a realm bearer token for the calling user, valid for 24 hours.

**Authentication**: a realm bearer token without path restrictions, which refreshes itself, or an
OIDC token from a configured issuer whose subject has been registered at
`POST /access/users/register`. The token is always minted for the caller and never on behalf of
somebody else.

**Behavior**
- The token preserves a bound session's kind; an OIDC or unbound caller receives a `portal` session.
- The token is returned in this response only, so a lost one has to be reissued here.

**Limits**
- Issuance prunes expired and revoked sessions first and refuses when 256 active ones remain."#,
    responses(
        (
            status = 200,
            description = "A freshly issued realm bearer token for the caller, shown only here",
            body = GetTokenResponse,
            example = json!({
                "token": "<aruna-access-token>"
            })
        ),
        (status = 401, description = "Missing or invalid bearer token, or this node knows no user for the presented OIDC subject", body = ErrorResponse),
        (status = 403, description = "The presented token is path-restricted, or its user is an alias of the canonical user of that OIDC subject", body = ErrorResponse),
        (status = 409, description = "The caller already holds 256 active sessions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_token(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<GetTokenResponse>)> {
    let (user_id, kind) = match auth {
        Some(aruna_ctx) => {
            if aruna_ctx.path_restrictions.is_some() {
                return Err(ServerError::Forbidden);
            }
            ensure_token_subject(&state, aruna_ctx.user_id).await?;
            let kind = aruna_ctx
                .session
                .as_ref()
                .map_or(SessionKind::Portal, |session| session.kind);
            (aruna_ctx.user_id, kind)
        }
        None => {
            let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
            let oidc_identity = validate_oidc_token(&state, token).await?;
            let user = drive(
                GetOidcOperation::new(GetOidcInput {
                    issuer: oidc_identity.issuer,
                    subject_id: oidc_identity.subject_id,
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(|err| ServerError::InternalError(err.to_string()))?;
            (user.user_id, SessionKind::Portal)
        }
    };

    let expiry = now_timestamp()
        .checked_add(USER_TOKEN_EXPIRY_SECONDS)
        .ok_or_else(|| ServerError::InternalError("token expiry overflow".to_string()))?;
    let token = issue_user_session(&state, user_id, expiry, kind).await?;

    Ok((StatusCode::OK, Json(GetTokenResponse { token })))
}

#[utoipa::path(
    get,
    path = "/access/users/me",
    tag = "access/users",
    summary = "Get the calling user's profile",
    description = r#"Describes the calling user: profile, realm roles, group roles and UI preferences.

**Authentication**: realm bearer token without path restrictions. It always describes the caller and
takes no user id.

**Behavior**
- Preferences are derived from the caller's `ui.theme`, `ui.preferred_profile_path`,
  `ui.favourite_metadata_ids` (a comma separated list) and `ui.dashboard_scope` (`personal` or
  `realm`, absent when unset or unknown) attributes.
- Group membership is collected from the groups this node holds, so a group that has not arrived
  here yet is missing."#,
    responses(
        (
            status = 200,
            description = "The caller's user document, realm roles, group roles and UI preferences",
            body = UserInfoResponse,
            example = json!({
                "user": {
                    "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                    "name": "Alice Example",
                    "subject_ids": ["YXJ1bmEtZXhhbXBsZS1vaWRjLXN1YmplY3QtMDAwMDA"],
                    "attributes": {
                        "email": "user@example.test",
                        "orcid": "0000-0002-1825-0097",
                        "ui.theme": "dark",
                        "ui.preferred_profile_path": "datasets/proteomics",
                        "ui.favourite_metadata_ids": "01JMETADATA0123456789ABCDE"
                    }
                },
                "realm": {
                    "realm_id": "YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                    "roles": [
                        {
                            "role_id": "01JR0123456789ABCDEFGHJKMN",
                            "name": "realm-admin",
                            "permissions": {"/YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA/admin/**": "Write"},
                            "assigned_users": ["01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"]
                        }
                    ]
                },
                "groups": [
                    {
                        "group_id": "01JGRP00123456789ABCDEFGHJ",
                        "display_name": "Proteomics",
                        "roles": [
                            {
                                "role_id": "01JR0123456789ABCDEFGHJKMP",
                                "name": "group-writer",
                                "permissions": {"/YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA/g/01JGRP00123456789ABCDEFGHJ/**": "Write"},
                                "assigned_users": ["01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"]
                            }
                        ]
                    }
                ],
                "preferences": {
                    "preferred_profile_path": "datasets/proteomics",
                    "favourite_metadata_ids": ["01JMETADATA0123456789ABCDE"],
                    "theme": "dark",
                    "dashboard_scope": "personal"
                }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or is path-restricted", body = ErrorResponse),
        (status = 404, description = "This node holds no user document for the caller, which can happen while a registration made elsewhere is still replicating", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UserInfoResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    Ok((
        StatusCode::OK,
        Json(build_user_response(&state, auth).await?),
    ))
}

#[utoipa::path(
    patch,
    path = "/access/users/me",
    tag = "access/users",
    summary = "Update the calling user's profile",
    description = r#"Updates the calling user's display name and attributes and returns the refreshed profile.

**Authentication**: realm bearer token without path restrictions. It always writes the caller's own
user document and takes no user id.

**Behavior**
- Fields left out change nothing, and removals are applied before sets, so a key named in both ends
  up set to the new value.
- Profile visibility uses `profile.visibility.<field>` attributes set to `public` or `private`.
  Names default to public; other attributes default to private.
- UI preferences are ordinary attributes: `ui.theme`, `ui.preferred_profile_path`,
  `ui.favourite_metadata_ids` as a comma separated list, and `ui.dashboard_scope`.
- The write is durable here and reaches the other realm nodes through document sync.

**Limits**
- The name is trimmed and must be 1 to 256 characters.
- An attribute key is ASCII letters, digits, dot, underscore, hyphen or colon of at most 128 bytes.
- An attribute value is at most 4096 bytes and carries no control characters.
- A user holds at most 128 attributes."#,
    request_body(
        content = PatchUserRequest,
        description = "Optional new display name, attributes to set and attribute keys to remove. Send only what changes.",
        example = json!({
            "name": "Alice Example",
            "set_attributes": {
                "ui.theme": "dark",
                "orcid": "0000-0002-1825-0097"
            },
            "remove_attributes": ["ui.preferred_profile_path"]
        })
    ),
    responses(
        (
            status = 200,
            description = "The caller's profile after the update, with realm roles, group roles and the recomputed preferences",
            body = UserInfoResponse,
            example = json!({
                "user": {
                    "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                    "name": "Alice Example",
                    "subject_ids": ["YXJ1bmEtZXhhbXBsZS1vaWRjLXN1YmplY3QtMDAwMDA"],
                    "attributes": {
                        "email": "user@example.test",
                        "orcid": "0000-0002-1825-0097",
                        "ui.theme": "dark"
                    }
                },
                "realm": {
                    "realm_id": "YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                    "roles": []
                },
                "groups": [
                    {
                        "group_id": "01JGRP00123456789ABCDEFGHJ",
                        "display_name": "Proteomics",
                        "roles": [
                            {
                                "role_id": "01JR0123456789ABCDEFGHJKMP",
                                "name": "group-writer",
                                "permissions": {"/YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA/g/01JGRP00123456789ABCDEFGHJ/**": "Write"},
                                "assigned_users": ["01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"]
                            }
                        ]
                    }
                ],
                "preferences": {
                    "preferred_profile_path": null,
                    "favourite_metadata_ids": [],
                    "theme": "dark",
                    "dashboard_scope": null
                }
            })
        ),
        (status = 400, description = "The name is out of range, an attribute key or value is rejected, or the user would hold more than 128 attributes", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or is path-restricted", body = ErrorResponse),
        (status = 404, description = "This node holds no user document for the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn patch_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<PatchUserRequest>,
) -> ServerResult<(StatusCode, Json<UserInfoResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }

    drive(
        UpdateUserOperation::new(UpdateUserInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            auth_context: auth.clone(),
            self_realm_id: realm_id,
            user_id: auth.user_id.to_string(),
            name: request.name,
            set_attributes: request.set_attributes,
            remove_attributes: request.remove_attributes,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        aruna_operations::users::update_user::UpdateUserError::Unauthorized => {
            ServerError::Forbidden
        }
        aruna_operations::users::update_user::UpdateUserError::UserNotFound => {
            ServerError::NotFound
        }
        aruna_operations::users::update_user::UpdateUserError::InvalidUserName
        | aruna_operations::users::update_user::UpdateUserError::InvalidAttributeKey(_)
        | aruna_operations::users::update_user::UpdateUserError::InvalidAttributeValue(_)
        | aruna_operations::users::update_user::UpdateUserError::TooManyAttributes
        | aruna_operations::users::update_user::UpdateUserError::ConversionError(_) => {
            ServerError::BadRequest
        }
        aruna_operations::users::update_user::UpdateUserError::AuthorizationError(_) => {
            ServerError::Forbidden
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((
        StatusCode::OK,
        Json(build_user_response(&state, auth).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/access/users",
    tag = "access/users",
    summary = "List the users of this realm",
    description = r#"Pages this realm's user documents in user id order.

**Authentication**: realm bearer token with READ on the realm's user administration path. The realm
request policies are evaluated as well and may deny a read the role grant alone would allow.

**Behavior**
- This is a node-local read of replicated documents, so a user registered on another node appears
  once it arrives here.
- Every entry is the full user document including its attributes.
- Pagination is cursor based: `next_start_after` repeats the last returned user id, and its absence
  means the end of the listing was reached.

**Limits**
- `limit` defaults to 100 and is clamped into 1 to 1000."#,
    params(
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 100 and is clamped into 1 to 1000"),
        ("start_after" = Option<String>, Query, description = "Exclusive cursor: the `next_start_after` of the previous page; omit it to start at the first user")
    ),
    responses(
        (
            status = 200,
            description = "One page of this realm's users held by this node, with the cursor for the next",
            body = ListUsersResponse,
            example = json!({
                "users": [
                    {
                        "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                        "name": "Alice Example",
                        "subject_ids": ["YXJ1bmEtZXhhbXBsZS1vaWRjLXN1YmplY3QtMDAwMDA"],
                        "attributes": {
                            "email": "user@example.test",
                            "orcid": "0000-0002-1825-0097"
                        }
                    }
                ],
                "next_start_after": "01JB2C3D4E5F6G7H8J9KABCDEF@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"
            })
        ),
        (status = 400, description = "The `start_after` cursor is not a user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the caller lacks READ on the realm's user administration path", body = ErrorResponse)
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
    ensure_permission(
        &state,
        &auth,
        format!("/{realm_id}/admin/u/**"),
        Permission::READ,
    )
    .await?;

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
        aruna_operations::users::list_users::ListUsersError::Unauthorized => ServerError::Forbidden,
        aruna_operations::users::list_users::ListUsersError::ConversionError(_) => {
            ServerError::BadRequest
        }
        aruna_operations::users::list_users::ListUsersError::AuthorizationError(_) => {
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

pub(crate) async fn authorize_directory(
    state: &ServerState,
    auth: &AuthContext,
    user_id: Option<&str>,
) -> ServerResult<()> {
    crate::auth::require_unrestricted_auth(state, Some(auth.clone()))?;
    if auth.user_id.is_nil() || auth.user_id.realm_id != auth.realm_id {
        return Err(ServerError::Forbidden);
    }
    let path = format!(
        "/{}/admin/u/{}",
        state.get_realm_id(),
        user_id.unwrap_or("**")
    );
    aruna_operations::auth::request_policy::enforce_policies(
        &state.get_ctx(),
        state.get_realm_id(),
        &aruna_operations::auth::request_policy::policy_request_with(
            &path,
            &Permission::READ,
            Some(auth),
            aruna_operations::auth::request_policy::PolicyRequestExtras::rest(),
        ),
    )
    .await
    .map_err(|error| match error {
        aruna_operations::auth::request_policy::PolicyEnforcementError::Denied { .. } => {
            ServerError::Forbidden
        }
        other => ServerError::InternalError(other.to_string()),
    })
}

#[utoipa::path(
    get,
    path = "/access/users/search",
    tag = "access/users",
    summary = "Search public user profile fields",
    description = r#"Searches public profile fields of users in this realm.

**Authentication**: unrestricted realm bearer token. Realm request policies may deny the read.

**Behavior**
- Matches public names and explicitly public attributes case insensitively. Private fields never
  participate in matching. Names are public by default; other attributes are private by default.
- Returns user ids and public display names. A private name is replaced by the user id.
- Reads this node's replicated user directory. Pagination uses `next_start_after`.
- `q` must contain at least 2 trimmed characters. `limit` defaults to 20, clamped to 1 to 20."#,
    params(
        ("q" = String, Query, description = "Substring matched against public profile fields; at least 2 characters"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 20 and is clamped into 1 to 20"),
        ("start_after" = Option<String>, Query, description = "Exclusive cursor: the `next_start_after` of the previous page; omit it to start at the first user")
    ),
    responses(
        (
            status = 200,
            description = "One page of matching users, reduced to user id and display name, with the cursor for the next",
            body = SearchUsersResponse,
            example = json!({
                "users": [
                    {
                        "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                        "name": "Alice Example"
                    }
                ],
                "next_start_after": null
            })
        ),
        (status = 400, description = "The query is shorter than 2 characters after trimming, or the `start_after` cursor is not a user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the token is path-restricted, or realm policy denies the read", body = ErrorResponse)
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
    authorize_directory(&state, &auth, None).await?;

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
    post,
    path = "/access/users/resolve",
    tag = "access/users",
    summary = "Resolve user ids to directory entries",
    description = r#"Resolves a batch of user ids to directory entries held by this node.

**Authentication**: unrestricted realm bearer token. Realm request policies may deny the read.

**Behavior**
- Duplicate ids collapse and ids unknown to this node are dropped silently, so the result may be
  shorter than the request and carries no positional mapping; match the entries by user id.
- Only explicitly public attributes are returned. Names default to public; a private name is
  replaced by the user id.
- The response body is a JSON array, not an object.

**Limits**
- At most 100 user ids per request."#,
    request_body(
        content = ResolveUsersRequest,
        description = "Up to 100 user ids, each in the form `<ulid>@<realm>`. Duplicates are collapsed and unknown ids are omitted from the result.",
        example = json!({
            "user_ids": [
                "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "01JB2C3D4E5F6G7H8J9KABCDEF@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA"
            ]
        })
    ),
    responses(
        (
            status = 200,
            description = "Directory entries for the ids this node could resolve, with only their public attributes",
            body = [ResolveUserResult],
            example = json!([
                {
                    "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                    "name": "Alice Example",
                    "attributes": {
                        "orcid": "0000-0002-1825-0097",
                        "affiliation": "Example University"
                    }
                }
            ])
        ),
        (status = 400, description = "More than 100 user ids were sent, or an entry is not a user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or the token is path-restricted, or realm policy denies the read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn resolve_users(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ResolveUsersRequest>,
) -> ServerResult<(StatusCode, Json<Vec<ResolveUserResult>>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }
    if request.user_ids.len() > MAX_RESOLVE_USER_IDS {
        return Err(ServerError::BadRequest);
    }
    let user_ids = request
        .user_ids
        .iter()
        .map(|user_id| UserId::from_string(user_id).map_err(|_| ServerError::BadRequest))
        .collect::<ServerResult<Vec<_>>>()?;
    authorize_directory(&state, &auth, None).await?;

    let output = drive(
        ResolveUsersOperation::new(ResolveUsersInput { realm_id, user_ids }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    Ok((
        StatusCode::OK,
        Json(
            output
                .users
                .into_iter()
                .map(|user| ResolveUserResult {
                    user_id: user.user_id.to_string(),
                    name: user.name,
                    attributes: user.attributes,
                })
                .collect(),
        ),
    ))
}

#[utoipa::path(
    get,
    path = "/access/users/{id}",
    tag = "access/users",
    summary = "Get a user of this realm by id",
    description = r#"Returns one user document of this realm as held by the responding node.

**Authentication**: realm bearer token. An unrestricted caller can read public profile fields.
The caller's own profile is complete; READ on the user's administration path also returns the
complete document. Realm request policies may deny either read.

**Behavior**
- This is a node-local read of a replicated document, so a user registered on another node appears
  once it arrives here.
- The response is the full user document including its attributes."#,
    params(("id" = String, Path, description = "User id in the form `<ulid>@<realm>`, as returned by the listing and search operations")),
    responses(
        (
            status = 200,
            description = "The user document held by this node",
            body = GetUserResponse,
            example = json!({
                "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Alice Example",
                "subject_ids": ["YXJ1bmEtZXhhbXBsZS1vaWRjLXN1YmplY3QtMDAwMDA"],
                "attributes": {
                    "email": "user@example.test",
                    "orcid": "0000-0002-1825-0097"
                }
            })
        ),
        (status = 400, description = "Malformed user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Path-restricted token without a matching grant, or realm policy denies the read", body = ErrorResponse),
        (status = 404, description = "This node holds no user with that id, and the caller is allowed to know that", body = ErrorResponse),
        (status = 501, description = "The token was issued by another trusted realm; forwarding the read to the owning realm is not implemented", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(user_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GetUserResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        // TODO: Forwarding for foreign realm users
        return Err(ServerError::Unimplemented);
    }
    let target = UserId::from_string(&user_id).map_err(|_| ServerError::BadRequest)?;
    if target.realm_id != realm_id {
        return Err(ServerError::NotFound);
    }
    let privileged = crate::auth::permission_granted(
        &state,
        &auth,
        format!("/{realm_id}/admin/u/{user_id}"),
        Permission::READ,
    )
    .await?;
    if !privileged {
        authorize_directory(&state, &auth, Some(&user_id)).await?;
        let user = drive(ReadUserOperation::new(target), &state.get_ctx())
            .await
            .map_err(|error| match error {
                ReadUserError::NotFound => ServerError::NotFound,
                other => ServerError::InternalError(other.to_string()),
            })?;
        let response = if auth.user_id == target {
            user.into()
        } else {
            GetUserResponse {
                user_id: user.user_id.to_string(),
                name: user.public_name(),
                subject_ids: Vec::new(),
                attributes: user.public_attributes(),
            }
        };
        return Ok((StatusCode::OK, Json(response)));
    }

    let user = drive(
        GetUserOperation::new(GetUserInput {
            auth_context: auth,
            self_realm_id: realm_id,
            user_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        aruna_operations::users::get_user::GetUserError::Unauthorized => ServerError::Forbidden,
        aruna_operations::users::get_user::GetUserError::UserNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(user.into())))
}

#[utoipa::path(
    patch,
    path = "/access/users/{id}",
    tag = "access/users",
    summary = "Update a user of this realm by id",
    description = r#"Updates one user's display name and attributes and returns the updated document.

**Authentication**: realm bearer token. Updating the caller's own user needs no further grant but
refuses a path-restricted token; updating anybody else requires WRITE on that user's administration
path and additionally passes the realm request policies.

**Behavior**
- Fields left out change nothing, and removals are applied before sets, so a key named in both ends
  up set to the new value.
- The write is durable here and reaches the other realm nodes through document sync.

**Limits** (the same as for the self-service profile update)
- The name is trimmed and must be 1 to 256 characters.
- An attribute key is ASCII letters, digits, dot, underscore, hyphen or colon of at most 128 bytes.
- An attribute value is at most 4096 bytes and carries no control characters.
- A user holds at most 128 attributes."#,
    params(("id" = String, Path, description = "User id in the form `<ulid>@<realm>` of the user to update; the caller's own id for a self-service update")),
    request_body(
        content = UpdateUserRequest,
        description = "Optional new display name, attributes to set and attribute keys to remove. Send only what changes.",
        example = json!({
            "name": "Alice Example",
            "set_attributes": {
                "affiliation": "Example University"
            },
            "remove_attributes": ["department"]
        })
    ),
    responses(
        (
            status = 200,
            description = "The user document after the update",
            body = GetUserResponse,
            example = json!({
                "user_id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Alice Example",
                "subject_ids": ["YXJ1bmEtZXhhbXBsZS1vaWRjLXN1YmplY3QtMDAwMDA"],
                "attributes": {
                    "email": "user@example.test",
                    "affiliation": "Example University"
                }
            })
        ),
        (status = 400, description = "The id is not a user id, the name is out of range, or an attribute key or value is rejected", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, a path-restricted token attempted a self-update, or the caller lacks WRITE on that user's administration path", body = ErrorResponse),
        (status = 404, description = "This node holds no user with that id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn update_user(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(user_id): Path<String>,
    Json(request): Json<UpdateUserRequest>,
) -> ServerResult<(StatusCode, Json<GetUserResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    // A non-self update is a privileged write; run the realm policy and RBAC
    // boundary at the route before the operation repeats it as defense in depth.
    let target_user_id = UserId::from_string(&user_id).map_err(|_| ServerError::BadRequest)?;
    if auth.user_id != target_user_id {
        ensure_permission(
            &state,
            &auth,
            format!("/{realm_id}/admin/u/{target_user_id}"),
            Permission::WRITE,
        )
        .await?;
    }

    let user = drive(
        UpdateUserOperation::new(UpdateUserInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            auth_context: auth,
            self_realm_id: realm_id,
            user_id,
            name: request.name,
            set_attributes: request.set_attributes,
            remove_attributes: request.remove_attributes,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        aruna_operations::users::update_user::UpdateUserError::Unauthorized => {
            ServerError::Forbidden
        }
        aruna_operations::users::update_user::UpdateUserError::UserNotFound => {
            ServerError::NotFound
        }
        aruna_operations::users::update_user::UpdateUserError::InvalidUserName
        | aruna_operations::users::update_user::UpdateUserError::InvalidAttributeKey(_)
        | aruna_operations::users::update_user::UpdateUserError::InvalidAttributeValue(_)
        | aruna_operations::users::update_user::UpdateUserError::TooManyAttributes
        | aruna_operations::users::update_user::UpdateUserError::ConversionError(_) => {
            ServerError::BadRequest
        }
        aruna_operations::users::update_user::UpdateUserError::AuthorizationError(_) => {
            ServerError::Forbidden
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(user.into())))
}

/// What an outstanding enrollment reads as. An unclaimed secret past its expiry
/// is dead whether or not a later request has pruned it, so it must never be
/// reported as an enrollment still in flight.
fn enrollment_status(claimed: bool, expires_at: u64, now: u64) -> &'static str {
    match (claimed, expires_at <= now) {
        (true, _) => "claimed",
        (false, true) => "expired",
        (false, false) => "pending",
    }
}

/// The caller's devices, enrolled and still enrolling. A device's owner lives
/// in its membership kind, so the realm configuration is the authority here and
/// an outstanding enrollment secret only shows what has not landed yet.
async fn owned_devices(
    state: &Arc<ServerState>,
    owner: aruna_core::UserId,
) -> ServerResult<Vec<UserDeviceResponse>> {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        GetConfigError::DocumentNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    let mut devices = config
        .nodes
        .iter()
        .filter(|node| node.kind.owner() == Some(owner))
        .map(|node| UserDeviceResponse {
            id: node.node_id.clone(),
            node_id: Some(node.node_id.clone()),
            enrollment_id: None,
            status: "enrolled".to_string(),
            expires_at: None,
        })
        .collect::<Vec<_>>();

    let secrets = drive(ListSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let now = aruna_core::time::unix_timestamp_secs();
    for entry in secrets {
        if entry.record.mode.owner() != Some(owner) {
            continue;
        }
        let claimed = entry.state.claimed_node_id().map(str::to_string);
        if claimed
            .as_ref()
            .is_some_and(|node_id| devices.iter().any(|device| &device.id == node_id))
        {
            continue;
        }
        devices.push(UserDeviceResponse {
            id: entry.record.enrollment_id.to_string(),
            node_id: claimed.clone(),
            enrollment_id: Some(entry.record.enrollment_id.to_string()),
            status: enrollment_status(claimed.is_some(), entry.record.expires_at, now).to_string(),
            expires_at: Some(entry.record.expires_at),
        });
    }

    Ok(devices)
}

#[utoipa::path(
    get,
    path = "/access/users/me/devices",
    tag = "access/users",
    summary = "List the calling user's devices",
    description = r#"Lists the devices the calling user has enrolled, plus the enrollments still in flight.

**Authentication**: realm bearer token. It always lists the caller's own devices and takes no user
id, so it grants no view of anybody else's.

**Behavior**
- An enrolled device is a realm member of kind `User` owned by the caller; it reads `enrolled` and
  is addressed by its node id.
- An enrollment whose secret is still outstanding reads `pending`, or `claimed` once a device has
  redeemed the secret but the realm configuration has not caught up; it is addressed by its
  enrollment id.
- An unclaimed enrollment past its `expires_at` reads `expired` and stays listed until a later mint
  or admin listing prunes it.
- An enrollment is dropped as soon as the device it claimed appears as a member, so one device is
  listed once.
- The realm configuration is the authority on ownership while outstanding secrets are this node's
  own state, so a device enrolled elsewhere appears once that configuration arrives here."#,
    responses(
        (
            status = 200,
            description = "The caller's enrolled devices and in-flight enrollments",
            body = UserDevicesResponse,
            example = json!({
                "devices": [
                    {
                        "id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "enrollment_id": null,
                        "status": "enrolled",
                        "expires_at": null
                    },
                    {
                        "id": "01JABCDEF0123456789ABCDEFG",
                        "node_id": null,
                        "enrollment_id": "01JABCDEF0123456789ABCDEFG",
                        "status": "pending",
                        "expires_at": 1775748191
                    }
                ]
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_user_devices(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<UserDevicesResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let devices = owned_devices(&state, auth.user_id).await?;
    Ok((StatusCode::OK, Json(UserDevicesResponse { devices })))
}

#[utoipa::path(
    delete,
    path = "/access/users/me/devices/{id}",
    tag = "access/users",
    summary = "Revoke one of the calling user's devices",
    description = r#"Revokes a device enrollment of the calling user, making its secret unredeemable from here on.

**Authentication**: realm bearer token. Only a device owned by the caller can be revoked; a device
owned by anybody else answers 404 rather than admitting it exists.

**Behavior**
- `id` is what `GET /access/users/me/devices` reported: an enrollment id while the enrollment is
  still in flight, or a node id once the device has joined.
- Revoking an in-flight enrollment deletes the enrollment record, so the secret can no longer be
  redeemed. It does not reach back into a completed enrollment.
- Evicting a device that already joined drops it from the realm configuration and retires the secret
  it redeemed. The change replicates like any other configuration change, and each node closes the
  device's open connections when it applies the new membership.
- Eviction is a realm configuration change, so a management node serves it and every other node
  relays the call to one."#,
    params(("id" = String, Path, description = "Enrollment id or node id of the device, as reported by `GET /access/users/me/devices`")),
    responses(
        (status = 204, description = "Device enrollment revoked, or the device evicted from the realm"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No device of the calling user carries this id, including one an earlier call already revoked", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Called on a node that is not a management node and no management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn revoke_user_device(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(device_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let device = owned_devices(&state, auth.user_id)
        .await?
        .into_iter()
        .find(|device| device.id == device_id)
        .ok_or(ServerError::NotFound)?;
    if let Some(enrollment_id) = device.enrollment_id {
        let enrollment_id = Ulid::from_string(&enrollment_id).map_err(|_| ServerError::NotFound)?;
        delete_enrollment(&state, enrollment_id).await?;
        return Ok(StatusCode::NO_CONTENT);
    }

    let node_id = aruna_core::NodeId::from_str(&device.id).map_err(|_| ServerError::NotFound)?;
    evict_node(&state, &auth, node_id, DeviceEvictionScope::Owner).await?;

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    delete,
    path = "/access/devices/{node_id}",
    tag = "access/devices",
    summary = "Evict any enrolled device",
    description = r#"Evicts one enrolled user device from the realm on behalf of the realm's administration.

**Authentication**: realm bearer token with WRITE on the realm's onboarding administration path, the
same grant the onboarding administration routes carry, so the realm request policies constrain it
too. A management node serves it, and every other node relays the call to one.

**Behavior**
- The device is dropped from the realm configuration and the onboarding secret it redeemed is
  retired, so the eviction cannot be undone by replaying that secret.
- The change replicates like any other configuration change, and each node closes the device's open
  connections when it applies the new membership.
- Only an enrolled device is reachable here: a management or server node is not a device, so this
  route can never remove realm infrastructure.
- The owner's own `DELETE /access/users/me/devices/{id}` stays available and unchanged."#,
    params(("node_id" = String, Path, description = "Node id of the enrolled device to evict, as the realm configuration lists it")),
    responses(
        (status = 204, description = "Device evicted from the realm"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller lacks WRITE on the realm's onboarding administration path", body = ErrorResponse),
        (status = 404, description = "No enrolled device carries this node id, including one an earlier call already evicted", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Called on a node that is not a management node and no management node was reachable; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn evict_device(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(node_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = authorize_onboarding_admin(&state, auth).await?;
    let node_id = aruna_core::NodeId::from_str(&node_id).map_err(|_| ServerError::NotFound)?;
    evict_node(&state, &auth, node_id, DeviceEvictionScope::RealmAdmin).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// Retires the redeemed secret and drops the device from the membership. The
/// secret goes first: it would otherwise resurface as an in-flight enrollment
/// once the device is no longer listed as a member.
async fn evict_node(
    state: &Arc<ServerState>,
    auth: &AuthContext,
    node_id: aruna_core::NodeId,
    scope: DeviceEvictionScope,
) -> ServerResult<()> {
    if let Some(enrollment_id) = claimed_enrollment(state, &node_id.to_string()).await? {
        delete_enrollment(state, enrollment_id).await?;
    }
    drive(
        RemoveNodeOperation::new(RemoveNodeConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            node_id,
            scope,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        RemoveNodeError::DeviceNotFound { .. } | RemoveNodeError::RealmConfigNotFound => {
            ServerError::NotFound
        }
        RemoveNodeError::NotManagementNode => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(())
}

/// Enrollment whose secret this node claimed, so an eviction can retire it.
async fn claimed_enrollment(state: &Arc<ServerState>, node_id: &str) -> ServerResult<Option<Ulid>> {
    let secrets = drive(ListSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(secrets
        .into_iter()
        .find(|entry| entry.state.claimed_node_id() == Some(node_id))
        .map(|entry| entry.record.enrollment_id))
}

async fn delete_enrollment(state: &Arc<ServerState>, enrollment_id: Ulid) -> ServerResult<()> {
    drive(
        DeleteSecretOperation::new(DeleteSecretInput { enrollment_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        DeleteSecretError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(())
}

#[cfg(test)]
#[path = "users_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "users_resolve_tests.rs"]
mod resolve_tests;

#[cfg(test)]
#[path = "users_device_tests.rs"]
mod device_tests;
