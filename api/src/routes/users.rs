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
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::delete_onboarding_secret::{
    DeleteOnboardingSecretError, DeleteOnboardingSecretInput, DeleteOnboardingSecretOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::ensure_canonical_user_token_subject::{
    EnsureCanonicalUserTokenSubjectError, EnsureCanonicalUserTokenSubjectOperation,
};
use aruna_operations::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::get_oidc_user::{GetOidcUserInput, GetOidcUserOperation};
use aruna_operations::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use aruna_operations::get_user::{GetUserInput, GetUserOperation};
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::list_groups::ListGroupOperation;
use aruna_operations::list_onboarding_secrets::ListOnboardingSecretsOperation;
use aruna_operations::list_users::{ListUsersInput, ListUsersOperation};
use aruna_operations::read_realm_authorization::{
    ReadRealmAuthorizationError, ReadRealmAuthorizationOperation,
};
use aruna_operations::read_user_document::{ReadUserDocumentError, ReadUserDocumentOperation};
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use aruna_operations::remove_device_node::{
    DeviceEvictionScope, RemoveDeviceNodeConfig, RemoveDeviceNodeError, RemoveDeviceNodeOperation,
};
use aruna_operations::resolve_users::{ResolveUsersInput, ResolveUsersOperation};
use aruna_operations::search_users::{SearchUsersInput, SearchUsersOperation};
use aruna_operations::session::{CreateSessionConfig, CreateSessionOperation};
use aruna_operations::update_user::{UpdateUserInput, UpdateUserOperation};
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
    tags((name = "users", description = "User operations"))
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
    /// Scholarly attributes only; sensitive keys such as email are excluded.
    pub attributes: HashMap<String, String>,
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

pub type PatchUserInfoRequest = UpdateUserRequest;

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

async fn issue_portal_session(
    state: &Arc<ServerState>,
    user_id: UserId,
    expiry: u64,
) -> ServerResult<String> {
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now_timestamp(),
            expiry,
            user_id,
            realm_id: state.get_realm_id(),
            node_capabilities: state.node_capabilities().clone(),
            kind: SessionKind::Portal,
            label: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(created.token.expose().to_string())
}

async fn ensure_canonical_user_token_subject(
    state: &Arc<ServerState>,
    user_id: UserId,
) -> ServerResult<()> {
    drive(
        EnsureCanonicalUserTokenSubjectOperation::new(user_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_canonical_subject_error)
}

async fn read_current_user(state: &ServerState, user_id: UserId) -> ServerResult<User> {
    drive(ReadUserDocumentOperation::new(user_id), &state.get_ctx())
        .await
        .map_err(map_read_user_document_error)
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

fn map_canonical_subject_error(error: EnsureCanonicalUserTokenSubjectError) -> ServerError {
    match error {
        EnsureCanonicalUserTokenSubjectError::Unauthorized => ServerError::Unauthorized,
        EnsureCanonicalUserTokenSubjectError::Forbidden => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_read_user_document_error(error: ReadUserDocumentError) -> ServerError {
    match error {
        ReadUserDocumentError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
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
    auth: AuthContext,
) -> ServerResult<GetUserInfoResponse> {
    if auth.realm_id != state.get_realm_id() || auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    let user = read_current_user(state, auth.user_id).await?;
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
        session: None,
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
    if onboarding_secret.realm_id != state.get_realm_id() {
        return Err(ServerError::Unauthorized);
    }
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
    if inspected.purpose != OnboardingPurpose::InitialAdministrator {
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
    summary = "Register the calling OIDC identity",
    description = r#"Registers the subject of the presented OIDC token as a user of this realm.

**Authentication**: an OIDC bearer token from a configured issuer, not an Aruna access token. Any
holder of a valid one registers themselves; an onboarding secret in the body additionally claims
the initial realm administrator role.

**Behavior**
- Two callers are served: a user registering themselves, and an operator bootstrapping the first
  realm administrator.
- Leaving the onboarding secret out is get or create, so a subject that already has a user gets
  the existing user back unchanged, still with 201.
- An onboarding secret must have been issued for this realm with the initial administrator
  purpose, it is consumed single use, and the registered user then claims that role.
- The registered identity is always the subject of the presented token, never a user id chosen by
  the caller, and the display name comes from the token.
- The user document is written on the node that serves the request and replicates to the other
  realm nodes asynchronously, so it may not be visible elsewhere immediately.
- No access token is returned here: exchange the same OIDC token at `GET /users/token`.

**Errors**: an onboarding secret issued for another purpose is refused with 403, while one that is
unknown, expired, already claimed or issued for another realm is refused with 401."#,
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
            description = "The user of this OIDC subject, either newly created or already present from an earlier registration",
            body = RegisterUserResponse,
            example = json!({
                "id": "01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Alice Example"
            })
        ),
        (status = 400, description = "The request body is not valid JSON for this operation", body = ErrorResponse),
        (status = 401, description = "No OIDC bearer token was presented, it failed validation against the configured issuers, or the onboarding secret is unknown, expired, already claimed or issued for another realm", body = ErrorResponse),
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
    summary = "Issue an access token for the calling identity",
    description = r#"Mints a realm access token for the calling identity.

**Authentication**: an Aruna access token without path restrictions, which refreshes itself, or an
OIDC token from a configured issuer whose subject has been registered at `POST /users/register`.
Self-scoped: the token is always minted for the caller's own identity and can never be requested
on behalf of somebody else.

**Behavior**
- The issued token is a realm bearer credential valid for 24 hours.
- The issued token is recorded on this node as a listable `portal` session.
- It is returned only in this response and is not retrievable afterwards, so a lost token has to
  be reissued here.

**Errors**: a path-restricted (delegated) token is refused with 403, as is a token whose subject is
an alias rather than the canonical user of that OIDC subject."#,
    responses(
        (
            status = 200,
            description = "A freshly issued access token for the caller, valid for 24 hours and shown only here",
            body = GetTokenResponse,
            example = json!({
                "token": "<aruna-access-token>"
            })
        ),
        (status = 400, description = "Not produced by this operation; a request that cannot be authenticated is answered with 401 or 403 instead", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, the OIDC token failed validation, or this node knows no user for the presented identity", body = ErrorResponse),
        (status = 403, description = "The presented access token carries path restrictions, or its subject is an alias of the canonical user of that OIDC subject", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_token(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<GetTokenResponse>)> {
    let user_id = match auth {
        Some(aruna_ctx) => {
            if aruna_ctx.path_restrictions.is_some() {
                return Err(ServerError::Forbidden);
            }
            ensure_canonical_user_token_subject(&state, aruna_ctx.user_id).await?;
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

    let expiry = now_timestamp()
        .checked_add(USER_TOKEN_EXPIRY_SECONDS)
        .ok_or_else(|| ServerError::InternalError("token expiry overflow".to_string()))?;
    let token = issue_portal_session(&state, user_id, expiry).await?;

    Ok((StatusCode::OK, Json(GetTokenResponse { token })))
}

#[utoipa::path(
    get,
    path = "/users/info",
    tag = "users",
    summary = "Get the calling user's profile, roles and preferences",
    description = r#"Describes the calling user: profile, realm roles, group memberships and UI preferences.

**Authentication**: realm bearer token that carries no path restrictions; a delegated token, or a
token issued by another realm, is refused with 403. Self-scoped: it always describes the caller
and takes no user id.

**Behavior**
- The response combines the caller's user document, the realm roles whose assignment list contains
  the caller, and one entry for every group known to this node whose roles list the caller.
- The preferences are derived from the caller's `ui.theme`, `ui.preferred_profile_path` and
  `ui.favourite_metadata_ids` attributes, where the favourites attribute is a comma separated
  list.
- Group membership is collected from the groups this node holds, so a group that has not
  replicated here yet is missing.

**Errors**: 404 means this node holds no user document for the caller, which can happen while a
registration made on another node is still replicating."#,
    responses(
        (
            status = 200,
            description = "The caller's user document, realm roles, group memberships and UI preferences",
            body = GetUserInfoResponse,
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
                    "theme": "dark"
                }
            })
        ),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "This node holds no user document for the calling identity", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<GetUserInfoResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    Ok((
        StatusCode::OK,
        Json(build_user_info_response(&state, auth).await?),
    ))
}

#[utoipa::path(
    patch,
    path = "/users/info",
    tag = "users",
    summary = "Update the calling user's profile",
    description = r#"Updates the calling user's display name and attributes and returns the refreshed profile.

**Authentication**: realm bearer token that carries no path restrictions; a delegated token, or a
token issued by another realm, is refused with 403. Self-scoped: it always writes the caller's own
user document and takes no user id.

**Behavior**
- Fields left out change nothing.
- Removals are applied before sets, so a key named in both ends up set to the new value.
- UI preferences are ordinary attributes: `ui.theme`, `ui.preferred_profile_path` and
  `ui.favourite_metadata_ids` as a comma separated list.
- The write is durable on the node that answers and replicates to the other realm nodes
  asynchronously.
- The response is the caller's refreshed profile in the same shape as `GET /users/info`.

**Limits**
- The name is trimmed and must be 1 to 256 characters.
- An attribute key is ASCII letters, digits, dot, underscore, hyphen or colon of at most 128
  bytes.
- An attribute value is at most 4096 bytes and carries no control characters.
- A user holds at most 128 attributes."#,
    request_body(
        content = PatchUserInfoRequest,
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
            description = "The caller's profile after the update, with realm roles, group memberships and the recomputed preferences",
            body = GetUserInfoResponse,
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
                    "theme": "dark"
                }
            })
        ),
        (status = 400, description = "The name is empty or longer than 256 characters, or an attribute key or value is rejected, or the user would hold more than 128 attributes", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm or carries path restrictions", body = ErrorResponse),
        (status = 404, description = "This node holds no user document for the calling identity", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn patch_user_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<PatchUserInfoRequest>,
) -> ServerResult<(StatusCode, Json<GetUserInfoResponse>)> {
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
        aruna_operations::update_user::UpdateUserError::Unauthorized => ServerError::Forbidden,
        aruna_operations::update_user::UpdateUserError::UserNotFound => ServerError::NotFound,
        aruna_operations::update_user::UpdateUserError::InvalidUserName
        | aruna_operations::update_user::UpdateUserError::InvalidAttributeKey(_)
        | aruna_operations::update_user::UpdateUserError::InvalidAttributeValue(_)
        | aruna_operations::update_user::UpdateUserError::TooManyAttributes
        | aruna_operations::update_user::UpdateUserError::ConversionError(_) => {
            ServerError::BadRequest
        }
        aruna_operations::update_user::UpdateUserError::AuthorizationError(_) => {
            ServerError::Forbidden
        }
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((
        StatusCode::OK,
        Json(build_user_info_response(&state, auth).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/users",
    tag = "users",
    summary = "List the users of this realm",
    description = r#"Pages this realm's user documents in user id order.

**Authentication**: realm bearer token with READ on the realm's user administration path, so an
ordinary member is refused with 403; the realm request policies are evaluated as well and may deny
a read that the role grant alone would allow.

**Behavior**
- Only users of this realm are listed, from the replica held by the node that serves the request,
  so a user registered on another node appears once replication has caught up.
- Every entry is the full user document including its attributes.
- Pagination is cursor based: `next_start_after` repeats the last returned user id, and its
  absence means the end of the listing was reached.

**Limits**
- `limit` defaults to 100 and is clamped into 1 to 1000."#,
    params(
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 100 and is clamped into 1 to 1000"),
        ("start_after" = Option<String>, Query, description = "Exclusive cursor: the `next_start_after` of the previous page; omit it to start at the first user")
    ),
    responses(
        (
            status = 200,
            description = "One page of this realm's users held by this node, with the cursor for the next page",
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
        (status = 400, description = "The start_after cursor is not a user id", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, or the caller has no read access on the realm's user administration path", body = ErrorResponse)
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
    summary = "Search this realm's users by name or email",
    description = r#"Pages this realm's users whose display name or email attribute contains the query.

**Authentication**: realm bearer token with READ on the realm's user administration path, the same
grant as the full listing, so an ordinary member is refused with 403.

**Behavior**
- The query is trimmed and matched case insensitively as a substring of the display name and of
  the email attribute, over the users of this realm held by the node that serves the request; a
  user registered elsewhere is found once replication has caught up.
- A result carries only the user id and the display name, never attributes.
- Pagination is cursor based: `next_start_after` repeats the last returned user id, and its
  absence means the scan reached the end of the realm's users rather than that no further match
  exists on a later page.

**Limits**
- `q` must hold at least 2 characters after trimming.
- `limit` defaults to 20 and is clamped into 1 to 20."#,
    params(
        ("q" = String, Query, description = "Substring matched case insensitively against the user name and the email attribute; at least 2 characters"),
        ("limit" = Option<usize>, Query, description = "Page size; defaults to 20 and is clamped into 1 to 20"),
        ("start_after" = Option<String>, Query, description = "Exclusive cursor: the `next_start_after` of the previous page; omit it to start at the first user")
    ),
    responses(
        (
            status = 200,
            description = "One page of matching users, reduced to user id and display name, with the cursor for the next page",
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
        (status = 400, description = "The query is shorter than 2 characters after trimming, or the start_after cursor is not a user id", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, or the caller has no read access on the realm's user administration path", body = ErrorResponse)
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
    ensure_permission(
        &state,
        &auth,
        format!("/{realm_id}/admin/u/**"),
        Permission::READ,
    )
    .await?;

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
    path = "/users/resolve",
    tag = "users",
    summary = "Resolve user ids to directory entries",
    description = r#"Resolves a batch of user ids to directory entries held by this node.

**Authentication**: realm bearer token with READ on the realm's user administration path, so an
ordinary member is refused with 403.

**Behavior**
- The lookup runs against the replica held by the node that serves the request.
- Duplicate ids collapse and ids unknown to this node are dropped silently, so the result may be
  shorter than the request and carries no positional mapping; match the entries by user id.
- Only the directory safe attributes `orcid`, `affiliation` and `department` are exposed, while
  `email` and every other attribute are withheld here.
- The response body is a JSON array, not an object.

**Limits**
- At most 100 user ids per request."#,
    request_body(
        content = ResolveUsersRequest,
        description = "Up to 100 user ids, each in the form ulid@realm-id. Duplicates are collapsed and unknown ids are omitted from the result.",
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
            description = "Directory entries for the ids this node could resolve, with only the safe attributes",
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
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, or the caller has no read access on the realm's user administration path", body = ErrorResponse)
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
    ensure_permission(
        &state,
        &auth,
        format!("/{realm_id}/admin/u/**"),
        Permission::READ,
    )
    .await?;

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
    path = "/users/{id}",
    tag = "users",
    summary = "Get a user of this realm by id",
    description = r#"Returns one user document of this realm as held by the responding node.

**Authentication**: realm bearer token with READ on the administration path of that specific user,
so a caller without the grant is refused with 403 whether or not the user exists and existence
stays hidden; the realm request policies are evaluated as well and may deny a read the role grant
would allow.

**Behavior**
- The reply is served from the replica held by the node that receives the request, so a user
  registered on another node appears once replication has caught up.
- The response is the full user document including its attributes.

**Errors**: 404 is only reachable for a caller that may read that user. A token issued by another
trusted realm is answered with 501, because forwarding a read to the owning realm is not
implemented."#,
    params(("id" = String, Path, description = "User id in the form ulid@realm-id, as returned by the listing and search operations")),
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
        (status = 400, description = "Declared for malformed input; in practice an id that is not a user id is refused by the authorization check with 403 instead", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The caller has no read access on that user's administration path; the same answer is given whether or not the user exists", body = ErrorResponse),
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
    ensure_permission(
        &state,
        &auth,
        format!("/{realm_id}/admin/u/{user_id}"),
        Permission::READ,
    )
    .await?;

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
        aruna_operations::get_user::GetUserError::Unauthorized => ServerError::Forbidden,
        aruna_operations::get_user::GetUserError::UserNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;

    Ok((StatusCode::OK, Json(user.into())))
}

#[utoipa::path(
    patch,
    path = "/users/{id}",
    tag = "users",
    summary = "Update a user of this realm by id",
    description = r#"Updates one user's display name and attributes and returns the updated document.

**Authentication**: realm bearer token; a token issued by another realm is refused with 403.
Updating the caller's own user needs no further grant but refuses a path-restricted (delegated)
token with 403. Updating anybody else requires WRITE on that user's administration path and
additionally passes the realm request policies, which may deny the write even when the role grant
allows it.

**Behavior**
- Fields left out change nothing.
- Removals are applied before sets, so a key named in both ends up set.
- The write is durable on the node that answers and replicates to the other realm nodes
  asynchronously.
- The response is the updated user document.

**Limits** (validation matches the self service profile update)
- The name is trimmed and must be 1 to 256 characters.
- An attribute key is ASCII letters, digits, dot, underscore, hyphen or colon of at most 128
  bytes.
- An attribute value is at most 4096 bytes and carries no control characters.
- A user holds at most 128 attributes."#,
    params(("id" = String, Path, description = "User id in the form ulid@realm-id of the user to update; the caller's own id for a self service update")),
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
        (status = 400, description = "The id is not a user id, the name is empty or longer than 256 characters, or an attribute key or value is rejected", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm, a delegated token attempted a self update, or the caller has no write access on that user's administration path", body = ErrorResponse),
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
        aruna_operations::update_user::UpdateUserError::Unauthorized => ServerError::Forbidden,
        aruna_operations::update_user::UpdateUserError::UserNotFound => ServerError::NotFound,
        aruna_operations::update_user::UpdateUserError::InvalidUserName
        | aruna_operations::update_user::UpdateUserError::InvalidAttributeKey(_)
        | aruna_operations::update_user::UpdateUserError::InvalidAttributeValue(_)
        | aruna_operations::update_user::UpdateUserError::TooManyAttributes
        | aruna_operations::update_user::UpdateUserError::ConversionError(_) => {
            ServerError::BadRequest
        }
        aruna_operations::update_user::UpdateUserError::AuthorizationError(_) => {
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
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        GetRealmConfigError::DocumentNotFound => ServerError::NotFound,
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

    let secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    let now = aruna_core::util::unix_timestamp_secs();
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
    path = "/users/me/devices",
    tag = "users",
    summary = "List the calling user's devices",
    description = r#"Lists the devices this user has enrolled, plus the device enrollments still in flight.

**Authentication**: realm bearer token. Self-scoped: it always lists the caller's own devices and
takes no user id, so it grants no view of anybody else's.

**Behavior**
- An enrolled device is a realm member of kind `User` whose owner is the caller; it reads
  `enrolled` and is addressed by its node id.
- An enrollment whose secret is still outstanding reads `pending`, or `claimed` once a device has
  redeemed the secret but the realm configuration has not caught up; it is addressed by its
  enrollment id and carries the secret's expiry.
- An unclaimed enrollment whose `expires_at` has passed reads `expired`. It stays listed until a
  later mint or admin listing prunes it, but it is never reported as still in flight.
- An enrollment is dropped from the list as soon as the device it claimed appears as a member, so
  one device is listed once.
- The realm configuration is the authority on ownership, and the outstanding secrets are this
  node's local state, so a device enrolled elsewhere appears once that configuration replicates
  here.

**Errors**: 404 means this node holds no configuration document for its realm yet."#,
    responses(
        (
            status = 200,
            description = "The caller's enrolled devices and in-flight device enrollments",
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
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm", body = ErrorResponse),
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
    path = "/users/me/devices/{id}",
    tag = "users",
    summary = "Revoke one of the calling user's devices",
    description = r#"Revokes a device enrollment of the calling user, making its secret unredeemable from here on.

**Authentication**: realm bearer token. Self-scoped: only a device owned by the caller can be
revoked, and a device owned by anybody else answers 404 rather than admitting it exists.

**Behavior**
- `id` is what `GET /users/me/devices` reported: an enrollment id while the enrollment is still in
  flight, or a node id once the device has joined.
- Revoking an in-flight enrollment deletes the enrollment record on the management node holding it,
  so the secret can no longer be redeemed. It does not reach back into a completed enrollment.
- Evicting a device that already joined drops it from the realm configuration and retires the
  secret it redeemed. The eviction replicates like any other configuration change, and each node
  closes the device's open connections when it applies the new membership.
- The eviction is a realm configuration change, so it is served by a management node; a call to any
  other node is relayed to one, because its peers would refuse the event.

**Errors**: an id that is neither an enrollment id nor a node id of a device owned by the caller
answers 404, which is also what a caller sees after an earlier revoke."#,
    params(("id" = String, Path, description = "Enrollment id or node id of the device, as reported by GET /users/me/devices")),
    responses(
        (status = 204, description = "Device enrollment revoked, or the device evicted from the realm; no response body"),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The token was issued by another realm", body = ErrorResponse),
        (status = 404, description = "No device of the calling user carries this id", body = ErrorResponse),
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
    path = "/admin/devices/{node_id}",
    tag = "users",
    summary = "Evict any enrolled device as a realm admin",
    description = r#"Evicts one enrolled user device from the realm on behalf of the realm's administration.

**Authentication**: realm bearer token with WRITE on the realm's onboarding administration path. A
management node serves it, and every other node relays the call to one. This is the same
authorization the onboarding administration routes carry, so the realm request policies constrain
it too.

**Behavior**
- `node_id` is the device's node id, which `GET /users/me/devices` reports to its owner and the
  realm configuration lists as a node of kind `User`.
- The device is dropped from the realm configuration and the enrollment secret it redeemed is
  retired, so the eviction cannot be undone by replaying that secret.
- The eviction replicates like any other configuration change, and each node closes the device's
  open connections when it applies the new membership.
- Only an enrolled device is reachable here: a management or server node is not a device and
  answers 404, so this route can never remove realm infrastructure.
- The owner's own `DELETE /users/me/devices/{id}` is unchanged and stays self-scoped; this route
  neither replaces nor requires it.

**Errors**: a node id that names no enrolled device answers 404, which is also what a caller sees
after an earlier eviction."#,
    params(("node_id" = String, Path, description = "Node id of the enrolled device to evict")),
    responses(
        (status = 204, description = "Device evicted from the realm; no response body"),
        (status = 401, description = "No bearer token was presented, or the presented token failed validation", body = ErrorResponse),
        (status = 403, description = "The caller is not a realm onboarding admin", body = ErrorResponse),
        (status = 404, description = "No enrolled device carries this node id", body = ErrorResponse),
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
        RemoveDeviceNodeOperation::new(RemoveDeviceNodeConfig {
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
        RemoveDeviceNodeError::DeviceNotFound { .. }
        | RemoveDeviceNodeError::RealmConfigNotFound => ServerError::NotFound,
        RemoveDeviceNodeError::NotManagementNode => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(())
}

/// Enrollment whose secret this node claimed, so an eviction can retire it.
async fn claimed_enrollment(state: &Arc<ServerState>, node_id: &str) -> ServerResult<Option<Ulid>> {
    let secrets = drive(ListOnboardingSecretsOperation::new(), &state.get_ctx())
        .await
        .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(secrets
        .into_iter()
        .find(|entry| entry.state.claimed_node_id() == Some(node_id))
        .map(|entry| entry.record.enrollment_id))
}

async fn delete_enrollment(state: &Arc<ServerState>, enrollment_id: Ulid) -> ServerResult<()> {
    drive(
        DeleteOnboardingSecretOperation::new(DeleteOnboardingSecretInput { enrollment_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| match err {
        DeleteOnboardingSecretError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{GetTokenResponse, RegisterUserRequest, RegisterUserResponse, enrollment_status};
    use crate::auth::OidcValidator;
    use crate::error::ErrorResponse;
    use crate::server::Server;
    use crate::server::ServerConfig;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
    use aruna_core::onboarding::{
        OnboardingMode, OnboardingPurpose, OnboardingSecret, OnboardingSecretRecord,
    };
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

    // An unclaimed enrollment past its expiry is dead: reporting it as pending
    // would show an enrollment in flight that can never complete.
    #[test]
    fn reports_expired_enrollment() {
        assert_eq!(enrollment_status(false, 100, 99), "pending");
        assert_eq!(enrollment_status(false, 100, 100), "expired");
        assert_eq!(enrollment_status(false, 100, 101), "expired");
        assert_eq!(enrollment_status(true, 100, 101), "claimed");
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
            jti: Ulid::generate().to_string(),
            sid: None,
            session_kind: None,
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
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
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
            compute_handle: None,
        });
        initialize_net_incoming(driver_ctx.clone());
        initialize_task_incoming(
            driver_ctx.clone(),
            task_handle,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await;

        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = net_handle.node_id();
        let realm_admin_id = UserId::local(Ulid::generate(), realm_id);

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
                aruna_operations::jobs::runtime::JobsRuntime::new(),
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
            enrollment_id: Ulid::generate(),
            secret: [7u8; 32],
            mode: OnboardingMode::Server,
            realm_id: node.realm_id,
            purpose: OnboardingPurpose::InitialAdministrator,
        };
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: OnboardingSecretRecord {
                    enrollment_id: onboarding_secret.enrollment_id,
                    secret_hash: onboarding_secret.secret_hash(),
                    mode: OnboardingMode::Server,
                    purpose: OnboardingPurpose::InitialAdministrator,
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
        let signing_key = generate_signing_key();
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
        let signing_key = generate_signing_key();
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
        let signing_key = generate_signing_key();
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
        let signing_key = generate_signing_key();
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

        let missing_user_id = UserId::local(Ulid::generate(), node.realm_id);
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
    async fn policy_denies_update() {
        // A realm deny-writes policy must block a non-self update at the route,
        // even though the admin holds the RBAC write the operation would accept.
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = generate_signing_key();
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (target, _target_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-target",
            "Target Bob",
            None,
        )
        .await;
        let admin = aruna_core::structs::AuthContext {
            user_id: node.realm_admin_id,
            realm_id: node.realm_id,
            path_restrictions: None,
            session: None,
        };

        let rename = |name: &str| super::UpdateUserRequest {
            name: Some(name.to_string()),
            set_attributes: Default::default(),
            remove_attributes: Default::default(),
        };

        let allowed = super::update_user(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin.clone())),
            axum::extract::Path(target.id.clone()),
            axum::Json(rename("FirstRename")),
        )
        .await;
        assert!(matches!(allowed, Ok((StatusCode::OK, _))));

        install_deny_policy(&node, "permission == 'write'").await;

        let denied = super::update_user(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin.clone())),
            axum::extract::Path(target.id.clone()),
            axum::Json(rename("SecondRename")),
        )
        .await;
        assert!(matches!(denied, Err(crate::error::ServerError::Forbidden)));

        let fetched = super::get_user(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin)),
            axum::extract::Path(target.id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fetched.1.0.name, "FirstRename");

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    async fn install_deny_policy(node: &TestNode, expression: &str) {
        let mut config = read_realm_config(node.context.as_ref(), &node.realm_id).await;
        config
            .request_policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "deny".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: expression.to_string(),
                enabled: true,
            });
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: node.realm_admin_id,
            realm_id: node.realm_id,
        };
        let bytes = config.to_bytes(&actor).unwrap();
        let _ = node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(node.realm_id.as_bytes().to_vec()),
                value: ByteView::from(bytes),
                txn_id: None,
            }))
            .await;
    }

    #[tokio::test]
    async fn policy_denies_reads() {
        // A realm deny-reads policy must block both admin user reads at the
        // route, though RBAC alone would let the realm admin through.
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = generate_signing_key();
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let (target, _target_token) = register_via_oidc(
            &node,
            issuer,
            kid,
            &signing_key,
            "subject-read",
            "Target Bob",
            None,
        )
        .await;
        let admin = aruna_core::structs::AuthContext {
            user_id: node.realm_admin_id,
            realm_id: node.realm_id,
            path_restrictions: None,
            session: None,
        };
        let query = || {
            axum::extract::Query(super::ListUsersQuery {
                limit: None,
                start_after: None,
            })
        };

        let fetched = super::get_user(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin.clone())),
            axum::extract::Path(target.id.clone()),
        )
        .await
        .expect("the realm admin may read a user");
        assert_eq!(fetched.1.0.name, "Target Bob");
        let listed = super::list_users(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin.clone())),
            query(),
        )
        .await
        .expect("the realm admin may list users");
        assert!(!listed.1.0.users.is_empty());

        install_deny_policy(&node, "permission == 'read'").await;

        let denied_get = super::get_user(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin.clone())),
            axum::extract::Path(target.id.clone()),
        )
        .await;
        assert!(matches!(
            denied_get,
            Err(crate::error::ServerError::Forbidden)
        ));
        let denied_list = super::list_users(
            axum::extract::State(node.state.clone()),
            axum::Extension(Some(admin)),
            query(),
        )
        .await;
        assert!(matches!(
            denied_list,
            Err(crate::error::ServerError::Forbidden)
        ));

        node.server_task.abort();
        node.net.shutdown().await;
        oidc_task.abort();
    }

    #[tokio::test]
    async fn get_user_requires_authentication() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = generate_signing_key();
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let missing_user_id = UserId::local(Ulid::generate(), node.realm_id);
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
        let signing_key = generate_signing_key();
        let (provider, oidc_task) = spawn_oidc_provider(issuer, kid, &signing_key).await;
        let node = spawn_test_node(provider, true).await;

        let foreign_realm_signing_key = generate_signing_key();
        let foreign_realm_id =
            RealmId::from_bytes(foreign_realm_signing_key.verifying_key().to_bytes());
        node.state.add_trusted_realm(foreign_realm_id).await;
        let foreign_user_id = UserId::local(Ulid::generate(), foreign_realm_id);
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
        // Revocation lookup is fail-closed against the issuing realm, so the
        // foreign config must exist for the request to reach the route.
        match node
            .context
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: ByteView::from(foreign_realm_id.as_bytes().to_vec()),
                value: ByteView::from(
                    RealmConfigDocument::default_for_realm(foreign_realm_id, Vec::new())
                        .to_bytes(&Actor {
                            node_id: node.net.node_id(),
                            user_id: foreign_user_id,
                            realm_id: foreign_realm_id,
                        })
                        .unwrap(),
                ),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected foreign realm config write result: {other:?}"),
        }
        let token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time: super::now_timestamp(),
                expiry: None,
                user_id: foreign_user_id,
                realm_id: foreign_realm_id,
                node_capabilities: NodeCapabilities::management_node(foreign_realm_signing_key)
                    .unwrap(),

                session: None,
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
        let signing_key = generate_signing_key();
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
        let signing_key = generate_signing_key();
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
        let signing_key = generate_signing_key();
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

#[cfg(test)]
mod resolve_tests {
    use super::{ResolveUsersRequest, resolve_users};
    use crate::error::ServerError;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{AuthContext, NodeCapabilities, RealmId};
    use aruna_operations::driver::DriverContext;
    use aruna_storage::FjallStorage;
    use axum::extract::State;
    use axum::{Extension, Json};
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn setup_state() -> (Arc<ServerState>, TempDir) {
        let tempdir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
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
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        (state, tempdir)
    }

    fn realm_auth(realm_id: RealmId) -> AuthContext {
        AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    #[tokio::test]
    async fn requires_auth() {
        let (state, _tempdir) = setup_state().await;
        let result = resolve_users(
            State(state),
            Extension(None),
            Json(ResolveUsersRequest { user_ids: vec![] }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Unauthorized)));
    }

    #[tokio::test]
    async fn requires_directory_read() {
        // A realm member without the admin user directory grant may not resolve.
        let (state, _tempdir) = setup_state().await;
        let realm_id = state.get_realm_id();
        let result = resolve_users(
            State(state),
            Extension(Some(realm_auth(realm_id))),
            Json(ResolveUsersRequest { user_ids: vec![] }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn rejects_foreign_realm() {
        let (state, _tempdir) = setup_state().await;
        let foreign = realm_auth(RealmId::from_bytes([7u8; 32]));
        let result = resolve_users(
            State(state),
            Extension(Some(foreign)),
            Json(ResolveUsersRequest { user_ids: vec![] }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn caps_batch_size() {
        let (state, _tempdir) = setup_state().await;
        let realm_id = state.get_realm_id();
        let user_ids = (0..=super::MAX_RESOLVE_USER_IDS)
            .map(|_| UserId::local(Ulid::generate(), realm_id).to_string())
            .collect();
        let result = resolve_users(
            State(state),
            Extension(Some(realm_auth(realm_id))),
            Json(ResolveUsersRequest { user_ids }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_bad_id() {
        let (state, _tempdir) = setup_state().await;
        let realm_id = state.get_realm_id();
        let result = resolve_users(
            State(state),
            Extension(Some(realm_auth(realm_id))),
            Json(ResolveUsersRequest {
                user_ids: vec!["not-a-user-id".to_string()],
            }),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }
}

#[cfg(test)]
mod device_tests {
    use super::{UserDeviceResponse, evict_device, list_user_devices, revoke_user_device};
    use crate::error::ServerError;
    use crate::server_state::ServerState;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::onboarding::{OnboardingMode, OnboardingPurpose, OnboardingSecretRecord};
    use aruna_core::structs::{Actor, AuthContext, NodeCapabilities, RealmId, RealmNodeKind};
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::ensure_realm_config::{
        EnsureRealmConfigConfig, EnsureRealmConfigOperation,
    };
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::{Extension, Json};
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    struct Fixture {
        state: Arc<ServerState>,
        owner: UserId,
        other: UserId,
        admin: UserId,
        _dir: TempDir,
    }

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// One device and one outstanding enrollment per owner, plus a management
    /// node, so every filter the routes apply has something to reject. The realm
    /// and its members are produced by the operations enrollment uses.
    async fn setup_devices() -> Fixture {
        let dir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let realm_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let owner = UserId::local(Ulid::generate(), realm_id);
        let other = UserId::local(Ulid::generate(), realm_id);
        let admin = UserId::local(Ulid::generate(), realm_id);
        let actor = Actor {
            node_id: node(1),
            user_id: UserId::nil(realm_id),
            realm_id,
        };

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "devices".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();
        for (device, device_owner) in [(node(2), owner), (node(3), other)] {
            drive(
                EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
                    actor: actor.clone(),
                    target_node_id: device,
                    target_node_kind: RealmNodeKind::User {
                        owner: device_owner,
                    },
                    default_metadata_replication_factor: 3,
                    realm_description: String::new(),
                    create_if_missing: false,
                    reject_kind_mismatch: true,
                }),
                driver_ctx.as_ref(),
            )
            .await
            .unwrap();
        }

        for secret_owner in [owner, other] {
            drive(
                CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                    record: OnboardingSecretRecord {
                        enrollment_id: Ulid::generate(),
                        secret_hash: Ulid::generate().to_string(),
                        mode: OnboardingMode::User {
                            owner: secret_owner,
                        },
                        purpose: OnboardingPurpose::NodeEnrollment,
                        expires_at: u64::MAX,
                        claimed_node_id: None,
                    },
                }),
                driver_ctx.as_ref(),
            )
            .await
            .unwrap();
        }

        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput {
                actor: Actor {
                    node_id: node(1),
                    user_id: admin,
                    realm_id,
                },
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node(1),
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        Fixture {
            state,
            owner,
            other,
            admin,
            _dir: dir,
        }
    }

    fn auth(user_id: UserId) -> AuthContext {
        AuthContext {
            user_id,
            realm_id: user_id.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    async fn devices(state: &Arc<ServerState>, owner: UserId) -> Vec<UserDeviceResponse> {
        let (_, Json(listed)) =
            list_user_devices(State(state.clone()), Extension(Some(auth(owner))))
                .await
                .unwrap();
        listed.devices
    }

    #[tokio::test]
    async fn lists_owned_devices() {
        let fixture = setup_devices().await;
        let listed = devices(&fixture.state, fixture.owner).await;

        assert_eq!(listed.len(), 2);
        let enrolled = listed
            .iter()
            .find(|device| device.status == "enrolled")
            .expect("the enrolled device");
        assert_eq!(
            enrolled.node_id.as_deref(),
            Some(node(2).to_string()).as_deref()
        );
        assert!(enrolled.enrollment_id.is_none());
        let pending = listed
            .iter()
            .find(|device| device.status == "pending")
            .expect("the outstanding enrollment");
        assert_eq!(pending.enrollment_id.as_deref(), Some(pending.id.as_str()));
        assert!(pending.expires_at.is_some());

        let foreign = devices(&fixture.state, fixture.other).await;
        assert!(
            foreign
                .iter()
                .all(|device| device.id != node(2).to_string())
        );
    }

    #[tokio::test]
    async fn revokes_pending_device() {
        let fixture = setup_devices().await;
        let pending = devices(&fixture.state, fixture.owner)
            .await
            .into_iter()
            .find(|device| device.status == "pending")
            .expect("the outstanding enrollment");

        let status = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(pending.id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
        let listed = devices(&fixture.state, fixture.owner).await;
        assert!(listed.iter().all(|device| device.status == "enrolled"));

        let repeated = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(pending.id),
        )
        .await;
        assert!(matches!(repeated, Err(ServerError::NotFound)));

        // Another owner's outstanding enrollment stays untouched.
        assert!(
            devices(&fixture.state, fixture.other)
                .await
                .iter()
                .any(|device| device.status == "pending")
        );
    }

    #[tokio::test]
    async fn rejects_stranger_device() {
        let fixture = setup_devices().await;

        let foreign = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(node(3).to_string()),
        )
        .await;
        assert!(matches!(foreign, Err(ServerError::NotFound)));

        let anonymous = list_user_devices(State(fixture.state), Extension(None)).await;
        assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
    }

    #[tokio::test]
    async fn evicts_enrolled_device() {
        // Eviction drops the membership itself, so the device stops being a
        // realm peer instead of merely losing an unredeemed secret.
        let fixture = setup_devices().await;

        let status = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(node(2).to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);

        let listed = devices(&fixture.state, fixture.owner).await;
        assert!(listed.iter().all(|device| device.id != node(2).to_string()));

        let repeated = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.owner))),
            Path(node(2).to_string()),
        )
        .await;
        assert!(matches!(repeated, Err(ServerError::NotFound)));

        // Another owner's device keeps its membership.
        assert!(
            devices(&fixture.state, fixture.other)
                .await
                .iter()
                .any(|device| device.id == node(3).to_string())
        );
    }

    #[tokio::test]
    async fn admin_evicts_device() {
        // A realm admin reaches a device it does not own, and the owner path is
        // left as it was for every other device.
        let fixture = setup_devices().await;

        let status = evict_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.admin))),
            Path(node(2).to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(
            devices(&fixture.state, fixture.owner)
                .await
                .iter()
                .all(|device| device.id != node(2).to_string())
        );

        let status = revoke_user_device(
            State(fixture.state.clone()),
            Extension(Some(auth(fixture.other))),
            Path(node(3).to_string()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn refuses_non_admin() {
        // Owning a device is not administering the realm: the admin route
        // refuses a plain member and an anonymous caller, and the device stays.
        let fixture = setup_devices().await;

        assert!(matches!(
            evict_device(
                State(fixture.state.clone()),
                Extension(Some(auth(fixture.owner))),
                Path(node(2).to_string()),
            )
            .await,
            Err(ServerError::Forbidden)
        ));
        assert!(matches!(
            evict_device(
                State(fixture.state.clone()),
                Extension(None),
                Path(node(2).to_string()),
            )
            .await,
            Err(ServerError::Unauthorized)
        ));
        assert!(
            devices(&fixture.state, fixture.owner)
                .await
                .iter()
                .any(|device| device.id == node(2).to_string())
        );
    }

    #[tokio::test]
    async fn admin_spares_management() {
        // The route reaches enrolled devices only, so the realm's own management
        // node is not removable through it.
        let fixture = setup_devices().await;

        assert!(matches!(
            evict_device(
                State(fixture.state.clone()),
                Extension(Some(auth(fixture.admin))),
                Path(node(1).to_string()),
            )
            .await,
            Err(ServerError::NotFound)
        ));
    }
}
