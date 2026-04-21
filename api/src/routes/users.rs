use crate::auth::{OidcIdentity, bearer_token};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret};
use aruna_core::structs::{Actor, AuthContext, User};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::drive;
use aruna_operations::get_oidc_user::{GetOidcUserInput, GetOidcUserOperation};
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::trace;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "users", description = "User operations")),
    paths(
        register_user,
        get_token,
        get_user,
    )
)]
pub struct UsersApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/users/register", post(register_user))
        .route("/users/token", get(get_token))
        .route("/users", get(get_user))
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
    pub name: String,
}

impl From<User> for GetUserResponse {
    fn from(value: User) -> Self {
        GetUserResponse { name: value.name }
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
        | ConsumeOnboardingSecretError::AlreadyConsumed
        | ConsumeOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_inspect_onboarding_error(error: InspectOnboardingSecretError) -> ServerError {
    match error {
        InspectOnboardingSecretError::NotFound
        | InspectOnboardingSecretError::Expired
        | InspectOnboardingSecretError::AlreadyConsumed
        | InspectOnboardingSecretError::InvalidSecret => ServerError::Unauthorized,
        other => ServerError::InternalError(other.to_string()),
    }
}

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

async fn try_claim_initial_admin(state: &Arc<ServerState>, user_id: UserId) {
    let auth_context = AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
    };
    if let Err(error) = state.claim_initial_realm_admin(&auth_context).await {
        trace!(error = %error, "Failed to claim initial realm admin after user registration");
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
    let secret_hash = blake3::hash(&onboarding_secret.secret).to_string();
    let inspected = drive(
        InspectOnboardingSecretOperation::new(InspectOnboardingSecretInput {
            enrollment_id: onboarding_secret.enrollment_id,
            secret_hash: secret_hash.clone(),
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

    try_claim_initial_admin(&state, user.user_id).await;
    Ok(user)
}

#[utoipa::path(
    post,
    path = "/users/register",
    tag = "users",
    request_body = RegisterUserRequest,
    responses(
        (status = 201, description = "User registered via onboarding secret", body = RegisterUserResponse),
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
            let user = drive(
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
            .map_err(|err| ServerError::InternalError(err.to_string()))?;

            user
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
        (status = 201, description = "Group created", body = GetTokenResponse),
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
) -> ServerResult<(StatusCode, Json<GetTokenResponse>)> {
    let user_id = match auth {
        Some(aruna_ctx) => aruna_ctx.user_id,
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

    // TODO: expiry
    let expiry = None;
    let token = issue_user_token(&state, user_id, expiry).await?;

    Ok((StatusCode::OK, Json(GetTokenResponse { token })))
}

#[utoipa::path(
    get,
    path = "/users/{id}",
    tag = "users",
    params(("id" = String, Path, description = "User id")),
    responses(
        (status = 201, description = "Group created", body = GetUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_user(
    State(_state): State<Arc<ServerState>>,
    Extension(_auth): Extension<Option<AuthContext>>,
    Path(_user_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GetUserResponse>)> {
    todo!()
}

// TODO: Remove foreign users
// #[utoipa::path(
//     post,
//     path = "/users/foreign",
//     tag = "users",
//     request_body = RegisterUserRequest,
//     responses(
//         (status = 201, description = "Trusted foreign user joined the realm", body = RegisterUserResponse),
//         (status = 400, description = "Invalid request", body = ErrorResponse),
//         (status = 401, description = "Unauthorized", body = ErrorResponse),
//         (status = 403, description = "Forbidden", body = ErrorResponse)
//     ),
//     security(("bearer_auth" = []))
// )]
// async fn register_foreign_user(
//     State(state): State<Arc<ServerState>>,
//     headers: HeaderMap,
//     Json(request): Json<RegisterUserRequest>,
// ) -> ServerResult<(StatusCode, Json<RegisterUserResponse>)> {
//     let auth = authenticate_foreign_user(&state, &headers).await?;
//     let realm_id = state.get_realm_id();
//
//     let user = drive(
//         CreateUserRecordOperation::new(CreateUserRecordInput {
//             actor: Actor {
//                 node_id: state.get_node_id(),
//                 user_id: auth.user_id,
//                 realm_id,
//             },
//             user_id: auth.user_id,
//             name: request.name,
//             subject_ids: vec![],
//         }),
//         &state.get_ctx(),
//     )
//     .await
//     .map_err(|err| ServerError::InternalError(err.to_string()))?;
//
//     trace!(
//         event = "request.user.foreign_join.completed",
//         realm_id = %realm_id,
//         user_id = %auth.user_id,
//         user_name = %user.name,
//         "Completed foreign user join request"
//     );
//
//     Ok((StatusCode::CREATED, Json(user.into())))
// }
//
// #[utoipa::path(
//     post,
//     path = "/users/oidc",
//     tag = "users",
//     request_body = RegisterOidcUserRequest,
//     responses(
//         (status = 200, description = "User resolved or registered via OIDC", body = RegisterOidcUserResponse),
//         (status = 400, description = "Invalid request", body = ErrorResponse),
//         (status = 401, description = "Unauthorized", body = ErrorResponse),
//         (status = 403, description = "Forbidden", body = ErrorResponse)
//     ),
//     security(("bearer_auth" = []))
// )]
// async fn register_oidc_user(
//     State(state): State<Arc<ServerState>>,
//     headers: HeaderMap,
//     Json(payload): Json<RegisterOidcUserRequest>,
// ) -> ServerResult<(StatusCode, Json<RegisterOidcUserResponse>)> {
//     let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
//     let validator = state
//         .oidc_validator()
//         .map_err(|_| ServerError::Unauthorized)?;
//     let selector = validator
//         .token_selector(token)
//         .map_err(|_| ServerError::Unauthorized)?;
//     let provider = state
//         .get_oidc_provider_by_token(&selector)
//         .await
//         .map_err(|_| ServerError::Unauthorized)?;
//     let oidc_identity = validator
//         .validate(&provider, token)
//         .await
//         .map_err(|_| ServerError::Unauthorized)?;
//
//     let name = payload
//         .name
//         .or(oidc_identity.display_name.clone())
//         .unwrap_or_else(|| oidc_identity.subject_id.clone());
//     let realm_id = state.get_realm_id();
//     let user = drive(
//         RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
//             actor: Actor {
//                 node_id: state.get_node_id(),
//                 user_id: UserId::nil(realm_id),
//                 realm_id,
//             },
//             issuer: oidc_identity.issuer,
//             subject_id: oidc_identity.subject_id,
//             name,
//             user_id: UserId::local(Ulid::new(), realm_id),
//         }),
//         &state.get_ctx(),
//     )
//     .await
//     .map_err(|err| ServerError::InternalError(err.to_string()))?;
//
//     let token = issue_user_token(&state, user.user_id).await?;
//     try_claim_initial_admin(&state, user.user_id).await;
//
//     Ok((
//         StatusCode::OK,
//         Json(RegisterOidcUserResponse {
//             user: user.into(),
//             token,
//         }),
//     ))
// }

// TODO: - Unify admin and register
//       - Remove register for foreign users
// #[utoipa::path(
//     post,
//     path = "/users",
//     tag = "users",
//     request_body = RegisterUserRequest,
//     responses(
//         (status = 201, description = "User registered", body = RegisterUserResponse),
//         (status = 400, description = "Invalid request", body = ErrorResponse),
//         (status = 401, description = "Unauthorized", body = ErrorResponse),
//         (status = 403, description = "Forbidden", body = ErrorResponse)
//     ),
//     security(("bearer_auth" = []))
// )]
// async fn register_user(
//     State(state): State<Arc<ServerState>>,
//     Extension(auth): Extension<Option<AuthContext>>,
//     Json(request): Json<RegisterUserRequest>,
// ) -> ServerResult<(StatusCode, Json<RegisterUserResponse>)> {
//     let auth = auth.ok_or(ServerError::Unauthorized)?;
//     let realm_id = state.get_realm_id();
//     if auth.realm_id != realm_id {
//         return Err(ServerError::Forbidden);
//     }
//
//     let result = drive(
//         RegisterUserOperation::new(RegisterUserInput {
//             actor: Actor {
//                 node_id: state.get_node_id(),
//                 user_id: auth.user_id,
//                 realm_id,
//             },
//             user_id: UserId::local(Ulid::new(), realm_id),
//             name: request.name,
//             subject_ids: vec![],
//         }),
//         &state.get_ctx(),
//     )
//     .await
//     .map_err(|err| ServerError::InternalError(err.to_string()))?;
//
//     trace!(
//         event = "request.group.create.completed",
//         realm_id = %realm_id,
//         user_id = %auth.user_id,
//         user_name = %result.name,
//         "Completed user registration request"
//     );
//
//     Ok((StatusCode::CREATED, Json(result.into())))
// }

// #[cfg(test)]
// mod tests {
//     use super::{RegisterUserRequest, authenticate_foreign_user};
//     use crate::error::ServerError;
//     use crate::routes::users::register_user;
//     use crate::server_state::ServerState;
//     use aruna_core::UserId;
//     use aruna_core::effects::{Effect, StorageEffect};
//     use aruna_core::events::{Event, StorageEvent};
//     use aruna_core::handle::Handle;
//     use aruna_core::keyspaces::{AUTH_KEYSPACE, USER_KEYSPACE};
//     use aruna_core::onboarding::OnboardingSecret;
//     use aruna_core::structs::{Actor, NodeCapabilities, RealmAuthorizationDocument, RealmId, User};
//     use aruna_net::{NetConfig, NetHandle};
//     use aruna_operations::create_onboarding_secret::{
//         CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
//     };
//     use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
//     use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
//     use aruna_operations::driver::{DriverContext, drive};
//     use aruna_storage::storage;
//     use aruna_tasks::TaskHandle;
//     use axum::Json;
//     use axum::extract::State;
//     use axum::http::{HeaderMap, StatusCode, header};
//     use byteview::ByteView;
//     use ed25519_dalek::SigningKey;
//     use std::sync::Arc;
//     use tempfile::{TempDir, tempdir};
//     use ulid::Ulid;
//
//     async fn read_auth_doc(
//         driver_ctx: &DriverContext,
//         realm_id: &RealmId,
//     ) -> RealmAuthorizationDocument {
//         match driver_ctx
//             .storage_handle
//             .send_effect(Effect::Storage(StorageEffect::Read {
//                 key_space: AUTH_KEYSPACE.to_string(),
//                 key: ByteView::from(realm_id.as_bytes().to_vec()),
//                 txn_id: None,
//             }))
//             .await
//         {
//             Event::Storage(StorageEvent::ReadResult {
//                 value: Some(bytes), ..
//             }) => RealmAuthorizationDocument::from_bytes(&bytes).unwrap(),
//             other => panic!("unexpected auth doc read result: {other:?}"),
//         }
//     }
//
//     async fn read_user(driver_ctx: &DriverContext, user_id: UserId) -> User {
//         match driver_ctx
//             .storage_handle
//             .send_effect(Effect::Storage(StorageEffect::Read {
//                 key_space: USER_KEYSPACE.to_string(),
//                 key: ByteView::from(user_id.to_bytes()),
//                 txn_id: None,
//             }))
//             .await
//         {
//             Event::Storage(StorageEvent::ReadResult {
//                 value: Some(bytes), ..
//             }) => User::from_bytes(&bytes).unwrap(),
//             other => panic!("unexpected user read result: {other:?}"),
//         }
//     }
//
//     async fn setup_initial_state() -> (
//         Arc<ServerState>,
//         Arc<DriverContext>,
//         RealmId,
//         OnboardingSecret,
//         NetHandle,
//         TempDir,
//     ) {
//         let tempdir = tempdir().unwrap();
//         let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
//         let net_handle = NetHandle::new(
//             NetConfig {
//                 bind_addr: "127.0.0.1:0".parse().unwrap(),
//                 use_dns_discovery: false,
//                 ..NetConfig::default()
//             },
//             storage_handle.clone(),
//         )
//         .await
//         .unwrap();
//         let driver_ctx = Arc::new(DriverContext {
//             storage_handle,
//             net_handle: Some(net_handle.clone()),
//             blob_handle: None,
//             automerge_handle: None,
//             metadata_handle: None,
//             task_handle: Some(TaskHandle::new()),
//         });
//
//         let realm_signing_key = SigningKey::from_bytes(&[17u8; 32]);
//         let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
//         let node_id = net_handle.node_id();
//
//         drive(
//             CreateRealmOperation::new(CreateRealmConfig {
//                 actor: Actor {
//                     node_id,
//                     user_id: UserId::nil(realm_id),
//                     realm_id,
//                 },
//                 realm_description: "Realm".to_string(),
//             }),
//             driver_ctx.as_ref(),
//         )
//         .await
//         .unwrap();
//
//         let onboarding_secret = OnboardingSecret {
//             seed_url: "http://127.0.0.1:3000".to_string(),
//             enrollment_id: Ulid::new(),
//             secret: [7u8; 32],
//             mode: aruna_core::onboarding::OnboardingMode::Local,
//         };
//         drive(
//             CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
//                 record: aruna_core::onboarding::OnboardingSecretRecord {
//                     enrollment_id: onboarding_secret.enrollment_id,
//                     secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
//                     mode: aruna_core::onboarding::OnboardingMode::Local,
//                     expires_at: u64::MAX,
//                     consumed: false,
//                 },
//             }),
//             driver_ctx.as_ref(),
//         )
//         .await
//         .unwrap();
//
//         let state = Arc::new(
//             ServerState::new(
//                 driver_ctx.clone(),
//                 realm_id,
//                 node_id,
//                 NodeCapabilities::management_node(realm_signing_key).unwrap(),
//                 true,
//                 None,
//             )
//             .await,
//         );
//
//         (
//             state,
//             driver_ctx,
//             realm_id,
//             onboarding_secret,
//             net_handle,
//             tempdir,
//         )
//     }
//
//     #[tokio::test]
//     async fn bootstrap_registration_creates_first_admin_user() {
//         let (state, driver_ctx, realm_id, onboarding_secret, net_handle, _tempdir) =
//             setup_initial_state().await;
//
//         let header_map =
//
//         let (status, Json(response)) = register_user(
//             State(state),
//             HeaderMap::new()
//             Json(RegisterUserRequest {
//                 onboarding_secret: Some(onboarding_secret.encode().unwrap()),
//             }),
//         )
//         .await
//         .unwrap();
//
//         assert_eq!(status, StatusCode::CREATED);
//         assert_eq!(response.user.name, "Alice");
//         assert!(!response.token.is_empty());
//
//         let auth_doc = read_auth_doc(driver_ctx.as_ref(), &realm_id).await;
//         let realm_admin = auth_doc
//             .roles
//             .values()
//             .find(|role| role.name == "realm_admin")
//             .unwrap();
//         assert!(
//             realm_admin
//                 .assigned_users
//                 .contains(&UserId::from_string(&response.user.id).unwrap())
//         );
//
//         net_handle.shutdown().await;
//     }
//
//     #[tokio::test]
//     async fn foreign_registration_preserves_home_realm_user_id() {
//         let (state, driver_ctx, realm_id, _onboarding_secret, net_handle, _tempdir) =
//             setup_initial_state().await;
//
//         let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
//         let foreign_realm_signing_key = SigningKey::generate(&mut csprng);
//         let foreign_realm_id =
//             RealmId::from_bytes(foreign_realm_signing_key.verifying_key().to_bytes());
//         state.add_trusted_realm(foreign_realm_id).await;
//
//         let foreign_user_id = UserId::local(Ulid::new(), foreign_realm_id);
//         let token = drive(
//             CreateTokenOperation::new(CreateTokenConfig {
//                 time: super::now_timestamp(),
//                 expiry: None,
//                 user_id: foreign_user_id,
//                 realm_id: foreign_realm_id,
//                 node_capabilities: NodeCapabilities::management_node(foreign_realm_signing_key)
//                     .unwrap(),
//             })
//             .unwrap(),
//             driver_ctx.as_ref(),
//         )
//         .await
//         .unwrap();
//
//         let mut headers = HeaderMap::new();
//         headers.insert(
//             header::AUTHORIZATION,
//             axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
//         );
//         let auth = authenticate_foreign_user(&state, &headers).await;
//
//         let (status, Json(response)) = register_foreign_user(
//             State(state.clone()),
//             headers,
//             Json(RegisterUserRequest {
//                 name: "Foreign Alice".to_string(),
//             }),
//         )
//         .await
//         .unwrap();
//
//         assert!(auth.is_ok());
//         assert_eq!(status, StatusCode::CREATED);
//         assert_eq!(response.id, foreign_user_id.to_string());
//
//         let stored_user = read_user(driver_ctx.as_ref(), foreign_user_id).await;
//         assert_eq!(stored_user.user_id, foreign_user_id);
//         assert_eq!(stored_user.name, "Foreign Alice");
//
//         let auth_doc = read_auth_doc(driver_ctx.as_ref(), &realm_id).await;
//         let realm_admin = auth_doc
//             .roles
//             .values()
//             .find(|role| role.name == "realm_admin")
//             .unwrap();
//         assert!(realm_admin.assigned_users.is_empty());
//
//         net_handle.shutdown().await;
//     }
//
//     #[tokio::test]
//     async fn foreign_registration_rejects_local_realm_token() {
//         let (state, driver_ctx, realm_id, _onboarding_secret, net_handle, _tempdir) =
//             setup_initial_state().await;
//         let local_user_id = UserId::local(Ulid::new(), realm_id);
//
//         let local_realm_signing_key = SigningKey::from_bytes(&[17u8; 32]);
//         let local_realm_id =
//             RealmId::from_bytes(local_realm_signing_key.verifying_key().to_bytes());
//         assert_eq!(local_realm_id, realm_id);
//         let token = drive(
//             CreateTokenOperation::new(CreateTokenConfig {
//                 time: super::now_timestamp(),
//                 expiry: None,
//                 user_id: local_user_id,
//                 realm_id,
//                 node_capabilities: NodeCapabilities::management_node(local_realm_signing_key)
//                     .unwrap(),
//             })
//             .unwrap(),
//             driver_ctx.as_ref(),
//         )
//         .await
//         .unwrap();
//
//         let mut headers = HeaderMap::new();
//         headers.insert(
//             header::AUTHORIZATION,
//             axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
//         );
//         let auth = authenticate_foreign_user(&state, &headers).await;
//
//         let result = register_foreign_user(
//             State(state),
//             headers,
//             Json(RegisterUserRequest {
//                 name: "Local Alice".to_string(),
//             }),
//         )
//         .await;
//
//         assert!(matches!(auth, Err(ServerError::Forbidden)));
//         assert!(matches!(result, Err(ServerError::Forbidden)));
//         net_handle.shutdown().await;
//     }
// }
