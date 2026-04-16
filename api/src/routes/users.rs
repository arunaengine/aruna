use crate::auth::bearer_token;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::onboarding::{OnboardingMode, OnboardingSecret};
use aruna_core::structs::{Actor, AuthContext, User};
use aruna_operations::create_user_record::{CreateUserRecordInput, CreateUserRecordOperation};
use aruna_operations::consume_onboarding_secret::{
    ConsumeOnboardingSecretError, ConsumeOnboardingSecretInput, ConsumeOnboardingSecretOperation,
};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::drive;
use aruna_operations::inspect_onboarding_secret::{
    InspectOnboardingSecretError, InspectOnboardingSecretInput, InspectOnboardingSecretOperation,
};
use aruna_operations::register_or_get_oidc_user::{
    RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
};
use aruna_operations::register_user::{RegisterUserInput, RegisterUserOperation};
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
        register_bootstrap_user,
        register_user,
        register_oidc_user,
        get_user,
    )
)]
pub struct UsersApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/users", post(register_user))
        .route("/users/bootstrap", post(register_bootstrap_user))
        .route("/users/oidc", post(register_oidc_user))
        .route("/users", get(get_user))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserRequest {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterUserResponse {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterOidcUserRequest {
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterBootstrapUserRequest {
    pub onboarding_secret: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterOidcUserResponse {
    pub user: RegisterUserResponse,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterBootstrapUserResponse {
    pub user: RegisterUserResponse,
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
    user_id: Ulid,
) -> ServerResult<String> {
    drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: now_timestamp(),
            expiry: None,
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

async fn try_claim_initial_admin(state: &Arc<ServerState>, user_id: Ulid) {
    let auth_context = AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
    };
    if let Err(error) = state.claim_initial_realm_admin(&auth_context).await {
        trace!(error = %error, "Failed to claim initial realm admin after user registration");
    }
}

#[utoipa::path(
    post,
    path = "/users/bootstrap",
    tag = "users",
    request_body = RegisterBootstrapUserRequest,
    responses(
        (status = 201, description = "User registered via onboarding secret", body = RegisterBootstrapUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    )
)]
async fn register_bootstrap_user(
    State(state): State<Arc<ServerState>>,
    Json(request): Json<RegisterBootstrapUserRequest>,
) -> ServerResult<(StatusCode, Json<RegisterBootstrapUserResponse>)> {
    let onboarding_secret = OnboardingSecret::decode(&request.onboarding_secret)
        .map_err(|_| ServerError::Unauthorized)?;
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
        CreateUserRecordOperation::new(CreateUserRecordInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: Ulid::from_bytes([0u8; 16]),
                realm_id: state.get_realm_id(),
            },
            user_id: Ulid::new(),
            name: request.name,
            subject_ids: vec![],
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    try_claim_initial_admin(&state, user.user_id).await;
    let token = issue_user_token(&state, user.user_id).await?;

    Ok((
        StatusCode::CREATED,
        Json(RegisterBootstrapUserResponse {
            user: user.into(),
            token,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users/oidc",
    tag = "users",
    request_body = RegisterOidcUserRequest,
    responses(
        (status = 200, description = "User resolved or registered via OIDC", body = RegisterOidcUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn register_oidc_user(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Json(payload): Json<RegisterOidcUserRequest>,
) -> ServerResult<(StatusCode, Json<RegisterOidcUserResponse>)> {
    let token = bearer_token(&headers).ok_or(ServerError::Unauthorized)?;
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

    let name = payload
        .name
        .or(oidc_identity.display_name.clone())
        .unwrap_or_else(|| oidc_identity.subject_id.clone());
    let realm_id = state.get_realm_id();
    let user = drive(
        RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: Ulid::from_bytes([0u8; 16]),
                realm_id: realm_id.clone(),
            },
            issuer: oidc_identity.issuer,
            subject_id: oidc_identity.subject_id,
            name,
            user_id: Ulid::new(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let token = issue_user_token(&state, user.user_id).await?;
    try_claim_initial_admin(&state, user.user_id).await;

    Ok((
        StatusCode::OK,
        Json(RegisterOidcUserResponse {
            user: user.into(),
            token,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/users",
    tag = "users",
    request_body = RegisterUserRequest,
    responses(
        (status = 201, description = "User registered", body = RegisterUserResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn register_user(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<RegisterUserRequest>,
) -> ServerResult<(StatusCode, Json<RegisterUserResponse>)> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    let realm_id = state.get_realm_id();
    if auth.realm_id != realm_id {
        return Err(ServerError::Forbidden);
    }

    let result = drive(
        RegisterUserOperation::new(RegisterUserInput {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: realm_id.clone(),
            },
            user_id: Ulid::new(),
            name: request.name,
            subject_ids: vec![],
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    trace!(
        event = "request.group.create.completed",
        realm_id = %realm_id,
        user_id = %auth.user_id,
        user_name = %result.name,
        "Completed user registration request"
    );

    Ok((StatusCode::CREATED, Json(result.into())))
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

#[cfg(test)]
mod tests {
    use super::{RegisterBootstrapUserRequest, register_bootstrap_user};
    use crate::server_state::ServerState;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::onboarding::OnboardingSecret;
    use aruna_core::structs::{Actor, NodeCapabilities, RealmAuthorizationDocument, RealmId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_operations::create_onboarding_secret::{
        CreateOnboardingSecretInput, CreateOnboardingSecretOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::Json;
    use axum::extract::State;
    use axum::http::StatusCode;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    async fn read_auth_doc(
        driver_ctx: &DriverContext,
        realm_id: &RealmId,
    ) -> RealmAuthorizationDocument {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: AUTH_KEYSPACE.to_string(),
                key: ByteView::from(realm_id.as_bytes().to_vec()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmAuthorizationDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected auth doc read result: {other:?}"),
        }
    }

    async fn setup_initial_state(
    ) -> (
        Arc<ServerState>,
        Arc<DriverContext>,
        RealmId,
        OnboardingSecret,
        NetHandle,
        TempDir,
    ) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                use_dns_discovery: false,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = net_handle.node_id();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: Ulid::from_bytes([0u8; 16]),
                    realm_id: realm_id.clone(),
                },
                realm_description: "Realm".to_string(),
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();

        let onboarding_secret = OnboardingSecret {
            seed_url: "http://127.0.0.1:3000".to_string(),
            enrollment_id: Ulid::new(),
            secret: [7u8; 32],
            mode: aruna_core::onboarding::OnboardingMode::Local,
        };
        drive(
            CreateOnboardingSecretOperation::new(CreateOnboardingSecretInput {
                record: aruna_core::onboarding::OnboardingSecretRecord {
                    enrollment_id: onboarding_secret.enrollment_id,
                    secret_hash: blake3::hash(&onboarding_secret.secret).to_string(),
                    mode: aruna_core::onboarding::OnboardingMode::Local,
                    expires_at: u64::MAX,
                    consumed: false,
                },
            }),
            driver_ctx.as_ref(),
        )
        .await
        .unwrap();

        let state = Arc::new(
            ServerState::new(
                driver_ctx.clone(),
                realm_id.clone(),
                node_id,
                NodeCapabilities::management_node(realm_signing_key).unwrap(),
                true,
                None,
            )
            .await,
        );

        (state, driver_ctx, realm_id, onboarding_secret, net_handle, tempdir)
    }

    #[tokio::test]
    async fn bootstrap_registration_creates_first_admin_user() {
        let (state, driver_ctx, realm_id, onboarding_secret, net_handle, _tempdir) =
            setup_initial_state().await;

        let (status, Json(response)) = register_bootstrap_user(
            State(state),
            Json(RegisterBootstrapUserRequest {
                onboarding_secret: onboarding_secret.encode().unwrap(),
                name: "Alice".to_string(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response.user.name, "Alice");
        assert!(!response.token.is_empty());

        let auth_doc = read_auth_doc(driver_ctx.as_ref(), &realm_id).await;
        let realm_admin = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .unwrap();
        assert!(realm_admin
            .assigned_users
            .contains(&Ulid::from_string(&response.user.id).unwrap()));

        net_handle.shutdown().await;
    }
}
