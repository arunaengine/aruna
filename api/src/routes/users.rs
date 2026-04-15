use crate::auth::bearer_token;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{Actor, AuthContext, User};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::drive;
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
    tags((name = "user", description = "User operations")),
    paths(
        register_user,
        register_oidc_user,
        get_user,
    )
)]
pub struct OnboardingApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/user", post(register_user))
        .route("/user/oidc", post(register_oidc_user))
        .route("/user", get(get_user))
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
    pub provider_id: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterOidcUserResponse {
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

#[utoipa::path(
    post,
    path = "/user/oidc",
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
    let provider = state
        .get_oidc_provider(&payload.provider_id)
        .await
        .map_err(|_| ServerError::Unauthorized)?;
    let validator = state
        .oidc_validator()
        .map_err(|_| ServerError::Unauthorized)?;
    let oidc_identity = validator
        .validate(&provider, token)
        .await
        .map_err(|_| ServerError::Unauthorized)?;

    let name = payload
        .name
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

    let time = chrono::Utc::now().timestamp().max(0) as u64;
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time,
            expiry: None,
            user_id: user.user_id,
            realm_id: realm_id.clone(),
            node_capabilities: state.node_capabilities().clone(),
        })
        .map_err(|err| ServerError::InternalError(err.to_string()))?,
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    let auth_context = AuthContext {
        user_id: user.user_id,
        realm_id,
        path_restrictions: None,
    };
    if let Err(error) = state.claim_initial_realm_admin(&auth_context).await {
        trace!(error = %error, "Failed to claim initial realm admin after OIDC registration");
    }

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
    path = "/user",
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
    path = "/user/{id}",
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
