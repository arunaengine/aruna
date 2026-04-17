use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::AuthorizationError;
use aruna_core::structs::{AuthContext, PathRestriction, Permission, UserIdentity};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use aruna_operations::s3::create_user_access::{
    CreateUserAccessConfig, CreateUserAccessOperation, DEFAULT_CREDENTIAL_TTL,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::revoke_user_access::{RevokeUserAccessError, RevokeUserAccessOperation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use std::{str::FromStr, sync::Arc};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "credentials", description = "User credential management")),
    paths(create_s3_credentials, revoke_s3_credentials)
)]
pub struct CredentialsApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/users/credentials", post(create_s3_credentials))
        .route(
            "/users/credentials/{access_key_id}",
            delete(revoke_s3_credentials),
        )
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3PathRestriction {
    pub pattern: String,
    pub permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsRequest {
    pub group_id: String,
    pub expires_in_seconds: Option<u64>,
    pub path_restrictions: Option<Vec<CreateS3PathRestriction>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsResponse {
    pub access_key_id: String,
    pub access_secret: String,
}

#[utoipa::path(
    post,
    path = "/users/credentials",
    tag = "credentials",
    request_body = CreateS3CredentialsRequest,
    responses(
        (status = 201, description = "Credentials created", body = CreateS3CredentialsResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
    };
    let group_id = Ulid::from_str(&request.group_id).map_err(|_| ServerError::BadRequest)?;
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

    let path_restrictions =
        build_credential_restrictions(&auth, &state, group_id, request.path_restrictions.clone())
            .await?;
    let expiry = credential_expiry(SystemTime::now(), request.expires_in_seconds)?;
    let result = drive(
        CreateUserAccessOperation::new(CreateUserAccessConfig {
            user_identity,
            group_id,
            expiry,
            path_restrictions,
            issued_by: *node_id.as_bytes(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    match result {
        Ok((access_key_id, access_secret)) => Ok((
            StatusCode::CREATED,
            Json(CreateS3CredentialsResponse {
                access_key_id,
                access_secret: access_secret.secret,
            }),
        )),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

#[utoipa::path(
    delete,
    path = "/users/credentials/{access_key_id}",
    tag = "credentials",
    params(("access_key_id" = String, Path, description = "S3 access key to revoke")),
    responses(
        (status = 204, description = "Credential revoked"),
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
            auth_context: auth,
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
        Ok(Some(Ok(_))) => Ok(StatusCode::NO_CONTENT),
        Ok(None)
        | Ok(Some(Err(RevokeUserAccessError::NotFound)))
        | Err(RevokeUserAccessError::NotFound) => Err(ServerError::NotFound),
        Ok(Some(Err(err))) | Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
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
