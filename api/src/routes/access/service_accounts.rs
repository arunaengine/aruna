//! Routes to create and list a group's service accounts and to issue their tokens.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{parse_group_id, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::credentials::CreatePathRestriction;
use crate::routes::access::sessions::{
    CreateSessionResponse, map_create_error, session_restrictions, unix_rfc3339,
};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, AuthContext, SessionKind};
use aruna_core::structs::identity::user::User;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::session::{
    CreateSessionConfig, CreateSessionOperation, MAX_SESSION_TTL, bound_session_expiry,
};
use aruna_operations::users::service_account::check::{
    ServiceCheckConfig, ServiceCheckError, ServiceCheckOperation,
};
use aruna_operations::users::service_account::create::{
    CreateServiceConfig, CreateServiceError, CreateServiceOperation,
};
use aruna_operations::users::service_account::list::{ListServiceError, ListServiceOperation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

const SERVICE_LABEL: &str = "service account";

#[derive(OpenApi)]
#[openapi(tags((name = "access/service-accounts", description = "Service accounts of a group")))]
pub struct ServiceApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ServiceApiDoc::openapi())
        .routes(routes!(create_account, list_accounts))
        .routes(routes!(create_token))
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateServiceRequest {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ServiceAccountResponse {
    pub id: String,
    pub name: String,
    pub active: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ListServiceResponse {
    pub accounts: Vec<ServiceAccountResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ServiceTokenRequest {
    pub expires_in_seconds: Option<u64>,
    pub path_restrictions: Option<Vec<CreatePathRestriction>>,
}

fn account_response(user: User) -> ServiceAccountResponse {
    ServiceAccountResponse {
        active: !user.is_deactivated(),
        id: user.user_id.to_string(),
        name: user.name,
    }
}

fn map_check_error(error: ServiceCheckError) -> ServerError {
    match error {
        ServiceCheckError::Refused => ServerError::Forbidden,
        ServiceCheckError::NotFound => ServerError::NotFound,
        ServiceCheckError::Deactivated => {
            ServerError::Conflict("the service account is deactivated".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

#[utoipa::path(
    post,
    path = "/access/groups/{id}/service-accounts",
    tag = "access/service-accounts",
    summary = "Create a service account",
    description = r#"Creates a service account that the group owns.

**Authentication**: realm bearer token without path restrictions and with WRITE on the group's
`/{realm}/g/{group}/admin`. A service account never creates another one.

**Behavior**
- A service account is a user without any login; it acts only through tokens issued at
  `POST /access/groups/{id}/service-accounts/{account_id}/tokens`.
- It starts without roles. Add it to the group's roles like any member, and it never gains more
  than those roles grant.
- Deactivate it with `PUT /access/users/{id}/status`, which group administrators may call for the
  group's service accounts.

**Limits**
- The name is trimmed and must be 1 to 256 bytes."#,
    params(("id" = String, Path, description = "Group ULID")),
    request_body(content = CreateServiceRequest, example = json!({ "name": "Nightly import" })),
    responses(
        (status = 201, description = "The new service account", body = ServiceAccountResponse,
            example = json!({
                "id": "01JCNCTR0123456789ABCDEFGH@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Nightly import",
                "active": true
            })),
        (status = 400, description = "The group id or the name is invalid", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or from another realm, the caller is a service account, or the caller is no group administrator", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn create_account(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateServiceRequest>,
) -> ServerResult<(StatusCode, Json<ServiceAccountResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let user = drive(
        CreateServiceOperation::new(CreateServiceConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            user_id: UserId::local(Ulid::generate(), auth.realm_id),
            auth_context: auth,
            group_id,
            name: request.name,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        CreateServiceError::Unauthorized => ServerError::Forbidden,
        CreateServiceError::InvalidName => {
            ServerError::BadRequestReason("the name must be 1 to 256 bytes".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    })?;
    Ok((StatusCode::CREATED, Json(account_response(user))))
}

#[utoipa::path(
    get,
    path = "/access/groups/{id}/service-accounts",
    tag = "access/service-accounts",
    summary = "List a group's service accounts",
    description = r#"Lists the service accounts the group owns, deactivated ones included.

**Authentication**: realm bearer token without path restrictions and with WRITE on the group's
`/{realm}/g/{group}/admin`. A service account never lists them."#,
    params(("id" = String, Path, description = "Group ULID")),
    responses(
        (status = 200, description = "The group's service accounts", body = ListServiceResponse,
            example = json!({ "accounts": [{
                "id": "01JCNCTR0123456789ABCDEFGH@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA",
                "name": "Nightly import",
                "active": true
            }] })),
        (status = 400, description = "The group id is invalid", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or from another realm, the caller is a service account, or the caller is no group administrator", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_accounts(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ListServiceResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let accounts = drive(ListServiceOperation::new(auth, group_id), &state.get_ctx())
        .await
        .map_err(|error| match error {
            ListServiceError::Unauthorized => ServerError::Forbidden,
            error => ServerError::InternalError(error.to_string()),
        })?;
    let accounts = accounts.into_iter().map(account_response).collect();
    Ok((StatusCode::OK, Json(ListServiceResponse { accounts })))
}

#[utoipa::path(
    post,
    path = "/access/groups/{id}/service-accounts/{account_id}/tokens",
    tag = "access/service-accounts",
    summary = "Issue a service account token",
    description = r#"Issues a bearer token that acts as the service account and returns it once.

**Authentication**: realm bearer token without path restrictions and with WRITE on the group's
`/{realm}/g/{group}/admin`. A service account never issues tokens for another one.

**Behavior**
- The token carries the service account's identity and roles, never the caller's.
- `path_restrictions` narrows it like at `POST /access/sessions`, checked against the service
  account's own access.
- The service account renews the token itself at `GET /access/token`, keeping its restrictions.
- The token is an `api` session of the service account with the label `service account`.

**Limits**
- The lifetime is at most 24 hours, which is also the default."#,
    params(
        ("id" = String, Path, description = "Group ULID"),
        ("account_id" = String, Path, description = "Service account id in the form `<ulid>@<realm>`")
    ),
    request_body(
        content = ServiceTokenRequest,
        example = json!({
            "expires_in_seconds": 3600,
            "path_restrictions": [{ "pattern": "g/01JCNCTR0123456789ABCDEFGH/data/**", "permission": "READ" }]
        })
    ),
    responses(
        (status = 201, description = "Token issued; it is shown only here", body = CreateSessionResponse,
            example = json!({
                "session_id": "01JCNCTR0123456789ABCDEFGJ",
                "kind": "api",
                "label": "service account",
                "token": "EXAMPLE-SESSION-TOKEN-PLACEHOLDER",
                "expires_at": "2026-04-09T12:00:00Z"
            })),
        (status = 400, description = "Invalid ids, lifetime or restrictions", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller may not administer the group's service accounts, or a scope exceeds the service account's access", body = ErrorResponse),
        (status = 404, description = "The group owns no service account with that id", body = ErrorResponse),
        (status = 409, description = "The service account is deactivated, or holds 256 active sessions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn create_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, account_id)): Path<(String, String)>,
    Json(request): Json<ServiceTokenRequest>,
) -> ServerResult<(StatusCode, Json<CreateSessionResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let account_id = UserId::from_string(&account_id).map_err(|_| ServerError::BadRequest)?;
    drive(
        ServiceCheckOperation::new(ServiceCheckConfig {
            auth_context: auth.clone(),
            group_id,
            target: Some(account_id),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_check_error)?;
    let account = AuthContext {
        user_id: account_id,
        realm_id: auth.realm_id,
        path_restrictions: None,
        session: None,
    };
    let restrictions = session_restrictions(&state, &account, request.path_restrictions).await?;
    let now = unix_timestamp_secs();
    let expiry = bound_session_expiry(now, request.expires_in_seconds, now + MAX_SESSION_TTL)
        .map_err(map_create_error)?;
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now,
            expiry,
            user_id: account_id,
            realm_id: auth.realm_id,
            node_capabilities: state.node_capabilities().clone(),
            kind: SessionKind::Api,
            label: Some(SERVICE_LABEL.to_string()),
            name: None,
            restrictions,
            via: None,
            auth_time: None,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;
    Ok((
        StatusCode::CREATED,
        Json(CreateSessionResponse {
            session_id: created.session.sid,
            kind: created.session.kind.to_string(),
            label: created.session.label.unwrap_or_default(),
            token: created.token.expose().to_string(),
            expires_at: unix_rfc3339(created.session.expires_at),
        }),
    ))
}

#[cfg(test)]
#[path = "service_accounts_tests.rs"]
mod tests;
