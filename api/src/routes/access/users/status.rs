//! Route that deactivates or reactivates an account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::users::account_status::{
    AccountStatusConfig, AccountStatusError, AccountStatusOperation,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct AccountStatusRequest {
    /// `false` deactivates the account, `true` reactivates it.
    pub active: bool,
}

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(put_status))
}

fn map_status_error(error: AccountStatusError) -> ServerError {
    match error {
        AccountStatusError::NotFound => ServerError::NotFound,
        AccountStatusError::Unauthorized => ServerError::Forbidden,
        AccountStatusError::LastAdministrator => {
            ServerError::Conflict("the realm must keep one active human administrator".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

#[utoipa::path(
    put,
    path = "/access/users/{id}/status",
    tag = "access/users",
    summary = "Deactivate or reactivate an account",
    description = r#"Sets whether an account may act in the realm.

**Authentication**: realm bearer token without path restrictions. A human account needs WRITE on
`/{realm}/admin/config`; a service account needs WRITE on its group's `/{realm}/g/{group}/admin`.
A service account never changes any account's status.

**Behavior**
- Deactivation denies, on every node, each bearer token, S3 credential and S3 session of the account
  issued up to five minutes after the change, and no new token is issued while it lasts.
- Reactivation lets the account sign in again. Tokens and credentials cut off by the deactivation
  stay invalid, including those issued in the five minutes after it.
- Group memberships, roles and owned data stay as they are.
- Repeating the same change succeeds again.

**Limits**
- The realm keeps at least one active human account holding the `realm_admin` role."#,
    params(("id" = String, Path, description = "User id in the form `<ulid>@<realm>`")),
    request_body(
        content = AccountStatusRequest,
        example = json!({ "active": false })
    ),
    responses(
        (status = 204, description = "The account has the requested status"),
        (status = 400, description = "The id is not a user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm, the caller is a service account, or the caller lacks the administration grant", body = ErrorResponse),
        (status = 404, description = "This node holds no account with that id", body = ErrorResponse),
        (status = 409, description = "The change would leave no active human realm administrator", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn put_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
    Json(request): Json<AccountStatusRequest>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let target = UserId::from_string(&id).map_err(|_| ServerError::BadRequest)?;
    drive(
        AccountStatusOperation::new(AccountStatusConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            auth_context: auth,
            target,
            active: request.active,
            now: unix_timestamp_secs(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_status_error)?;
    Ok(StatusCode::NO_CONTENT)
}
