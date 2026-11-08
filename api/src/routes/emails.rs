//! Serves the caller's own opt-in for notification emails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::driver::drive;
use aruna_operations::notifications::email::{EmailAction, EmailError, EmailOperation};
use aruna_operations::notifications::placement::holds_inbox;
use aruna_operations::realm::get_config::GetConfigOperation;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct EmailsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(EmailsApiDoc::openapi()).routes(routes!(
        get_email,
        set_email,
        delete_email
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EmailRequest {
    /// The account's verified email address.
    pub address: String,
    /// `group.membership`, `share.upload` and `bucket.encryption`.
    pub categories: BTreeSet<String>,
}

/// The opt-in without its address, which stays sealed on the node.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct EmailResponse {
    pub categories: BTreeSet<String>,
    pub created_at_ms: u64,
}

#[utoipa::path(
    get,
    path = "/system/notifications/email",
    tag = "system/notifications",
    summary = "Read the caller's email opt-in",
    description = r#"Returns the notification categories the caller receives by email on this node.

**Authentication**: realm bearer token without path restrictions; only the caller's own opt-in
is read, and its address is never returned."#,
    responses(
        (status = 200, description = "The opt-in", body = EmailResponse, example = json!({
            "categories": ["share.upload"],
            "created_at_ms": 1762160400000u64
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 404, description = "The caller receives no notification emails here", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_email(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<EmailResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let preference = drive(
        EmailOperation::new(auth.user_id, EmailAction::Read),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    Ok(Json(response(preference.ok_or(ServerError::NotFound)?)))
}

#[utoipa::path(
    put,
    path = "/system/notifications/email",
    tag = "system/notifications",
    summary = "Turn on notification emails",
    description = r#"Sends the chosen notification categories to the caller's verified address by email.

**Authentication**: realm bearer token without path restrictions; the opt-in belongs to the
caller.

**Behavior**
- Only the address the caller's sign-in provider verified is accepted, so no one can send
  notices to someone else's mailbox.
- Categories are `group.membership`, `share.upload` and `bucket.encryption`. The inbox stays
  authoritative: an email only says that a notice arrived and names no group, path or file.
- Each email is a job of the caller with a bounded number of retries. Its `Message-ID` stays the
  same across retries, so mail systems can drop repeats.
- The address is sealed in the node vault and never returned or logged.
- Setting the opt-in again replaces it; queued emails of the old one are not sent.

**Errors**
- 409 when the realm sends no email, or this node does not hold the caller's inbox.
- 503 when this node cannot read the realm configuration now."#,
    request_body(content = EmailRequest, example = json!({
        "address": "ada@example.org",
        "categories": ["share.upload", "group.membership"]
    })),
    responses(
        (status = 200, description = "The new opt-in", body = EmailResponse, example = json!({
            "categories": ["group.membership", "share.upload"],
            "created_at_ms": 1762160400000u64
        })),
        (status = 400, description = "No category, an unknown one, or an address that is not the caller's verified address", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 409, description = "The realm sends no email, or this node does not hold the caller's inbox", body = ErrorResponse),
        (status = 503, description = "The realm configuration cannot be read now; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_email(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<EmailRequest>,
) -> ServerResult<Json<EmailResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let config = drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    if config.email.is_none() {
        return Err(ServerError::Conflict(
            "email is not configured in this realm".to_string(),
        ));
    }
    if !holds_inbox(&state.get_ctx(), auth.user_id).await {
        return Err(ServerError::Conflict(
            "this node does not hold the caller's inbox".to_string(),
        ));
    }
    let action = EmailAction::OptIn {
        address: request.address,
        categories: request.categories,
    };
    let preference = drive(EmailOperation::new(auth.user_id, action), &state.get_ctx())
        .await
        .map_err(map_error)?
        .ok_or_else(|| ServerError::InternalError("the opt-in was not stored".to_string()))?;
    Ok(Json(response(preference)))
}

#[utoipa::path(
    delete,
    path = "/system/notifications/email",
    tag = "system/notifications",
    summary = "Turn off notification emails",
    description = r#"Removes the caller's email opt-in and its sealed address from this node.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Queued emails are not sent afterwards. Removing a missing opt-in succeeds too."#,
    responses(
        (status = 204, description = "No opt-in is left on this node"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_email(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    drive(
        EmailOperation::new(auth.user_id, EmailAction::OptOut),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn response(preference: EmailPreference) -> EmailResponse {
    EmailResponse {
        categories: preference.categories,
        created_at_ms: preference.created_at_ms,
    }
}

fn map_error(error: EmailError) -> ServerError {
    match error {
        EmailError::InvalidCategories | EmailError::NotVerified => {
            ServerError::BadRequestReason(error.to_string())
        }
        EmailError::Storage(_)
        | EmailError::Conversion(_)
        | EmailError::NotFinished
        | EmailError::UnexpectedEvent { .. } => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
#[path = "emails_tests.rs"]
mod tests;
