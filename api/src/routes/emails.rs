//! Serves the caller's own opt-in for notification emails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;

use aruna_core::structs::execution::email::EmailPreference;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_operations::driver::drive;
use aruna_operations::notifications::email::{self, EmailError, read_email, remove_email};
use aruna_operations::notifications::settings::SettingError;
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

/// The opt-in without its address, which stays sealed to the realm key.
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
    description = r#"Returns the notification categories the caller receives by email.

**Authentication**: realm bearer token without path restrictions; only the caller's own opt-in
is read, and its address is never returned.

**Behavior**
- The opt-in is a replicated realm setting. This node shows the current setting it received so
  far; a node that has not received a recent change yet shows the previous one.

**Errors**
- 409 on a user device, which keeps no notification settings.
- 503 when this node cannot read the realm configuration now."#,
    responses(
        (status = 200, description = "The opt-in", body = EmailResponse, example = json!({
            "categories": ["share.upload"],
            "created_at_ms": 1762160400000u64
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 404, description = "The caller receives no notification emails", body = ErrorResponse),
        (status = 409, description = "This node is a user device", body = ErrorResponse),
        (status = 503, description = "The realm configuration cannot be read now; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_email(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<EmailResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    settings_node(&state, &auth).await?;
    let preference = read_email(&state.get_ctx(), auth.user_id)
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
- The opt-in is a replicated realm setting, so it keeps working when the caller's inbox moves
  to another node. The address is sealed to the realm key, which only management nodes hold,
  and is never returned or logged.
- A 200 means this node stored the opt-in. It reaches the other nodes through replication, so
  a node that has not received it yet still decides from the previous setting.
- The last write wins: a change made on this node after it saw another change replaces it.
  Two changes made on different nodes at about the same time both answer 200, and one of them
  is kept on every node.
- Each email is a job of the caller with a bounded number of retries. Its `Message-ID` stays the
  same across retries, so mail systems can drop repeats. Each job reads the setting when it
  runs, so a later change applies to queued emails too.

**Errors**
- 409 when the realm sends no email, or on a user device.
- 503 when the realm configuration cannot be read now."#,
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
        (status = 409, description = "The realm sends no email, or this node is a user device", body = ErrorResponse),
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
    settings_node(&state, &auth).await?;
    let ctx = state.get_ctx();
    let local = state.get_node_id();
    let preference = email::set_email(
        &ctx,
        local,
        auth.user_id,
        request.address,
        request.categories,
    )
    .await
    .map_err(map_error)?;
    Ok(Json(response(preference)))
}

#[utoipa::path(
    delete,
    path = "/system/notifications/email",
    tag = "system/notifications",
    summary = "Turn off notification emails",
    description = r#"Turns the caller's notification emails off.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- A 204 means this node stored the change. Queued emails are not sent once a node has received
  it; a node that has not received it yet still decides from the previous setting. Turning off a
  missing opt-in succeeds too.
- The last write wins, as for turning email on.

**Errors**
- 409 on a user device.
- 503 when the realm configuration cannot be read now."#,
    responses(
        (status = 204, description = "This node stored that email is off"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 409, description = "This node is a user device", body = ErrorResponse),
        (status = 503, description = "The realm configuration cannot be read now; retry", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_email(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    settings_node(&state, &auth).await?;
    remove_email(&state.get_ctx(), state.get_node_id(), auth.user_id)
        .await
        .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// User devices keep no settings; other nodes need a readable realm configuration.
pub(crate) async fn settings_node(state: &ServerState, auth: &AuthContext) -> ServerResult<()> {
    if matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(ServerError::Conflict(
            "user devices keep no notification settings".to_string(),
        ));
    }
    drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    Ok(())
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
        EmailError::NotConfigured => ServerError::Conflict(error.to_string()),
        EmailError::Setting(error) => setting_error(error),
        EmailError::Storage(_) | EmailError::Conversion(_) => {
            ServerError::InternalError(error.to_string())
        }
    }
}

pub(crate) fn setting_error(error: SettingError) -> ServerError {
    match error {
        SettingError::Config(_) => ServerError::ServiceUnavailableReason(error.to_string()),
        SettingError::Register(_) => ServerError::Conflict(error.to_string()),
        // The write is one local batch, so a failed one stored nothing.
        SettingError::Build(_) | SettingError::Storage(_) => {
            ServerError::InternalError(error.to_string())
        }
    }
}

#[cfg(test)]
#[path = "emails_tests.rs"]
mod tests;
