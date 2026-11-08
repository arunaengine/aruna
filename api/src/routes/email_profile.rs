//! Serves the realm email profile that every infrastructure node sends notification emails with.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::compute::SecretBytes;
use aruna_core::errors::StorageError;
use aruna_core::realm_vault::RealmVaultKey;
use aruna_core::structs::execution::email::{RealmEmailProfile, SmtpTls};
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities, Permission};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_operations::driver::drive;
use aruna_operations::notifications::email::{SmtpMailer, SmtpSettings, refresh_mail};
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::realm::set_email::{
    EmailInput, SetEmailConfig, SetEmailError, SetEmailOperation,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi(tags((name = "system/realm", description = "Realm configuration, placement and quota")))]
pub struct ProfileApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ProfileApiDoc::openapi()).routes(routes!(
        get_email_profile,
        set_email_profile,
        delete_email_profile
    ))
}

/// How the connection to the mail server is secured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    Starttls,
    Tls,
    Plain,
}

impl From<TlsMode> for SmtpTls {
    fn from(mode: TlsMode) -> Self {
        match mode {
            TlsMode::Starttls => SmtpTls::Starttls,
            TlsMode::Tls => SmtpTls::Tls,
            TlsMode::Plain => SmtpTls::Plain,
        }
    }
}

impl From<SmtpTls> for TlsMode {
    fn from(tls: SmtpTls) -> Self {
        match tls {
            SmtpTls::Starttls => TlsMode::Starttls,
            SmtpTls::Tls => TlsMode::Tls,
            SmtpTls::Plain => TlsMode::Plain,
        }
    }
}

#[derive(Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EmailProfileRequest {
    pub host: String,
    pub port: Option<u16>,
    pub tls: TlsMode,
    /// SMTP login; sent together with `password`, or alone to keep the stored password.
    pub username: Option<String>,
    /// Never returned or logged. Missing keeps the stored password while `username` is unchanged.
    pub password: Option<String>,
    /// Sender of every notification email, such as `Aruna <aruna@example.org>`.
    pub from: String,
}

impl std::fmt::Debug for EmailProfileRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmailProfileRequest")
            .field("host", &self.host)
            .field("has_password", &self.password.is_some())
            .finish_non_exhaustive()
    }
}

/// Every member learns whether the realm sends email; realm config admins see the profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EmailProfileResponse {
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<EmailProfileView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EmailProfileView {
    pub host: String,
    pub port: Option<u16>,
    pub tls: TlsMode,
    pub username: Option<String>,
    pub from: String,
    pub has_password: bool,
    #[schema(value_type = String)]
    pub revision: Ulid,
    pub changed_by: String,
    pub changed_at_ms: u64,
}

impl From<RealmEmailProfile> for EmailProfileView {
    fn from(profile: RealmEmailProfile) -> Self {
        Self {
            host: profile.host,
            port: profile.port,
            tls: profile.tls.into(),
            username: profile.username,
            from: profile.from,
            has_password: profile.password.is_some(),
            revision: profile.revision,
            changed_by: profile.changed_by.to_string(),
            changed_at_ms: profile.changed_at_ms,
        }
    }
}

#[utoipa::path(
    get,
    path = "/system/realm/email",
    tag = "system/realm",
    summary = "Read the realm email profile",
    description = r#"Says whether this realm sends notification emails.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Every member gets `configured`. Callers with WRITE on the realm configuration admin path also
  get the profile: host, port, TLS mode, username, sender, whether a password is stored, its
  revision and who changed it when. The password itself is never returned.
- Served from this node's copy of the realm configuration."#,
    responses(
        (status = 200, description = "Whether email is configured, and the profile for realm config admins", body = EmailProfileResponse,
            example = json!({"configured": true, "profile": {"host": "smtp.example.org", "port": 587, "tls": "starttls", "username": "aruna", "from": "Aruna <aruna@example.org>", "has_password": true, "revision": "01JBX7Y3S5M3Q8Y1V9K2T4R6P0", "changed_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "changed_at_ms": 1762160400000u64}})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A path-restricted token", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_email_profile(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<EmailProfileResponse>> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let config = drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetConfigError::DocumentNotFound => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;
    let admin = config.email.is_some()
        && ensure_permission(
            &state,
            &auth,
            policy_admin_path(state.get_realm_id()),
            Permission::WRITE,
        )
        .await
        .is_ok();
    Ok(Json(EmailProfileResponse {
        configured: config.email.is_some(),
        profile: config.email.filter(|_| admin).map(Into::into),
    }))
}

#[utoipa::path(
    put,
    path = "/system/realm/email",
    tag = "system/realm",
    summary = "Replace the realm email profile",
    description = r#"Sets the SMTP profile that server and management nodes send notification emails with.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- Replaces the profile with a new revision. Nodes fetch a new password on their next email.
- `username` and `password` come together. A missing `password` keeps the stored one while the
  username is unchanged; a new or changed username needs a password.
- The password is sealed in the realm vault with a key every holder of the realm signing key
  can derive. Server nodes fetch it from a management node and cache it.
- Nodes that held the password, removed nodes and former management nodes included, keep what
  they had: change the password at the mail provider to revoke it.

**Errors**
- 400 when the host or sender address is invalid, or a password comes without a username."#,
    request_body(content = EmailProfileRequest, example = json!({
        "host": "smtp.example.org", "port": 587, "tls": "starttls",
        "username": "aruna", "password": "app-password", "from": "Aruna <aruna@example.org>"
    })),
    responses(
        (status = 200, description = "The stored profile, without the password", body = EmailProfileView,
            example = json!({"host": "smtp.example.org", "port": 587, "tls": "starttls", "username": "aruna", "from": "Aruna <aruna@example.org>", "has_password": true, "revision": "01JBX7Y3S5M3Q8Y1V9K2T4R6P0", "changed_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "changed_at_ms": 1762160400000u64})),
        (status = 400, description = "Invalid host or sender, a password without username, or a changed username without password", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_email_profile(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<EmailProfileRequest>,
) -> ServerResult<Json<EmailProfileView>> {
    let settings = SmtpSettings {
        host: request.host.trim().to_string(),
        port: request.port,
        tls: request.tls.into(),
        username: None,
        password: None,
        from: request.from.trim().to_string(),
    };
    SmtpMailer::new(&settings).map_err(ServerError::BadRequestReason)?;
    let input = EmailInput {
        host: settings.host,
        port: request.port,
        tls: request.tls.into(),
        username: request.username,
        password: request
            .password
            .map(|password| SecretBytes::new(password.into_bytes())),
        from: settings.from,
    };
    let stored = change_profile(&state, auth, Some(input)).await?;
    stored
        .map(|profile| Json(profile.into()))
        .ok_or_else(|| ServerError::InternalError("the profile was not stored".to_string()))
}

#[utoipa::path(
    delete,
    path = "/system/realm/email",
    tag = "system/realm",
    summary = "Remove the realm email profile",
    description = r#"Turns notification emails off for the realm and removes the stored password.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- Users' opt-ins stay and work again once a profile is set. Nodes drop their cached password
  when the change reaches them."#,
    responses(
        (status = 204, description = "No profile is left"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "No management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_email_profile(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    change_profile(&state, auth, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn change_profile(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
    profile: Option<EmailInput>,
) -> ServerResult<Option<RealmEmailProfile>> {
    let auth = require_unrestricted_auth(state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let NodeCapabilities::Management {
        realm_signing_key, ..
    } = state.node_capabilities()
    else {
        return Err(ServerError::Forbidden);
    };
    let stored = drive(
        SetEmailOperation::new(SetEmailConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            vault_key: RealmVaultKey::derive(realm_signing_key),
            profile,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_email_error)?;
    let mail = state.jobs_runtime().mail();
    refresh_mail(&state.get_ctx(), state.get_realm_id(), &mail).await;
    Ok(stored)
}

fn map_email_error(error: SetEmailError) -> ServerError {
    match error {
        SetEmailError::ConfigMissing => ServerError::NotFound,
        SetEmailError::Unauthorized | SetEmailError::NotManagementNode => ServerError::Forbidden,
        SetEmailError::InvalidProfile { .. } | SetEmailError::PasswordRequired => {
            ServerError::BadRequestReason(error.to_string())
        }
        SetEmailError::StorageError(StorageError::TransactionConflict) => ServerError::Conflict(
            "concurrent realm configuration update conflict; retry".to_string(),
        ),
        SetEmailError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
#[path = "email_profile_tests.rs"]
mod tests;
