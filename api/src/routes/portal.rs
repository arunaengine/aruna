//! Serves the realm default on/off state of optional portal features.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_operations::driver::drive;
use aruna_operations::realm::get_config::{GetConfigError, GetConfigOperation};
use aruna_operations::realm::set_portal::{SetPortalConfig, SetPortalError, SetPortalOperation};
use axum::extract::State;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi(tags((name = "system/realm", description = "Realm configuration, placement and quota")))]
pub struct PortalApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PortalApiDoc::openapi())
        .routes(routes!(get_portal_features, set_portal_features))
}

/// Realm default on/off state per portal feature name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PortalFeatures {
    pub features: BTreeMap<String, bool>,
}

#[utoipa::path(
    get,
    path = "/system/realm/portal-features",
    tag = "system/realm",
    summary = "Read the realm portal feature defaults",
    description = r#"Returns the realm default on/off state of optional portal features.

**Authentication**: any realm bearer token.

**Behavior**
- A feature missing from the map is on. A user can override each default with the user
  attribute `ui.feature.<name>`."#,
    responses(
        (status = 200, description = "The stored feature defaults", body = PortalFeatures,
            example = json!({"features": {"compute": false}})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_portal_features(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<PortalFeatures>> {
    let auth = require_realm_auth(&state, auth)?;
    let config = drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetConfigError::DocumentNotFound => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(Json(PortalFeatures {
        features: config.portal_features,
    }))
}

#[utoipa::path(
    put,
    path = "/system/realm/portal-features",
    tag = "system/realm",
    summary = "Replace the realm portal feature defaults",
    description = r#"Replaces the realm default on/off state of optional portal features.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The body replaces the stored map. Features absent from it fall back to on.

**Limits**
- At most 64 features. Names hold 1 to 64 characters of `a-z`, `0-9` and `-`."#,
    request_body(
        content = PortalFeatures,
        description = "The complete feature defaults to store",
        example = json!({"features": {"compute": false}})
    ),
    responses(
        (status = 200, description = "The stored feature defaults", body = PortalFeatures,
            example = json!({"features": {"compute": false}})),
        (status = 400, description = "Too many features or a malformed feature name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Storage cleanup capacity exhausted, or no management node was reachable to serve the relayed call; code `no_management_node`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_portal_features(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<PortalFeatures>,
) -> ServerResult<Json<PortalFeatures>> {
    let auth = require_realm_auth(&state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    ensure_permission(
        &state,
        &auth,
        policy_admin_path(state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let stored = drive(
        SetPortalOperation::new(SetPortalConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: state.get_realm_id(),
            },
            auth_context: auth,
            features: request.features,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_portal_error)?;
    Ok(Json(PortalFeatures {
        features: stored.portal_features,
    }))
}

fn map_portal_error(error: SetPortalError) -> ServerError {
    match error {
        SetPortalError::ConfigMissing => ServerError::NotFound,
        SetPortalError::Unauthorized | SetPortalError::NotManagementNode => ServerError::Forbidden,
        SetPortalError::InvalidFeatures { reason } => ServerError::BadRequestReason(reason),
        SetPortalError::StorageError(StorageError::TransactionConflict) => ServerError::Conflict(
            "concurrent realm configuration update conflict; retry".to_string(),
        ),
        SetPortalError::StorageError(StorageError::CleanupCapacity) => {
            ServerError::ServiceUnavailableReason(
                "storage cleanup capacity exhausted; retry".to_string(),
            )
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
#[path = "portal_tests.rs"]
mod tests;
