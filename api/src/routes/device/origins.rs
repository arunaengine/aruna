//! Serves which local file a realm version this device published came from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_owner;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::device::sync::outbox::local_origin;

use super::folders::map_folder_error;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(get_local_origin))
}

/// The local file one realm version was published from.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceOrigin {
    pub version_id: String,
    pub folder_id: String,
    /// Path relative to the folder root.
    pub path: String,
    /// Modification time of the local file when it was published, where the filesystem kept one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at_ms: Option<u64>,
    /// Weak fingerprint of the local file when it was published.
    pub fingerprint: String,
}

#[utoipa::path(
    get,
    path = "/device/origins/{version_id}",
    tag = "device",
    summary = "Find the local file of a realm version",
    description = r#"Looks up which local file of a synced folder this device published as a version.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- A row is kept for every upload the realm node acknowledged. The realm itself only records which
  device a version came from; the local path never leaves this device.
- A version this device did not publish, or published before this record existed, is not found.
- The list is node-local and answers while the realm is unreachable."#,
    params(("version_id" = String, Path, description = "Realm version ULID")),
    responses(
        (status = 200, description = "The local file the version came from", body = DeviceOrigin,
            example = json!({
                "version_id": "01JVERSION123456789ABCDEFG",
                "folder_id": "01JFOLDER0123456789ABCDEFG",
                "path": "notes/paper.txt",
                "modified_at_ms": 1775748191000_i64,
                "fingerprint": "1000-18a2b3c4d5e6f708-18a2b3c4d5e6f708-2a"
            })),
        (status = 400, description = "The version id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This device did not publish the version, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet, or its state could not be read", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_local_origin(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(version_id): Path<String>,
) -> ServerResult<Json<DeviceOrigin>> {
    require_owner(&state, auth).await?;
    let version_id = Ulid::from_str(&version_id).map_err(|_| ServerError::BadRequest)?;
    let origin = local_origin(&state.get_ctx(), version_id)
        .await
        .map_err(map_folder_error)?
        .ok_or(ServerError::NotFound)?;
    Ok(Json(DeviceOrigin {
        version_id: version_id.to_string(),
        folder_id: origin.folder_id.to_string(),
        path: origin.relative,
        modified_at_ms: origin.modified_at_ms,
        fingerprint: origin.fingerprint,
    }))
}
