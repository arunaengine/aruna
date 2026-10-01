//! Routes that read, save, and delete the sealed vault payload of the calling user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::sessions::unix_rfc3339;
use crate::server::state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::vault::UserVault;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::users::user_vault::{
    DeleteVaultOperation, ReadVaultOperation, VaultStoreError, WriteVaultOperation,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// The vault of the caller; the payload is the portal's own ciphertext.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct VaultResponse {
    /// Null before the first save and after a delete.
    pub payload: Option<String>,
    /// 0 before the first save, bumped by every save and delete; pass it back with the next save.
    pub revision: u64,
    /// Null before the first save.
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SaveVaultRequest {
    /// The sealed vault as the portal encodes it, at most 64 KiB.
    pub payload: String,
    /// The revision the caller last read. Absent overwrites whatever is stored.
    pub revision: Option<u64>,
}

fn map_vault_error(error: VaultStoreError) -> ServerError {
    match error {
        VaultStoreError::Stale => {
            ServerError::Conflict("the vault changed in another browser".to_string())
        }
        VaultStoreError::Storage(StorageError::TransactionConflict) => {
            ServerError::Conflict("the vault is being written concurrently; retry".to_string())
        }
        VaultStoreError::TooLarge(cap) => ServerError::PayloadTooLarge(cap.to_string()),
        error => ServerError::InternalError(error.to_string()),
    }
}

fn vault_response(vault: Option<UserVault>) -> VaultResponse {
    match vault {
        Some(vault) => VaultResponse {
            payload: Some(vault.payload).filter(|payload| !payload.is_empty()),
            revision: vault.revision,
            updated_at: Some(unix_rfc3339(vault.updated_at)),
        },
        None => VaultResponse {
            payload: None,
            revision: 0,
            updated_at: None,
        },
    }
}

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(get_vault, put_vault, delete_vault))
}

#[utoipa::path(
    get,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Read the caller's vault",
    description = r#"Returns the passphrase-sealed vault this node holds for the calling user.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller reaches only their own.

**Behavior**
- The payload is the portal's own ciphertext, stored opaque and returned unchanged; the node holds
  no key that opens it.
- Before the first save the payload and `updated_at` are null and the revision is 0.
- After a delete the payload is null again, while the revision and `updated_at` keep counting
  from the deleted vault.
- The vault lives on the node that received it and is not replicated to the realm's other nodes."#,
    responses(
        (status = 200, description = "The caller's vault, or the empty state before the first save", body = VaultResponse,
            example = json!({
                "payload": "{\"version\":1,\"kdf\":{\"name\":\"pbkdf2-sha256\",\"iterations\":600000,\"salt\":\"c2FsdA==\"},\"keys\":[]}",
                "revision": 3,
                "updated_at": "2026-04-09T12:30:00Z"
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<VaultResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let vault = drive(ReadVaultOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(map_vault_error)?;
    Ok((StatusCode::OK, Json(vault_response(vault))))
}

#[utoipa::path(
    put,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Save the caller's vault",
    description = r#"Stores the passphrase-sealed vault of the calling user and bumps its revision.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller writes only their own.

**Behavior**
- The payload replaces the stored one as it is; the node does not read into it.
- Pass the `revision` last read, so a save from a second browser cannot silently drop what this
  one holds; a differing revision is refused with 409. Leaving it out overwrites. The first save
  may pass 0.
- A delete bumps the revision too, so a browser that still holds the deleted vault cannot
  overwrite a vault re-created elsewhere.
- The vault after the save is returned; the first save answers revision 1.

**Limits**
- The payload holds at most 64 KiB; a larger one is refused with 413."#,
    request_body(
        content = SaveVaultRequest,
        description = "The sealed vault as the portal encodes it, and the revision it was read at",
        example = json!({
            "payload": "{\"version\":1,\"kdf\":{\"name\":\"pbkdf2-sha256\",\"iterations\":600000,\"salt\":\"c2FsdA==\"},\"keys\":[]}",
            "revision": 2
        })
    ),
    responses(
        (status = 200, description = "The vault after the save", body = VaultResponse,
            example = json!({
                "payload": "{\"version\":1,\"kdf\":{\"name\":\"pbkdf2-sha256\",\"iterations\":600000,\"salt\":\"c2FsdA==\"},\"keys\":[]}",
                "revision": 3,
                "updated_at": "2026-04-09T12:30:00Z"
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 409, description = "The vault changed in another browser", body = ErrorResponse),
        (status = 413, description = "The payload exceeds 64 KiB", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SaveVaultRequest>,
) -> ServerResult<(StatusCode, Json<VaultResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let vault = drive(
        WriteVaultOperation::new(
            auth.user_id,
            request.payload,
            request.revision,
            unix_timestamp_secs(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_vault_error)?;
    Ok((StatusCode::OK, Json(vault_response(Some(vault)))))
}

#[utoipa::path(
    delete,
    path = "/access/users/me/vault",
    tag = "access/users",
    summary = "Delete the caller's vault",
    description = r#"Removes the passphrase-sealed vault of the calling user from this node.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. The vault is self-scoped, so a caller deletes only their own.

**Behavior**
- The next read answers a null payload, while the revision and `updated_at` keep counting from
  the deleted vault, so a browser that still holds it cannot overwrite a re-created one.
- Deleting a vault that is not there, or one already deleted, answers 204 without a write."#,
    responses(
        (status = 204, description = "The vault is deleted or was never there"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_vault(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    drive(
        DeleteVaultOperation::new(auth.user_id, unix_timestamp_secs()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_vault_error)?;
    Ok(StatusCode::NO_CONTENT)
}
