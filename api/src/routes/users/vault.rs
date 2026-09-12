use crate::auth::require_unrestricted_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::sessions::unix_rfc3339;
use crate::server_state::ServerState;
use aruna_core::errors::StorageError;
use aruna_core::structs::{AuthContext, UserVault};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::users::tests::fixtures::{realm_auth, setup_state};
    use aruna_core::structs::MAX_USER_VAULT_BYTES;
    use axum::response::IntoResponse;

    async fn read(
        state: &Arc<ServerState>,
        auth: Option<AuthContext>,
    ) -> Result<VaultResponse, StatusCode> {
        get_vault(State(state.clone()), Extension(auth))
            .await
            .map(|(_, Json(vault))| vault)
            .map_err(|error| error.into_response().status())
    }

    async fn save(
        state: &Arc<ServerState>,
        auth: Option<AuthContext>,
        payload: &str,
        revision: Option<u64>,
    ) -> Result<VaultResponse, StatusCode> {
        put_vault(
            State(state.clone()),
            Extension(auth),
            Json(SaveVaultRequest {
                payload: payload.to_string(),
                revision,
            }),
        )
        .await
        .map(|(_, Json(vault))| vault)
        .map_err(|error| error.into_response().status())
    }

    async fn delete(
        state: &Arc<ServerState>,
        auth: Option<AuthContext>,
    ) -> Result<StatusCode, StatusCode> {
        delete_vault(State(state.clone()), Extension(auth))
            .await
            .map_err(|error| error.into_response().status())
    }

    #[tokio::test]
    async fn reads_empty_state() {
        // Before the first save: null payload, revision 0, null time.
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        let vault = read(&state, Some(auth)).await.unwrap();
        assert_eq!(
            (vault.payload, vault.revision, vault.updated_at),
            (None, 0, None)
        );
    }

    #[tokio::test]
    async fn saves_and_reads() {
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        let saved = save(&state, Some(auth.clone()), "sealed", Some(0))
            .await
            .unwrap();
        assert_eq!(
            (saved.payload.as_deref(), saved.revision),
            (Some("sealed"), 1)
        );
        assert!(saved.updated_at.is_some());

        let vault = read(&state, Some(auth.clone())).await.unwrap();
        assert_eq!(vault.payload.as_deref(), Some("sealed"));
        assert_eq!((vault.revision, vault.updated_at), (1, saved.updated_at));

        // Without a revision the save overwrites.
        let again = save(&state, Some(auth), "sealed again", None)
            .await
            .unwrap();
        assert_eq!(
            (again.payload.as_deref(), again.revision),
            (Some("sealed again"), 2)
        );
    }

    #[tokio::test]
    async fn refuses_stale_revision() {
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        save(&state, Some(auth.clone()), "one", None).await.unwrap();
        assert_eq!(
            save(&state, Some(auth.clone()), "two", Some(0))
                .await
                .unwrap_err(),
            StatusCode::CONFLICT
        );
        let vault = read(&state, Some(auth.clone())).await.unwrap();
        assert_eq!((vault.payload.as_deref(), vault.revision), (Some("one"), 1));
        assert_eq!(
            save(&state, Some(auth), "two", Some(1))
                .await
                .unwrap()
                .revision,
            2
        );
    }

    #[tokio::test]
    async fn refuses_large_payload() {
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        let payload = "x".repeat(MAX_USER_VAULT_BYTES + 1);
        assert_eq!(
            save(&state, Some(auth.clone()), &payload, None)
                .await
                .unwrap_err(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(read(&state, Some(auth)).await.unwrap().revision, 0);
    }

    #[tokio::test]
    async fn deletes_twice() {
        // A delete of nothing is a no-op; a delete keeps the revision and a repeat adds nothing.
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        assert_eq!(
            delete(&state, Some(auth.clone())).await.unwrap(),
            StatusCode::NO_CONTENT
        );
        let vault = read(&state, Some(auth.clone())).await.unwrap();
        assert_eq!(
            (vault.payload, vault.revision, vault.updated_at),
            (None, 0, None)
        );

        save(&state, Some(auth.clone()), "sealed", None)
            .await
            .unwrap();
        assert_eq!(
            delete(&state, Some(auth.clone())).await.unwrap(),
            StatusCode::NO_CONTENT
        );
        let vault = read(&state, Some(auth.clone())).await.unwrap();
        assert_eq!((vault.payload, vault.revision), (None, 2));
        assert!(vault.updated_at.is_some());
        assert_eq!(
            delete(&state, Some(auth.clone())).await.unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(read(&state, Some(auth)).await.unwrap().revision, 2);
    }

    #[tokio::test]
    async fn refuses_stale_recreate() {
        // A browser holding the deleted vault's revision cannot overwrite a re-created one.
        let (state, _dir) = setup_state().await;
        let auth = realm_auth(state.get_realm_id());
        save(&state, Some(auth.clone()), "old", Some(0))
            .await
            .unwrap();
        delete(&state, Some(auth.clone())).await.unwrap();
        assert_eq!(
            save(&state, Some(auth.clone()), "new", Some(2))
                .await
                .unwrap()
                .revision,
            3
        );
        assert_eq!(
            save(&state, Some(auth.clone()), "old again", Some(1))
                .await
                .unwrap_err(),
            StatusCode::CONFLICT
        );
        let vault = read(&state, Some(auth)).await.unwrap();
        assert_eq!((vault.payload.as_deref(), vault.revision), (Some("new"), 3));
    }

    #[tokio::test]
    async fn requires_unrestricted_token() {
        // No token is 401 and a path-restricted token is 403 on every route.
        let (state, _dir) = setup_state().await;
        assert_eq!(
            read(&state, None).await.unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            save(&state, None, "sealed", None).await.unwrap_err(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            delete(&state, None).await.unwrap_err(),
            StatusCode::UNAUTHORIZED
        );

        let mut auth = realm_auth(state.get_realm_id());
        auth.path_restrictions = Some(Vec::new());
        assert_eq!(
            read(&state, Some(auth.clone())).await.unwrap_err(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            save(&state, Some(auth.clone()), "sealed", None)
                .await
                .unwrap_err(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            delete(&state, Some(auth)).await.unwrap_err(),
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn maps_store_errors() {
        let status = |error| map_vault_error(error).into_response().status();
        assert_eq!(
            status(VaultStoreError::NotFinished),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(VaultStoreError::Storage(StorageError::TransactionConflict)),
            StatusCode::CONFLICT
        );
        assert_eq!(status(VaultStoreError::Stale), StatusCode::CONFLICT);
        assert_eq!(
            status(VaultStoreError::TooLarge("cap")),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }
}
