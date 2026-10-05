//! Routes that publish the caller's public keys and read the public keys of a user.
//! Key records share the placement of the user's vault; the node stores their fingerprint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::vault::{append, read};
use crate::auth::{ValidatedBearer, require_realm_auth, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::forwarded_auth_token;
use crate::routes::access::sessions::unix_rfc3339;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::effects::VaultQuery;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::user::vault::{KEY_ID_BYTES, UserKeyRecord, VaultRecords};
use aruna_operations::s3::holder_seal::seal_published;
use aruna_operations::users::vault_write::{VaultAppended, VaultChange};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct PublishKeyRequest {
    /// The id of the keypair in the vault `keys` slot.
    pub key_id: String,
    /// Standard base64 of the 32-byte X25519 public key.
    pub public_key: String,
    /// Whether the caller's vault holds a recovery code.
    pub has_recovery: bool,
}

/// One published public key of a user.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct UserKeyResponse {
    pub record_id: String,
    pub key_id: String,
    /// Standard base64 of the 32-byte X25519 public key.
    pub public_key: String,
    /// Lowercase hex of the SHA-256 of the public key.
    pub fingerprint: String,
    pub has_recovery: bool,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct UserKeysResponse {
    /// Newest first; the first key is the one to seal to.
    pub keys: Vec<UserKeyResponse>,
}

fn key_response(record: UserKeyRecord) -> UserKeyResponse {
    UserKeyResponse {
        record_id: record.record_id.to_string(),
        key_id: record.key_id,
        public_key: STANDARD.encode(record.public_key),
        fingerprint: hex::encode(record.fingerprint),
        has_recovery: record.has_recovery,
        created_at: unix_rfc3339(record.created_at_ms / 1000),
    }
}

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(publish_key))
        .routes(routes!(list_keys))
}

#[utoipa::path(
    post,
    path = "/access/users/me/keys",
    tag = "access/users",
    summary = "Publish one of the caller's public keys",
    description = r#"Publishes an X25519 public key of the calling user to the key directory.

**Authentication**: unrestricted realm bearer token of this realm; a path-restricted token is
refused. A caller publishes only their own keys.

**Behavior**
- The record is stored on the holders of the user's vault placement, like the vault. A node that
  is no holder forwards it with the caller's token.
- Records are immutable. Publishing a key again, for example after adding a recovery code, adds a
  newer record; readers use the newest one.
- The node stores the SHA-256 fingerprint of the public key with it.
- `has_recovery` is declared by the client; the node cannot see inside the vault.
- Encrypted buckets on this node whose key is unlocked seal a copy of their key to the new
  record when the caller holds them; locked buckets seal it at their next unlock.

**Limits**
- `key_id` holds 1 to 128 bytes. A user may publish at most 64 key records."#,
    request_body(
        content = PublishKeyRequest,
        description = "The public key and the id of its keypair in the vault",
        example = json!({
            "key_id": "01K6A5R7C8D9E0F1G2H3J4K5M6",
            "public_key": "9h8Ue2YJHkVQ7c3X0s1uK5u9Zr3l1pYv8TgC0cJz2kQ=",
            "has_recovery": true
        })
    ),
    responses(
        (status = 201, description = "The published key record", body = UserKeyResponse,
            example = json!({
                "record_id": "01K6A5S1T2V3W4X5Y6Z7A8B9C0",
                "key_id": "01K6A5R7C8D9E0F1G2H3J4K5M6",
                "public_key": "9h8Ue2YJHkVQ7c3X0s1uK5u9Zr3l1pYv8TgC0cJz2kQ=",
                "fingerprint": "5b1f0c0d8a4e3f2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b",
                "has_recovery": true,
                "created_at": "2026-10-01T12:30:00Z"
            })),
        (status = 400, description = "The key id or public key is malformed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm or carries path restrictions", body = ErrorResponse),
        (status = 409, description = "The user has published the most key records allowed", body = ErrorResponse),
        (status = 503, description = "No vault holder accepted the record", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn publish_key(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Json(request): Json<PublishKeyRequest>,
) -> ServerResult<(StatusCode, Json<UserKeyResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if request.key_id.is_empty() || request.key_id.len() > KEY_ID_BYTES {
        return Err(ServerError::BadRequestReason(
            "key_id must hold 1 to 128 bytes".to_string(),
        ));
    }
    let public_key: [u8; 32] = STANDARD
        .decode(&request.public_key)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| {
            ServerError::BadRequestReason("public_key must be 32 bytes of base64".to_string())
        })?;
    let change = VaultChange::PublishKey {
        key_id: request.key_id,
        public_key,
        has_recovery: request.has_recovery,
    };
    let auth_token = forwarded_auth_token(bearer_token)?;
    match append(&state, &auth, auth_token, change).await? {
        VaultAppended::Key(record) => {
            let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
            // Buckets on this node seal the holder's missing copies now; locked ones at unlock.
            if let Err(error) = seal_published(&state.get_ctx(), realm_id, node_id, &record).await {
                tracing::warn!(%error, "could not seal bucket key copies for a new user key");
            }
            Ok((StatusCode::CREATED, Json(key_response(*record))))
        }
        VaultAppended::Heads(_) => Err(ServerError::InternalError(
            "key publish answered vault heads".to_string(),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/access/users/{id}/keys",
    tag = "access/users",
    summary = "Read a user's public keys",
    description = r#"Returns the published X25519 public key records of one user, newest first.

**Authentication**: realm bearer token of this realm. Public keys are not secret.

**Behavior**
- The records are read from the holders of the user's vault placement.
- An empty list means the user has published no key.
- 503 means no holder answered; it never means the user has no key."#,
    params(("id" = String, Path, description = "User id in the form `<ulid>@<realm>`")),
    responses(
        (status = 200, description = "The user's key records", body = UserKeysResponse,
            example = json!({
                "keys": [{
                    "record_id": "01K6A5S1T2V3W4X5Y6Z7A8B9C0",
                    "key_id": "01K6A5R7C8D9E0F1G2H3J4K5M6",
                    "public_key": "9h8Ue2YJHkVQ7c3X0s1uK5u9Zr3l1pYv8TgC0cJz2kQ=",
                    "fingerprint": "5b1f0c0d8a4e3f2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b",
                    "has_recovery": true,
                    "created_at": "2026-10-01T12:30:00Z"
                }]
            })),
        (status = 400, description = "The user id is malformed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 503, description = "No vault holder answered", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_keys(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<(StatusCode, Json<UserKeysResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let user_id = UserId::from_str(&id).map_err(|_| ServerError::BadRequest)?;
    if user_id.realm_id != auth.realm_id {
        return Err(ServerError::BadRequest);
    }
    let mut keys = match read(&state, user_id, VaultQuery::Keys).await? {
        VaultRecords::Keys(keys) => keys,
        VaultRecords::Heads(_) => {
            return Err(ServerError::InternalError(
                "key read answered vault heads".to_string(),
            ));
        }
    };
    keys.sort_by(|left, right| {
        (right.created_at_ms, right.record_id).cmp(&(left.created_at_ms, left.record_id))
    });
    let keys = keys.into_iter().map(key_response).collect();
    Ok((StatusCode::OK, Json(UserKeysResponse { keys })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::access::users::vault::tests::{status, vault_realm};
    use crate::tests::users::realm_auth;

    #[tokio::test]
    async fn publishes_keys() {
        let (state, _dir, auth) = vault_realm(true).await;
        let publish = |key_id: &str, public_key: String| {
            publish_key(
                State(state.clone()),
                Extension(Some(auth.clone())),
                Extension(None),
                Json(PublishKeyRequest {
                    key_id: key_id.to_string(),
                    public_key,
                    has_recovery: true,
                }),
            )
        };
        let public_key = [7u8; 32];
        let (code, Json(published)) = publish("key-1", STANDARD.encode(public_key)).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        assert_eq!(
            published.fingerprint,
            hex::encode(aruna_core::vault_format::key_fingerprint(&public_key))
        );
        assert_eq!(
            status(
                publish("key-1", STANDARD.encode([7u8; 16]))
                    .await
                    .unwrap_err()
            ),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(publish("", STANDARD.encode(public_key)).await.unwrap_err()),
            StatusCode::BAD_REQUEST
        );
        // Any realm user reads the public keys.
        let reader = realm_auth(state.get_realm_id());
        let (_, Json(listed)) = list_keys(
            State(state.clone()),
            Extension(Some(reader)),
            Path(auth.user_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(listed.keys.len(), 1);
        assert_eq!(listed.keys[0].public_key, STANDARD.encode(public_key));
    }
}
