use crate::auth::{handle_token, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult, TokenError};
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    tags((name = "tokens", description = "Bearer token revocation")),
    paths(revoke_token)
)]
pub struct TokensApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/users/tokens/revoke", post(revoke_token))
}

#[derive(Debug, Clone, Deserialize, Serialize, ToSchema)]
pub struct RevokeTokenRequest {
    pub token: String,
}

#[utoipa::path(
    post,
    path = "/users/tokens/revoke",
    tag = "tokens",
    request_body = RevokeTokenRequest,
    responses(
        (status = 204, description = "Token revoked"),
        (status = 400, description = "Not a valid bearer token of this realm", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<RevokeTokenRequest>,
) -> ServerResult<StatusCode> {
    require_realm_auth(&state, auth)?;
    // Validate the target is a real bearer token of a trusted realm before
    // recording it, so the blacklist cannot be filled with arbitrary strings.
    // An already-revoked token is accepted so revocation stays idempotent.
    match handle_token(&state, &request.token).await {
        Ok(_) | Err(TokenError::TokenBlacklisted) => {
            state.add_token_to_blacklist(&request.token).await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(_) => Err(ServerError::BadRequest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{Actor, NodeCapabilities, RealmId};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;

    async fn state_with_token() -> (Arc<ServerState>, RealmId, UserId, String) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_signing_key: SigningKey = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: UserId::nil(realm_id),
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &ctx,
        )
        .await
        .unwrap();
        let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
        let state = Arc::new(
            ServerState::new(
                ctx.clone(),
                realm_id,
                node_id,
                capabilities.clone(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time: chrono::Utc::now().timestamp() as u64,
                expiry: None,
                user_id,
                realm_id,
                node_capabilities: capabilities,
            })
            .unwrap(),
            &ctx,
        )
        .await
        .unwrap();
        (state, realm_id, user_id, token)
    }

    fn caller(realm_id: RealmId, user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        })
    }

    #[tokio::test]
    async fn unauthorized_cannot_revoke() {
        let (state, _realm, _user, token) = state_with_token().await;
        let error = revoke_token(
            State(state.clone()),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Unauthorized));
        assert!(!state.is_token_blacklisted(&token).await);
    }

    #[tokio::test]
    async fn revocation_is_idempotent() {
        let (state, realm_id, user_id, token) = state_with_token().await;
        for _ in 0..2 {
            let status = revoke_token(
                State(state.clone()),
                Extension(caller(realm_id, user_id)),
                Json(RevokeTokenRequest {
                    token: token.clone(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(status, StatusCode::NO_CONTENT);
        }
        assert!(state.is_token_blacklisted(&token).await);
        assert!(matches!(
            handle_token(&state, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }

    #[tokio::test]
    async fn revocation_survives_restart() {
        let (state, realm_id, user_id, token) = state_with_token().await;
        revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, user_id)),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap();

        // A fresh state over the same storage loads the persisted revocation.
        let restarted = ServerState::new(
            state.get_ctx(),
            realm_id,
            state.get_node_id(),
            NodeCapabilities::local_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        assert!(restarted.is_token_blacklisted(&token).await);
    }
}
