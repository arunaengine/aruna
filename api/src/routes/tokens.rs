use crate::auth::{
    ValidatedArunaBearerTokenCarrier, claims_for_revocation, ensure_permission, require_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::map_metadata_api_error;
use crate::server_state::ServerState;
use aruna_core::auth::{bearer_token_hash, valid_revocation_expiry};
use aruna_core::structs::{Actor, AuthContext, Permission};
use aruna_core::util::unix_timestamp_secs;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::forwarded_bearer;
use aruna_operations::metadata::forward::{forward_token_revoke, is_user_origin};
use aruna_operations::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "tokens", description = "Bearer token revocation"))
)]
pub struct TokensApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(TokensApiDoc::openapi()).routes(routes!(revoke_token))
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
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 503, description = "No eligible revocation peer available or token revocation capacity exhausted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<RevokeTokenRequest>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let ctx = state.get_ctx();
    let user_origin = is_user_origin(&ctx, state.get_realm_id(), state.get_node_id())
        .await
        .map_err(map_metadata_api_error)?;
    // Only a real bearer token of a trusted realm may enter the revocation set,
    // and only its own subject or a realm admin may revoke it, so a token holder
    // cannot invalidate other users' sessions.
    let claims = claims_for_revocation(&state, &request.token)
        .await
        .map_err(|_| ServerError::BadRequest)?;
    let expires_at = claims.exp;
    let now = unix_timestamp_secs();
    if !valid_revocation_expiry(expires_at, now) {
        return Err(ServerError::BadRequest);
    }
    let subject: AuthContext = claims.try_into().map_err(|_| ServerError::BadRequest)?;
    if subject.realm_id != state.get_realm_id() {
        return Err(ServerError::BadRequest);
    }
    // A path-restricted (delegated) token stays confined: it may retire itself,
    // but revoking the subject's other sessions needs administrative authority.
    let self_service = auth.user_id == subject.user_id
        && (auth.path_restrictions.is_none()
            || revokes_self(bearer_token.as_ref(), &request.token));
    if !self_service && !user_origin {
        ensure_permission(
            &state,
            &auth,
            format!("/{}/admin/u/{}", auth.realm_id, subject.user_id),
            Permission::WRITE,
        )
        .await?;
    }

    if user_origin {
        let caller_token = bearer_token.as_ref().ok_or(ServerError::Unauthorized)?;
        let auth_token = forwarded_bearer(Some(caller_token.as_str()))
            .map_err(map_metadata_api_error)?
            .ok_or(ServerError::Unauthorized)?;
        forward_token_revoke(
            &ctx,
            state.get_realm_id(),
            auth_token,
            request.token.clone(),
        )
        .await
        .map_err(map_metadata_api_error)?;
        return Ok(StatusCode::NO_CONTENT);
    }

    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            token_hash: bearer_token_hash(&request.token),
            expires_at,
            token_owner: subject.user_id,
            admission: if self_service {
                RevokeTokenAdmission::SelfService
            } else {
                RevokeTokenAdmission::Privileged
            },
            now,
        }),
        ctx.as_ref(),
    )
    .await
    .map_err(map_revoke_error)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Whether the caller presented the very token it asks to revoke.
fn revokes_self(caller: Option<&ValidatedArunaBearerTokenCarrier>, token: &str) -> bool {
    caller.is_some_and(|caller| bearer_token_hash(caller.as_str()) == bearer_token_hash(token))
}

fn map_revoke_error(error: RevokeTokenError) -> ServerError {
    match error {
        RevokeTokenError::CapacityReached => {
            ServerError::ServiceUnavailableReason("token_revocation_capacity_reached".to_string())
        }
        RevokeTokenError::InvalidTokenExpiry => ServerError::BadRequest,
        error => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::handle_token;
    use crate::error::TokenError;
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{Actor, NodeCapabilities, PathRestriction, RealmId, TokenRevocation};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::get_realm_config::GetRealmConfigOperation;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use axum::response::IntoResponse;
    use ed25519_dalek::SigningKey;
    use ulid::Ulid;

    #[test]
    fn capacity_maps_503() {
        let response = map_revoke_error(RevokeTokenError::CapacityReached).into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    async fn state_with_token() -> (tempfile::TempDir, Arc<ServerState>, RealmId, UserId, String) {
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
        (dir, state, realm_id, user_id, token)
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
        let (_dir, state, _realm, _user, token) = state_with_token().await;
        let error = revoke_token(
            State(state.clone()),
            Extension(None),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ServerError::Unauthorized));
        assert!(handle_token(&state, &token).await.is_ok());
    }

    #[tokio::test]
    async fn revocation_is_idempotent() {
        let (_dir, state, realm_id, user_id, token) = state_with_token().await;
        for _ in 0..2 {
            let status = revoke_token(
                State(state.clone()),
                Extension(caller(realm_id, user_id)),
                Extension(None),
                Json(RevokeTokenRequest {
                    token: token.clone(),
                }),
            )
            .await
            .unwrap();
            assert_eq!(status, StatusCode::NO_CONTENT);
        }
        assert!(matches!(
            handle_token(&state, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }

    #[tokio::test]
    async fn revocation_reaches_config() {
        // The route must record the revocation in the replicated realm config,
        // which is what carries it to the other nodes of the realm.
        let (_dir, state, realm_id, user_id, token) = state_with_token().await;
        revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, user_id)),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap();

        let config = drive(
            GetRealmConfigOperation::new(realm_id),
            state.get_ctx().as_ref(),
        )
        .await
        .unwrap();
        assert!(config.token_revoked(&bearer_token_hash(&token), unix_timestamp_secs()));
    }

    #[tokio::test]
    async fn replicated_revocation_rejects() {
        // A revocation that only arrived through the replicated realm config
        // must deny the token here and after a restart that rebuilds state.
        let (_dir, state, realm_id, _user, token) = state_with_token().await;
        assert!(handle_token(&state, &token).await.is_ok());

        let ctx = state.get_ctx();
        let mut config = drive(GetRealmConfigOperation::new(realm_id), ctx.as_ref())
            .await
            .unwrap();
        config.revoked_tokens.push(TokenRevocation {
            token_hash: bearer_token_hash(&token),
            expires_at: unix_timestamp_secs() + 600,
        });
        write_realm_config(ctx.as_ref(), realm_id, &config).await;

        assert!(matches!(
            handle_token(&state, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));

        let restarted = ServerState::new(
            ctx.clone(),
            realm_id,
            state.get_node_id(),
            NodeCapabilities::local_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        assert!(matches!(
            handle_token(&restarted, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }

    async fn write_realm_config(
        ctx: &DriverContext,
        realm_id: RealmId,
        config: &aruna_core::structs::RealmConfigDocument,
    ) {
        let target = aruna_core::document::DocumentSyncTarget::RealmConfig { realm_id };
        store_bytes(
            ctx,
            target.storage_keyspace(),
            target.storage_key().to_vec(),
            postcard::to_allocvec(config).unwrap(),
        )
        .await;
    }

    /// Assigns every realm role, including `realm_admin`, to one user.
    async fn grant_realm_admin(ctx: &DriverContext, realm_id: RealmId, user_id: UserId) {
        let mut auth_doc =
            aruna_core::structs::RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        for role in auth_doc.roles.values_mut() {
            role.assigned_users.insert(user_id);
        }
        store_bytes(
            ctx,
            aruna_core::keyspaces::AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&auth_doc).unwrap(),
        )
        .await;
    }

    async fn store_bytes(ctx: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        match ctx
            .storage_handle
            .send_storage_effect(aruna_core::effects::StorageEffect::Write {
                key_space: key_space.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await
        {
            aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::WriteResult {
                ..
            }) => {}
            other => panic!("unexpected write result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn stranger_cannot_revoke() {
        // A realm user who neither owns the token nor holds the realm admin
        // write must not be able to invalidate someone else's session.
        let (_dir, state, realm_id, _user, token) = state_with_token().await;
        let stranger = UserId::local(Ulid::generate(), realm_id);
        let error = revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, stranger)),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ServerError::Forbidden));
        assert!(handle_token(&state, &token).await.is_ok());
        let config = drive(
            GetRealmConfigOperation::new(realm_id),
            state.get_ctx().as_ref(),
        )
        .await
        .unwrap();
        assert!(config.revoked_tokens.is_empty());
    }

    #[tokio::test]
    async fn delegate_cannot_revoke() {
        // A path-restricted delegation of a user must not retire that user's
        // unrestricted sessions, which its confinement does not cover.
        let (_dir, state, realm_id, user_id, token) = state_with_token().await;
        let delegate = Some(AuthContext {
            user_id,
            realm_id,
            path_restrictions: Some(vec![PathRestriction {
                pattern: format!("/{realm_id}/g/**"),
                permission: Permission::READ,
            }]),
        });

        let error = revoke_token(
            State(state.clone()),
            Extension(delegate),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ServerError::Forbidden));
        assert!(handle_token(&state, &token).await.is_ok());
    }

    #[tokio::test]
    async fn admin_can_revoke() {
        // The realm admin gate that guards non-self user writes also permits
        // revoking another user's token.
        let (_dir, state, realm_id, _user, token) = state_with_token().await;
        let admin = UserId::local(Ulid::generate(), realm_id);
        grant_realm_admin(state.get_ctx().as_ref(), realm_id, admin).await;

        let status = revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, admin)),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap();

        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(matches!(
            handle_token(&state, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }

    #[tokio::test]
    async fn foreign_token_rejected() {
        // A trusted foreign token must not enter this realm's revocation log.
        let (_dir, state, realm_id, _user, _token) = state_with_token().await;
        let foreign_signing_key = generate_signing_key();
        let foreign_realm_id = RealmId::from_bytes(foreign_signing_key.verifying_key().to_bytes());
        let foreign_user = UserId::local(Ulid::generate(), foreign_realm_id);
        let foreign_capabilities = NodeCapabilities::management_node(foreign_signing_key).unwrap();
        let foreign_token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time: chrono::Utc::now().timestamp() as u64,
                expiry: None,
                user_id: foreign_user,
                realm_id: foreign_realm_id,
                node_capabilities: foreign_capabilities,
            })
            .unwrap(),
            &state.get_ctx(),
        )
        .await
        .unwrap();
        state.add_trusted_realm(foreign_realm_id).await;
        // Revocation lookups fail closed, so a trusted realm's token is only
        // usable here once that realm's replicated config is present.
        write_realm_config(
            state.get_ctx().as_ref(),
            foreign_realm_id,
            &aruna_core::structs::RealmConfigDocument::default_for_realm(
                foreign_realm_id,
                Vec::new(),
            ),
        )
        .await;

        let admin = UserId::local(Ulid::generate(), realm_id);
        grant_realm_admin(state.get_ctx().as_ref(), realm_id, admin).await;
        let error = revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, admin)),
            Extension(None),
            Json(RevokeTokenRequest {
                token: foreign_token.clone(),
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ServerError::BadRequest));
        assert!(handle_token(&state, &foreign_token).await.is_ok());
        let config = drive(
            GetRealmConfigOperation::new(realm_id),
            state.get_ctx().as_ref(),
        )
        .await
        .unwrap();
        assert!(config.revoked_tokens.is_empty());
    }

    #[tokio::test]
    async fn revocation_survives_restart() {
        let (_dir, state, realm_id, user_id, token) = state_with_token().await;
        revoke_token(
            State(state.clone()),
            Extension(caller(realm_id, user_id)),
            Extension(None),
            Json(RevokeTokenRequest {
                token: token.clone(),
            }),
        )
        .await
        .unwrap();

        // A fresh state over the same storage still denies the revoked token.
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
        assert!(matches!(
            handle_token(&restarted, &token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }
}
