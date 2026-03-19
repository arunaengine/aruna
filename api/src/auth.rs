use aruna_core::errors::ConversionError;
use aruna_core::structs::{AuthContext, RealmId, TokenClaims};
use axum::extract::Request;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Validation, decode};
use std::str::FromStr;
use thiserror::Error;

use crate::error::TokenError;
use crate::server_state::ServerState;

#[derive(Debug)]
pub struct OidcValidator {}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("Error decoding AuthContext")]
    DecodingError,
    #[error(transparent)]
    IntoUlid(#[from] ulid::DecodeError),
    #[error(transparent)]
    IntoRealmId(#[from] ConversionError),
}

async fn extract_auth_context(state: &ServerState, headers: &HeaderMap) -> Option<AuthContext> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))?;

    let token = handle_token(state, token).await.ok()?;
    token.try_into().ok()
}

pub async fn handle_token(state: &ServerState, token: &str) -> Result<TokenClaims, TokenError> {
    let unvalidated_claims = insecure_decode::<TokenClaims>(token)?;

    // - Check token hash against revocation list
    if state.is_token_blacklisted(&token).await {
        return Err(TokenError::TokenBlacklisted);
    }

    let claims = match (
        unvalidated_claims.claims.issuer_pubkey,
        unvalidated_claims.claims.delegation_signature.is_some(),
    ) {
        (Some(issuer), true) => {
            let decoding_key = state.get_cached_pubkey(issuer).await?;
            let claims = decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?;
            claims
        }
        (_, _) => {
            let pubkey = state
                .get_cached_pubkey(unvalidated_claims.claims.iss)
                .await?;
            let claims = decode::<TokenClaims>(
                token,
                &pubkey,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?;
            claims
        }
    };
    validate_claims(state, &claims.claims).await?;
    Ok(claims.claims)
}

pub async fn validate_claims(state: &ServerState, claims: &TokenClaims) -> Result<(), TokenError> {
    let now = chrono::Utc::now().timestamp() as u64;
    if now > claims.exp {
        return Err(TokenError::Expired);
    }

    if !state
        .is_trusted_realm(
            &RealmId::from_base64(&claims.iss).map_err(|_| TokenError::InvalidIssuerKey)?,
        )
        .await
    {
        return Err(TokenError::RealmNotTrusted);
    }

    // Check server token claims
    match (&claims.delegation_signature, &claims.issuer_pubkey) {
        (Some(delegation_token), Some(issuer_pubkey)) => {
            // Check delegation signature
            let realm_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&claims.iss)?;
            let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
            let signature = Signature::from_str(delegation_token)?;
            realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
            Ok(())
        }
        (None, None) => Ok(()),
        (_, _) => Err(TokenError::InvalidServerToken),
    }
}

pub async fn auth_middleware(
    state: axum::extract::State<std::sync::Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Extract and validate token, get Option<AuthContext>
    // We clone headers to avoid borrowing issues with the async function
    let headers = request.headers().clone();
    let auth_ctx: Option<AuthContext> = extract_auth_context(&state, &headers).await;

    // Always insert (Some or None) - handlers decide if auth is required
    request.extensions_mut().insert(auth_ctx);

    // Always continue to handler
    next.run(request).await
}

#[cfg(test)]
mod test {
    use crate::auth::extract_auth_context;
    use crate::server_state::ServerState;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use axum::http::{HeaderMap, header};
    use base64::Engine;
    use chrono::Days;
    use ed25519_dalek::SigningKey;
    use jsonwebtoken::signature::SignerMut;
    use std::sync::Arc;
    use tempfile::env::temp_dir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_token_capabilities() {
        let mut tempdir = temp_dir();
        tempdir.push(format!("{}", Ulid::new().to_string()));
        let storage_handle = storage::FjallStorage::open(&tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let mut realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);

        let node_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let node_id =
            iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

        let time = chrono::Utc::now().timestamp() as u64;
        let expiry = None;
        let user_id = Ulid::new();

        //
        // Test Management Nodes
        //
        let capabilities = NodeCapabilities::management_node(realm_signing_key.clone()).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let management_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id);
        assert_eq!(ctx.user_id, token_config.user_id);

        //
        // Test Server Nodes
        //
        let issuer_key = SigningKey::generate(&mut csprng);

        let message = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let delegation_signature = realm_signing_key.sign(message.as_bytes()).to_string();

        let capabilities =
            NodeCapabilities::server_node(issuer_key, realm_id.clone(), delegation_signature)
                .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let server_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id);
        assert_eq!(ctx.user_id, token_config.user_id);

        //
        // Test Local Nodes
        //
        let capabilities = NodeCapabilities::local_node(realm_id.clone()).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id);
        assert_eq!(ctx.user_id, token_config.user_id);

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id);
        assert_eq!(ctx.user_id, token_config.user_id);
    }

    #[tokio::test]
    pub async fn test_token_validation() {
        let mut tempdir = temp_dir();
        tempdir.push(format!("{}", Ulid::new().to_string()));
        let storage_handle = storage::FjallStorage::open(&tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let mut realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = realm_signing_key.verifying_key().to_bytes();
        let realm_id = RealmId::from_bytes(pubkey);

        let node_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let node_id =
            iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

        let time = chrono::Utc::now().timestamp() as u64;
        let expiry = Some(
            chrono::Utc::now()
                .checked_add_days(Days::new(10))
                .unwrap()
                .timestamp() as u64,
        );
        let user_id = Ulid::new();

        let capabilities = NodeCapabilities::management_node(realm_signing_key.clone()).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        //
        // Valid management token with expiry
        //
        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities.clone(),
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let management_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id);
        assert_eq!(ctx.user_id, token_config.user_id);

        let old_time = chrono::Utc::now()
            .checked_sub_days(Days::new(10))
            .unwrap()
            .timestamp() as u64;
        let expired = Some(chrono::Utc::now().timestamp() as u64 - 1);

        //
        // Expired management token
        //
        let token_config = CreateTokenConfig {
            time: old_time,
            expiry: expired,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let management_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
        );
        assert!(extract_auth_context(&state, &headers).await.is_none());

        //
        // Expired server token
        //
        let issuer_key = SigningKey::generate(&mut csprng);

        let message = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_key.verifying_key().to_bytes());
        let delegation_signature = realm_signing_key.sign(message.as_bytes()).to_string();

        let capabilities = NodeCapabilities::server_node(
            issuer_key.clone(),
            realm_id.clone(),
            delegation_signature.clone(),
        )
        .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let old_time = chrono::Utc::now()
            .checked_sub_days(Days::new(10))
            .unwrap()
            .timestamp() as u64;
        let expired = Some(chrono::Utc::now().timestamp() as u64 - 1);

        let token_config = CreateTokenConfig {
            time: old_time,
            expiry: expired,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let server_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
        );

        assert!(extract_auth_context(&state, &headers).await.is_none());

        //
        // Invalid delegation signature
        //
        let invalid_signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0u8; 32]);
        let capabilities =
            NodeCapabilities::server_node(issuer_key.clone(), realm_id.clone(), invalid_signature)
                .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id: realm_id.clone(),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let server_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
        );
        assert!(extract_auth_context(&state, &headers).await.is_none());

        //
        // Invalid realm key
        //
        let capabilities = NodeCapabilities::server_node(
            issuer_key,
            realm_id.clone(),
            delegation_signature.clone(),
        )
        .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id.clone(),
            capabilities.clone(),
            None,
        )
        .await;

        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id: RealmId([0u8; 32]),
            node_capabilities: capabilities,
        };
        let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
        let server_token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
        );
        assert!(extract_auth_context(&state, &headers).await.is_none());
    }
}
