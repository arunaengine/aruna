use aruna_core::errors::ParseRealmIdError;
use aruna_core::structs::RealmId;
use aruna_core::types::UserId;
use axum::extract::Request;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::server::ServerState;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject: user identity in format `{user_ulid}@{realm_pubkey_base64}`.
    pub sub: String,
    /// Issuer: realm public key (base64-encoded).
    pub iss: String,
    /// Issued at: Unix timestamp in seconds.
    pub iat: u64,
    /// Expiration: Unix timestamp in seconds.
    pub exp: u64,
    /// JWT ID: unique token identifier (ULID string).
    pub jti: String,
}

#[derive(Debug)]
pub struct OidcValidator {}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: UserId,
    pub realm_id: RealmId,
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("Error decoding AuthContext")]
    DecodingError,
    #[error(transparent)]
    IntoUlid(#[from] ulid::DecodeError),
    #[error(transparent)]
    IntoRealmId(#[from] ParseRealmIdError),
}

impl TryFrom<TokenClaims> for AuthContext {
    type Error = AuthorizationError;

    fn try_from(value: TokenClaims) -> Result<Self, Self::Error> {
        let (user, realm) = value
            .sub
            .split_once('@')
            .ok_or_else(|| AuthorizationError::DecodingError)?;
        let user_id = Ulid::from_string(&user)?;
        let realm_id = RealmId::from_base64(realm)?;

        Ok(Self { user_id, realm_id })
    }
}

async fn extract_auth_context(state: &ServerState, headers: &HeaderMap) -> Option<AuthContext> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))?;
    let keypair = state.get_keypair()?;
    let decoding_key = DecodingKey::from_secret(&keypair);
    let claims = decode::<TokenClaims>(token, &decoding_key, &Validation::default()).ok()?;
    claims.claims.try_into().ok()
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
    use crate::server::ServerState;
    use aruna_core::structs::RealmId;
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use axum::http::HeaderMap;
    use std::sync::Arc;

    #[tokio::test]
    pub async fn test_middleware() {
        // Test setup
        let storage_handle = storage::FjallStorage::open("/tmp/aruna_test_db").unwrap();
        let driver_ctx = Arc::new(DriverContext { storage_handle });
        let realm_id = Some(RealmId([0u8; 32]));
        let realm_keypair = Some([0u8; 64]);
        let state = ServerState::new(driver_ctx, realm_keypair, realm_id, None);

        // Token setup
        let token = todo!("Create Token");

        // Header setup
        let mut headers = HeaderMap::new();
        headers.insert("Bearer", token);

        let ctx = extract_auth_context(&state, &headers).await;

        ctx.unwrap();
    }
}
