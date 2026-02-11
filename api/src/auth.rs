use aruna_core::errors::ParseRealmIdError;
use aruna_core::structs::{RealmId, TokenClaims};
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
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use axum::http::{HeaderMap, header};
    use std::sync::Arc;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_middleware() {
        // Test setup
        println!("Server setup");
        let storage_handle = storage::FjallStorage::open("/tmp/aruna_test_db").unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
        });
        let realm_id = Some(RealmId([0u8; 32]));
        let realm_keypair = Some([0u8; 64]);
        let state = ServerState::new(driver_ctx.clone(), realm_keypair, realm_id.clone(), None);

        // Token setup
        println!("Token setup");
        let token_config = CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id: Ulid::new(),
            realm_id: realm_id.clone().unwrap(),
            keypair: realm_keypair.unwrap(),
        };
        let token_operation = CreateTokenOperation::new(token_config.clone());
        let token = drive(token_operation, &driver_ctx).await.unwrap();

        // Header setup
        println!("Header setup");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );

        println!("Test");
        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id.unwrap());
        assert_eq!(ctx.user_id, token_config.user_id);
    }
}
