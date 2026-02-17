use aruna_core::errors::ParseRealmIdError;
use aruna_core::structs::{RealmId, TokenClaims};
use aruna_core::types::UserId;
use axum::extract::Request;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use jsonwebtoken::{DecodingKey, Validation, decode};
use thiserror::Error;
use ulid::Ulid;

use crate::server_state::ServerState;

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
    let signing_key = SigningKey::from_keypair_bytes(&keypair).ok()?;
    let verifying_key = signing_key
        .verifying_key()
        .to_public_key_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::default())
        .ok()?;

    let decoding_key = DecodingKey::from_ed_pem(verifying_key.as_bytes()).ok()?;
    let claims = decode::<TokenClaims>(
        token,
        &decoding_key,
        &Validation::new(jsonwebtoken::Algorithm::EdDSA),
    )
    .ok()?;
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
    use crate::server_state::ServerState;
    use aruna_core::structs::RealmId;
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use axum::http::{HeaderMap, header};
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;
    use tempfile::env::temp_dir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_middleware() {
        let tempdir = temp_dir();
        let storage_handle = storage::FjallStorage::open(&tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let pubkey = signing_key.verifying_key().to_bytes();
        let realm_id = Some(RealmId::from_bytes(pubkey));
        let realm_keypair = Some(signing_key.to_keypair_bytes());

        let state = ServerState::new(driver_ctx.clone(), realm_keypair, realm_id.clone(), None);

        let token_config = CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id: Ulid::new(),
            realm_id: realm_id.clone().unwrap(),
            keypair: realm_keypair.unwrap(),
        };
        let token_operation = CreateTokenOperation::new(token_config.clone());
        let token = drive(token_operation, &driver_ctx).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
        );

        let ctx = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(ctx.realm_id, realm_id.unwrap());
        assert_eq!(ctx.user_id, token_config.user_id);
    }
}
