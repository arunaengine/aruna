use crate::error::TokenError;
use crate::server_state::ServerState;
use aruna_core::errors::ConversionError;
use aruna_core::structs::{AuthContext, RealmId, TokenClaims};
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use http::HeaderMap;
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Validation, decode};
use s3s::header;
use std::str::FromStr;
use thiserror::Error;
use tracing::warn;

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
    let auth_context: AuthContext = token.try_into().ok()?;
    if let Err(error) = state.claim_initial_realm_admin(&auth_context).await {
        warn!(error = %error, "Failed to claim initial realm admin");
    }
    Some(auth_context)
}

pub async fn handle_token(state: &ServerState, token: &str) -> Result<TokenClaims, TokenError> {
    let unvalidated_claims = insecure_decode::<TokenClaims>(token)?;

    // - Check token hash against revocation list
    if state.is_token_blacklisted(token).await {
        return Err(TokenError::TokenBlacklisted);
    }

    let claims = match (
        unvalidated_claims.claims.issuer_pubkey,
        unvalidated_claims.claims.delegation_signature.is_some(),
    ) {
        (Some(issuer), true) => {
            let decoding_key = state.get_cached_pubkey(issuer).await?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
        (_, _) => {
            let pubkey = state
                .get_cached_pubkey(unvalidated_claims.claims.iss)
                .await?;

            decode::<TokenClaims>(
                token,
                &pubkey,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
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
    use crate::server::ServerState;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::{Actor, NodeCapabilities, RealmAuthorizationDocument, RealmId};
    use aruna_net::{NetConfig, NetHandle};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use axum::http::{HeaderMap, header};
    use base64::Engine;
    use byteview::ByteView;
    use chrono::Days;
    use ed25519_dalek::SigningKey;
    use jsonwebtoken::signature::SignerMut;
    use std::sync::Arc;
    use tempfile::env::temp_dir;
    use ulid::Ulid;
    use aruna_tasks::TaskHandle;

    async fn read_auth_doc(driver_ctx: &DriverContext, realm_id: &RealmId) -> RealmAuthorizationDocument {
        match driver_ctx
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: AUTH_KEYSPACE.to_string(),
                key: ByteView::from(*realm_id.as_bytes()),
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => RealmAuthorizationDocument::from_bytes(&bytes).unwrap(),
            other => panic!("unexpected auth doc read result: {other:?}"),
        }
    }

    #[tokio::test]
    pub async fn test_token_capabilities() {
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
            blob_handle: None,
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
            node_id,
            capabilities.clone(),
            false,
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
            node_id,
            capabilities.clone(),
            false,
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
            node_id,
            capabilities.clone(),
            false,
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
    pub async fn test_first_local_user_claims_initial_realm_admin() {
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                use_dns_discovery: false,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let task_handle = TaskHandle::new();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net_handle.clone()),
            automerge_handle: None,
            task_handle: Some(task_handle),
            blob_handle: None,
        });

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());

        let node_signing_key: SigningKey = SigningKey::generate(&mut csprng);
        let node_id =
            iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: Ulid::from_bytes([0u8; 16]),
                    realm_id: realm_id.clone(),
                },
                realm_description: "Realm".to_string(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id.clone(),
            node_id,
            capabilities.clone(),
            true,
            None,
        )
        .await;

        let time = chrono::Utc::now().timestamp() as u64;
        let first_user = Ulid::new();
        let first_token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time,
                expiry: None,
                user_id: first_user,
                realm_id: realm_id.clone(),
                node_capabilities: capabilities.clone(),
            })
            .unwrap(),
            &driver_ctx,
        )
        .await
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", first_token)).unwrap(),
        );
        let auth_context = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(auth_context.user_id, first_user);

        let auth_doc = read_auth_doc(&driver_ctx, &realm_id).await;
        assert!(auth_doc.roles.values().any(|role| {
            role.name == "realm_admin" && role.assigned_users.contains(&first_user)
        }));

        let second_user = Ulid::new();
        let second_token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time,
                expiry: None,
                user_id: second_user,
                realm_id: realm_id.clone(),
                node_capabilities: capabilities,
            })
            .unwrap(),
            &driver_ctx,
        )
        .await
        .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {}", second_token)).unwrap(),
        );
        let auth_context = extract_auth_context(&state, &headers).await.unwrap();
        assert_eq!(auth_context.user_id, second_user);

        let auth_doc = read_auth_doc(&driver_ctx, &realm_id).await;
        let realm_admin = auth_doc
            .roles
            .values()
            .find(|role| role.name == "realm_admin")
            .unwrap();
        assert!(realm_admin.assigned_users.contains(&first_user));
        assert!(!realm_admin.assigned_users.contains(&second_user));

        net_handle.shutdown().await;
    }

    #[tokio::test]
    pub async fn test_token_validation() {
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            automerge_handle: None,
            task_handle: None,
            blob_handle: None,
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
            node_id,
            capabilities.clone(),
            false,
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
            node_id,
            capabilities.clone(),
            false,
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
            node_id,
            capabilities.clone(),
            false,
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
            node_id,
            capabilities.clone(),
            false,
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
