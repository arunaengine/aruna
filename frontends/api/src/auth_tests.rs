//! Tests token handling, auth extractors and permission paths of the auth module.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{
    CACHE_TTL_SECS, OidcValidator, REFRESH_COOLDOWN_SECS, blob_permission_path,
    extract_auth_context, extract_auth_parts, handle_token,
};
use crate::error::{OidcError, ServerError, TokenError};
use crate::server::state::ServerState;
use aruna_core::auth::bearer_token_hash;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, TokenClaims};
use aruna_core::structs::identity::realm::OidcProviderConfig;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::user::id::UserId;
use aruna_ops_identity::auth::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation,
};
use aruna_ops_identity::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_ops_identity::users::oidc_user::{ResolveOidcInput, ResolveOidcOperation};
use aruna_p2p::NetHandle;
use aruna_p2p::config::{DiscoveryMethod, NetConfig, RelayMethod};
use aruna_runner::driver::{DriverContext, drive};
use aruna_scheduler::TaskHandle;
use aruna_storage::storage;
use axum::Json;
use axum::http::{HeaderMap, header};
use axum::routing::get;
use axum::{Router, extract::State};
use base64::Engine;
use byteview::ByteView;
use chrono::Days;
use ed25519_dalek::Signer;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock};
use ulid::Ulid;

#[test]
fn capacity_is_retryable() {
    // The choke point must not hide a storage fault behind 403 or 500.
    assert!(matches!(
        ServerError::from(AuthorizeError::Storage(StorageError::CleanupCapacity)),
        ServerError::ServiceUnavailableReason(_)
    ));
    assert!(matches!(
        ServerError::from(AuthorizeError::Storage(StorageError::DeleteError)),
        ServerError::InternalError(_)
    ));
    assert!(matches!(
        ServerError::from(AuthorizeError::PermissionDenied),
        ServerError::Forbidden
    ));
}

#[tokio::test]
async fn canonical_blob_path() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let realm_id = RealmId::from_bytes(
        *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
            .verifying_key()
            .as_bytes(),
    );
    let node_id = iroh::SecretKey::generate().public();
    let state = ServerState::new(
        Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }),
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).unwrap(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let group_id = Ulid::from_bytes([9u8; 16]);
    assert_eq!(
        blob_permission_path(&state, group_id, "bucket", "nested/file.txt"),
        aruna_core::structs::storage::blob::object_permission_path(
            realm_id,
            group_id,
            node_id,
            "bucket",
            "nested/file.txt",
        )
    );
}

#[derive(Clone)]
struct OidcServerState {
    issuer: String,
    jwks_uri: String,
    jwks: Arc<RwLock<serde_json::Value>>,
    discovery_requests: Arc<AtomicUsize>,
    jwks_requests: Arc<AtomicUsize>,
    gate: Arc<RwLock<()>>,
    entered: Arc<Notify>,
}

/// A null `jwks` answers 503. A held `gate` write guard keeps key answers pending, and
/// `entered` signals each key request that reached the gate.
#[derive(Clone)]
struct OidcServerMetrics {
    discovery_requests: Arc<AtomicUsize>,
    jwks_requests: Arc<AtomicUsize>,
    jwks: Arc<RwLock<serde_json::Value>>,
    gate: Arc<RwLock<()>>,
    entered: Arc<Notify>,
}

#[derive(Clone, Serialize, Deserialize)]
struct TestOidcClaims {
    sub: String,
    iss: String,
    aud: String,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct MultiAudienceClaims {
    sub: String,
    iss: String,
    aud: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    azp: Option<String>,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

async fn oidc_discovery(State(state): State<OidcServerState>) -> Json<serde_json::Value> {
    state.discovery_requests.fetch_add(1, Ordering::Relaxed);
    Json(serde_json::json!({
        "issuer": state.issuer,
        "jwks_uri": state.jwks_uri,
    }))
}

async fn oidc_jwks(
    State(state): State<OidcServerState>,
) -> Result<Json<serde_json::Value>, http::StatusCode> {
    state.jwks_requests.fetch_add(1, Ordering::Relaxed);
    state.entered.notify_one();
    let _open = state.gate.read().await;
    let jwks = state.jwks.read().await.clone();
    match jwks.is_null() {
        true => Err(http::StatusCode::SERVICE_UNAVAILABLE),
        false => Ok(Json(jwks)),
    }
}

async fn spawn_oidc_provider(
    issuer: &str,
    kid: &str,
    jwks: serde_json::Value,
) -> (
    String,
    String,
    OidcServerMetrics,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let discovery_url = format!("http://{addr}/.well-known/openid-configuration");
    let jwks_uri = format!("http://{addr}/jwks/{kid}.json");
    let discovery_requests = Arc::new(AtomicUsize::new(0));
    let jwks_requests = Arc::new(AtomicUsize::new(0));
    let jwks = Arc::new(RwLock::new(jwks));
    let gate = Arc::new(RwLock::new(()));
    let entered = Arc::new(Notify::new());
    let state = OidcServerState {
        issuer: issuer.to_string(),
        jwks_uri: jwks_uri.clone(),
        jwks: jwks.clone(),
        discovery_requests: discovery_requests.clone(),
        jwks_requests: jwks_requests.clone(),
        gate: gate.clone(),
        entered: entered.clone(),
    };
    let router = Router::new()
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route(&format!("/jwks/{kid}.json"), get(oidc_jwks))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (
        discovery_url,
        jwks_uri,
        OidcServerMetrics {
            discovery_requests,
            jwks_requests,
            jwks,
            gate,
            entered,
        },
        task,
    )
}

fn eddsa_jwk(kid: &str, signing_key: &SigningKey) -> serde_json::Value {
    serde_json::json!({
        "keys": [{
            "kty": "OKP",
            "alg": "EdDSA",
            "use": "sig",
            "kid": kid,
            "crv": "Ed25519",
            "x": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes()),
        }]
    })
}

fn sign_oidc_token(
    issuer: &str,
    audience: &str,
    subject: &str,
    kid: &str,
    signing_key: &SigningKey,
    algorithm: Algorithm,
    name: Option<&str>,
) -> String {
    let mut header = Header::new(algorithm);
    header.kid = Some(kid.to_string());
    let claims = TestOidcClaims {
        sub: subject.to_string(),
        iss: issuer.to_string(),
        aud: audience.to_string(),
        exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
        name: name.map(str::to_string),
    };
    let der = signing_key
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .unwrap();
    encode(
        &header,
        &claims,
        &EncodingKey::from_ed_pem(der.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn sign_audience_token(
    issuer: &str,
    audiences: &[&str],
    azp: Option<&str>,
    subject: &str,
    kid: &str,
    signing_key: &SigningKey,
) -> String {
    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some(kid.to_string());
    let claims = MultiAudienceClaims {
        sub: subject.to_string(),
        iss: issuer.to_string(),
        aud: audiences
            .iter()
            .map(|audience| audience.to_string())
            .collect(),
        azp: azp.map(str::to_string),
        exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
        name: None,
    };
    let der = signing_key
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .unwrap();
    encode(
        &header,
        &claims,
        &EncodingKey::from_ed_pem(der.as_bytes()).unwrap(),
    )
    .unwrap()
}

async fn read_auth_doc(
    driver_ctx: &DriverContext,
    realm_id: &RealmId,
) -> RealmAuthorizationDocument {
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

async fn revoke(driver_ctx: &Arc<DriverContext>, actor: Actor, token: &str) {
    let now = aruna_clock::unix_timestamp_secs();
    let token_owner = actor.user_id;
    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor,
            token_hash: bearer_token_hash(token),
            expires_at: now + 600,
            token_owner,
            admission: RevokeTokenAdmission::SelfService,
            now,
        }),
        driver_ctx,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn valid_token_required() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });
    let realm_signing_key = generate_signing_key();
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
        &driver_ctx,
    )
    .await
    .unwrap();
    let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: chrono::Utc::now().timestamp() as u64,
            expiry: None,
            user_id,
            realm_id,
            node_capabilities: capabilities,

            session: None,
            restrictions: None,
        })
        .unwrap(),
        &driver_ctx,
    )
    .await
    .unwrap()
    .expose()
    .to_string();
    let headers_for = |token: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    };

    let (auth, carrier) = extract_auth_parts(&state, &headers_for(&token)).await;
    assert_eq!(auth.unwrap().user_id, user_id);
    assert_eq!(carrier.unwrap().as_str(), token);

    revoke(
        &driver_ctx,
        Actor {
            node_id,
            user_id,
            realm_id,
        },
        &token,
    )
    .await;
    let (auth, carrier) = extract_auth_parts(&state, &headers_for(&token)).await;
    assert!(auth.is_none());
    assert!(carrier.is_none());

    let oidc_signing_key = generate_signing_key();
    let oidc_token = sign_oidc_token(
        "https://issuer.example",
        "aruna-api",
        "subject-1",
        "main-key",
        &oidc_signing_key,
        Algorithm::EdDSA,
        None,
    );
    let (auth, carrier) = extract_auth_parts(&state, &headers_for(&oidc_token)).await;
    assert!(auth.is_none());
    assert!(carrier.is_none());

    let (auth, carrier) = extract_auth_parts(&state, &headers_for("not-a-jwt")).await;
    assert!(auth.is_none());
    assert!(carrier.is_none());
}

#[tokio::test]
async fn validator_accepts_eddsa() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, _metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        Some("Alice OIDC"),
    );

    let validator = OidcValidator::new().unwrap();
    let identity = validator.validate(&provider, &token).await.unwrap();
    assert_eq!(identity.issuer, issuer);
    assert_eq!(identity.subject_id, "subject-1");
    assert_eq!(identity.display_name.as_deref(), Some("Alice OIDC"));

    task.abort();
}

#[tokio::test]
async fn validator_rejects_hs256() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "symm-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, _metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };

    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(kid.to_string());
    let claims = TestOidcClaims {
        sub: "subject-1".to_string(),
        iss: issuer.to_string(),
        aud: audience.to_string(),
        exp: chrono::Utc::now().timestamp().max(0) as u64 + 600,
        name: None,
    };
    let key = EncodingKey::from_secret(b"super-secret").unwrap();
    let token = encode(&header, &claims, &key).unwrap();

    let validator = OidcValidator::new().unwrap();
    let error = validator.validate(&provider, &token).await.unwrap_err();
    assert!(matches!(
        error,
        crate::error::OidcError::UnsupportedAlgorithm
    ));

    task.abort();
}

#[tokio::test]
async fn validator_rejects_audience() {
    let issuer = "https://issuer.example";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, _metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: "aruna-api".to_string(),
        discovery_url,
    };
    let token = sign_oidc_token(
        issuer,
        "different-audience",
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );

    let validator = OidcValidator::new().unwrap();
    assert!(validator.validate(&provider, &token).await.is_err());

    task.abort();
}

#[tokio::test]
async fn validator_caches_metadata() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );

    let validator = OidcValidator::new().unwrap();
    validator.validate(&provider, &token).await.unwrap();
    validator.validate(&provider, &token).await.unwrap();

    assert_eq!(metrics.discovery_requests.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.jwks_requests.load(Ordering::Relaxed), 1);

    task.abort();
}

#[tokio::test]
async fn validator_refreshes_metadata() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );
    let validator = OidcValidator::new().unwrap();

    validator.validate(&provider, &token).await.unwrap();
    let expired_at = Instant::now()
        .checked_sub(Duration::from_secs(CACHE_TTL_SECS + 1))
        .unwrap();
    validator
        .provider_metadata_cache
        .write()
        .await
        .get_mut(&provider.discovery_url)
        .unwrap()
        .fetched_at = expired_at;
    validator.validate(&provider, &token).await.unwrap();

    assert_eq!(metrics.discovery_requests.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.jwks_requests.load(Ordering::Relaxed), 2);

    task.abort();
}

#[tokio::test]
async fn validator_requires_azp() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, _metrics, task) = spawn_oidc_provider(issuer, kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();

    let missing_azp = sign_audience_token(
        issuer,
        &[audience, "other-client"],
        None,
        "subject-1",
        kid,
        &signing_key,
    );
    assert!(validator.validate(&provider, &missing_azp).await.is_err());

    let wrong_azp = sign_audience_token(
        issuer,
        &[audience, "other-client"],
        Some("other-client"),
        "subject-1",
        kid,
        &signing_key,
    );
    assert!(validator.validate(&provider, &wrong_azp).await.is_err());

    let valid = sign_audience_token(
        issuer,
        &[audience, "other-client"],
        Some(audience),
        "subject-1",
        kid,
        &signing_key,
    );
    let identity = validator.validate(&provider, &valid).await.unwrap();
    assert_eq!(identity.subject_id, "subject-1");

    task.abort();
}

#[tokio::test]
async fn validator_refreshes_jwks() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let old_kid = "old-key";
    let new_kid = "new-key";
    let old_signing_key = generate_signing_key();
    let new_signing_key = generate_signing_key();
    let (discovery_url, _jwks_uri, metrics, task) =
        spawn_oidc_provider(issuer, old_kid, eddsa_jwk(old_kid, &old_signing_key)).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();

    let old_token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        old_kid,
        &old_signing_key,
        Algorithm::EdDSA,
        None,
    );
    validator.validate(&provider, &old_token).await.unwrap();
    // The rotation is seen once the refresh cooldown has passed.
    cool_down(&validator, &provider.discovery_url).await;

    *metrics.jwks.write().await = eddsa_jwk(new_kid, &new_signing_key);

    let new_token = sign_oidc_token(
        issuer,
        audience,
        "subject-2",
        new_kid,
        &new_signing_key,
        Algorithm::EdDSA,
        None,
    );
    let identity = validator.validate(&provider, &new_token).await.unwrap();

    assert_eq!(identity.issuer, issuer);
    assert_eq!(identity.subject_id, "subject-2");
    assert_eq!(metrics.discovery_requests.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.jwks_requests.load(Ordering::Relaxed), 2);

    task.abort();
}

#[tokio::test]
async fn forged_key_throttled() {
    // A forged key id right after a fetch is refused without calling the provider again.
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (discovery_url, _jwks_uri, metrics, task) =
        spawn_oidc_provider(issuer, kid, eddsa_jwk(kid, &signing_key)).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();
    let token = |kid| {
        sign_oidc_token(
            issuer,
            audience,
            "subject-1",
            kid,
            &signing_key,
            Algorithm::EdDSA,
            None,
        )
    };
    validator.validate(&provider, &token(kid)).await.unwrap();
    for _ in 0..3 {
        let forged = validator.validate(&provider, &token("forged-key")).await;
        assert!(matches!(forged, Err(OidcError::SigningNotFound)));
    }
    assert_eq!(metrics.discovery_requests.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.jwks_requests.load(Ordering::Relaxed), 1);

    task.abort();
}

/// Moves the last fetch and the last failed call of a provider past the refresh cooldown.
async fn cool_down(validator: &OidcValidator, discovery_url: &str) {
    let cooled_at = Instant::now()
        .checked_sub(Duration::from_secs(REFRESH_COOLDOWN_SECS + 1))
        .unwrap();
    let mut cache = validator.provider_metadata_cache.write().await;
    if let Some(metadata) = cache.get_mut(discovery_url) {
        metadata.fetched_at = cooled_at;
    }
    let slots = validator.refresh_slots.lock().await;
    *slots.get(discovery_url).unwrap().lock().await = Some(cooled_at);
}

/// Moves the last fetch of a provider past the cache lifetime.
async fn expire_cache(validator: &OidcValidator, discovery_url: &str) {
    let expired_at = Instant::now()
        .checked_sub(Duration::from_secs(CACHE_TTL_SECS + 1))
        .unwrap();
    let mut cache = validator.provider_metadata_cache.write().await;
    cache.get_mut(discovery_url).unwrap().fetched_at = expired_at;
}

#[tokio::test]
async fn overlapping_loads_coalesce() {
    // Logins that overlap one pending key answer share it instead of calling the provider.
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (discovery_url, _jwks_uri, metrics, task) =
        spawn_oidc_provider(issuer, kid, eddsa_jwk(kid, &signing_key)).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );
    let held = metrics.gate.clone().write_owned().await;
    let release = async {
        metrics.entered.notified().await;
        drop(held);
    };
    let (first, second, third, ()) = tokio::join!(
        validator.validate(&provider, &token),
        validator.validate(&provider, &token),
        validator.validate(&provider, &token),
        release,
    );
    for identity in [first, second, third] {
        assert_eq!(identity.unwrap().subject_id, "subject-1");
    }
    assert_eq!(metrics.discovery_requests.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.jwks_requests.load(Ordering::Relaxed), 1);

    task.abort();
}

#[tokio::test]
async fn failed_refresh_cools() {
    // A failed key refresh starts the cooldown and keeps the cached keys, then a later
    // refresh calls the provider again.
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let new_kid = "new-key";
    let signing_key = generate_signing_key();
    let new_signing_key = generate_signing_key();
    let (discovery_url, _jwks_uri, metrics, task) =
        spawn_oidc_provider(issuer, kid, eddsa_jwk(kid, &signing_key)).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();
    let token = |kid, signing_key| {
        sign_oidc_token(
            issuer,
            audience,
            "subject-1",
            kid,
            signing_key,
            Algorithm::EdDSA,
            None,
        )
    };
    let calls = || {
        (
            metrics.discovery_requests.load(Ordering::Relaxed),
            metrics.jwks_requests.load(Ordering::Relaxed),
        )
    };
    let rotated = token(new_kid, &new_signing_key);
    validator
        .validate(&provider, &token(kid, &signing_key))
        .await
        .unwrap();
    cool_down(&validator, &provider.discovery_url).await;
    *metrics.jwks.write().await = serde_json::Value::Null;

    let failed = validator.validate(&provider, &rotated).await;
    assert!(matches!(failed, Err(OidcError::Http(_))));
    assert_eq!(calls(), (2, 2));
    let cooling = validator.validate(&provider, &rotated).await;
    assert!(matches!(cooling, Err(OidcError::SigningNotFound)));
    let cached = validator
        .validate(&provider, &token(kid, &signing_key))
        .await;
    assert_eq!(cached.unwrap().subject_id, "subject-1");
    assert_eq!(calls(), (2, 2));

    *metrics.jwks.write().await = eddsa_jwk(new_kid, &new_signing_key);
    cool_down(&validator, &provider.discovery_url).await;
    let identity = validator.validate(&provider, &rotated).await.unwrap();
    assert_eq!(identity.subject_id, "subject-1");
    assert_eq!(calls(), (3, 3));

    task.abort();
}

#[tokio::test]
async fn failed_load_cools() {
    // Without usable keys, a failed load cools later logins with an unavailable provider,
    // for an empty and for an expired cache, and expired keys stay unused.
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let (discovery_url, _jwks_uri, metrics, task) =
        spawn_oidc_provider(issuer, kid, serde_json::Value::Null).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let validator = OidcValidator::new().unwrap();
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );
    let calls = || {
        (
            metrics.discovery_requests.load(Ordering::Relaxed),
            metrics.jwks_requests.load(Ordering::Relaxed),
        )
    };

    let failed = validator.validate(&provider, &token).await;
    assert!(matches!(failed, Err(OidcError::Http(_))));
    let cooling = validator.validate(&provider, &token).await;
    assert!(matches!(cooling, Err(OidcError::ProviderUnavailable)));
    assert_eq!(calls(), (1, 1));

    *metrics.jwks.write().await = eddsa_jwk(kid, &signing_key);
    cool_down(&validator, &provider.discovery_url).await;
    validator.validate(&provider, &token).await.unwrap();
    assert_eq!(calls(), (2, 2));
    *metrics.jwks.write().await = serde_json::Value::Null;
    expire_cache(&validator, &provider.discovery_url).await;

    let failed = validator.validate(&provider, &token).await;
    assert!(matches!(failed, Err(OidcError::Http(_))));
    let cooling = validator.validate(&provider, &token).await;
    assert!(matches!(cooling, Err(OidcError::ProviderUnavailable)));
    assert_eq!(calls(), (3, 3));

    *metrics.jwks.write().await = eddsa_jwk(kid, &signing_key);
    cool_down(&validator, &provider.discovery_url).await;
    expire_cache(&validator, &provider.discovery_url).await;
    validator.validate(&provider, &token).await.unwrap();
    assert_eq!(calls(), (4, 4));

    task.abort();
}

#[tokio::test]
async fn validator_rejects_issuer() {
    let issuer = "https://issuer.example";
    let audience = "aruna-api";
    let kid = "main-key";
    let signing_key = generate_signing_key();
    let jwks = eddsa_jwk(kid, &signing_key);
    let (discovery_url, _jwks_uri, _metrics, task) =
        spawn_oidc_provider("https://different-issuer.example", kid, jwks).await;
    let provider = OidcProviderConfig {
        id: "main".to_string(),
        issuer: issuer.to_string(),
        audience: audience.to_string(),
        discovery_url,
    };
    let token = sign_oidc_token(
        issuer,
        audience,
        "subject-1",
        kid,
        &signing_key,
        Algorithm::EdDSA,
        None,
    );

    let validator = OidcValidator::new().unwrap();
    let error = validator.validate(&provider, &token).await.unwrap_err();
    assert!(matches!(
        error,
        crate::error::OidcError::Jwt(jsonwebtoken::errors::Error { .. })
    ));

    task.abort();
}

#[tokio::test]
pub async fn test_token_capabilities() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });

    let realm_signing_key: SigningKey = generate_signing_key();
    let pubkey = realm_signing_key.verifying_key().to_bytes();
    let realm_id = RealmId::from_bytes(pubkey);

    let node_signing_key: SigningKey = generate_signing_key();
    let node_id = iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: vec![],
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let time = chrono::Utc::now().timestamp() as u64;
    let expiry = None;
    let user_id = UserId::local(Ulid::generate(), realm_id);

    drive(
        ResolveOidcOperation::new(ResolveOidcInput {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            user_id,
            name: "capabilities-user".to_string(),
            subject_id: "a-random-subject-id".to_string(),
            issuer: "issuer_id".to_string(),
            email_hash: None,
            create_new: false,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let capabilities = NodeCapabilities::management_node(realm_signing_key.clone()).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token_config = CreateTokenConfig {
        time,
        expiry,
        user_id,
        realm_id,
        node_capabilities: capabilities,
        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let management_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
    );

    let ctx = extract_auth_context(&state, &headers).await.unwrap();
    assert_eq!(ctx.realm_id, realm_id);
    assert_eq!(ctx.user_id, token_config.user_id);

    let issuer_key = generate_signing_key();

    let message = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let delegation_signature = realm_signing_key.sign(message.as_bytes()).to_string();

    let capabilities =
        NodeCapabilities::server_node(issuer_key, realm_id, delegation_signature).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token_config = CreateTokenConfig {
        time,
        expiry,
        user_id,
        realm_id,
        node_capabilities: capabilities,
        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let server_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
    );

    let ctx = extract_auth_context(&state, &headers).await.unwrap();
    assert_eq!(ctx.realm_id, realm_id);
    assert_eq!(ctx.user_id, token_config.user_id);

    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
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
pub async fn unknown_user_rejected() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
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
        graph_handle: None,
        task_handle: Some(task_handle),
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });

    let realm_signing_key: SigningKey = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());

    let node_signing_key: SigningKey = generate_signing_key();
    let node_id = iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

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
        &driver_ctx,
    )
    .await
    .unwrap();

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
        &driver_ctx,
    )
    .await
    .unwrap();

    let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        true,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let time = chrono::Utc::now().timestamp() as u64;
    let unknown_user = UserId::local(Ulid::generate(), realm_id);
    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time,
            expiry: None,
            user_id: unknown_user,
            realm_id,
            node_capabilities: capabilities.clone(),
            session: None,
            restrictions: None,
        })
        .unwrap(),
        &driver_ctx,
    )
    .await
    .unwrap()
    .expose()
    .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    assert!(extract_auth_context(&state, &headers).await.is_some());

    let auth_doc = read_auth_doc(&driver_ctx, &realm_id).await;
    let realm_admin = auth_doc
        .roles
        .values()
        .find(|role| role.name == "realm_admin")
        .unwrap();
    assert!(realm_admin.assigned_users.is_empty());

    net_handle.shutdown().await;
}

#[tokio::test]
pub async fn test_token_validation() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });

    let realm_signing_key: SigningKey = generate_signing_key();
    let pubkey = realm_signing_key.verifying_key().to_bytes();
    let realm_id = RealmId::from_bytes(pubkey);

    let node_signing_key: SigningKey = generate_signing_key();
    let node_id = iroh::PublicKey::from_bytes(node_signing_key.verifying_key().as_bytes()).unwrap();

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
        &driver_ctx,
    )
    .await
    .unwrap();

    let time = chrono::Utc::now().timestamp() as u64;
    let expiry = Some(
        chrono::Utc::now()
            .checked_add_days(Days::new(10))
            .unwrap()
            .timestamp() as u64,
    );
    let user_id = UserId::local(Ulid::generate(), realm_id);

    drive(
        ResolveOidcOperation::new(ResolveOidcInput {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            user_id,
            name: "validation-user".to_string(),
            subject_id: "validation-user-subject_id".to_string(),
            issuer: "validation-user-issuer-id".to_string(),
            email_hash: None,
            create_new: false,
        }),
        &driver_ctx,
    )
    .await
    .unwrap();

    let capabilities = NodeCapabilities::management_node(realm_signing_key.clone()).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token_config = CreateTokenConfig {
        time,
        expiry,
        user_id,
        realm_id,
        node_capabilities: capabilities.clone(),
        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let management_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
    );

    let ctx = extract_auth_context(&state, &headers).await.unwrap();
    assert_eq!(ctx.realm_id, realm_id);
    assert_eq!(ctx.user_id, token_config.user_id);

    revoke(
        &driver_ctx,
        Actor {
            node_id,
            user_id,
            realm_id,
        },
        &management_token,
    )
    .await;
    assert!(matches!(
        handle_token(&state, &management_token).await,
        Err(TokenError::TokenBlacklisted)
    ));

    let old_time = chrono::Utc::now()
        .checked_sub_days(Days::new(10))
        .unwrap()
        .timestamp() as u64;
    let expired = Some(chrono::Utc::now().timestamp() as u64 - 1);

    let token_config = CreateTokenConfig {
        time: old_time,
        expiry: expired,
        user_id,
        realm_id,
        node_capabilities: capabilities,

        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let management_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", management_token)).unwrap(),
    );
    assert!(extract_auth_context(&state, &headers).await.is_none());

    let issuer_key = generate_signing_key();

    let message = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let delegation_signature = realm_signing_key.sign(message.as_bytes()).to_string();

    let capabilities =
        NodeCapabilities::server_node(issuer_key.clone(), realm_id, delegation_signature.clone())
            .unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
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
        realm_id,
        node_capabilities: capabilities,

        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let server_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
    );

    assert!(extract_auth_context(&state, &headers).await.is_none());

    let invalid_signature = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0u8; 32]);
    let capabilities =
        NodeCapabilities::server_node(issuer_key.clone(), realm_id, invalid_signature).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token_config = CreateTokenConfig {
        time,
        expiry,
        user_id,
        realm_id,
        node_capabilities: capabilities,
        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let server_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
    );
    assert!(extract_auth_context(&state, &headers).await.is_none());

    let capabilities =
        NodeCapabilities::server_node(issuer_key, realm_id, delegation_signature.clone()).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token_config = CreateTokenConfig {
        time,
        expiry,
        user_id,
        realm_id: RealmId([0u8; 32]),
        node_capabilities: capabilities,
        session: None,
        restrictions: None,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone()).unwrap();
    let server_token = drive(token_operation, &driver_ctx)
        .await
        .unwrap()
        .expose()
        .to_string();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        axum::http::HeaderValue::from_str(&format!("Bearer {}", server_token)).unwrap(),
    );
    assert!(extract_auth_context(&state, &headers).await.is_none());
}

fn sign_untrusted_token(issuer_key: &SigningKey, now: u64) -> String {
    let issuer_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let claims = TokenClaims {
        sub: format!("{}@{}", Ulid::generate(), issuer_b64),
        iss: issuer_b64,
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    };
    let der = issuer_key
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(der.as_bytes()).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn untrusted_issuers_rejected() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });

    let realm_signing_key: SigningKey = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();

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
        &driver_ctx,
    )
    .await
    .unwrap();

    let capabilities = NodeCapabilities::management_node(realm_signing_key).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities,
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let now = chrono::Utc::now().timestamp().max(0) as u64;
    for _ in 0..2048 {
        let issuer_key = generate_signing_key();
        let token = sign_untrusted_token(&issuer_key, now);
        assert!(handle_token(&state, &token).await.is_err());
    }
}

#[tokio::test]
async fn delegated_token_accepted() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
        blob_handle: None,
    });

    let realm_signing_key: SigningKey = generate_signing_key();
    let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();

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
        &driver_ctx,
    )
    .await
    .unwrap();

    let issuer_key = generate_signing_key();
    let message = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(issuer_key.verifying_key().to_bytes());
    let delegation_signature = realm_signing_key.sign(message.as_bytes()).to_string();
    let capabilities =
        NodeCapabilities::server_node(issuer_key, realm_id, delegation_signature).unwrap();
    let state = ServerState::new(
        driver_ctx.clone(),
        realm_id,
        node_id,
        capabilities.clone(),
        false,
        None,
        aruna_runner::jobs::runtime::JobsRuntime::new(),
    )
    .await;

    let token = drive(
        CreateTokenOperation::new(CreateTokenConfig {
            time: chrono::Utc::now().timestamp().max(0) as u64,
            expiry: None,
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            node_capabilities: capabilities,

            session: None,
            restrictions: None,
        })
        .unwrap(),
        &driver_ctx,
    )
    .await
    .unwrap()
    .expose()
    .to_string();

    handle_token(&state, &token).await.unwrap();
    handle_token(&state, &token).await.unwrap();
}

/// A user node with a fresh store; the routes below refuse before they read it.
async fn extractor_state() -> (tempfile::TempDir, std::sync::Arc<ServerState>) {
    use crate::tests::routes::{test_state, test_storage};
    use aruna_runner::testing::test_context;
    let (dir, storage) = test_storage();
    let realm_key = ed25519_dalek::SigningKey::from_bytes(&[5; 32]);
    let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
    let state = test_state(
        std::sync::Arc::new(test_context(storage)),
        realm_id,
        iroh::SecretKey::from_bytes(&[6; 32]).public(),
        NodeCapabilities::user_node(realm_id).unwrap(),
    )
    .await;
    (dir, std::sync::Arc::new(state))
}

fn caller(realm_id: RealmId) -> aruna_core::structs::identity::auth::AuthContext {
    aruna_core::structs::identity::auth::AuthContext {
        user_id: UserId::local(ulid::Ulid::from_bytes([7; 16]), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    }
}

// A request with one fault keeps its answer; only a request with two faults now
// meets the token check first.
#[tokio::test]
async fn realm_keeps_faults() {
    use crate::tests::routes::send_json;
    use axum::http::StatusCode;
    let (_dir, state) = extractor_state().await;
    let own = caller(state.get_realm_id());
    let foreign = caller(RealmId::from_bytes([8; 32]));
    let route = crate::routes::portal::router;
    let put = |body| ("PUT", "/system/realm/portal-features", body);
    let valid = r#"{"features":{"compute":false}}"#;
    let cases = [
        (None, valid, StatusCode::UNAUTHORIZED),
        (Some(foreign), valid, StatusCode::FORBIDDEN),
        (Some(own), "{", StatusCode::BAD_REQUEST),
        (None, "{", StatusCode::UNAUTHORIZED),
    ];
    for (auth, body, expected) in cases {
        let status = send_json(route(), state.clone(), auth, put(body)).await;
        assert_eq!(status, expected, "body {body}");
    }
}

#[tokio::test]
async fn unrestricted_keeps_faults() {
    use crate::tests::routes::send_json;
    use axum::http::StatusCode;
    let (_dir, state) = extractor_state().await;
    let own = caller(state.get_realm_id());
    let restricted = aruna_core::structs::identity::auth::AuthContext {
        path_restrictions: Some(Vec::new()),
        ..own.clone()
    };
    let route = crate::routes::webhooks::router;
    let put = |body| ("PUT", "/system/notifications/webhook", body);
    let valid = r#"{"url":"https://example.org/hook"}"#;
    let cases = [
        (None, valid, StatusCode::UNAUTHORIZED),
        (
            Some(caller(RealmId::from_bytes([8; 32]))),
            valid,
            StatusCode::FORBIDDEN,
        ),
        (Some(restricted), valid, StatusCode::FORBIDDEN),
        (Some(own), "{", StatusCode::BAD_REQUEST),
        (None, "{", StatusCode::UNAUTHORIZED),
    ];
    for (auth, body, expected) in cases {
        let status = send_json(route(), state.clone(), auth, put(body)).await;
        assert_eq!(status, expected, "body {body}");
    }
}

#[tokio::test]
async fn token_keeps_faults() {
    use crate::tests::routes::send_json;
    use axum::http::StatusCode;
    let (_dir, state) = extractor_state().await;
    let foreign = caller(RealmId::from_bytes([8; 32]));
    let route = crate::routes::access::credentials::router;
    let post = |body| ("POST", "/access/credentials", body);
    let valid = r#"{"group_id":"01JZ8Y6T0K4W7M2N9Q5R3S8V1X"}"#;
    let cases = [
        (None, valid, StatusCode::UNAUTHORIZED),
        (Some(foreign), "{", StatusCode::BAD_REQUEST),
        (None, "{", StatusCode::UNAUTHORIZED),
    ];
    for (auth, body, expected) in cases {
        let status = send_json(route(), state.clone(), auth, post(body)).await;
        assert_eq!(status, expected, "body {body}");
    }
}
