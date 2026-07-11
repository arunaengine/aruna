use crate::error::{OidcError, ServerError, ServerResult, TokenError};
use crate::server_state::ServerState;
use crate::telemetry::record_auth_context;
use aruna_core::errors::ConversionError;
use aruna_core::structs::{
    AuthContext, OidcProviderConfig, Permission, TokenClaims, blob_object_permission_path,
};
use aruna_operations::auth::{decode_aruna_bearer_token, validate_aruna_bearer_token_claims};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::drive;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::HeaderMap;
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use s3s::header;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::RwLock;
use ulid::Ulid;

const OIDC_HTTP_TIMEOUT_SECS: u64 = 5;
const OIDC_HTTP_CONNECT_TIMEOUT_SECS: u64 = 2;
const OIDC_PROVIDER_METADATA_CACHE_TTL_SECS: u64 = 300;

#[derive(Debug)]
pub struct OidcValidator {
    client: reqwest::Client,
    provider_metadata_cache: RwLock<HashMap<String, CachedOidcProviderMetadata>>,
}

#[derive(Debug, Clone)]
struct CachedOidcProviderMetadata {
    issuer: String,
    jwks: Value,
    fetched_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcIdentity {
    pub issuer: String,
    pub subject_id: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OidcTokenSelector {
    pub issuer: String,
    pub audience: Vec<String>,
}

impl OidcTokenSelector {
    pub fn matches_audience(&self, expected: &str) -> bool {
        self.audience.iter().any(|value| value == expected)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct OidcDiscoveryDocument {
    issuer: String,
    jwks_uri: String,
}

#[derive(Debug, Clone, Deserialize)]
struct OidcClaims {
    sub: String,
    iss: String,
    exp: u64,
    #[serde(default)]
    nbf: Option<u64>,
    aud: OidcAudience,
    #[serde(default)]
    azp: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    given_name: Option<String>,
    #[serde(default)]
    family_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum OidcAudience {
    Single(String),
    Multiple(Vec<String>),
}

impl OidcValidator {
    pub fn new() -> Result<Self, AuthorizationError> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(OIDC_HTTP_TIMEOUT_SECS))
                .connect_timeout(Duration::from_secs(OIDC_HTTP_CONNECT_TIMEOUT_SECS))
                .build()?,
            provider_metadata_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn token_selector(&self, token: &str) -> Result<OidcTokenSelector, OidcError> {
        let claims = insecure_decode::<OidcClaims>(token)?.claims;
        if claims.iss.is_empty() {
            return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidIssuer,
            )));
        }

        Ok(OidcTokenSelector {
            issuer: claims.iss,
            audience: match claims.aud {
                OidcAudience::Single(value) => vec![value],
                OidcAudience::Multiple(values) => values,
            },
        })
    }

    pub async fn issuers(&self) -> Vec<String> {
        self.provider_metadata_cache
            .read()
            .await
            .values()
            .map(|metadata| metadata.issuer.clone())
            .collect()
    }

    async fn fetch_provider_metadata(
        &self,
        provider: &OidcProviderConfig,
    ) -> Result<CachedOidcProviderMetadata, OidcError> {
        let discovery = self
            .client
            .get(&provider.discovery_url)
            .send()
            .await?
            .error_for_status()?
            .json::<OidcDiscoveryDocument>()
            .await?;
        if discovery.issuer != provider.issuer {
            return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidIssuer,
            )));
        }
        let jwks = self
            .client
            .get(&discovery.jwks_uri)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;

        Ok(CachedOidcProviderMetadata {
            issuer: discovery.issuer,
            jwks,
            fetched_at: Instant::now(),
        })
    }

    async fn get_provider_metadata(
        &self,
        provider: &OidcProviderConfig,
        refresh: bool,
    ) -> Result<CachedOidcProviderMetadata, OidcError> {
        if !refresh {
            let ttl = Duration::from_secs(OIDC_PROVIDER_METADATA_CACHE_TTL_SECS);
            if let Some(metadata) = self
                .provider_metadata_cache
                .read()
                .await
                .get(&provider.discovery_url)
                .filter(|metadata| metadata.fetched_at.elapsed() < ttl)
                .cloned()
            {
                return Ok(metadata);
            }
        }

        let metadata = self.fetch_provider_metadata(provider).await?;
        self.provider_metadata_cache
            .write()
            .await
            .insert(provider.discovery_url.clone(), metadata.clone());
        Ok(metadata)
    }

    async fn decoding_key_for(
        &self,
        provider: &OidcProviderConfig,
        kid: &str,
    ) -> Result<DecodingKey, OidcError> {
        for refresh in [false, true] {
            let metadata = self.get_provider_metadata(provider, refresh).await?;
            if metadata.issuer != provider.issuer {
                return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                    jsonwebtoken::errors::ErrorKind::InvalidIssuer,
                )));
            }
            let jwks = serde_json::from_value::<JwkSet>(metadata.jwks.clone())?;
            if let Some(jwk) = jwks.find(kid) {
                return DecodingKey::from_jwk(jwk).map_err(Into::into);
            }
        }

        Err(OidcError::SigningKeyNotFound)
    }

    pub async fn validate(
        &self,
        provider: &OidcProviderConfig,
        token: &str,
    ) -> Result<OidcIdentity, OidcError> {
        let header = decode_header(token)?;
        let kid = header.kid.ok_or(OidcError::MissingKeyId)?;
        let algorithm = header.alg;
        if !is_supported_oidc_algorithm(algorithm) {
            return Err(OidcError::UnsupportedAlgorithm);
        }
        let decoding_key = self.decoding_key_for(provider, &kid).await?;

        let mut validation = Validation::new(algorithm);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.set_issuer(&[provider.issuer.as_str()]);
        validation.set_audience(&[provider.audience.as_str()]);
        validation.validate_nbf = true;

        let claims = decode::<OidcClaims>(token, &decoding_key, &validation)?.claims;
        if !matches_audience(&claims.aud, &provider.audience) {
            return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidAudience,
            )));
        }
        validate_authorized_party(&claims.aud, claims.azp.as_deref(), &provider.audience)?;
        if claims.iss != provider.issuer {
            return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidIssuer,
            )));
        }
        if claims.sub.is_empty() {
            return Err(OidcError::MissingSubject);
        }
        let _ = claims.exp;
        let _ = claims.nbf;
        let display_name = oidc_display_name(&claims);

        Ok(OidcIdentity {
            issuer: claims.iss,
            subject_id: claims.sub,
            display_name,
        })
    }
}

fn matches_audience(aud: &OidcAudience, expected: &str) -> bool {
    match aud {
        OidcAudience::Single(value) => value == expected,
        OidcAudience::Multiple(values) => values.iter().any(|value| value == expected),
    }
}

fn validate_authorized_party(
    aud: &OidcAudience,
    azp: Option<&str>,
    expected: &str,
) -> Result<(), OidcError> {
    if let OidcAudience::Multiple(values) = aud
        && values.len() > 1
        && azp != Some(expected)
    {
        return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
            jsonwebtoken::errors::ErrorKind::InvalidAudience,
        )));
    }
    Ok(())
}

fn oidc_display_name(claims: &OidcClaims) -> Option<String> {
    claims
        .name
        .as_ref()
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .or_else(|| {
            claims
                .preferred_username
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .or_else(|| {
            match (
                claims.given_name.as_deref().map(str::trim),
                claims.family_name.as_deref().map(str::trim),
            ) {
                (Some(given), Some(family)) if !given.is_empty() && !family.is_empty() => {
                    Some(format!("{given} {family}"))
                }
                (Some(given), _) if !given.is_empty() => Some(given.to_string()),
                (_, Some(family)) if !family.is_empty() => Some(family.to_string()),
                _ => None,
            }
        })
        .or_else(|| {
            claims
                .email
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
}

fn is_supported_oidc_algorithm(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
            | Algorithm::ES256
            | Algorithm::ES384
            | Algorithm::EdDSA
    )
}

pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedArunaBearerTokenCarrier {
    token: String,
}

impl ValidatedArunaBearerTokenCarrier {
    fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(token: impl Into<String>) -> Self {
        Self::new(token)
    }

    pub fn as_str(&self) -> &str {
        &self.token
    }
}

#[derive(Debug, Error)]
pub enum AuthorizationError {
    #[error("Error decoding AuthContext")]
    DecodingError,
    #[error(transparent)]
    IntoUlid(#[from] ulid::DecodeError),
    #[error(transparent)]
    IntoRealmId(#[from] ConversionError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
}

#[cfg(test)]
async fn extract_auth_context(state: &ServerState, headers: &HeaderMap) -> Option<AuthContext> {
    extract_auth_context_and_bearer_token(state, headers)
        .await
        .0
}

async fn extract_auth_context_and_bearer_token(
    state: &ServerState,
    headers: &HeaderMap,
) -> (
    Option<AuthContext>,
    Option<ValidatedArunaBearerTokenCarrier>,
) {
    let Some(token) = bearer_token(headers) else {
        return (None, None);
    };

    let claims = match handle_token(state, token).await {
        Ok(claims) => claims,
        Err(_) => return (None, None),
    };
    let auth_context: AuthContext = match claims.try_into() {
        Ok(auth_context) => auth_context,
        Err(_) => return (None, None),
    };
    (
        Some(auth_context),
        Some(ValidatedArunaBearerTokenCarrier::new(token)),
    )
}

pub async fn handle_token(state: &ServerState, token: &str) -> Result<TokenClaims, TokenError> {
    decode_aruna_bearer_token(state, token)
        .await
        .map_err(Into::into)
}

pub async fn validate_claims(state: &ServerState, claims: &TokenClaims) -> Result<(), TokenError> {
    validate_aruna_bearer_token_claims(state, claims)
        .await
        .map_err(Into::into)
}

pub async fn auth_middleware(
    state: axum::extract::State<std::sync::Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Extract and validate token, get Option<AuthContext>
    // We clone headers to avoid borrowing issues with the async function
    let headers = request.headers().clone();
    let (auth_ctx, bearer_token) = aruna_core::telemetry::time_stage(
        "auth",
        extract_auth_context_and_bearer_token(&state, &headers),
    )
    .await;
    record_auth_context(auth_ctx.as_ref());

    // Always insert (Some or None) - handlers decide if auth is required
    request.extensions_mut().insert(auth_ctx);
    request.extensions_mut().insert(bearer_token);

    // Always continue to handler
    next.run(request).await
}

pub(crate) fn parse_group_id(group_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(group_id).map_err(|_| ServerError::BadRequest)
}

pub(crate) fn parse_source_connector_id(connector_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(connector_id).map_err(|_| ServerError::BadRequest)
}

pub(crate) fn require_realm_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = auth.ok_or(ServerError::Unauthorized)?;
    if auth.realm_id != state.get_realm_id() {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

pub(crate) async fn ensure_permission(
    state: &ServerState,
    auth: &AuthContext,
    path: String,
    required_permission: Permission,
) -> ServerResult<()> {
    let allowed = aruna_core::telemetry::time_stage(
        "permission",
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: auth.clone(),
                path,
                required_permission,
            }),
            &state.get_ctx(),
        ),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;

    if allowed {
        Ok(())
    } else {
        Err(ServerError::Forbidden)
    }
}

pub(crate) fn bucket_blob_permission_path(
    state: &ServerState,
    group_id: Ulid,
    bucket: &str,
    key: &str,
) -> String {
    blob_object_permission_path(
        state.get_realm_id(),
        group_id,
        state.get_node_id(),
        bucket,
        key,
    )
}

#[cfg(test)]
mod test {
    use crate::auth::{
        OIDC_PROVIDER_METADATA_CACHE_TTL_SECS, OidcValidator, bucket_blob_permission_path,
        extract_auth_context, extract_auth_context_and_bearer_token, handle_token,
    };
    use crate::error::TokenError;
    use crate::server::ServerState;
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::OidcProviderConfig;
    use aruna_core::structs::{Actor, NodeCapabilities, RealmAuthorizationDocument, RealmId};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::register_or_get_oidc_user::{
        RegisterOrGetOidcUserInput, RegisterOrGetOidcUserOperation,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use axum::Json;
    use axum::http::{HeaderMap, header};
    use axum::routing::get;
    use axum::{Router, extract::State};
    use base64::Engine;
    use byteview::ByteView;
    use chrono::Days;
    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use jsonwebtoken::signature::SignerMut;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use tempfile::env::temp_dir;
    use tokio::net::TcpListener;
    use tokio::sync::RwLock;
    use ulid::Ulid;

    #[tokio::test]
    async fn bucket_blob_permission_path_matches_canonical_blob_object_path() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
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
                metadata_handle: None,
                task_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::local_node(realm_id).unwrap(),
            false,
            None,
        )
        .await;

        let group_id = Ulid::from_bytes([9u8; 16]);
        assert_eq!(
            bucket_blob_permission_path(&state, group_id, "bucket", "nested/file.txt"),
            aruna_core::structs::blob_object_permission_path(
                realm_id,
                group_id,
                node_id,
                "bucket",
                "nested/file.txt",
            )
        );
    }

    #[derive(Clone)]
    struct OidcTestServerState {
        issuer: String,
        jwks_uri: String,
        jwks: Arc<RwLock<serde_json::Value>>,
        discovery_requests: Arc<AtomicUsize>,
        jwks_requests: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct OidcTestServerMetrics {
        discovery_requests: Arc<AtomicUsize>,
        jwks_requests: Arc<AtomicUsize>,
        jwks: Arc<RwLock<serde_json::Value>>,
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
    struct TestMultiAudienceOidcClaims {
        sub: String,
        iss: String,
        aud: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        azp: Option<String>,
        exp: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    }

    async fn oidc_discovery(State(state): State<OidcTestServerState>) -> Json<serde_json::Value> {
        state.discovery_requests.fetch_add(1, Ordering::Relaxed);
        Json(serde_json::json!({
            "issuer": state.issuer,
            "jwks_uri": state.jwks_uri,
        }))
    }

    async fn oidc_jwks(State(state): State<OidcTestServerState>) -> Json<serde_json::Value> {
        state.jwks_requests.fetch_add(1, Ordering::Relaxed);
        Json(state.jwks.read().await.clone())
    }

    async fn spawn_oidc_provider(
        issuer: &str,
        kid: &str,
        jwks: serde_json::Value,
    ) -> (
        String,
        String,
        OidcTestServerMetrics,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let discovery_url = format!("http://{addr}/.well-known/openid-configuration");
        let jwks_uri = format!("http://{addr}/jwks/{kid}.json");
        let discovery_requests = Arc::new(AtomicUsize::new(0));
        let jwks_requests = Arc::new(AtomicUsize::new(0));
        let jwks = Arc::new(RwLock::new(jwks));
        let state = OidcTestServerState {
            issuer: issuer.to_string(),
            jwks_uri: jwks_uri.clone(),
            jwks: jwks.clone(),
            discovery_requests: discovery_requests.clone(),
            jwks_requests: jwks_requests.clone(),
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
            OidcTestServerMetrics {
                discovery_requests,
                jwks_requests,
                jwks,
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

    fn sign_multi_audience_oidc_token(
        issuer: &str,
        audiences: &[&str],
        azp: Option<&str>,
        subject: &str,
        kid: &str,
        signing_key: &SigningKey,
    ) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_string());
        let claims = TestMultiAudienceOidcClaims {
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

    #[tokio::test]
    async fn bearer_token_carrier_requires_valid_aruna_token() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            metadata_handle: None,
            task_handle: None,
            blob_handle: None,
        });
        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_id = iroh::SecretKey::generate().public();
        let user_id = UserId::local(Ulid::new(), realm_id);
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
        )
        .await;
        let token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time: chrono::Utc::now().timestamp() as u64,
                expiry: None,
                user_id,
                realm_id,
                node_capabilities: capabilities,
            })
            .unwrap(),
            &driver_ctx,
        )
        .await
        .unwrap();
        let headers_for = |token: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
            );
            headers
        };

        let (auth, carrier) =
            extract_auth_context_and_bearer_token(&state, &headers_for(&token)).await;
        assert_eq!(auth.unwrap().user_id, user_id);
        assert_eq!(carrier.unwrap().as_str(), token);

        state.add_token_to_blacklist(&token).await;
        let (auth, carrier) =
            extract_auth_context_and_bearer_token(&state, &headers_for(&token)).await;
        assert!(auth.is_none());
        assert!(carrier.is_none());

        let oidc_signing_key = SigningKey::generate(&mut csprng);
        let oidc_token = sign_oidc_token(
            "https://issuer.example",
            "aruna-api",
            "subject-1",
            "main-key",
            &oidc_signing_key,
            Algorithm::EdDSA,
            None,
        );
        let (auth, carrier) =
            extract_auth_context_and_bearer_token(&state, &headers_for(&oidc_token)).await;
        assert!(auth.is_none());
        assert!(carrier.is_none());

        let (auth, carrier) =
            extract_auth_context_and_bearer_token(&state, &headers_for("not-a-jwt")).await;
        assert!(auth.is_none());
        assert!(carrier.is_none());
    }

    #[tokio::test]
    async fn oidc_validator_accepts_valid_eddsa_token() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, _metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
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
    async fn oidc_validator_rejects_hs256_tokens() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "symm-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, _metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
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
        let token = encode(&header, &claims, &EncodingKey::from_secret(b"super-secret")).unwrap();

        let validator = OidcValidator::new().unwrap();
        let error = validator.validate(&provider, &token).await.unwrap_err();
        assert!(matches!(
            error,
            crate::error::OidcError::UnsupportedAlgorithm
        ));

        task.abort();
    }

    #[tokio::test]
    async fn oidc_validator_rejects_wrong_audience() {
        let issuer = "https://issuer.example";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, _metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
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
    async fn oidc_validator_caches_provider_metadata_between_requests() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
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
    async fn oidc_validator_refreshes_expired_provider_metadata() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
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
            .checked_sub(Duration::from_secs(
                OIDC_PROVIDER_METADATA_CACHE_TTL_SECS + 1,
            ))
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
    async fn oidc_validator_requires_azp_for_multi_audience_token() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let jwks = eddsa_jwk(kid, &signing_key);
        let (discovery_url, _jwks_uri, _metrics, task) =
            spawn_oidc_provider(issuer, kid, jwks).await;
        let provider = OidcProviderConfig {
            id: "main".to_string(),
            issuer: issuer.to_string(),
            audience: audience.to_string(),
            discovery_url,
        };
        let validator = OidcValidator::new().unwrap();

        let missing_azp = sign_multi_audience_oidc_token(
            issuer,
            &[audience, "other-client"],
            None,
            "subject-1",
            kid,
            &signing_key,
        );
        assert!(validator.validate(&provider, &missing_azp).await.is_err());

        let wrong_azp = sign_multi_audience_oidc_token(
            issuer,
            &[audience, "other-client"],
            Some("other-client"),
            "subject-1",
            kid,
            &signing_key,
        );
        assert!(validator.validate(&provider, &wrong_azp).await.is_err());

        let valid = sign_multi_audience_oidc_token(
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
    async fn oidc_validator_refreshes_jwks_when_kid_is_rotated() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let old_kid = "old-key";
        let new_kid = "new-key";
        let old_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let new_signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
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
    async fn oidc_validator_rejects_discovery_issuer_mismatch() {
        let issuer = "https://issuer.example";
        let audience = "aruna-api";
        let kid = "main-key";
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
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
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            metadata_handle: None,
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
        let user_id = UserId::local(Ulid::new(), realm_id);

        drive(
            RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
                actor: Actor {
                    node_id,
                    user_id: UserId::nil(realm_id),
                    realm_id,
                },
                user_id,
                name: "capabilities-user".to_string(),
                subject_id: "a-random-subject-id".to_string(),
                issuer: "issuer_id".to_string(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap();

        //
        // Test Management Nodes
        //
        let capabilities = NodeCapabilities::management_node(realm_signing_key.clone()).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
            realm_id,
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
            NodeCapabilities::server_node(issuer_key, realm_id, delegation_signature).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
            realm_id,
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
        let capabilities = NodeCapabilities::local_node(realm_id).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
    pub async fn test_unknown_token_user_is_rejected() {
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
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
            metadata_handle: None,
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
        )
        .await;

        let time = chrono::Utc::now().timestamp() as u64;
        let unknown_user = UserId::local(Ulid::new(), realm_id);
        let token = drive(
            CreateTokenOperation::new(CreateTokenConfig {
                time,
                expiry: None,
                user_id: unknown_user,
                realm_id,
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
        let mut tempdir = temp_dir();
        tempdir.push(Ulid::new().to_string());
        let storage_handle = storage::FjallStorage::open(tempdir.to_str().unwrap()).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            metadata_handle: None,
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
        let user_id = UserId::local(Ulid::new(), realm_id);

        drive(
            RegisterOrGetOidcUserOperation::new(RegisterOrGetOidcUserInput {
                actor: Actor {
                    node_id,
                    user_id: UserId::nil(realm_id),
                    realm_id,
                },
                user_id,
                name: "validation-user".to_string(),
                subject_id: "validation-user-subject_id".to_string(),
                issuer: "validation-user-issuer-id".to_string(),
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
        )
        .await;

        //
        // Valid management token with expiry
        //
        let token_config = CreateTokenConfig {
            time,
            expiry,
            user_id,
            realm_id,
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

        state.add_token_to_blacklist(&management_token).await;
        assert!(matches!(
            handle_token(&state, &management_token).await,
            Err(TokenError::TokenBlacklisted)
        ));

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
            realm_id,
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
            realm_id,
            delegation_signature.clone(),
        )
        .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
            realm_id,
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
            NodeCapabilities::server_node(issuer_key.clone(), realm_id, invalid_signature).unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
            realm_id,
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
        let capabilities =
            NodeCapabilities::server_node(issuer_key, realm_id, delegation_signature.clone())
                .unwrap();
        let state = ServerState::new(
            driver_ctx.clone(),
            realm_id,
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
