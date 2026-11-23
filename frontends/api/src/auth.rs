//! Validates bearer and OIDC tokens on requests and checks caller permissions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::{OidcError, ServerError, ServerResult, TokenError};
use crate::server::state::ServerState;
use crate::telemetry::record_auth_context;
use aruna_core::errors::ConversionError;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities, Permission, TokenClaims};
use aruna_core::structs::identity::realm::OidcProviderConfig;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_runner::driver::drive;
use axum::Extension;
use axum::extract::{FromRequestParts, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::HeaderMap;
use http::request::Parts;
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use s3s::header;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use ulid::Ulid;

const HTTP_TIMEOUT_SECS: u64 = 5;
const CONNECT_TIMEOUT_SECS: u64 = 2;
const CACHE_TTL_SECS: u64 = 300;
/// An unknown key id refetches the provider keys at most this often after any provider call,
/// failed or not, so forged ids or an outage cannot turn every request into provider calls.
const REFRESH_COOLDOWN_SECS: u64 = 30;

#[derive(Debug)]
pub struct OidcValidator {
    client: reqwest::Client,
    provider_metadata_cache: RwLock<HashMap<String, CachedProviderMetadata>>,
    /// Per discovery URL, the end of its last failed provider call. Holding the slot runs the
    /// only call.
    refresh_slots: Mutex<HashMap<String, Arc<Mutex<Option<Instant>>>>>,
}

#[derive(Debug, Clone)]
struct CachedProviderMetadata {
    issuer: String,
    jwks: Value,
    fetched_at: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcIdentity {
    pub issuer: String,
    pub subject_id: String,
    pub display_name: Option<String>,
    /// Verified provider login time; `None` when the provider did not send it.
    pub auth_time: Option<u64>,
    /// The email address, only when the provider marks it verified.
    #[serde(default)]
    pub verified_email: Option<String>,
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
    #[serde(default)]
    auth_time: Option<u64>,
    /// A boolean, or the string `"true"` some providers send.
    #[serde(default)]
    email_verified: Option<Value>,
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
                .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
                .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
                .build()?,
            provider_metadata_cache: RwLock::new(HashMap::new()),
            refresh_slots: Mutex::new(HashMap::new()),
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
    ) -> Result<CachedProviderMetadata, OidcError> {
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

        Ok(CachedProviderMetadata {
            issuer: discovery.issuer,
            jwks,
            fetched_at: Instant::now(),
        })
    }

    /// Returns usable cached metadata, else fetches it with one call per provider at a time.
    /// `stale` is the fetch time of cached metadata that lacked the wanted key.
    async fn get_provider_metadata(
        &self,
        provider: &OidcProviderConfig,
        stale: Option<Instant>,
    ) -> Result<CachedProviderMetadata, OidcError> {
        let usable = |metadata: &CachedProviderMetadata| match stale {
            None => metadata.fetched_at.elapsed() < Duration::from_secs(CACHE_TTL_SECS),
            Some(seen) => metadata.fetched_at > seen,
        };
        if let Some(metadata) = self.cached_metadata(provider, &usable).await {
            return Ok(metadata);
        }
        let asked_at = Instant::now();
        let slot = self
            .refresh_slots
            .lock()
            .await
            .entry(provider.discovery_url.clone())
            .or_default()
            .clone();
        let mut failed_at = slot.lock().await;
        if let Some(metadata) = self.cached_metadata(provider, &usable).await {
            return Ok(metadata);
        }
        // A failed call that ended while this request waited, or within the cooldown, stands
        // for every load and refresh; a successful call cools refreshes through `fetched_at`.
        let shared = failed_at.is_some_and(|at| {
            at >= asked_at || at.elapsed() < Duration::from_secs(REFRESH_COOLDOWN_SECS)
        });
        if shared {
            return Err(match stale {
                None => OidcError::ProviderUnavailable,
                Some(_) => OidcError::SigningNotFound,
            });
        }
        let fetched = self.fetch_provider_metadata(provider).await;
        *failed_at = fetched.is_err().then(Instant::now);
        let metadata = fetched?;
        self.provider_metadata_cache
            .write()
            .await
            .insert(provider.discovery_url.clone(), metadata.clone());
        Ok(metadata)
    }

    async fn cached_metadata(
        &self,
        provider: &OidcProviderConfig,
        usable: &impl Fn(&CachedProviderMetadata) -> bool,
    ) -> Option<CachedProviderMetadata> {
        self.provider_metadata_cache
            .read()
            .await
            .get(&provider.discovery_url)
            .filter(|metadata| usable(metadata))
            .cloned()
    }

    async fn decoding_key_for(
        &self,
        provider: &OidcProviderConfig,
        kid: &str,
    ) -> Result<DecodingKey, OidcError> {
        let mut stale = None;
        for _ in 0..2 {
            let metadata = self.get_provider_metadata(provider, stale).await?;
            if metadata.issuer != provider.issuer {
                return Err(OidcError::Jwt(jsonwebtoken::errors::Error::from(
                    jsonwebtoken::errors::ErrorKind::InvalidIssuer,
                )));
            }
            let jwks = serde_json::from_value::<JwkSet>(metadata.jwks.clone())?;
            if let Some(jwk) = jwks.find(kid) {
                return DecodingKey::from_jwk(jwk).map_err(Into::into);
            }
            if metadata.fetched_at.elapsed() < Duration::from_secs(REFRESH_COOLDOWN_SECS) {
                break;
            }
            stale = Some(metadata.fetched_at);
        }

        Err(OidcError::SigningNotFound)
    }

    pub async fn validate(
        &self,
        provider: &OidcProviderConfig,
        token: &str,
    ) -> Result<OidcIdentity, OidcError> {
        let header = decode_header(token)?;
        let kid = header.kid.ok_or(OidcError::MissingKeyId)?;
        let algorithm = header.alg;
        if !oidc_algorithm_supported(algorithm) {
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
        let verified = claims
            .email_verified
            .as_ref()
            .is_some_and(|value| value.as_bool() == Some(true) || value.as_str() == Some("true"));

        Ok(OidcIdentity {
            issuer: claims.iss,
            subject_id: claims.sub,
            display_name,
            auth_time: claims.auth_time,
            verified_email: claims
                .email
                .filter(|email| verified && !email.trim().is_empty()),
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

fn oidc_algorithm_supported(algorithm: Algorithm) -> bool {
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
pub struct ValidatedBearer {
    token: String,
    expires_at_secs: u64,
}

impl ValidatedBearer {
    fn new(token: impl Into<String>, expires_at_secs: u64) -> Self {
        Self {
            token: token.into(),
            expires_at_secs,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn new_for_test(token: impl Into<String>) -> Self {
        Self::new(token, u64::MAX)
    }

    pub fn as_str(&self) -> &str {
        &self.token
    }

    pub fn expires_at_secs(&self) -> u64 {
        self.expires_at_secs
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

/// Awaits `future` and attributes its wall time to the named request stage.
async fn time_stage<F: Future>(name: &'static str, future: F) -> F::Output {
    let started = Instant::now();
    let output = future.await;
    aruna_core::telemetry::record_stage(name, started.elapsed());
    output
}

#[cfg(test)]
async fn extract_auth_context(state: &ServerState, headers: &HeaderMap) -> Option<AuthContext> {
    extract_auth_parts(state, headers).await.0
}

async fn extract_auth_parts(
    state: &ServerState,
    headers: &HeaderMap,
) -> (Option<AuthContext>, Option<ValidatedBearer>) {
    let Some(token) = bearer_token(headers) else {
        return (None, None);
    };
    match bearer_auth(state, token).await {
        Some((auth_context, bearer)) => (Some(auth_context), Some(bearer)),
        None => (None, None),
    }
}

/// The caller a bearer token names now, checked in full: signature, expiry and revocation.
pub(crate) async fn bearer_auth(
    state: &ServerState,
    token: &str,
) -> Option<(AuthContext, ValidatedBearer)> {
    let claims = handle_token(state, token).await.ok()?;
    let expires_at_secs = claims.exp;
    let auth_context: AuthContext = claims.try_into().ok()?;
    // Reject tokens whose restriction set exceeds the fail-closed size limits so
    // an oversized delegation never reaches the permission evaluator.
    if let Some(restrictions) = auth_context.path_restrictions.as_deref()
        && aruna_core::permission_path::validate_restriction_limits(restrictions).is_err()
    {
        return None;
    }
    Some((auth_context, ValidatedBearer::new(token, expires_at_secs)))
}

pub async fn handle_token(state: &ServerState, token: &str) -> Result<TokenClaims, TokenError> {
    state.decode_token(token).await.map_err(Into::into)
}

/// Fully verified claims of a token that may already be revoked, so revoking it
/// again is authorized against the same subject as the first time.
pub(crate) async fn claims_for_revocation(
    state: &ServerState,
    token: &str,
) -> Result<TokenClaims, TokenError> {
    state.decode_revoked(token).await.map_err(Into::into)
}

pub async fn auth_middleware(
    state: axum::extract::State<std::sync::Arc<ServerState>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Extract and validate token, getting Option<AuthContext>.
    let (auth_ctx, bearer_token) =
        time_stage("auth", extract_auth_parts(&state, request.headers())).await;
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

pub(crate) fn parse_connector_id(connector_id: &str) -> ServerResult<Ulid> {
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

/// Realm auth that additionally rejects path-restricted (delegated) tokens. User-scoped
/// surfaces cannot honour a token's path confinement, so a delegated token must not reach
/// them even when a per-resource permission check would otherwise pass.
pub(crate) fn require_unrestricted_auth(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = require_realm_auth(state, auth)?;
    if auth.path_restrictions.is_some() {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

/// Reads the device owner from local replicated realm configuration.
/// This authorizes the device surface only for its bound user on a User-kind node.
/// Local reads keep the device plane available while the realm is unreachable.
pub(crate) async fn require_owner(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(ServerError::NotFound);
    }
    let auth = require_unrestricted_auth(state, auth)?;
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        // A device that enrolled but has not received the configuration yet
        // cannot resolve its owner; that resolves on its own.
        GetConfigError::DocumentNotFound => ServerError::ServiceUnavailableReason(
            "the realm configuration has not reached this device yet".to_string(),
        ),
        other => ServerError::InternalError(other.into()),
    })?;
    let node_id = state.get_node_id().to_string();
    let owner = config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .and_then(|node| node.kind.owner())
        .ok_or(ServerError::Forbidden)?;
    if owner != auth.user_id {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

/// A signed-in caller of any realm; answers like a missing token otherwise.
pub struct TokenAuth(pub AuthContext);

/// A signed-in caller of this realm, checked by [`require_realm_auth`].
pub struct RealmAuth(pub AuthContext);

/// A caller of this realm without path restrictions, checked by [`require_unrestricted_auth`].
pub struct UnrestrictedAuth(pub AuthContext);

/// The bound owner of this device, checked by [`require_owner`].
pub struct OwnerAuth(pub AuthContext);

/// Reads the context the auth middleware stored; a missing one answers like `Extension` does.
async fn request_auth(
    parts: &mut Parts,
    state: &Arc<ServerState>,
) -> Result<Option<AuthContext>, Response> {
    Extension::<Option<AuthContext>>::from_request_parts(parts, state)
        .await
        .map(|Extension(auth)| auth)
        .map_err(IntoResponse::into_response)
}

impl FromRequestParts<Arc<ServerState>> for TokenAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let auth = request_auth(parts, state).await?;
        auth.map(Self)
            .ok_or_else(|| ServerError::Unauthorized.into_response())
    }
}

impl FromRequestParts<Arc<ServerState>> for RealmAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let auth = request_auth(parts, state).await?;
        require_realm_auth(state, auth)
            .map(Self)
            .map_err(IntoResponse::into_response)
    }
}

impl FromRequestParts<Arc<ServerState>> for UnrestrictedAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let auth = request_auth(parts, state).await?;
        require_unrestricted_auth(state, auth)
            .map(Self)
            .map_err(IntoResponse::into_response)
    }
}

impl FromRequestParts<Arc<ServerState>> for OwnerAuth {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<ServerState>,
    ) -> Result<Self, Self::Rejection> {
        let auth = request_auth(parts, state).await?;
        require_owner(state, auth)
            .await
            .map(Self)
            .map_err(IntoResponse::into_response)
    }
}

/// Runs an auth extractor on a request that carries `auth` as the middleware leaves it.
#[cfg(test)]
pub(crate) async fn extract<T>(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> Result<T, axum::http::StatusCode>
where
    T: FromRequestParts<Arc<ServerState>, Rejection = Response>,
{
    let (mut parts, ()) = Request::new(()).into_parts();
    parts.extensions.insert(auth);
    T::from_request_parts(&mut parts, state)
        .await
        .map_err(|response| response.status())
}

pub(crate) async fn ensure_permission(
    state: &ServerState,
    auth: &AuthContext,
    path: String,
    required_permission: Permission,
) -> ServerResult<()> {
    ensure_permission_with(
        state,
        auth,
        path,
        required_permission,
        aruna_core::request_policy::PolicyRequestExtras::rest(),
    )
    .await
}

/// Single REST authorization choke point: ordinary RBAC and public visibility
/// first, then the realm and group request policies, sharing the one
/// operations-owned boundary with the S3 hook.
pub(crate) async fn ensure_permission_with(
    state: &ServerState,
    auth: &AuthContext,
    path: String,
    required_permission: Permission,
    extras: aruna_core::request_policy::PolicyRequestExtras,
) -> ServerResult<()> {
    time_stage(
        "permission",
        drive(
            AuthorizeOperation::new(
                state.get_realm_id(),
                auth,
                &path,
                &required_permission,
                extras,
            ),
            &state.get_ctx(),
        ),
    )
    .await
    .map_err(ServerError::from)
}

/// An infrastructure fault inside the permission check is not a verdict:
/// exhausted transaction-cleanup capacity stays retryable, every other storage
/// fault stays internal, and only real denials become `Forbidden`.
impl From<AuthorizeError> for ServerError {
    fn from(error: AuthorizeError) -> Self {
        match error {
            AuthorizeError::Storage(error) => error.into(),
            AuthorizeError::CheckFailed(message) => ServerError::InternalError(message.into()),
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => ServerError::Forbidden,
        }
    }
}

/// Boolean form of [`ensure_permission`] for the routes that grade a caller
/// instead of rejecting it. Every denial, including one from a request policy,
/// reads as not authorized; only a failed check stays an internal error.
pub(crate) async fn permission_granted(
    state: &ServerState,
    auth: &AuthContext,
    path: String,
    required_permission: Permission,
) -> ServerResult<bool> {
    match ensure_permission(state, auth, path, required_permission).await {
        Ok(()) => Ok(true),
        Err(ServerError::Forbidden) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) fn blob_permission_path(
    state: &ServerState,
    group_id: Ulid,
    bucket: &str,
    key: &str,
) -> String {
    object_permission_path(
        state.get_realm_id(),
        group_id,
        state.get_node_id(),
        bucket,
        key,
    )
}

#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
