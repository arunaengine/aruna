use aruna_core::auth::bearer_token_hash;
use aruna_core::errors::ConversionError;
use aruna_core::structs::{AuthContext, RealmId, TokenClaims};
use async_trait::async_trait;
use base64::Engine;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use lru::LruCache;
use std::array::TryFromSliceError;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Mutex;

#[async_trait]
pub trait ArunaBearerTokenValidationState: Sync {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool;
    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool;

    async fn decoding_key_for_issuer(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        decoding_key_from_base64_public_key(issuer_pubkey)
    }
}

#[derive(Debug, Error)]
pub enum ArunaBearerTokenError {
    #[error("Realm is not trusted")]
    RealmNotTrusted,
    #[error("Token is revoked")]
    TokenRevoked,
    #[error("Invalid issuer key")]
    InvalidIssuerKey,
    #[error("Token expired")]
    Expired,
    #[error("Invalid server token")]
    InvalidServerToken,
    #[error(transparent)]
    AuthContextConversion(#[from] ConversionError),
    #[error(transparent)]
    PublicKeyError(#[from] ed25519_dalek::ed25519::Error),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    PublicKeyConversionError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    JwtError(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),
}

pub async fn validate_aruna_bearer_token<S>(
    state: &S,
    token: &str,
) -> Result<AuthContext, ArunaBearerTokenError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let claims = decode_aruna_bearer_token(state, token).await?;
    Ok(claims.try_into()?)
}

pub async fn decode_aruna_bearer_token<S>(
    state: &S,
    token: &str,
) -> Result<TokenClaims, ArunaBearerTokenError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let unvalidated_claims = insecure_decode::<TokenClaims>(token)?;

    let token_hash = bearer_token_hash(token);
    if state.is_bearer_token_revoked(&token_hash).await {
        return Err(ArunaBearerTokenError::TokenRevoked);
    }

    let issuer = match (
        unvalidated_claims.claims.issuer_pubkey.as_deref(),
        unvalidated_claims.claims.delegation_signature.is_some(),
    ) {
        (Some(issuer_pubkey), true) => issuer_pubkey,
        _ => &unvalidated_claims.claims.iss,
    };
    // Only trusted realm or delegated issuers may populate the bounded cache;
    // untrusted issuers are verified with an ephemeral key that is discarded.
    let decoding_key = if issuer_key_is_trusted(state, &unvalidated_claims.claims).await {
        state.decoding_key_for_issuer(issuer).await?
    } else {
        decoding_key_from_base64_public_key(issuer)?
    };
    let claims = decode::<TokenClaims>(token, &decoding_key, &Validation::new(Algorithm::EdDSA))?;

    validate_aruna_bearer_token_claims(state, &claims.claims).await?;
    Ok(claims.claims)
}

async fn issuer_key_is_trusted<S>(state: &S, claims: &TokenClaims) -> bool
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Ok(realm_id) = RealmId::from_base64(&claims.iss) else {
        return false;
    };
    if !state.is_trusted_realm(&realm_id).await {
        return false;
    }
    match (&claims.delegation_signature, &claims.issuer_pubkey) {
        (Some(delegation_signature), Some(issuer_pubkey)) => {
            verify_realm_delegation(&claims.iss, issuer_pubkey, delegation_signature).is_ok()
        }
        (None, None) => true,
        (_, _) => false,
    }
}

pub async fn validate_aruna_bearer_token_claims<S>(
    state: &S,
    claims: &TokenClaims,
) -> Result<(), ArunaBearerTokenError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let now = chrono::Utc::now().timestamp() as u64;
    if now > claims.exp {
        return Err(ArunaBearerTokenError::Expired);
    }

    let realm_id =
        RealmId::from_base64(&claims.iss).map_err(|_| ArunaBearerTokenError::InvalidIssuerKey)?;
    if !state.is_trusted_realm(&realm_id).await {
        return Err(ArunaBearerTokenError::RealmNotTrusted);
    }

    match (&claims.delegation_signature, &claims.issuer_pubkey) {
        (Some(delegation_signature), Some(issuer_pubkey)) => {
            verify_realm_delegation(&claims.iss, issuer_pubkey, delegation_signature)
        }
        (None, None) => Ok(()),
        (_, _) => Err(ArunaBearerTokenError::InvalidServerToken),
    }
}

fn verify_realm_delegation(
    realm_iss: &str,
    issuer_pubkey: &str,
    delegation_signature: &str,
) -> Result<(), ArunaBearerTokenError> {
    let realm_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(realm_iss)?;
    let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
    let signature = Signature::from_str(delegation_signature)?;
    realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
    Ok(())
}

pub fn decoding_key_from_base64_public_key(
    issuer_pubkey: &str,
) -> Result<DecodingKey, ArunaBearerTokenError> {
    let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(issuer_pubkey)?
        .try_into()
        .map_err(|_| ArunaBearerTokenError::InvalidIssuerKey)?;
    let public_key = VerifyingKey::from_bytes(&issuer_pubkey)?;
    let public_key_pem = public_key.to_public_key_pem(LineEnding::default())?;
    Ok(DecodingKey::from_ed_pem(public_key_pem.as_bytes())?)
}

/// Maximum number of issuer decoding keys retained in the process cache.
pub const ISSUER_KEY_CACHE_CAPACITY: usize = 1024;
/// Time an issuer decoding key is retained before it is refreshed.
pub const ISSUER_KEY_CACHE_TTL: Duration = Duration::from_secs(3600);

/// Bounded, TTL + LRU cache of issuer decoding keys keyed by base64 public key.
pub struct IssuerKeyCache {
    entries: Mutex<LruCache<String, CachedIssuerKey>>,
    ttl: Duration,
}

#[derive(Clone)]
struct CachedIssuerKey {
    key: DecodingKey,
    inserted_at: Instant,
}

impl std::fmt::Debug for IssuerKeyCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuerKeyCache").finish_non_exhaustive()
    }
}

impl Default for IssuerKeyCache {
    fn default() -> Self {
        Self::new()
    }
}

impl IssuerKeyCache {
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(ISSUER_KEY_CACHE_CAPACITY, ISSUER_KEY_CACHE_TTL)
    }

    pub fn with_capacity_and_ttl(capacity: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
            ttl,
        }
    }

    pub async fn get_or_insert(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get(issuer_pubkey).cloned() {
            if entry.inserted_at.elapsed() < self.ttl {
                return Ok(entry.key);
            }
            entries.pop(issuer_pubkey);
        }
        let key = decoding_key_from_base64_public_key(issuer_pubkey)?;
        entries.put(
            issuer_pubkey.to_string(),
            CachedIssuerKey {
                key: key.clone(),
                inserted_at: Instant::now(),
            },
        );
        Ok(key)
    }

    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use ed25519_dalek::SigningKey;

    fn pubkey_b64(key: &SigningKey) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.verifying_key().to_bytes())
    }

    #[tokio::test]
    async fn issuer_key_cache_evicts_beyond_capacity() {
        let cache = IssuerKeyCache::with_capacity_and_ttl(2, ISSUER_KEY_CACHE_TTL);
        let mut rng = jsonwebtoken::signature::rand_core::OsRng;
        for _ in 0..5 {
            let key = SigningKey::generate(&mut rng);
            cache.get_or_insert(&pubkey_b64(&key)).await.unwrap();
        }
        assert_eq!(cache.len().await, 2);
    }

    async fn backdate_entry(cache: &IssuerKeyCache, pubkey: &str, age: Duration) -> Instant {
        let inserted_at = Instant::now().checked_sub(age).unwrap();
        cache
            .entries
            .lock()
            .await
            .peek_mut(pubkey)
            .unwrap()
            .inserted_at = inserted_at;
        inserted_at
    }

    async fn entry_inserted_at(cache: &IssuerKeyCache, pubkey: &str) -> Instant {
        cache.entries.lock().await.peek(pubkey).unwrap().inserted_at
    }

    #[tokio::test]
    async fn issuer_key_cache_refreshes_expired_entries() {
        let ttl = Duration::from_secs(3600);
        let cache = IssuerKeyCache::with_capacity_and_ttl(4, ttl);
        let key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let pubkey = pubkey_b64(&key);
        cache.get_or_insert(&pubkey).await.unwrap();

        // Just inside the TTL: served from cache, entry untouched.
        let inside = backdate_entry(&cache, &pubkey, ttl - Duration::from_secs(60)).await;
        cache.get_or_insert(&pubkey).await.unwrap();
        assert_eq!(entry_inserted_at(&cache, &pubkey).await, inside);

        // Just past the TTL: refreshed in place without accumulating.
        let expired = backdate_entry(&cache, &pubkey, ttl + Duration::from_secs(1)).await;
        cache.get_or_insert(&pubkey).await.unwrap();
        assert!(entry_inserted_at(&cache, &pubkey).await > expired);
        assert_eq!(cache.len().await, 1);
    }

    #[tokio::test]
    async fn issuer_key_cache_rejects_invalid_pubkey() {
        let cache = IssuerKeyCache::new();
        assert!(cache.get_or_insert("not-base64!!").await.is_err());
        assert!(cache.is_empty().await);
    }
}
