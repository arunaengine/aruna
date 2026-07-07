use aruna_core::auth::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::structs::{AuthContext, RealmId, TokenClaims};
use aruna_storage::StorageHandle;
use async_trait::async_trait;
use base64::Engine;
use byteview::ByteView;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::de::DeserializeOwned;
use std::array::TryFromSliceError;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::warn;

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
    let decoding_key = state.decoding_key_for_issuer(issuer).await?;
    let claims = decode::<TokenClaims>(token, &decoding_key, &Validation::new(Algorithm::EdDSA))?;

    validate_aruna_bearer_token_claims(state, &claims.claims).await?;
    Ok(claims.claims)
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
            let realm_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&claims.iss)?;
            let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
            let signature = Signature::from_str(delegation_signature)?;
            realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
            Ok(())
        }
        (None, None) => Ok(()),
        (_, _) => Err(ArunaBearerTokenError::InvalidServerToken),
    }
}

/// Node-wide bearer validation state backed by the local API state keyspace.
///
/// Reads the token revocation and trusted-realm lists from storage and caches
/// issuer decoding keys in memory. Shared by every inbound path that must
/// re-validate a forwarded bearer against this node's trust configuration (the
/// metadata handle and the holder-routing proxy).
#[derive(Clone)]
pub struct NodeBearerValidationState {
    storage_handle: StorageHandle,
    issuer_keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl NodeBearerValidationState {
    pub fn new(storage_handle: StorageHandle) -> Self {
        Self {
            storage_handle,
            issuer_keys: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ArunaBearerTokenValidationState for NodeBearerValidationState {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool {
        match load_node_auth_state::<HashSet<String>>(
            &self.storage_handle,
            TOKEN_REVOCATION_LIST_KEY,
        )
        .await
        {
            Ok(revoked) => revoked.contains(token_hash),
            Err(error) => {
                warn!(error = %error, "Failed to read node token revocation state");
                true
            }
        }
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        match load_node_auth_state::<HashSet<RealmId>>(
            &self.storage_handle,
            TRUSTED_REALMS_LIST_KEY,
        )
        .await
        {
            Ok(trusted) => trusted.contains(realm_id),
            Err(error) => {
                warn!(error = %error, "Failed to read node trusted realms state");
                false
            }
        }
    }

    async fn decoding_key_for_issuer(
        &self,
        issuer_pubkey: &str,
    ) -> Result<DecodingKey, ArunaBearerTokenError> {
        let read_lock = self.issuer_keys.read().await;
        let key = read_lock.get(issuer_pubkey).cloned();
        drop(read_lock);
        if let Some(key) = key {
            return Ok(key);
        }

        let decoding_key = decoding_key_from_base64_public_key(issuer_pubkey)?;
        self.issuer_keys
            .write()
            .await
            .insert(issuer_pubkey.to_string(), decoding_key.clone());
        Ok(decoding_key)
    }
}

async fn load_node_auth_state<T>(storage_handle: &StorageHandle, key: &[u8]) -> Result<T, String>
where
    T: DeserializeOwned + Default,
{
    match storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => postcard::from_bytes(&bytes).map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(T::default()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected node auth state read result: {other:?}")),
    }
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
