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
use std::array::TryFromSliceError;
use std::str::FromStr;
use thiserror::Error;

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
