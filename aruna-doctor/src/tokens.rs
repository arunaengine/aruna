use crate::error::CliError;
use aruna::config::load;
use aruna_api::error::TokenError;
use aruna_api::server_state::{
    TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, load_persisted_state,
};
use aruna_core::structs::{RealmId, TokenClaims};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_storage::storage;
use base64::Engine;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;

pub async fn create_token(
    user_id: Option<String>,
    expiry: Option<u64>,
) -> Result<String, CliError> {
    let user_id = user_id
        .map(|id| Ulid::from_string(&id))
        .unwrap_or(Ok(Ulid::new()))?;

    let (config, _) = load().await?;
    let storage_handle = storage::FjallStorage::open(&config.storage_path)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        automerge_handle: None,
        task_handle: None,
    });

    let token_config = CreateTokenConfig {
        time: chrono::Utc::now().timestamp() as u64,
        expiry,
        user_id,
        realm_id: config.realm_id.clone(),
        node_capabilities: config.node_capabilities,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone())?;

    Ok(drive(token_operation, &driver_ctx).await?)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenView {
    pub claims: TokenClaims,
    pub is_blacklisted: bool,
    pub valid: Valid,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Valid {
    True,
    False { reason: String },
}

pub async fn view_token(token: String) -> Result<String, CliError> {
    let hash = blake3::hash(token.as_bytes()).to_string();
    let unvalidated_claims = insecure_decode::<TokenClaims>(&token)?;

    let (config, _) = load().await.unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        automerge_handle: None,
        task_handle: None,
    });
    let persisted_blacklist = load_persisted_state::<HashSet<String, ahash::RandomState>>(
        &driver_ctx,
        TOKEN_REVOCATION_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let blacklisted = persisted_blacklist.contains(&hash);

    let trusted_realms = load_persisted_state::<HashSet<RealmId, ahash::RandomState>>(
        &driver_ctx,
        TRUSTED_REALMS_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let valid = match validate(unvalidated_claims.claims.clone(), token, trusted_realms).await {
        Ok(_) => Valid::True,
        Err(err) => Valid::False {
            reason: err.to_string(),
        },
    };

    let token_view = TokenView {
        claims: unvalidated_claims.claims,
        is_blacklisted: blacklisted,
        valid,
    };

    Ok(serde_json::to_string_pretty(&token_view)?)
}

async fn validate(
    claims: TokenClaims,
    token: String,
    trusted_realms: HashSet<RealmId, ahash::RandomState>,
) -> Result<(), TokenError> {
    let claims = match (claims.issuer_pubkey, claims.delegation_signature.is_some()) {
        (Some(issuer), true) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(issuer.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
        (_, _) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(claims.iss.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
    };

    let now = chrono::Utc::now().timestamp() as u64;
    if now > claims.claims.exp {
        return Err(TokenError::Expired);
    }

    if !trusted_realms.contains(
        &RealmId::from_base64(&claims.claims.iss).map_err(|_| TokenError::InvalidIssuerKey)?,
    ) {
        return Err(TokenError::RealmNotTrusted);
    }

    // Check server token claims
    match (
        &claims.claims.delegation_signature,
        &claims.claims.issuer_pubkey,
    ) {
        (Some(delegation_token), Some(issuer_pubkey)) => {
            // Check delegation signature
            let realm_key =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&claims.claims.iss)?;
            let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
            let signature = Signature::from_str(delegation_token)?;
            realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
            Ok(())
        }
        (None, None) => Ok(()),
        (_, _) => Err(TokenError::InvalidServerToken),
    }
}
