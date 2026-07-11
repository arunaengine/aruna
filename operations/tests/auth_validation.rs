use aruna_core::UserId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::structs::{RealmId, TokenClaims};
use aruna_operations::auth::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, validate_aruna_bearer_token,
};
use async_trait::async_trait;
use base64::Engine;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signer, SigningKey};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, errors::ErrorKind};
use std::collections::HashSet;
use ulid::Ulid;

#[derive(Default)]
struct TestAuthState {
    revoked_hashes: HashSet<String>,
    trusted_realms: HashSet<RealmId>,
}

#[async_trait]
impl ArunaBearerTokenValidationState for TestAuthState {
    async fn is_bearer_token_revoked(&self, token_hash: &str) -> bool {
        self.revoked_hashes.contains(token_hash)
    }

    async fn is_trusted_realm(&self, realm_id: &RealmId) -> bool {
        self.trusted_realms.contains(realm_id)
    }
}

#[tokio::test]
async fn validates_management_token_into_auth_context() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let claims = token_claims(realm_id, user_id);
    let token = sign_token(&realm_signing_key, &claims);
    let state = trusted_state(realm_id);

    let auth = validate_aruna_bearer_token(&state, &token).await.unwrap();

    assert_eq!(auth.user_id, user_id);
    assert_eq!(auth.realm_id, realm_id);
}

#[tokio::test]
async fn rejects_revoked_token_by_shared_hash() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let claims = token_claims(realm_id, user_id);
    let token = sign_token(&realm_signing_key, &claims);
    let mut state = trusted_state(realm_id);
    state.revoked_hashes.insert(bearer_token_hash(&token));

    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();

    assert!(matches!(error, ArunaBearerTokenError::TokenRevoked));
}

#[tokio::test]
async fn rejects_expired_token() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let mut claims = token_claims(realm_id, user_id);
    claims.iat = now.saturating_sub(7200);
    claims.exp = now.saturating_sub(3600);
    let token = sign_token(&realm_signing_key, &claims);
    let state = trusted_state(realm_id);

    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();

    match error {
        ArunaBearerTokenError::Expired => {}
        ArunaBearerTokenError::JwtError(error) => {
            assert!(matches!(error.kind(), ErrorKind::ExpiredSignature));
        }
        other => panic!("unexpected expired token error: {other:?}"),
    }
}

#[tokio::test]
async fn rejects_untrusted_realm() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let claims = token_claims(realm_id, user_id);
    let token = sign_token(&realm_signing_key, &claims);
    let state = TestAuthState::default();

    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();

    assert!(matches!(error, ArunaBearerTokenError::RealmNotTrusted));
}

#[tokio::test]
async fn validates_server_token_delegation_signature() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let issuer_signing_key = signing_key();
    let issuer_pubkey = public_key_base64(&issuer_signing_key);
    let mut claims = token_claims(realm_id, user_id);
    claims.issuer_pubkey = Some(issuer_pubkey.clone());
    claims.delegation_signature =
        Some(realm_signing_key.sign(issuer_pubkey.as_bytes()).to_string());
    let token = sign_token(&issuer_signing_key, &claims);
    let state = trusted_state(realm_id);

    let auth = validate_aruna_bearer_token(&state, &token).await.unwrap();

    assert_eq!(auth.user_id, user_id);
    assert_eq!(auth.realm_id, realm_id);
}

#[tokio::test]
async fn rejects_invalid_server_delegation_signature() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let issuer_signing_key = signing_key();
    let issuer_pubkey = public_key_base64(&issuer_signing_key);
    let other_issuer_pubkey = public_key_base64(&signing_key());
    let mut claims = token_claims(realm_id, user_id);
    claims.issuer_pubkey = Some(issuer_pubkey);
    claims.delegation_signature = Some(
        realm_signing_key
            .sign(other_issuer_pubkey.as_bytes())
            .to_string(),
    );
    let token = sign_token(&issuer_signing_key, &claims);
    let state = trusted_state(realm_id);

    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();

    assert!(matches!(error, ArunaBearerTokenError::PublicKeyError(_)));
}

#[tokio::test]
async fn rejects_mixed_delegation_claims() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let issuer_pubkey = public_key_base64(&signing_key());
    let state = trusted_state(realm_id);

    let mut issuer_only = token_claims(realm_id, user_id);
    issuer_only.issuer_pubkey = Some(issuer_pubkey.clone());
    let token = sign_token(&realm_signing_key, &issuer_only);
    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();
    assert!(matches!(error, ArunaBearerTokenError::InvalidServerToken));

    let mut delegation_only = token_claims(realm_id, user_id);
    delegation_only.delegation_signature =
        Some(realm_signing_key.sign(issuer_pubkey.as_bytes()).to_string());
    let token = sign_token(&realm_signing_key, &delegation_only);
    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();
    assert!(matches!(error, ArunaBearerTokenError::InvalidServerToken));
}

#[tokio::test]
async fn rejects_auth_context_conversion_failure() {
    let (realm_signing_key, realm_id, _) = realm_fixture();
    let (_, other_realm_id, other_user_id) = realm_fixture();
    let claims = token_claims(realm_id, other_user_id);
    assert_ne!(realm_id, other_realm_id);
    let token = sign_token(&realm_signing_key, &claims);
    let state = trusted_state(realm_id);

    let error = validate_aruna_bearer_token(&state, &token)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        ArunaBearerTokenError::AuthContextConversion(_)
    ));
}

fn realm_fixture() -> (SigningKey, RealmId, UserId) {
    let signing_key = signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::r#gen(), realm_id);
    (signing_key, realm_id, user_id)
}

fn signing_key() -> SigningKey {
    let mut rng = jsonwebtoken::signature::rand_core::OsRng;
    SigningKey::generate(&mut rng)
}

fn trusted_state(realm_id: RealmId) -> TestAuthState {
    let mut state = TestAuthState::default();
    state.trusted_realms.insert(realm_id);
    state
}

fn token_claims(realm_id: RealmId, user_id: UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::r#gen().to_string(),
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    }
}

fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn public_key_base64(signing_key: &SigningKey) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes())
}
