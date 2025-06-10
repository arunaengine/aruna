use ed25519_dalek::{SigningKey, VerifyingKey};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

use crate::error::{PermissionError, Result};
use crate::manager::UserIdentity;
use crate::paths::RealmKey;

/// JWT Claims for realm identity tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmTokenClaims {
    /// User ULID within the realm
    pub user_ulid: String,
    /// Realm key (hex encoded) that issued this token
    pub realm_key: String,
    /// Standard JWT issued at time
    pub iat: u64,
    /// Standard JWT expiration time
    pub exp: u64,
    /// Standard JWT issuer (aruna@realm_key format)
    pub iss: String,
    /// Standard JWT subject (user identifier)
    pub sub: String,
}

impl RealmTokenClaims {
    /// Create new claims for a user identity
    pub fn new(user_identity: &UserIdentity, expiration_hours: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let realm_hex = hex::encode(user_identity.realm_key);

        Self {
            user_ulid: user_identity.user_ulid.to_string(),
            realm_key: realm_hex.clone(),
            iat: now,
            exp: now + (expiration_hours * 3600),
            iss: format!("aruna@{}", realm_hex),
            sub: user_identity.user_ulid.to_string(),
        }
    }

    /// Convert claims back to UserIdentity
    pub fn to_user_identity(&self) -> Result<UserIdentity> {
        let user_ulid = Ulid::from_string(&self.user_ulid).map_err(|_| {
            PermissionError::ResourceNotFound("Invalid user ULID in token".to_string())
        })?;

        let realm_bytes = hex::decode(&self.realm_key).map_err(|_| {
            PermissionError::ResourceNotFound("Invalid realm key in token".to_string())
        })?;

        if realm_bytes.len() != 32 {
            return Err(PermissionError::ResourceNotFound(
                "Invalid realm key length in token".to_string(),
            ));
        }

        let mut realm_key = [0u8; 32];
        realm_key.copy_from_slice(&realm_bytes);

        Ok(UserIdentity::new(user_ulid, realm_key))
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.exp
    }
}

/// Standard OIDC JWT token claims (compatible with Keycloak and other providers)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcToken {
    /// Issuer (e.g., "https://keycloak.example.com/realms/myrealm")
    pub iss: String,
    /// Subject (user ID)
    pub sub: String,
    /// Audience
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<serde_json::Value>, // Can be string or array
    /// Expiration time
    pub exp: u64,
    /// Issued at
    pub iat: u64,
    /// Email address
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Email verified
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_verified: Option<bool>,
    /// Preferred username (common in Keycloak)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,
    /// Full name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Given name (first name)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub given_name: Option<String>,
    /// Family name (last name)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family_name: Option<String>,
    /// Additional claims (for flexibility)
    #[serde(flatten)]
    pub additional_claims: HashMap<String, serde_json::Value>,
}

impl OidcToken {
    /// Get a user identifier (preferring email, then preferred_username, then sub)
    pub fn get_user_identifier(&self) -> String {
        self.email
            .as_ref()
            .or(self.preferred_username.as_ref())
            .map(|s| s.clone())
            .unwrap_or_else(|| self.sub.clone())
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now > self.exp
    }
}

/// Configuration for trusted OIDC issuers
#[derive(Debug, Clone)]
pub enum OidcTrustConfig {
    /// Trust specific issuers only
    TrustedIssuers(Vec<String>),
    /// Trust all issuers (useful for development)
    TrustAll,
}

impl OidcTrustConfig {
    /// Check if an issuer is trusted
    pub fn is_trusted(&self, issuer: &str) -> bool {
        match self {
            OidcTrustConfig::TrustedIssuers(issuers) => issuers.contains(&issuer.to_string()),
            OidcTrustConfig::TrustAll => true,
        }
    }
}

/// Ed25519 key pair for realm token operations
#[derive(Debug, Clone)]
pub struct Ed25519KeyPair {
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
}

impl Ed25519KeyPair {
    /// Generate a new Ed25519 key pair using iroh's SecretKey
    pub fn generate() -> Self {
        let iroh_secret = iroh::SecretKey::generate(&mut OsRng);
        let signing_key = SigningKey::from_bytes(&iroh_secret.to_bytes());
        let verifying_key = signing_key.verifying_key();

        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Get the signing key as PKCS8 PEM format
    pub fn signing_key_pem(&self) -> Result<String> {
        use pkcs8::EncodePrivateKey;
        self.signing_key
            .to_pkcs8_pem(pkcs8::LineEnding::LF)
            .map(|s| s.to_string())
            .map_err(|e| {
                PermissionError::ResourceNotFound(format!("Failed to encode signing key: {}", e))
            })
    }

    /// Get the verifying key as PEM format
    pub fn verifying_key_pem(&self) -> Result<String> {
        use pkcs8::EncodePublicKey;
        self.verifying_key
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .map_err(|e| {
                PermissionError::ResourceNotFound(format!("Failed to encode verifying key: {}", e))
            })
    }
}

/// Simple token system for identity management
pub struct TokenSystem {
    /// Local realm key
    local_realm_key: RealmKey,
    /// Static configuration for trusted OIDC issuers
    oidc_trust_config: OidcTrustConfig,
    /// OIDC provider public keys (issuer -> public_key_pem as String)
    oidc_public_keys: HashMap<String, String>,
    /// Realm Ed25519 verifying keys for verification (realm_key -> verifying_key_pem as String)
    realm_public_keys: HashMap<RealmKey, String>,
    /// Default token expiration in hours
    default_expiration_hours: u64,
}

impl TokenSystem {
    /// Create new token system for a specific realm
    pub fn new(local_realm_key: RealmKey, oidc_trust_config: OidcTrustConfig) -> Self {
        Self {
            local_realm_key,
            oidc_trust_config,
            oidc_public_keys: HashMap::new(),
            realm_public_keys: HashMap::new(),
            default_expiration_hours: 24,
        }
    }

    /// Add OIDC provider public key for token verification (expects PEM format)
    pub fn add_oidc_public_key(&mut self, issuer: String, public_key_pem: String) {
        self.oidc_public_keys.insert(issuer, public_key_pem);
    }

    /// Add realm Ed25519 verifying key for verification (expects PEM format)
    pub fn add_realm_public_key(&mut self, realm_key: RealmKey, verifying_key_pem: String) {
        self.realm_public_keys.insert(realm_key, verifying_key_pem);
    }

    /// Register user from OIDC token - creates new UserIdentity after verification or returns existing one
    pub fn register_user<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &self,
        token: &str,
        store: &'a S,
        txn: &mut <S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<UserIdentity> {
        // First verify and decode the OIDC token
        let oidc_token = self.verify_oidc_token(token)?;

        // Use issuer as provider and sub as subject for OIDC identity mapping
        self.register_user_from_oidc_claims(&oidc_token.iss, &oidc_token.sub, store, txn)
    }

    /// Register user from OIDC provider and subject - creates new UserIdentity or returns existing one
    /// This method can be used when you already have the OIDC provider and subject information
    pub fn register_user_from_oidc_claims<
        'a,
        S: aruna_storage::storage::store::Store<'a> + 'static,
    >(
        &self,
        oidc_provider: &str,
        oidc_sub: &str,
        store: &'a S,
        txn: &mut <S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<UserIdentity> {
        // Check if provider is trusted
        if !self.oidc_trust_config.is_trusted(oidc_provider) {
            return Err(PermissionError::ResourceNotFound(format!(
                "Untrusted OIDC issuer: {}",
                oidc_provider
            )));
        }

        // Try to lookup existing user ULID
        if let Some(existing_user_ulid) =
            self.get_user_from_oidc(oidc_provider, oidc_sub, store, txn)?
        {
            // Return existing identity
            return Ok(UserIdentity::new(existing_user_ulid, self.local_realm_key));
        }

        // No existing mapping found, create new user
        let user_ulid = Ulid::new();

        // Store the OIDC identity mapping
        self.add_oidc_identity(oidc_provider, oidc_sub, user_ulid, store, txn)?;

        // Create UserIdentity for local realm
        let user_identity = UserIdentity::new(user_ulid, self.local_realm_key);

        Ok(user_identity)
    }

    /// Add OIDC identity mapping (compatible with manager.rs pattern)
    pub fn add_oidc_identity<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &self,
        oidc_provider: &str,
        oidc_sub: &str,
        user_ulid: Ulid,
        store: &'a S,
        txn: &mut <S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<()> {
        let key = format!("{}:{}", oidc_provider, oidc_sub);
        let value = postcard::to_allocvec(&user_ulid).map_err(PermissionError::PostcardError)?;
        store
            .put(
                txn,
                crate::manager::OIDC_IDENTITIES_DB,
                key.as_bytes(),
                &value,
            )
            .map_err(PermissionError::StorageError)?;
        Ok(())
    }

    /// Lookup user ULID from OIDC identity (compatible with manager.rs pattern)
    pub fn get_user_from_oidc<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &self,
        oidc_provider: &str,
        oidc_sub: &str,
        store: &'a S,
        txn: &<S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<Option<Ulid>> {
        let key = format!("{}:{}", oidc_provider, oidc_sub);

        if let Some(user_bytes) = store
            .get(txn, crate::manager::OIDC_IDENTITIES_DB, key.as_bytes())
            .map_err(PermissionError::StorageError)?
        {
            let user_ulid: Ulid =
                postcard::from_bytes(&user_bytes).map_err(PermissionError::PostcardError)?;
            Ok(Some(user_ulid))
        } else {
            Ok(None)
        }
    }

    /// Verify and decode OIDC token (supports RSA, EC, EdDSA algorithms)
    fn verify_oidc_token(&self, token: &str) -> Result<OidcToken> {
        // Decode without verification first to get issuer for key lookup
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(Algorithm::RS256);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        let unverified =
            decode::<OidcToken>(token, &dummy_key, &unverified_validation).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid OIDC token format".to_string())
            })?;

        let issuer = &unverified.claims.iss;

        // Check if issuer is trusted
        if !self.oidc_trust_config.is_trusted(issuer) {
            return Err(PermissionError::ResourceNotFound(format!(
                "Untrusted OIDC issuer: {}",
                issuer
            )));
        }

        // Get public key for this issuer
        let public_key_pem = self.oidc_public_keys.get(issuer).ok_or_else(|| {
            PermissionError::ResourceNotFound(format!(
                "No public key available for issuer: {}",
                issuer
            ))
        })?;

        // Get algorithm from token header
        let header = decode_header(token)
            .map_err(|_| PermissionError::ResourceNotFound("Invalid JWT format".to_string()))?;

        // Create decoding key based on algorithm
        let decoding_key = match header.alg {
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
                DecodingKey::from_rsa_pem(public_key_pem.as_bytes()).map_err(|_| {
                    PermissionError::ResourceNotFound("Invalid RSA public key".to_string())
                })?
            }
            Algorithm::ES256 | Algorithm::ES384 => {
                DecodingKey::from_ec_pem(public_key_pem.as_bytes()).map_err(|_| {
                    PermissionError::ResourceNotFound("Invalid EC public key".to_string())
                })?
            }
            Algorithm::EdDSA => {
                DecodingKey::from_ed_pem(public_key_pem.as_bytes()).map_err(|_| {
                    PermissionError::ResourceNotFound("Invalid Ed25519 public key".to_string())
                })?
            }
            _ => {
                return Err(PermissionError::ResourceNotFound(format!(
                    "Unsupported algorithm: {:?}",
                    header.alg
                )));
            }
        };

        // Set up proper validation
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer]);

        // Verify and decode token
        let token_data = decode::<OidcToken>(token, &decoding_key, &validation).map_err(|e| {
            PermissionError::ResourceNotFound(format!("OIDC token verification failed: {}", e))
        })?;

        // Additional expiration check (though JWT library should handle this)
        if token_data.claims.is_expired() {
            return Err(PermissionError::ResourceNotFound(
                "OIDC token expired".to_string(),
            ));
        }

        Ok(token_data.claims)
    }

    /// Get identity from token (OIDC JWT or Aruna JWT)
    pub fn get_identity<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &self,
        token: &str,
        store: &'a S,
        txn: &mut <S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<UserIdentity> {
        // Decode without verification to check issuer type
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(Algorithm::EdDSA);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        // Try to decode as generic JSON to get issuer
        let unverified = decode::<serde_json::Value>(token, &dummy_key, &unverified_validation)
            .map_err(|_| PermissionError::ResourceNotFound("Invalid JWT format".to_string()))?;

        let issuer = unverified
            .claims
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PermissionError::ResourceNotFound("Missing issuer in token".to_string())
            })?;

        // Check if it's an Aruna token (issuer starts with "aruna@")
        if issuer.starts_with("aruna@") {
            return self.validate_realm_token(token);
        }

        // Otherwise, treat as OIDC token and register/lookup user
        self.register_user(token, store, txn)
    }

    /// Generate realm token for UserIdentity using Ed25519 signing key (PKCS8 PEM format)
    pub fn generate_token(
        &self,
        user_identity: &UserIdentity,
        signing_key_pem: &str,
    ) -> Result<String> {
        // Only generate tokens for users in our local realm
        if user_identity.realm_key != self.local_realm_key {
            return Err(PermissionError::ResourceNotFound(
                "Cannot generate token for foreign realm user".to_string(),
            ));
        }

        let claims = RealmTokenClaims::new(user_identity, self.default_expiration_hours);

        // Use Ed25519 for realm tokens only
        let encoding_key = EncodingKey::from_ed_pem(signing_key_pem.as_bytes()).map_err(|e| {
            PermissionError::ResourceNotFound(format!(
                "Failed to create Ed25519 encoding key: {}",
                e
            ))
        })?;

        let header = Header::new(Algorithm::EdDSA);
        encode(&header, &claims, &encoding_key).map_err(|e| {
            PermissionError::ResourceNotFound(format!("Failed to encode EdDSA token: {}", e))
        })
    }

    /// Validate realm JWT token and extract UserIdentity (Ed25519 only)
    pub fn validate_realm_token(&self, token: &str) -> Result<UserIdentity> {
        // Decode without verification first to get realm info for key lookup
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(Algorithm::EdDSA);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        let unverified = decode::<RealmTokenClaims>(token, &dummy_key, &unverified_validation)
            .map_err(|_| {
                PermissionError::ResourceNotFound("Failed to decode realm token claims".to_string())
            })?;

        // Check expiration early
        if unverified.claims.is_expired() {
            return Err(PermissionError::ResourceNotFound(
                "Token expired".to_string(),
            ));
        }

        // Extract realm key from issuer
        let realm_key = if let Some(realm_hex) = unverified.claims.iss.strip_prefix("aruna@") {
            let realm_bytes = hex::decode(realm_hex).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid realm key in issuer".to_string())
            })?;

            if realm_bytes.len() != 32 {
                return Err(PermissionError::ResourceNotFound(
                    "Invalid realm key length in issuer".to_string(),
                ));
            }

            let mut realm_key = [0u8; 32];
            realm_key.copy_from_slice(&realm_bytes);
            realm_key
        } else {
            return Err(PermissionError::ResourceNotFound(
                "Invalid Aruna token issuer format".to_string(),
            ));
        };

        // Get Ed25519 verifying key for verification
        let verifying_key_pem = self.realm_public_keys.get(&realm_key).ok_or_else(|| {
            PermissionError::ResourceNotFound("Realm public key not found".to_string())
        })?;

        // Get algorithm from token header - must be EdDSA for realm tokens
        let header = decode_header(token)
            .map_err(|_| PermissionError::ResourceNotFound("Invalid JWT format".to_string()))?;

        if header.alg != Algorithm::EdDSA {
            return Err(PermissionError::ResourceNotFound(format!(
                "Invalid algorithm for realm token: {:?}, expected EdDSA",
                header.alg
            )));
        }

        // Create Ed25519 decoding key
        let decoding_key = DecodingKey::from_ed_pem(verifying_key_pem.as_bytes()).map_err(|e| {
            PermissionError::ResourceNotFound(format!("Invalid Ed25519 verifying key: {}", e))
        })?;

        // Verify token signature with proper validation
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&unverified.claims.iss]);

        let verified_token = decode::<RealmTokenClaims>(token, &decoding_key, &validation)
            .map_err(|e| {
                PermissionError::ResourceNotFound(format!("Token verification failed: {}", e))
            })?;

        // Convert to UserIdentity
        verified_token.claims.to_user_identity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_ulid(suffix: u8) -> Ulid {
        let mut bytes = [0u8; 16];
        bytes[15] = suffix;
        Ulid::from_bytes(bytes)
    }

    fn create_test_realm_key(suffix: u8) -> RealmKey {
        let mut key = [0u8; 32];
        key[31] = suffix;
        key
    }

    #[test]
    fn test_ed25519_key_pair_generation() {
        let key_pair = Ed25519KeyPair::generate();

        let signing_pem = key_pair.signing_key_pem().unwrap();
        let verifying_pem = key_pair.verifying_key_pem().unwrap();

        assert!(signing_pem.contains("BEGIN PRIVATE KEY"));
        assert!(verifying_pem.contains("BEGIN PUBLIC KEY"));
    }

    #[test]
    fn test_oidc_trust_config() {
        let trusted_config = OidcTrustConfig::TrustedIssuers(vec![
            "https://accounts.google.com".to_string(),
            "https://keycloak.example.com/realms/test".to_string(),
        ]);

        assert!(trusted_config.is_trusted("https://accounts.google.com"));
        assert!(trusted_config.is_trusted("https://keycloak.example.com/realms/test"));
        assert!(!trusted_config.is_trusted("https://evil.com"));

        let trust_all_config = OidcTrustConfig::TrustAll;
        assert!(trust_all_config.is_trusted("https://accounts.google.com"));
        assert!(trust_all_config.is_trusted("https://evil.com"));
    }

    #[test]
    fn test_realm_token_claims() {
        let realm_key = create_test_realm_key(1);
        let user_ulid = create_test_ulid(2);
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let claims = RealmTokenClaims::new(&user_identity, 24);
        let recovered_identity = claims.to_user_identity().unwrap();

        assert_eq!(recovered_identity, user_identity);
        assert_eq!(claims.iss, format!("aruna@{}", hex::encode(realm_key)));
        assert!(!claims.is_expired());
    }

    #[test]
    fn test_token_system_creation() {
        let realm_key = create_test_realm_key(1);
        let trust_config =
            OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);

        let mut token_system = TokenSystem::new(realm_key, trust_config);

        // Add OIDC public key (RSA for testing)
        let rsa_public_key = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3VoPN9PKUjKFLMwOge9+
GnWSGqIFRN5PefrZKFJ9d0I0P9DI/hI1/P8TdTGczkOQG+QYkwQYRbHvyIKOAPjM
zJBEwOv8/sYKrJnM4CbzA+p9+f5X5XhqyuVzgE5pGHpE7Jg1g3bvBbKh9B0BvuOQ
LhRj9dE8aJb2yS7Jk1kEOGfN8QnMLRf4Mj+ZUZ7SuKJwdEQ1vJE9pEgZQHr45FPf
OYPh1F2PZhQ1Jf5p0tYwMHT7k2+4HgWOz1R23XvFxT8Y4PnN8X3+j1S3WyqKO1R2
g8MQTS1BnfL1XHZj5r9OhF2h/Qzlx/eHf1zA9L+rYCdm9FjKNT3bJl6sKO9zJp8k
ZwIDAQAB
-----END PUBLIC KEY-----"#;

        token_system.add_oidc_public_key(
            "https://accounts.google.com".to_string(),
            rsa_public_key.to_string(),
        );

        // Add realm Ed25519 key
        let key_pair = Ed25519KeyPair::generate();
        let verifying_pem = key_pair.verifying_key_pem().unwrap();
        token_system.add_realm_public_key(realm_key, verifying_pem);

        assert_eq!(token_system.local_realm_key, realm_key);
    }

    #[test]
    fn test_ed25519_token_round_trip() {
        let realm_key = create_test_realm_key(1);
        let user_ulid = create_test_ulid(2);
        let mut token_system = TokenSystem::new(realm_key, OidcTrustConfig::TrustAll);

        // Generate Ed25519 key pair
        let key_pair = Ed25519KeyPair::generate();
        let signing_pem = key_pair.signing_key_pem().unwrap();
        let verifying_pem = key_pair.verifying_key_pem().unwrap();

        // Configure the token system with test keys
        token_system.add_realm_public_key(realm_key, verifying_pem);

        let user_identity = UserIdentity::new(user_ulid, realm_key);

        // Generate token using Ed25519
        let token = token_system
            .generate_token(&user_identity, &signing_pem)
            .unwrap();
        assert!(!token.is_empty());

        // Validate token and extract identity
        let recovered_identity = token_system.validate_realm_token(&token).unwrap();

        assert_eq!(recovered_identity, user_identity);
    }

    #[test]
    fn test_cross_realm_token_validation() {
        let realm_a = create_test_realm_key(1);
        let realm_b = create_test_realm_key(2);
        let user_ulid = create_test_ulid(10);

        let mut token_system_a = TokenSystem::new(realm_a, OidcTrustConfig::TrustAll);
        let mut token_system_b = TokenSystem::new(realm_b, OidcTrustConfig::TrustAll);

        // Generate Ed25519 key pair for realm A
        let key_pair = Ed25519KeyPair::generate();
        let signing_pem = key_pair.signing_key_pem().unwrap();
        let verifying_pem = key_pair.verifying_key_pem().unwrap();

        // Configure realm A with keys
        token_system_a.add_realm_public_key(realm_a, verifying_pem.clone());

        // Configure realm B to recognize realm A's public key
        token_system_b.add_realm_public_key(realm_a, verifying_pem);

        let user_identity_a = UserIdentity::new(user_ulid, realm_a);

        // Generate token in realm A
        let token = token_system_a
            .generate_token(&user_identity_a, &signing_pem)
            .unwrap();

        // Validate token in realm B (should work since B has A's public key)
        let recovered_identity = token_system_b.validate_realm_token(&token).unwrap();
        assert_eq!(recovered_identity, user_identity_a);
    }

    #[test]
    fn test_token_generation_foreign_realm() {
        let realm_a = create_test_realm_key(1);
        let realm_b = create_test_realm_key(2);
        let user_ulid = create_test_ulid(10);
        let token_system = TokenSystem::new(realm_a, OidcTrustConfig::TrustAll);

        let key_pair = Ed25519KeyPair::generate();
        let signing_pem = key_pair.signing_key_pem().unwrap();

        // Try to generate token for user from different realm
        let foreign_user_identity = UserIdentity::new(user_ulid, realm_b);
        let result = token_system.generate_token(&foreign_user_identity, &signing_pem);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_validation_without_public_key() {
        let realm_key = create_test_realm_key(1);
        let user_ulid = create_test_ulid(2);
        let mut token_system_generator = TokenSystem::new(realm_key, OidcTrustConfig::TrustAll);

        let key_pair = Ed25519KeyPair::generate();
        let signing_pem = key_pair.signing_key_pem().unwrap();
        let verifying_pem = key_pair.verifying_key_pem().unwrap();

        token_system_generator.add_realm_public_key(realm_key, verifying_pem);

        let user_identity = UserIdentity::new(user_ulid, realm_key);
        let token = token_system_generator
            .generate_token(&user_identity, &signing_pem)
            .unwrap();

        // Create a new token system without the public key
        let validator_system = TokenSystem::new(realm_key, OidcTrustConfig::TrustAll);

        // Should fail because no public key is available for validation
        let result = validator_system.validate_realm_token(&token);
        assert!(result.is_err());
    }
}
