use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use ulid::Ulid;

use crate::error::{PermissionError, Result};
use crate::manager::UserIdentity;

/// JWT Claims for realm identity tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmTokenClaims {
    /// User ULID within the realm
    pub user_ulid: String,
    /// Realm ULID that issued this token
    pub realm_ulid: String,
    /// Standard JWT issued at time
    pub iat: u64,
    /// Standard JWT expiration time
    pub exp: u64,
    /// Standard JWT issuer (aruna@realm_id format)
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

        Self {
            user_ulid: user_identity.user_ulid.to_string(),
            realm_ulid: user_identity.realm_ulid.to_string(),
            iat: now,
            exp: now + (expiration_hours * 3600),
            iss: format!("aruna@{}", user_identity.realm_ulid),
            sub: user_identity.user_ulid.to_string(),
        }
    }

    /// Convert claims back to UserIdentity
    pub fn to_user_identity(&self) -> Result<UserIdentity> {
        let user_ulid = Ulid::from_string(&self.user_ulid).map_err(|_| {
            PermissionError::ResourceNotFound("Invalid user ULID in token".to_string())
        })?;
        let realm_ulid = Ulid::from_string(&self.realm_ulid).map_err(|_| {
            PermissionError::ResourceNotFound("Invalid realm ULID in token".to_string())
        })?;

        Ok(UserIdentity::new(user_ulid, realm_ulid))
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

/// Convert raw Ed25519 private key bytes to PKCS#8 DER format
fn ed25519_private_key_to_pkcs8_der(private_key_bytes: &[u8; 32]) -> Vec<u8> {
    // PKCS#8 DER encoding for Ed25519 private key
    // This is the ASN.1 structure for Ed25519 private keys
    let mut pkcs8_der = Vec::new();

    // PKCS#8 header for Ed25519
    pkcs8_der.extend_from_slice(&[
        0x30, 0x2e, // SEQUENCE, length 46
        0x02, 0x01, 0x00, // INTEGER version (0)
        0x30, 0x05, // SEQUENCE, length 5 (algorithm identifier)
        0x06, 0x03, 0x2b, 0x65, 0x70, // OID for Ed25519 (1.3.101.112)
        0x04, 0x22, // OCTET STRING, length 34
        0x04, 0x20, // OCTET STRING, length 32 (inner key)
    ]);

    // Add the actual private key bytes
    pkcs8_der.extend_from_slice(private_key_bytes);

    pkcs8_der
}

/// Convert raw Ed25519 public key bytes to SubjectPublicKeyInfo DER format
fn ed25519_public_key_to_spki_der(public_key_bytes: &[u8; 32]) -> Vec<u8> {
    // SubjectPublicKeyInfo DER encoding for Ed25519 public key
    let mut spki_der = Vec::new();

    // SPKI header for Ed25519
    spki_der.extend_from_slice(&[
        0x30, 0x2a, // SEQUENCE, length 42
        0x30, 0x05, // SEQUENCE, length 5 (algorithm identifier)
        0x06, 0x03, 0x2b, 0x65, 0x70, // OID for Ed25519 (1.3.101.112)
        0x03, 0x21, 0x00, // BIT STRING, length 33, unused bits 0
    ]);

    // Add the actual public key bytes
    spki_der.extend_from_slice(public_key_bytes);

    spki_der
}

/// Simple token system for identity management
pub struct TokenSystem {
    /// Local realm ULID
    local_realm_ulid: Ulid,
    /// Static configuration for trusted OIDC issuers
    oidc_trust_config: OidcTrustConfig,
    /// OIDC provider public keys (issuer -> public_key_bytes)
    oidc_public_keys: HashMap<String, Vec<u8>>,
    /// Realm public keys for verification (realm_ulid -> public_key_bytes)
    realm_public_keys: HashMap<Ulid, [u8; 32]>,
    /// Default token expiration in hours
    default_expiration_hours: u64,
}

impl TokenSystem {
    /// Create new token system for a specific realm
    pub fn new(local_realm_ulid: Ulid, oidc_trust_config: OidcTrustConfig) -> Self {
        Self {
            local_realm_ulid,
            oidc_trust_config,
            oidc_public_keys: HashMap::new(),
            realm_public_keys: HashMap::new(),
            default_expiration_hours: 24,
        }
    }

    /// Add OIDC provider public key for token verification
    pub fn add_oidc_public_key(&mut self, issuer: String, public_key: Vec<u8>) {
        self.oidc_public_keys.insert(issuer, public_key);
    }

    /// Add realm public key for verification
    pub fn add_realm_public_key(&mut self, realm_ulid: Ulid, public_key: [u8; 32]) {
        self.realm_public_keys.insert(realm_ulid, public_key);
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
            return Ok(UserIdentity::new(existing_user_ulid, self.local_realm_ulid));
        }

        // No existing mapping found, create new user
        let user_ulid = Ulid::new();

        // Store the OIDC identity mapping
        self.add_oidc_identity(oidc_provider, oidc_sub, user_ulid, store, txn)?;

        // Create UserIdentity for local realm
        let user_identity = UserIdentity::new(user_ulid, self.local_realm_ulid);

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

    /// Verify and decode OIDC token
    fn verify_oidc_token(&self, token: &str) -> Result<OidcToken> {
        // Decode header to get algorithm and key info
        let header = decode_header(token)
            .map_err(|_| PermissionError::ResourceNotFound("Invalid JWT format".to_string()))?;

        // Decode claims without verification first to get issuer
        let token_parts: Vec<&str> = token.split('.').collect();
        if token_parts.len() != 3 {
            return Err(PermissionError::ResourceNotFound(
                "Invalid JWT format".to_string(),
            ));
        }

        // Decode claims part as JSON
        let claims_json = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            token_parts[1],
        )
        .map_err(|_| {
            PermissionError::ResourceNotFound("Invalid JWT claims encoding".to_string())
        })?;

        let claims_value: serde_json::Value =
            serde_json::from_slice(&claims_json).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid JWT claims JSON".to_string())
            })?;

        let issuer = claims_value
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                PermissionError::ResourceNotFound("Missing issuer in token".to_string())
            })?;

        // Check if issuer is trusted
        if !self.oidc_trust_config.is_trusted(issuer) {
            return Err(PermissionError::ResourceNotFound(format!(
                "Untrusted OIDC issuer: {}",
                issuer
            )));
        }

        // Get public key for this issuer
        let public_key = self.oidc_public_keys.get(issuer).ok_or_else(|| {
            PermissionError::ResourceNotFound(format!(
                "No public key available for issuer: {}",
                issuer
            ))
        })?;

        // Create decoding key based on algorithm
        let decoding_key = match header.alg {
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
                DecodingKey::from_rsa_pem(public_key).map_err(|_| {
                    PermissionError::ResourceNotFound("Invalid RSA public key".to_string())
                })?
            }
            Algorithm::ES256 | Algorithm::ES384 => {
                DecodingKey::from_ec_pem(public_key).map_err(|_| {
                    PermissionError::ResourceNotFound("Invalid EC public key".to_string())
                })?
            }
            Algorithm::EdDSA => DecodingKey::from_ed_pem(public_key).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid Ed25519 public key".to_string())
            })?,
            _ => {
                return Err(PermissionError::ResourceNotFound(format!(
                    "Unsupported algorithm: {:?}",
                    header.alg
                )));
            }
        };

        // Set up validation
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
        // Decode claims part to check issuer
        let token_parts: Vec<&str> = token.split('.').collect();
        if token_parts.len() != 3 {
            return Err(PermissionError::ResourceNotFound(
                "Invalid JWT format".to_string(),
            ));
        }

        let claims_json = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            token_parts[1],
        )
        .map_err(|_| {
            PermissionError::ResourceNotFound("Invalid JWT claims encoding".to_string())
        })?;

        let claims_value: serde_json::Value =
            serde_json::from_slice(&claims_json).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid JWT claims JSON".to_string())
            })?;

        let issuer = claims_value
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

    /// Generate realm token for UserIdentity using provided private key
    pub fn generate_token(
        &self,
        user_identity: &UserIdentity,
        private_key: &[u8; 32],
    ) -> Result<String> {
        // Only generate tokens for users in our local realm
        if user_identity.realm_ulid != self.local_realm_ulid {
            return Err(PermissionError::ResourceNotFound(
                "Cannot generate token for foreign realm user".to_string(),
            ));
        }

        let claims = RealmTokenClaims::new(user_identity, self.default_expiration_hours);
        let header = Header::new(Algorithm::EdDSA);

        // Convert raw Ed25519 private key to PKCS#8 DER format
        let pkcs8_der = ed25519_private_key_to_pkcs8_der(private_key);
        let encoding_key = EncodingKey::from_ed_der(&pkcs8_der);

        encode(&header, &claims, &encoding_key)
            .map_err(|_| PermissionError::ResourceNotFound("Failed to encode token".to_string()))
    }

    /// Validate realm JWT token and extract UserIdentity
    fn validate_realm_token(&self, token: &str) -> Result<UserIdentity> {
        // Decode claims to get realm info
        let token_parts: Vec<&str> = token.split('.').collect();
        if token_parts.len() != 3 {
            return Err(PermissionError::ResourceNotFound(
                "Invalid JWT format".to_string(),
            ));
        }

        let claims_json = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            token_parts[1],
        )
        .map_err(|_| {
            PermissionError::ResourceNotFound("Invalid JWT claims encoding".to_string())
        })?;

        let unverified_claims: RealmTokenClaims =
            serde_json::from_slice(&claims_json).map_err(|_| {
                PermissionError::ResourceNotFound("Failed to decode realm token claims".to_string())
            })?;

        // Check expiration
        if unverified_claims.is_expired() {
            return Err(PermissionError::ResourceNotFound(
                "Token expired".to_string(),
            ));
        }

        // Extract realm ULID from issuer
        let realm_ulid = if let Some(realm_str) = unverified_claims.iss.strip_prefix("aruna@") {
            Ulid::from_string(realm_str).map_err(|_| {
                PermissionError::ResourceNotFound("Invalid realm ULID in issuer".to_string())
            })?
        } else {
            return Err(PermissionError::ResourceNotFound(
                "Invalid Aruna token issuer format".to_string(),
            ));
        };

        // Get public key for verification
        let public_key = self
            .realm_public_keys
            .get(&realm_ulid)
            .copied()
            .ok_or_else(|| {
                PermissionError::ResourceNotFound("Realm public key not found".to_string())
            })?;

        // Convert raw Ed25519 public key to SPKI DER format
        let spki_der = ed25519_public_key_to_spki_der(&public_key);
        let decoding_key = DecodingKey::from_ed_der(&spki_der);

        // Verify token signature with proper validation
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&unverified_claims.iss]);

        let verified_token = decode::<RealmTokenClaims>(token, &decoding_key, &validation)
            .map_err(|_| {
                PermissionError::ResourceNotFound("Token verification failed".to_string())
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

    fn create_test_keypair() -> ([u8; 32], [u8; 32]) {
        // Generate a test Ed25519 keypair (in practice you'd use a proper crypto library)
        let mut private_key = [0u8; 32];
        let mut public_key = [0u8; 32];

        // Fill with test data (in reality, use proper key generation)
        for i in 0..32 {
            private_key[i] = i as u8;
            public_key[i] = (i + 32) as u8;
        }

        (private_key, public_key)
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
        let realm_ulid = create_test_ulid(1);
        let user_ulid = create_test_ulid(2);
        let user_identity = UserIdentity::new(user_ulid, realm_ulid);

        let claims = RealmTokenClaims::new(&user_identity, 24);
        let recovered_identity = claims.to_user_identity().unwrap();

        assert_eq!(recovered_identity, user_identity);
        assert_eq!(claims.iss, format!("aruna@{}", realm_ulid));
        assert!(!claims.is_expired());
    }

    #[test]
    fn test_token_system_creation() {
        let realm_ulid = create_test_ulid(1);
        let trust_config =
            OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);

        let mut token_system = TokenSystem::new(realm_ulid, trust_config);

        // Add OIDC public key (dummy PEM for testing)
        let dummy_rsa_public_key = b"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1234567890...
-----END PUBLIC KEY-----"
            .to_vec();

        token_system.add_oidc_public_key(
            "https://accounts.google.com".to_string(),
            dummy_rsa_public_key,
        );

        // Add realm public key
        let (_, public_key) = create_test_keypair();
        token_system.add_realm_public_key(realm_ulid, public_key);

        assert_eq!(token_system.local_realm_ulid, realm_ulid);
    }

    #[test]
    fn test_oidc_mapping_storage() {
        // This test demonstrates the concept but would need a real store implementation
        // In practice, this would be tested in integration tests with actual storage
        let realm_ulid = create_test_ulid(1);
        let trust_config = OidcTrustConfig::TrustAll;
        let token_system = TokenSystem::new(realm_ulid, trust_config);

        // The actual storage testing would happen in integration tests
        // where we have access to a real Store implementation
        assert_eq!(token_system.local_realm_ulid, realm_ulid);
    }

    #[test]
    fn test_oidc_claims_registration() {
        let realm_ulid = create_test_ulid(1);
        let trust_config =
            OidcTrustConfig::TrustedIssuers(vec!["https://accounts.google.com".to_string()]);
        let token_system = TokenSystem::new(realm_ulid, trust_config);

        // This demonstrates the interface - actual testing requires integration tests
        // with real storage implementation

        // The methods are available for integration testing:
        // 1. register_user_from_oidc_claims() - for direct provider/sub registration
        // 2. add_oidc_identity() - for storing mappings
        // 3. get_user_from_oidc() - for looking up existing mappings

        assert_eq!(token_system.local_realm_ulid, realm_ulid);
    }

    #[test]
    fn test_ed25519_key_conversion() {
        let (private_key, public_key) = create_test_keypair();

        // Test key conversion functions
        let pkcs8_der = ed25519_private_key_to_pkcs8_der(&private_key);
        let spki_der = ed25519_public_key_to_spki_der(&public_key);

        // Basic sanity checks
        assert!(pkcs8_der.len() > 32); // Should be longer than raw key
        assert!(spki_der.len() > 32); // Should be longer than raw key

        // Check DER structure headers
        assert_eq!(pkcs8_der[0], 0x30); // SEQUENCE tag
        assert_eq!(spki_der[0], 0x30); // SEQUENCE tag
    }

    #[test]
    fn test_different_realms() {
        let realm_a = create_test_ulid(1);
        let realm_b = create_test_ulid(2);

        let trust_config_a = OidcTrustConfig::TrustAll;
        let trust_config_b = OidcTrustConfig::TrustAll;

        let token_system_a = TokenSystem::new(realm_a, trust_config_a);
        let token_system_b = TokenSystem::new(realm_b, trust_config_b);

        // Different realms should be separate
        assert_eq!(token_system_a.local_realm_ulid, realm_a);
        assert_eq!(token_system_b.local_realm_ulid, realm_b);
    }
}
