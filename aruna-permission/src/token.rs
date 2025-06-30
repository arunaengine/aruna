use ed25519_dalek::{SigningKey, VerifyingKey};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use parking_lot::RwLock;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

use crate::error::{ConversionError, PermissionError, Result};
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
    /// Not before time (prevent pre-generated tokens)
    pub nbf: u64,
    /// Unique token ID for revocation support
    pub jti: String,
}

impl RealmTokenClaims {
    /// Create new claims for a user identity
    pub fn new(user_identity: &UserIdentity, expiration_hours: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let realm_hex = hex::encode(user_identity.realm_key);
        let jti = Ulid::new().to_string();

        Self {
            user_ulid: user_identity.user_ulid.to_string(),
            realm_key: realm_hex.clone(),
            iat: now,
            exp: now + (expiration_hours * 3600),
            nbf: now, // Token is valid immediately
            iss: format!("aruna@{}", realm_hex),
            sub: user_identity.user_ulid.to_string(),
            jti,
        }
    }

    /// Convert claims back to UserIdentity
    pub fn to_user_identity(&self) -> Result<UserIdentity> {
        let user_ulid =
            Ulid::from_string(&self.user_ulid).map_err(|e| ConversionError::InvalidUlid(e))?;

        let realm_bytes =
            hex::decode(&self.realm_key).map_err(|e| ConversionError::InvalidRealmKey(e))?;

        if realm_bytes.len() != 32 {
            return Err(PermissionError::ConversionError(
                ConversionError::InvalidRealmKey(hex::FromHexError::OddLength),
            ));
        }

        let mut realm_key = [0u8; 32];
        realm_key.copy_from_slice(&realm_bytes);

        Ok(UserIdentity::new(user_ulid, realm_key))
    }

    /// Check if token is valid (not expired and not used before nbf)
    pub fn is_valid(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        now >= self.nbf && now <= self.exp
    }

    /// Check if token is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
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
            .expect("Time went backwards")
            .as_secs();
        now > self.exp
    }

    /// Check if token was issued in the future
    pub fn is_future(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.iat > now + 300 // Allow 5 minutes clock skew
    }
}

#[derive(Clone)]
pub struct Issuer {
    pub issuer_name: String,
    /// The issuer URL (e.g., "https://keycloak.example.com/realms/myrealm")
    pub pubkey_url: String,
    pub aud: Vec<String>,
}

impl Issuer {
    pub fn fetch_jwks(&self) -> anyhow::Result<HashMap<String, DecodingKey>> {
        if self.pubkey_url.is_empty() {
            return Err(anyhow::anyhow!("Empty pubkey URL"));
        }

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;

        let res = client.get(&self.pubkey_url).send()?;

        if !res.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to fetch JWKS: HTTP {}",
                res.status()
            ));
        }

        let jwks: JwkSet = res.json()?;

        Ok(jwks
            .keys
            .iter()
            .filter_map(|jwk| {
                let key = DecodingKey::from_jwk(jwk).ok()?;
                let kid = jwk.common.key_id.as_ref()?;
                Some((kid.clone(), key))
            })
            .collect::<HashMap<_, _>>())
    }
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer")
            .field("issuer_name", &self.issuer_name)
            .field("pubkey_url", &self.pubkey_url)
            .field("aud", &self.aud)
            .finish()
    }
}

/// Ed25519 key pair for realm token operations
#[derive(Debug, Clone)]
pub struct Ed25519KeyPair {
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
}

impl Ed25519KeyPair {
    /// Generate a new Ed25519 key pair
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();

        Self {
            signing_key,
            verifying_key,
        }
    }

    /// Create from existing signing key bytes
    pub fn from_bytes(key_bytes: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(key_bytes);
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
            .map_err(|e| PermissionError::OidcError(format!("Failed to encode signing key: {}", e)))
    }

    /// Get the verifying key as PEM format
    pub fn verifying_key_pem(&self) -> Result<String> {
        use pkcs8::EncodePublicKey;
        self.verifying_key
            .to_public_key_pem(pkcs8::LineEnding::LF)
            .map_err(|e| {
                PermissionError::OidcError(format!("Failed to encode verifying key: {}", e))
            })
    }
}

/// Cache entry for JWKS
struct JwksCache {
    keys: HashMap<String, DecodingKey>,
    fetched_at: SystemTime,
}

/// Simple token system for identity management
pub struct TokenSystem {
    /// Local realm key
    local_realm_key: RealmKey,
    /// Static configuration for trusted OIDC issuers
    issuers: Vec<Issuer>,
    /// OIDC provider public keys with caching
    oidc_cache: Arc<RwLock<HashMap<String, JwksCache>>>,
    /// Cache duration (default: 1 hour)
    cache_duration: Duration,
    /// Realm Ed25519 verifying keys for verification (realm_key -> verifying_key_pem as String)
    realm_public_keys: HashMap<RealmKey, String>,
    /// Default token expiration in hours
    default_expiration_hours: u64,
    /// Revoked token JTIs (in production, use Redis or similar)
    revoked_tokens: Arc<RwLock<HashMap<String, u64>>>, // jti -> expiration time
}

impl TokenSystem {
    /// Create new token system for a specific realm
    pub fn new(local_realm_key: RealmKey, issuers: Vec<Issuer>) -> Result<Self> {
        // Validate inputs
        for issuer in &issuers {
            if issuer.issuer_name.is_empty() {
                return Err(PermissionError::OidcError(
                    "Issuer name cannot be empty".to_string(),
                ));
            }
            if issuer.issuer_name.contains('|') {
                return Err(PermissionError::OidcError(
                    "Issuer name cannot contain '|' character".to_string(),
                ));
            }
        }

        let mut system = Self {
            local_realm_key,
            issuers,
            oidc_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_duration: Duration::from_secs(3600), // 1 hour
            realm_public_keys: HashMap::new(),
            default_expiration_hours: 24,
            revoked_tokens: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initial JWKS fetch with better error handling
        system.refresh_jwks()?;

        Ok(system)
    }

    /// Get OIDC decoding key with caching
    fn get_oidc_key(&self, issuer_name: &str, kid: Option<&str>) -> Result<DecodingKey> {
        // Check cache first
        {
            let cache = self.oidc_cache.read();
            if let Some(entry) = cache.get(issuer_name) {
                if entry.fetched_at.elapsed().unwrap_or(Duration::MAX) < self.cache_duration {
                    // Cache is still valid
                    if let Some(kid) = kid {
                        if let Some(key) = entry.keys.get(kid) {
                            return Ok(key.clone());
                        }
                    } else if let Some((_, key)) = entry.keys.iter().next() {
                        return Ok(key.clone());
                    }
                }
            }
        }

        // Cache miss or expired, fetch new keys
        let issuer = self
            .issuers
            .iter()
            .find(|i| i.issuer_name == issuer_name)
            .ok_or_else(|| {
                PermissionError::OidcError(format!("Unknown issuer: {}", issuer_name))
            })?;

        let keys = issuer
            .fetch_jwks()
            .map_err(|e| PermissionError::OidcError(format!("Failed to fetch JWKS: {}", e)))?;

        // Update cache
        {
            let mut cache = self.oidc_cache.write();
            cache.insert(
                issuer_name.to_string(),
                JwksCache {
                    keys: keys.clone(),
                    fetched_at: SystemTime::now(),
                },
            );
        }

        // Return requested key
        if let Some(kid) = kid {
            keys.get(kid).cloned().ok_or_else(|| {
                PermissionError::OidcError(format!("Key ID '{}' not found for issuer", kid))
            })
        } else {
            keys.into_iter().next().map(|(_, key)| key).ok_or_else(|| {
                PermissionError::OidcError("No keys available for issuer".to_string())
            })
        }
    }

    /// Add realm Ed25519 verifying key for verification (expects PEM format)
    pub fn add_realm_public_key(
        &mut self,
        realm_key: RealmKey,
        verifying_key_pem: String,
    ) -> Result<()> {
        // Validate the PEM can be parsed
        DecodingKey::from_ed_pem(verifying_key_pem.as_bytes()).map_err(|e| {
            PermissionError::OidcError(format!("Invalid Ed25519 public key PEM: {}", e))
        })?;

        self.realm_public_keys.insert(realm_key, verifying_key_pem);
        Ok(())
    }

    /// Refresh JWKs for all configured issuers
    pub fn refresh_jwks(&mut self) -> Result<()> {
        let mut fetch_errors = Vec::new();

        for issuer in &self.issuers {
            match issuer.fetch_jwks() {
                Ok(keys) => {
                    let mut cache = self.oidc_cache.write();
                    cache.insert(
                        issuer.issuer_name.clone(),
                        JwksCache {
                            keys,
                            fetched_at: SystemTime::now(),
                        },
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to refresh JWKs for issuer {}: {}",
                        issuer.issuer_name,
                        e
                    );
                    fetch_errors.push((issuer.issuer_name.clone(), e));
                }
            }
        }

        // If all fetches failed, return error
        if fetch_errors.len() == self.issuers.len() && !self.issuers.is_empty() {
            return Err(PermissionError::OidcError(
                "Failed to fetch JWKS from all configured issuers".to_string(),
            ));
        }

        Ok(())
    }

    /// Revoke a token by its JTI
    pub fn revoke_token(&self, jti: &str, exp: u64) {
        let mut revoked = self.revoked_tokens.write();
        revoked.insert(jti.to_string(), exp);

        // Clean up expired revocations
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        revoked.retain(|_, &mut exp_time| exp_time > now);
    }

    /// Check if a token is revoked
    fn is_token_revoked(&self, jti: &str) -> bool {
        let revoked = self.revoked_tokens.read();
        revoked.contains_key(jti)
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
        // Validate inputs
        if oidc_provider.is_empty() || oidc_sub.is_empty() {
            return Err(PermissionError::OidcError(
                "OIDC provider and subject cannot be empty".to_string(),
            ));
        }

        // Check if provider is trusted
        let Some(_issuer) = self.issuers.iter().find(|i| i.issuer_name == oidc_provider) else {
            return Err(PermissionError::OidcError(format!(
                "Unrecognized OIDC issuer: {}",
                oidc_provider
            )));
        };

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

        tracing::info!(
            "Registered new user {} from OIDC provider {}",
            user_ulid,
            oidc_provider
        );

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

    /// Lookup user ULID from OIDC identity
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
    pub fn verify_oidc_token(&self, token: &str) -> Result<OidcToken> {
        // Decode header to get algorithm and kid
        let header = decode_header(token)
            .map_err(|e| PermissionError::OidcError(format!("Invalid JWT header: {}", e)))?;

        // Decode without verification first to get issuer
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(header.alg);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        let unverified = decode::<OidcToken>(token, &dummy_key, &unverified_validation)
            .map_err(|e| PermissionError::OidcError(format!("Invalid OIDC token format: {}", e)))?;

        let issuer_name = &unverified.claims.iss;

        // Check if issuer is trusted
        let Some(issuer) = self.issuers.iter().find(|i| i.issuer_name == *issuer_name) else {
            return Err(PermissionError::OidcError(format!(
                "Unrecognized OIDC issuer: {}",
                issuer_name
            )));
        };

        // Check if token was issued in the future
        if unverified.claims.is_future() {
            return Err(PermissionError::OidcError(
                "Token issued in the future".to_string(),
            ));
        }

        // Get the decoding key
        let decoding_key = self.get_oidc_key(issuer_name, header.kid.as_deref())?;

        // Set up proper validation
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&issuer.issuer_name]);
        if !issuer.aud.is_empty() {
            validation.set_audience(&issuer.aud);
        }
        validation.validate_nbf = true;
        validation.leeway = 60; // 1 minute clock skew tolerance

        // Verify and decode token
        let token_data = decode::<OidcToken>(token, &decoding_key, &validation).map_err(|e| {
            PermissionError::OidcError(format!("OIDC token verification failed: {}", e))
        })?;

        Ok(token_data.claims)
    }

    /// Get identity from token (OIDC JWT or Aruna JWT)
    pub fn get_identity<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &self,
        token: &str,
        store: &'a S,
        txn: &<S as aruna_storage::storage::store::Store<'a>>::Txn,
    ) -> Result<UserIdentity> {
        // Decode without verification to check issuer type
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(Algorithm::EdDSA);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        // Try to decode as generic JSON to get issuer
        let unverified = decode::<serde_json::Value>(token, &dummy_key, &unverified_validation)
            .map_err(|e| PermissionError::OidcError(format!("Invalid JWT format: {}", e)))?;

        let issuer = unverified
            .claims
            .get("iss")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PermissionError::OidcError("Missing issuer in token".to_string()))?;

        // Check if it's an Aruna token (issuer starts with "aruna@")
        if issuer.starts_with("aruna@") {
            self.validate_realm_token(token)
        } else {
            // It's an OIDC token - verify it and lookup the user
            let oidc_token = self.verify_oidc_token(token)?;

            // Check if user exists in our system
            match self.get_user_from_oidc(&oidc_token.iss, &oidc_token.sub, store, txn)? {
                Some(user_ulid) => Ok(UserIdentity::new(user_ulid, self.local_realm_key)),
                None => {
                    // User not found - they need to be registered first
                    Err(PermissionError::OidcError(
                        "User not found. Please register first.".to_string(),
                    ))
                }
            }
        }
    }

    /// Generate realm token for UserIdentity using Ed25519 signing key (PKCS8 PEM format)
    pub fn generate_token(
        &self,
        user_identity: &UserIdentity,
        signing_key_pem: &str,
    ) -> Result<String> {
        // Only generate tokens for users in our local realm
        if user_identity.realm_key != self.local_realm_key {
            return Err(PermissionError::OidcError(
                "Cannot generate token for foreign realm user".to_string(),
            ));
        }

        let claims = RealmTokenClaims::new(user_identity, self.default_expiration_hours);

        // Use Ed25519 for realm tokens only
        let encoding_key = EncodingKey::from_ed_pem(signing_key_pem.as_bytes()).map_err(|e| {
            PermissionError::OidcError(format!("Invalid Ed25519 signing key: {}", e))
        })?;

        let header = Header::new(Algorithm::EdDSA);
        encode(&header, &claims, &encoding_key)
            .map_err(|e| PermissionError::OidcError(format!("Failed to encode token: {}", e)))
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
            .map_err(|e| {
                PermissionError::OidcError(format!("Failed to decode realm token: {}", e))
            })?;

        // Check if token is revoked
        if self.is_token_revoked(&unverified.claims.jti) {
            return Err(PermissionError::OidcError(
                "Token has been revoked".to_string(),
            ));
        }

        // Check token validity (expiration and nbf)
        if !unverified.claims.is_valid() {
            return Err(PermissionError::OidcError(
                "Token is not valid (expired or not yet valid)".to_string(),
            ));
        }

        // Extract realm key from issuer
        let realm_key = if let Some(realm_hex) = unverified.claims.iss.strip_prefix("aruna@") {
            let realm_bytes = hex::decode(realm_hex).map_err(|e| {
                PermissionError::OidcError(format!("Invalid realm key in issuer: {}", e))
            })?;

            if realm_bytes.len() != 32 {
                return Err(PermissionError::OidcError(
                    "Realm key must be exactly 32 bytes".to_string(),
                ));
            }

            let mut realm_key = [0u8; 32];
            realm_key.copy_from_slice(&realm_bytes);
            realm_key
        } else {
            return Err(PermissionError::OidcError(
                "Invalid Aruna token issuer format".to_string(),
            ));
        };

        // Verify that the realm key in claims matches the one in issuer
        let claims_realm_bytes = hex::decode(&unverified.claims.realm_key).map_err(|e| {
            PermissionError::OidcError(format!("Invalid realm key in claims: {}", e))
        })?;

        if claims_realm_bytes != realm_key {
            return Err(PermissionError::OidcError(
                "Realm key mismatch between issuer and claims".to_string(),
            ));
        }

        // Get Ed25519 verifying key for verification
        let verifying_key_pem = self.realm_public_keys.get(&realm_key).ok_or_else(|| {
            PermissionError::OidcError(format!(
                "No public key configured for realm: {}",
                hex::encode(realm_key)
            ))
        })?;

        // Get algorithm from token header - must be EdDSA for realm tokens
        let header = decode_header(token)
            .map_err(|e| PermissionError::OidcError(format!("Invalid JWT header: {}", e)))?;

        if header.alg != Algorithm::EdDSA {
            return Err(PermissionError::OidcError(format!(
                "Invalid algorithm for realm token: {:?}, expected EdDSA",
                header.alg
            )));
        }

        // Create Ed25519 decoding key
        let decoding_key = DecodingKey::from_ed_pem(verifying_key_pem.as_bytes()).map_err(|e| {
            PermissionError::OidcError(format!("Invalid Ed25519 verifying key: {}", e))
        })?;

        // Verify token signature with proper validation
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&unverified.claims.iss]);
        validation.validate_nbf = true;
        validation.leeway = 60; // 1 minute clock skew tolerance

        let verified_token = decode::<RealmTokenClaims>(token, &decoding_key, &validation)
            .map_err(|e| PermissionError::OidcError(format!("Token verification failed: {}", e)))?;

        // Convert to UserIdentity
        verified_token.claims.to_user_identity()
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::ed25519::signature::SignerMut;

    use super::*;

    #[test]
    fn test_realm_token_claims_creation() {
        let user_ulid = Ulid::new();
        let realm_key = [1u8; 32];
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let claims = RealmTokenClaims::new(&user_identity, 24);

        assert_eq!(claims.user_ulid, user_ulid.to_string());
        assert_eq!(claims.realm_key, hex::encode(realm_key));
        assert_eq!(claims.iss, format!("aruna@{}", hex::encode(realm_key)));
        assert_eq!(claims.sub, user_ulid.to_string());
        assert!(claims.exp > claims.iat);
        assert_eq!(claims.nbf, claims.iat);
        assert!(!claims.jti.is_empty());
    }

    #[test]
    fn test_realm_token_claims_validity() {
        let user_ulid = Ulid::new();
        let realm_key = [1u8; 32];
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        // Valid token
        let claims = RealmTokenClaims::new(&user_identity, 24);
        assert!(claims.is_valid());
        assert!(!claims.is_expired());

        // Expired token
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let expired_claims = RealmTokenClaims {
            user_ulid: user_ulid.to_string(),
            realm_key: hex::encode(realm_key),
            iat: now - 3600,
            exp: now - 1800, // Expired 30 minutes ago
            nbf: now - 3600,
            iss: format!("aruna@{}", hex::encode(realm_key)),
            sub: user_ulid.to_string(),
            jti: Ulid::new().to_string(),
        };
        assert!(!expired_claims.is_valid());
        assert!(expired_claims.is_expired());
    }

    #[test]
    fn test_realm_token_claims_to_user_identity() {
        let user_ulid = Ulid::new();
        let realm_key = [1u8; 32];
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let claims = RealmTokenClaims::new(&user_identity, 24);
        let recovered_identity = claims.to_user_identity().unwrap();

        assert_eq!(recovered_identity.user_ulid, user_ulid);
        assert_eq!(recovered_identity.realm_key, realm_key);
    }

    #[test]
    fn test_realm_token_claims_invalid_ulid() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = RealmTokenClaims {
            user_ulid: "invalid-ulid".to_string(),
            realm_key: hex::encode([1u8; 32]),
            iat: now,
            exp: now + 3600,
            nbf: now,
            iss: "aruna@test".to_string(),
            sub: "invalid-ulid".to_string(),
            jti: Ulid::new().to_string(),
        };

        assert!(claims.to_user_identity().is_err());
    }

    #[test]
    fn test_realm_token_claims_invalid_realm_key() {
        let user_ulid = Ulid::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = RealmTokenClaims {
            user_ulid: user_ulid.to_string(),
            realm_key: "invalid-hex".to_string(),
            iat: now,
            exp: now + 3600,
            nbf: now,
            iss: "aruna@test".to_string(),
            sub: user_ulid.to_string(),
            jti: Ulid::new().to_string(),
        };

        assert!(claims.to_user_identity().is_err());
    }

    #[test]
    fn test_oidc_token_user_identifier() {
        let mut token = OidcToken {
            iss: "https://example.com".to_string(),
            sub: "12345".to_string(),
            aud: None,
            exp: 1234567890,
            iat: 1234567800,
            email: Some("user@example.com".to_string()),
            email_verified: Some(true),
            preferred_username: Some("testuser".to_string()),
            name: Some("Test User".to_string()),
            given_name: Some("Test".to_string()),
            family_name: Some("User".to_string()),
            additional_claims: HashMap::new(),
        };

        // Should prefer email
        assert_eq!(token.get_user_identifier(), "user@example.com");

        // Should fall back to preferred_username
        token.email = None;
        assert_eq!(token.get_user_identifier(), "testuser");

        // Should fall back to sub
        token.preferred_username = None;
        assert_eq!(token.get_user_identifier(), "12345");
    }

    #[test]
    fn test_oidc_token_expiration() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let valid_token = OidcToken {
            iss: "https://example.com".to_string(),
            sub: "12345".to_string(),
            aud: None,
            exp: now + 3600, // Valid for 1 hour
            iat: now,
            email: None,
            email_verified: None,
            preferred_username: None,
            name: None,
            given_name: None,
            family_name: None,
            additional_claims: HashMap::new(),
        };

        assert!(!valid_token.is_expired());
        assert!(!valid_token.is_future());

        let expired_token = OidcToken {
            exp: now - 3600, // Expired 1 hour ago
            iat: now - 7200,
            ..valid_token.clone()
        };

        assert!(expired_token.is_expired());

        let future_token = OidcToken {
            exp: now + 3600,
            iat: now + 600, // Issued 10 minutes in the future (beyond 5 min skew)
            ..valid_token
        };

        assert!(future_token.is_future());
    }

    #[test]
    fn test_ed25519_keypair_generation() {
        let mut keypair = Ed25519KeyPair::generate();

        // Verify that the keys are related
        let message = b"test message";
        let signature = keypair.signing_key.try_sign(message).unwrap();
        keypair
            .verifying_key
            .verify_strict(message, &signature)
            .unwrap();
    }

    #[test]
    fn test_ed25519_keypair_from_bytes() {
        let original_key = SigningKey::generate(&mut OsRng);
        let key_bytes = original_key.to_bytes();

        let keypair = Ed25519KeyPair::from_bytes(&key_bytes);

        // Verify keys match
        assert_eq!(keypair.signing_key.to_bytes(), key_bytes);
        assert_eq!(keypair.verifying_key, original_key.verifying_key());
    }

    #[test]
    fn test_ed25519_keypair_pem_formats() {
        let keypair = Ed25519KeyPair::generate();

        let signing_pem = keypair.signing_key_pem().unwrap();
        let verifying_pem = keypair.verifying_key_pem().unwrap();

        assert!(signing_pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(signing_pem.ends_with("-----END PRIVATE KEY-----\n"));
        assert!(verifying_pem.starts_with("-----BEGIN PUBLIC KEY-----"));
        assert!(verifying_pem.ends_with("-----END PUBLIC KEY-----\n"));

        // Verify we can parse them back
        let _decoded_signing = EncodingKey::from_ed_pem(signing_pem.as_bytes()).unwrap();
        let _decoded_verifying = DecodingKey::from_ed_pem(verifying_pem.as_bytes()).unwrap();
    }

    #[test]
    fn test_issuer_debug() {
        let issuer = Issuer {
            issuer_name: "test-issuer".to_string(),
            pubkey_url: "https://example.com/.well-known/jwks.json".to_string(),
            aud: vec!["audience1".to_string(), "audience2".to_string()],
        };

        let debug_output = format!("{:?}", issuer);
        assert!(debug_output.contains("test-issuer"));
        assert!(debug_output.contains("https://example.com/.well-known/jwks.json"));
        assert!(debug_output.contains("audience1"));
    }

    #[tokio::test]
    async fn test_token_system_creation_with_invalid_issuers() {
        let realm_key = [1u8; 32];

        // Empty issuer name
        let invalid_issuers = vec![Issuer {
            issuer_name: "".to_string(),
            pubkey_url: "https://example.com/.well-known/jwks.json".to_string(),
            aud: vec![],
        }];

        let result = TokenSystem::new(realm_key, invalid_issuers);
        assert!(result.is_err());

        // Issuer name with pipe character
        let invalid_issuers = vec![Issuer {
            issuer_name: "test|issuer".to_string(),
            pubkey_url: "https://example.com/.well-known/jwks.json".to_string(),
            aud: vec![],
        }];

        let result = TokenSystem::new(realm_key, invalid_issuers);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_token_revocation() {
        let realm_key = [1u8; 32];
        let token_system = TokenSystem::new(realm_key, vec![]).unwrap();

        let jti = "test-jti";
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        // Token should not be revoked initially
        assert!(!token_system.is_token_revoked(jti));

        // Revoke the token
        token_system.revoke_token(jti, exp);

        // Token should now be revoked
        assert!(token_system.is_token_revoked(jti));
    }

    #[test]
    fn test_token_system_add_realm_public_key() {
        let realm_key = [1u8; 32];
        let keypair = Ed25519KeyPair::generate();
        let public_key_pem = keypair.verifying_key_pem().unwrap();

        let mut token_system = TokenSystem::new(realm_key, vec![]).unwrap();

        // Should succeed with valid PEM
        let result = token_system.add_realm_public_key(realm_key, public_key_pem);
        assert!(result.is_ok());

        // Should fail with invalid PEM
        let result = token_system.add_realm_public_key(realm_key, "invalid-pem".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_token_generation_and_validation() {
        let realm_key = [1u8; 32];
        let keypair = Ed25519KeyPair::generate();
        let signing_pem = keypair.signing_key_pem().unwrap();
        let verifying_pem = keypair.verifying_key_pem().unwrap();

        let user_ulid = Ulid::new();
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let mut token_system = TokenSystem::new(realm_key, vec![]).unwrap();

        token_system
            .add_realm_public_key(realm_key, verifying_pem)
            .unwrap();

        // Generate token
        let token = token_system
            .generate_token(&user_identity, &signing_pem)
            .unwrap();
        assert!(!token.is_empty());

        // Validate token
        let validated_identity = token_system.validate_realm_token(&token).unwrap();

        assert_eq!(validated_identity.user_ulid, user_ulid);
        assert_eq!(validated_identity.realm_key, realm_key);
    }

    #[test]
    fn test_token_generation_foreign_realm() {
        let local_realm_key = [1u8; 32];
        let foreign_realm_key = [2u8; 32];
        let keypair = Ed25519KeyPair::generate();
        let signing_pem = keypair.signing_key_pem().unwrap();

        let user_ulid = Ulid::new();
        let foreign_user_identity = UserIdentity::new(user_ulid, foreign_realm_key);

        let token_system = TokenSystem::new(local_realm_key, vec![]).unwrap();

        // Should fail to generate token for foreign realm user
        let result = token_system.generate_token(&foreign_user_identity, &signing_pem);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_realm_token_with_revoked_jti() {
        let realm_key = [1u8; 32];
        let keypair = Ed25519KeyPair::generate();
        let signing_pem = keypair.signing_key_pem().unwrap();
        let verifying_pem = keypair.verifying_key_pem().unwrap();

        let user_ulid = Ulid::new();
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let mut token_system = TokenSystem::new(realm_key, vec![]).unwrap();
        token_system
            .add_realm_public_key(realm_key, verifying_pem)
            .unwrap();

        // Generate token
        let token = token_system
            .generate_token(&user_identity, &signing_pem)
            .unwrap();

        // Decode token to get JTI
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.insecure_disable_signature_validation();
        validation.validate_exp = false;

        let decoded = decode::<RealmTokenClaims>(&token, &dummy_key, &validation).unwrap();
        let jti = decoded.claims.jti;

        // Revoke the token
        token_system.revoke_token(&jti, decoded.claims.exp);

        // Validation should fail
        let result = token_system.validate_realm_token(&token);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_realm_token_invalid_algorithm() {
        let realm_key = [1u8; 32];
        let user_ulid = Ulid::new();
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let token_system = TokenSystem::new(realm_key, vec![]).unwrap();

        // Create a token with wrong algorithm (HS256 instead of EdDSA)
        let claims = RealmTokenClaims::new(&user_identity, 24);
        let header = Header::new(Algorithm::HS256);
        let encoding_key = EncodingKey::from_secret("secret".as_ref());
        let token = encode(&header, &claims, &encoding_key).unwrap();

        // Should fail validation due to wrong algorithm
        let result = token_system.validate_realm_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_realm_token_claims_realm_key_mismatch() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = RealmTokenClaims {
            user_ulid: Ulid::new().to_string(),
            realm_key: hex::encode([1u8; 32]), // Different from issuer
            iat: now,
            exp: now + 3600,
            nbf: now,
            iss: format!("aruna@{}", hex::encode([2u8; 32])), // Different realm key
            sub: Ulid::new().to_string(),
            jti: Ulid::new().to_string(),
        };

        let realm_key = [2u8; 32];
        let keypair = Ed25519KeyPair::generate();
        let verifying_pem = keypair.verifying_key_pem().unwrap();

        let mut token_system = TokenSystem::new(realm_key, vec![]).unwrap();

        token_system
            .add_realm_public_key(realm_key, verifying_pem)
            .unwrap();

        // Create token with mismatched realm keys
        let header = Header::new(Algorithm::EdDSA);
        let signing_pem = keypair.signing_key_pem().unwrap();
        let encoding_key = EncodingKey::from_ed_pem(signing_pem.as_bytes()).unwrap();
        let token = encode(&header, &claims, &encoding_key).unwrap();

        // Should fail validation due to realm key mismatch
        let result = token_system.validate_realm_token(&token);
        assert!(result.is_err());
    }
}
