use ed25519_dalek::{SigningKey, VerifyingKey};
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

use crate::error::{ConversionError, PermissionError, Result};
use crate::manager::UserIdentity;
use crate::paths::RealmKey;

/// JWT Claims for realm identity tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmTokenClaims {
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
}

impl RealmTokenClaims {
    /// Create new claims for a user identity
    pub fn new(user_identity: &UserIdentity, expiration_hours: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        let realm_hex = hex::encode(user_identity.realm_key);

        Self {
            iat: now,
            exp: now + (expiration_hours * 3600),
            nbf: now, // Token is valid immediately
            iss: format!("aruna@{}", realm_hex),
            sub: user_identity.user_ulid.to_string(),
        }
    }

    /// Get user ULID from sub field
    pub fn user_ulid(&self) -> Result<Ulid> {
        Ulid::from_string(&self.sub)
            .map_err(|e| PermissionError::ConversionError(ConversionError::InvalidUlid(e)))
    }

    /// Get realm key from iss field
    pub fn realm_key(&self) -> Result<RealmKey> {
        let realm_hex = self.iss.strip_prefix("aruna@").ok_or_else(|| {
            PermissionError::ConversionError(ConversionError::InvalidRealmKey(
                hex::FromHexError::InvalidStringLength,
            ))
        })?;

        let realm_bytes = hex::decode(realm_hex)
            .map_err(|e| PermissionError::ConversionError(ConversionError::InvalidRealmKey(e)))?;

        if realm_bytes.len() != 32 {
            return Err(PermissionError::ConversionError(
                ConversionError::InvalidRealmKey(hex::FromHexError::OddLength),
            ));
        }

        let mut realm_key = [0u8; 32];
        realm_key.copy_from_slice(&realm_bytes);
        Ok(realm_key)
    }

    /// Convert claims back to UserIdentity
    pub fn to_user_identity(&self) -> Result<UserIdentity> {
        let user_ulid = self.user_ulid()?;
        let realm_key = self.realm_key()?;
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

#[derive(Clone, Serialize, Deserialize)]
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

/// Helper function to generate a new Ed25519 private key
pub fn generate_realm_private_key() -> [u8; 32] {
    SigningKey::generate(&mut OsRng).to_bytes()
}

/// Helper function to derive public key from private key
pub fn derive_realm_key(private_key_bytes: &[u8; 32]) -> RealmKey {
    let signing_key = SigningKey::from_bytes(private_key_bytes);
    signing_key.verifying_key().to_bytes()
}

/// Simple token system for identity management
pub struct TokenSystem {
    /// Local realm public key (derived from private key)
    local_realm_key: RealmKey,
    /// JWT encoding key for signing tokens
    local_encoding_key: EncodingKey,
    /// Static configuration for trusted OIDC issuers
    issuers: Vec<Issuer>,
    /// OIDC provider public keys (simple storage)
    oidc_keys: HashMap<String, HashMap<String, DecodingKey>>,
    /// Last OIDC fetch attempt per issuer (for grace period)
    last_oidc_fetch: HashMap<String, SystemTime>,
    /// Realm Ed25519 decoding keys for verification (foreign realms)
    realm_public_keys: HashMap<RealmKey, DecodingKey>,
    /// Default token expiration in hours
    default_expiration_hours: u64,
    /// Grace period for OIDC key refetching (10 minutes default)
    oidc_grace_period: Duration,
}

impl TokenSystem {
    /// Create new token system from Ed25519 private key bytes
    pub fn new(private_key_bytes: &[u8; 32], issuers: Vec<Issuer>) -> Result<Self> {
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

        // Create signing key from private key bytes
        let signing_key = SigningKey::from_bytes(private_key_bytes);
        let verifying_key = signing_key.verifying_key();
        let local_realm_key = verifying_key.to_bytes();

        // Create JWT keys using PEM format for local realm
        let signing_pem = {
            use pkcs8::EncodePrivateKey;
            signing_key
                .to_pkcs8_pem(pkcs8::LineEnding::LF)
                .map_err(|e| {
                    PermissionError::OidcError(format!("Failed to encode signing key: {}", e))
                })?
                .to_string()
        };

        let local_encoding_key = EncodingKey::from_ed_pem(signing_pem.as_bytes()).map_err(|e| {
            PermissionError::OidcError(format!("Invalid Ed25519 signing key: {}", e))
        })?;

        let mut system = Self {
            local_realm_key,
            local_encoding_key,
            issuers,
            oidc_keys: HashMap::new(),
            last_oidc_fetch: HashMap::new(),
            realm_public_keys: HashMap::new(),
            default_expiration_hours: 24,
            oidc_grace_period: Duration::from_secs(600), // 10 minutes
        };

        // Add local realm key as a verifying key (for foreign realm verification if needed)
        system.add_realm_key(local_realm_key)?;

        // Initial JWKS fetch
        system.refresh_jwks()?;

        Ok(system)
    }

    /// Create new token system by generating a random private key
    pub fn generate() -> Result<Self> {
        let private_key_bytes = generate_realm_private_key();
        Self::new(&private_key_bytes, vec![])
    }

    /// Create new token system with issuers by generating a random private key
    pub fn generate_with_issuers(issuers: Vec<Issuer>) -> Result<Self> {
        let private_key_bytes = generate_realm_private_key();
        Self::new(&private_key_bytes, issuers)
    }

    /// Get the local realm key
    pub fn local_realm_key(&self) -> RealmKey {
        self.local_realm_key
    }

    /// Add realm key directly (assumes RealmKey is ED25519 public key bytes)
    pub fn add_realm_key(&mut self, realm_key: RealmKey) -> Result<()> {
        let verifying_key = VerifyingKey::from_bytes(&realm_key)
            .map_err(|e| PermissionError::OidcError(format!("Invalid ED25519 key: {}", e)))?;
        let verifying_pem = {
            use pkcs8::EncodePublicKey;
            verifying_key
                .to_public_key_pem(pkcs8::LineEnding::LF)
                .map_err(|e| {
                    PermissionError::OidcError(format!("Failed to encode verifying key: {}", e))
                })?
        };

        let decoding_key = DecodingKey::from_ed_pem(&verifying_pem.as_bytes()).map_err(|e| {
            PermissionError::OidcError(format!("Invalid Ed25519 verifying key: {}", e))
        })?;

        self.realm_public_keys.insert(realm_key, decoding_key);
        Ok(())
    }

    /// Get OIDC decoding key with grace period logic
    fn get_oidc_key(&mut self, issuer_name: &str, kid: Option<&str>) -> Result<DecodingKey> {
        // Check if we have keys and if grace period allows fetching
        let should_fetch = {
            let last_fetch = self.last_oidc_fetch.get(issuer_name);
            let keys_exist = self
                .oidc_keys
                .get(issuer_name)
                .map_or(false, |keys| !keys.is_empty());

            if !keys_exist {
                true // No keys, always fetch
            } else if let Some(last_time) = last_fetch {
                // Check if grace period has passed
                last_time.elapsed().unwrap_or(Duration::MAX) > self.oidc_grace_period
            } else {
                false // Keys exist and no previous fetch recorded
            }
        };

        // Fetch if needed
        if should_fetch {
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

            self.oidc_keys.insert(issuer_name.to_string(), keys);
            self.last_oidc_fetch
                .insert(issuer_name.to_string(), SystemTime::now());
        }

        // Return requested key
        let keys = self.oidc_keys.get(issuer_name).ok_or_else(|| {
            PermissionError::OidcError(format!("No keys available for issuer: {}", issuer_name))
        })?;

        if let Some(kid) = kid {
            keys.get(kid).cloned().ok_or_else(|| {
                PermissionError::OidcError(format!("Key ID '{}' not found for issuer", kid))
            })
        } else {
            keys.iter()
                .next()
                .map(|(_, key)| key.clone())
                .ok_or_else(|| {
                    PermissionError::OidcError("No keys available for issuer".to_string())
                })
        }
    }

    /// Refresh JWKs for all configured issuers
    pub fn refresh_jwks(&mut self) -> Result<()> {
        let mut fetch_errors = Vec::new();

        for issuer in &self.issuers {
            match issuer.fetch_jwks() {
                Ok(keys) => {
                    self.oidc_keys.insert(issuer.issuer_name.clone(), keys);
                    self.last_oidc_fetch
                        .insert(issuer.issuer_name.clone(), SystemTime::now());
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

    /// Register user from OIDC token - creates new UserIdentity after verification or returns existing one
    pub fn register_user<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &mut self,
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
            return Ok(UserIdentity::new(
                existing_user_ulid,
                self.local_realm_key(),
            ));
        }

        // No existing mapping found, create new user
        let user_ulid = Ulid::new();

        // Store the OIDC identity mapping
        self.add_oidc_identity(oidc_provider, oidc_sub, user_ulid, store, txn)?;

        // Create UserIdentity for local realm
        let user_identity = UserIdentity::new(user_ulid, self.local_realm_key());

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
    #[tracing::instrument(level = "trace", skip(self))]
    pub fn verify_oidc_token(&mut self, token: &str) -> Result<OidcToken> {
        // Decode header to get algorithm and kid
        let header = decode_header(token).map_err(|e| {
            tracing::error!(?e);
            PermissionError::OidcError(format!("Invalid JWT header: {}", e))
        })?;

        // Decode without verification first to get issuer
        let dummy_key = DecodingKey::from_secret("dummy".as_ref());
        let mut unverified_validation = Validation::new(header.alg);
        unverified_validation.insecure_disable_signature_validation();
        unverified_validation.validate_exp = false;
        unverified_validation.validate_aud = false;

        let unverified =
            decode::<OidcToken>(token, &dummy_key, &unverified_validation).map_err(|e| {
                tracing::error!(?e);
                PermissionError::OidcError(format!("Invalid OIDC token format: {}", e))
            })?;

        let issuer_name = &unverified.claims.iss;
        // Get the decoding key
        let decoding_key = self
            .get_oidc_key(issuer_name, header.kid.as_deref())
            .map_err(|e| {
                tracing::error!(?e);
                e
            })?;
        // Check if issuer is trusted
        let Some(issuer) = self.issuers.iter().find(|i| i.issuer_name == *issuer_name) else {
            let e =
                PermissionError::OidcError(format!("Unrecognized OIDC issuer: {}", issuer_name));
            tracing::error!(?e);
            return Err(e);
        };

        // Check if token was issued in the future
        if unverified.claims.is_future() {
            let e = PermissionError::OidcError("Token issued in the future".to_string());
            tracing::error!(?e);
            return Err(e);
        }

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
            tracing::error!(?e);
            PermissionError::OidcError(format!("OIDC token verification failed: {}", e))
        })?;

        Ok(token_data.claims)
    }

    /// Get identity from token (OIDC JWT or Aruna JWT)
    pub fn get_identity<'a, S: aruna_storage::storage::store::Store<'a> + 'static>(
        &mut self,
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
                Some(user_ulid) => Ok(UserIdentity::new(user_ulid, self.local_realm_key())),
                None => {
                    // User not found - they need to be registered first
                    Err(PermissionError::OidcError(
                        "User not found. Please register first.".to_string(),
                    ))
                }
            }
        }
    }

    /// Generate realm token for UserIdentity using internal signing key
    pub fn generate_token(&self, user_identity: &UserIdentity) -> Result<String> {
        // Only generate tokens for users in our local realm
        if user_identity.realm_key != self.local_realm_key() {
            return Err(PermissionError::OidcError(
                "Cannot generate token for foreign realm user".to_string(),
            ));
        }

        let claims = RealmTokenClaims::new(user_identity, self.default_expiration_hours);
        let header = Header::new(Algorithm::EdDSA);

        encode(&header, &claims, &self.local_encoding_key)
            .map_err(|e| PermissionError::OidcError(format!("Failed to encode token: {}", e)))
    }

    /// Generate realm token with custom expiration hours
    pub fn generate_token_with_expiration(
        &self,
        user_identity: &UserIdentity,
        expiration_hours: u64,
    ) -> Result<String> {
        // Only generate tokens for users in our local realm
        if user_identity.realm_key != self.local_realm_key() {
            return Err(PermissionError::OidcError(
                "Cannot generate token for foreign realm user".to_string(),
            ));
        }

        let claims = RealmTokenClaims::new(user_identity, expiration_hours);
        let header = Header::new(Algorithm::EdDSA);

        encode(&header, &claims, &self.local_encoding_key)
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

        // Get Ed25519 decoding key for verification
        let decoding_key = self.realm_public_keys.get(&realm_key).ok_or_else(|| {
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

        // Verify token signature with proper validation
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[&unverified.claims.iss]);
        validation.validate_nbf = true;
        validation.leeway = 60; // 1 minute clock skew tolerance

        let verified_token = decode::<RealmTokenClaims>(token, decoding_key, &validation)
            .map_err(|e| PermissionError::OidcError(format!("Token verification failed: {}", e)))?;

        // Convert to UserIdentity
        verified_token.claims.to_user_identity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_realm_token_claims_creation() {
        let user_ulid = Ulid::new();
        let realm_key = [1u8; 32];
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let claims = RealmTokenClaims::new(&user_identity, 24);

        assert_eq!(claims.user_ulid().unwrap(), user_ulid);
        assert_eq!(claims.realm_key().unwrap(), realm_key);
        assert_eq!(claims.iss, format!("aruna@{}", hex::encode(realm_key)));
        assert_eq!(claims.sub, user_ulid.to_string());
        assert!(claims.exp > claims.iat);
        assert_eq!(claims.nbf, claims.iat);
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
            iat: now - 3600,
            exp: now - 1800, // Expired 30 minutes ago
            nbf: now - 3600,
            iss: format!("aruna@{}", hex::encode(realm_key)),
            sub: user_ulid.to_string(),
        };
        assert!(!expired_claims.is_valid());
        assert!(expired_claims.is_expired());
    }

    #[test]
    fn test_generate_realm_private_key() {
        let key1 = generate_realm_private_key();
        let key2 = generate_realm_private_key();

        // Keys should be different
        assert_ne!(key1, key2);

        // Should be valid Ed25519 private keys
        let _signing_key1 = SigningKey::from_bytes(&key1);
        let _signing_key2 = SigningKey::from_bytes(&key2);
    }

    #[test]
    fn test_derive_realm_key() {
        let private_key = generate_realm_private_key();
        let public_key = derive_realm_key(&private_key);

        // Verify the derivation is correct
        let signing_key = SigningKey::from_bytes(&private_key);
        assert_eq!(public_key, signing_key.verifying_key().to_bytes());
    }

    #[test]
    fn test_token_system_creation() {
        let private_key = generate_realm_private_key();
        let token_system = TokenSystem::new(&private_key, vec![]);
        assert!(token_system.is_ok());
    }

    #[test]
    fn test_token_system_generate() {
        let token_system = TokenSystem::generate();
        assert!(token_system.is_ok());
    }

    #[test]
    fn test_token_system_add_realm_key() {
        let private_key = generate_realm_private_key();
        let mut token_system = TokenSystem::new(&private_key, vec![]).unwrap();

        let another_realm_key = generate_realm_private_key();
        let another_realm_key = derive_realm_key(&another_realm_key);
        token_system.add_realm_key(another_realm_key).unwrap();
    }

    #[test]
    fn test_token_generation_and_validation() {
        let private_key = generate_realm_private_key();
        let realm_key = derive_realm_key(&private_key);

        let user_ulid = Ulid::new();
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let token_system = TokenSystem::new(&private_key, vec![]).unwrap();

        let token = token_system.generate_token(&user_identity).unwrap();
        assert!(!token.is_empty());

        // Validate token

        dbg!(&token);
        let validated_identity = token_system.validate_realm_token(&token).unwrap();

        assert_eq!(validated_identity.user_ulid, user_ulid);
        assert_eq!(validated_identity.realm_key, realm_key);
    }

    #[test]
    fn test_token_generation_foreign_realm() {
        let local_private_key = generate_realm_private_key();
        let foreign_realm_key = [2u8; 32];

        let user_ulid = Ulid::new();
        let foreign_user_identity = UserIdentity::new(user_ulid, foreign_realm_key);

        let token_system = TokenSystem::new(&local_private_key, vec![]).unwrap();

        // Should fail to generate token for foreign realm user
        let result = token_system.generate_token(&foreign_user_identity);
        assert!(result.is_err());
    }

    #[test]
    fn test_token_generation_with_custom_expiration() {
        let private_key = generate_realm_private_key();
        let realm_key = derive_realm_key(&private_key);

        let user_ulid = Ulid::new();
        let user_identity = UserIdentity::new(user_ulid, realm_key);

        let token_system = TokenSystem::new(&private_key, vec![]).unwrap();

        // Generate token with custom expiration
        let token = token_system
            .generate_token_with_expiration(&user_identity, 48)
            .unwrap();
        assert!(!token.is_empty());

        // Validate token
        let validated_identity = token_system.validate_realm_token(&token).unwrap();
        assert_eq!(validated_identity.user_ulid, user_ulid);
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
    fn test_token_system_creation_with_invalid_issuers() {
        let private_key = generate_realm_private_key();

        // Empty issuer name
        let invalid_issuers = vec![Issuer {
            issuer_name: "".to_string(),
            pubkey_url: "https://example.com/.well-known/jwks.json".to_string(),
            aud: vec![],
        }];

        let result = TokenSystem::new(&private_key, invalid_issuers);
        assert!(result.is_err());

        // Issuer name with pipe character
        let invalid_issuers = vec![Issuer {
            issuer_name: "test|issuer".to_string(),
            pubkey_url: "https://example.com/.well-known/jwks.json".to_string(),
            aud: vec![],
        }];

        let result = TokenSystem::new(&private_key, invalid_issuers);
        assert!(result.is_err());
    }
}
