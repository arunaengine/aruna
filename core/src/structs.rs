use crate::errors::{ConversionError, ParseRealmIdError};
use crate::types::{GroupId, RoleId, UserId};
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RealmId(pub [u8; 32]);

impl RealmId {
    #[inline]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.0)
    }

    pub fn from_base64(base64_str: &str) -> Result<Self, ParseRealmIdError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(base64_str)
            .map_err(|e| ParseRealmIdError::ParsingError(format!("invalid base64: {}", e)))?;
        if bytes.len() != 32 {
            return Err(ParseRealmIdError::ParsingError(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    pub fn to_pkcs8_pem_bytes(&self) -> Result<[u8; 113], ConversionError> {
        let verifiying_key = VerifyingKey::from_bytes(&self.0)?;
        let pkcs8 = verifiying_key.to_public_key_pem(LineEnding::default())?;
        Ok(pkcs8.as_bytes().try_into()?)
    }
}

impl fmt::Debug for RealmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RealmId({}...)", &self.to_base64()[..8])
    }
}

impl fmt::Display for RealmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base64())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Group {
    pub display_name: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub roles: HashSet<RoleId>,
}

impl Group {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum Permission {
    READ,
    WRITE,
    DENY,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Permission::READ => "Read",
                Permission::WRITE => "Write",
                Permission::DENY => "Deny",
            }
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct AuthorizationDocument {
    pub group_id: GroupId,
    pub roles: HashMap<RoleId, Role>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Role {
    pub role_id: RoleId,
    pub name: String,
    pub permissions: HashMap<String, Permission>,
    pub assigned_users: HashSet<UserId>,
}

impl AuthorizationDocument {
    pub fn new_with_default(user_id: UserId, realm_id: RealmId, group_id: GroupId) -> Self {
        let mut roles = HashMap::new();
        let admin = Ulid::new();
        roles.insert(
            admin,
            Role {
                role_id: admin,
                name: "admin".to_string(),
                permissions: HashMap::from([(
                    format!("/{realm_id}/g/{group_id}/**"),
                    Permission::WRITE,
                )]),
                assigned_users: HashSet::from([(user_id)]),
            },
        );

        let user = Ulid::new();
        roles.insert(
            user,
            Role {
                role_id: user,
                name: "user".to_string(),
                assigned_users: HashSet::new(),
                permissions: HashMap::from([
                    (
                        format!("/{realm_id}/g/{group_id}/meta/**"),
                        Permission::WRITE,
                    ),
                    (
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        Permission::WRITE,
                    ),
                    (
                        format!("/{realm_id}/g/{group_id}/admin/**"),
                        Permission::READ,
                    ),
                ]),
            },
        );

        let viewer = Ulid::new();
        roles.insert(
            viewer,
            Role {
                role_id: viewer,
                name: "viewer".to_string(),
                assigned_users: HashSet::new(),
                permissions: HashMap::from([
                    (
                        format!("/{realm_id}/g/{group_id}/meta/**"),
                        Permission::READ,
                    ),
                    (
                        format!("/{realm_id}/g/{group_id}/data/**"),
                        Permission::READ,
                    ),
                ]),
            },
        );
        AuthorizationDocument { group_id, roles }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject: user identity in format `{user_ulid}@{realm_pubkey_base64}`.
    pub sub: String,
    /// Issuer: realm public key (base64-encoded).
    pub iss: String,
    /// Issued at: Unix timestamp in seconds.
    pub iat: u64,
    /// Expiration: Unix timestamp in seconds.
    pub exp: u64,
    /// JWT ID: unique token identifier (ULID string).
    pub jti: String,
    /// Path restrictions: List of (path_pattern, permission) pairs acting as a whitelist
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Issuer public key (base64-encoded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_pubkey: Option<String>,
    /// Delegation signature: Realm signature over issuer_pubkey
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation_signature: Option<String>,
}

/// Path restriction for token scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathRestriction {
    /// Path pattern (supports * and ** wildcards).
    pub pattern: String,
    /// Permission level for this pattern.
    pub permission: Permission,
}

#[derive(Clone, Debug)]
pub enum NodeCapabilities {
    // This may seem redundant, but because we need both representations both
    // and need both on (nearly) every request, this mitigates unneeded conversions
    Management {
        /// ed25519-dalek representation
        realm_signing_key: SigningKey,
        /// pkcs8_pem bytes representation
        realm_verifying_key: [u8; 113],
        realm_encoding_key: [u8; 168],
    },
    Server {
        /// ed25519-dalek representation
        issuer_signing_key: SigningKey,
        /// pkcs8_pem bytes representation
        issuer_verifying_key: [u8; 113],
        issuer_encoding_key: [u8; 168],

        /// pkcs8_pem bytes representation
        realm_verifying_key: [u8; 113],
        /// Realm signature over issuer_pubkey
        delegation_signature: String,
    },
    Local {
        //realm_pubkey: [u8; 32],
        realm_verifying_key: [u8; 113],
    },
}

impl NodeCapabilities {
    pub fn management_node(realm_signing_key: SigningKey) -> Result<Self, ConversionError> {
        let privkey = realm_signing_key.to_pkcs8_pem(LineEnding::default())?;
        let pubkey = realm_signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::default())?;
        let realm_verifying_key = pubkey.as_bytes().try_into()?;
        let realm_encoding_key = privkey.as_bytes().try_into()?;
        Ok(NodeCapabilities::Management {
            realm_signing_key,
            realm_verifying_key,
            realm_encoding_key,
        })
    }
    pub fn server_node(
        issuer_signing_key: SigningKey,
        realm_id: RealmId,
        delegation_signature: String,
    ) -> Result<Self, ConversionError> {
        let privkey = issuer_signing_key.to_pkcs8_pem(LineEnding::default())?;
        let pubkey = issuer_signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::default())?;
        let issuer_verifying_key = pubkey.as_bytes().try_into()?;
        let issuer_encoding_key = privkey.as_bytes().try_into()?;
        let realm_verifying_key = realm_id.to_pkcs8_pem_bytes()?;
        Ok(NodeCapabilities::Server {
            issuer_signing_key,
            issuer_verifying_key,
            issuer_encoding_key,
            delegation_signature,
            realm_verifying_key,
        })
    }
    pub fn local_node(realm_id: RealmId) -> Result<Self, ConversionError> {
        let realm_verifying_key = realm_id.to_pkcs8_pem_bytes()?;
        Ok(NodeCapabilities::Local {
            realm_verifying_key,
        })
    }
}
