use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::realm::RealmId;
use crate::types::{RoleId, UserId};
use core::fmt;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Serialize)]
struct OidcSubjectKey<'a> {
    kind: &'static str,
    issuer: &'a str,
    sub: &'a str,
}

pub fn oidc_subject_key(issuer: &str, subject_id: &str) -> Result<String, ConversionError> {
    Ok(serde_json::to_string(&OidcSubjectKey {
        kind: "oidc",
        issuer,
        sub: subject_id,
    })?)
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
pub struct Role {
    pub role_id: RoleId,
    pub name: String,
    pub permissions: HashMap<String, Permission>,
    pub assigned_users: HashSet<UserId>,
}

impl Role {
    /// A role assigned to the Everyone principal (the nil user id) applies to
    /// every request in the realm — including anonymous, unauthenticated ones
    /// (see `AuthContext::anonymous`). Encoding publicness as a member instead
    /// of a struct field keeps stored auth documents (postcard, positional)
    /// readable without a migration.
    pub fn is_public(&self) -> bool {
        self.assigned_users
            .iter()
            .any(|user| user.user_ulid.is_nil())
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

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthContext {
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub path_restrictions: Option<Vec<PathRestriction>>,
}

impl AuthContext {
    /// The Everyone principal: unauthenticated requests are permission-checked
    /// as the nil user, so exactly the roles that assign `UserId::nil` (public
    /// roles, `Role::is_public`) grant them access.
    pub fn anonymous(realm_id: RealmId) -> Self {
        Self {
            user_id: UserId::nil(realm_id),
            realm_id,
            path_restrictions: None,
        }
    }
}

impl TryFrom<TokenClaims> for AuthContext {
    type Error = ConversionError;

    fn try_from(value: TokenClaims) -> Result<Self, Self::Error> {
        let user_id = UserId::from_string(&value.sub)?;
        let realm_id = RealmId::from_base64(&value.iss)?;
        if user_id.realm_id != realm_id {
            return Err(ConversionError::InvalidUserId);
        }
        let path_restrictions = value.restrictions;

        Ok(Self {
            user_id,
            realm_id,
            path_restrictions,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Actor {
    pub node_id: NodeId,
    pub user_id: UserId,
    pub realm_id: RealmId,
}

impl TryFrom<&[u8]> for Actor {
    type Error = ConversionError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(postcard::from_bytes(value)?)
    }
}

impl TryFrom<&Actor> for Vec<u8> {
    type Error = ConversionError;

    fn try_from(value: &Actor) -> Result<Self, Self::Error> {
        Ok(postcard::to_allocvec(value)?)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct User {
    pub user_id: UserId,
    pub name: String,
    pub subject_ids: Vec<String>,
    #[serde(default)]
    pub alias_user_ids: HashSet<UserId>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::{AuthContext, PathRestriction, Permission, TokenClaims, oidc_subject_key};
    use crate::UserId;
    use crate::structs::RealmId;
    use ulid::Ulid;

    #[test]
    fn oidc_subject_key_uses_structured_encoding() {
        assert_eq!(
            oidc_subject_key("https://issuer.example", "subject-1").unwrap(),
            r#"{"kind":"oidc","issuer":"https://issuer.example","sub":"subject-1"}"#
        );
    }

    #[test]
    fn oidc_subject_key_avoids_delimiter_collisions() {
        let first = oidc_subject_key("a:b", "c");
        let second = oidc_subject_key("a", "b:c");

        assert_ne!(first, second);
    }

    #[test]
    fn auth_context_conversion_preserves_path_restrictions() {
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let user_id = UserId::new(Ulid::from_bytes([9u8; 16]), realm_id);
        let restrictions = vec![PathRestriction {
            pattern: "/realm/g/group/data/**".to_string(),
            permission: Permission::READ,
        }];

        let auth = AuthContext::try_from(TokenClaims {
            sub: user_id.to_string(),
            iss: realm_id.to_base64(),
            iat: 1,
            exp: 2,
            jti: "token-id".to_string(),
            restrictions: Some(restrictions.clone()),
            issuer_pubkey: None,
            delegation_signature: None,
        })
        .unwrap();

        assert_eq!(auth.user_id, user_id);
        assert_eq!(auth.realm_id, realm_id);
        assert_eq!(auth.path_restrictions, Some(restrictions));
    }
}

impl User {
    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn reconcile_bytes(
        &self,
        current: Option<&[u8]>,
        actor: &Actor,
    ) -> Result<Vec<u8>, ConversionError> {
        let _ = (current, actor);
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod test {
    use crate::UserId;
    use crate::structs::{Actor, Permission, RealmId, Role, User};
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    #[test]
    pub fn test_role_conversion() {
        let role = Role {
            role_id: Ulid::new(),
            name: "admin".to_string(),
            permissions: HashMap::from([(
                format!("/{}/g/{}/**", RealmId([0u8; 32]), Ulid::new().to_string()),
                Permission::WRITE,
            )]),
            assigned_users: HashSet::from([UserId::new(Ulid::new(), RealmId([1u8; 32]))]),
        };

        let bytes = postcard::to_allocvec(&role).unwrap();
        let hydrated_role: Role = postcard::from_bytes(&bytes).unwrap();

        assert_eq!(role, hydrated_role);
    }

    #[test]
    pub fn test_user_attributes_roundtrip() {
        let realm_id = RealmId([2u8; 32]);
        let user_id = UserId::new(Ulid::new(), realm_id);
        let user = User {
            user_id,
            name: "alice".to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: HashMap::from([("orcid".to_string(), "0000-0002-1825-0097".to_string())]),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            user_id,
            realm_id,
        };

        let bytes = user.to_bytes(&actor).unwrap();
        let hydrated_user = User::from_bytes(&bytes).unwrap();

        assert_eq!(user, hydrated_user);
    }
}
