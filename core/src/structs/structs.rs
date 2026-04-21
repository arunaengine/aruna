use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::realm::RealmId;
use crate::types::{RoleId, UserId};
use crate::types::{autosurgeon_ulid, autosurgeon_user_id};
use autosurgeon::{Hydrate, Reconcile, hydrate, reconcile};
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct Role {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub role_id: RoleId,
    pub name: String,
    pub permissions: HashMap<String, Permission>,
    #[autosurgeon(with = "autosurgeon_user_id_set")]
    pub assigned_users: HashSet<UserId>,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
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

pub mod autosurgeon_user_id_set {
    use std::collections::{HashMap, HashSet};

    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    use crate::types::UserId;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashSet<UserId>, HydrateError> {
        let inner: HashMap<String, String> = HashMap::hydrate(doc, obj, prop)?;
        let role_set = inner
            .keys()
            .map(|k| UserId::from_string(k))
            .collect::<Result<HashSet<UserId>, crate::errors::ConversionError>>()
            .map_err(|e| {
                HydrateError::unexpected("valid UserId string", format!("Invalid UserId {}", e))
            })?;
        Ok(role_set)
    }
    pub fn reconcile<R: Reconciler>(
        ulid: &HashSet<UserId>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        map.retain(|id, _| {
            UserId::from_string(id)
                .ok()
                .is_some_and(|id| ulid.contains(&id))
        })?;
        for id in ulid.iter().map(|k| k.to_string()) {
            map.put(&id, "")?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct User {
    #[autosurgeon(with = "autosurgeon_user_id")]
    pub user_id: UserId,
    pub name: String,
    pub subject_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::oidc_subject_key;

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
}

impl User {
    pub fn to_bytes(&self, actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        let actor = postcard::to_allocvec(actor)?;
        let mut doc = automerge::AutoCommit::new().with_actor((&actor).into());
        reconcile(&mut doc, self)?;
        Ok(doc.save())
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let doc = automerge::AutoCommit::load(bytes)?;
        Ok(hydrate(&doc)?)
    }
}

#[cfg(test)]
mod test {
    use crate::UserId;
    use crate::structs::{Permission, RealmId, Role};
    use autosurgeon::{hydrate, reconcile};
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

        let mut automerge_doc = automerge::AutoCommit::new();
        reconcile(&mut automerge_doc, &role).unwrap();

        let bytes = automerge_doc.save();

        let stored_automerge_doc = automerge::AutoCommit::load(&bytes).unwrap();
        let hydrated_role: Role = hydrate(&stored_automerge_doc).unwrap();

        assert_eq!(role, hydrated_role);
    }
}
