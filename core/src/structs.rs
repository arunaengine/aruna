use crate::errors::{ConversionError, ParseRealmIdError};
use crate::types::{GroupId, RoleId, UserId};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

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
