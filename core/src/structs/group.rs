use crate::errors::ConversionError;
use crate::structs::Actor;
use crate::structs::realm::RealmId;
use crate::structs::structs::{Permission, Role};
use crate::types::{GroupId, RoleId, UserId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Group {
    pub display_name: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub roles: HashSet<RoleId>,
}

impl Group {
    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct GroupAuthorizationDocument {
    pub group_id: GroupId,
    pub roles: HashMap<RoleId, Role>,
}

impl GroupAuthorizationDocument {
    pub fn new_default_group_doc(user_id: UserId, realm_id: RealmId, group_id: GroupId) -> Self {
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
        GroupAuthorizationDocument { group_id, roles }
    }

    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::UserId;
    use crate::structs::{Actor, Group, GroupAuthorizationDocument, RealmId};
    use ulid::Ulid;

    #[test]
    pub fn test_group_conversion() {
        let group = Group {
            display_name: "A group".to_string(),
            group_id: Ulid::new(),
            realm_id: RealmId([0u8; 32]),
            roles: HashSet::from([Ulid::new(), Ulid::new()]),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::new(), RealmId([0u8; 32])),
            realm_id: RealmId([0u8; 32]),
        };
        let bytes = group.to_bytes(&actor).unwrap();
        let hydrated_group = Group::from_bytes(&bytes).unwrap();

        assert_eq!(group, hydrated_group);
    }

    #[test]
    pub fn test_group_auth_doc_conversion() {
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            UserId::local(Ulid::new(), RealmId([0u8; 32])),
            RealmId([0u8; 32]),
            Ulid::new(),
        );
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::new(), RealmId([0u8; 32])),
            realm_id: RealmId([0u8; 32]),
        };
        let bytes = auth_doc.to_bytes(&actor).unwrap();
        let hydrated_auth_doc = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();

        assert_eq!(auth_doc, hydrated_auth_doc);
    }
}
