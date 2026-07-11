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
    pub owner: UserId,
}

impl Group {
    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Key in GROUP_OWNER_INDEX_KEYSPACE: owner storage key (realm + user ulid)
/// followed by the group id, so a prefix scan counts a user's owned groups.
pub fn group_owner_index_key(owner: UserId, group_id: GroupId) -> Vec<u8> {
    let mut bytes = owner.to_storage_key();
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes
}

pub fn group_owner_index_prefix(owner: UserId) -> Vec<u8> {
    owner.to_storage_key()
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct GroupAuthorizationDocument {
    pub group_id: GroupId,
    pub roles: HashMap<RoleId, Role>,
}

impl GroupAuthorizationDocument {
    pub fn new_default_group_doc(user_id: UserId, realm_id: RealmId, group_id: GroupId) -> Self {
        let mut roles = HashMap::new();
        let admin = Ulid::r#gen();
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

        let user = Ulid::r#gen();
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

        let viewer = Ulid::r#gen();
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
            group_id: Ulid::r#gen(),
            realm_id: RealmId([0u8; 32]),
            roles: HashSet::from([Ulid::r#gen(), Ulid::r#gen()]),
            owner: UserId::local(Ulid::r#gen(), RealmId([0u8; 32])),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::r#gen(), RealmId([0u8; 32])),
            realm_id: RealmId([0u8; 32]),
        };
        let bytes = group.to_bytes(&actor).unwrap();
        let hydrated_group = Group::from_bytes(&bytes).unwrap();

        assert_eq!(group, hydrated_group);
    }

    #[test]
    pub fn test_group_auth_doc_conversion() {
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            UserId::local(Ulid::r#gen(), RealmId([0u8; 32])),
            RealmId([0u8; 32]),
            Ulid::r#gen(),
        );
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: UserId::local(Ulid::r#gen(), RealmId([0u8; 32])),
            realm_id: RealmId([0u8; 32]),
        };
        let bytes = auth_doc.to_bytes(&actor).unwrap();
        let hydrated_auth_doc = GroupAuthorizationDocument::from_bytes(&bytes).unwrap();

        assert_eq!(auth_doc, hydrated_auth_doc);
    }
}
