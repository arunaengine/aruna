use crate::errors::ConversionError;
use crate::structs::realm::{RealmId, autosurgeon_realm_id};
use crate::structs::structs::{Permission, Role};
use crate::types::autosurgeon_ulid;
use crate::types::{GroupId, RoleId, UserId};
use autosurgeon::{Hydrate, Reconcile};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct Group {
    pub display_name: String,
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub group_id: GroupId,
    #[autosurgeon(with = "autosurgeon_realm_id")]
    pub realm_id: RealmId,
    #[autosurgeon(with = "autosurgeon_role_set")]
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct GroupAuthorizationDocument {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub group_id: GroupId,
    #[autosurgeon(with = "autosurgeon_role_map")]
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

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

pub mod autosurgeon_role_map {
    use crate::errors::ConversionError;
    use crate::structs::Role;
    use crate::types::RoleId;
    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use std::collections::HashMap;
    use ulid::Ulid;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashMap<RoleId, Role>, HydrateError> {
        let inner: HashMap<String, Role> = HashMap::hydrate(doc, obj, prop)?;
        let role_set = inner
            .into_iter()
            .map(|(id, role)| -> Result<(RoleId, Role), ConversionError> {
                let id = Ulid::from_string(&id).map_err(|_e| ConversionError::InvalidUserId)?;
                Ok((id, role))
            })
            .collect::<Result<HashMap<RoleId, Role>, ConversionError>>()
            .map_err(|e| {
                HydrateError::unexpected("valid Ulid string", format!("Invalid Ulid {}", e))
            })?;
        Ok(role_set)
    }
    pub fn reconcile<R: Reconciler>(
        role_map: &HashMap<RoleId, Role>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        for (role_id, role) in role_map.iter() {
            map.put(role_id.to_string(), role)?;
        }
        Ok(())
    }
}

pub mod autosurgeon_role_set {
    use std::collections::{HashMap, HashSet};

    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use ulid::Ulid;

    use crate::errors::ConversionError;
    use crate::types::RoleId;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashSet<RoleId>, HydrateError> {
        let inner: HashMap<String, String> = HashMap::hydrate(doc, obj, prop)?;
        let role_set = inner
            .into_iter()
            .map(|(id, _)| -> Result<RoleId, ConversionError> {
                let id = Ulid::from_string(&id).map_err(|_e| ConversionError::InvalidUserId)?;
                Ok(id)
            })
            .collect::<Result<HashSet<RoleId>, ConversionError>>()
            .map_err(|e| {
                HydrateError::unexpected("valid Ulid string", format!("Invalid Ulid {}", e))
            })?;
        Ok(role_set)
    }
    pub fn reconcile<R: Reconciler>(
        role_set: &HashSet<RoleId>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        for role_id in role_set.iter() {
            map.put(role_id.to_string(), String::new())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::structs::{GroupAuthorizationDocument, RealmId};
    use autosurgeon::{hydrate, reconcile};
    use ulid::Ulid;

    #[test]
    pub fn test_group_auth_doc_conversion() {
        let auth_doc = GroupAuthorizationDocument::new_default_group_doc(
            Ulid::new(),
            RealmId([0u8; 32]),
            Ulid::new(),
        );
        let mut automerge_doc = automerge::AutoCommit::new();
        reconcile(&mut automerge_doc, &auth_doc).unwrap();

        let bytes = automerge_doc.save();

        let stored_automerge_doc = automerge::AutoCommit::load(&bytes).unwrap();
        let hydrated_auth_doc: GroupAuthorizationDocument = hydrate(&stored_automerge_doc).unwrap();

        assert_eq!(auth_doc, hydrated_auth_doc);
    }
}
