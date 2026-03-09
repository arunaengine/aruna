use crate::errors::ConversionError;
use crate::structs::Actor;
use crate::structs::group::autosurgeon_role_map;
use crate::structs::structs::{Permission, Role};
use crate::types::{RoleId, UserId};
use autosurgeon::{Hydrate, Reconcile, hydrate, reconcile};
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    pub fn from_base64(base64_str: &str) -> Result<Self, ConversionError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(base64_str)?;
        if bytes.len() != 32 {
            return Err(ConversionError::InvalidLength(format!(
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

#[derive(Debug, Clone, Serialize, Deserialize, Hydrate, Reconcile, PartialEq)]
pub struct Realm {
    #[autosurgeon(with = "autosurgeon_realm_id")]
    pub realm_id: RealmId,
    pub description: String,
}

impl Realm {
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
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct RealmAuthorizationDocument {
    #[autosurgeon(with = "autosurgeon_realm_id")]
    pub realm_id: RealmId,
    #[autosurgeon(with = "autosurgeon_role_map")]
    pub roles: HashMap<RoleId, Role>,
    #[autosurgeon(with = "autosurgeon_operation_map")]
    pub operation_restrictions: HashMap<RealmLevelOperation, HashSet<Ulid>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub enum RealmLevelOperation {
    CreateGroup,
    ListGroups,
    ManageRealmRoles,
    ManageRealmConfig,
}

impl TryFrom<String> for RealmLevelOperation {
    type Error = ConversionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "CreateGroup" => Ok(RealmLevelOperation::CreateGroup),
            "ListGroups" => Ok(RealmLevelOperation::ListGroups),
            "ManageRealmRoles" => Ok(RealmLevelOperation::ManageRealmRoles),
            "ManageRealmConfig" => Ok(RealmLevelOperation::ManageRealmConfig),
            a => Err(ConversionError::InvalidOperationConversion(a.to_string())),
        }
    }
}

impl std::fmt::Display for RealmLevelOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                RealmLevelOperation::CreateGroup => "CreateGroup",
                RealmLevelOperation::ListGroups => "ListGroups",
                RealmLevelOperation::ManageRealmRoles => "ManageRealmRoles",
                RealmLevelOperation::ManageRealmConfig => "ManageRealmConfig",
            }
        )
    }
}

impl RealmAuthorizationDocument {
    pub fn new_default_realm_doc(user_id: UserId, realm_id: RealmId) -> Self {
        let mut roles = HashMap::new();
        let admin = Ulid::new();
        roles.insert(
            admin,
            Role {
                role_id: admin,
                name: "admin".to_string(),
                permissions: HashMap::from([(format!("/{realm_id}/admin/**"), Permission::WRITE)]),
                assigned_users: HashSet::from([(user_id)]),
            },
        );
        RealmAuthorizationDocument {
            realm_id,
            roles,
            operation_restrictions: HashMap::new(),
        }
    }

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

pub mod autosurgeon_realm_id {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};

    use crate::structs::RealmId;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<RealmId, HydrateError> {
        let inner = autosurgeon::bytes::ByteVec::hydrate(doc, obj, prop)?;
        let realm_id = RealmId(inner.as_slice().try_into().map_err(|_| {
            HydrateError::unexpected("&[u8; 16]", "Invalid slice of bytes".to_string())
        })?);
        Ok(realm_id)
    }
    pub fn reconcile<R: Reconciler>(bytes: &RealmId, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(bytes.0)
    }
}

pub mod autosurgeon_operation_map {
    use std::collections::{HashMap, HashSet};

    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use ulid::Ulid;

    use crate::errors::ConversionError;
    use crate::structs::RealmLevelOperation;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashMap<RealmLevelOperation, HashSet<Ulid>>, HydrateError> {
        let inner: HashMap<String, HashMap<String, String>> = HashMap::hydrate(doc, obj, prop)?;
        let role_set = inner
            .into_iter()
            .map(
                |(operation, users)| -> Result<(RealmLevelOperation, HashSet<Ulid>), ConversionError> {
                    let operation: RealmLevelOperation = operation.try_into()?;
                    let user_map: Result<HashSet<Ulid>, ConversionError> = users.keys().map(|u| {
                            Ulid::from_string(u).map_err(|_e| ConversionError::InvalidUserId)
                        })
                        .collect();

                    Ok((operation, user_map?))
                },
            )
            .collect::<Result<HashMap<RealmLevelOperation, HashSet<Ulid>>, ConversionError>>()
            .map_err(|e| {
                HydrateError::unexpected("valid Ulid string", format!("Invalid Ulid {}", e))
            })?;
        Ok(role_set)
    }
    pub fn reconcile<R: Reconciler>(
        operation_map: &HashMap<RealmLevelOperation, HashSet<Ulid>>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        for (operation, users) in operation_map.iter() {
            map.put(
                operation.to_string(),
                users
                    .iter()
                    .map(|u| (u.to_string(), String::new()))
                    .collect::<HashMap<String, String>>(),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::structs::{RealmAuthorizationDocument, RealmId};
    use autosurgeon::{hydrate, reconcile};
    use ulid::Ulid;

    #[test]
    pub fn test_realm_auth_doc_conversion() {
        let auth_doc =
            RealmAuthorizationDocument::new_default_realm_doc(Ulid::new(), RealmId([0u8; 32]));
        let mut automerge_doc = automerge::AutoCommit::new();
        reconcile(&mut automerge_doc, &auth_doc).unwrap();

        let bytes = automerge_doc.save();

        let stored_automerge_doc = automerge::AutoCommit::load(&bytes).unwrap();
        let hydrated_auth_doc: RealmAuthorizationDocument = hydrate(&stored_automerge_doc).unwrap();

        assert_eq!(auth_doc, hydrated_auth_doc);
    }
}
