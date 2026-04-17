use crate::errors::ConversionError;
use crate::structs::group::autosurgeon_role_map;
use crate::structs::structs::{Permission, Role};
use crate::structs::{Actor, User};
use crate::types::{GroupId, RoleId, autosurgeon_ulid};
use autosurgeon::{Hydrate, Reconcile, hydrate, reconcile};
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use ulid::Ulid;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    pub fn new_default_realm_doc(realm_id: RealmId) -> Self {
        let mut roles = HashMap::new();
        let admin = Ulid::new();
        roles.insert(
            admin,
            Role {
                role_id: admin,
                name: "realm_admin".to_string(),
                permissions: HashMap::from([(format!("/{realm_id}/admin/**"), Permission::WRITE)]),
                assigned_users: HashSet::new(),
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

pub const DEFAULT_METADATA_REPLICATION_FACTOR: u32 = 3;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct RealmConfigDocument {
    #[autosurgeon(with = "autosurgeon_realm_id")]
    pub realm_id: RealmId,
    pub metadata_replication: MetadataReplicationConfig,
    pub oidc_providers: Vec<OidcProviderConfig>,
    pub users: Vec<User>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct OidcProviderConfig {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

impl RealmConfigDocument {
    pub fn new(realm_id: RealmId, default_replication_factor: u32) -> Self {
        Self {
            realm_id,
            metadata_replication: MetadataReplicationConfig::new(default_replication_factor),
            oidc_providers: Vec::new(),
            users: Vec::new(),
        }
    }

    pub fn default_for_realm(realm_id: RealmId) -> Self {
        Self::new(realm_id, DEFAULT_METADATA_REPLICATION_FACTOR)
    }

    pub fn metadata_replication_factor_for(&self, group_id: GroupId, path: Option<&str>) -> usize {
        self.metadata_replication.factor_for(group_id, path)
    }

    pub fn to_bytes(&self, actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        self.reconcile_bytes(None, actor)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let doc = automerge::AutoCommit::load(bytes)?;
        Ok(hydrate(&doc)?)
    }

    pub fn reconcile_bytes(
        &self,
        current: Option<&[u8]>,
        actor: &Actor,
    ) -> Result<Vec<u8>, ConversionError> {
        let actor = postcard::to_allocvec(actor)?;
        let mut doc = match current {
            Some(bytes) if !bytes.is_empty() => automerge::AutoCommit::load(bytes)?,
            _ => automerge::AutoCommit::new(),
        };
        doc.set_actor((&actor).into());
        reconcile(&mut doc, self)?;
        Ok(doc.save())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct MetadataReplicationConfig {
    pub default_replication_factor: u32,
    pub group_overrides: Vec<MetadataGroupReplicationOverride>,
    pub path_overrides: Vec<MetadataPathReplicationOverride>,
}

impl MetadataReplicationConfig {
    pub fn new(default_replication_factor: u32) -> Self {
        Self {
            default_replication_factor,
            group_overrides: Vec::new(),
            path_overrides: Vec::new(),
        }
    }

    pub fn factor_for(&self, group_id: GroupId, path: Option<&str>) -> usize {
        if let Some(path) = path
            && let Some(path_override) = self
                .path_overrides
                .iter()
                .filter(|override_| {
                    override_.group_id == group_id && path.starts_with(&override_.path_prefix)
                })
                .max_by_key(|override_| override_.path_prefix.len())
        {
            return normalize_replication_factor(path_override.replication_factor);
        }

        if let Some(group_override) = self
            .group_overrides
            .iter()
            .find(|override_| override_.group_id == group_id)
        {
            return normalize_replication_factor(group_override.replication_factor);
        }

        normalize_replication_factor(self.default_replication_factor)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct MetadataGroupReplicationOverride {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub group_id: GroupId,
    pub replication_factor: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct MetadataPathReplicationOverride {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub group_id: GroupId,
    pub path_prefix: String,
    pub replication_factor: u32,
}

fn normalize_replication_factor(replication_factor: u32) -> usize {
    replication_factor.max(1) as usize
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
            HydrateError::unexpected("&[u8; 32]", "Invalid slice of bytes".to_string())
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
        map.retain(|operation, _| {
            RealmLevelOperation::try_from(operation.to_string())
                .ok()
                .is_some_and(|operation| operation_map.contains_key(&operation))
        })?;
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
    use crate::structs::{
        Actor, MetadataGroupReplicationOverride, MetadataPathReplicationOverride,
        OidcProviderConfig, RealmAuthorizationDocument, RealmConfigDocument, RealmId, User,
    };
    use crate::UserId;
    use autosurgeon::{hydrate, reconcile};
    use ulid::Ulid;

    #[test]
    pub fn test_realm_auth_doc_conversion() {
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(RealmId([0u8; 32]));
        let mut automerge_doc = automerge::AutoCommit::new();
        reconcile(&mut automerge_doc, &auth_doc).unwrap();

        let bytes = automerge_doc.save();

        let stored_automerge_doc = automerge::AutoCommit::load(&bytes).unwrap();
        let hydrated_auth_doc: RealmAuthorizationDocument = hydrate(&stored_automerge_doc).unwrap();

        assert_eq!(auth_doc, hydrated_auth_doc);
        assert!(
            hydrated_auth_doc.roles.iter().any(|(_id, role)| {
                role.name == "realm_admin" && role.assigned_users.is_empty()
            })
        );
    }

    #[test]
    pub fn test_realm_config_doc_roundtrip() {
        let group_id = Ulid::new();
        let document = RealmConfigDocument {
            realm_id: RealmId([4u8; 32]),
            metadata_replication: super::MetadataReplicationConfig {
                default_replication_factor: 3,
                group_overrides: vec![MetadataGroupReplicationOverride {
                    group_id,
                    replication_factor: 5,
                }],
                path_overrides: vec![MetadataPathReplicationOverride {
                    group_id,
                    path_prefix: "/datasets/demo".to_string(),
                    replication_factor: 7,
                }],
            },
            oidc_providers: vec![OidcProviderConfig {
                id: "main".to_string(),
                issuer: "https://issuer.example".to_string(),
                audience: "aruna-api".to_string(),
                discovery_url: "https://issuer.example/.well-known/openid-configuration"
                    .to_string(),
            }],
            users: vec![User {
                user_id: UserId::new(Ulid::new(), RealmId([4u8; 32])),
                name: "a_name".to_string(),
                subject_ids: vec!["id1".to_string(), "id2".to_string()],
            }],
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[14u8; 32]).public(),
            user_id: UserId::new(Ulid::new(), RealmId([4u8; 32])),
            realm_id: RealmId([4u8; 32]),
        };

        let bytes = document.to_bytes(&actor).expect("to bytes");
        let restored = RealmConfigDocument::from_bytes(&bytes).expect("from bytes");

        assert_eq!(document, restored);
    }

    #[test]
    pub fn test_realm_config_replication_resolution() {
        let group_id = Ulid::new();
        let other_group_id = Ulid::new();
        let document = RealmConfigDocument {
            realm_id: RealmId([5u8; 32]),
            metadata_replication: super::MetadataReplicationConfig {
                default_replication_factor: 3,
                group_overrides: vec![MetadataGroupReplicationOverride {
                    group_id,
                    replication_factor: 5,
                }],
                path_overrides: vec![
                    MetadataPathReplicationOverride {
                        group_id,
                        path_prefix: "/datasets".to_string(),
                        replication_factor: 6,
                    },
                    MetadataPathReplicationOverride {
                        group_id,
                        path_prefix: "/datasets/important".to_string(),
                        replication_factor: 7,
                    },
                ],
            },
            oidc_providers: vec![],
            users: vec![User {
                user_id: UserId::new(Ulid::new(), RealmId([5u8; 32])),
                name: "b_name".to_string(),
                subject_ids: vec!["id3".to_string(), "id4".to_string()],
            }],
        };

        assert_eq!(
            document.metadata_replication_factor_for(other_group_id, None),
            3
        );
        assert_eq!(document.metadata_replication_factor_for(group_id, None), 5);
        assert_eq!(
            document.metadata_replication_factor_for(group_id, Some("/datasets/demo")),
            6
        );
        assert_eq!(
            document.metadata_replication_factor_for(group_id, Some("/datasets/important/item")),
            7
        );
    }
}
