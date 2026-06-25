use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::structs::{
    Actor, MetadataReplicationConfig, OidcProviderConfig, Permission, RealmDiscoveryConfig,
    RealmId, RealmNodeKind, Role,
};
use crate::types::{GroupId, RoleId, UserId};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentClock {
    pub origins: BTreeMap<NodeId, u64>,
}

impl AdminDocumentClock {
    pub fn sequence_for(&self, origin_node_id: &NodeId) -> u64 {
        self.origins
            .get(origin_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn observes(&self, dot: &AdminDocumentDot) -> bool {
        self.sequence_for(&dot.origin_node_id) >= dot.origin_seq
    }

    pub fn advance(&mut self, origin_node_id: NodeId, origin_seq: u64) {
        let current = self.origins.entry(origin_node_id).or_default();
        *current = (*current).max(origin_seq);
    }

    pub fn with_observed(mut self, origin_node_id: NodeId, origin_seq: u64) -> Self {
        self.advance(origin_node_id, origin_seq);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AdminDocumentDot {
    pub event_id: Ulid,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentTarget {
    Group { group_id: GroupId },
    Realm { realm_id: RealmId },
    User { user_id: UserId },
    RealmConfig { realm_id: RealmId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentRoleDefinition {
    pub role_id: RoleId,
    pub name: String,
    pub permissions: BTreeMap<String, Permission>,
}

impl From<&Role> for AdminDocumentRoleDefinition {
    fn from(role: &Role) -> Self {
        Self {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
        }
    }
}

impl From<Role> for AdminDocumentRoleDefinition {
    fn from(role: Role) -> Self {
        Self::from(&role)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminDocumentOperation {
    GroupRoleAdded {
        role_id: RoleId,
    },
    GroupRoleUserAssignmentAdded {
        role_id: RoleId,
        user_id: UserId,
    },
    GroupRoleUserAssignmentRemoved {
        role_id: RoleId,
        user_id: UserId,
    },
    UserAttributeSet {
        key: String,
        value: String,
    },
    UserAttributeRemoved {
        key: String,
    },
    UserNameSet {
        name: String,
    },
    UserSubjectIdAdded {
        subject_id: String,
    },
    UserSubjectIdRemoved {
        subject_id: String,
    },
    RealmRoleAdded {
        role_id: RoleId,
    },
    RealmRoleUserAssignmentAdded {
        role_id: RoleId,
        user_id: UserId,
    },
    RealmRoleUserAssignmentRemoved {
        role_id: RoleId,
        user_id: UserId,
    },
    GroupRoleCreated {
        role: AdminDocumentRoleDefinition,
    },
    RealmRoleCreated {
        role: AdminDocumentRoleDefinition,
    },
    RealmConfigNodeEnsured {
        node_id: NodeId,
        kind: RealmNodeKind,
    },
    RealmConfigOidcProviderUpserted {
        provider: OidcProviderConfig,
    },
    RealmConfigOidcProviderRemoved {
        provider_id: String,
    },
    RealmConfigSettingsSet {
        metadata_replication: MetadataReplicationConfig,
        discovery: RealmDiscoveryConfig,
    },
}

#[cfg(test)]
mod tests {
    use super::{AdminDocumentOperation, AdminDocumentRoleDefinition, AdminDocumentTarget};
    use crate::NodeId;
    use crate::structs::{
        MetadataReplicationConfig, OidcProviderConfig, Permission, RealmDiscoveryConfig, RealmId,
        RealmNodeKind,
    };
    use crate::types::{GroupId, RoleId, UserId};
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn role_id(seed: u8) -> RoleId {
        Ulid::from_bytes([seed; 16])
    }

    fn group_id(seed: u8) -> GroupId {
        Ulid::from_bytes([seed; 16])
    }

    fn user_id(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([9; 32]))
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn role_definition(role_id: RoleId) -> AdminDocumentRoleDefinition {
        AdminDocumentRoleDefinition {
            role_id,
            name: "admin".to_string(),
            permissions: BTreeMap::from([("/dataset/**".to_string(), Permission::READ)]),
        }
    }

    fn oidc_provider(id: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: format!("https://issuer.example/{id}"),
            audience: "aruna".to_string(),
            discovery_url: format!("https://issuer.example/{id}/.well-known/openid-configuration"),
        }
    }

    fn postcard_discriminant(op: &AdminDocumentOperation) -> u8 {
        postcard::to_allocvec(op).expect("operation serializes")[0]
    }

    fn target_postcard_discriminant(target: &AdminDocumentTarget) -> u8 {
        postcard::to_allocvec(target).expect("target serializes")[0]
    }

    fn postcard_roundtrip<T>(value: T) -> T
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let bytes = postcard::to_allocvec(&value).expect("value serializes");
        postcard::from_bytes(&bytes).expect("value deserializes")
    }

    #[test]
    fn admin_document_operation_postcard_discriminants_preserve_legacy_order() {
        let role_id = role_id(1);
        let user_id = user_id(2);
        let operations = [
            (AdminDocumentOperation::GroupRoleAdded { role_id }, 0),
            (
                AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
                1,
            ),
            (
                AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
                2,
            ),
            (
                AdminDocumentOperation::UserAttributeSet {
                    key: "department".to_string(),
                    value: "biology".to_string(),
                },
                3,
            ),
            (
                AdminDocumentOperation::UserAttributeRemoved {
                    key: "department".to_string(),
                },
                4,
            ),
            (
                AdminDocumentOperation::UserNameSet {
                    name: "Alice".to_string(),
                },
                5,
            ),
            (
                AdminDocumentOperation::UserSubjectIdAdded {
                    subject_id: "subject-1".to_string(),
                },
                6,
            ),
            (
                AdminDocumentOperation::UserSubjectIdRemoved {
                    subject_id: "subject-1".to_string(),
                },
                7,
            ),
            (AdminDocumentOperation::RealmRoleAdded { role_id }, 8),
            (
                AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id },
                9,
            ),
            (
                AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id },
                10,
            ),
            (
                AdminDocumentOperation::GroupRoleCreated {
                    role: role_definition(role_id),
                },
                11,
            ),
            (
                AdminDocumentOperation::RealmRoleCreated {
                    role: role_definition(role_id),
                },
                12,
            ),
            (
                AdminDocumentOperation::RealmConfigNodeEnsured {
                    node_id: node(1),
                    kind: RealmNodeKind::Management,
                },
                13,
            ),
            (
                AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                    provider: oidc_provider("default"),
                },
                14,
            ),
            (
                AdminDocumentOperation::RealmConfigOidcProviderRemoved {
                    provider_id: "default".to_string(),
                },
                15,
            ),
            (
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication: MetadataReplicationConfig::new(3),
                    discovery: RealmDiscoveryConfig::Static {
                        endpoints: Vec::new(),
                    },
                },
                16,
            ),
        ];

        for (op, discriminant) in operations {
            assert_eq!(postcard_discriminant(&op), discriminant);
        }
    }

    #[test]
    fn admin_document_target_postcard_discriminants_preserve_legacy_order() {
        let realm_id = RealmId::from_bytes([9; 32]);
        let targets = [
            (
                AdminDocumentTarget::Group {
                    group_id: group_id(1),
                },
                0,
            ),
            (AdminDocumentTarget::Realm { realm_id }, 1),
            (
                AdminDocumentTarget::User {
                    user_id: user_id(2),
                },
                2,
            ),
            (AdminDocumentTarget::RealmConfig { realm_id }, 3),
        ];

        for (target, discriminant) in targets {
            assert_eq!(target_postcard_discriminant(&target), discriminant);
            assert_eq!(postcard_roundtrip(target.clone()), target);
        }
    }

    #[test]
    fn realm_config_node_ensured_operation_roundtrips() {
        let operation = AdminDocumentOperation::RealmConfigNodeEnsured {
            node_id: node(3),
            kind: RealmNodeKind::Server,
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }

    #[test]
    fn realm_config_oidc_provider_operations_roundtrip() {
        let operations = [
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: oidc_provider("default"),
            },
            AdminDocumentOperation::RealmConfigOidcProviderRemoved {
                provider_id: "default".to_string(),
            },
        ];

        for operation in operations {
            assert_eq!(postcard_roundtrip(operation.clone()), operation);
        }
    }

    #[test]
    fn realm_config_settings_operation_roundtrips() {
        let operation = AdminDocumentOperation::RealmConfigSettingsSet {
            metadata_replication: MetadataReplicationConfig::new(3),
            discovery: RealmDiscoveryConfig::Static {
                endpoints: Vec::new(),
            },
        };

        assert_eq!(postcard_roundtrip(operation.clone()), operation);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentEvent {
    pub event_id: Ulid,
    pub target: AdminDocumentTarget,
    pub origin_node_id: NodeId,
    pub origin_seq: u64,
    pub observed: AdminDocumentClock,
    pub actor: Actor,
    pub op: AdminDocumentOperation,
}

impl AdminDocumentEvent {
    pub fn dot(&self) -> AdminDocumentDot {
        AdminDocumentDot {
            event_id: self.event_id,
            origin_node_id: self.origin_node_id,
            origin_seq: self.origin_seq,
        }
    }
}
