use crate::NodeId;
use crate::errors::ConversionError;
use crate::structs::structs::{Permission, Role};
use crate::structs::{
    Actor, BindingScope, DocumentClass, NodePlacementEntry, PlacementOverride, PlacementStrategy,
    StrategyBinding,
};
use crate::types::{GroupId, RoleId, UserId};
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use ulid::Ulid;

pub const REALM_ENDPOINT_ANNOUNCEMENT_DOMAIN: &str = "aruna-realm-endpoint-v1";

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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RealmAuthorizationDocument {
    pub realm_id: RealmId,
    pub roles: HashMap<RoleId, Role>,
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

    pub fn to_bytes(&self, _actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

pub const DEFAULT_METADATA_REPLICATION_FACTOR: u32 = 3;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RealmConfigDocument {
    pub realm_id: RealmId,
    pub metadata_replication: MetadataReplicationConfig,
    pub oidc_providers: Vec<OidcProviderConfig>,
    pub discovery: RealmDiscoveryConfig,
    pub nodes: Vec<RealmNode>,
    pub quota: QuotaConfig,
    pub description: String,
    pub placement_map: Vec<NodePlacementEntry>,
    pub strategies: Vec<PlacementStrategy>,
    pub default_strategy_id: Option<Ulid>,
    pub strategy_bindings: Vec<StrategyBinding>,
    pub placement_overrides: Vec<PlacementOverride>,
}

/// Realm-wide quota policy. Lives in the realm config (Class-1, replicated
/// everywhere) so enforcement is local and group admins cannot raise their
/// own limits.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct QuotaConfig {
    pub default_group_quota_bytes: Option<u64>,
    pub grace_factor_percent: u32,
    pub warn_threshold_percent: u32,
    pub group_overrides: Vec<GroupQuotaOverride>,
    pub max_groups_per_user: Option<u32>,
    pub user_group_cap_overrides: Vec<UserGroupCapOverride>,
    pub max_devices_per_user: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct GroupQuotaOverride {
    pub group_id: GroupId,
    pub quota_bytes: Option<u64>,
    pub grace_factor_percent: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct UserGroupCapOverride {
    pub user_id: UserId,
    pub max_groups: Option<u32>,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            default_group_quota_bytes: None,
            grace_factor_percent: 110,
            warn_threshold_percent: 85,
            group_overrides: Vec::new(),
            max_groups_per_user: Some(3),
            user_group_cap_overrides: Vec::new(),
            max_devices_per_user: None,
        }
    }
}

impl QuotaConfig {
    /// None = unlimited.
    pub fn max_groups_for(&self, user_id: &UserId) -> Option<u32> {
        self.user_group_cap_overrides
            .iter()
            .find(|over| over.user_id == *user_id)
            .map(|over| over.max_groups)
            .unwrap_or(self.max_groups_per_user)
    }

    /// Resolves the effective pre-grace quota (in bytes) for a group: the group
    /// override's `quota_bytes` when an override exists — an existing override
    /// with `quota_bytes: None` means the group is explicitly unlimited — else
    /// the realm `default_group_quota_bytes`. `None` means unlimited.
    pub fn effective_group_quota_bytes(&self, group_id: &GroupId) -> Option<u64> {
        match self
            .group_overrides
            .iter()
            .find(|over| over.group_id == *group_id)
        {
            Some(over) => over.quota_bytes,
            None => self.default_group_quota_bytes,
        }
    }

    /// Resolves the hard ceiling (in bytes) a group's realm-wide `logical_bytes`
    /// may reach before writes are rejected: the effective quota
    /// (`effective_group_quota_bytes`) scaled by the effective grace factor (group
    /// override if present, else the global `grace_factor_percent`). Returns
    /// `None` when no quota applies — an existing override with `quota_bytes: None`
    /// or no override and no `default_group_quota_bytes` — i.e. the group is
    /// unlimited and no gate is enforced.
    pub fn effective_group_ceiling(&self, group_id: &GroupId) -> Option<u64> {
        let over = self
            .group_overrides
            .iter()
            .find(|over| over.group_id == *group_id);
        let quota = self.effective_group_quota_bytes(group_id)?;
        let grace = over
            .and_then(|over| over.grace_factor_percent)
            .unwrap_or(self.grace_factor_percent);
        let ceiling = u128::from(quota) * u128::from(grace) / 100;
        Some(ceiling.min(u128::from(u64::MAX)) as u64)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RealmDiscoveryConfig {
    Static {
        endpoints: Vec<StaticRealmEndpoint>,
    },
    Dynamic {
        methods: Vec<DynamicDiscoveryMethod>,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum DynamicDiscoveryMethod {
    IrohDns {
        origins: Vec<String>,
        relay_policy: RelayPolicy,
    },
    DhtSigned {
        ttl_secs: u64,
        refresh_after_secs: u64,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RelayPolicy {
    Disabled,
    Default,
    Custom { relays: Vec<String> },
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RealmNode {
    pub node_id: String,
    pub kind: RealmNodeKind,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum RealmNodeKind {
    Management,
    Server,
    Local,
    /// Owner-bound user device (laptop). Never a sync/holder target.
    User,
}

impl RealmNodeKind {
    /// User nodes must never become document holders or sync targets.
    pub fn is_sync_eligible(&self) -> bool {
        !matches!(self, RealmNodeKind::User)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct StaticRealmEndpoint {
    pub node_id: String,
    pub endpoint_addr: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct RealmEndpointAnnouncement {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub endpoint_addr: iroh::EndpointAddr,
    pub issued_at: u64,
    pub expires_at: u64,
    pub sequence: u64,
    pub signature: iroh::Signature,
}

pub fn realm_endpoint_announcement_signing_bytes(
    realm_id: &RealmId,
    node_id: &NodeId,
    endpoint_addr: &iroh::EndpointAddr,
    issued_at: u64,
    expires_at: u64,
    sequence: u64,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&(
        REALM_ENDPOINT_ANNOUNCEMENT_DOMAIN,
        realm_id,
        node_id,
        endpoint_addr,
        issued_at,
        expires_at,
        sequence,
    ))
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct OidcProviderConfig {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

impl RealmConfigDocument {
    pub fn new(
        realm_id: RealmId,
        oidc_providers: Vec<OidcProviderConfig>,
        default_replication_factor: u32,
    ) -> Self {
        Self {
            realm_id,
            metadata_replication: MetadataReplicationConfig::new(default_replication_factor),
            oidc_providers,
            discovery: default_realm_discovery_config(),
            nodes: Vec::new(),
            quota: QuotaConfig::default(),
            description: String::new(),
            placement_map: Vec::new(),
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
        }
    }

    pub fn default_for_realm(realm_id: RealmId, oidc_providers: Vec<OidcProviderConfig>) -> Self {
        Self::new(
            realm_id,
            oidc_providers,
            DEFAULT_METADATA_REPLICATION_FACTOR,
        )
    }

    /// Seeds the default placement strategies realm creation installs: a
    /// `default` strategy using the configured metadata replication factor (the
    /// realm default) plus an `everywhere` strategy bound to the
    /// `MetadataRegistry` and `Admin` document classes. Replaces any existing
    /// strategy configuration.
    pub fn seed_default_placement(&mut self) {
        let default_strategy = PlacementStrategy {
            strategy_id: Ulid::new(),
            name: "default".to_string(),
            replica_count: Some(self.metadata_replication.default_replication_factor),
            distinct_locations: false,
            affinity: Vec::new(),
        };
        let everywhere_strategy = PlacementStrategy {
            strategy_id: Ulid::new(),
            name: "everywhere".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
        };
        self.default_strategy_id = Some(default_strategy.strategy_id);
        self.strategy_bindings = vec![
            StrategyBinding {
                scope: BindingScope::Class(DocumentClass::MetadataRegistry),
                strategy_id: everywhere_strategy.strategy_id,
            },
            StrategyBinding {
                scope: BindingScope::Class(DocumentClass::Admin),
                strategy_id: everywhere_strategy.strategy_id,
            },
        ];
        self.strategies = vec![default_strategy, everywhere_strategy];
    }

    pub fn metadata_replication_factor_for(&self, group_id: GroupId, path: Option<&str>) -> usize {
        self.metadata_replication.factor_for(group_id, path)
    }

    pub fn effective_default_metadata_replication_factor(&self) -> Option<u32> {
        let strategy = match self.default_strategy_id {
            Some(strategy_id) => self.strategy(&strategy_id),
            None => self.strategies.first(),
        };
        strategy
            .map(|strategy| strategy.replica_count)
            .unwrap_or(Some(self.metadata_replication.default_replication_factor))
    }

    pub fn ensure_node(&mut self, node_id: NodeId, kind: RealmNodeKind) {
        let node_id = node_id.to_string();
        if let Some(existing) = self.nodes.iter_mut().find(|node| node.node_id == node_id) {
            existing.kind = kind;
            return;
        }

        self.nodes.push(RealmNode { node_id, kind });
    }

    pub fn has_node(&self, node_id: NodeId) -> bool {
        let node_id = node_id.to_string();
        self.nodes.iter().any(|node| node.node_id == node_id)
    }

    pub fn node_ids(&self) -> Result<Vec<NodeId>, ConversionError> {
        self.nodes
            .iter()
            .map(|node| {
                NodeId::from_str(&node.node_id)
                    .map_err(|error| ConversionError::FromStrError(error.to_string()))
            })
            .collect()
    }

    /// Node ids eligible as sync peers / document holders (excludes User
    /// kind nodes).
    pub fn sync_eligible_node_ids(&self) -> Result<Vec<NodeId>, ConversionError> {
        self.nodes
            .iter()
            .filter(|node| node.kind.is_sync_eligible())
            .map(|node| {
                NodeId::from_str(&node.node_id)
                    .map_err(|error| ConversionError::FromStrError(error.to_string()))
            })
            .collect()
    }

    pub fn placement_entry(&self, node_id: NodeId) -> Option<&NodePlacementEntry> {
        self.placement_map
            .iter()
            .find(|entry| entry.node_id == node_id)
    }

    pub fn strategy(&self, strategy_id: &Ulid) -> Option<&PlacementStrategy> {
        self.strategies
            .iter()
            .find(|strategy| strategy.strategy_id == *strategy_id)
    }

    pub fn to_bytes(&self, actor: &Actor) -> Result<Vec<u8>, ConversionError> {
        self.reconcile_bytes(None, actor)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn reconcile_bytes(
        &self,
        current: Option<&[u8]>,
        actor: &Actor,
    ) -> Result<Vec<u8>, ConversionError> {
        let _ = (current, actor);
        Ok(postcard::to_allocvec(self)?)
    }
}

pub fn default_realm_discovery_config() -> RealmDiscoveryConfig {
    RealmDiscoveryConfig::Dynamic {
        methods: vec![
            DynamicDiscoveryMethod::IrohDns {
                origins: vec!["n0".to_string()],
                relay_policy: RelayPolicy::Default,
            },
            DynamicDiscoveryMethod::DhtSigned {
                ttl_secs: 300,
                refresh_after_secs: 60,
            },
        ],
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct MetadataGroupReplicationOverride {
    pub group_id: GroupId,
    pub replication_factor: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct MetadataPathReplicationOverride {
    pub group_id: GroupId,
    pub path_prefix: String,
    pub replication_factor: u32,
}

fn normalize_replication_factor(replication_factor: u32) -> usize {
    replication_factor.max(1) as usize
}

#[cfg(test)]
mod test {
    use crate::NodeId;
    use crate::structs::{
        Actor, DynamicDiscoveryMethod, MetadataGroupReplicationOverride,
        MetadataPathReplicationOverride, OidcProviderConfig, RealmAuthorizationDocument,
        RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind,
        default_realm_discovery_config,
    };
    use ulid::Ulid;

    #[test]
    pub fn test_realm_auth_doc_conversion() {
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(RealmId([0u8; 32]));
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: crate::UserId::new(Ulid::new(), RealmId([0u8; 32])),
            realm_id: RealmId([0u8; 32]),
        };
        let bytes = auth_doc.to_bytes(&actor).unwrap();
        let hydrated_auth_doc = RealmAuthorizationDocument::from_bytes(&bytes).unwrap();

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
            discovery: default_realm_discovery_config(),
            nodes: Vec::new(),
            quota: super::QuotaConfig::default(),
            description: "Example Realm".to_string(),
            placement_map: Vec::new(),
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[14u8; 32]).public(),
            user_id: crate::UserId::new(Ulid::new(), RealmId([4u8; 32])),
            realm_id: RealmId([4u8; 32]),
        };

        let bytes = document.to_bytes(&actor).expect("to bytes");
        let restored = RealmConfigDocument::from_bytes(&bytes).expect("from bytes");

        assert_eq!(document, restored);
    }

    #[test]
    fn effective_group_ceiling_resolves_override_and_grace() {
        let group = Ulid::from_bytes([1u8; 16]);
        let other = Ulid::from_bytes([2u8; 16]);
        let quota = super::QuotaConfig {
            default_group_quota_bytes: Some(1_000),
            grace_factor_percent: 110,
            group_overrides: vec![super::GroupQuotaOverride {
                group_id: group,
                quota_bytes: Some(2_000),
                grace_factor_percent: Some(150),
            }],
            ..super::QuotaConfig::default()
        };

        // Override quota_bytes and grace win for the overridden group.
        assert_eq!(quota.effective_group_ceiling(&group), Some(3_000));
        // Default quota and global grace apply otherwise.
        assert_eq!(quota.effective_group_ceiling(&other), Some(1_100));

        // No default and no override => unlimited (no gate).
        let unlimited = super::QuotaConfig {
            default_group_quota_bytes: None,
            ..super::QuotaConfig::default()
        };
        assert_eq!(unlimited.effective_group_ceiling(&other), None);

        // An existing override with quota_bytes: None is explicitly unlimited even
        // when a finite default exists.
        let unlimited_override = super::QuotaConfig {
            default_group_quota_bytes: Some(1_000),
            grace_factor_percent: 110,
            group_overrides: vec![super::GroupQuotaOverride {
                group_id: group,
                quota_bytes: None,
                grace_factor_percent: None,
            }],
            ..super::QuotaConfig::default()
        };
        assert_eq!(unlimited_override.effective_group_ceiling(&group), None);
        assert_eq!(
            unlimited_override.effective_group_ceiling(&other),
            Some(1_100)
        );

        let huge = super::QuotaConfig {
            default_group_quota_bytes: Some(u64::MAX),
            grace_factor_percent: 100,
            ..super::QuotaConfig::default()
        };
        assert_eq!(huge.effective_group_ceiling(&other), Some(u64::MAX));

        let over_huge = super::QuotaConfig {
            default_group_quota_bytes: Some(u64::MAX),
            grace_factor_percent: 110,
            ..super::QuotaConfig::default()
        };
        assert_eq!(over_huge.effective_group_ceiling(&other), Some(u64::MAX));
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
            discovery: default_realm_discovery_config(),
            nodes: Vec::new(),
            quota: super::QuotaConfig::default(),
            description: String::new(),
            placement_map: Vec::new(),
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
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

    #[test]
    fn effective_default_replication_uses_placement_strategy_with_legacy_fallback() {
        let mut document = RealmConfigDocument::new(RealmId([6u8; 32]), Vec::new(), 5);
        document.seed_default_placement();

        let default_strategy_id = document.default_strategy_id.unwrap();
        assert_eq!(
            document
                .strategy(&default_strategy_id)
                .unwrap()
                .replica_count,
            Some(5)
        );
        assert_eq!(
            document.effective_default_metadata_replication_factor(),
            Some(5)
        );

        document
            .strategies
            .iter_mut()
            .find(|strategy| strategy.strategy_id == default_strategy_id)
            .unwrap()
            .replica_count = Some(2);
        assert_eq!(
            document.effective_default_metadata_replication_factor(),
            Some(2)
        );

        document
            .strategies
            .iter_mut()
            .find(|strategy| strategy.strategy_id == default_strategy_id)
            .unwrap()
            .replica_count = None;
        assert_eq!(
            document.effective_default_metadata_replication_factor(),
            None
        );

        document.default_strategy_id = None;
        assert_eq!(
            document.effective_default_metadata_replication_factor(),
            None
        );

        document.strategies.clear();
        assert_eq!(
            document.effective_default_metadata_replication_factor(),
            Some(5)
        );
    }

    #[test]
    pub fn default_discovery() {
        let discovery = default_realm_discovery_config();

        match discovery {
            RealmDiscoveryConfig::Dynamic { methods } => {
                assert!(matches!(
                    methods.as_slice(),
                    [
                        DynamicDiscoveryMethod::IrohDns { .. },
                        DynamicDiscoveryMethod::DhtSigned {
                            ttl_secs: 300,
                            refresh_after_secs: 60,
                        }
                    ]
                ));
            }
            other => panic!("unexpected default discovery config: {other:?}"),
        }
    }

    #[test]
    fn sync_eligible_node_ids_excludes_user_kind_nodes() {
        fn node_id(seed: u8) -> NodeId {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            iroh::SecretKey::from_bytes(&bytes).public()
        }

        let server = node_id(1);
        let user_device = node_id(2);
        let mut document = RealmConfigDocument::new(RealmId::from_bytes([9u8; 32]), Vec::new(), 3);
        document.ensure_node(server, RealmNodeKind::Server);
        document.ensure_node(user_device, RealmNodeKind::User);

        assert_eq!(document.node_ids().unwrap(), vec![server, user_device]);
        assert_eq!(document.sync_eligible_node_ids().unwrap(), vec![server]);
    }
}
