//! Realm identity with its authorization and config documents, nodes, quotas and discovery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{
    MAX_TOKEN_LIFETIME, REVOCATION_GRACE_SECS, revocation_live, revocation_retained,
    user_cutoff_hash,
};
use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::id_mint::named_id;
use crate::reducer::{AdminDocumentState, RevocationIndex};
use crate::structs::execution::job::family::SubmissionId;
use crate::structs::execution::job::identity::JobId;
use crate::structs::identity::auth::Actor;
use crate::structs::identity::auth::{Permission, Role};
use crate::structs::placement::binding_directory::{BindingDirectory, BindingError};
use crate::structs::placement::handle_allocation::HandleRangeDirectory;
use crate::structs::placement::record::{
    BandPool, BindingScope, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DEFAULT_SHARD_COUNT,
    DocumentClass, HandleRange, METADATA_HANDLE, NodePlacementEntry, PlacementBinding,
    PlacementOverride, PlacementRef, PlacementScope, PlacementStrategy, SHARD_SUBJECT_LEN,
    StrategyBinding, band_start, shard_for_subject,
};
use crate::structs::placement::transition::{
    CandidateMapNode, CandidatePlacementMap, FrozenStrategySelector, PlacementActivation,
    PlacementTransition,
};
use crate::structs::storage::node_info::{KIND_LABEL_KEY, NODE_LABEL_KEY};
use crate::structured_id::{PlacementHandle, StructuredId};
use crate::types::{GroupId, RoleId};
use crate::user::id::UserId;
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

pub const ENDPOINT_ANNOUNCEMENT_DOMAIN: &str = "aruna-realm-endpoint-v1";

/// This node's id and realm, while it has a realm transport.
pub type NodeRealm = Option<(crate::id::NodeId, RealmId)>;

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

    pub fn pkcs8_pem_bytes(&self) -> Result<[u8; 113], ConversionError> {
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
    /// The realm administrator role of a new realm; its id follows from the realm id.
    pub fn default_realm_doc(realm_id: RealmId) -> Self {
        let mut roles = HashMap::new();
        let admin = named_id(realm_id.as_bytes(), "realm_admin");
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

pub const METADATA_REPLICATION_FACTOR: u32 = 3;

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
    /// Strategy every node derives submission-family placement from. It and its
    /// shard count are immutable while v1 family records are retained.
    #[serde(rename = "job_family_strategy_id")]
    pub family_strategy_id: Ulid,
    pub strategy_bindings: Vec<StrategyBinding>,
    pub placement_overrides: Vec<PlacementOverride>,
    /// Append-only bindings materialized by the reducer overlay. Divergent
    /// same-handle values remain available to the fail-closed directory.
    pub placement_bindings: Vec<PlacementBinding>,
    /// Append-only grants retained for fail-closed overlap detection.
    pub placement_handle_ranges: Vec<HandleRange>,
    /// Append-only coordinator band pools forming a causal delegation tree.
    /// Each coordinator grants node bands only from pools it owns; precedence
    /// is by lineage (see [`crate::structs::placement::record::coordinator_spans`]).
    pub band_pools: Vec<BandPool>,
    /// Immutable snapshots of the placement view. `placement_map` stays the
    /// edit surface; only a published map can become a holder-set input.
    pub candidate_maps: Vec<CandidatePlacementMap>,
    /// One activated map epoch per `(strategy, bucket)`; the pinned input every
    /// holder resolution reads.
    pub placement_activations: Vec<PlacementActivation>,
    /// Non-terminal transitions plus their reduced barrier, proof, and
    /// completion sets.
    pub placement_transitions: Vec<PlacementTransition>,
    /// CEL request policies applied realm-wide (Class-1 replicated, so
    /// evaluation is local on every node).
    pub request_policies: Vec<crate::request_policy::RequestPolicy>,
    /// Bearer tokens revoked realm-wide. Class-1 replicated, so every node
    /// rejects a token revoked on any other node.
    pub revoked_tokens: Vec<TokenRevocation>,
    /// Highest validation/compaction time observed for the revocation set.
    pub revocation_floor: u64,
    /// Operator knowledge the planner reads: directed transfer bandwidth and
    /// the staleness bound of ranking telemetry.
    pub compute: crate::structs::placement::compute_config::RealmComputeConfig,
    /// Realm-wide federation settings and the descriptor signed from them.
    pub federation: Option<crate::federation::FederationSettings>,
    /// Realm default on/off state of optional portal features; a missing name means on.
    pub portal_features: BTreeMap<String, bool>,
    /// Whether replicas count toward group quotas; set with the quota policy.
    pub replica_quota: ReplicaQuota,
    /// The realm SMTP profile; a conflict or a removal turns email off.
    pub email: Option<crate::structs::execution::email::RealmEmailProfile>,
    /// Sealed realm vault records, sorted by entry key. A conflicting or deleted entry is absent.
    pub vault: Vec<crate::realm_vault::RealmVaultRecord>,
    /// Named realm secret records, sorted by name. A conflicting or deleted name is absent.
    pub secrets: Vec<crate::secrets::SecretRecord>,
    /// Published plugin versions, sorted by name and version. A conflicting version is absent.
    pub plugins: Vec<crate::plugins::PublishedPlugin>,
    /// Plugin installs, sorted by install id. A conflicting install is absent.
    pub installs: Vec<crate::plugin_installs::PluginInstall>,
}

pub const MAX_PORTAL_FEATURES: usize = 64;
pub const MAX_FEATURE_NAME: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PortalFeatureError {
    #[error("at most {MAX_PORTAL_FEATURES} portal features are allowed")]
    TooMany,
    #[error("portal feature names need 1 to {MAX_FEATURE_NAME} characters of a-z, 0-9 or '-'")]
    InvalidName,
}

/// Bounds the realm portal feature defaults that every node replicates.
pub fn validate_portal_features(
    features: &BTreeMap<String, bool>,
) -> Result<(), PortalFeatureError> {
    if features.len() > MAX_PORTAL_FEATURES {
        return Err(PortalFeatureError::TooMany);
    }
    let valid = |name: &String| {
        (1..=MAX_FEATURE_NAME).contains(&name.len())
            && name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    if !features.keys().all(valid) {
        return Err(PortalFeatureError::InvalidName);
    }
    Ok(())
}

/// Longest node display name, in characters.
pub const MAX_NODE_NAME: usize = 64;

/// The one node display name rule: trimmed, non-empty, at most [`MAX_NODE_NAME`] characters
/// and free of control characters. `None` means the name is refused.
pub fn normalize_node_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let length = trimmed.chars().count();
    ((1..=MAX_NODE_NAME).contains(&length) && !trimmed.chars().any(char::is_control))
        .then(|| trimmed.to_string())
}

/// A node's position on a map in degrees times 10^7, so replicated state holds no floats.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCoordinates {
    pub lat_e7: i32,
    pub lon_e7: i32,
}

impl NodeCoordinates {
    /// Rounds finite degrees within the latitude and longitude ranges; others are refused.
    pub fn from_degrees(lat: f64, lon: f64) -> Option<Self> {
        let valid = lat.is_finite() && lon.is_finite() && lat.abs() <= 90.0 && lon.abs() <= 180.0;
        valid.then(|| Self {
            lat_e7: (lat * 1e7).round() as i32,
            lon_e7: (lon * 1e7).round() as i32,
        })
    }

    /// Latitude and longitude in degrees.
    pub fn degrees(&self) -> (f64, f64) {
        (f64::from(self.lat_e7) / 1e7, f64::from(self.lon_e7) / 1e7)
    }

    pub fn valid(&self) -> bool {
        self.lat_e7.unsigned_abs() <= 900_000_000 && self.lon_e7.unsigned_abs() <= 1_800_000_000
    }
}

/// Where a node is shown on a map: a readable place name and coordinates. Presentation only:
/// placement, distinct locations and routing never read it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePlace {
    pub name: Option<String>,
    pub coordinates: Option<NodeCoordinates>,
}

impl NodePlace {
    /// Whether the place holds something, a name in its normalized form and valid coordinates.
    pub fn valid(&self) -> bool {
        let name = (self.name.as_ref())
            .is_none_or(|name| normalize_node_name(name).as_ref() == Some(name));
        (self.name.is_some() || self.coordinates.is_some())
            && name
            && self
                .coordinates
                .is_none_or(|coordinates| coordinates.valid())
    }
}

/// One realm-wide bearer token revocation. The expiry is the revoked token's
/// own `exp`: past it the token no longer validates, so the entry is pruned and
/// the replicated set stays bounded by one token lifetime of revocations.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TokenRevocation {
    pub token_hash: String,
    pub expires_at: u64,
}

/// Realm-wide quota policy. Lives in the realm config (Class-1, replicated
/// everywhere) so enforcement is local and group admins cannot raise their
/// own limits.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct QuotaConfig {
    #[serde(rename = "default_group_quota_bytes")]
    pub default_quota_bytes: Option<u64>,
    pub grace_factor_percent: u32,
    pub warn_threshold_percent: u32,
    pub group_overrides: Vec<GroupQuotaOverride>,
    #[serde(rename = "max_groups_per_user")]
    pub groups_per_user: Option<u32>,
    #[serde(rename = "user_group_cap_overrides")]
    pub group_cap_overrides: Vec<UserCapOverride>,
    #[serde(rename = "max_devices_per_user")]
    pub devices_per_user: Option<u32>,
    /// Inbound requests one user device may send a realm node per minute.
    /// `None` leaves devices uncapped.
    #[serde(rename = "device_requests_per_minute")]
    pub device_request_rate: Option<u32>,
    /// Inbound requests one user device may keep in flight at a realm node at
    /// once, which is what bounds long blob pulls. `None` leaves them uncapped.
    pub device_concurrent_pulls: Option<u32>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct GroupQuotaOverride {
    pub group_id: GroupId,
    pub quota_bytes: Option<u64>,
    pub grace_factor_percent: Option<u32>,
}

/// Whether copies of a distributed range besides its coordinator's count toward group quotas:
/// the realm default and per-group exceptions. References never count.
#[derive(Clone, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
pub struct ReplicaQuota {
    pub count: bool,
    pub groups: BTreeMap<GroupId, bool>,
}

impl ReplicaQuota {
    /// The group's exception, else the realm default.
    pub fn counts(&self, group_id: &GroupId) -> bool {
        self.groups.get(group_id).copied().unwrap_or(self.count)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct UserCapOverride {
    pub user_id: UserId,
    pub max_groups: Option<u32>,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            default_quota_bytes: None,
            grace_factor_percent: 110,
            warn_threshold_percent: 85,
            group_overrides: Vec::new(),
            groups_per_user: Some(3),
            group_cap_overrides: Vec::new(),
            devices_per_user: None,
            device_request_rate: None,
            device_concurrent_pulls: None,
        }
    }
}

impl QuotaConfig {
    /// None = unlimited.
    pub fn max_groups_for(&self, user_id: &UserId) -> Option<u32> {
        self.group_cap_overrides
            .iter()
            .find(|over| over.user_id == *user_id)
            .map(|over| over.max_groups)
            .unwrap_or(self.groups_per_user)
    }

    /// Resolves the effective pre-grace quota (in bytes) for a group: the group override's `quota_bytes`
    /// when an override exists (an override with `quota_bytes: None` makes the group explicitly unlimited),
    /// else the realm `default_quota_bytes`. `None` means unlimited.
    pub fn group_quota_bytes(&self, group_id: &GroupId) -> Option<u64> {
        match self
            .group_overrides
            .iter()
            .find(|over| over.group_id == *group_id)
        {
            Some(over) => over.quota_bytes,
            None => self.default_quota_bytes,
        }
    }

    /// Returns the effective group quota scaled by its group or global grace factor.
    /// `None` means the applicable override or realm default leaves the group unlimited.
    pub fn effective_group_ceiling(&self, group_id: &GroupId) -> Option<u64> {
        let over = self
            .group_overrides
            .iter()
            .find(|over| over.group_id == *group_id);
        let quota = self.group_quota_bytes(group_id)?;
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
    /// Owner-bound user device (laptop). Never a sync/holder target. The owner
    /// is fixed at enrollment and is a security input: every User-node decision
    /// reads it from here, never from a label or a self-authored descriptor.
    User {
        owner: UserId,
    },
}

impl RealmNodeKind {
    /// Whether nodes of this kind carry shared realm responsibilities: holder and placement membership,
    /// sync publication and relay, administrative event origination, routing and distributed-query
    /// targeting. A User node has none of them, whoever owns it.
    pub fn is_sync_eligible(&self) -> bool {
        !matches!(self, RealmNodeKind::User { .. })
    }

    /// Owner of a User-kind device; `None` for infrastructure nodes.
    pub fn owner(&self) -> Option<UserId> {
        match self {
            RealmNodeKind::User { owner } => Some(*owner),
            RealmNodeKind::Management | RealmNodeKind::Server => None,
        }
    }

    /// Value of the derived, read-only kind label placement views carry.
    pub fn label(&self) -> &'static str {
        match self {
            RealmNodeKind::Management => "management",
            RealmNodeKind::Server => "server",
            RealmNodeKind::User { .. } => "user",
        }
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

pub fn endpoint_signing_bytes(
    realm_id: &RealmId,
    node_id: &NodeId,
    endpoint_addr: &iroh::EndpointAddr,
    issued_at: u64,
    expires_at: u64,
    sequence: u64,
) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&(
        ENDPOINT_ANNOUNCEMENT_DOMAIN,
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
    /// Hashes the canonical realm configuration used to bind routed requests.
    pub fn digest(&self) -> Result<[u8; 32], ConversionError> {
        let mut canonical = self.clone();
        sort_canonical(&mut canonical.oidc_providers)?;
        sort_canonical(&mut canonical.nodes)?;
        sort_canonical(&mut canonical.placement_map)?;
        sort_canonical(&mut canonical.strategies)?;
        sort_canonical(&mut canonical.strategy_bindings)?;
        sort_canonical(&mut canonical.placement_overrides)?;
        sort_canonical(&mut canonical.placement_bindings)?;
        sort_canonical(&mut canonical.placement_handle_ranges)?;
        sort_canonical(&mut canonical.band_pools)?;
        sort_canonical(&mut canonical.placement_activations)?;
        // Exclude transition proofs because only activations affect routing. Retain referenced candidate
        // maps because holders depend on them; unreferenced maps prune independently per node.
        canonical.placement_transitions.clear();
        let referenced: std::collections::BTreeSet<u64> = canonical
            .placement_activations
            .iter()
            .map(|activation| activation.candidate_map_epoch)
            .collect();
        canonical
            .candidate_maps
            .retain(|map| referenced.contains(&map.epoch));
        sort_canonical(&mut canonical.candidate_maps)?;
        // Exclude independently converging revocations from routing digests, or each revocation would reject
        // forwarded requests until it replicated.
        canonical.revoked_tokens.clear();
        canonical.revocation_floor = 0;
        // Exclude request policies because they do not route requests; each receiver enforces its local set,
        // and nodes without a realm-config bucket may never observe them.
        canonical.request_policies.clear();
        // Exclude federation, portal features, email, vault and secrets: they never route requests.
        canonical.federation = None;
        canonical.portal_features.clear();
        canonical.email = None;
        canonical.vault.clear();
        canonical.secrets.clear();
        let encoded = postcard::to_allocvec(&canonical)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"aruna-realm-config-v1");
        hasher.update(&encoded);
        Ok(*hasher.finalize().as_bytes())
    }

    pub fn new(
        realm_id: RealmId,
        oidc_providers: Vec<OidcProviderConfig>,
        default_replication_factor: u32,
    ) -> Self {
        Self {
            realm_id,
            metadata_replication: MetadataReplicationConfig::new(default_replication_factor),
            oidc_providers,
            discovery: default_discovery_config(),
            nodes: Vec::new(),
            quota: QuotaConfig::default(),
            description: String::new(),
            placement_map: Vec::new(),
            request_policies: Vec::new(),
            revoked_tokens: Vec::new(),
            revocation_floor: 0,
            strategies: Vec::new(),
            default_strategy_id: None,
            family_strategy_id: Ulid::nil(),
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
            placement_bindings: Vec::new(),
            placement_handle_ranges: Vec::new(),
            band_pools: Vec::new(),
            candidate_maps: Vec::new(),
            placement_activations: Vec::new(),
            placement_transitions: Vec::new(),
            compute: crate::structs::placement::compute_config::RealmComputeConfig::default(),
            federation: None,
            portal_features: BTreeMap::new(),
            replica_quota: ReplicaQuota::default(),
            email: None,
            vault: Vec::new(),
            secrets: Vec::new(),
            plugins: Vec::new(),
            installs: Vec::new(),
        }
    }

    pub fn default_for_realm(realm_id: RealmId, oidc_providers: Vec<OidcProviderConfig>) -> Self {
        Self::new(realm_id, oidc_providers, METADATA_REPLICATION_FACTOR)
    }

    /// Installs `default` metadata replication and `everywhere` control-document strategies.
    /// Control documents stay local everywhere because permission checks require local authorization data.
    pub fn seed_default_placement(&mut self) {
        let default_strategy = PlacementStrategy {
            strategy_id: named_id(self.realm_id.as_bytes(), "default"),
            name: "default".to_string(),
            replica_count: Some(self.metadata_replication.default_replication_factor),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        };
        let everywhere_strategy = PlacementStrategy {
            strategy_id: named_id(self.realm_id.as_bytes(), "everywhere"),
            name: "everywhere".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        };
        // Submission families retain their dedicated immutable strategy and shard count, leaving ordinary
        // default-strategy administration independent.
        let job_family_strategy = PlacementStrategy {
            strategy_id: named_id(self.realm_id.as_bytes(), "job-family"),
            name: "job-family".to_string(),
            replica_count: Some(self.metadata_replication.default_replication_factor),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        };
        self.default_strategy_id = Some(default_strategy.strategy_id);
        self.family_strategy_id = job_family_strategy.strategy_id;
        self.strategy_bindings = [
            DocumentClass::MetadataRegistry,
            DocumentClass::Admin,
            DocumentClass::Group,
            DocumentClass::User,
        ]
        .into_iter()
        .map(|class| StrategyBinding {
            scope: BindingScope::Class(class),
            strategy_id: everywhere_strategy.strategy_id,
        })
        .collect();
        // Policy documents are replica-capped like ordinary data documents: a
        // non-holder resolves their holders from the ref and fetches by id.
        self.strategy_bindings.push(StrategyBinding {
            scope: BindingScope::Class(DocumentClass::PlacementPolicy),
            strategy_id: default_strategy.strategy_id,
        });
        // User vaults follow the metadata replication factor; a realm admin may rebind them.
        self.strategy_bindings.push(StrategyBinding {
            scope: BindingScope::Class(DocumentClass::UserVault),
            strategy_id: default_strategy.strategy_id,
        });
        // Reserve the low-band Metadata binding before grants begin. JobControl
        // bindings are per node band and appended at onboarding, never seeded.
        self.placement_bindings = vec![PlacementBinding {
            handle: PlacementHandle::new(METADATA_HANDLE).expect("handle is allocatable"),
            scope: PlacementScope::Realm(self.realm_id),
            document_class: DocumentClass::Metadata,
            strategy_id: default_strategy.strategy_id,
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        }];
        self.strategies = vec![default_strategy, everywhere_strategy, job_family_strategy];
    }

    /// Grants `node` the band `band` and appends the immutable JobControl
    /// binding on the band's first handle, exactly as onboarding does. The
    /// range id is derived from the band so distinct bands never collide.
    pub fn seed_job_control(&mut self, node: NodeId, band: u32) -> PlacementHandle {
        let mut range_bytes = [0u8; 16];
        range_bytes[..4].copy_from_slice(&(band + 1).to_be_bytes());
        let range = HandleRange {
            range_id: Ulid::from_bytes(range_bytes),
            owner: node,
            start: band_start(band),
            end: band_start(band + 1),
        };
        let handle = PlacementHandle::new(range.start).expect("band start is a valid handle");
        self.placement_handle_ranges.push(range);
        self.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(self.realm_id),
            document_class: DocumentClass::JobControl,
            strategy_id: self
                .default_strategy_id
                .expect("seeded config has a default strategy"),
            allocator_range_id: Some(range.range_id),
            allocated_by: Some(node),
            allocated_at_ms: Some(1),
        });
        handle
    }

    pub fn replication_factor_for(&self, group_id: GroupId, path: Option<&str>) -> usize {
        self.metadata_replication.factor_for(group_id, path)
    }

    pub fn effective_replication_factor(&self) -> Option<u32> {
        let strategy = match self.default_strategy_id {
            Some(strategy_id) => self.strategy(&strategy_id),
            None => self.strategies.first(),
        };
        strategy
            .map(|strategy| strategy.replica_count)
            .unwrap_or(Some(self.metadata_replication.default_replication_factor))
    }

    /// Replaces the provider with the same id in place or appends it. Portals sign in with the
    /// first provider, so an update must never reorder the list.
    pub fn upsert_provider(&mut self, provider: OidcProviderConfig) {
        match self.oidc_providers.iter_mut().find(|p| p.id == provider.id) {
            Some(existing) => *existing = provider,
            None => self.oidc_providers.push(provider),
        }
    }

    pub fn ensure_node(&mut self, node_id: NodeId, kind: RealmNodeKind) {
        let node_id = node_id.to_string();
        if let Some(existing) = self.nodes.iter_mut().find(|node| node.node_id == node_id) {
            existing.kind = kind;
            return;
        }

        self.nodes.push(RealmNode { node_id, kind });
    }

    /// Whether the realm-wide revocation set denies this bearer token hash.
    /// The floor is stamped by whichever node merged last, so only a clock more
    /// than the skew grace below it counts as a rollback and fails closed.
    pub fn token_revoked(&self, token_hash: &str, now: u64) -> bool {
        if self.revocation_floor.saturating_sub(now) > REVOCATION_GRACE_SECS {
            return true;
        }
        self.revoked_tokens
            .iter()
            .any(|entry| entry.token_hash == token_hash && revocation_live(entry.expires_at, now))
    }

    /// Unix seconds before which every credential of this user is denied, if a cutoff is live.
    pub fn user_cutoff(&self, user_id: &UserId, now: u64) -> Option<u64> {
        let hash = user_cutoff_hash(user_id);
        self.revoked_tokens
            .iter()
            .filter(|entry| entry.token_hash == hash && revocation_live(entry.expires_at, now))
            .map(|entry| entry.expires_at.saturating_sub(MAX_TOKEN_LIFETIME))
            .max()
    }

    /// Unions the locally accepted revocations with the reducer's materialized
    /// set and drops entries whose token has expired. Ownership stays in the
    /// reducer path; the deny overlay only needs one hash and expiry per token.
    pub fn merge_revocations(&mut self, reducer_state: &AdminDocumentState, now: u64) {
        self.revocation_floor = self.revocation_floor.max(reducer_state.revocation_floor);
        let index = reducer_state.revocation_index(now);
        self.merge_revocation_index(&index, now);
    }

    /// Merges an existing revocation index without rebuilding reducer paths.
    pub fn merge_revocation_index(&mut self, index: &RevocationIndex, now: u64) {
        self.revocation_floor = self.revocation_floor.max(index.watermark()).max(now);
        let effective_now = self.revocation_floor;
        let mut merged: BTreeMap<String, u64> = self
            .revoked_tokens
            .drain(..)
            .map(|entry| (entry.token_hash, entry.expires_at))
            .collect();
        for (token_hash, expires_at) in index.materialized() {
            merged
                .entry(token_hash)
                .and_modify(|current| *current = (*current).max(expires_at))
                .or_insert(expires_at);
        }
        // Retain one grace window past expiry so a node lagging inside the skew
        // bound still sees the entry; per-entry checks stay `revocation_live`.
        self.revoked_tokens = merged
            .into_iter()
            .filter(|(_, expires_at)| revocation_retained(*expires_at, effective_now))
            .map(|(token_hash, expires_at)| TokenRevocation {
                token_hash,
                expires_at,
            })
            .collect();
    }

    pub fn has_node(&self, node_id: NodeId) -> bool {
        let node_id = node_id.to_string();
        self.nodes.iter().any(|node| node.node_id == node_id)
    }

    /// The configured kind of `node_id`, or `None` when the realm does not list it.
    pub fn node_kind(&self, node_id: &NodeId) -> Option<&RealmNodeKind> {
        let node_id = node_id.to_string();
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| &node.kind)
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
    pub fn sync_eligible_nodes(&self) -> Result<Vec<NodeId>, ConversionError> {
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

    /// The one submission-family placement every node derives. Ingress, holder,
    /// target, fetch, and audit routes all resolve it here; a missing or
    /// dangling family strategy is refused instead of defaulted.
    pub fn family_placement(
        &self,
        submission_id: SubmissionId,
    ) -> Result<PlacementRef, JobFamilyError> {
        if self.family_strategy_id.is_nil() {
            return Err(JobFamilyError::Unset);
        }
        let strategy = self
            .strategy(&self.family_strategy_id)
            .ok_or(JobFamilyError::Dangling(self.family_strategy_id))?;
        Ok(PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: shard_for_subject(&submission_id.0, strategy.shard_count),
        })
    }

    /// Strategy a class-scoped document rides: class binding, realm binding,
    /// then the realm default. `Err(())` marks a configured ref naming no
    /// strategy, which fails closed instead of falling through to the next one.
    pub fn class_strategy(
        &self,
        class: DocumentClass,
    ) -> Result<Option<&PlacementStrategy>, ClassStrategyError> {
        for scope in [BindingScope::Class(class), BindingScope::Realm] {
            if let Some(binding) = self
                .strategy_bindings
                .iter()
                .find(|binding| binding.scope == scope)
            {
                return self.strategy(&binding.strategy_id).map(Some).ok_or(
                    ClassStrategyError::Dangling {
                        strategy_id: binding.strategy_id,
                    },
                );
            }
        }
        match self.default_strategy_id {
            Some(id) => self
                .strategy(&id)
                .map(Some)
                .ok_or(ClassStrategyError::Dangling { strategy_id: id }),
            None => Ok(None),
        }
    }

    /// Bucket one immutable policy document rides: its policy id hashed into
    /// the resolved strategy's shards, never which subjects the rule allows.
    pub fn policy_placement(&self, policy_id: Ulid) -> Option<PlacementRef> {
        let subject = policy_id.to_bytes();
        let override_ = self
            .placement_overrides
            .iter()
            .find(|over| over.subject == subject);
        let strategy = match override_.and_then(|over| over.strategy_id) {
            Some(strategy_id) => self.strategy(&strategy_id)?,
            None => match self.class_strategy(DocumentClass::PlacementPolicy) {
                Ok(Some(strategy)) => strategy,
                Ok(None) => self.strategies.first()?,
                Err(_) => return None,
            },
        };
        Some(PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: shard_for_subject(&subject, strategy.shard_count),
        })
    }

    /// Eligible-node view derived from the live config: one entry per node with a parseable id,
    /// `placement_map` fields defaulted, and the derived kind label overlaying entry labels (it always
    /// wins). This is what a published candidate map freezes.
    pub fn candidate_nodes(&self) -> Vec<CandidateMapNode> {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for realm_node in &self.nodes {
            let Ok(node_id) = NodeId::from_str(&realm_node.node_id) else {
                continue;
            };
            let entry = self.placement_entry(node_id);
            let mut labels = entry.map(|entry| entry.labels.clone()).unwrap_or_default();
            labels.insert(
                KIND_LABEL_KEY.to_string(),
                realm_node.kind.label().to_string(),
            );
            // The config's own id string, so a submission can pin this node.
            labels.insert(NODE_LABEL_KEY.to_string(), realm_node.node_id.clone());
            crate::structs::storage::node_info::stamp_location(
                &mut labels,
                entry
                    .map(|entry| entry.location.as_str())
                    .unwrap_or_default(),
            );
            nodes.push(CandidateMapNode {
                node_id,
                kind: realm_node.kind.clone(),
                location: entry
                    .map(|entry| entry.effective_location().to_string())
                    .unwrap_or_else(|| DEFAULT_LOCATION.to_string()),
                weight: entry
                    .map(|entry| entry.weight)
                    .unwrap_or(DEFAULT_NODE_WEIGHT),
                full: entry.is_some_and(|entry| entry.full),
                draining: entry.is_some_and(|entry| entry.draining),
                labels,
            });
        }
        nodes
    }

    /// Freezes selectors and shard overrides into the next candidate map, initializing only new
    /// buckets. Callers must publish the map and initialization together so reducers can advance them.
    pub fn freeze_map(&self, epoch: u64) -> CandidatePlacementMap {
        CandidatePlacementMap {
            epoch,
            nodes: self.candidate_nodes(),
            selectors: self
                .strategies
                .iter()
                .map(|strategy| FrozenStrategySelector {
                    strategy_id: strategy.strategy_id,
                    replica_count: strategy.replica_count,
                    distinct_locations: strategy.distinct_locations,
                    affinity: strategy.affinity.clone(),
                })
                .collect(),
            shard_overrides: self
                .placement_overrides
                .iter()
                .filter(|record| record.subject.len() == SHARD_SUBJECT_LEN)
                .cloned()
                .collect(),
        }
    }

    pub fn snapshot_candidate_map(&mut self) -> u64 {
        let epoch = self.newest_map_epoch().unwrap_or(0) + 1;
        self.candidate_maps.push(self.freeze_map(epoch));
        for strategy in &self.strategies {
            for shard in 0..strategy.shard_count {
                if self
                    .placement_activations
                    .iter()
                    .any(|entry| entry.strategy_id == strategy.strategy_id && entry.shard == shard)
                {
                    continue;
                }
                self.placement_activations.push(PlacementActivation {
                    strategy_id: strategy.strategy_id,
                    shard,
                    activation_epoch: 1,
                    candidate_map_epoch: epoch,
                    transition_id: None,
                });
            }
        }
        epoch
    }

    pub fn newest_map_epoch(&self) -> Option<u64> {
        self.candidate_maps.iter().map(|map| map.epoch).max()
    }

    /// The map at `epoch`, or `None` when it is missing or conflicted (two
    /// divergent maps at one epoch keep the epoch unusable).
    pub fn candidate_map(&self, epoch: u64) -> Option<&CandidatePlacementMap> {
        let mut matches = self.candidate_maps.iter().filter(|map| map.epoch == epoch);
        let map = matches.next()?;
        matches.next().is_none().then_some(map)
    }

    /// The activation of one bucket, or `None` when it is missing or
    /// conflicted. Fail-closed: a conflicted bucket resolves no holders.
    pub fn activation(&self, strategy_id: &Ulid, shard: u32) -> Option<&PlacementActivation> {
        let mut matches = self
            .placement_activations
            .iter()
            .filter(|entry| entry.strategy_id == *strategy_id && entry.shard == shard);
        let activation = matches.next()?;
        matches.next().is_none().then_some(activation)
    }

    pub fn transition(&self, transition_id: &Ulid) -> Option<&PlacementTransition> {
        self.placement_transitions
            .iter()
            .find(|transition| transition.plan.transition_id == *transition_id)
    }

    /// Rebuilds the derived Placement Binding Directory from the stored set.
    pub fn binding_directory(&self) -> BindingDirectory {
        BindingDirectory::from_parts(&self.placement_bindings, &self.handle_range_directory())
    }

    /// Rebuilds the derived Handle Range Directory from the granted set.
    pub fn handle_range_directory(&self) -> HandleRangeDirectory {
        HandleRangeDirectory::from_ranges(&self.placement_handle_ranges)
    }

    /// `node`'s JobControl handle: the reserved first handle of its lowest
    /// granted band, valid only while its binding resolves non-conflicted.
    pub fn job_control_handle(&self, node: &NodeId) -> Option<PlacementHandle> {
        let directory = self.binding_directory();
        self.placement_bindings
            .iter()
            .filter(|binding| {
                binding.document_class == DocumentClass::JobControl
                    && binding.scope == PlacementScope::Realm(self.realm_id)
                    && binding.allocated_by.as_ref() == Some(node)
                    && directory.resolve(binding.handle).is_ok()
            })
            .map(|binding| binding.handle)
            .min()
    }

    /// Immutable owner of `job_id`, derived purely from replicated state:
    /// `handle -> JobControl binding -> granting band -> owner`. Fail-closed:
    /// a missing or conflicted link is `Unavailable` (503), never absence.
    pub fn job_owner(&self, job_id: JobId) -> Result<NodeId, JobOwnerError> {
        let routable = job_id
            .as_routable()
            .map_err(|_| JobOwnerError::NotJobControl)?;
        let resolved = self
            .binding_directory()
            .resolve_id(&routable, |strategy_id| {
                self.strategy(&strategy_id)
                    .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
            })
            .map_err(|error| match error {
                // A bucket beyond the strategy's immutable capacity is proof of
                // an invalid id, not of unsynced state.
                BindingError::OutOfRange(_) => JobOwnerError::NotJobControl,
                error => JobOwnerError::Unavailable(error.to_string()),
            })?;
        if resolved.document_class != DocumentClass::JobControl
            || resolved.scope != PlacementScope::Realm(self.realm_id)
        {
            return Err(JobOwnerError::NotJobControl);
        }
        let handle = routable.placement_handle();
        self.placement_bindings
            .iter()
            .find(|binding| {
                binding.handle == handle && binding.document_class == DocumentClass::JobControl
            })
            .and_then(|binding| binding.allocated_by)
            .ok_or_else(|| {
                JobOwnerError::Unavailable("job-control binding lacks an owner".to_string())
            })
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

/// A class or realm binding names a strategy the config does not define, so no
/// class placement can be derived without inventing one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ClassStrategyError {
    #[error("strategy binding references undefined strategy {strategy_id}")]
    Dangling { strategy_id: Ulid },
}

/// Why submission-family placement cannot be derived. Both cases refuse
/// admission; neither ever falls back to the realm default strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum JobFamilyError {
    #[error("realm config names no job family placement strategy")]
    Unset,
    #[error("job family strategy {0} is not defined in this realm")]
    Dangling(Ulid),
}

/// Failure of the pure `JobId -> owner` derivation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JobOwnerError {
    /// The id provably names something other than this realm's job control,
    /// so it can belong to no job; any node may answer absence.
    #[error("job id does not name a job-control binding")]
    NotJobControl,
    /// Fail-closed: binding or range state is missing, conflicted, or unsynced.
    #[error("job-control placement unavailable: {0}")]
    Unavailable(String),
}

fn sort_canonical<T: Serialize>(values: &mut Vec<T>) -> Result<(), ConversionError> {
    let mut keyed = std::mem::take(values)
        .into_iter()
        .map(|value| Ok((postcard::to_allocvec(&value)?, value)))
        .collect::<Result<Vec<_>, ConversionError>>()?;
    keyed.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    *values = keyed.into_iter().map(|(_, value)| value).collect();
    Ok(())
}

pub fn default_discovery_config() -> RealmDiscoveryConfig {
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
    pub group_overrides: Vec<MetadataReplicationOverride>,
    pub path_overrides: Vec<MetadataPathOverride>,
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
pub struct MetadataReplicationOverride {
    pub group_id: GroupId,
    pub replication_factor: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct MetadataPathOverride {
    pub group_id: GroupId,
    pub path_prefix: String,
    pub replication_factor: u32,
}

fn normalize_replication_factor(replication_factor: u32) -> usize {
    replication_factor.max(1) as usize
}

#[cfg(test)]
#[path = "realm_tests.rs"]
mod test;
