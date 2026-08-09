use crate::NodeId;
use crate::admin_document_reducer::{AdminDocumentReducerState, RevocationIndex};
use crate::auth::{REVOCATION_GRACE_SECS, revocation_live, revocation_retained};
use crate::errors::ConversionError;
use crate::structs::structs::{Permission, Role};
use crate::structs::{
    Actor, BandPool, BindingDirectory, BindingError, BindingScope, CandidateMapNode,
    CandidatePlacementMap, DEFAULT_LOCATION, DEFAULT_NODE_WEIGHT, DEFAULT_SHARD_COUNT,
    DocumentClass, HandleRange, HandleRangeDirectory, JobId, KIND_LABEL_KEY, METADATA_HANDLE,
    NodePlacementEntry, PlacementActivation, PlacementBinding, PlacementOverride, PlacementScope,
    PlacementStrategy, PlacementTransition, StrategyBinding, coordinator_spans,
};
use crate::structured_id::{PlacementHandle, StructuredId};
use crate::types::{GroupId, RoleId, UserId};
use core::fmt;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use thiserror::Error;
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
        let admin = Ulid::generate();
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
    /// Append-only bindings materialized by the reducer overlay. Divergent
    /// same-handle values remain available to the fail-closed directory.
    pub placement_bindings: Vec<PlacementBinding>,
    /// Append-only grants retained for fail-closed overlap detection.
    pub placement_handle_ranges: Vec<HandleRange>,
    /// Append-only coordinator band pools forming a causal delegation tree.
    /// Each coordinator grants node bands only from pools it owns; precedence
    /// is by lineage (see [`coordinator_spans`]).
    pub band_pools: Vec<BandPool>,
    /// Immutable snapshots of the placement view. `placement_map` stays the
    /// edit surface; only a published map can become a holder-set input.
    #[serde(default)]
    pub candidate_maps: Vec<CandidatePlacementMap>,
    /// One activated map epoch per `(strategy, bucket)`; the pinned input every
    /// holder resolution reads.
    #[serde(default)]
    pub placement_activations: Vec<PlacementActivation>,
    /// Non-terminal transitions plus their reduced barrier, proof, and
    /// completion sets.
    #[serde(default)]
    pub placement_transitions: Vec<PlacementTransition>,
    /// CEL request policies applied realm-wide (Class-1 replicated, so
    /// evaluation is local on every node).
    #[serde(default)]
    pub request_policies: Vec<crate::request_policy::RequestPolicy>,
    /// Bearer tokens revoked realm-wide. Class-1 replicated, so every node
    /// rejects a token revoked on any other node.
    #[serde(default)]
    pub revoked_tokens: Vec<TokenRevocation>,
    /// Highest validation/compaction time observed for the revocation set.
    pub revocation_floor: u64,
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

    /// Value of the derived, read-only kind label placement views carry.
    pub fn label(&self) -> &'static str {
        match self {
            RealmNodeKind::Management => "management",
            RealmNodeKind::Server => "server",
            RealmNodeKind::Local => "local",
            RealmNodeKind::User => "user",
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
        // Transitions are excluded for the same reason as revocations: their
        // barrier and proof sets converge independently, and only the
        // activation they advance changes where a request routes. Candidate
        // maps follow them: an unreferenced map is pruned once the transition
        // that named it is released, which is a per-node moment, and a map an
        // activation still names is immutable and fails closed when divergent.
        canonical.placement_transitions.clear();
        canonical.candidate_maps.clear();
        // Revocations are excluded: the digest binds routing agreement between
        // nodes, and a deny-list that converges independently would otherwise
        // make every revocation reject forwarded requests until it replicated.
        canonical.revoked_tokens.clear();
        canonical.revocation_floor = 0;
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
            discovery: default_realm_discovery_config(),
            nodes: Vec::new(),
            quota: QuotaConfig::default(),
            description: String::new(),
            placement_map: Vec::new(),
            request_policies: Vec::new(),
            revoked_tokens: Vec::new(),
            revocation_floor: 0,
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
            placement_bindings: Vec::new(),
            placement_handle_ranges: Vec::new(),
            band_pools: Vec::new(),
            candidate_maps: Vec::new(),
            placement_activations: Vec::new(),
            placement_transitions: Vec::new(),
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
    /// realm default) plus an `everywhere` strategy bound to the control-document
    /// classes. Replaces any existing strategy configuration.
    ///
    /// Group, user and metadata-registry documents are bound to `everywhere`
    /// (`replica_count: None`, i.e. every sync-eligible node) rather than to the
    /// capped default. They are control documents — O(groups + users), not
    /// O(documents) — and the permission system structurally requires them
    /// locally: `CheckPermissionsOperation` reads the group authorization
    /// document from the local `AUTH_KEYSPACE` and hard-fails when it is absent,
    /// so a node outside a group's replica set could not authorize any request
    /// touching that group. `DocumentClass::Group` covers the group document and
    /// its authorization document alike (see `placement::document_class`).
    pub fn seed_default_placement(&mut self) {
        let default_strategy = PlacementStrategy {
            strategy_id: Ulid::generate(),
            name: "default".to_string(),
            replica_count: Some(self.metadata_replication.default_replication_factor),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        };
        let everywhere_strategy = PlacementStrategy {
            strategy_id: Ulid::generate(),
            name: "everywhere".to_string(),
            replica_count: None,
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: DEFAULT_SHARD_COUNT,
        };
        self.default_strategy_id = Some(default_strategy.strategy_id);
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

    /// Unions the locally accepted revocations with the reducer's materialized
    /// set and drops entries whose token has expired. Ownership stays in the
    /// reducer path; the deny overlay only needs one hash and expiry per token.
    pub fn merge_revocations(&mut self, reducer_state: &AdminDocumentReducerState, now: u64) {
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

    /// Eligible-node view derived from the live config: one entry per node with
    /// a parseable id, `placement_map` fields defaulted, and the derived kind
    /// label overlaying entry labels (it always wins). This is what a published
    /// candidate map freezes.
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

    /// Freezes the current view as the next candidate map epoch and activates
    /// it for every bucket that has no activation yet. Already activated
    /// buckets keep their epoch: only a transition moves those.
    ///
    /// The activations this writes are literal document values. A production
    /// caller MUST pair it with a reduced `PublishCandidateMap` plus
    /// `InitializeActivations`, or the buckets can never advance: the reducer
    /// re-derives activations only for strategies it owns a path for.
    pub fn snapshot_candidate_map(&mut self) -> u64 {
        let epoch = self.newest_map_epoch().unwrap_or(0) + 1;
        self.candidate_maps.push(CandidatePlacementMap {
            epoch,
            nodes: self.candidate_nodes(),
        });
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

    /// Handle spans `node` may grant bands from (see [`coordinator_spans`]).
    pub fn coordinator_pool(&self, node: &NodeId) -> Vec<(u32, u32)> {
        coordinator_spans(&self.band_pools, node)
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
                BindingError::BucketOutOfRange(_) => JobOwnerError::NotJobControl,
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
    use crate::admin_document_reducer::AdminDocumentReducerState;
    use crate::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
    use crate::auth::REVOCATION_GRACE_SECS;
    use crate::structs::{
        Actor, CandidatePlacementMap, DynamicDiscoveryMethod, KIND_LABEL_KEY,
        MetadataGroupReplicationOverride, MetadataPathReplicationOverride, OidcProviderConfig,
        RealmAuthorizationDocument, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
        RealmNodeKind, TokenRevocation, default_realm_discovery_config,
    };
    use ulid::Ulid;

    #[test]
    pub fn test_realm_auth_doc_conversion() {
        let auth_doc = RealmAuthorizationDocument::new_default_realm_doc(RealmId([0u8; 32]));
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: crate::UserId::new(Ulid::generate(), RealmId([0u8; 32])),
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
        let group_id = Ulid::generate();
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
            request_policies: Vec::new(),
            revoked_tokens: Vec::new(),
            revocation_floor: 0,
            description: "Example Realm".to_string(),
            placement_map: Vec::new(),
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
            placement_bindings: Vec::new(),
            placement_handle_ranges: Vec::new(),
            band_pools: Vec::new(),
            candidate_maps: Vec::new(),
            placement_activations: Vec::new(),
            placement_transitions: Vec::new(),
        };
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[14u8; 32]).public(),
            user_id: crate::UserId::new(Ulid::generate(), RealmId([4u8; 32])),
            realm_id: RealmId([4u8; 32]),
        };

        let bytes = document.to_bytes(&actor).expect("to bytes");
        let restored = RealmConfigDocument::from_bytes(&bytes).expect("from bytes");

        assert_eq!(document, restored);
    }

    #[test]
    fn config_digest_changes() {
        let config = RealmConfigDocument::new(RealmId([4u8; 32]), Vec::new(), 3);
        let mut changed = config.clone();
        changed.description = "changed".to_string();

        assert_eq!(config.digest().unwrap(), config.clone().digest().unwrap());
        assert_ne!(config.digest().unwrap(), changed.digest().unwrap());
    }

    #[test]
    fn digest_ignores_revocations() {
        // A revocation converges on its own; if it moved the digest, forwarded
        // requests would be rejected between nodes until it replicated.
        let config = RealmConfigDocument::new(RealmId([5u8; 32]), Vec::new(), 3);
        let mut revoked = config.clone();
        revoked.revoked_tokens.push(TokenRevocation {
            token_hash: crate::auth::bearer_token_hash("token"),
            expires_at: 2_000,
        });
        revoked.revocation_floor = 1_500;

        assert!(revoked.token_revoked(&crate::auth::bearer_token_hash("token"), 1_000));
        assert_eq!(config.digest().unwrap(), revoked.digest().unwrap());
    }

    #[test]
    fn expired_revocation_ignored() {
        // Past its token's expiry an entry denies nothing and is pruned, so the
        // replicated set cannot grow without bound.
        let realm_id = RealmId([6u8; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let token_hash = crate::auth::bearer_token_hash("token");
        config.revoked_tokens.push(TokenRevocation {
            token_hash: token_hash.clone(),
            expires_at: 1_000,
        });

        assert!(config.token_revoked(&token_hash, 1_000));
        assert!(!config.token_revoked(&token_hash, 1_001));

        let reducer_state = AdminDocumentReducerState::new(
            crate::admin_documents::AdminDocumentTarget::RealmConfig { realm_id },
        );
        // Retained for one grace window so a skewed peer still sees it, then pruned.
        config.merge_revocations(&reducer_state, 1_001);
        assert_eq!(config.revoked_tokens.len(), 1);
        config.merge_revocations(&reducer_state, 1_001 + REVOCATION_GRACE_SECS + 1);
        assert!(config.revoked_tokens.is_empty());
    }

    #[test]
    fn floor_blocks_rollback() {
        // A compacted hash must stay expired when the wall clock moves back.
        let realm_id = RealmId([8u8; 32]);
        let owner = crate::UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
            user_id: owner,
            realm_id,
        };
        let target = AdminDocumentTarget::RealmConfig { realm_id };
        let token_hash = crate::auth::bearer_token_hash("rollback-token");
        let mut reducer = AdminDocumentReducerState::new(target);
        reducer
            .apply_operation(
                &actor,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: token_hash.clone(),
                    expires_at: 1_000,
                    token_owner: owner,
                },
            )
            .unwrap();

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        let index = reducer.revocation_index(1_000);
        config.merge_revocation_index(&index, 1_000);
        assert!(config.token_revoked(&token_hash, 999));

        reducer.compact_revocations(2_000);
        let index = reducer.revocation_index(2_000);
        config.merge_revocation_index(&index, 2_000);
        assert!(config.revoked_tokens.is_empty());
        assert_eq!(config.revocation_floor, 2_000);
        assert!(config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS - 1));
        assert!(!config.token_revoked(&token_hash, 2_000));
    }

    #[test]
    fn floor_tolerates_skew() {
        // The floor carries the merging node's clock, so a node a few seconds
        // behind it must keep serving instead of denying every bearer token.
        let realm_id = RealmId([9u8; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.revocation_floor = 2_000;

        let token_hash = crate::auth::bearer_token_hash("skewed-token");
        assert!(!config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS));
        assert!(config.token_revoked(&token_hash, 2_000 - REVOCATION_GRACE_SECS - 1));
    }

    #[test]
    fn owner_overlay_deduplicates() {
        let realm_id = RealmId([7u8; 32]);
        let owner_a = crate::UserId::local(Ulid::from_bytes([1u8; 16]), realm_id);
        let owner_b = crate::UserId::local(Ulid::from_bytes([2u8; 16]), realm_id);
        let actor_a = Actor {
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            user_id: owner_a,
            realm_id,
        };
        let actor_b = Actor {
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            user_id: owner_b,
            realm_id,
        };
        let mut reducer =
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id });
        let token_hash = crate::auth::bearer_token_hash("owner-overlay");
        reducer
            .apply_operation(
                &actor_a,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: token_hash.clone(),
                    expires_at: 2_000,
                    token_owner: owner_a,
                },
            )
            .unwrap();
        reducer
            .apply_operation(
                &actor_b,
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash: token_hash.clone(),
                    expires_at: 2_000,
                    token_owner: owner_b,
                },
            )
            .unwrap();

        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.merge_revocations(&reducer, 1_000);

        assert_eq!(
            config.revoked_tokens,
            vec![TokenRevocation {
                token_hash,
                expires_at: 2_000,
            }]
        );
    }

    #[test]
    fn snapshot_freezes_the_view() {
        use crate::structs::NodePlacementEntry;

        fn node_id(seed: u8) -> NodeId {
            iroh::SecretKey::from_bytes(&[seed; 32]).public()
        }
        let mut config = RealmConfigDocument::new(RealmId([11u8; 32]), Vec::new(), 3);
        config.seed_default_placement();
        config.ensure_node(node_id(1), RealmNodeKind::Server);
        config.placement_map.push(NodePlacementEntry {
            node_id: node_id(1),
            location: "eu".to_string(),
            weight: 250,
            full: false,
            draining: false,
            labels: Default::default(),
        });

        assert_eq!(config.snapshot_candidate_map(), 1);
        let buckets: u32 = config.strategies.iter().map(|s| s.shard_count).sum();
        assert_eq!(config.placement_activations.len(), buckets as usize);
        let strategy_id = config.default_strategy_id.unwrap();
        let activation = *config
            .activation(&strategy_id, 7)
            .expect("bucket activated");
        assert_eq!(activation.activation_epoch, 1);
        assert_eq!(activation.candidate_map_epoch, 1);
        assert_eq!(activation.transition_id, None);

        // A later config edit changes the live view but never a published map.
        config.placement_map[0].weight = 1;
        config.ensure_node(node_id(2), RealmNodeKind::Server);
        let frozen = config.candidate_map(1).expect("epoch 1 is unconflicted");
        assert_eq!(frozen.nodes.len(), 1);
        assert_eq!(frozen.nodes[0].weight, 250);
        assert_eq!(
            frozen.nodes[0].labels.get(KIND_LABEL_KEY),
            Some(&"server".to_string())
        );
        assert_eq!(config.candidate_nodes().len(), 2);

        // A second snapshot opens a new epoch and leaves activations pinned.
        assert_eq!(config.snapshot_candidate_map(), 2);
        assert_eq!(config.newest_map_epoch(), Some(2));
        assert_eq!(config.activation(&strategy_id, 7), Some(&activation));
        assert_eq!(config.candidate_map(2).unwrap().nodes.len(), 2);

        // Divergent maps at one epoch keep the epoch unusable.
        let conflicting = CandidatePlacementMap {
            epoch: 2,
            nodes: Vec::new(),
        };
        config.candidate_maps.push(conflicting);
        assert!(config.candidate_map(2).is_none());
    }

    #[test]
    fn digest_ignores_order() {
        let mut config = RealmConfigDocument::new(
            RealmId([4u8; 32]),
            vec![
                OidcProviderConfig {
                    id: "z".to_string(),
                    issuer: "z".to_string(),
                    audience: "z".to_string(),
                    discovery_url: "z".to_string(),
                },
                OidcProviderConfig {
                    id: "a".to_string(),
                    issuer: "a".to_string(),
                    audience: "a".to_string(),
                    discovery_url: "a".to_string(),
                },
            ],
            3,
        );
        config.seed_default_placement();
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[1; 32]).public(),
            RealmNodeKind::Management,
        );
        config.ensure_node(
            iroh::SecretKey::from_bytes(&[2; 32]).public(),
            RealmNodeKind::Server,
        );
        let mut reordered = config.clone();
        reordered.oidc_providers.reverse();
        reordered.nodes.reverse();
        reordered.strategies.reverse();
        reordered.strategy_bindings.reverse();
        reordered.placement_bindings.reverse();

        assert_eq!(config.digest().unwrap(), reordered.digest().unwrap());
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
        let group_id = Ulid::generate();
        let other_group_id = Ulid::generate();
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
            request_policies: Vec::new(),
            revoked_tokens: Vec::new(),
            revocation_floor: 0,
            description: String::new(),
            placement_map: Vec::new(),
            strategies: Vec::new(),
            default_strategy_id: None,
            strategy_bindings: Vec::new(),
            placement_overrides: Vec::new(),
            placement_bindings: Vec::new(),
            placement_handle_ranges: Vec::new(),
            band_pools: Vec::new(),
            candidate_maps: Vec::new(),
            placement_activations: Vec::new(),
            placement_transitions: Vec::new(),
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

    #[test]
    fn owner_survives_rebalance() {
        // The derived owner is a pure function of band + binding; arbitrary
        // placement-map, strategy, and override changes never move it.
        use crate::structs::{
            DocumentClass, FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, JobId,
            JobOwnerError, NodePlacementEntry, PlacementBinding, PlacementOverride, PlacementScope,
        };
        use crate::structured_id::{BucketId, PlacementHandle};

        fn node_id(seed: u8) -> NodeId {
            iroh::SecretKey::from_bytes(&[seed; 32]).public()
        }
        let realm_id = RealmId([7u8; 32]);
        let owner = node_id(1);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.seed_default_placement();
        let range_id = Ulid::from_bytes([9; 16]);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        });
        let handle = PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap();
        config.placement_bindings.push(PlacementBinding {
            handle,
            scope: PlacementScope::Realm(realm_id),
            document_class: DocumentClass::JobControl,
            strategy_id: config.default_strategy_id.unwrap(),
            allocator_range_id: Some(range_id),
            allocated_by: Some(owner),
            allocated_at_ms: Some(1),
        });
        assert_eq!(config.job_control_handle(&owner), Some(handle));
        assert_eq!(config.job_control_handle(&node_id(2)), None);

        let job_id = JobId::from_parts(1, handle, BucketId::new(5).unwrap(), 9).unwrap();
        assert_eq!(config.job_owner(job_id), Ok(owner));

        // Holders move: nodes join, the owner is excluded everywhere, weights
        // shift, the default strategy is replaced. The owner does not.
        for seed in 2..6u8 {
            config.ensure_node(node_id(seed), RealmNodeKind::Management);
            config.placement_map.push(NodePlacementEntry {
                node_id: node_id(seed),
                location: String::new(),
                weight: 500,
                full: false,
                draining: false,
                labels: Default::default(),
            });
        }
        config.placement_overrides.push(PlacementOverride {
            subject: b"any-subject".to_vec(),
            pinned: vec![node_id(2)],
            excluded: vec![owner],
            strategy_id: None,
        });
        assert_eq!(config.job_owner(job_id), Ok(owner));

        // An unknown handle is fail-closed 503 material, never absence.
        let unsynced = PlacementHandle::new(FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE).unwrap();
        let foreign = JobId::from_parts(1, unsynced, BucketId::new(0).unwrap(), 9).unwrap();
        assert!(matches!(
            config.job_owner(foreign),
            Err(JobOwnerError::Unavailable(_))
        ));

        // A non-JobControl handle proves the id is no job of this realm.
        let metadata = PlacementHandle::new(super::METADATA_HANDLE).unwrap();
        let invalid = JobId::from_parts(1, metadata, BucketId::new(0).unwrap(), 9).unwrap();
        assert_eq!(config.job_owner(invalid), Err(JobOwnerError::NotJobControl));

        // A bucket past the strategy capacity is proof too.
        let wide = JobId::from_parts(1, handle, BucketId::new(64).unwrap(), 9).unwrap();
        assert_eq!(config.job_owner(wide), Err(JobOwnerError::NotJobControl));

        // A conflicted binding fails closed as unavailable.
        let mut divergent = config.placement_bindings.last().unwrap().clone();
        divergent.allocated_at_ms = Some(2);
        config.placement_bindings.push(divergent);
        assert!(matches!(
            config.job_owner(job_id),
            Err(JobOwnerError::Unavailable(_))
        ));
        assert_eq!(config.job_control_handle(&owner), None);
    }

    #[test]
    fn directory_rebuilds_state() {
        use crate::structs::{DocumentClass, HandleRange, PlacementBinding, PlacementScope};
        use crate::structured_id::PlacementHandle;

        let owner = iroh::SecretKey::from_bytes(&[3; 32]).public();
        let range_id = Ulid::from_bytes([8; 16]);
        fn binding(handle: u32, seed: u8, owner: NodeId, range_id: Ulid) -> PlacementBinding {
            PlacementBinding {
                handle: PlacementHandle::new(handle).unwrap(),
                scope: PlacementScope::Group(Ulid::from_bytes([seed; 16])),
                document_class: DocumentClass::Metadata,
                strategy_id: Ulid::from_bytes([seed.wrapping_add(1); 16]),
                allocator_range_id: Some(range_id),
                allocated_by: Some(owner),
                allocated_at_ms: Some(1),
            }
        }

        let mut config = RealmConfigDocument::new(RealmId([2u8; 32]), Vec::new(), 3);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner,
            start: 3,
            end: 20,
        });
        let first = binding(10, 1, owner, range_id);
        config.placement_bindings.push(first.clone());

        let directory = config.binding_directory();
        assert_eq!(
            directory.resolve(first.handle).map(|t| t.strategy_id),
            Ok(first.strategy_id)
        );
        assert_eq!(directory.allocated(), 1);

        let mut provenance_conflict = config.clone();
        let mut divergent_provenance = first.clone();
        divergent_provenance.allocated_at_ms = Some(2);
        provenance_conflict
            .placement_bindings
            .push(divergent_provenance);
        assert_eq!(provenance_conflict.binding_directory().conflicted(), 1);

        // A same-handle, different-tuple entry fails closed as conflicted.
        config
            .placement_bindings
            .push(binding(10, 2, owner, range_id));
        assert!(config.binding_directory().resolve(first.handle).is_err());
        assert_eq!(config.binding_directory().conflicted(), 1);
    }
}
