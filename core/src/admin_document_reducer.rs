use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Included, Unbounded};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
    AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use crate::auth::{REVOCATION_GRACE_SECS, revocation_live, revocation_retained, valid_token_hash};
use crate::structs::{
    Actor, BandPool, BindingScope, DocumentClass, HandleRange, MAX_PLACEMENT_SHARD_COUNT,
    MetadataRegistryRecord, MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig,
    PlacementBinding, PlacementOverride, PlacementStrategy, QuotaConfig, RealmConfigDocument,
    RealmDiscoveryConfig, RealmId, RealmNodeKind, StrategyBinding, reserved_label,
};
use crate::structured_id::PlacementHandle;
use crate::types::{RoleId, UserId};
use crate::user_update_validation::{
    UserAttributeValidationError, validate_user_attribute_key, validate_user_attribute_value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminDocumentApplyStatus {
    Applied,
    Duplicate,
    Redundant,
    StaleOriginSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdminDocumentReducerError {
    #[error("admin document event target does not match reducer state")]
    TargetMismatch,
    #[error("admin document event operation is not supported for target")]
    UnsupportedTarget,
    #[error(transparent)]
    InvalidUserAttribute(#[from] UserAttributeValidationError),
    #[error("placement labels must not set the derived label `{0}`")]
    ReservedPlacementLabel(String),
    #[error("placement strategy replica count must not be zero")]
    ZeroPlacementReplicaCount,
    #[error(
        "placement strategy shard count must be a non-zero power of two no greater than {}",
        MAX_PLACEMENT_SHARD_COUNT
    )]
    InvalidPlacementShardCount,
    #[error("placement strategy shard count cannot be changed")]
    PlacementShardCountChanged,
    #[error("placement handle range is malformed")]
    InvalidHandleRange,
    #[error("revoked bearer token hash is malformed")]
    InvalidTokenHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentAttributeVersion {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflictValue {
    pub value: Option<String>,
    pub dot: AdminDocumentDot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentConflict {
    pub path: String,
    pub values: Vec<AdminDocumentConflictValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminDocumentReducerState {
    pub target: AdminDocumentTarget,
    pub clock: AdminDocumentClock,
    pub applied_event_ids: BTreeSet<Ulid>,
    pub user_attributes: BTreeMap<String, AdminDocumentAttributeVersion>,
    pub conflicts: BTreeMap<String, AdminDocumentConflict>,
    #[serde(default)]
    pub user_name: Option<AdminDocumentAttributeVersion>,
    #[serde(default)]
    pub user_subject_ids: BTreeMap<String, AdminDocumentAttributeVersion>,
    #[serde(default)]
    pub equivalent_value_dots: BTreeMap<String, BTreeSet<AdminDocumentDot>>,
    pub revocation_floor: u64,
    pub revocation_next_expiry: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RevocationPath {
    hash: String,
    expires_at: u64,
    token_owner: UserId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RevocationCandidate {
    path: String,
    expires_at: u64,
    token_owner: UserId,
    dot: AdminDocumentDot,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RevocationGroup {
    paths: BTreeSet<String>,
    candidates: Vec<RevocationCandidate>,
    event_ids: BTreeSet<Ulid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RevocationIndex {
    now: u64,
    groups: BTreeMap<String, RevocationGroup>,
    retained: BTreeMap<String, RevocationCandidate>,
    live: BTreeMap<String, RevocationCandidate>,
    origin_counts: BTreeMap<NodeId, usize>,
    owner_counts: BTreeMap<(NodeId, UserId), usize>,
    next_expiry: Option<u64>,
}

fn expiry_threshold(expires_at: u64) -> u64 {
    expires_at.saturating_add(REVOCATION_GRACE_SECS)
}

fn candidate_cmp(left: &RevocationCandidate, right: &RevocationCandidate) -> Ordering {
    left.expires_at
        .cmp(&right.expires_at)
        .then_with(|| right.dot.cmp(&left.dot))
        .then_with(|| right.token_owner.cmp(&left.token_owner))
}

fn value_matches(version: &AdminDocumentAttributeVersion, expires_at: u64) -> bool {
    version
        .value
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        == Some(expires_at)
}

fn add_paths<T>(
    paths: &BTreeMap<String, T>,
    indexed: &mut BTreeMap<String, Option<RevocationPath>>,
) {
    let prefix = format!("{REALM_CONFIG_REVOKED_TOKENS_PATH}.");
    for path in paths
        .range((Included(prefix.clone()), Unbounded))
        .map(|(path, _)| path)
    {
        if !path.starts_with(&prefix) {
            break;
        }
        indexed.entry(path.clone()).or_insert_with(|| {
            revoked_token_entry(path).map(|(hash, expires_at, token_owner)| RevocationPath {
                hash: hash.to_string(),
                expires_at,
                token_owner,
            })
        });
    }
}

impl RevocationIndex {
    fn build(state: &AdminDocumentReducerState, now: u64) -> Self {
        if !matches!(&state.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Self {
                now,
                groups: BTreeMap::new(),
                retained: BTreeMap::new(),
                live: BTreeMap::new(),
                origin_counts: BTreeMap::new(),
                owner_counts: BTreeMap::new(),
                next_expiry: None,
            };
        }

        let mut indexed = BTreeMap::new();
        add_paths(&state.user_subject_ids, &mut indexed);
        add_paths(&state.equivalent_value_dots, &mut indexed);
        add_paths(&state.conflicts, &mut indexed);

        let mut groups = BTreeMap::new();
        for (path, entry) in indexed {
            let Some(entry) = entry else {
                continue;
            };
            let group = groups
                .entry(entry.hash.clone())
                .or_insert_with(RevocationGroup::default);
            group.paths.insert(path.clone());

            if let Some(version) = state.user_subject_ids.get(&path) {
                group.event_ids.insert(version.dot.event_id);
                if value_matches(version, entry.expires_at) {
                    group.candidates.push(RevocationCandidate {
                        path: path.clone(),
                        expires_at: entry.expires_at,
                        token_owner: entry.token_owner,
                        dot: version.dot,
                    });
                }
            }
            if let Some(dots) = state.equivalent_value_dots.get(&path) {
                group.event_ids.extend(dots.iter().map(|dot| dot.event_id));
                group
                    .candidates
                    .extend(dots.iter().copied().map(|dot| RevocationCandidate {
                        path: path.clone(),
                        expires_at: entry.expires_at,
                        token_owner: entry.token_owner,
                        dot,
                    }));
            }
            if let Some(conflict) = state.conflicts.get(&path) {
                group
                    .event_ids
                    .extend(conflict.values.iter().map(|value| value.dot.event_id));
                group.candidates.extend(
                    conflict
                        .values
                        .iter()
                        .filter(|value| {
                            value.value.as_deref() == Some(entry.expires_at.to_string().as_str())
                        })
                        .map(|value| RevocationCandidate {
                            path: path.clone(),
                            expires_at: entry.expires_at,
                            token_owner: entry.token_owner,
                            dot: value.dot,
                        }),
                );
            }
        }

        let mut retained = BTreeMap::new();
        let mut live = BTreeMap::new();
        let mut origin_counts = BTreeMap::new();
        let mut owner_counts = BTreeMap::new();
        let mut next_expiry = None;
        for (hash, group) in &groups {
            let Some(winner) = group
                .candidates
                .iter()
                .max_by(|left, right| candidate_cmp(left, right))
                .cloned()
            else {
                continue;
            };
            next_expiry = Some(
                next_expiry.map_or(expiry_threshold(winner.expires_at), |current| {
                    current.min(expiry_threshold(winner.expires_at))
                }),
            );
            if revocation_retained(winner.expires_at, now) {
                *origin_counts.entry(winner.dot.origin_node_id).or_insert(0) += 1;
                *owner_counts
                    .entry((winner.dot.origin_node_id, winner.token_owner))
                    .or_insert(0) += 1;
                retained.insert(hash.clone(), winner.clone());
            }
            if revocation_live(winner.expires_at, now) {
                live.insert(hash.clone(), winner);
            }
        }

        Self {
            now,
            groups,
            retained,
            live,
            origin_counts,
            owner_counts,
            next_expiry,
        }
    }

    pub fn origin(&self, token_hash: &str) -> Option<NodeId> {
        self.retained
            .get(token_hash)
            .map(|candidate| candidate.dot.origin_node_id)
    }

    pub fn owner(&self, token_hash: &str) -> Option<UserId> {
        self.retained
            .get(token_hash)
            .map(|candidate| candidate.token_owner)
    }

    pub fn count(&self, origin_node_id: &NodeId) -> usize {
        self.origin_counts
            .get(origin_node_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn owner_count(&self, origin_node_id: &NodeId, token_owner: &UserId) -> usize {
        self.owner_counts
            .get(&(*origin_node_id, *token_owner))
            .copied()
            .unwrap_or_default()
    }

    pub fn materialized(&self) -> BTreeMap<String, u64> {
        self.live
            .iter()
            .map(|(hash, candidate)| (hash.clone(), candidate.expires_at))
            .collect()
    }

    pub(crate) fn watermark(&self) -> u64 {
        self.now
    }

    fn next_expiry(&self) -> Option<u64> {
        self.next_expiry
    }

    fn refresh_expiry(&mut self) {
        self.next_expiry = self
            .groups
            .values()
            .flat_map(|group| group.candidates.iter())
            .map(|candidate| expiry_threshold(candidate.expires_at))
            .min();
    }

    fn clear_hash(&mut self, hash: &str) {
        if let Some(candidate) = self.retained.remove(hash) {
            let origin = candidate.dot.origin_node_id;
            let owner = (origin, candidate.token_owner);
            if let Some(count) = self.origin_counts.get_mut(&origin) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.origin_counts.remove(&origin);
                }
            }
            if let Some(count) = self.owner_counts.get_mut(&owner) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.owner_counts.remove(&owner);
                }
            }
        }
        self.live.remove(hash);
    }

    fn set_hash(&mut self, hash: &str, winner: RevocationCandidate) {
        self.clear_hash(hash);
        if revocation_retained(winner.expires_at, self.now) {
            *self
                .origin_counts
                .entry(winner.dot.origin_node_id)
                .or_insert(0) += 1;
            *self
                .owner_counts
                .entry((winner.dot.origin_node_id, winner.token_owner))
                .or_insert(0) += 1;
            self.retained.insert(hash.to_string(), winner.clone());
        }
        if revocation_live(winner.expires_at, self.now) {
            self.live.insert(hash.to_string(), winner);
        }
    }

    fn canonical_group(winner: &RevocationCandidate) -> RevocationGroup {
        RevocationGroup {
            paths: BTreeSet::from([winner.path.clone()]),
            candidates: vec![winner.clone()],
            event_ids: BTreeSet::from([winner.dot.event_id]),
        }
    }

    fn apply(
        &mut self,
        state: &mut AdminDocumentReducerState,
        event: &AdminDocumentEvent,
        token_hash: &str,
        expires_at: u64,
        token_owner: UserId,
    ) -> AdminDocumentApplyStatus {
        self.clear_hash(token_hash);
        let group = self.groups.remove(token_hash).unwrap_or_default();
        let winner = state.canonicalize_group(
            token_hash,
            group,
            Some((expires_at, token_owner, event.dot())),
        );
        let status = winner
            .as_ref()
            .filter(|winner| winner.dot == event.dot())
            .map_or(AdminDocumentApplyStatus::Redundant, |_| {
                AdminDocumentApplyStatus::Applied
            });
        if let Some(winner) = winner {
            self.groups
                .insert(token_hash.to_string(), Self::canonical_group(&winner));
            self.set_hash(token_hash, winner);
        }
        self.refresh_expiry();
        state.revocation_next_expiry = self.next_expiry();
        state.clock.advance(event.origin_node_id, event.origin_seq);
        if status != AdminDocumentApplyStatus::Redundant {
            state.applied_event_ids.insert(event.event_id);
        }
        status
    }

    pub fn compact(&mut self, state: &mut AdminDocumentReducerState) {
        let groups = std::mem::take(&mut self.groups);
        let retained = std::mem::take(&mut self.retained);
        self.live.clear();
        self.origin_counts.clear();
        self.owner_counts.clear();
        state.revocation_floor = state.revocation_floor.max(self.now);
        for (hash, group) in groups {
            let Some(winner) = retained.get(&hash) else {
                state.remove_revocation_group(&group);
                continue;
            };
            let unchanged = group.paths.len() == 1
                && group.paths.contains(&winner.path)
                && state
                    .user_subject_ids
                    .get(&winner.path)
                    .is_some_and(|version| {
                        value_matches(version, winner.expires_at) && version.dot == winner.dot
                    })
                && !state.equivalent_value_dots.contains_key(&winner.path)
                && !state.conflicts.contains_key(&winner.path);
            if !unchanged {
                state.remove_revocation_group(&group);
                state.user_subject_ids.insert(
                    winner.path.clone(),
                    AdminDocumentAttributeVersion {
                        value: Some(winner.expires_at.to_string()),
                        dot: winner.dot,
                    },
                );
            }
            state.applied_event_ids.insert(winner.dot.event_id);
            self.groups
                .insert(hash.clone(), Self::canonical_group(winner));
            self.set_hash(&hash, winner.clone());
        }
        self.refresh_expiry();
        state.revocation_next_expiry = self.next_expiry();
    }
}

pub fn decode_admin_document_reducer_state(
    bytes: &[u8],
) -> Result<AdminDocumentReducerState, postcard::Error> {
    postcard::from_bytes(bytes)
}

/// Overlays the realm-config placement paths owned by `reducer_state` onto `config`.
/// Paths absent from both the reducer values and conflicts remain untouched.
///
/// The final repair uses the live strategy with the lowest id as the deterministic
/// fallback for missing defaults, bindings, and explicit override strategy ids. If
/// no strategy is live, references are cleared while override pins and exclusions
/// are retained. Reducer values are not changed, so a later strategy upsert can
/// restore an assignment that was only dangling in the materialized snapshot.
pub fn overlay_realm_config_placement_reducer_materialization(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
) {
    if reducer_state
        .user_subject_ids
        .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
        || reducer_state
            .conflicts
            .contains_key(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
    {
        config.default_strategy_id = reducer_state.materialized_realm_config_default_strategy();
    }

    let materialized_placement_map = reducer_state.materialized_realm_config_placement_map();
    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = realm_config_placement_node_id_from_path(path) {
            config
                .placement_map
                .retain(|entry| entry.node_id != node_id);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(node_id) = realm_config_placement_node_id_from_path(path) else {
            continue;
        };
        config
            .placement_map
            .retain(|entry| entry.node_id != node_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(entry) = materialized_placement_map.get(&node_id) {
            config.placement_map.push(entry.clone());
        }
    }

    let materialized_strategies = reducer_state.materialized_realm_config_placement_strategies();
    for path in reducer_state.conflicts.keys() {
        if let Some(strategy_id) = realm_config_placement_strategy_id_from_path(path) {
            config
                .strategies
                .retain(|strategy| strategy.strategy_id != strategy_id);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(strategy_id) = realm_config_placement_strategy_id_from_path(path) else {
            continue;
        };
        config
            .strategies
            .retain(|strategy| strategy.strategy_id != strategy_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(strategy) = materialized_strategies.get(&strategy_id) {
            config.strategies.push(strategy.clone());
        }
    }

    let materialized_bindings = reducer_state.materialized_realm_config_strategy_bindings();
    for path in reducer_state.conflicts.keys() {
        if let Some(scope_key) = realm_config_strategy_binding_scope_key_from_path(path) {
            config
                .strategy_bindings
                .retain(|binding| binding_scope_key(&binding.scope) != scope_key);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(scope_key) = realm_config_strategy_binding_scope_key_from_path(path) else {
            continue;
        };
        config
            .strategy_bindings
            .retain(|binding| binding_scope_key(&binding.scope) != scope_key);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(binding) = materialized_bindings.get(scope_key) {
            config.strategy_bindings.push(binding.clone());
        }
    }

    let materialized_overrides = reducer_state.materialized_realm_config_placement_overrides();
    for path in reducer_state.conflicts.keys() {
        if let Some(subject_key) = realm_config_placement_override_subject_key_from_path(path) {
            config
                .placement_overrides
                .retain(|record| hex::encode(&record.subject) != subject_key);
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(subject_key) = realm_config_placement_override_subject_key_from_path(path) else {
            continue;
        };
        config
            .placement_overrides
            .retain(|record| hex::encode(&record.subject) != subject_key);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(record) = materialized_overrides.get(subject_key) {
            config.placement_overrides.push(record.clone());
        }
    }

    // Placement bindings are immutable and fail closed: unlike strategy bindings
    // (which drop a conflicted scope), every divergent value for a conflicted
    // handle is retained so the derived binding directory reports a conflict.
    let materialized_bindings = reducer_state.materialized_placement_bindings();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(handle) = placement_binding_handle(path) else {
            continue;
        };
        config
            .placement_bindings
            .retain(|binding| binding.handle != handle);
        for value in &conflict.values {
            if let Some(binding) = value.value.as_deref().and_then(parse_placement_binding) {
                config.placement_bindings.push(binding);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(handle) = placement_binding_handle(path) else {
            continue;
        };
        config
            .placement_bindings
            .retain(|binding| binding.handle != handle);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(binding) = materialized_bindings.get(&handle) {
            config.placement_bindings.push(binding.clone());
        }
    }

    // Same-id grant conflicts retain every value; the range directory derives
    // distinct-id overlap conflicts.
    let materialized_ranges = reducer_state.materialized_handle_ranges();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(range_id) = handle_range_id(path) else {
            continue;
        };
        config
            .placement_handle_ranges
            .retain(|range| range.range_id != range_id);
        for value in &conflict.values {
            if let Some(range) = value.value.as_deref().and_then(parse_handle_range) {
                config.placement_handle_ranges.push(range);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(range_id) = handle_range_id(path) else {
            continue;
        };
        config
            .placement_handle_ranges
            .retain(|range| range.range_id != range_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(range) = materialized_ranges.get(&range_id) {
            config.placement_handle_ranges.push(*range);
        }
    }

    // Band pools mirror handle ranges: same-id divergence retains every value.
    let materialized_pools = reducer_state.materialized_band_pools();
    for (path, conflict) in &reducer_state.conflicts {
        let Some(pool_id) = band_pool_id(path) else {
            continue;
        };
        config.band_pools.retain(|pool| pool.pool_id != pool_id);
        for value in &conflict.values {
            if let Some(pool) = value.value.as_deref().and_then(parse_band_pool) {
                config.band_pools.push(pool);
            }
        }
    }
    for path in reducer_state.user_subject_ids.keys() {
        let Some(pool_id) = band_pool_id(path) else {
            continue;
        };
        config.band_pools.retain(|pool| pool.pool_id != pool_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(pool) = materialized_pools.get(&pool_id) {
            config.band_pools.push(*pool);
        }
    }

    repair_realm_config_placement_references(config);
}

fn repair_realm_config_placement_references(config: &mut RealmConfigDocument) {
    // `placement_bindings` are intentionally exempt: they are immutable, so a
    // binding naming a removed strategy fails closed at resolve rather than
    // being repaired here.
    let live_strategy_ids: BTreeSet<_> = config
        .strategies
        .iter()
        .map(|strategy| strategy.strategy_id)
        .collect();
    let fallback_strategy_id = live_strategy_ids.first().copied();

    let Some(fallback_strategy_id) = fallback_strategy_id else {
        config.default_strategy_id = None;
        config.strategy_bindings.clear();
        for record in &mut config.placement_overrides {
            record.strategy_id = None;
        }
        return;
    };

    if config
        .default_strategy_id
        .is_none_or(|strategy_id| !live_strategy_ids.contains(&strategy_id))
    {
        config.default_strategy_id = Some(fallback_strategy_id);
    }
    for binding in &mut config.strategy_bindings {
        if !live_strategy_ids.contains(&binding.strategy_id) {
            binding.strategy_id = fallback_strategy_id;
        }
    }
    for record in &mut config.placement_overrides {
        if record
            .strategy_id
            .is_some_and(|strategy_id| !live_strategy_ids.contains(&strategy_id))
        {
            record.strategy_id = Some(fallback_strategy_id);
        }
    }
}

impl AdminDocumentReducerState {
    pub fn new(target: AdminDocumentTarget) -> Self {
        Self {
            target,
            clock: AdminDocumentClock::default(),
            applied_event_ids: BTreeSet::new(),
            user_attributes: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            user_name: None,
            user_subject_ids: BTreeMap::new(),
            equivalent_value_dots: BTreeMap::new(),
            revocation_floor: 0,
            revocation_next_expiry: None,
        }
    }

    pub fn apply_operation(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
    ) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
        let observed = self.clock.clone();
        let event = AdminDocumentEvent {
            event_id: Ulid::generate(),
            target: self.target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: observed.sequence_for(&actor.node_id) + 1,
            observed,
            actor: actor.clone(),
            op,
        };
        self.apply(&event)?;
        Ok(event)
    }

    pub fn apply_revocation_operation(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
        index: &mut RevocationIndex,
    ) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
        let observed = self.clock.clone();
        let event = AdminDocumentEvent {
            event_id: Ulid::generate(),
            target: self.target.clone(),
            origin_node_id: actor.node_id,
            origin_seq: observed.sequence_for(&actor.node_id) + 1,
            observed,
            actor: actor.clone(),
            op,
        };
        self.apply_revocation_event(&event, index)?;
        Ok(event)
    }

    pub fn apply_revocation_event(
        &mut self,
        event: &AdminDocumentEvent,
        index: &mut RevocationIndex,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        let AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
        } = &event.op
        else {
            return Err(AdminDocumentReducerError::UnsupportedTarget);
        };
        if !matches!(&event.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Err(AdminDocumentReducerError::UnsupportedTarget);
        }
        if !valid_token_hash(token_hash) {
            return Err(AdminDocumentReducerError::InvalidTokenHash);
        }
        Ok(index.apply(self, event, token_hash, *expires_at, *token_owner))
    }

    pub fn apply(
        &mut self,
        event: &AdminDocumentEvent,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        if event.target != self.target {
            return Err(AdminDocumentReducerError::TargetMismatch);
        }
        if self.applied_event_ids.contains(&event.event_id) {
            return Ok(AdminDocumentApplyStatus::Duplicate);
        }
        let stale_on_all_paths = !matches!(
            &event.op,
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
                | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
                | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
                | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
        ) && operation_paths(&event.op)
            .iter()
            .all(|path| self.event_is_stale_for_path(event, path));
        let mut apply_status = AdminDocumentApplyStatus::Applied;

        match (&event.target, &event.op) {
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name,
                    owner,
                },
            ) => {
                self.apply_group_created(event, realm_id, display_name, owner);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleAdded { role_id },
            ) => {
                self.apply_group_role(event, role_id, role_id.to_string());
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleCreated { role },
            ) => {
                self.apply_group_role(event, &role.role_id, role_definition_value(role));
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleRemoved { role_id },
            ) => {
                self.apply_group_role_removed(event, role_id);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
            ) => {
                self.apply_group_role_user_assignment(
                    event,
                    role_id,
                    user_id,
                    Some(user_id.to_string()),
                );
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
            ) => {
                self.apply_group_role_user_assignment(event, role_id, user_id, None);
            }
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupPoliciesSet { policies },
            ) => {
                self.apply_group_field(event, GROUP_POLICIES_PATH, Some(policies_value(policies)));
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleAdded { role_id },
            ) => {
                self.apply_realm_role(event, role_id, role_id.to_string());
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleCreated { role },
            ) => {
                self.apply_realm_role(event, &role.role_id, role_definition_value(role));
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id },
            ) => {
                self.apply_realm_role_user_assignment(
                    event,
                    role_id,
                    user_id,
                    Some(user_id.to_string()),
                );
            }
            (
                AdminDocumentTarget::Realm { .. },
                AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id },
            ) => {
                self.apply_realm_role_user_assignment(event, role_id, user_id, None);
            }
            (AdminDocumentTarget::User { .. }, AdminDocumentOperation::UserNameSet { name }) => {
                self.apply_user_name(event, name);
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserSubjectIdAdded { subject_id },
            ) => {
                self.apply_user_subject_id(event, subject_id, Some(subject_id.clone()));
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserSubjectIdRemoved { subject_id },
            ) => {
                self.apply_user_subject_id(event, subject_id, None);
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserAttributeSet { key, value },
            ) => {
                validate_user_attribute_key(key)?;
                validate_user_attribute_value(key, value)?;
                self.apply_user_attribute(event, key, Some(value.clone()));
            }
            (
                AdminDocumentTarget::User { .. },
                AdminDocumentOperation::UserAttributeRemoved { key },
            ) => {
                validate_user_attribute_key(key)?;
                self.apply_user_attribute(event, key, None);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
            ) => {
                self.apply_realm_config_node(event, node_id, kind);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
            ) => {
                self.apply_realm_config_oidc_provider(
                    event,
                    &provider.id,
                    Some(oidc_provider_value(provider)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigOidcProviderRemoved { provider_id },
            ) => {
                self.apply_realm_config_oidc_provider(event, provider_id, None);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigSettingsSet {
                    metadata_replication,
                    discovery,
                },
            ) => {
                self.apply_realm_config_settings(event, metadata_replication, discovery);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigDescriptionSet { description },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_DESCRIPTION_PATH,
                    description.clone(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigQuotaSet { quota },
            ) => {
                self.apply_realm_config_setting(event, REALM_CONFIG_QUOTA_PATH, quota_value(quota));
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPoliciesSet { policies },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_POLICIES_PATH,
                    policies_value(policies),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigTokenRevoked {
                    token_hash,
                    expires_at,
                    token_owner,
                },
            ) => {
                if !valid_token_hash(token_hash) {
                    return Err(AdminDocumentReducerError::InvalidTokenHash);
                }
                apply_status =
                    self.apply_revocation_full(event, token_hash, *expires_at, *token_owner);
                self.refresh_revocation_expiry();
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodePlacementSet { entry },
            ) => {
                if let Some(label) = reserved_label(&entry.labels) {
                    return Err(AdminDocumentReducerError::ReservedPlacementLabel(
                        label.to_string(),
                    ));
                }
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_node_path(&entry.node_id),
                    Some(placement_entry_value(entry)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_node_path(node_id),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
            ) => {
                if strategy.replica_count == Some(0) {
                    return Err(AdminDocumentReducerError::ZeroPlacementReplicaCount);
                }
                if strategy.shard_count == 0
                    || !strategy.shard_count.is_power_of_two()
                    || strategy.shard_count > MAX_PLACEMENT_SHARD_COUNT
                {
                    return Err(AdminDocumentReducerError::InvalidPlacementShardCount);
                }
                if self
                    .materialized_realm_config_placement_strategies()
                    .get(&strategy.strategy_id)
                    .is_some_and(|current| current.shard_count != strategy.shard_count)
                {
                    return Err(AdminDocumentReducerError::PlacementShardCountChanged);
                }
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_strategy_path(&strategy.strategy_id),
                    Some(placement_strategy_value(strategy)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_strategy_path(strategy_id),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
            ) => {
                self.apply_realm_config_setting(
                    event,
                    REALM_CONFIG_DEFAULT_STRATEGY_PATH,
                    strategy_id.to_string(),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigStrategyBindingSet { binding },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_strategy_binding_path(&binding.scope),
                    Some(strategy_binding_value(binding)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_strategy_binding_path(scope),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementOverrideSet { record },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_override_path(&record.subject),
                    Some(placement_override_value(record)),
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject },
            ) => {
                self.apply_realm_config_placement_field(
                    event,
                    realm_config_placement_override_path(subject),
                    None,
                );
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
            ) => {
                self.apply_placement_binding(event, binding);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
            ) => {
                if !range.is_well_formed() {
                    return Err(AdminDocumentReducerError::InvalidHandleRange);
                }
                self.apply_handle_range(event, range);
            }
            (
                AdminDocumentTarget::RealmConfig { .. },
                AdminDocumentOperation::RealmConfigBandPoolAssigned { pool },
            ) => {
                if !pool.is_well_formed() {
                    return Err(AdminDocumentReducerError::InvalidHandleRange);
                }
                self.apply_band_pool(event, pool);
            }
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }

        if stale_on_all_paths {
            self.applied_event_ids.insert(event.event_id);
            self.clock.advance(event.origin_node_id, event.origin_seq);
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        self.clock.advance(event.origin_node_id, event.origin_seq);
        if apply_status != AdminDocumentApplyStatus::Redundant {
            self.applied_event_ids.insert(event.event_id);
        }
        Ok(apply_status)
    }

    pub fn materialized_user_name(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::User { .. }) {
            return None;
        }

        self.user_name
            .as_ref()
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_user_subject_ids(&self) -> BTreeSet<String> {
        if !matches!(&self.target, AdminDocumentTarget::User { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .values()
            .filter_map(|version| version.value.clone())
            .collect()
    }

    pub fn materialized_user_attributes(&self) -> BTreeMap<String, String> {
        if !matches!(&self.target, AdminDocumentTarget::User { .. }) {
            return BTreeMap::new();
        }

        self.user_attributes
            .iter()
            .filter_map(|(key, version)| {
                version
                    .value
                    .as_ref()
                    .map(|value| (key.clone(), value.clone()))
            })
            .collect()
    }

    pub fn materialized_group_display_name(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(GROUP_DISPLAY_NAME_PATH)
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_group_realm_id(&self) -> Option<RealmId> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(GROUP_REALM_ID_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| RealmId::from_base64(value).ok())
    }

    pub fn materialized_group_owner(&self) -> Option<UserId> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(GROUP_OWNER_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| UserId::from_string(value).ok())
    }

    pub fn materialized_group_policies(&self) -> Option<Vec<crate::request_policy::RequestPolicy>> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(GROUP_POLICIES_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(policies_from_value)
    }

    pub fn materialized_group_roles(&self) -> BTreeSet<RoleId> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| version.value.as_ref().map(|_| path))
            .filter_map(|path| group_role_id_from_path(path))
            .collect()
    }

    pub fn materialized_group_role_user_assignments(&self) -> BTreeMap<RoleId, BTreeSet<UserId>> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return BTreeMap::new();
        }

        let active_roles = self.materialized_group_roles();

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let role_id = group_role_user_assignment_role_id_from_path(path)?;
                let user_id = version
                    .value
                    .as_ref()
                    .and_then(|value| UserId::from_string(value).ok())?;

                active_roles
                    .contains(&role_id)
                    .then_some((role_id, user_id))
            })
            .fold(BTreeMap::new(), |mut assignments, (role_id, user_id)| {
                assignments
                    .entry(role_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(user_id);
                assignments
            })
    }

    pub fn materialized_realm_roles(&self) -> BTreeSet<RoleId> {
        if !matches!(&self.target, AdminDocumentTarget::Realm { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| version.value.as_ref().map(|_| path))
            .filter_map(|path| realm_role_id_from_path(path))
            .collect()
    }

    pub fn materialized_realm_role_user_assignments(&self) -> BTreeMap<RoleId, BTreeSet<UserId>> {
        if !matches!(&self.target, AdminDocumentTarget::Realm { .. }) {
            return BTreeMap::new();
        }

        let active_roles = self.materialized_realm_roles();

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let role_id = realm_role_user_assignment_role_id_from_path(path)?;
                let user_id = version
                    .value
                    .as_ref()
                    .and_then(|value| UserId::from_string(value).ok())?;

                active_roles
                    .contains(&role_id)
                    .then_some((role_id, user_id))
            })
            .fold(BTreeMap::new(), |mut assignments, (role_id, user_id)| {
                assignments
                    .entry(role_id)
                    .or_insert_with(BTreeSet::new)
                    .insert(user_id);
                assignments
            })
    }

    pub fn materialized_realm_config_nodes(&self) -> BTreeMap<NodeId, RealmNodeKind> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = realm_config_node_id_from_path(path)?;
                let kind = version
                    .value
                    .as_deref()
                    .and_then(realm_node_kind_from_value)?;
                Some((node_id, kind))
            })
            .collect()
    }

    pub fn materialized_realm_config_oidc_providers(&self) -> BTreeMap<String, OidcProviderConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let provider_id = realm_config_oidc_provider_id_from_path(path)?;
                let provider = version
                    .value
                    .as_deref()
                    .and_then(oidc_provider_from_value)?;

                (provider.id == provider_id).then(|| (provider_id.to_string(), provider))
            })
            .collect()
    }

    pub fn materialized_realm_config_metadata_replication(
        &self,
    ) -> Option<MetadataReplicationConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_METADATA_REPLICATION_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(metadata_replication_from_value)
    }

    pub fn materialized_realm_config_discovery(&self) -> Option<RealmDiscoveryConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_DISCOVERY_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(realm_discovery_from_value)
    }

    pub fn materialized_realm_config_description(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_DESCRIPTION_PATH)
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_realm_policies(&self) -> Option<Vec<crate::request_policy::RequestPolicy>> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_POLICIES_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(policies_from_value)
    }

    pub fn materialized_realm_config_quota(&self) -> Option<QuotaConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_QUOTA_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(quota_from_value)
    }

    pub fn materialized_realm_config_placement_map(&self) -> BTreeMap<NodeId, NodePlacementEntry> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = realm_config_placement_node_id_from_path(path)?;
                let entry = version
                    .value
                    .as_deref()
                    .and_then(placement_entry_from_value)?;

                (entry.node_id == node_id).then_some((node_id, entry))
            })
            .collect()
    }

    pub fn materialized_realm_config_placement_strategies(
        &self,
    ) -> BTreeMap<Ulid, PlacementStrategy> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let strategy_id = realm_config_placement_strategy_id_from_path(path)?;
                let strategy = version
                    .value
                    .as_deref()
                    .and_then(placement_strategy_from_value)?;

                (strategy.strategy_id == strategy_id).then_some((strategy_id, strategy))
            })
            .collect()
    }

    pub fn materialized_realm_config_default_strategy(&self) -> Option<Ulid> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_DEFAULT_STRATEGY_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| Ulid::from_string(value).ok())
    }

    pub fn materialized_realm_config_strategy_bindings(&self) -> BTreeMap<String, StrategyBinding> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let scope_key = realm_config_strategy_binding_scope_key_from_path(path)?;
                let binding = version
                    .value
                    .as_deref()
                    .and_then(strategy_binding_from_value)
                    .map(|binding| normalized_strategy_binding(&binding))?;

                let canonical_scope_key = binding_scope_key(&binding.scope);
                (canonical_scope_key == scope_key).then_some((canonical_scope_key, binding))
            })
            .collect()
    }

    pub fn materialized_realm_config_placement_overrides(
        &self,
    ) -> BTreeMap<String, PlacementOverride> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let subject_key = realm_config_placement_override_subject_key_from_path(path)?;
                let record = version
                    .value
                    .as_deref()
                    .and_then(placement_override_from_value)?;

                (hex::encode(&record.subject) == subject_key)
                    .then(|| (subject_key.to_string(), record))
            })
            .collect()
    }

    pub fn materialized_placement_bindings(&self) -> BTreeMap<PlacementHandle, PlacementBinding> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let handle = placement_binding_handle(path)?;
                let binding = version.value.as_deref().and_then(parse_placement_binding)?;

                (binding.handle == handle).then_some((handle, binding))
            })
            .collect()
    }

    pub fn materialized_handle_ranges(&self) -> BTreeMap<Ulid, HandleRange> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let range_id = handle_range_id(path)?;
                let range = version.value.as_deref().and_then(parse_handle_range)?;

                (range.range_id == range_id).then_some((range_id, range))
            })
            .collect()
    }

    pub fn revocation_index(&self, now: u64) -> RevocationIndex {
        RevocationIndex::build(self, now)
    }

    pub fn revocation_compaction_due(&self, now: u64) -> bool {
        self.revocation_next_expiry
            .is_some_and(|threshold| now > threshold)
    }

    pub fn advance_revocation_floor(&mut self, now: u64) {
        self.revocation_floor = self.revocation_floor.max(now);
    }

    pub fn materialized_revoked_tokens(&self) -> BTreeMap<String, u64> {
        self.revocation_index(self.revocation_floor).materialized()
    }

    /// Drops expired revocations and canonicalizes each live token to one dot.
    /// The floor prevents a later clock rollback from deleting more state.
    pub fn compact_revocations(&mut self, now: u64) {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return;
        }

        self.revocation_floor = self.revocation_floor.max(now);
        let mut index = self.revocation_index(self.revocation_floor);
        index.compact(self);
    }

    pub fn live_revocation_count(&self, origin_node_id: &NodeId, now: u64) -> usize {
        self.revocation_index(now).count(origin_node_id)
    }

    pub fn live_owner_count(
        &self,
        origin_node_id: &NodeId,
        token_owner: &UserId,
        now: u64,
    ) -> usize {
        self.revocation_index(now)
            .owner_count(origin_node_id, token_owner)
    }

    pub fn revocation_origin(&self, token_hash: &str) -> Option<NodeId> {
        self.revocation_index(self.revocation_floor)
            .origin(token_hash)
    }

    pub fn revocation_owner(&self, token_hash: &str) -> Option<UserId> {
        self.revocation_index(self.revocation_floor)
            .owner(token_hash)
    }

    fn apply_revocation_full(
        &mut self,
        event: &AdminDocumentEvent,
        token_hash: &str,
        expires_at: u64,
        token_owner: UserId,
    ) -> AdminDocumentApplyStatus {
        let retained =
            self.canonicalize_revocation(token_hash, Some((expires_at, token_owner, event.dot())));
        if retained == Some(event.dot()) {
            AdminDocumentApplyStatus::Applied
        } else {
            AdminDocumentApplyStatus::Redundant
        }
    }

    fn refresh_revocation_expiry(&mut self) {
        self.revocation_next_expiry = self.revocation_index(self.revocation_floor).next_expiry();
    }

    fn canonicalize_revocation(
        &mut self,
        token_hash: &str,
        candidate: Option<(u64, UserId, AdminDocumentDot)>,
    ) -> Option<AdminDocumentDot> {
        let group = self
            .revocation_index(self.revocation_floor)
            .groups
            .remove(token_hash)
            .unwrap_or_default();
        let winner = self.canonicalize_group(token_hash, group, candidate);
        let Some(winner) = winner else {
            return None;
        };
        Some(winner.dot)
    }

    fn canonicalize_group(
        &mut self,
        token_hash: &str,
        mut group: RevocationGroup,
        candidate: Option<(u64, UserId, AdminDocumentDot)>,
    ) -> Option<RevocationCandidate> {
        if let Some((expires_at, token_owner, dot)) = candidate {
            let path = revoked_token_path(token_hash, expires_at, &token_owner);
            group.paths.insert(path.clone());
            group.event_ids.insert(dot.event_id);
            group.candidates.push(RevocationCandidate {
                path,
                expires_at,
                token_owner,
                dot,
            });
        }
        let winner = group
            .candidates
            .iter()
            .max_by(|left, right| candidate_cmp(left, right))
            .cloned();
        let Some(winner) = winner.clone() else {
            self.remove_revocation_group(&group);
            return None;
        };
        let unchanged = group.paths.len() == 1
            && group.paths.contains(&winner.path)
            && self
                .user_subject_ids
                .get(&winner.path)
                .is_some_and(|version| {
                    value_matches(version, winner.expires_at) && version.dot == winner.dot
                })
            && !self.equivalent_value_dots.contains_key(&winner.path)
            && !self.conflicts.contains_key(&winner.path);
        if !unchanged {
            self.remove_revocation_group(&group);
            self.user_subject_ids.insert(
                winner.path.clone(),
                AdminDocumentAttributeVersion {
                    value: Some(winner.expires_at.to_string()),
                    dot: winner.dot,
                },
            );
        }
        self.applied_event_ids.insert(winner.dot.event_id);
        Some(winner)
    }

    fn remove_revocation_group(&mut self, group: &RevocationGroup) {
        for event_id in &group.event_ids {
            self.applied_event_ids.remove(event_id);
        }
        for path in &group.paths {
            self.user_subject_ids.remove(path);
            self.equivalent_value_dots.remove(path);
            self.conflicts.remove(path);
        }
    }

    pub fn materialized_band_pools(&self) -> BTreeMap<Ulid, BandPool> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let pool_id = band_pool_id(path)?;
                let pool = version.value.as_deref().and_then(parse_band_pool)?;

                (pool.pool_id == pool_id).then_some((pool_id, pool))
            })
            .collect()
    }

    fn apply_user_name(&mut self, event: &AdminDocumentEvent, name: &str) {
        self.user_name = self.reduce_value(
            event,
            USER_NAME_PATH,
            self.user_name.clone(),
            Some(name.to_string()),
        );
    }

    fn apply_user_subject_id(
        &mut self,
        event: &AdminDocumentEvent,
        subject_id: &str,
        value: Option<String>,
    ) {
        let path = user_subject_id_path(subject_id);
        let current = self.user_subject_ids.get(subject_id).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids
                    .insert(subject_id.to_string(), version);
            }
            None => {
                self.user_subject_ids.remove(subject_id);
            }
        }
    }

    fn apply_user_attribute(
        &mut self,
        event: &AdminDocumentEvent,
        key: &str,
        value: Option<String>,
    ) {
        let path = user_attribute_path(key);
        let current = self.user_attributes.get(key).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_attributes.insert(key.to_string(), version);
            }
            None => {
                self.user_attributes.remove(key);
            }
        }
    }

    fn apply_group_created(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
        display_name: &str,
        owner: &UserId,
    ) {
        self.apply_group_field(
            event,
            GROUP_DISPLAY_NAME_PATH,
            Some(display_name.to_string()),
        );
        self.apply_group_field(event, GROUP_REALM_ID_PATH, Some(realm_id.to_string()));
        self.apply_group_field(event, GROUP_OWNER_PATH, Some(owner.to_string()));
    }

    fn apply_group_field(&mut self, event: &AdminDocumentEvent, path: &str, value: Option<String>) {
        let current = self.user_subject_ids.get(path).cloned();

        match self.reduce_value(event, path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path.to_string(), version);
            }
            None => {
                self.user_subject_ids.remove(path);
            }
        }
    }

    fn apply_group_role(&mut self, event: &AdminDocumentEvent, role_id: &RoleId, value: String) {
        let path = group_role_path(role_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_role_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_group_role_removed(&mut self, event: &AdminDocumentEvent, role_id: &RoleId) {
        let path = group_role_path(role_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, None) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_group_role_user_assignment(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        user_id: &UserId,
        value: Option<String>,
    ) {
        let path = group_role_user_assignment_path(role_id, user_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_realm_role(&mut self, event: &AdminDocumentEvent, role_id: &RoleId, value: String) {
        let path = realm_role_path(role_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_role_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_realm_role_user_assignment(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        user_id: &UserId,
        value: Option<String>,
    ) {
        let path = realm_role_user_assignment_path(role_id, user_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_realm_config_node(
        &mut self,
        event: &AdminDocumentEvent,
        node_id: &NodeId,
        kind: &RealmNodeKind,
    ) {
        let path = realm_config_node_path(node_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, Some(realm_node_kind_value(kind))) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_realm_config_oidc_provider(
        &mut self,
        event: &AdminDocumentEvent,
        provider_id: &str,
        value: Option<String>,
    ) {
        let path = realm_config_oidc_provider_path(provider_id);
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_realm_config_settings(
        &mut self,
        event: &AdminDocumentEvent,
        metadata_replication: &MetadataReplicationConfig,
        discovery: &RealmDiscoveryConfig,
    ) {
        self.apply_realm_config_setting(
            event,
            REALM_CONFIG_METADATA_REPLICATION_PATH,
            metadata_replication_value(metadata_replication),
        );
        self.apply_realm_config_setting(
            event,
            REALM_CONFIG_DISCOVERY_PATH,
            realm_discovery_value(discovery),
        );
    }

    fn apply_realm_config_setting(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        value: String,
    ) {
        let current = self.user_subject_ids.get(path).cloned();

        match self.reduce_value(event, path, current, Some(value)) {
            Some(version) => {
                self.user_subject_ids.insert(path.to_string(), version);
            }
            None => {
                self.user_subject_ids.remove(path);
            }
        }
    }

    fn apply_realm_config_placement_field(
        &mut self,
        event: &AdminDocumentEvent,
        path: String,
        value: Option<String>,
    ) {
        let current = self.user_subject_ids.get(&path).cloned();

        match self.reduce_value(event, &path, current, value) {
            Some(version) => {
                self.user_subject_ids.insert(path, version);
            }
            None => {
                self.user_subject_ids.remove(&path);
            }
        }
    }

    fn apply_placement_binding(&mut self, event: &AdminDocumentEvent, binding: &PlacementBinding) {
        self.apply_immutable_value(
            event,
            placement_binding_path(binding.handle),
            placement_binding_value(binding),
        );
    }

    fn apply_handle_range(&mut self, event: &AdminDocumentEvent, range: &HandleRange) {
        // Like bindings, divergent values for one id fail closed. Distinct-id
        // overlap is derived later by `HandleRangeDirectory::from_ranges`.
        self.apply_immutable_value(
            event,
            handle_range_path(range.range_id),
            handle_range_value(range),
        );
    }

    fn apply_band_pool(&mut self, event: &AdminDocumentEvent, pool: &BandPool) {
        self.apply_immutable_value(event, band_pool_path(pool.pool_id), band_pool_value(pool));
    }

    /// Append-only path: a divergent value for an existing path fails closed as
    /// a conflict instead of selecting a winner.
    fn apply_immutable_value(&mut self, event: &AdminDocumentEvent, path: String, value: String) {
        let value = Some(value);
        if self.conflicts.contains_key(&path) {
            self.record_conflict_value(&path, value, event.dot());
            return;
        }

        let Some(current) = self.user_subject_ids.get(&path).cloned() else {
            let version = self.version_with_dots(&path, value, BTreeSet::from([event.dot()]));
            self.user_subject_ids.insert(path, version);
            return;
        };
        let mut dots = self.take_version_dots(&path, &current);
        if current.value != value {
            for dot in dots {
                self.record_conflict_value(&path, current.value.clone(), dot);
            }
            self.record_conflict_value(&path, value, event.dot());
            self.user_subject_ids.remove(&path);
            return;
        }

        dots.insert(event.dot());
        let version = self.version_with_dots(&path, value, dots);
        self.user_subject_ids.insert(path, version);
    }

    fn reduce_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        current: Option<AdminDocumentAttributeVersion>,
        value: Option<String>,
    ) -> Option<AdminDocumentAttributeVersion> {
        if self.event_is_stale_for_path(event, path) {
            return current;
        }
        self.remove_conflict_values_superseded_by(event, path);

        if self.conflicts.contains_key(path) {
            self.record_conflict_value(path, value.clone(), event.dot());
            let equal_values = self.conflicts.get(path).is_some_and(|conflict| {
                conflict
                    .values
                    .iter()
                    .all(|candidate| candidate.value == value)
            });
            if equal_values {
                let dots = self
                    .conflicts
                    .remove(path)
                    .into_iter()
                    .flat_map(|conflict| conflict.values)
                    .map(|candidate| candidate.dot)
                    .collect();
                return Some(self.version_with_dots(path, value, dots));
            }
            return None;
        }

        let Some(current) = current else {
            return Some(self.version_with_dots(path, value, BTreeSet::from([event.dot()])));
        };
        let mut unobserved_dots = self.take_version_dots(path, &current);
        unobserved_dots.retain(|dot| !event_observes_dot(event, dot));
        if unobserved_dots.is_empty() {
            return Some(self.version_with_dots(path, value, BTreeSet::from([event.dot()])));
        }

        if current.value != value {
            for dot in unobserved_dots {
                self.record_conflict_value(path, current.value.clone(), dot);
            }
            self.record_conflict_value(path, value, event.dot());
            return None;
        }

        unobserved_dots.insert(event.dot());
        Some(self.version_with_dots(path, value, unobserved_dots))
    }

    fn reduce_role_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        current: Option<AdminDocumentAttributeVersion>,
        value: String,
    ) -> Option<AdminDocumentAttributeVersion> {
        self.reduce_value(event, path, current, Some(value))
    }

    fn event_is_stale_for_path(&self, event: &AdminDocumentEvent, path: &str) -> bool {
        let same_origin_at_or_after = |dot: &AdminDocumentDot| {
            dot.origin_node_id == event.origin_node_id && dot.origin_seq >= event.origin_seq
        };

        self.version_for_path(path)
            .is_some_and(|version| same_origin_at_or_after(&version.dot))
            || self
                .equivalent_value_dots
                .get(path)
                .is_some_and(|dots| dots.iter().any(same_origin_at_or_after))
            || self.conflicts.get(path).is_some_and(|conflict| {
                conflict
                    .values
                    .iter()
                    .any(|value| same_origin_at_or_after(&value.dot))
            })
    }

    fn version_for_path(&self, path: &str) -> Option<&AdminDocumentAttributeVersion> {
        if path == USER_NAME_PATH {
            return self.user_name.as_ref();
        }
        if let Some(key) = path.strip_prefix("user.attributes.") {
            return self.user_attributes.get(key);
        }
        if let Some(subject_id) = path.strip_prefix("user.subject_ids.") {
            return self.user_subject_ids.get(subject_id);
        }
        self.user_subject_ids.get(path)
    }

    fn remove_conflict_values_superseded_by(&mut self, event: &AdminDocumentEvent, path: &str) {
        let should_remove_conflict = self.conflicts.get_mut(path).is_some_and(|conflict| {
            conflict
                .values
                .retain(|value| !event_observes_dot(event, &value.dot));
            conflict.values.is_empty()
        });
        if should_remove_conflict {
            self.conflicts.remove(path);
        }
    }

    fn take_version_dots(
        &mut self,
        path: &str,
        version: &AdminDocumentAttributeVersion,
    ) -> BTreeSet<AdminDocumentDot> {
        let mut dots = self.equivalent_value_dots.remove(path).unwrap_or_default();
        dots.insert(version.dot);
        dots
    }

    fn version_with_dots(
        &mut self,
        path: &str,
        value: Option<String>,
        mut dots: BTreeSet<AdminDocumentDot>,
    ) -> AdminDocumentAttributeVersion {
        let dot = dots.pop_first().expect("admin value has a causal dot");
        if dots.is_empty() {
            self.equivalent_value_dots.remove(path);
        } else {
            self.equivalent_value_dots.insert(path.to_string(), dots);
        }
        AdminDocumentAttributeVersion { value, dot }
    }

    fn record_conflict_value(&mut self, path: &str, value: Option<String>, dot: AdminDocumentDot) {
        let conflict =
            self.conflicts
                .entry(path.to_string())
                .or_insert_with(|| AdminDocumentConflict {
                    path: path.to_string(),
                    values: Vec::new(),
                });

        if !conflict.values.iter().any(|candidate| candidate.dot == dot) {
            conflict
                .values
                .push(AdminDocumentConflictValue { value, dot });
            conflict.values.sort_by_key(|value| value.dot);
        }
    }
}

pub const USER_NAME_PATH: &str = "user.name";
pub const GROUP_DISPLAY_NAME_PATH: &str = "group.display_name";
pub const GROUP_REALM_ID_PATH: &str = "group.realm_id";
pub const GROUP_OWNER_PATH: &str = "group.owner";
pub const GROUP_POLICIES_PATH: &str = "group.policies";
pub const REALM_CONFIG_METADATA_REPLICATION_PATH: &str =
    "realm_config.settings.metadata_replication";
pub const REALM_CONFIG_DISCOVERY_PATH: &str = "realm_config.settings.discovery";
pub const REALM_CONFIG_DESCRIPTION_PATH: &str = "realm_config.description";
pub const REALM_CONFIG_QUOTA_PATH: &str = "realm_config.quota";
pub const REALM_CONFIG_POLICIES_PATH: &str = "realm_config.request_policies";
pub const REALM_CONFIG_DEFAULT_STRATEGY_PATH: &str = "realm_config.placement.default_strategy";
pub const REALM_CONFIG_REVOKED_TOKENS_PATH: &str = "realm_config.revoked_tokens";
pub const MAX_LIVE_REVOCATIONS_PER_ORIGIN: usize = 1024;

fn event_observes_dot(event: &AdminDocumentEvent, dot: &AdminDocumentDot) -> bool {
    event.observed.observes(dot)
        || (event.origin_node_id == dot.origin_node_id && event.origin_seq > dot.origin_seq)
}

fn operation_paths(op: &AdminDocumentOperation) -> Vec<String> {
    match op {
        AdminDocumentOperation::GroupRoleAdded { role_id }
        | AdminDocumentOperation::GroupRoleCreated {
            role: AdminDocumentRoleDefinition { role_id, .. },
        }
        | AdminDocumentOperation::GroupRoleRemoved { role_id } => vec![group_role_path(role_id)],
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
            vec![group_role_user_assignment_path(role_id, user_id)]
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => vec![user_attribute_path(key)],
        AdminDocumentOperation::UserNameSet { .. } => vec![USER_NAME_PATH.to_string()],
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
            vec![user_subject_id_path(subject_id)]
        }
        AdminDocumentOperation::RealmRoleAdded { role_id }
        | AdminDocumentOperation::RealmRoleCreated {
            role: AdminDocumentRoleDefinition { role_id, .. },
        } => vec![realm_role_path(role_id)],
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
            vec![realm_role_user_assignment_path(role_id, user_id)]
        }
        AdminDocumentOperation::RealmConfigNodeEnsured { node_id, .. } => {
            vec![realm_config_node_path(node_id)]
        }
        AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider } => {
            vec![realm_config_oidc_provider_path(&provider.id)]
        }
        AdminDocumentOperation::RealmConfigOidcProviderRemoved { provider_id } => {
            vec![realm_config_oidc_provider_path(provider_id)]
        }
        AdminDocumentOperation::RealmConfigSettingsSet { .. } => vec![
            REALM_CONFIG_METADATA_REPLICATION_PATH.to_string(),
            REALM_CONFIG_DISCOVERY_PATH.to_string(),
        ],
        AdminDocumentOperation::GroupCreated { .. } => vec![
            GROUP_DISPLAY_NAME_PATH.to_string(),
            GROUP_REALM_ID_PATH.to_string(),
            GROUP_OWNER_PATH.to_string(),
        ],
        AdminDocumentOperation::RealmConfigDescriptionSet { .. } => {
            vec![REALM_CONFIG_DESCRIPTION_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigQuotaSet { .. } => {
            vec![REALM_CONFIG_QUOTA_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigPoliciesSet { .. } => {
            vec![REALM_CONFIG_POLICIES_PATH.to_string()]
        }
        AdminDocumentOperation::GroupPoliciesSet { .. } => {
            vec![GROUP_POLICIES_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry } => {
            vec![realm_config_placement_node_path(&entry.node_id)]
        }
        AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id } => {
            vec![realm_config_placement_node_path(node_id)]
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy } => {
            vec![realm_config_placement_strategy_path(&strategy.strategy_id)]
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id } => {
            vec![realm_config_placement_strategy_path(strategy_id)]
        }
        AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {
            vec![REALM_CONFIG_DEFAULT_STRATEGY_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => {
            vec![realm_config_strategy_binding_path(&binding.scope)]
        }
        AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope } => {
            vec![realm_config_strategy_binding_path(scope)]
        }
        AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => {
            vec![realm_config_placement_override_path(&record.subject)]
        }
        AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject } => {
            vec![realm_config_placement_override_path(subject)]
        }
        AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding } => {
            vec![placement_binding_path(binding.handle)]
        }
        AdminDocumentOperation::RealmConfigHandleRangeGranted { range } => {
            vec![handle_range_path(range.range_id)]
        }
        AdminDocumentOperation::RealmConfigBandPoolAssigned { pool } => {
            vec![band_pool_path(pool.pool_id)]
        }
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
        } => {
            vec![revoked_token_path(token_hash, *expires_at, token_owner)]
        }
    }
}

fn role_definition_value(role: &AdminDocumentRoleDefinition) -> String {
    serde_json::to_string(role).expect("admin document role definition serializes")
}

pub fn user_attribute_path(key: &str) -> String {
    format!("user.attributes.{key}")
}

pub fn user_subject_id_path(subject_id: &str) -> String {
    format!("user.subject_ids.{subject_id}")
}

pub fn group_role_path(role_id: &RoleId) -> String {
    format!("group.roles.{role_id}")
}

pub fn group_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("group.roles.{role_id}.assigned_users.{user_id}")
}

pub fn realm_role_path(role_id: &RoleId) -> String {
    format!("realm.roles.{role_id}")
}

pub fn realm_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("realm.roles.{role_id}.assigned_users.{user_id}")
}

pub fn realm_config_node_path(node_id: &NodeId) -> String {
    format!("realm_config.nodes.{node_id}")
}

pub fn realm_config_oidc_provider_path(provider_id: &str) -> String {
    format!("realm_config.oidc_providers.{provider_id}")
}

pub fn realm_config_placement_node_path(node_id: &NodeId) -> String {
    format!("realm_config.placement.nodes.{node_id}")
}

pub fn realm_config_placement_strategy_path(strategy_id: &Ulid) -> String {
    format!("realm_config.placement.strategies.{strategy_id}")
}

pub fn realm_config_strategy_binding_path(scope: &BindingScope) -> String {
    format!(
        "realm_config.placement.bindings.{}",
        binding_scope_key(scope)
    )
}

pub fn realm_config_placement_override_path(subject: &[u8]) -> String {
    format!("realm_config.placement.overrides.{}", hex::encode(subject))
}

pub fn placement_binding_path(handle: PlacementHandle) -> String {
    format!("realm_config.placement.placement_bindings.{}", handle.get())
}

pub fn handle_range_path(range_id: Ulid) -> String {
    format!("realm_config.placement.handle_ranges.{range_id}")
}

pub fn band_pool_path(pool_id: Ulid) -> String {
    format!("realm_config.placement.band_pools.{pool_id}")
}

pub fn revoked_token_path(token_hash: &str, expires_at: u64, token_owner: &UserId) -> String {
    format!("{REALM_CONFIG_REVOKED_TOKENS_PATH}.{token_hash}.{expires_at}.{token_owner}")
}

pub fn revoked_token_entry(path: &str) -> Option<(&str, u64, UserId)> {
    let rest = path.strip_prefix(REALM_CONFIG_REVOKED_TOKENS_PATH)?;
    let mut parts = rest.strip_prefix('.')?.split('.');
    let hash = parts.next()?;
    let expires_at = parts.next()?.parse().ok()?;
    let token_owner = UserId::from_string(parts.next()?).ok()?;
    (parts.next().is_none() && valid_token_hash(hash)).then_some((hash, expires_at, token_owner))
}

pub fn binding_scope_key(scope: &BindingScope) -> String {
    match scope {
        BindingScope::Realm => "realm".to_string(),
        BindingScope::Group(group_id) => format!("group:{group_id}"),
        BindingScope::Class(class) => match class {
            DocumentClass::Admin => "class:admin",
            DocumentClass::Group => "class:group",
            DocumentClass::User => "class:user",
            DocumentClass::Metadata => "class:metadata",
            DocumentClass::MetadataRegistry => "class:metadata_registry",
            DocumentClass::JobControl => "class:job_control",
        }
        .to_string(),
        BindingScope::MetadataPathPrefix(prefix) => format!(
            "metadata_path_prefix:{}",
            MetadataRegistryRecord::normalize_document_path(prefix)
        ),
    }
}

fn normalized_binding_scope(scope: &BindingScope) -> BindingScope {
    match scope {
        BindingScope::MetadataPathPrefix(prefix) => BindingScope::MetadataPathPrefix(
            MetadataRegistryRecord::normalize_document_path(prefix),
        ),
        BindingScope::Realm => BindingScope::Realm,
        BindingScope::Group(group_id) => BindingScope::Group(*group_id),
        BindingScope::Class(class) => BindingScope::Class(*class),
    }
}

fn normalized_strategy_binding(binding: &StrategyBinding) -> StrategyBinding {
    StrategyBinding {
        scope: normalized_binding_scope(&binding.scope),
        strategy_id: binding.strategy_id,
    }
}

fn metadata_replication_value(metadata_replication: &MetadataReplicationConfig) -> String {
    serde_json::to_string(metadata_replication)
        .expect("admin document metadata replication config serializes")
}

fn realm_discovery_value(discovery: &RealmDiscoveryConfig) -> String {
    serde_json::to_string(discovery).expect("admin document realm discovery config serializes")
}

fn policies_value(policies: &[crate::request_policy::RequestPolicy]) -> String {
    serde_json::to_string(policies).expect("admin document policies serialize")
}

fn policies_from_value(value: &str) -> Option<Vec<crate::request_policy::RequestPolicy>> {
    serde_json::from_str(value).ok()
}

fn quota_value(quota: &QuotaConfig) -> String {
    serde_json::to_string(&supported_quota(quota)).expect("admin document quota config serializes")
}

fn supported_quota(quota: &QuotaConfig) -> QuotaConfig {
    let mut quota = quota.clone();
    quota.max_devices_per_user = None;
    quota.group_overrides.sort_by_key(|over| over.group_id);
    quota
        .user_group_cap_overrides
        .sort_by_key(|over| over.user_id);
    quota
}

fn placement_entry_value(entry: &NodePlacementEntry) -> String {
    serde_json::to_string(entry).expect("admin document placement entry serializes")
}

fn placement_strategy_value(strategy: &PlacementStrategy) -> String {
    serde_json::to_string(strategy).expect("admin document placement strategy serializes")
}

fn strategy_binding_value(binding: &StrategyBinding) -> String {
    serde_json::to_string(&normalized_strategy_binding(binding))
        .expect("admin document strategy binding serializes")
}

fn placement_override_value(record: &PlacementOverride) -> String {
    serde_json::to_string(record).expect("admin document placement override serializes")
}

fn placement_binding_value(binding: &PlacementBinding) -> String {
    serde_json::to_string(binding).expect("admin document placement binding serializes")
}

fn handle_range_value(range: &HandleRange) -> String {
    // A handle range carries no provenance to normalize away: the whole record
    // (id, owner, bounds) is the identity compared for same-key divergence.
    serde_json::to_string(range).expect("admin document handle range serializes")
}

fn band_pool_value(pool: &BandPool) -> String {
    // The whole record (id, lineage, owner, bounds) is the identity compared
    // for same-key divergence.
    serde_json::to_string(pool).expect("admin document band pool serializes")
}

fn oidc_provider_value(provider: &OidcProviderConfig) -> String {
    serde_json::to_string(provider).expect("admin document OIDC provider config serializes")
}

fn realm_node_kind_value(kind: &RealmNodeKind) -> String {
    match kind {
        RealmNodeKind::Management => "management",
        RealmNodeKind::Server => "server",
        RealmNodeKind::Local => "local",
        RealmNodeKind::User => "user",
    }
    .to_string()
}

pub fn group_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("group.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

pub fn group_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("group.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;

    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

fn group_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    group_role_user_assignment_from_path(path).map(|(role_id, _)| role_id)
}

pub fn realm_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("realm.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

pub fn realm_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("realm.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;

    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

fn realm_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    realm_role_user_assignment_from_path(path).map(|(role_id, _)| role_id)
}

pub fn realm_config_node_id_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.nodes.")?;
    NodeId::from_str(node_id).ok()
}

pub fn realm_config_oidc_provider_id_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.oidc_providers.")
}

pub fn realm_config_placement_node_id_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.placement.nodes.")?;
    NodeId::from_str(node_id).ok()
}

pub fn realm_config_placement_strategy_id_from_path(path: &str) -> Option<Ulid> {
    let strategy_id = path.strip_prefix("realm_config.placement.strategies.")?;
    Ulid::from_string(strategy_id).ok()
}

pub fn realm_config_strategy_binding_scope_key_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.placement.bindings.")
}

pub fn realm_config_placement_override_subject_key_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.placement.overrides.")
}

pub fn placement_binding_handle(path: &str) -> Option<PlacementHandle> {
    let handle = path.strip_prefix("realm_config.placement.placement_bindings.")?;
    PlacementHandle::new(handle.parse().ok()?).ok()
}

pub fn band_pool_id(path: &str) -> Option<Ulid> {
    let pool_id = path.strip_prefix("realm_config.placement.band_pools.")?;
    Ulid::from_string(pool_id).ok()
}

pub fn handle_range_id(path: &str) -> Option<Ulid> {
    let range_id = path.strip_prefix("realm_config.placement.handle_ranges.")?;
    Ulid::from_string(range_id).ok()
}

fn oidc_provider_from_value(value: &str) -> Option<OidcProviderConfig> {
    serde_json::from_str(value).ok()
}

fn metadata_replication_from_value(value: &str) -> Option<MetadataReplicationConfig> {
    serde_json::from_str(value).ok()
}

fn realm_discovery_from_value(value: &str) -> Option<RealmDiscoveryConfig> {
    serde_json::from_str(value).ok()
}

fn quota_from_value(value: &str) -> Option<QuotaConfig> {
    serde_json::from_str(value)
        .ok()
        .map(|quota| supported_quota(&quota))
}

fn placement_entry_from_value(value: &str) -> Option<NodePlacementEntry> {
    serde_json::from_str(value).ok()
}

fn placement_strategy_from_value(value: &str) -> Option<PlacementStrategy> {
    serde_json::from_str(value).ok()
}

fn strategy_binding_from_value(value: &str) -> Option<StrategyBinding> {
    serde_json::from_str(value).ok()
}

fn placement_override_from_value(value: &str) -> Option<PlacementOverride> {
    serde_json::from_str(value).ok()
}

fn parse_placement_binding(value: &str) -> Option<PlacementBinding> {
    serde_json::from_str(value).ok()
}

fn parse_handle_range(value: &str) -> Option<HandleRange> {
    serde_json::from_str(value).ok()
}

fn parse_band_pool(value: &str) -> Option<BandPool> {
    serde_json::from_str(value).ok()
}

fn realm_node_kind_from_value(value: &str) -> Option<RealmNodeKind> {
    match value {
        "management" => Some(RealmNodeKind::Management),
        "server" => Some(RealmNodeKind::Server),
        "local" => Some(RealmNodeKind::Local),
        "user" => Some(RealmNodeKind::User),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdminDocumentApplyStatus, AdminDocumentAttributeVersion, AdminDocumentConflict,
        AdminDocumentConflictValue, AdminDocumentReducerError, AdminDocumentReducerState,
        GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DEFAULT_STRATEGY_PATH,
        REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
        REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
        binding_scope_key, group_role_id_from_path, group_role_path,
        group_role_user_assignment_from_path, group_role_user_assignment_path, handle_range_path,
        metadata_replication_value, oidc_provider_value,
        overlay_realm_config_placement_reducer_materialization, placement_binding_handle,
        placement_binding_path, realm_config_node_id_from_path, realm_config_node_path,
        realm_config_oidc_provider_id_from_path, realm_config_oidc_provider_path,
        realm_config_placement_node_id_from_path, realm_config_placement_node_path,
        realm_config_placement_strategy_id_from_path, realm_config_placement_strategy_path,
        realm_config_strategy_binding_path, realm_config_strategy_binding_scope_key_from_path,
        realm_discovery_value, realm_role_id_from_path, realm_role_path,
        realm_role_user_assignment_from_path, realm_role_user_assignment_path,
        role_definition_value, user_attribute_path, user_subject_id_path,
    };
    use crate::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use crate::auth::REVOCATION_GRACE_SECS;
    use crate::structs::{
        Actor, AffinityEffect, AffinityRule, BindingError, BindingScope, DocumentClass,
        FIRST_GRANTABLE_HANDLE, GroupQuotaOverride, HandleRange, KIND_LABEL_KEY, LabelMatch,
        MAX_PLACEMENT_SHARD_COUNT, MetadataReplicationConfig, NodePlacementEntry,
        OidcProviderConfig, Permission, PlacementBinding, PlacementOverride, PlacementScope,
        PlacementStrategy, QuotaConfig, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
        RealmNodeKind, STORAGE_CLASS_LABEL_PREFIX, StrategyBinding, UserGroupCapOverride,
    };
    use crate::structured_id::PlacementHandle;
    use crate::types::{GroupId, RoleId};
    use crate::user_update_validation::UserAttributeValidationError;
    use crate::{NodeId, UserId};
    use std::collections::{BTreeMap, BTreeSet};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm_id_with_seed(seed: u8) -> RealmId {
        RealmId::from_bytes([seed; 32])
    }

    fn realm_id() -> RealmId {
        realm_id_with_seed(9)
    }

    fn group_id() -> GroupId {
        Ulid::from_bytes([7u8; 16])
    }

    fn role_id(seed: u8) -> RoleId {
        Ulid::from_bytes([seed; 16])
    }

    fn role_definition(role_id: RoleId, name: &str) -> AdminDocumentRoleDefinition {
        AdminDocumentRoleDefinition {
            role_id,
            name: name.to_string(),
            permissions: BTreeMap::from([
                ("/dataset/**".to_string(), Permission::READ),
                ("/project/admin/**".to_string(), Permission::WRITE),
            ]),
        }
    }

    fn oidc_provider(id: &str, issuer_suffix: &str) -> OidcProviderConfig {
        OidcProviderConfig {
            id: id.to_string(),
            issuer: format!("https://issuer.example/{issuer_suffix}"),
            audience: "aruna".to_string(),
            discovery_url: format!(
                "https://issuer.example/{issuer_suffix}/.well-known/openid-configuration"
            ),
        }
    }

    fn user_id_with_seed(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), realm_id())
    }

    fn user_id() -> UserId {
        user_id_with_seed(8)
    }

    fn actor(origin_node_id: NodeId) -> Actor {
        Actor {
            node_id: origin_node_id,
            user_id: user_id(),
            realm_id: realm_id(),
        }
    }

    fn user_state() -> AdminDocumentReducerState {
        AdminDocumentReducerState::new(AdminDocumentTarget::User { user_id: user_id() })
    }

    fn group_state() -> AdminDocumentReducerState {
        AdminDocumentReducerState::new(AdminDocumentTarget::Group {
            group_id: group_id(),
        })
    }

    fn realm_state() -> AdminDocumentReducerState {
        AdminDocumentReducerState::new(AdminDocumentTarget::Realm {
            realm_id: realm_id(),
        })
    }

    fn realm_config_state() -> AdminDocumentReducerState {
        AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig {
            realm_id: realm_id(),
        })
    }

    fn event(
        event_seed: u8,
        origin_node_id: NodeId,
        origin_seq: u64,
        observed: AdminDocumentClock,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::User { user_id: user_id() },
            origin_node_id,
            origin_seq,
            observed,
            actor: actor(origin_node_id),
            op,
        }
    }

    fn group_event(
        event_seed: u8,
        origin_node_id: NodeId,
        origin_seq: u64,
        observed: AdminDocumentClock,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::Group {
                group_id: group_id(),
            },
            origin_node_id,
            origin_seq,
            observed,
            actor: actor(origin_node_id),
            op,
        }
    }

    fn realm_event(
        event_seed: u8,
        origin_node_id: NodeId,
        origin_seq: u64,
        observed: AdminDocumentClock,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::Realm {
                realm_id: realm_id(),
            },
            origin_node_id,
            origin_seq,
            observed,
            actor: actor(origin_node_id),
            op,
        }
    }

    fn realm_config_event(
        event_seed: u8,
        origin_node_id: NodeId,
        origin_seq: u64,
        observed: AdminDocumentClock,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        AdminDocumentEvent {
            event_id: Ulid::from_bytes([event_seed; 16]),
            target: AdminDocumentTarget::RealmConfig {
                realm_id: realm_id(),
            },
            origin_node_id,
            origin_seq,
            observed,
            actor: actor(origin_node_id),
            op,
        }
    }

    fn set_attr(event_seed: u8, origin_seed: u8, key: &str, value: &str) -> AdminDocumentEvent {
        event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: key.to_string(),
                value: value.to_string(),
            },
        )
    }

    fn set_name(event_seed: u8, origin_seed: u8, name: &str) -> AdminDocumentEvent {
        event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserNameSet {
                name: name.to_string(),
            },
        )
    }

    fn add_subject(event_seed: u8, origin_seed: u8, subject_id: &str) -> AdminDocumentEvent {
        event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserSubjectIdAdded {
                subject_id: subject_id.to_string(),
            },
        )
    }

    fn remove_subject(event_seed: u8, origin_seed: u8, subject_id: &str) -> AdminDocumentEvent {
        event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserSubjectIdRemoved {
                subject_id: subject_id.to_string(),
            },
        )
    }

    fn create_group(
        event_seed: u8,
        origin_seed: u8,
        display_name: &str,
        realm_id: RealmId,
    ) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: display_name.to_string(),
                owner: user_id_with_seed(5),
            },
        )
    }

    fn add_group_role(event_seed: u8, origin_seed: u8, role_id: RoleId) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleAdded { role_id },
        )
    }

    fn create_group_role(
        event_seed: u8,
        origin_seed: u8,
        role: AdminDocumentRoleDefinition,
    ) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleCreated { role },
        )
    }

    fn remove_group_role(event_seed: u8, origin_seed: u8, role_id: RoleId) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleRemoved { role_id },
        )
    }

    fn assign_group_role_user(
        event_seed: u8,
        origin_seed: u8,
        role_id: RoleId,
        user_id: UserId,
    ) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
        )
    }

    fn remove_group_role_user_assignment(
        event_seed: u8,
        origin_seed: u8,
        role_id: RoleId,
        user_id: UserId,
    ) -> AdminDocumentEvent {
        group_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
        )
    }

    fn add_realm_role(event_seed: u8, origin_seed: u8, role_id: RoleId) -> AdminDocumentEvent {
        realm_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmRoleAdded { role_id },
        )
    }

    fn create_realm_role(
        event_seed: u8,
        origin_seed: u8,
        role: AdminDocumentRoleDefinition,
    ) -> AdminDocumentEvent {
        realm_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmRoleCreated { role },
        )
    }

    fn assign_realm_role_user(
        event_seed: u8,
        origin_seed: u8,
        role_id: RoleId,
        user_id: UserId,
    ) -> AdminDocumentEvent {
        realm_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id },
        )
    }

    fn remove_realm_role_user_assignment(
        event_seed: u8,
        origin_seed: u8,
        role_id: RoleId,
        user_id: UserId,
    ) -> AdminDocumentEvent {
        realm_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id },
        )
    }

    fn ensure_realm_config_node(
        event_seed: u8,
        origin_seed: u8,
        node_id: NodeId,
        kind: RealmNodeKind,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
        )
    }

    fn upsert_oidc_provider(
        event_seed: u8,
        origin_seed: u8,
        provider: OidcProviderConfig,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
        )
    }

    fn set_realm_config_settings(
        event_seed: u8,
        origin_seed: u8,
        metadata_replication: MetadataReplicationConfig,
        discovery: RealmDiscoveryConfig,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication,
                discovery,
            },
        )
    }

    fn set_realm_config_description(
        event_seed: u8,
        origin_seed: u8,
        description: &str,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigDescriptionSet {
                description: description.to_string(),
            },
        )
    }

    #[test]
    fn apply_operation_uses_next_origin_sequence_and_applies_event() {
        let mut state = user_state();
        let actor = actor(node(1));

        let first = state
            .apply_operation(
                &actor,
                AdminDocumentOperation::UserNameSet {
                    name: "Alice".to_string(),
                },
            )
            .unwrap();
        let second = state
            .apply_operation(
                &actor,
                AdminDocumentOperation::UserAttributeSet {
                    key: "department".to_string(),
                    value: "biology".to_string(),
                },
            )
            .unwrap();

        assert_eq!(first.origin_seq, 1);
        assert_eq!(first.observed.sequence_for(&actor.node_id), 0);
        assert_eq!(second.origin_seq, 2);
        assert_eq!(second.observed.sequence_for(&actor.node_id), 1);
        assert!(second.observed.observes(&first.dot()));
        assert_eq!(state.clock.sequence_for(&actor.node_id), 2);
        assert_eq!(state.materialized_user_name().as_deref(), Some("Alice"));
        assert_eq!(
            state.materialized_user_attributes().get("department"),
            Some(&"biology".to_string())
        );
    }

    #[test]
    fn admin_document_paths_preserve_strings_and_round_trip() {
        let role_id = role_id(4);
        let user_id = user_id_with_seed(5);
        let node_id = node(6);

        assert_eq!(USER_NAME_PATH, "user.name");
        assert_eq!(GROUP_DISPLAY_NAME_PATH, "group.display_name");
        assert_eq!(GROUP_REALM_ID_PATH, "group.realm_id");
        assert_eq!(
            REALM_CONFIG_METADATA_REPLICATION_PATH,
            "realm_config.settings.metadata_replication"
        );
        assert_eq!(
            REALM_CONFIG_DISCOVERY_PATH,
            "realm_config.settings.discovery"
        );
        assert_eq!(REALM_CONFIG_DESCRIPTION_PATH, "realm_config.description");
        assert_eq!(
            user_attribute_path("department"),
            "user.attributes.department"
        );
        assert_eq!(
            user_subject_id_path("subject-1"),
            "user.subject_ids.subject-1"
        );

        let group_role = group_role_path(&role_id);
        let group_assignment = group_role_user_assignment_path(&role_id, &user_id);
        assert_eq!(group_role, format!("group.roles.{role_id}"));
        assert_eq!(
            group_assignment,
            format!("group.roles.{role_id}.assigned_users.{user_id}")
        );
        assert_eq!(group_role_id_from_path(&group_role), Some(role_id));
        assert_eq!(group_role_id_from_path(&group_assignment), None);
        assert_eq!(
            group_role_user_assignment_from_path(&group_assignment),
            Some((role_id, user_id))
        );

        let realm_role = realm_role_path(&role_id);
        let realm_assignment = realm_role_user_assignment_path(&role_id, &user_id);
        assert_eq!(realm_role, format!("realm.roles.{role_id}"));
        assert_eq!(
            realm_assignment,
            format!("realm.roles.{role_id}.assigned_users.{user_id}")
        );
        assert_eq!(realm_role_id_from_path(&realm_role), Some(role_id));
        assert_eq!(realm_role_id_from_path(&realm_assignment), None);
        assert_eq!(
            realm_role_user_assignment_from_path(&realm_assignment),
            Some((role_id, user_id))
        );

        let node_path = realm_config_node_path(&node_id);
        assert_eq!(node_path, format!("realm_config.nodes.{node_id}"));
        assert_eq!(realm_config_node_id_from_path(&node_path), Some(node_id));
        assert_eq!(
            realm_config_oidc_provider_path("default"),
            "realm_config.oidc_providers.default"
        );
        assert_eq!(
            realm_config_oidc_provider_id_from_path("realm_config.oidc_providers.default"),
            Some("default")
        );

        assert_eq!(
            group_role_user_assignment_from_path("group.roles.invalid"),
            None
        );
        assert_eq!(
            realm_role_user_assignment_from_path("realm.roles.invalid"),
            None
        );
        assert_eq!(
            realm_config_node_id_from_path("realm_config.nodes.invalid"),
            None
        );
        assert_eq!(
            realm_config_oidc_provider_id_from_path("unknown.path"),
            None
        );
    }

    #[test]
    fn user_disjoint_attribute_updates_merge() {
        let mut state = user_state();

        assert_eq!(
            state.apply(&set_attr(1, 1, "orcid", "0000-0002-1825-0097")),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            state.apply(&set_attr(2, 2, "department", "biology")),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        assert_eq!(
            state.materialized_user_attributes(),
            BTreeMap::from([
                ("department".to_string(), "biology".to_string()),
                ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
            ])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn invalid_user_attribute_key_is_rejected_without_state_change() {
        let mut state = user_state();
        let before = state.clone();

        assert_eq!(
            state.apply(&set_attr(1, 1, "display name", "biology")),
            Err(AdminDocumentReducerError::InvalidUserAttribute(
                UserAttributeValidationError::InvalidKey("display name".to_string())
            ))
        );
        assert_eq!(state, before);
    }

    #[test]
    fn invalid_user_attribute_value_is_rejected_without_state_change() {
        let mut state = user_state();
        let before = state.clone();

        assert_eq!(
            state.apply(&set_attr(1, 1, "department", "bio\nmedicine")),
            Err(AdminDocumentReducerError::InvalidUserAttribute(
                UserAttributeValidationError::InvalidValue("department".to_string())
            ))
        );
        assert_eq!(state, before);
    }

    #[test]
    fn same_user_attribute_conflict_is_recorded() {
        let mut state = user_state();

        state
            .apply(&set_attr(1, 1, "department", "physics"))
            .unwrap();
        state
            .apply(&set_attr(2, 2, "department", "biology"))
            .unwrap();

        assert!(
            !state
                .materialized_user_attributes()
                .contains_key("department")
        );
        let conflict = state
            .conflicts
            .get("user.attributes.department")
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("physics"))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("biology"))
        );
    }

    #[test]
    fn disjoint_subject_additions_merge() {
        let mut state = user_state();

        state.apply(&add_subject(1, 1, "subject-1")).unwrap();
        state.apply(&add_subject(2, 2, "subject-2")).unwrap();

        assert_eq!(
            state.materialized_user_subject_ids(),
            BTreeSet::from(["subject-1".to_string(), "subject-2".to_string()])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn subject_add_remove_conflict_fails_closed_and_materializes_absent() {
        let mut state = user_state();

        state.apply(&add_subject(1, 1, "subject-1")).unwrap();
        state.apply(&remove_subject(2, 2, "subject-1")).unwrap();

        assert!(!state.materialized_user_subject_ids().contains("subject-1"));
        let conflict = state
            .conflicts
            .get("user.subject_ids.subject-1")
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("subject-1"))
        );
        assert!(conflict.values.iter().any(|value| value.value.is_none()));
    }

    #[test]
    fn duplicate_event_id_is_idempotent() {
        let mut state = user_state();
        let event = set_attr(1, 1, "department", "biology");

        assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Applied));
        let applied_once = state.clone();

        assert_eq!(state.apply(&event), Ok(AdminDocumentApplyStatus::Duplicate));
        assert_eq!(state, applied_once);
    }

    #[test]
    fn same_origin_out_of_order_disjoint_updates_converge() {
        let origin = node(1);
        let newer = event(
            2,
            origin,
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );
        let stale = event(
            1,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "orcid".to_string(),
                value: "0000-0002-1825-0097".to_string(),
            },
        );

        let mut newer_first = user_state();
        assert_eq!(
            newer_first.apply(&newer),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            newer_first.apply(&stale),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        let mut older_first = user_state();
        older_first.apply(&stale).unwrap();
        older_first.apply(&newer).unwrap();

        assert_eq!(newer_first, older_first);
        assert_eq!(
            newer_first.materialized_user_attributes(),
            BTreeMap::from([
                ("department".to_string(), "biology".to_string()),
                ("orcid".to_string(), "0000-0002-1825-0097".to_string()),
            ])
        );
        assert_eq!(newer_first.clock.sequence_for(&origin), 2);
    }

    #[test]
    fn same_origin_out_of_order_same_field_is_stale_and_duplicate_replay_is_idempotent() {
        let origin = node(1);
        let older = event(
            1,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "physics".to_string(),
            },
        );
        let newer = event(
            2,
            origin,
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );

        let mut newer_first = user_state();
        assert_eq!(
            newer_first.apply(&newer),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        let before_stale = newer_first.clone();
        assert_eq!(
            newer_first.apply(&older),
            Ok(AdminDocumentApplyStatus::StaleOriginSequence)
        );
        assert_eq!(
            newer_first.materialized_user_attributes(),
            before_stale.materialized_user_attributes()
        );
        assert!(newer_first.applied_event_ids.contains(&older.event_id));
        assert_eq!(
            newer_first.apply(&newer),
            Ok(AdminDocumentApplyStatus::Duplicate)
        );

        let mut older_first = user_state();
        older_first.apply(&older).unwrap();
        older_first.apply(&newer).unwrap();
        assert_eq!(newer_first, older_first);
        assert_eq!(
            older_first.apply(&older),
            Ok(AdminDocumentApplyStatus::Duplicate)
        );
        assert_eq!(
            newer_first
                .materialized_user_attributes()
                .get("department")
                .map(String::as_str),
            Some("biology")
        );
        assert!(newer_first.conflicts.is_empty());
    }

    #[test]
    fn newer_same_origin_value_replaces_its_older_conflict_value_in_any_order() {
        let first_origin = node(1);
        let concurrent_origin = node(2);
        let older = event(
            1,
            first_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "physics".to_string(),
            },
        );
        let concurrent = event(
            2,
            concurrent_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "chemistry".to_string(),
            },
        );
        let newer = event(
            3,
            first_origin,
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );

        let mut conflict_first = user_state();
        conflict_first.apply(&older).unwrap();
        conflict_first.apply(&concurrent).unwrap();
        conflict_first.apply(&newer).unwrap();

        let mut newer_first = user_state();
        newer_first.apply(&older).unwrap();
        newer_first.apply(&newer).unwrap();
        newer_first.apply(&concurrent).unwrap();

        assert_eq!(conflict_first, newer_first);
        let conflict = conflict_first
            .conflicts
            .get("user.attributes.department")
            .expect("newer and concurrent values conflict");
        assert_eq!(conflict.values.len(), 2);
        assert!(conflict.values.iter().all(|value| value.dot != older.dot()));
    }

    #[test]
    fn same_origin_out_of_order_multi_field_operation_is_atomically_stale() {
        let origin = node(1);
        let older = realm_config_event(
            1,
            origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: RealmDiscoveryConfig::Static {
                    endpoints: Vec::new(),
                },
            },
        );
        let newer_metadata = MetadataReplicationConfig::new(5);
        let newer_discovery = RealmDiscoveryConfig::Dynamic {
            methods: Vec::new(),
        };
        let newer = realm_config_event(
            2,
            origin,
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: newer_metadata.clone(),
                discovery: newer_discovery.clone(),
            },
        );

        let mut newer_first = realm_config_state();
        newer_first.apply(&newer).unwrap();
        assert_eq!(
            newer_first.apply(&older),
            Ok(AdminDocumentApplyStatus::StaleOriginSequence)
        );

        let mut older_first = realm_config_state();
        older_first.apply(&older).unwrap();
        older_first.apply(&newer).unwrap();

        assert_eq!(newer_first, older_first);
        assert_eq!(
            newer_first.materialized_realm_config_metadata_replication(),
            Some(newer_metadata)
        );
        assert_eq!(
            newer_first.materialized_realm_config_discovery(),
            Some(newer_discovery)
        );
        assert!(newer_first.conflicts.is_empty());
    }

    #[test]
    fn observed_sequential_user_attribute_update_replaces_prior_value() {
        let mut state = user_state();
        let first_origin = node(1);
        let first = event(
            1,
            first_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "physics".to_string(),
            },
        );
        let second = event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(first_origin, 1),
            AdminDocumentOperation::UserAttributeSet {
                key: "department".to_string(),
                value: "biology".to_string(),
            },
        );

        state.apply(&first).unwrap();
        state.apply(&second).unwrap();

        assert_eq!(
            state
                .materialized_user_attributes()
                .get("department")
                .map(String::as_str),
            Some("biology")
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn observed_name_update_replaces_prior_name() {
        let mut state = user_state();
        let first_origin = node(1);
        let first = event(
            1,
            first_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserNameSet {
                name: "Alice".to_string(),
            },
        );
        let second = event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(first_origin, 1),
            AdminDocumentOperation::UserNameSet {
                name: "Bob".to_string(),
            },
        );

        state.apply(&first).unwrap();
        state.apply(&second).unwrap();

        assert_eq!(state.materialized_user_name().as_deref(), Some("Bob"));
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_name_conflict_is_recorded() {
        let mut state = user_state();

        state.apply(&set_name(1, 1, "Alice")).unwrap();
        state.apply(&set_name(2, 2, "Bob")).unwrap();

        assert_eq!(state.materialized_user_name(), None);
        let conflict = state
            .conflicts
            .get(USER_NAME_PATH)
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("Alice"))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("Bob"))
        );
    }

    #[test]
    fn group_policies_materialize() {
        let mut state = group_state();
        let policies = vec![crate::request_policy::RequestPolicy {
            policy_id: Ulid::from_bytes([2; 16]),
            name: "no-writes".to_string(),
            kind: crate::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'write'".to_string(),
            enabled: true,
        }];
        state
            .apply_operation(
                &actor(node(1)),
                AdminDocumentOperation::GroupPoliciesSet {
                    policies: policies.clone(),
                },
            )
            .unwrap();
        assert_eq!(state.materialized_group_policies(), Some(policies));
    }

    #[test]
    fn group_created_materializes_display_name_realm_id_and_owner() {
        let mut state = group_state();
        let realm_id = realm_id();
        let owner = user_id_with_seed(5);

        state
            .apply(&create_group(1, 1, "Engineering", realm_id))
            .unwrap();

        assert_eq!(
            state.materialized_group_display_name().as_deref(),
            Some("Engineering")
        );
        assert_eq!(state.materialized_group_realm_id(), Some(realm_id));
        assert_eq!(state.materialized_group_owner(), Some(owner));
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn group_created_display_name_conflict_withholds_only_display_name() {
        let mut state = group_state();
        let realm_id = realm_id();

        state
            .apply(&create_group(1, 1, "Engineering", realm_id))
            .unwrap();
        state
            .apply(&create_group(2, 2, "Research", realm_id))
            .unwrap();

        assert_eq!(state.materialized_group_display_name(), None);
        assert_eq!(state.materialized_group_realm_id(), Some(realm_id));
        assert!(!state.conflicts.contains_key(GROUP_REALM_ID_PATH));

        let conflict = state
            .conflicts
            .get(GROUP_DISPLAY_NAME_PATH)
            .expect("display name conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("Engineering"))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("Research"))
        );
    }

    #[test]
    fn group_created_realm_id_conflict_withholds_only_realm_id() {
        let mut state = group_state();
        let first_realm_id = realm_id_with_seed(9);
        let second_realm_id = realm_id_with_seed(10);
        let first_realm_value = first_realm_id.to_string();
        let second_realm_value = second_realm_id.to_string();

        state
            .apply(&create_group(1, 1, "Engineering", first_realm_id))
            .unwrap();
        state
            .apply(&create_group(2, 2, "Engineering", second_realm_id))
            .unwrap();

        assert_eq!(
            state.materialized_group_display_name().as_deref(),
            Some("Engineering")
        );
        assert_eq!(state.materialized_group_realm_id(), None);
        assert!(!state.conflicts.contains_key(GROUP_DISPLAY_NAME_PATH));

        let conflict = state
            .conflicts
            .get(GROUP_REALM_ID_PATH)
            .expect("realm id conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(first_realm_value.as_str()))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(second_realm_value.as_str()))
        );
    }

    #[test]
    fn group_created_operation_is_rejected_for_non_group_target_without_state_change() {
        let mut state = user_state();
        let before = state.clone();
        let event = event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupCreated {
                realm_id: realm_id(),
                display_name: "Engineering".to_string(),
                owner: user_id_with_seed(5),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::UnsupportedTarget)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn group_role_and_user_assignment_materialize() {
        let mut state = group_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state.apply(&add_group_role(1, 1, role_id)).unwrap();
        state
            .apply(&assign_group_role_user(2, 2, role_id, user_id))
            .unwrap();

        assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
        assert_eq!(
            state.materialized_group_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn group_role_user_assignment_is_hidden_until_role_exists() {
        let mut state = group_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state
            .apply(&assign_group_role_user(1, 1, role_id, user_id))
            .unwrap();

        assert!(state.materialized_group_roles().is_empty());
        assert!(state.materialized_group_role_user_assignments().is_empty());

        state.apply(&add_group_role(2, 2, role_id)).unwrap();

        assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
        assert_eq!(
            state.materialized_group_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
        );
    }

    #[test]
    fn group_role_body_creation_materializes_role_id_and_records_body() {
        let mut state = group_state();
        let role_id = role_id(3);
        let role = role_definition(role_id, "Group admin");
        let expected_value = role_definition_value(&role);

        state.apply(&create_group_role(1, 1, role)).unwrap();

        assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
        assert_eq!(
            state
                .user_subject_ids
                .get(&format!("group.roles.{role_id}"))
                .and_then(|version| version.value.as_deref()),
            Some(expected_value.as_str())
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn observed_group_role_removal_clears_role_and_assignments() {
        let mut state = group_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state.apply(&add_group_role(1, 1, role_id)).unwrap();
        state
            .apply(&assign_group_role_user(2, 2, role_id, user_id))
            .unwrap();
        state
            .apply_operation(
                &actor(node(3)),
                AdminDocumentOperation::GroupRoleRemoved { role_id },
            )
            .unwrap();

        assert!(state.materialized_group_roles().is_empty());
        assert!(state.materialized_group_role_user_assignments().is_empty());
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_role_body_creation_materializes_role_id_and_records_body() {
        let mut state = realm_state();
        let role_id = role_id(3);
        let role = role_definition(role_id, "Realm admin");
        let expected_value = role_definition_value(&role);

        state.apply(&create_realm_role(1, 1, role)).unwrap();

        assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
        assert_eq!(
            state
                .user_subject_ids
                .get(&format!("realm.roles.{role_id}"))
                .and_then(|version| version.value.as_deref()),
            Some(expected_value.as_str())
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn same_role_conflicting_body_recording() {
        let mut state = group_state();
        let role_id = role_id(3);
        let first = role_definition(role_id, "Group reader");
        let second = role_definition(role_id, "Group writer");
        let first_value = role_definition_value(&first);
        let second_value = role_definition_value(&second);

        state.apply(&create_group_role(1, 1, first)).unwrap();
        state.apply(&create_group_role(2, 2, second)).unwrap();

        assert!(state.materialized_group_roles().is_empty());
        let conflict = state
            .conflicts
            .get(&format!("group.roles.{role_id}"))
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(first_value.as_str()))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(second_value.as_str()))
        );
    }

    #[test]
    fn concurrent_group_role_create_remove_conflict_fails_closed() {
        let mut state = group_state();
        let role_id = role_id(3);
        let role = role_definition(role_id, "Group reader");

        state.apply(&create_group_role(1, 1, role.clone())).unwrap();
        state.apply(&remove_group_role(2, 2, role_id)).unwrap();

        assert!(state.materialized_group_roles().is_empty());
        let conflict = state
            .conflicts
            .get(&format!("group.roles.{role_id}"))
            .expect("conflict is recorded");
        let role_value = role_definition_value(&role);
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(role_value.as_str()))
        );
        assert!(conflict.values.iter().any(|value| value.value.is_none()));
    }

    #[test]
    fn observed_group_role_user_assignment_removal_clears_assignment() {
        let mut state = group_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);
        let assignment_origin = node(2);
        let assignment = group_event(
            2,
            assignment_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
        );
        let removal = group_event(
            3,
            node(3),
            1,
            AdminDocumentClock::default().with_observed(assignment_origin, 1),
            AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
        );

        state.apply(&add_group_role(1, 1, role_id)).unwrap();
        state.apply(&assignment).unwrap();
        state.apply(&removal).unwrap();

        assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
        assert!(state.materialized_group_role_user_assignments().is_empty());
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_group_role_user_assignments_converge_independent_of_order() {
        let role_id = role_id(3);
        let first_user_id = user_id_with_seed(4);
        let second_user_id = user_id_with_seed(5);
        let role_seed = add_group_role(1, 1, role_id);
        let first_assignment = assign_group_role_user(2, 2, role_id, first_user_id);
        let second_assignment = assign_group_role_user(3, 3, role_id, second_user_id);

        let mut left = group_state();
        left.apply(&role_seed).unwrap();
        left.apply(&first_assignment).unwrap();
        left.apply(&second_assignment).unwrap();

        let mut right = group_state();
        right.apply(&role_seed).unwrap();
        right.apply(&second_assignment).unwrap();
        right.apply(&first_assignment).unwrap();

        let expected = BTreeMap::from([(role_id, BTreeSet::from([first_user_id, second_user_id]))]);
        assert_eq!(left.materialized_group_role_user_assignments(), expected);
        assert_eq!(right.materialized_group_role_user_assignments(), expected);
        assert_eq!(left.conflicts, right.conflicts);
        assert!(left.conflicts.is_empty());
    }

    #[test]
    fn concurrent_group_role_additions_converge_independent_of_order() {
        let first_role = role_definition(role_id(3), "Group reader");
        let second_role = role_definition(role_id(4), "Group writer");
        let first = create_group_role(1, 1, first_role.clone());
        let second = create_group_role(2, 2, second_role.clone());

        let mut left = group_state();
        left.apply(&first).unwrap();
        left.apply(&second).unwrap();

        let mut right = group_state();
        right.apply(&second).unwrap();
        right.apply(&first).unwrap();

        let expected_roles = BTreeSet::from([first_role.role_id, second_role.role_id]);
        assert_eq!(left.materialized_group_roles(), expected_roles);
        assert_eq!(right.materialized_group_roles(), expected_roles);
        assert_eq!(left.user_subject_ids, right.user_subject_ids);
        assert!(left.conflicts.is_empty());
        assert!(right.conflicts.is_empty());
    }

    #[test]
    fn concurrent_group_role_user_assignment_add_remove_conflict_fails_closed() {
        let mut state = group_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state.apply(&add_group_role(1, 1, role_id)).unwrap();
        state
            .apply(&assign_group_role_user(2, 2, role_id, user_id))
            .unwrap();
        state
            .apply(&remove_group_role_user_assignment(3, 3, role_id, user_id))
            .unwrap();

        assert_eq!(state.materialized_group_roles(), BTreeSet::from([role_id]));
        assert!(state.materialized_group_role_user_assignments().is_empty());
        let conflict = state
            .conflicts
            .get(&format!("group.roles.{role_id}.assigned_users.{user_id}"))
            .expect("conflict is recorded");
        let expected_user_id = user_id.to_string();
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(expected_user_id.as_str()))
        );
        assert!(conflict.values.iter().any(|value| value.value.is_none()));
    }

    #[test]
    fn realm_role_and_user_assignment_materialize() {
        let mut state = realm_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state.apply(&add_realm_role(1, 1, role_id)).unwrap();
        state
            .apply(&assign_realm_role_user(2, 2, role_id, user_id))
            .unwrap();

        assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
        assert_eq!(
            state.materialized_realm_role_user_assignments(),
            BTreeMap::from([(role_id, BTreeSet::from([user_id]))])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_role_user_assignment_add_remove_conflict_fails_closed() {
        let mut state = realm_state();
        let role_id = role_id(3);
        let user_id = user_id_with_seed(4);

        state.apply(&add_realm_role(1, 1, role_id)).unwrap();
        state
            .apply(&assign_realm_role_user(2, 2, role_id, user_id))
            .unwrap();
        state
            .apply(&remove_realm_role_user_assignment(3, 3, role_id, user_id))
            .unwrap();

        assert_eq!(state.materialized_realm_roles(), BTreeSet::from([role_id]));
        assert!(state.materialized_realm_role_user_assignments().is_empty());
        let conflict = state
            .conflicts
            .get(&format!("realm.roles.{role_id}.assigned_users.{user_id}"))
            .expect("conflict is recorded");
        let expected_user_id = user_id.to_string();
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(expected_user_id.as_str()))
        );
        assert!(conflict.values.iter().any(|value| value.value.is_none()));
    }

    #[test]
    fn realm_config_disjoint_nodes_merge_deterministically() {
        let mut state = realm_config_state();
        let first_node = node(11);
        let second_node = node(12);

        state
            .apply(&ensure_realm_config_node(
                1,
                1,
                first_node,
                RealmNodeKind::Management,
            ))
            .unwrap();
        state
            .apply(&ensure_realm_config_node(
                2,
                2,
                second_node,
                RealmNodeKind::Server,
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_nodes(),
            BTreeMap::from([
                (first_node, RealmNodeKind::Management),
                (second_node, RealmNodeKind::Server),
            ])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_config_same_node_different_kind_conflicts_fail_closed() {
        let mut state = realm_config_state();
        let config_node = node(11);

        state
            .apply(&ensure_realm_config_node(
                1,
                1,
                config_node,
                RealmNodeKind::Management,
            ))
            .unwrap();
        state
            .apply(&ensure_realm_config_node(
                2,
                2,
                config_node,
                RealmNodeKind::Server,
            ))
            .unwrap();

        assert!(
            !state
                .materialized_realm_config_nodes()
                .contains_key(&config_node)
        );
        let conflict = state
            .conflicts
            .get(&format!("realm_config.nodes.{config_node}"))
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("management"))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some("server"))
        );
    }

    #[test]
    fn observed_realm_config_node_update_replaces_conflict() {
        let mut state = realm_config_state();
        let config_node = node(11);
        let first_origin = node(1);
        let second_origin = node(2);
        let first = realm_config_event(
            1,
            first_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: config_node,
                kind: RealmNodeKind::Management,
            },
        );
        let second = realm_config_event(
            2,
            second_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: config_node,
                kind: RealmNodeKind::Server,
            },
        );
        let replacement = realm_config_event(
            3,
            node(3),
            1,
            AdminDocumentClock::default()
                .with_observed(first_origin, 1)
                .with_observed(second_origin, 1),
            AdminDocumentOperation::RealmConfigNodeEnsured {
                node_id: config_node,
                kind: RealmNodeKind::Local,
            },
        );

        state.apply(&first).unwrap();
        state.apply(&second).unwrap();
        state.apply(&replacement).unwrap();

        assert_eq!(
            state.materialized_realm_config_nodes(),
            BTreeMap::from([(config_node, RealmNodeKind::Local)])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_disjoint_oidc_providers_merge_deterministically() {
        let mut state = realm_config_state();
        let first = oidc_provider("default", "one");
        let second = oidc_provider("partner", "two");

        state
            .apply(&upsert_oidc_provider(1, 1, first.clone()))
            .unwrap();
        state
            .apply(&upsert_oidc_provider(2, 2, second.clone()))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_oidc_providers(),
            BTreeMap::from([
                ("default".to_string(), first),
                ("partner".to_string(), second),
            ])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_config_node_provider_and_settings_ops_converge_independent_of_order() {
        let config_node = node(11);
        let provider = oidc_provider("default", "one");
        let metadata_replication = MetadataReplicationConfig::new(5);
        let discovery = RealmDiscoveryConfig::Static {
            endpoints: Vec::new(),
        };
        let node_event = ensure_realm_config_node(1, 1, config_node, RealmNodeKind::Management);
        let provider_event = upsert_oidc_provider(2, 2, provider.clone());
        let settings_event =
            set_realm_config_settings(3, 3, metadata_replication.clone(), discovery.clone());

        let mut left = realm_config_state();
        left.apply(&node_event).unwrap();
        left.apply(&provider_event).unwrap();
        left.apply(&settings_event).unwrap();

        let mut right = realm_config_state();
        right.apply(&settings_event).unwrap();
        right.apply(&provider_event).unwrap();
        right.apply(&node_event).unwrap();

        assert_eq!(left.user_subject_ids, right.user_subject_ids);
        assert_eq!(
            left.materialized_realm_config_nodes(),
            BTreeMap::from([(config_node, RealmNodeKind::Management)])
        );
        assert_eq!(
            left.materialized_realm_config_oidc_providers(),
            BTreeMap::from([("default".to_string(), provider)])
        );
        assert_eq!(
            left.materialized_realm_config_metadata_replication(),
            Some(metadata_replication)
        );
        assert_eq!(left.materialized_realm_config_discovery(), Some(discovery));
        assert!(left.conflicts.is_empty());
        assert!(right.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_config_settings_conflict_is_order_independent() {
        let first_metadata = MetadataReplicationConfig::new(3);
        let second_metadata = MetadataReplicationConfig::new(5);
        let discovery = RealmDiscoveryConfig::Dynamic {
            methods: Vec::new(),
        };
        let first = set_realm_config_settings(1, 1, first_metadata, discovery.clone());
        let second = set_realm_config_settings(2, 2, second_metadata, discovery.clone());

        let mut left = realm_config_state();
        left.apply(&first).unwrap();
        left.apply(&second).unwrap();

        let mut right = realm_config_state();
        right.apply(&second).unwrap();
        right.apply(&first).unwrap();

        assert_eq!(left.conflicts, right.conflicts);
        assert_eq!(left.materialized_realm_config_metadata_replication(), None);
        assert_eq!(right.materialized_realm_config_metadata_replication(), None);
        assert_eq!(
            left.materialized_realm_config_discovery(),
            Some(discovery.clone())
        );
        assert_eq!(right.materialized_realm_config_discovery(), Some(discovery));
    }

    #[test]
    fn concurrent_realm_config_same_oidc_provider_different_body_conflicts_fail_closed() {
        let mut state = realm_config_state();
        let first = oidc_provider("default", "one");
        let second = oidc_provider("default", "two");
        let first_value = oidc_provider_value(&first);
        let second_value = oidc_provider_value(&second);

        state.apply(&upsert_oidc_provider(1, 1, first)).unwrap();
        state.apply(&upsert_oidc_provider(2, 2, second)).unwrap();

        assert!(
            !state
                .materialized_realm_config_oidc_providers()
                .contains_key("default")
        );
        let conflict = state
            .conflicts
            .get("realm_config.oidc_providers.default")
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(first_value.as_str()))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(second_value.as_str()))
        );
    }

    #[test]
    fn observed_realm_config_oidc_provider_remove_removes_provider() {
        let mut state = realm_config_state();
        let provider = oidc_provider("default", "one");
        let upsert_origin = node(1);
        let upsert = realm_config_event(
            1,
            upsert_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
        );
        let removal = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(upsert_origin, 1),
            AdminDocumentOperation::RealmConfigOidcProviderRemoved {
                provider_id: "default".to_string(),
            },
        );

        state.apply(&upsert).unwrap();
        state.apply(&removal).unwrap();

        assert!(state.materialized_realm_config_oidc_providers().is_empty());
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_settings_materialize_metadata_replication_and_discovery() {
        let mut state = realm_config_state();
        let metadata_replication = MetadataReplicationConfig::new(5);
        let discovery = RealmDiscoveryConfig::Static {
            endpoints: Vec::new(),
        };

        state
            .apply(&set_realm_config_settings(
                1,
                1,
                metadata_replication.clone(),
                discovery.clone(),
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_metadata_replication(),
            Some(metadata_replication)
        );
        assert_eq!(state.materialized_realm_config_discovery(), Some(discovery));
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_description_materializes() {
        let mut state = realm_config_state();

        state
            .apply(&set_realm_config_description(1, 1, "Demo Realm"))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_description().as_deref(),
            Some("Demo Realm")
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_quota_materialization_drops_unsupported_max_devices_per_user() {
        let mut state = realm_config_state();
        let quota = QuotaConfig {
            default_group_quota_bytes: Some(1_000),
            max_devices_per_user: Some(6),
            ..QuotaConfig::default()
        };
        let expected = QuotaConfig {
            max_devices_per_user: None,
            ..quota.clone()
        };

        state
            .apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigQuotaSet {
                    quota: quota.clone(),
                },
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_quota(),
            Some(expected.clone())
        );

        let stored_value = state
            .user_subject_ids
            .get(REALM_CONFIG_QUOTA_PATH)
            .and_then(|version| version.value.as_deref())
            .expect("quota reducer value exists")
            .to_string();
        let stored_quota: QuotaConfig = serde_json::from_str(&stored_value).unwrap();
        assert_eq!(stored_quota, expected);

        state
            .user_subject_ids
            .get_mut(REALM_CONFIG_QUOTA_PATH)
            .expect("quota reducer value exists")
            .value = Some(serde_json::to_string(&quota).unwrap());
        assert_eq!(state.materialized_realm_config_quota(), Some(expected));
    }

    #[test]
    fn realm_config_quota_override_order_is_canonical_for_conflict_detection() {
        let group_a = Ulid::from_bytes([1; 16]);
        let group_b = Ulid::from_bytes([2; 16]);
        let user_a = user_id_with_seed(3);
        let user_b = user_id_with_seed(4);
        let expected = QuotaConfig {
            default_group_quota_bytes: Some(1_000),
            grace_factor_percent: 125,
            warn_threshold_percent: 80,
            group_overrides: vec![
                GroupQuotaOverride {
                    group_id: group_a,
                    quota_bytes: Some(500),
                    grace_factor_percent: None,
                },
                GroupQuotaOverride {
                    group_id: group_b,
                    quota_bytes: Some(750),
                    grace_factor_percent: Some(150),
                },
            ],
            max_groups_per_user: Some(4),
            user_group_cap_overrides: vec![
                UserGroupCapOverride {
                    user_id: user_a,
                    max_groups: Some(2),
                },
                UserGroupCapOverride {
                    user_id: user_b,
                    max_groups: Some(3),
                },
            ],
            max_devices_per_user: None,
        };
        let reordered = QuotaConfig {
            group_overrides: expected.group_overrides.iter().cloned().rev().collect(),
            user_group_cap_overrides: expected
                .user_group_cap_overrides
                .iter()
                .cloned()
                .rev()
                .collect(),
            max_devices_per_user: Some(6),
            ..expected.clone()
        };

        let first = realm_config_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigQuotaSet {
                quota: expected.clone(),
            },
        );
        let second = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigQuotaSet { quota: reordered },
        );

        let mut state = realm_config_state();
        state.apply(&first).unwrap();
        state.apply(&second).unwrap();

        assert!(state.conflicts.is_empty());
        assert_eq!(
            state.materialized_realm_config_quota(),
            Some(expected.clone())
        );

        let stored_value = state
            .user_subject_ids
            .get(REALM_CONFIG_QUOTA_PATH)
            .and_then(|version| version.value.as_deref())
            .expect("quota reducer value exists");
        let stored_quota: QuotaConfig = serde_json::from_str(stored_value).unwrap();
        assert_eq!(stored_quota, expected);
    }

    #[test]
    fn realm_config_settings_metadata_conflict_withholds_only_metadata_replication() {
        let mut state = realm_config_state();
        let first_metadata = MetadataReplicationConfig::new(3);
        let second_metadata = MetadataReplicationConfig::new(5);
        let discovery = RealmDiscoveryConfig::Dynamic {
            methods: Vec::new(),
        };
        let first_value = metadata_replication_value(&first_metadata);
        let second_value = metadata_replication_value(&second_metadata);

        state
            .apply(&set_realm_config_settings(
                1,
                1,
                first_metadata,
                discovery.clone(),
            ))
            .unwrap();
        state
            .apply(&set_realm_config_settings(
                2,
                2,
                second_metadata,
                discovery.clone(),
            ))
            .unwrap();

        assert_eq!(state.materialized_realm_config_metadata_replication(), None);
        assert_eq!(state.materialized_realm_config_discovery(), Some(discovery));
        assert!(!state.conflicts.contains_key(REALM_CONFIG_DISCOVERY_PATH));
        let conflict = state
            .conflicts
            .get(REALM_CONFIG_METADATA_REPLICATION_PATH)
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(first_value.as_str()))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(second_value.as_str()))
        );
    }

    #[test]
    fn realm_config_settings_discovery_conflict_withholds_only_discovery() {
        let mut state = realm_config_state();
        let metadata_replication = MetadataReplicationConfig::new(3);
        let first_discovery = RealmDiscoveryConfig::Static {
            endpoints: Vec::new(),
        };
        let second_discovery = RealmDiscoveryConfig::Dynamic {
            methods: Vec::new(),
        };
        let first_value = realm_discovery_value(&first_discovery);
        let second_value = realm_discovery_value(&second_discovery);

        state
            .apply(&set_realm_config_settings(
                1,
                1,
                metadata_replication.clone(),
                first_discovery,
            ))
            .unwrap();
        state
            .apply(&set_realm_config_settings(
                2,
                2,
                metadata_replication.clone(),
                second_discovery,
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_metadata_replication(),
            Some(metadata_replication)
        );
        assert_eq!(state.materialized_realm_config_discovery(), None);
        assert!(
            !state
                .conflicts
                .contains_key(REALM_CONFIG_METADATA_REPLICATION_PATH)
        );
        let conflict = state
            .conflicts
            .get(REALM_CONFIG_DISCOVERY_PATH)
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(first_value.as_str()))
        );
        assert!(
            conflict
                .values
                .iter()
                .any(|value| value.value.as_deref() == Some(second_value.as_str()))
        );
    }

    #[test]
    fn user_operation_is_rejected_for_group_target_without_state_change() {
        let mut state = group_state();
        let before = state.clone();
        let event = group_event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::UserNameSet {
                name: "Alice".to_string(),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::UnsupportedTarget)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn oidc_provider_operation_is_rejected_for_non_realm_config_target_without_state_change() {
        let mut state = user_state();
        let before = state.clone();
        let event = event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigOidcProviderUpserted {
                provider: oidc_provider("default", "one"),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::UnsupportedTarget)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn realm_config_settings_op_is_rejected_for_non_realm_config_target() {
        let mut state = user_state();
        let before = state.clone();
        let event = event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication: MetadataReplicationConfig::new(3),
                discovery: RealmDiscoveryConfig::Static {
                    endpoints: Vec::new(),
                },
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::UnsupportedTarget)
        );
        assert_eq!(state, before);
    }

    fn placement_entry(node_id: NodeId, weight: u32) -> NodePlacementEntry {
        NodePlacementEntry {
            node_id,
            location: "eu-west".to_string(),
            weight,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        }
    }

    fn placement_strategy(strategy_id: Ulid, replica_count: Option<u32>) -> PlacementStrategy {
        PlacementStrategy {
            strategy_id,
            name: "default".to_string(),
            replica_count,
            distinct_locations: false,
            affinity: vec![AffinityRule {
                matcher: LabelMatch {
                    key: "tier".to_string(),
                    value: "hot".to_string(),
                },
                effect: AffinityEffect::Filter,
            }],
            shard_count: 64,
        }
    }

    fn set_placement_entry(
        event_seed: u8,
        origin_seed: u8,
        entry: NodePlacementEntry,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodePlacementSet { entry },
        )
    }

    fn upsert_placement_strategy(
        state: &mut AdminDocumentReducerState,
        event_seed: u8,
        origin_seed: u8,
        strategy_id: Ulid,
    ) {
        state
            .apply(&realm_config_event(
                event_seed,
                node(origin_seed),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: placement_strategy(strategy_id, Some(3)),
                },
            ))
            .unwrap();
    }

    #[test]
    fn admin_document_placement_paths_preserve_strings_and_round_trip() {
        let node_id = node(6);
        let strategy_id = Ulid::from_bytes([4; 16]);

        let node_path = realm_config_placement_node_path(&node_id);
        assert_eq!(node_path, format!("realm_config.placement.nodes.{node_id}"));
        assert_eq!(
            realm_config_placement_node_id_from_path(&node_path),
            Some(node_id)
        );

        let strategy_path = realm_config_placement_strategy_path(&strategy_id);
        assert_eq!(
            strategy_path,
            format!("realm_config.placement.strategies.{strategy_id}")
        );
        assert_eq!(
            realm_config_placement_strategy_id_from_path(&strategy_path),
            Some(strategy_id)
        );

        assert_eq!(
            REALM_CONFIG_DEFAULT_STRATEGY_PATH,
            "realm_config.placement.default_strategy"
        );
        assert_eq!(
            realm_config_placement_node_id_from_path("realm_config.placement.nodes.invalid"),
            None
        );
    }

    #[test]
    fn binding_scope_keys_use_stable_canonical_text_and_parse_from_paths() {
        let group_id = group_id();
        let cases = [
            (BindingScope::Realm, "realm".to_string()),
            (BindingScope::Group(group_id), format!("group:{group_id}")),
            (
                BindingScope::Class(DocumentClass::Admin),
                "class:admin".to_string(),
            ),
            (
                BindingScope::Class(DocumentClass::Group),
                "class:group".to_string(),
            ),
            (
                BindingScope::Class(DocumentClass::User),
                "class:user".to_string(),
            ),
            (
                BindingScope::Class(DocumentClass::Metadata),
                "class:metadata".to_string(),
            ),
            (
                BindingScope::Class(DocumentClass::MetadataRegistry),
                "class:metadata_registry".to_string(),
            ),
            (
                BindingScope::Class(DocumentClass::JobControl),
                "class:job_control".to_string(),
            ),
            (
                BindingScope::MetadataPathPrefix(" /datasets/important/ ".to_string()),
                "metadata_path_prefix:datasets/important".to_string(),
            ),
        ];

        for (scope, expected_key) in cases {
            assert_eq!(binding_scope_key(&scope), expected_key);

            let path = realm_config_strategy_binding_path(&scope);
            assert_eq!(
                path,
                format!("realm_config.placement.bindings.{expected_key}")
            );
            assert_eq!(
                realm_config_strategy_binding_scope_key_from_path(&path),
                Some(expected_key.as_str())
            );
        }
    }

    #[test]
    fn realm_config_placement_overlay_replaces_owned_paths_deterministically() {
        let owned_node = node(11);
        let unowned_node = node(12);
        let owned_entry = placement_entry(owned_node, 250);
        let unowned_entry = placement_entry(unowned_node, 100);
        let owned_strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
        let unowned_strategy = placement_strategy(Ulid::from_bytes([5; 16]), None);
        let owned_binding = StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: owned_strategy.strategy_id,
        };
        let unowned_binding = StrategyBinding {
            scope: BindingScope::Realm,
            strategy_id: unowned_strategy.strategy_id,
        };
        let owned_override = PlacementOverride {
            subject: b"owned".to_vec(),
            pinned: vec![owned_node],
            excluded: Vec::new(),
            strategy_id: Some(owned_strategy.strategy_id),
        };
        let unowned_override = PlacementOverride {
            subject: b"unowned".to_vec(),
            pinned: vec![unowned_node],
            excluded: Vec::new(),
            strategy_id: Some(unowned_strategy.strategy_id),
        };

        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        config.placement_map = vec![unowned_entry.clone(), placement_entry(owned_node, 1)];
        config.strategies = vec![
            unowned_strategy.clone(),
            placement_strategy(owned_strategy.strategy_id, Some(1)),
        ];
        config.default_strategy_id = Some(unowned_strategy.strategy_id);
        config.strategy_bindings = vec![
            unowned_binding.clone(),
            StrategyBinding {
                scope: owned_binding.scope.clone(),
                strategy_id: unowned_strategy.strategy_id,
            },
        ];
        config.placement_overrides = vec![
            unowned_override.clone(),
            PlacementOverride {
                subject: owned_override.subject.clone(),
                pinned: Vec::new(),
                excluded: vec![owned_node],
                strategy_id: None,
            },
        ];

        let untouched = config.clone();
        overlay_realm_config_placement_reducer_materialization(&mut config, &realm_config_state());
        assert_eq!(config, untouched);

        let mut state = realm_config_state();
        let actor = actor(node(1));
        for op in [
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: owned_entry.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                strategy: owned_strategy.clone(),
            },
            AdminDocumentOperation::RealmConfigDefaultStrategySet {
                strategy_id: owned_strategy.strategy_id,
            },
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: owned_binding.clone(),
            },
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: owned_override.clone(),
            },
        ] {
            state.apply_operation(&actor, op).unwrap();
        }

        overlay_realm_config_placement_reducer_materialization(&mut config, &state);
        assert_eq!(config.placement_map, vec![unowned_entry, owned_entry]);
        assert_eq!(
            config.strategies,
            vec![unowned_strategy.clone(), owned_strategy.clone()]
        );
        assert_eq!(config.default_strategy_id, Some(owned_strategy.strategy_id));
        assert_eq!(
            config.strategy_bindings,
            vec![unowned_binding, owned_binding]
        );
        assert_eq!(
            config.placement_overrides,
            vec![unowned_override, owned_override]
        );

        let materialized = config.clone();
        overlay_realm_config_placement_reducer_materialization(&mut config, &state);
        assert_eq!(config, materialized);
    }

    #[test]
    fn realm_config_placement_repair_clears_refs_but_preserves_override_without_live_strategy() {
        let missing_strategy_id = Ulid::from_bytes([8; 16]);
        let pinned = node(11);
        let excluded = node(12);
        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        config.default_strategy_id = Some(missing_strategy_id);
        config.strategy_bindings = vec![StrategyBinding {
            scope: BindingScope::Realm,
            strategy_id: missing_strategy_id,
        }];
        config.placement_overrides = vec![PlacementOverride {
            subject: b"document-subject".to_vec(),
            pinned: vec![pinned],
            excluded: vec![excluded],
            strategy_id: Some(missing_strategy_id),
        }];

        overlay_realm_config_placement_reducer_materialization(&mut config, &realm_config_state());

        assert_eq!(config.default_strategy_id, None);
        assert!(config.strategy_bindings.is_empty());
        assert_eq!(config.placement_overrides.len(), 1);
        assert_eq!(config.placement_overrides[0].strategy_id, None);
        assert_eq!(config.placement_overrides[0].pinned, vec![pinned]);
        assert_eq!(config.placement_overrides[0].excluded, vec![excluded]);
    }

    #[test]
    fn realm_config_placement_entry_materializes() {
        let mut state = realm_config_state();
        let config_node = node(11);
        let entry = placement_entry(config_node, 250);

        state
            .apply(&set_placement_entry(1, 1, entry.clone()))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_placement_map(),
            BTreeMap::from([(config_node, entry)])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_disjoint_placement_entries_merge_deterministically() {
        let first_node = node(11);
        let second_node = node(12);
        let first = placement_entry(first_node, 100);
        let second = placement_entry(second_node, 200);
        let first_event = set_placement_entry(1, 1, first.clone());
        let second_event = set_placement_entry(2, 2, second.clone());

        let mut left = realm_config_state();
        left.apply(&first_event).unwrap();
        left.apply(&second_event).unwrap();

        let mut right = realm_config_state();
        right.apply(&second_event).unwrap();
        right.apply(&first_event).unwrap();

        assert_eq!(left.user_subject_ids, right.user_subject_ids);
        assert_eq!(
            left.materialized_realm_config_placement_map(),
            BTreeMap::from([(first_node, first), (second_node, second)])
        );
        assert!(left.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_config_same_placement_node_conflicts_fail_closed() {
        let mut state = realm_config_state();
        let config_node = node(11);

        state
            .apply(&set_placement_entry(
                1,
                1,
                placement_entry(config_node, 100),
            ))
            .unwrap();
        state
            .apply(&set_placement_entry(
                2,
                2,
                placement_entry(config_node, 250),
            ))
            .unwrap();

        assert!(
            !state
                .materialized_realm_config_placement_map()
                .contains_key(&config_node)
        );
        let conflict = state
            .conflicts
            .get(&realm_config_placement_node_path(&config_node))
            .expect("conflict is recorded");
        assert_eq!(conflict.values.len(), 2);
    }

    #[test]
    fn equal_concurrent_placement_writes_preserve_causal_frontier() {
        let config_node = node(11);
        let origin_a = node(1);
        let origin_b = node(2);
        let first_a = realm_config_event(
            1,
            origin_a,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(config_node, 100),
            },
        );
        let first_b = realm_config_event(
            2,
            origin_b,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(config_node, 100),
            },
        );
        let second_a = realm_config_event(
            3,
            origin_a,
            2,
            AdminDocumentClock::default().with_observed(origin_a, 1),
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(config_node, 250),
            },
        );
        let events = [first_a, first_b, second_a];
        let mut states = Vec::new();
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let mut state = realm_config_state();
            for index in order {
                state.apply(&events[index]).unwrap();
            }
            states.push(state);
        }

        for state in &states[1..] {
            assert_eq!(state, &states[0]);
        }
        let state = &states[0];
        assert!(
            !state
                .materialized_realm_config_placement_map()
                .contains_key(&config_node)
        );
        let conflict = state
            .conflicts
            .get(&realm_config_placement_node_path(&config_node))
            .expect("causally concurrent values conflict");
        assert_eq!(
            conflict
                .values
                .iter()
                .map(|candidate| candidate.dot)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([events[1].dot(), events[2].dot()])
        );
    }

    #[test]
    fn observed_realm_config_placement_entry_remove_removes_entry() {
        let mut state = realm_config_state();
        let config_node = node(11);
        let set_origin = node(1);
        let set = realm_config_event(
            1,
            set_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigNodePlacementSet {
                entry: placement_entry(config_node, 100),
            },
        );
        let removal = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(set_origin, 1),
            AdminDocumentOperation::RealmConfigNodePlacementRemoved {
                node_id: config_node,
            },
        );

        state.apply(&set).unwrap();
        state.apply(&removal).unwrap();

        assert!(state.materialized_realm_config_placement_map().is_empty());
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn rejects_derived_labels() {
        // Both the kind label and any storage class are stamped by the node.
        for key in [
            KIND_LABEL_KEY.to_string(),
            format!("{STORAGE_CLASS_LABEL_PREFIX}cold"),
        ] {
            let mut state = realm_config_state();
            let before = state.clone();
            let mut entry = placement_entry(node(11), 100);
            entry.labels.insert(key.clone(), "Server".to_string());

            assert_eq!(
                state.apply(&set_placement_entry(1, 1, entry)),
                Err(AdminDocumentReducerError::ReservedPlacementLabel(key))
            );
            assert_eq!(state, before);
        }
    }

    #[test]
    fn realm_config_placement_strategy_materializes() {
        let mut state = realm_config_state();
        let strategy_id = Ulid::from_bytes([4; 16]);
        let strategy = placement_strategy(strategy_id, Some(3));

        state
            .apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: strategy.clone(),
                },
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_placement_strategies(),
            BTreeMap::from([(strategy_id, strategy)])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn strategy_shards_immutable() {
        let mut state = realm_config_state();
        let origin = node(1);
        let strategy_id = Ulid::from_bytes([4; 16]);
        let initial = placement_strategy(strategy_id, Some(3));
        let mut renamed = initial.clone();
        renamed.name = "renamed".to_string();

        assert_eq!(
            state.apply(&realm_config_event(
                1,
                origin,
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: initial },
            )),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            state.apply(&realm_config_event(
                2,
                origin,
                2,
                AdminDocumentClock::default().with_observed(origin, 1),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: renamed.clone(),
                },
            )),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        let before = state.clone();
        let mut changed = renamed;
        changed.shard_count *= 2;
        assert_eq!(
            state.apply(&realm_config_event(
                3,
                origin,
                3,
                AdminDocumentClock::default().with_observed(origin, 2),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy: changed },
            )),
            Err(AdminDocumentReducerError::PlacementShardCountChanged)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn realm_config_placement_strategy_rejects_zero_replica_count() {
        let mut state = realm_config_state();
        let before = state.clone();

        assert_eq!(
            state.apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: placement_strategy(Ulid::from_bytes([4; 16]), Some(0)),
                },
            )),
            Err(AdminDocumentReducerError::ZeroPlacementReplicaCount)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn realm_config_placement_strategy_accepts_max_shard_count() {
        let mut state = realm_config_state();
        let strategy_id = Ulid::from_bytes([4; 16]);
        let mut strategy = placement_strategy(strategy_id, Some(3));
        strategy.shard_count = MAX_PLACEMENT_SHARD_COUNT;

        state
            .apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: strategy.clone(),
                },
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_placement_strategies(),
            BTreeMap::from([(strategy_id, strategy)])
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_placement_strategy_rejects_shard_count_above_max() {
        let mut state = realm_config_state();
        let before = state.clone();
        let mut strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
        strategy.shard_count = MAX_PLACEMENT_SHARD_COUNT * 2;

        assert_eq!(
            state.apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
            )),
            Err(AdminDocumentReducerError::InvalidPlacementShardCount)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn realm_config_placement_strategy_rejects_zero_and_non_power_of_two_shard_count() {
        let mut state = realm_config_state();
        let before = state.clone();

        for bad in [0u32, 3, 63] {
            let mut strategy = placement_strategy(Ulid::from_bytes([4; 16]), Some(3));
            strategy.shard_count = bad;
            assert_eq!(
                state.apply(&realm_config_event(
                    1,
                    node(1),
                    1,
                    AdminDocumentClock::default(),
                    AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy },
                )),
                Err(AdminDocumentReducerError::InvalidPlacementShardCount),
                "shard_count {bad} must be rejected"
            );
            assert_eq!(state, before);
        }
    }

    #[test]
    fn realm_config_default_strategy_materializes() {
        let mut state = realm_config_state();
        let strategy_id = Ulid::from_bytes([4; 16]);
        upsert_placement_strategy(&mut state, 9, 9, strategy_id);

        state
            .apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_default_strategy(),
            Some(strategy_id)
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn concurrent_realm_config_strategy_remove_and_references_are_replay_order_independent() {
        let strategy_id = Ulid::from_bytes([4; 16]);
        let fallback_strategy_id = Ulid::from_bytes([3; 16]);
        let subject = b"document-subject".to_vec();
        let reference_ops = vec![
            AdminDocumentOperation::RealmConfigDefaultStrategySet { strategy_id },
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: StrategyBinding {
                    scope: BindingScope::MetadataPathPrefix("datasets".to_string()),
                    strategy_id,
                },
            },
            AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                record: PlacementOverride {
                    subject,
                    pinned: vec![node(4)],
                    excluded: Vec::new(),
                    strategy_id: Some(strategy_id),
                },
            },
        ];

        for (index, reference_op) in reference_ops.into_iter().enumerate() {
            let seed = 40 + index as u8 * 10;
            let strategy_origin = node(seed);
            let mut initial = realm_config_state();
            upsert_placement_strategy(&mut initial, seed, seed, strategy_id);
            let observed_strategy = AdminDocumentClock::default().with_observed(strategy_origin, 1);
            let removal = realm_config_event(
                seed + 1,
                node(seed + 1),
                1,
                observed_strategy.clone(),
                AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id },
            );
            let reference = realm_config_event(
                seed + 2,
                node(seed + 2),
                1,
                observed_strategy,
                reference_op.clone(),
            );

            let mut remove_first = initial.clone();
            assert_eq!(
                remove_first.apply(&removal),
                Ok(AdminDocumentApplyStatus::Applied)
            );
            assert_eq!(
                remove_first.apply(&reference),
                Ok(AdminDocumentApplyStatus::Applied)
            );

            let mut reference_first = initial;
            assert_eq!(
                reference_first.apply(&reference),
                Ok(AdminDocumentApplyStatus::Applied)
            );
            assert_eq!(
                reference_first.apply(&removal),
                Ok(AdminDocumentApplyStatus::Applied)
            );

            assert_eq!(remove_first, reference_first);
            assert!(
                remove_first
                    .materialized_realm_config_placement_strategies()
                    .is_empty()
            );
            assert!(remove_first.conflicts.is_empty());

            let mut base_config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
            base_config.strategies = vec![
                placement_strategy(fallback_strategy_id, Some(1)),
                placement_strategy(strategy_id, Some(3)),
            ];
            base_config.default_strategy_id = Some(fallback_strategy_id);

            let mut remove_first_config = base_config.clone();
            overlay_realm_config_placement_reducer_materialization(
                &mut remove_first_config,
                &remove_first,
            );
            let mut reference_first_config = base_config;
            overlay_realm_config_placement_reducer_materialization(
                &mut reference_first_config,
                &reference_first,
            );
            assert_eq!(remove_first_config, reference_first_config);
            assert_eq!(
                remove_first_config.default_strategy_id,
                Some(fallback_strategy_id)
            );
            assert!(remove_first_config.strategy(&strategy_id).is_none());
            assert_realm_config_strategy_references_are_live(&remove_first_config);

            match &reference_op {
                AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {}
                AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => assert!(
                    remove_first_config
                        .strategy_bindings
                        .iter()
                        .any(|materialized| {
                            materialized.scope == binding.scope
                                && materialized.strategy_id == fallback_strategy_id
                        })
                ),
                AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => {
                    let materialized = remove_first_config
                        .placement_overrides
                        .iter()
                        .find(|materialized| materialized.subject == record.subject)
                        .expect("override remains materialized");
                    assert_eq!(materialized.strategy_id, Some(fallback_strategy_id));
                    assert_eq!(materialized.pinned, record.pinned);
                    assert_eq!(materialized.excluded, record.excluded);
                }
                _ => unreachable!("test only contains strategy reference operations"),
            }

            let restoration = realm_config_event(
                seed + 3,
                node(seed + 3),
                1,
                AdminDocumentClock::default().with_observed(removal.origin_node_id, 1),
                AdminDocumentOperation::RealmConfigPlacementStrategyUpserted {
                    strategy: placement_strategy(strategy_id, Some(3)),
                },
            );
            remove_first.apply(&restoration).unwrap();
            overlay_realm_config_placement_reducer_materialization(
                &mut remove_first_config,
                &remove_first,
            );
            assert!(remove_first_config.strategy(&strategy_id).is_some());
            assert_realm_config_strategy_references_are_live(&remove_first_config);
            match reference_op {
                AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {
                    assert_eq!(remove_first_config.default_strategy_id, Some(strategy_id))
                }
                AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => assert!(
                    remove_first_config
                        .strategy_bindings
                        .iter()
                        .any(|materialized| {
                            materialized.scope == binding.scope
                                && materialized.strategy_id == strategy_id
                        })
                ),
                AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => assert!(
                    remove_first_config
                        .placement_overrides
                        .iter()
                        .any(|materialized| {
                            materialized.subject == record.subject
                                && materialized.strategy_id == Some(strategy_id)
                                && materialized.pinned == record.pinned
                                && materialized.excluded == record.excluded
                        })
                ),
                _ => unreachable!("test only contains strategy reference operations"),
            }
        }
    }

    fn assert_realm_config_strategy_references_are_live(config: &RealmConfigDocument) {
        assert!(
            config
                .default_strategy_id
                .is_none_or(|strategy_id| config.strategy(&strategy_id).is_some())
        );
        assert!(
            config
                .strategy_bindings
                .iter()
                .all(|binding| config.strategy(&binding.strategy_id).is_some())
        );
        assert!(config.placement_overrides.iter().all(|record| {
            record
                .strategy_id
                .is_none_or(|strategy_id| config.strategy(&strategy_id).is_some())
        }));
    }

    #[test]
    fn realm_config_strategy_binding_materializes_and_removes() {
        let mut state = realm_config_state();
        let scope = BindingScope::Class(DocumentClass::MetadataRegistry);
        let binding = StrategyBinding {
            scope: scope.clone(),
            strategy_id: Ulid::from_bytes([4; 16]),
        };
        upsert_placement_strategy(&mut state, 9, 9, binding.strategy_id);
        let scope_key = binding_scope_key(&scope);
        let set_origin = node(1);
        let set = realm_config_event(
            1,
            set_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigStrategyBindingSet {
                binding: binding.clone(),
            },
        );

        state.apply(&set).unwrap();
        assert_eq!(
            state.materialized_realm_config_strategy_bindings(),
            BTreeMap::from([(scope_key, binding)])
        );
        assert!(
            !state
                .conflicts
                .contains_key(&realm_config_strategy_binding_path(&scope))
        );

        let removal = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(set_origin, 1),
            AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope },
        );
        state.apply(&removal).unwrap();
        assert!(
            state
                .materialized_realm_config_strategy_bindings()
                .is_empty()
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_metadata_path_prefix_binding_remove_uses_normalized_key() {
        let mut state = realm_config_state();
        let raw_scope = BindingScope::MetadataPathPrefix("/datasets/".to_string());
        let canonical_scope = BindingScope::MetadataPathPrefix("datasets".to_string());
        let binding = StrategyBinding {
            scope: raw_scope.clone(),
            strategy_id: Ulid::from_bytes([4; 16]),
        };
        upsert_placement_strategy(&mut state, 9, 9, binding.strategy_id);
        let canonical_binding = StrategyBinding {
            scope: canonical_scope.clone(),
            strategy_id: binding.strategy_id,
        };
        let canonical_scope_key = binding_scope_key(&canonical_scope);
        let unnormalized_path = "realm_config.placement.bindings.metadata_path_prefix:/datasets/";
        let set_origin = node(1);
        let set = realm_config_event(
            1,
            set_origin,
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigStrategyBindingSet { binding },
        );

        state.apply(&set).unwrap();
        assert_eq!(
            state.materialized_realm_config_strategy_bindings(),
            BTreeMap::from([(canonical_scope_key, canonical_binding)])
        );
        assert!(
            state
                .user_subject_ids
                .contains_key(&realm_config_strategy_binding_path(&canonical_scope))
        );
        assert!(!state.user_subject_ids.contains_key(unnormalized_path));

        let removal = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(set_origin, 1),
            AdminDocumentOperation::RealmConfigStrategyBindingRemoved {
                scope: BindingScope::MetadataPathPrefix(" datasets/ ".to_string()),
            },
        );

        state.apply(&removal).unwrap();
        assert!(
            state
                .materialized_realm_config_strategy_bindings()
                .is_empty()
        );
        assert!(state.conflicts.is_empty());
    }

    #[test]
    fn realm_config_placement_override_materializes() {
        let mut state = realm_config_state();
        let subject = b"document-subject".to_vec();
        let strategy_id = Ulid::from_bytes([4; 16]);
        upsert_placement_strategy(&mut state, 9, 9, strategy_id);
        let record = PlacementOverride {
            subject: subject.clone(),
            pinned: vec![node(4)],
            excluded: vec![node(5)],
            strategy_id: Some(strategy_id),
        };
        let subject_key = hex::encode(&subject);

        state
            .apply(&realm_config_event(
                1,
                node(1),
                1,
                AdminDocumentClock::default(),
                AdminDocumentOperation::RealmConfigPlacementOverrideSet {
                    record: record.clone(),
                },
            ))
            .unwrap();

        assert_eq!(
            state.materialized_realm_config_placement_overrides(),
            BTreeMap::from([(subject_key, record)])
        );
        assert_eq!(
            state.apply(&realm_config_placement_override_removed(2, subject)),
            Ok(AdminDocumentApplyStatus::Applied)
        );
    }

    fn realm_config_placement_override_removed(
        event_seed: u8,
        subject: Vec<u8>,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(2),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject },
        )
    }

    #[test]
    fn placement_op_is_rejected_for_non_realm_config_target() {
        let mut state = user_state();
        let before = state.clone();
        let event = event(
            1,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigDefaultStrategySet {
                strategy_id: Ulid::from_bytes([4; 16]),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::UnsupportedTarget)
        );
        assert_eq!(state, before);
    }

    fn placement_binding(handle: u32, strategy_seed: u8) -> PlacementBinding {
        PlacementBinding {
            handle: PlacementHandle::new(handle).unwrap(),
            scope: PlacementScope::Realm(realm_id()),
            document_class: DocumentClass::MetadataRegistry,
            strategy_id: Ulid::from_bytes([strategy_seed; 16]),
            allocator_range_id: None,
            allocated_by: None,
            allocated_at_ms: None,
        }
    }

    fn append_placement_binding(
        event_seed: u8,
        origin_seed: u8,
        binding: PlacementBinding,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding },
        )
    }

    fn handle_range(range_seed: u8, owner: NodeId, start: u32, end: u32) -> HandleRange {
        HandleRange {
            range_id: Ulid::from_bytes([range_seed; 16]),
            owner,
            start,
            end,
        }
    }

    fn grant_handle_range(
        event_seed: u8,
        origin_seed: u8,
        range: HandleRange,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigHandleRangeGranted { range },
        )
    }

    #[test]
    fn grants_replicate_usable() {
        let owner = node(3);
        let first = grant_handle_range(1, 1, handle_range(10, owner, FIRST_GRANTABLE_HANDLE, 1027));
        let second = grant_handle_range(2, 2, handle_range(20, owner, 1027, 2051));

        let mut state = realm_config_state();
        state.apply(&first).unwrap();
        state.apply(&second).unwrap();
        assert!(state.conflicts.is_empty());
        assert_eq!(state.materialized_handle_ranges().len(), 2);

        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut config, &state);
        let directory = config.handle_range_directory();
        assert_eq!(directory.conflicts(), 0);
        assert_eq!(directory.granted_to(&owner).len(), 2);
        assert_eq!(
            directory.free_band_in(&[(
                FIRST_GRANTABLE_HANDLE,
                crate::structs::band_start(crate::structs::HANDLE_BANDS)
            )]),
            Some((2051, 3075))
        );
    }

    #[test]
    fn malformed_range_rejected() {
        let event = grant_handle_range(1, 1, handle_range(10, node(3), 1, 1025));
        let mut state = realm_config_state();

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::InvalidHandleRange)
        );
    }

    #[test]
    fn overlap_conflicts_converge() {
        let owner = node(3);
        let first = grant_handle_range(1, 1, handle_range(10, owner, FIRST_GRANTABLE_HANDLE, 1027));
        let second = grant_handle_range(2, 2, handle_range(20, owner, 512, 2049));

        let mut left = realm_config_state();
        left.apply(&first).unwrap();
        left.apply(&second).unwrap();

        let mut right = realm_config_state();
        right.apply(&second).unwrap();
        right.apply(&first).unwrap();

        // Distinct ids retain separate paths; the derived directory catches overlap.
        assert!(left.conflicts.is_empty());
        assert_eq!(left.conflicts, right.conflicts);
        for range_seed in [10u8, 20] {
            let path = handle_range_path(Ulid::from_bytes([range_seed; 16]));
            assert!(left.user_subject_ids.contains_key(&path));
        }
        assert_eq!(left.materialized_handle_ranges().len(), 2);

        for state in [&left, &right] {
            let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
            overlay_realm_config_placement_reducer_materialization(&mut config, state);
            assert_eq!(config.placement_handle_ranges.len(), 2);
            let directory = config.handle_range_directory();
            assert_eq!(directory.conflicts(), 2);
            assert!(directory.granted_to(&owner).is_empty());
        }
    }

    #[test]
    fn binding_paths_disjoint() {
        let handle = PlacementHandle::new(42).unwrap();
        let path = placement_binding_path(handle);
        assert_eq!(path, "realm_config.placement.placement_bindings.42");
        assert_eq!(placement_binding_handle(&path), Some(handle));
        // The two placement-binding namespaces must not parse each other's paths.
        assert_eq!(
            realm_config_strategy_binding_scope_key_from_path(&path),
            None
        );
        let strategy_path = realm_config_strategy_binding_path(&BindingScope::Realm);
        assert_eq!(placement_binding_handle(&strategy_path), None);
    }

    #[test]
    fn binding_conflicts_converge() {
        let handle = PlacementHandle::new(7).unwrap();
        let first = append_placement_binding(1, 1, placement_binding(7, 1));
        let second = append_placement_binding(2, 2, placement_binding(7, 2));
        let observed_second = realm_config_event(
            2,
            node(1),
            2,
            AdminDocumentClock::default().with_observed(node(1), 1),
            AdminDocumentOperation::RealmConfigPlacementBindingAppended {
                binding: placement_binding(7, 2),
            },
        );

        let mut left = realm_config_state();
        left.apply(&first).unwrap();
        left.apply(&second).unwrap();

        let mut right = realm_config_state();
        right.apply(&second).unwrap();
        right.apply(&first).unwrap();

        let mut observed = realm_config_state();
        observed.apply(&first).unwrap();
        observed.apply(&observed_second).unwrap();

        let mut observed_reversed = realm_config_state();
        observed_reversed.apply(&observed_second).unwrap();
        assert_eq!(
            observed_reversed.apply(&first),
            Ok(AdminDocumentApplyStatus::Applied)
        );

        assert_eq!(left.conflicts, right.conflicts);
        let path = placement_binding_path(handle);
        assert!(left.conflicts.contains_key(&path));
        assert_eq!(observed.conflicts, observed_reversed.conflicts);
        assert!(observed.conflicts.contains_key(&path));
        assert!(left.materialized_placement_bindings().is_empty());
        assert!(right.materialized_placement_bindings().is_empty());

        for state in [&left, &right] {
            let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
            overlay_realm_config_placement_reducer_materialization(&mut config, state);
            let directory = config.binding_directory();
            assert_eq!(
                directory.resolve(handle),
                Err(BindingError::Conflicted(handle))
            );
            assert_eq!(directory.conflicted(), 1);
        }
    }

    #[test]
    fn provenance_conflicts() {
        let handle = PlacementHandle::new(9).unwrap();
        let mut first = placement_binding(9, 1);
        first.allocated_by = Some(node(3));
        first.allocated_at_ms = Some(1);
        let mut second = placement_binding(9, 1);
        second.allocated_by = Some(node(4));
        second.allocated_at_ms = Some(2);

        let mut state = realm_config_state();
        state.apply(&append_placement_binding(1, 1, first)).unwrap();
        state
            .apply(&append_placement_binding(2, 2, second))
            .unwrap();

        assert_eq!(state.conflicts.len(), 1);
        assert!(state.materialized_placement_bindings().is_empty());

        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        overlay_realm_config_placement_reducer_materialization(&mut config, &state);
        assert_eq!(config.placement_bindings.len(), 2);
        assert_eq!(
            config.binding_directory().resolve(handle),
            Err(BindingError::Conflicted(handle))
        );
    }

    #[test]
    fn binding_reappend_idempotent() {
        let handle = PlacementHandle::new(11).unwrap();
        let origin = node(1);
        let range_id = Ulid::from_bytes([8; 16]);
        let mut binding = placement_binding(11, 1);
        binding.allocator_range_id = Some(range_id);
        binding.allocated_by = Some(origin);
        binding.allocated_at_ms = Some(1);
        let first = append_placement_binding(1, 1, binding.clone());
        let second = realm_config_event(
            2,
            node(2),
            1,
            AdminDocumentClock::default().with_observed(origin, 1),
            AdminDocumentOperation::RealmConfigPlacementBindingAppended {
                binding: binding.clone(),
            },
        );

        let mut state = realm_config_state();
        state.apply(&first).unwrap();
        state.apply(&second).unwrap();

        assert!(state.conflicts.is_empty());
        assert_eq!(state.materialized_placement_bindings().len(), 1);
        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        config
            .placement_handle_ranges
            .push(handle_range(8, origin, 3, 20));
        overlay_realm_config_placement_reducer_materialization(&mut config, &state);
        assert_eq!(config.placement_bindings.len(), 1);
        assert_eq!(
            config
                .binding_directory()
                .resolve(handle)
                .map(|tuple| tuple.strategy_id),
            Ok(binding.strategy_id)
        );
    }

    #[test]
    fn overlay_retains_conflicts() {
        let first = placement_binding(13, 1);
        let second = placement_binding(13, 2);
        let mut state = realm_config_state();
        state
            .apply(&append_placement_binding(1, 1, first.clone()))
            .unwrap();
        state
            .apply(&append_placement_binding(2, 2, second.clone()))
            .unwrap();

        let mut config = RealmConfigDocument::new(realm_id(), Vec::new(), 3);
        // A stale local entry for the handle must be replaced, not accumulated.
        config.placement_bindings.push(first.clone());
        overlay_realm_config_placement_reducer_materialization(&mut config, &state);

        assert_eq!(config.placement_bindings.len(), 2);
        let strategies: BTreeSet<_> = config
            .placement_bindings
            .iter()
            .map(|binding| binding.strategy_id)
            .collect();
        assert_eq!(
            strategies,
            BTreeSet::from([first.strategy_id, second.strategy_id])
        );
        assert_eq!(config.binding_directory().conflicted(), 1);
    }

    fn revoke_token(event_seed: u8, origin_seed: u8, token: &str) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: crate::auth::bearer_token_hash(token),
                expires_at: 2_000,
                token_owner: user_id(),
            },
        )
    }

    fn revoke_token_at(
        event_seed: u8,
        origin_seed: u8,
        token: &str,
        expires_at: u64,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: crate::auth::bearer_token_hash(token),
                expires_at,
                token_owner: user_id(),
            },
        )
    }

    fn revoke_token_owned(
        event_seed: u8,
        origin_seed: u8,
        token: &str,
        expires_at: u64,
        token_owner: UserId,
    ) -> AdminDocumentEvent {
        realm_config_event(
            event_seed,
            node(origin_seed),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: crate::auth::bearer_token_hash(token),
                expires_at,
                token_owner,
            },
        )
    }

    #[test]
    fn revocations_accumulate() {
        // Two origins revoking different tokens both survive, and a repeat of
        // one revocation is not a conflict.
        let mut state = realm_config_state();
        state.apply(&revoke_token(1, 1, "first")).unwrap();
        state.apply(&revoke_token(2, 2, "second")).unwrap();
        state.apply(&revoke_token(3, 2, "first")).unwrap();

        assert!(state.conflicts.is_empty());
        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([
                (crate::auth::bearer_token_hash("first"), 2_000),
                (crate::auth::bearer_token_hash("second"), 2_000),
            ])
        );
    }

    #[test]
    fn repeated_revocations_bound() {
        let mut state = realm_config_state();
        let events: Vec<_> = (1..=8)
            .map(|seed| revoke_token(seed, seed, "repeat"))
            .collect();

        for (index, event) in events.iter().enumerate() {
            let expected = if index == 0 {
                AdminDocumentApplyStatus::Applied
            } else {
                AdminDocumentApplyStatus::Redundant
            };
            assert_eq!(state.apply(event), Ok(expected));
        }

        let path =
            super::revoked_token_path(&crate::auth::bearer_token_hash("repeat"), 2_000, &user_id());
        assert_eq!(state.user_subject_ids.len(), 1);
        assert_eq!(state.user_subject_ids[&path].dot, events[0].dot());
        assert!(state.equivalent_value_dots.is_empty());
        assert_eq!(
            state.applied_event_ids,
            BTreeSet::from([events[0].event_id])
        );
        assert_eq!(state.clock.sequence_for(&node(8)), 1);
    }

    #[test]
    fn revocation_order_converges() {
        let equal_first = revoke_token_at(10, 1, "equal", 4_000);
        let equal_second = revoke_token_at(11, 2, "equal", 4_000);
        let mut equal_left = realm_config_state();
        equal_left.apply(&equal_first).unwrap();
        equal_left.apply(&equal_second).unwrap();
        let mut equal_right = realm_config_state();
        equal_right.apply(&equal_second).unwrap();
        equal_right.apply(&equal_first).unwrap();

        assert_eq!(equal_left, equal_right);
        let equal_path =
            super::revoked_token_path(&crate::auth::bearer_token_hash("equal"), 4_000, &user_id());
        assert_eq!(
            equal_left.user_subject_ids[&equal_path].dot,
            equal_first.dot()
        );

        let shorter = revoke_token_at(12, 1, "different", 2_000);
        let longer = revoke_token_at(13, 2, "different", 5_000);
        let mut different_left = realm_config_state();
        different_left.apply(&shorter).unwrap();
        different_left.apply(&longer).unwrap();
        let mut different_right = realm_config_state();
        different_right.apply(&longer).unwrap();
        different_right.apply(&shorter).unwrap();

        assert_eq!(different_left, different_right);
        let longer_path = super::revoked_token_path(
            &crate::auth::bearer_token_hash("different"),
            5_000,
            &user_id(),
        );
        assert_eq!(different_left.user_subject_ids.len(), 1);
        assert_eq!(
            different_left.user_subject_ids[&longer_path].dot,
            longer.dot()
        );
    }

    #[test]
    fn compaction_canonicalizes() {
        let hash = crate::auth::bearer_token_hash("legacy");
        let longer = revoke_token_at(20, 1, "legacy", 5_000);
        let equal = revoke_token_at(21, 2, "legacy", 5_000);
        let shorter = revoke_token_at(22, 3, "legacy", 3_000);
        let longer_path = super::revoked_token_path(&hash, 5_000, &user_id());
        let shorter_path = super::revoked_token_path(&hash, 3_000, &user_id());
        let mut state = realm_config_state();
        state.user_subject_ids.insert(
            longer_path.clone(),
            AdminDocumentAttributeVersion {
                value: Some("5000".to_string()),
                dot: longer.dot(),
            },
        );
        state
            .equivalent_value_dots
            .insert(longer_path.clone(), BTreeSet::from([equal.dot()]));
        state.user_subject_ids.insert(
            shorter_path,
            AdminDocumentAttributeVersion {
                value: Some("3000".to_string()),
                dot: shorter.dot(),
            },
        );
        state
            .applied_event_ids
            .extend([longer.event_id, equal.event_id, shorter.event_id]);
        let description_first = set_realm_config_description(23, 4, "first");
        let description_second = set_realm_config_description(24, 5, "second");
        state.apply(&description_first).unwrap();
        state.apply(&description_second).unwrap();

        state.compact_revocations(1_000);

        assert_eq!(state.user_subject_ids.len(), 1);
        assert_eq!(state.user_subject_ids[&longer_path].dot, longer.dot());
        assert!(state.equivalent_value_dots.is_empty());
        assert!(!state.applied_event_ids.contains(&equal.event_id));
        assert!(!state.applied_event_ids.contains(&shorter.event_id));
        assert!(
            state
                .conflicts
                .contains_key(super::REALM_CONFIG_DESCRIPTION_PATH)
        );
        assert!(
            state
                .applied_event_ids
                .contains(&description_first.event_id)
        );
        assert!(
            state
                .applied_event_ids
                .contains(&description_second.event_id)
        );
    }

    #[test]
    fn revocation_origin_count() {
        let mut state = realm_config_state();
        state.apply(&revoke_token_at(30, 1, "one", 5_000)).unwrap();
        state.apply(&revoke_token_at(31, 1, "two", 5_000)).unwrap();
        state
            .apply(&revoke_token_at(32, 2, "three", 5_000))
            .unwrap();
        state.compact_revocations(1_000);

        assert_eq!(state.live_revocation_count(&node(1), 1_000), 2);
        assert_eq!(state.live_revocation_count(&node(2), 1_000), 1);
        assert_eq!(state.live_revocation_count(&node(3), 1_000), 0);
        assert_eq!(
            state.revocation_origin(&crate::auth::bearer_token_hash("one")),
            Some(node(1))
        );
        assert_eq!(
            state.revocation_origin(&crate::auth::bearer_token_hash("three")),
            Some(node(2))
        );
        assert_eq!(state.revocation_origin("missing"), None);
        assert_eq!(user_state().revocation_origin("missing"), None);
    }

    #[test]
    fn owner_conflict_order() {
        let owner_a = user_id_with_seed(1);
        let owner_b = user_id_with_seed(2);
        let first = revoke_token_owned(40, 1, "owned", 5_000, owner_a);
        let second = revoke_token_owned(41, 2, "owned", 5_000, owner_b);
        let mut left = realm_config_state();
        left.apply(&first).unwrap();
        left.apply(&second).unwrap();
        let mut right = realm_config_state();
        right.apply(&second).unwrap();
        right.apply(&first).unwrap();

        assert_eq!(left, right);
        let hash = crate::auth::bearer_token_hash("owned");
        let path = super::revoked_token_path(&hash, 5_000, &owner_a);
        assert_eq!(left.user_subject_ids.len(), 1);
        assert!(left.user_subject_ids.contains_key(&path));
    }

    #[test]
    fn stale_conflict_removed() {
        let owner = user_id();
        let canonical = revoke_token_owned(42, 1, "stale", 5_000, owner);
        let stale = revoke_token_owned(43, 2, "stale", 5_000, owner);
        let path =
            super::revoked_token_path(&crate::auth::bearer_token_hash("stale"), 5_000, &owner);
        let mut state = realm_config_state();
        state.user_subject_ids.insert(
            path.clone(),
            AdminDocumentAttributeVersion {
                value: Some("5000".to_string()),
                dot: canonical.dot(),
            },
        );
        state.conflicts.insert(
            path.clone(),
            AdminDocumentConflict {
                path: path.clone(),
                values: vec![AdminDocumentConflictValue {
                    value: Some("4000".to_string()),
                    dot: stale.dot(),
                }],
            },
        );
        state
            .applied_event_ids
            .extend([canonical.event_id, stale.event_id]);

        state.compact_revocations(1_000);

        assert!(state.conflicts.is_empty());
        assert_eq!(state.user_subject_ids[&path].dot, canonical.dot());
        assert!(state.applied_event_ids.contains(&canonical.event_id));
        assert!(!state.applied_event_ids.contains(&stale.event_id));
    }

    #[test]
    fn expired_count() {
        let mut state = realm_config_state();
        state
            .apply(&revoke_token_at(44, 1, "expired-count", 1_000))
            .unwrap();
        state
            .apply(&revoke_token_at(45, 1, "live-count", 2_000))
            .unwrap();

        assert_eq!(state.live_revocation_count(&node(1), 1_000), 2);
        assert_eq!(state.live_revocation_count(&node(1), 1_001), 2);
        state.compact_revocations(1_001);
        assert_eq!(state.live_revocation_count(&node(1), 1_001), 2);
    }

    #[test]
    fn owner_count() {
        let owner_a = user_id_with_seed(3);
        let owner_b = user_id_with_seed(4);
        let mut state = realm_config_state();
        for (seed, token, owner) in [
            (46u8, "owner-a-one", owner_a),
            (47u8, "owner-a-two", owner_a),
            (48u8, "owner-b-one", owner_b),
        ] {
            state
                .apply(&revoke_token_owned(seed, 1, token, 2_000, owner))
                .unwrap();
        }
        state
            .apply(&revoke_token_owned(49, 2, "owner-a-three", 2_000, owner_a))
            .unwrap();
        state.compact_revocations(1_000);

        assert_eq!(state.live_revocation_count(&node(1), 1_000), 3);
        assert_eq!(state.live_owner_count(&node(1), &owner_a, 1_000), 2);
        assert_eq!(state.live_owner_count(&node(1), &owner_b, 1_000), 1);
        assert_eq!(state.live_owner_count(&node(2), &owner_a, 1_000), 1);
    }

    #[test]
    fn index_counts_grace() {
        let mut state = realm_config_state();
        state.apply(&revoke_token_at(50, 1, "grace", 900)).unwrap();

        let index = state.revocation_index(1_000);
        assert_eq!(index.count(&node(1)), 1);
        assert_eq!(index.materialized(), BTreeMap::new());
        assert_eq!(
            index.origin(&crate::auth::bearer_token_hash("grace")),
            Some(node(1))
        );

        state.compact_revocations(1_200);
        let path =
            super::revoked_token_path(&crate::auth::bearer_token_hash("grace"), 900, &user_id());
        assert!(state.user_subject_ids.contains_key(&path));
        state.compact_revocations(1_201);
        assert!(!state.user_subject_ids.contains_key(&path));
    }

    #[test]
    fn expiry_schedule_bounds() {
        let mut state = realm_config_state();
        let event = revoke_token_at(52, 1, "scheduled", 2_000);
        state.apply(&event).unwrap();

        assert_eq!(
            state.revocation_next_expiry,
            Some(2_000 + REVOCATION_GRACE_SECS)
        );
        assert!(!state.revocation_compaction_due(2_000 + REVOCATION_GRACE_SECS));

        state
            .apply(&set_realm_config_description(53, 2, "unrelated"))
            .unwrap();
        state.advance_revocation_floor(2_100);
        assert!(!state.revocation_compaction_due(2_100));
        assert_eq!(
            state.revocation_next_expiry,
            Some(2_000 + REVOCATION_GRACE_SECS)
        );

        state.compact_revocations(2_000 + REVOCATION_GRACE_SECS + 1);
        assert_eq!(state.revocation_next_expiry, None);
        assert!(
            state
                .user_subject_ids
                .keys()
                .all(|path| !path.contains("scheduled"))
        );
    }

    #[test]
    fn indexed_apply_refreshes() {
        let mut state = realm_config_state();
        state
            .apply(&revoke_token_at(51, 1, "indexed", 2_000))
            .unwrap();
        let mut index = state.revocation_index(1_000);
        let event = revoke_token_at(52, 2, "indexed", 3_000);

        assert_eq!(
            state.apply_revocation_event(&event, &mut index),
            Ok(AdminDocumentApplyStatus::Applied)
        );
        assert_eq!(
            index.origin(&crate::auth::bearer_token_hash("indexed")),
            Some(node(2))
        );
        assert_eq!(index.count(&node(1)), 0);
        assert_eq!(index.count(&node(2)), 1);
        index.compact(&mut state);
        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("indexed"), 3_000)])
        );
    }

    #[test]
    fn divergent_expiry_keeps() {
        // A second expiry for one hash must never erase the revocation; the
        // longest expiry wins so the token stays denied.
        let mut state = realm_config_state();
        state.apply(&revoke_token(1, 1, "token")).unwrap();
        let mut longer = revoke_token(2, 2, "token");
        longer.op = AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash: crate::auth::bearer_token_hash("token"),
            expires_at: 5_000,
            token_owner: user_id(),
        };
        state.apply(&longer).unwrap();

        assert!(state.conflicts.is_empty());
        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("token"), 5_000)])
        );
    }

    #[test]
    fn stale_revocation_applies() {
        // A revocation from a lagging origin sequence must still deny the token.
        let mut state = realm_config_state();
        let mut ahead = revoke_token(1, 1, "ahead");
        ahead.origin_seq = 9;
        state.apply(&ahead).unwrap();
        let behind = revoke_token(2, 1, "behind");

        state.apply(&behind).unwrap();
        assert!(
            state
                .materialized_revoked_tokens()
                .contains_key(&crate::auth::bearer_token_hash("behind"))
        );
    }

    #[test]
    fn compaction_drops_expired() {
        // Expired revocations must leave no reducer residue, so a user revoking
        // token after token cannot grow the persisted state without bound.
        let mut state = realm_config_state();
        let expired = revoke_token(1, 1, "expired");
        let echoed = revoke_token(3, 2, "expired");
        let live = realm_config_event(
            2,
            node(1),
            2,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: crate::auth::bearer_token_hash("live"),
                expires_at: 9_000,
                token_owner: user_id(),
            },
        );
        for event in [&expired, &echoed, &live] {
            state.apply(event).unwrap();
        }

        state.compact_revocations(3_000);

        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("live"), 9_000)])
        );
        assert!(!state.applied_event_ids.contains(&expired.event_id));
        assert!(!state.applied_event_ids.contains(&echoed.event_id));
        assert!(state.applied_event_ids.contains(&live.event_id));
        assert!(state.equivalent_value_dots.is_empty());
    }

    #[test]
    fn compaction_keeps_unexpired() {
        // The expiry boundary matches the materialized set, which keeps an
        // entry while `expires_at >= now`.
        let mut state = realm_config_state();
        state.apply(&revoke_token(1, 1, "token")).unwrap();

        state.compact_revocations(2_000);

        assert_eq!(
            state.materialized_revoked_tokens(),
            BTreeMap::from([(crate::auth::bearer_token_hash("token"), 2_000)])
        );
    }

    #[test]
    fn compaction_spares_paths() {
        let mut state = realm_config_state();
        state
            .apply(&set_realm_config_description(1, 1, "realm"))
            .unwrap();
        state.apply(&revoke_token(2, 1, "token")).unwrap();

        state.compact_revocations(9_000);

        assert!(state.materialized_revoked_tokens().is_empty());
        assert_eq!(
            state.materialized_realm_config_description(),
            Some("realm".to_string())
        );
    }

    #[test]
    fn rejects_malformed_hash() {
        let mut state = realm_config_state();
        let event = realm_config_event(
            4,
            node(1),
            1,
            AdminDocumentClock::default(),
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash: "not-a-hash".to_string(),
                expires_at: 2_000,
                token_owner: user_id(),
            },
        );

        assert_eq!(
            state.apply(&event),
            Err(AdminDocumentReducerError::InvalidTokenHash)
        );
        assert!(state.materialized_revoked_tokens().is_empty());
    }
}
