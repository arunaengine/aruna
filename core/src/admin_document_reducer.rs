use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::NodeId;
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentDot, AdminDocumentEvent, AdminDocumentOperation,
    AdminDocumentRoleDefinition, AdminDocumentTarget,
};
use crate::structs::{
    Actor, BindingScope, DocumentClass, KIND_LABEL_KEY, MetadataRegistryRecord,
    MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig, PlacementOverride,
    PlacementStrategy, QuotaConfig, RealmConfigDocument, RealmDiscoveryConfig, RealmId,
    RealmNodeKind, StrategyBinding,
};
use crate::types::{RoleId, UserId};
use crate::user_update_validation::{
    UserAttributeValidationError, validate_user_attribute_key, validate_user_attribute_value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminDocumentApplyStatus {
    Applied,
    Duplicate,
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
    #[error("placement labels must not set the reserved kind label")]
    ReservedPlacementLabel,
    #[error("placement strategy replica count must not be zero")]
    ZeroPlacementReplicaCount,
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

    repair_realm_config_placement_references(config);
}

fn repair_realm_config_placement_references(config: &mut RealmConfigDocument) {
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
        }
    }

    pub fn apply_operation(
        &mut self,
        actor: &Actor,
        op: AdminDocumentOperation,
    ) -> Result<AdminDocumentEvent, AdminDocumentReducerError> {
        let observed = self.clock.clone();
        let event = AdminDocumentEvent {
            event_id: Ulid::new(),
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
        let stale_on_all_paths = operation_paths(&event.op)
            .iter()
            .all(|path| self.event_is_stale_for_path(event, path));

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
                AdminDocumentOperation::RealmConfigNodePlacementSet { entry },
            ) => {
                if entry.labels.contains_key(KIND_LABEL_KEY) {
                    return Err(AdminDocumentReducerError::ReservedPlacementLabel);
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
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
        }

        if stale_on_all_paths {
            self.applied_event_ids.insert(event.event_id);
            self.clock.advance(event.origin_node_id, event.origin_seq);
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        self.applied_event_ids.insert(event.event_id);
        self.clock.advance(event.origin_node_id, event.origin_seq);
        Ok(AdminDocumentApplyStatus::Applied)
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
pub const REALM_CONFIG_METADATA_REPLICATION_PATH: &str =
    "realm_config.settings.metadata_replication";
pub const REALM_CONFIG_DISCOVERY_PATH: &str = "realm_config.settings.discovery";
pub const REALM_CONFIG_DESCRIPTION_PATH: &str = "realm_config.description";
pub const REALM_CONFIG_QUOTA_PATH: &str = "realm_config.quota";
pub const REALM_CONFIG_DEFAULT_STRATEGY_PATH: &str = "realm_config.placement.default_strategy";

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
        AdminDocumentApplyStatus, AdminDocumentReducerError, AdminDocumentReducerState,
        GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DEFAULT_STRATEGY_PATH,
        REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
        REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
        binding_scope_key, group_role_id_from_path, group_role_path,
        group_role_user_assignment_from_path, group_role_user_assignment_path,
        metadata_replication_value, oidc_provider_value,
        overlay_realm_config_placement_reducer_materialization, realm_config_node_id_from_path,
        realm_config_node_path, realm_config_oidc_provider_id_from_path,
        realm_config_oidc_provider_path, realm_config_placement_node_id_from_path,
        realm_config_placement_node_path, realm_config_placement_strategy_id_from_path,
        realm_config_placement_strategy_path, realm_config_strategy_binding_path,
        realm_config_strategy_binding_scope_key_from_path, realm_discovery_value,
        realm_role_id_from_path, realm_role_path, realm_role_user_assignment_from_path,
        realm_role_user_assignment_path, role_definition_value, user_attribute_path,
        user_subject_id_path,
    };
    use crate::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use crate::structs::{
        Actor, AffinityEffect, AffinityRule, BindingScope, DocumentClass, GroupQuotaOverride,
        KIND_LABEL_KEY, LabelMatch, MetadataReplicationConfig, NodePlacementEntry,
        OidcProviderConfig, Permission, PlacementOverride, PlacementStrategy, QuotaConfig,
        RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind, StrategyBinding,
        UserGroupCapOverride,
    };
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
    fn realm_config_node_placement_rejects_reserved_kind_label() {
        let mut state = realm_config_state();
        let before = state.clone();
        let mut entry = placement_entry(node(11), 100);
        entry
            .labels
            .insert(KIND_LABEL_KEY.to_string(), "Server".to_string());

        assert_eq!(
            state.apply(&set_placement_entry(1, 1, entry)),
            Err(AdminDocumentReducerError::ReservedPlacementLabel)
        );
        assert_eq!(state, before);
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
}
