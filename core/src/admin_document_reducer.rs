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
    MetadataReplicationConfig, OidcProviderConfig, RealmDiscoveryConfig, RealmId, RealmNodeKind,
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
        }
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
        if event.origin_seq <= self.clock.sequence_for(&event.origin_node_id) {
            return Ok(AdminDocumentApplyStatus::StaleOriginSequence);
        }

        match (&event.target, &event.op) {
            (
                AdminDocumentTarget::Group { .. },
                AdminDocumentOperation::GroupCreated {
                    realm_id,
                    display_name,
                },
            ) => {
                self.apply_group_created(event, realm_id, display_name);
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
            _ => return Err(AdminDocumentReducerError::UnsupportedTarget),
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
    ) {
        self.apply_group_field(
            event,
            GROUP_DISPLAY_NAME_PATH,
            Some(display_name.to_string()),
        );
        self.apply_group_field(event, GROUP_REALM_ID_PATH, Some(realm_id.to_string()));
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

        match self.reduce_role_value(event, &path, role_id, current, value) {
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

        match self.reduce_role_value(event, &path, role_id, current, value) {
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

    fn reduce_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        current: Option<AdminDocumentAttributeVersion>,
        value: Option<String>,
    ) -> Option<AdminDocumentAttributeVersion> {
        let dot = event.dot();

        if self.conflict_is_observed(event, path) {
            self.conflicts.remove(path);
            return Some(AdminDocumentAttributeVersion { value, dot });
        }

        if self.conflicts.contains_key(path) {
            self.record_conflict_value(path, value, dot);
            return None;
        }

        let Some(current) = current else {
            return Some(AdminDocumentAttributeVersion { value, dot });
        };

        if event_observes_dot(event, &current.dot) {
            return Some(AdminDocumentAttributeVersion { value, dot });
        }

        if current.value != value {
            self.record_conflict_value(path, current.value, current.dot);
            self.record_conflict_value(path, value, dot);
            return None;
        }

        Some(current)
    }

    fn reduce_role_value(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        role_id: &RoleId,
        current: Option<AdminDocumentAttributeVersion>,
        value: String,
    ) -> Option<AdminDocumentAttributeVersion> {
        let dot = event.dot();

        if self.conflict_is_observed(event, path) {
            self.conflicts.remove(path);
            return Some(AdminDocumentAttributeVersion {
                value: Some(value),
                dot,
            });
        }

        if self.conflicts.contains_key(path) {
            self.record_conflict_value(path, Some(value), dot);
            return None;
        }

        let Some(current) = current else {
            return Some(AdminDocumentAttributeVersion {
                value: Some(value),
                dot,
            });
        };

        if current.value.as_deref() == Some(value.as_str()) {
            if event_observes_dot(event, &current.dot) {
                return Some(AdminDocumentAttributeVersion {
                    value: Some(value),
                    dot,
                });
            }
            return Some(current);
        }

        if role_values_are_compatible(role_id, current.value.as_deref(), &value) {
            if current
                .value
                .as_deref()
                .is_some_and(|value| role_value_is_definition_for(value, role_id))
            {
                return Some(current);
            }
            return Some(AdminDocumentAttributeVersion {
                value: Some(value),
                dot,
            });
        }

        if event_observes_dot(event, &current.dot) {
            return Some(AdminDocumentAttributeVersion {
                value: Some(value),
                dot,
            });
        }

        self.record_conflict_value(path, current.value, current.dot);
        self.record_conflict_value(path, Some(value), dot);
        None
    }

    fn conflict_is_observed(&self, event: &AdminDocumentEvent, path: &str) -> bool {
        self.conflicts.get(path).is_some_and(|conflict| {
            conflict
                .values
                .iter()
                .all(|value| event_observes_dot(event, &value.dot))
        })
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

const USER_NAME_PATH: &str = "user.name";
const GROUP_DISPLAY_NAME_PATH: &str = "group.display_name";
const GROUP_REALM_ID_PATH: &str = "group.realm_id";
const REALM_CONFIG_METADATA_REPLICATION_PATH: &str = "realm_config.settings.metadata_replication";
const REALM_CONFIG_DISCOVERY_PATH: &str = "realm_config.settings.discovery";

fn event_observes_dot(event: &AdminDocumentEvent, dot: &AdminDocumentDot) -> bool {
    event.observed.observes(dot)
        || (event.origin_node_id == dot.origin_node_id && event.origin_seq > dot.origin_seq)
}

fn role_definition_value(role: &AdminDocumentRoleDefinition) -> String {
    serde_json::to_string(role).expect("admin document role definition serializes")
}

fn role_values_are_compatible(role_id: &RoleId, current_value: Option<&str>, value: &str) -> bool {
    let Some(current_value) = current_value else {
        return false;
    };
    let legacy_value = role_id.to_string();

    (current_value == legacy_value && role_value_is_definition_for(value, role_id))
        || (value == legacy_value && role_value_is_definition_for(current_value, role_id))
}

fn role_value_is_definition_for(value: &str, role_id: &RoleId) -> bool {
    serde_json::from_str::<AdminDocumentRoleDefinition>(value)
        .map(|role| role.role_id == *role_id)
        .unwrap_or(false)
}

fn user_attribute_path(key: &str) -> String {
    format!("user.attributes.{key}")
}

fn user_subject_id_path(subject_id: &str) -> String {
    format!("user.subject_ids.{subject_id}")
}

fn group_role_path(role_id: &RoleId) -> String {
    format!("group.roles.{role_id}")
}

fn group_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("group.roles.{role_id}.assigned_users.{user_id}")
}

fn realm_role_path(role_id: &RoleId) -> String {
    format!("realm.roles.{role_id}")
}

fn realm_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("realm.roles.{role_id}.assigned_users.{user_id}")
}

fn realm_config_node_path(node_id: &NodeId) -> String {
    format!("realm_config.nodes.{node_id}")
}

fn realm_config_oidc_provider_path(provider_id: &str) -> String {
    format!("realm_config.oidc_providers.{provider_id}")
}

fn metadata_replication_value(metadata_replication: &MetadataReplicationConfig) -> String {
    serde_json::to_string(metadata_replication)
        .expect("admin document metadata replication config serializes")
}

fn realm_discovery_value(discovery: &RealmDiscoveryConfig) -> String {
    serde_json::to_string(discovery).expect("admin document realm discovery config serializes")
}

fn oidc_provider_value(provider: &OidcProviderConfig) -> String {
    serde_json::to_string(provider).expect("admin document OIDC provider config serializes")
}

fn realm_node_kind_value(kind: &RealmNodeKind) -> String {
    match kind {
        RealmNodeKind::Management => "management",
        RealmNodeKind::Server => "server",
        RealmNodeKind::Local => "local",
    }
    .to_string()
}

fn group_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("group.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

fn group_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    let path = path.strip_prefix("group.roles.")?;
    let (role_id, _) = path.split_once(".assigned_users.")?;

    Ulid::from_string(role_id).ok()
}

fn realm_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("realm.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

fn realm_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    let path = path.strip_prefix("realm.roles.")?;
    let (role_id, _) = path.split_once(".assigned_users.")?;

    Ulid::from_string(role_id).ok()
}

fn realm_config_node_id_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.nodes.")?;
    NodeId::from_str(node_id).ok()
}

fn realm_config_oidc_provider_id_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.oidc_providers.")
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

fn realm_node_kind_from_value(value: &str) -> Option<RealmNodeKind> {
    match value {
        "management" => Some(RealmNodeKind::Management),
        "server" => Some(RealmNodeKind::Server),
        "local" => Some(RealmNodeKind::Local),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdminDocumentApplyStatus, AdminDocumentReducerError, AdminDocumentReducerState,
        GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DISCOVERY_PATH,
        REALM_CONFIG_METADATA_REPLICATION_PATH, USER_NAME_PATH, metadata_replication_value,
        oidc_provider_value, realm_discovery_value, role_definition_value,
    };
    use crate::admin_documents::{
        AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation,
        AdminDocumentRoleDefinition, AdminDocumentTarget,
    };
    use crate::structs::{
        Actor, MetadataReplicationConfig, OidcProviderConfig, Permission, RealmDiscoveryConfig,
        RealmId, RealmNodeKind,
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
    fn stale_origin_sequence_is_ignored() {
        let mut state = user_state();
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

        state.apply(&newer).unwrap();
        let before_stale = state.clone();

        assert_eq!(
            state.apply(&stale),
            Ok(AdminDocumentApplyStatus::StaleOriginSequence)
        );
        assert_eq!(state, before_stale);
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
    fn group_created_materializes_display_name_and_realm_id() {
        let mut state = group_state();
        let realm_id = realm_id();

        state
            .apply(&create_group(1, 1, "Engineering", realm_id))
            .unwrap();

        assert_eq!(
            state.materialized_group_display_name().as_deref(),
            Some("Engineering")
        );
        assert_eq!(state.materialized_group_realm_id(), Some(realm_id));
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
    fn group_role_created_and_legacy_added_do_not_conflict() {
        let mut state = group_state();
        let role_id = role_id(3);
        let role = role_definition(role_id, "Group admin");
        let expected_value = role_definition_value(&role);

        state.apply(&create_group_role(1, 1, role)).unwrap();
        state.apply(&add_group_role(2, 2, role_id)).unwrap();

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
    fn realm_legacy_added_and_role_created_do_not_conflict() {
        let mut state = realm_state();
        let role_id = role_id(3);
        let role = role_definition(role_id, "Realm admin");
        let expected_value = role_definition_value(&role);

        state.apply(&add_realm_role(1, 1, role_id)).unwrap();
        state.apply(&create_realm_role(2, 2, role)).unwrap();

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
}
