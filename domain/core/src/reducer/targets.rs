//! Materializes the reducer state into per target views for users, groups and realm config.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::paths::{
    CONFIG_COMPUTE_PATH, CONFIG_DESCRIPTION_PATH, CONFIG_DISCOVERY_PATH, CONFIG_EMAIL_PATH,
    CONFIG_FEDERATION_PATH, CONFIG_MERGE_PATH, CONFIG_POLICIES_PATH, CONFIG_PORTAL_PATH,
    CONFIG_QUOTA_PATH, CONFIG_REPLICAS_PATH, DISPLAY_NAME_PATH, GROUP_LOCATION_PATH,
    GROUP_OWNER_PATH, GROUP_POLICIES_PATH, METADATA_REPLICATION_PATH, NODE_HISTORY_PREFIX,
    PLUGIN_ENTRY_PREFIX, PLUGIN_INSTALL_PREFIX, REALM_ID_PATH, SECRET_INFO_PREFIX,
    USER_ALIAS_PREFIX, USER_NAME_PATH, VAULT_ENTRY_PREFIX, compute_from_value, config_node_path,
    config_oidc_path, decode_metadata_replication, decode_oidc_provider, decode_realm_discovery,
    group_assignment_role, group_role_path, group_user_path, metadata_replication_value,
    node_history_path, parse_config_oidc, parse_group_role, parse_realm_role, policies_from_value,
    publisher_cutoffs_path, quota_from_value, realm_assignment_role, realm_discovery_value,
    realm_role_path, realm_user_path, secret_info_path, user_alias_path, user_attribute_path,
    user_subject_path,
};
use super::*;

impl AdminDocumentState {
    pub fn materialized_user_name(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::User { .. }) {
            return None;
        }

        self.user_name
            .as_ref()
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_subject_ids(&self) -> BTreeSet<String> {
        if !matches!(&self.target, AdminDocumentTarget::User { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter(|(path, _)| !path.starts_with(USER_ALIAS_PREFIX))
            .filter_map(|(_, version)| version.value.clone())
            .collect()
    }

    /// Whether `alias` is linked to this user without a conflicting replicated claim.
    pub fn materialized_alias(&self, alias: &UserId) -> bool {
        let path = user_alias_path(alias);
        !self.conflicts.contains_key(&path)
            && self
                .user_subject_ids
                .get(&path)
                .is_some_and(|version| version.value.is_some())
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
}

impl AdminDocumentState {
    /// The cutoffs a node's removal recorded, if the realm config removed it.
    pub fn publisher_cutoffs(&self, node_id: &NodeId) -> Option<Vec<PublisherCutoff>> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }
        self.user_subject_ids
            .get(&publisher_cutoffs_path(node_id))
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    pub fn materialized_group_name(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(DISPLAY_NAME_PATH)
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_group_realm(&self) -> Option<RealmId> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_ID_PATH)
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

    /// The group's default dataset location, if an admin set one.
    pub fn group_location(
        &self,
    ) -> Option<crate::structs::storage::dataset_location::DatasetLocation> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(GROUP_LOCATION_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    pub fn materialized_group_roles(&self) -> BTreeSet<RoleId> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| version.value.as_ref().map(|_| path))
            .filter_map(|path| parse_group_role(path))
            .collect()
    }

    pub fn materialized_group_assignments(&self) -> BTreeMap<RoleId, BTreeSet<UserId>> {
        if !matches!(&self.target, AdminDocumentTarget::Group { .. }) {
            return BTreeMap::new();
        }

        let active_roles = self.materialized_group_roles();

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let role_id = group_assignment_role(path)?;
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
}

impl AdminDocumentState {
    pub fn materialized_realm_roles(&self) -> BTreeSet<RoleId> {
        if !matches!(&self.target, AdminDocumentTarget::Realm { .. }) {
            return BTreeSet::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| version.value.as_ref().map(|_| path))
            .filter_map(|path| parse_realm_role(path))
            .collect()
    }

    pub fn materialized_realm_assignments(&self) -> BTreeMap<RoleId, BTreeSet<UserId>> {
        if !matches!(&self.target, AdminDocumentTarget::Realm { .. }) {
            return BTreeMap::new();
        }

        let active_roles = self.materialized_realm_roles();

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let role_id = realm_assignment_role(path)?;
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

    pub fn materialized_oidc_providers(&self) -> BTreeMap<String, OidcProviderConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }

        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let provider_id = parse_config_oidc(path)?;
                let provider = version.value.as_deref().and_then(decode_oidc_provider)?;

                (provider.id == provider_id).then(|| (provider_id.to_string(), provider))
            })
            .collect()
    }

    pub fn materialized_metadata_replication(&self) -> Option<MetadataReplicationConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(METADATA_REPLICATION_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(decode_metadata_replication)
    }

    pub fn materialized_realm_discovery(&self) -> Option<RealmDiscoveryConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_DISCOVERY_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(decode_realm_discovery)
    }

    pub fn materialized_realm_description(&self) -> Option<String> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_DESCRIPTION_PATH)
            .and_then(|version| version.value.clone())
    }

    pub fn materialized_realm_policies(&self) -> Option<Vec<crate::request_policy::RequestPolicy>> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_POLICIES_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(policies_from_value)
    }

    /// The realm compute configuration the events agree on, if any.
    pub fn materialized_realm_compute(&self) -> Option<RealmComputeConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_COMPUTE_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(compute_from_value)
    }

    pub fn materialized_realm_federation(&self) -> Option<crate::federation::FederationSettings> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_FEDERATION_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    /// Whether replicas count toward group quotas, as the events agree, if any event set it.
    pub fn materialized_realm_replicas(&self) -> Option<ReplicaQuota> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_REPLICAS_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    /// The node the events agree runs account merges; a conflict or no event names none.
    pub fn materialized_merge_coordinator(&self) -> Option<NodeId> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_MERGE_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| value.parse().ok())
    }

    /// The realm portal feature defaults the events agree on, if any.
    pub fn materialized_realm_portal(&self) -> Option<BTreeMap<String, bool>> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_PORTAL_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    /// The realm email profile; never set, removed and conflicting profiles all mean no email.
    pub fn materialized_realm_email(
        &self,
    ) -> Option<crate::structs::execution::email::RealmEmailProfile> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }
        self.user_subject_ids
            .get(CONFIG_EMAIL_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str(value).ok())
    }

    /// The current profile and every conflicting one, so a resolving write can retire their entries.
    pub fn realm_email_candidates(
        &self,
    ) -> Vec<crate::structs::execution::email::RealmEmailProfile> {
        let conflicting = (self.conflicts.get(CONFIG_EMAIL_PATH).into_iter())
            .flat_map(|conflict| &conflict.values)
            .filter_map(|candidate| candidate.value.as_deref());
        (self.user_subject_ids.get(CONFIG_EMAIL_PATH))
            .and_then(|version| version.value.as_deref())
            .into_iter()
            .chain(conflicting)
            .filter_map(|value| serde_json::from_str(value).ok())
            .collect()
    }

    /// Realm vault records sorted by entry key. A never set, deleted or conflicting entry is absent,
    /// and so is a named secret value no present secret record points at.
    pub fn materialized_realm_vault(&self) -> Vec<crate::realm_vault::RealmVaultRecord> {
        use crate::realm_vault::{RealmSecretRef, RealmVaultPurpose, RealmVaultRecord};
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Vec::new();
        }
        let referenced: BTreeSet<RealmSecretRef> = (self.materialized_realm_secrets().iter())
            .map(|info| RealmSecretRef::named(info.value_id))
            .collect();
        let mut records: Vec<RealmVaultRecord> = (self.user_subject_ids)
            .range(VAULT_ENTRY_PREFIX.to_string()..)
            .take_while(|(path, _)| path.starts_with(VAULT_ENTRY_PREFIX))
            .filter(|(path, _)| !self.conflicts.contains_key(*path))
            .filter_map(|(_, version)| serde_json::from_str(version.value.as_deref()?).ok())
            .filter(|record: &RealmVaultRecord| {
                let reference = RealmSecretRef {
                    entry: record.entry,
                    revision: record.revision,
                };
                record.entry.purpose != RealmVaultPurpose::NamedSecret
                    || referenced.contains(&reference)
            })
            .collect();
        records.sort_by_key(|record| record.entry.key());
        records
    }

    /// Named realm secret records sorted by name. A never set, deleted or conflicting name is absent.
    pub fn materialized_realm_secrets(&self) -> Vec<crate::secrets::SecretRecord> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Vec::new();
        }
        (self
            .user_subject_ids
            .range(SECRET_INFO_PREFIX.to_string()..))
        .take_while(|(path, _)| path.starts_with(SECRET_INFO_PREFIX))
        .filter(|(path, _)| !self.conflicts.contains_key(*path))
        .filter_map(|(_, version)| serde_json::from_str(version.value.as_deref()?).ok())
        .collect()
    }

    /// The current record of one realm secret name and every record competing with it. A write
    /// of that name supersedes them all, so their values may retire; nothing else may.
    pub fn realm_secret_candidates(
        &self,
        name: &crate::secrets::SecretName,
    ) -> Vec<crate::secrets::SecretRecord> {
        let path = secret_info_path(name);
        let conflicting = (self.conflicts.get(&path).into_iter())
            .flat_map(|conflict| &conflict.values)
            .filter_map(|candidate| candidate.value.as_deref());
        (self.user_subject_ids.get(&path))
            .and_then(|version| version.value.as_deref())
            .into_iter()
            .chain(conflicting)
            .filter_map(|value| serde_json::from_str(value).ok())
            .collect()
    }

    /// Published plugin versions sorted by name and version. A withdrawn version is absent, and
    /// so is a conflicting one unless every competing entry publishes the same artifact.
    pub fn materialized_plugins(&self) -> Vec<crate::plugins::PublishedPlugin> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Vec::new();
        }
        let prefixed = |path: &&String| path.starts_with(PLUGIN_ENTRY_PREFIX);
        let settled = (self
            .user_subject_ids
            .range(PLUGIN_ENTRY_PREFIX.to_string()..))
        .take_while(|(path, _)| prefixed(path))
        .filter(|(path, _)| !self.conflicts.contains_key(*path))
        .filter_map(|(path, version)| {
            Some((path, serde_json::from_str(version.value.as_deref()?).ok()?))
        });
        let agreed = (self.conflicts.range(PLUGIN_ENTRY_PREFIX.to_string()..))
            .take_while(|(path, _)| prefixed(path))
            .filter_map(|(path, conflict)| {
                Some((path, crate::plugins::same_artifact(&conflict.values)?))
            });
        let entries: BTreeMap<&String, crate::plugins::PublishedPlugin> =
            settled.chain(agreed).collect();
        entries.into_values().collect()
    }

    /// What the realm config holds at one install's path, deletions and conflicts included,
    /// which [`Self::materialized_installs`] leaves out.
    pub fn install_entry(&self, install_id: &ulid::Ulid) -> InstallEntry {
        let path = crate::reducer::paths::plugin_install_path(install_id);
        let parse = |value: &Option<String>| {
            (value.as_deref()).and_then(|value| serde_json::from_str(value).ok())
        };
        if let Some(conflict) = self.conflicts.get(&path) {
            return InstallEntry::Conflict {
                deletion: conflict.values.iter().any(|value| value.value.is_none()),
                installs: conflict
                    .values
                    .iter()
                    .filter_map(|value| parse(&value.value))
                    .collect(),
            };
        }
        match self.user_subject_ids.get(&path) {
            None => InstallEntry::Absent,
            Some(version) => InstallEntry::Settled(parse(&version.value).map(Box::new)),
        }
    }

    /// Plugin install records sorted by install id. A deleted or conflicting one is absent.
    pub fn materialized_installs(&self) -> Vec<crate::plugin_installs::PluginInstall> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Vec::new();
        }
        (self.user_subject_ids)
            .range(PLUGIN_INSTALL_PREFIX.to_string()..)
            .take_while(|(path, _)| path.starts_with(PLUGIN_INSTALL_PREFIX))
            .filter(|(path, _)| !self.conflicts.contains_key(*path))
            .filter_map(|(_, version)| serde_json::from_str(version.value.as_deref()?).ok())
            .collect()
    }

    /// Display names of realm nodes, by node id. A cleared or conflicting name is absent.
    pub fn materialized_node_names(&self) -> BTreeMap<NodeId, String> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }
        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = path.strip_prefix("realm_config.node_names.")?;
                let node_id = NodeId::from_str(node_id).ok()?;
                Some((node_id, version.value.clone()?))
            })
            .collect()
    }

    /// Map places of realm nodes, by node id. A cleared or conflicting place is absent.
    pub fn materialized_node_places(&self) -> BTreeMap<NodeId, NodePlace> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return BTreeMap::new();
        }
        self.user_subject_ids
            .iter()
            .filter_map(|(path, version)| {
                let node_id = path.strip_prefix("realm_config.node_places.")?;
                let node_id = NodeId::from_str(node_id).ok()?;
                let place = serde_json::from_str(version.value.as_deref()?).ok()?;
                Some((node_id, place))
            })
            .collect()
    }

    pub fn materialized_realm_quota(&self) -> Option<QuotaConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(CONFIG_QUOTA_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(quota_from_value)
    }
}

impl AdminDocumentState {
    pub(super) fn apply_user_name(&mut self, event: &AdminDocumentEvent, name: &str) {
        self.user_name = self.reduce_value(
            event,
            USER_NAME_PATH,
            self.user_name.clone(),
            Some(name.to_string()),
        );
    }

    pub(super) fn apply_user_subject(
        &mut self,
        event: &AdminDocumentEvent,
        subject_id: &str,
        value: Option<String>,
    ) {
        let path = user_subject_path(subject_id);
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

    pub(super) fn apply_user_attribute(
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
}

impl AdminDocumentState {
    pub(super) fn apply_group_created(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
        display_name: &str,
        owner: &UserId,
    ) {
        self.apply_group_field(event, DISPLAY_NAME_PATH, Some(display_name.to_string()));
        self.apply_group_field(event, REALM_ID_PATH, Some(realm_id.to_string()));
        self.apply_group_field(event, GROUP_OWNER_PATH, Some(owner.to_string()));
    }

    pub(super) fn apply_group_field(
        &mut self,
        event: &AdminDocumentEvent,
        path: &str,
        value: Option<String>,
    ) {
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

    pub(super) fn apply_group_role(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        value: String,
    ) {
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

    pub(super) fn remove_group_role(&mut self, event: &AdminDocumentEvent, role_id: &RoleId) {
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

    pub(super) fn apply_group_assignment(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        user_id: &UserId,
        value: Option<String>,
    ) {
        let path = group_user_path(role_id, user_id);
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
}

impl AdminDocumentState {
    pub(super) fn apply_realm_role(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        value: String,
    ) {
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

    pub(super) fn apply_realm_assignment(
        &mut self,
        event: &AdminDocumentEvent,
        role_id: &RoleId,
        user_id: &UserId,
        value: Option<String>,
    ) {
        let path = realm_user_path(role_id, user_id);
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

    pub(super) fn apply_config_node(
        &mut self,
        event: &AdminDocumentEvent,
        node_id: &NodeId,
        value: Option<String>,
    ) {
        let path = config_node_path(node_id);
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

    pub(super) fn apply_value(
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

    pub(super) fn apply_oidc_provider(
        &mut self,
        event: &AdminDocumentEvent,
        provider_id: &str,
        value: Option<String>,
    ) {
        let path = config_oidc_path(provider_id);
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

    pub(super) fn apply_config_settings(
        &mut self,
        event: &AdminDocumentEvent,
        metadata_replication: &MetadataReplicationConfig,
        discovery: &RealmDiscoveryConfig,
    ) {
        self.apply_config_setting(
            event,
            METADATA_REPLICATION_PATH,
            metadata_replication_value(metadata_replication),
        );
        self.apply_config_setting(
            event,
            CONFIG_DISCOVERY_PATH,
            realm_discovery_value(discovery),
        );
    }

    pub(super) fn apply_config_setting(
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
}

/// The stored form of one membership event; a removal keeps the clock it was made with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum MembershipEvent {
    Added {
        dot: AdminDocumentDot,
    },
    Removed {
        dot: AdminDocumentDot,
        observed: crate::admin_documents::AdminDocumentClock,
    },
}

impl AdminDocumentState {
    /// Keeps one infrastructure add or removal event of `node_id` for good, under its own path.
    pub(super) fn record_membership(
        &mut self,
        event: &AdminDocumentEvent,
        node_id: &NodeId,
        removal: bool,
    ) {
        let record = match removal {
            false => MembershipEvent::Added { dot: event.dot() },
            true => MembershipEvent::Removed {
                dot: event.dot(),
                observed: event.observed.clone(),
            },
        };
        let value = serde_json::to_string(&record).ok();
        self.apply_value(event, node_history_path(node_id, event.event_id), value);
    }

    /// Every infrastructure membership interval the realm config has seen, also of removed
    /// and re-added nodes. A removal closes the adds its event had seen.
    pub fn node_intervals(&self) -> Vec<crate::admin_documents::NodeInterval> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return Vec::new();
        }
        let events = self.membership_events();
        let mut adds = Vec::new();
        let mut removals = Vec::new();
        for (node_id, event) in &events {
            match event {
                MembershipEvent::Added { dot } => adds.push((*node_id, *dot)),
                MembershipEvent::Removed { dot, observed } => {
                    removals.push((*node_id, *dot, observed));
                }
            }
        }
        adds.extend(self.legacy_adds(&events));
        (adds.into_iter())
            .map(|(node_id, added)| {
                let removed = (removals.iter())
                    .filter(|(removed_node, dot, observed)| {
                        *removed_node == node_id
                            && (observed.observes(&added)
                                || (dot.origin_node_id == added.origin_node_id
                                    && dot.origin_seq > added.origin_seq))
                    })
                    .map(|(_, dot, _)| *dot)
                    .collect();
                crate::admin_documents::NodeInterval {
                    node_id,
                    added,
                    removed,
                }
            })
            .collect()
    }

    /// Every stored membership event with its node; an unreadable entry is skipped.
    fn membership_events(&self) -> Vec<(NodeId, MembershipEvent)> {
        let history = (self.user_subject_ids)
            .range(NODE_HISTORY_PREFIX.to_string()..)
            .take_while(|(path, _)| path.starts_with(NODE_HISTORY_PREFIX));
        let event = |(path, version): (&String, &AdminAttributeVersion)| {
            let node_id = (path.strip_prefix(NODE_HISTORY_PREFIX))
                .and_then(|rest| rest.split_once('.'))
                .and_then(|(node, _)| NodeId::from_str(node).ok())?;
            let event = serde_json::from_str(version.value.as_deref()?).ok()?;
            Some((node_id, event))
        };
        history.filter_map(event).collect()
    }

    /// The infrastructure adds the config holds without a history entry, as in a realm config
    /// written before the history was kept. Each keeps the dot of its add.
    fn legacy_adds(&self, events: &[(NodeId, MembershipEvent)]) -> Vec<(NodeId, AdminDocumentDot)> {
        let recorded: BTreeSet<(NodeId, AdminDocumentDot)> = (events.iter())
            .filter_map(|(node_id, event)| match event {
                MembershipEvent::Added { dot } => Some((*node_id, *dot)),
                MembershipEvent::Removed { .. } => None,
            })
            .collect();
        let mut legacy = Vec::new();
        for (node_id, kind) in self.materialized_config_nodes() {
            if !matches!(kind, RealmNodeKind::Server | RealmNodeKind::Management) {
                continue;
            }
            let path = config_node_path(&node_id);
            let Some(version) = self.user_subject_ids.get(&path) else {
                continue;
            };
            let equivalent = self.equivalent_value_dots.get(&path).into_iter().flatten();
            for dot in std::iter::once(&version.dot).chain(equivalent) {
                if !recorded.contains(&(node_id, *dot)) {
                    legacy.push((node_id, *dot));
                }
            }
        }
        legacy
    }

    /// Stores the history entry of every legacy add as its add event would have, so each node
    /// and each rerun writes the same entries once. Answers how many it wrote.
    pub fn keep_legacy_history(&mut self) -> usize {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return 0;
        }
        let legacy = self.legacy_adds(&self.membership_events());
        let kept = legacy
            .into_iter()
            .filter(|(node_id, dot)| self.keep_legacy_add(node_id, *dot));
        kept.count()
    }

    /// Keeps the legacy add of `node_id` before a removal or a new kind replaces the add.
    pub(super) fn keep_legacy_node(&mut self, node_id: &NodeId) {
        let legacy = self.legacy_adds(&self.membership_events());
        for (_, dot) in legacy
            .iter()
            .filter(|(legacy_node, _)| legacy_node == node_id)
        {
            self.keep_legacy_add(node_id, *dot);
        }
    }

    fn keep_legacy_add(&mut self, node_id: &NodeId, dot: AdminDocumentDot) -> bool {
        let Ok(value) = serde_json::to_string(&MembershipEvent::Added { dot }) else {
            return false;
        };
        // The path the add event writes since the history is kept, so the entry is the same.
        let path = node_history_path(node_id, dot.event_id);
        let version = AdminAttributeVersion {
            value: Some(value),
            dot,
        };
        self.user_subject_ids.insert(path, version);
        true
    }
}

/// An install's path in the realm config: never written, settled (`None` is the deletion
/// `remove_install` writes), or in conflict between candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallEntry {
    Absent,
    Settled(Option<Box<crate::plugin_installs::PluginInstall>>),
    Conflict {
        /// One candidate is the deletion.
        deletion: bool,
        /// The candidates that hold a record.
        installs: Vec<crate::plugin_installs::PluginInstall>,
    },
}
