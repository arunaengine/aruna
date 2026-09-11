use super::*;

impl AdminDocumentReducerState {
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
}

impl AdminDocumentReducerState {
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
}

impl AdminDocumentReducerState {
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

    /// The realm compute configuration the events agree on, if any.
    pub fn materialized_realm_compute(&self) -> Option<RealmComputeConfig> {
        if !matches!(&self.target, AdminDocumentTarget::RealmConfig { .. }) {
            return None;
        }

        self.user_subject_ids
            .get(REALM_CONFIG_COMPUTE_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(compute_from_value)
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
}

impl AdminDocumentReducerState {
    pub(super) fn apply_user_name(&mut self, event: &AdminDocumentEvent, name: &str) {
        self.user_name = self.reduce_value(
            event,
            USER_NAME_PATH,
            self.user_name.clone(),
            Some(name.to_string()),
        );
    }

    pub(super) fn apply_user_subject_id(
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

impl AdminDocumentReducerState {
    pub(super) fn apply_group_created(
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

    pub(super) fn apply_group_field(&mut self, event: &AdminDocumentEvent, path: &str, value: Option<String>) {
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

    pub(super) fn apply_group_role(&mut self, event: &AdminDocumentEvent, role_id: &RoleId, value: String) {
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

    pub(super) fn apply_group_role_removed(&mut self, event: &AdminDocumentEvent, role_id: &RoleId) {
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

    pub(super) fn apply_group_role_user_assignment(
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
}

impl AdminDocumentReducerState {
    pub(super) fn apply_realm_role(&mut self, event: &AdminDocumentEvent, role_id: &RoleId, value: String) {
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

    pub(super) fn apply_realm_role_user_assignment(
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

    pub(super) fn apply_realm_config_node(
        &mut self,
        event: &AdminDocumentEvent,
        node_id: &NodeId,
        value: Option<String>,
    ) {
        let path = realm_config_node_path(node_id);
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

    pub(super) fn apply_realm_config_oidc_provider(
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

    pub(super) fn apply_realm_config_settings(
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

    pub(super) fn apply_realm_config_setting(
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
