//! Applies realm config events for nodes, OIDC providers, settings and token revocations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl AdminDocumentState {
    pub(super) fn apply_config(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        match &event.op {
            AdminDocumentOperation::ConfigNodeEnsured { node_id, kind } => {
                self.apply_config_node(event, node_id, Some(node_kind_value(kind)));
            }
            AdminDocumentOperation::ConfigNodeRemoved { node_id, cutoffs } => {
                let mut sorted = cutoffs.clone();
                sorted.sort();
                sorted.dedup_by_key(|cutoff| cutoff.set_id);
                if sorted.len() != cutoffs.len() || sorted.len() > MAX_CUTOFFS {
                    return Err(AdminDocumentError::InvalidCutoffs);
                }
                self.apply_config_node(event, node_id, None);
                let value = serde_json::to_string(&sorted)
                    .map_err(|_| AdminDocumentError::InvalidCutoffs)?;
                self.apply_config_setting(event, &publisher_cutoffs_path(node_id), value);
            }
            AdminDocumentOperation::OidcProviderUpserted { provider } => {
                self.apply_oidc_provider(event, &provider.id, Some(oidc_provider_value(provider)));
            }
            AdminDocumentOperation::OidcProviderRemoved { provider_id } => {
                self.apply_oidc_provider(event, provider_id, None);
            }
            AdminDocumentOperation::ConfigSettingsSet {
                metadata_replication,
                discovery,
            } => self.apply_config_settings(event, metadata_replication, discovery),
            AdminDocumentOperation::ConfigDescriptionSet { description } => {
                self.apply_config_setting(event, CONFIG_DESCRIPTION_PATH, description.clone());
            }
            AdminDocumentOperation::ConfigQuotaSet { quota } => {
                self.apply_config_setting(event, CONFIG_QUOTA_PATH, quota_value(quota));
            }
            AdminDocumentOperation::ConfigComputeSet { compute } => {
                self.apply_config_setting(event, CONFIG_COMPUTE_PATH, compute_value(compute));
            }
            AdminDocumentOperation::ConfigFederationSet { settings } => {
                self.apply_config_setting(
                    event,
                    CONFIG_FEDERATION_PATH,
                    federation_value(settings),
                );
            }
            AdminDocumentOperation::ConfigPortalSet { features } => {
                self.apply_config_setting(event, CONFIG_PORTAL_PATH, portal_value(features));
            }
            AdminDocumentOperation::DatabaseSet {
                entry,
                predecessor,
                consent,
            } => {
                let path = database_path(entry.entry_id);
                // A consent holds for the one publication on the version it names.
                if consent.is_some() && !self.consent_current(event, &path, predecessor.as_ref()) {
                    return Ok(AdminApplyStatus::Redundant);
                }
                let value = serde_json::to_string(entry)
                    .map_err(|_| AdminDocumentError::InvalidDatabase)?;
                self.apply_register(event, &path, Some(value));
            }
            AdminDocumentOperation::DatabaseRemoved { entry_id } => {
                self.apply_register(event, &database_path(*entry_id), None);
            }
            AdminDocumentOperation::ConfigPoliciesSet { policies } => {
                self.apply_config_setting(event, CONFIG_POLICIES_PATH, policies_value(policies));
            }
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash,
                expires_at,
                token_owner,
            } => {
                if !valid_token_hash(token_hash) {
                    return Err(AdminDocumentError::InvalidTokenHash);
                }
                let status =
                    self.apply_revocation_full(event, token_hash, *expires_at, *token_owner);
                self.refresh_revocation_expiry();
                return Ok(status);
            }
            _ => return self.apply_placement(event, realm_id),
        }
        Ok(AdminApplyStatus::Applied)
    }

    /// Whether `event` publishes on `predecessor`: it observes that version and no other
    /// version of `path` this state holds. Versions it does not observe are concurrent.
    fn consent_current(
        &self,
        event: &AdminDocumentEvent,
        path: &str,
        predecessor: Option<&AdminDocumentDot>,
    ) -> bool {
        if predecessor.is_some_and(|dot| !event_observes_dot(event, dot)) {
            return false;
        }
        let version = self.user_subject_ids.get(path);
        // Equal values written concurrently share the version the consent names.
        let named = version.is_some_and(|version| Some(&version.dot) == predecessor);
        let equal = self.equivalent_value_dots.get(path).into_iter().flatten();
        let held =
            (version.map(|version| &version.dot).into_iter().chain(equal)).filter(|_| !named);
        let conflicts = self.conflicts.get(path).into_iter();
        let mut dots = held
            .chain(conflicts.flat_map(|conflict| conflict.values.iter().map(|value| &value.dot)));
        !dots.any(|dot| Some(dot) != predecessor && event_observes_dot(event, dot))
    }
}
