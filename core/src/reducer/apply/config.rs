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
}
