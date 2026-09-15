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
            AdminDocumentOperation::ConfigNodeRemoved { node_id } => {
                self.apply_config_node(event, node_id, None);
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
                self.apply_config_setting(
                    event,
                    CONFIG_DESCRIPTION_PATH,
                    description.clone(),
                );
            }
            AdminDocumentOperation::ConfigQuotaSet { quota } => {
                self.apply_config_setting(event, CONFIG_QUOTA_PATH, quota_value(quota));
            }
            AdminDocumentOperation::ConfigComputeSet { compute } => {
                self.apply_config_setting(event, CONFIG_COMPUTE_PATH, compute_value(compute));
            }
            AdminDocumentOperation::ConfigPoliciesSet { policies } => {
                self.apply_config_setting(
                    event,
                    CONFIG_POLICIES_PATH,
                    policies_value(policies),
                );
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
