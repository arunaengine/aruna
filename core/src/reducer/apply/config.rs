use super::*;

impl AdminDocumentReducerState {
    pub(super) fn apply_config(
        &mut self,
        event: &AdminDocumentEvent,
        realm_id: &RealmId,
    ) -> Result<AdminDocumentApplyStatus, AdminDocumentReducerError> {
        match &event.op {
            AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind } => {
                self.apply_config_node(event, node_id, Some(node_kind_value(kind)));
            }
            AdminDocumentOperation::RealmConfigNodeRemoved { node_id } => {
                self.apply_config_node(event, node_id, None);
            }
            AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider } => {
                self.apply_oidc_provider(event, &provider.id, Some(oidc_provider_value(provider)));
            }
            AdminDocumentOperation::RealmConfigOidcProviderRemoved { provider_id } => {
                self.apply_oidc_provider(event, provider_id, None);
            }
            AdminDocumentOperation::RealmConfigSettingsSet {
                metadata_replication,
                discovery,
            } => self.apply_config_settings(event, metadata_replication, discovery),
            AdminDocumentOperation::RealmConfigDescriptionSet { description } => {
                self.apply_config_setting(
                    event,
                    REALM_CONFIG_DESCRIPTION_PATH,
                    description.clone(),
                );
            }
            AdminDocumentOperation::RealmConfigQuotaSet { quota } => {
                self.apply_config_setting(event, REALM_CONFIG_QUOTA_PATH, quota_value(quota));
            }
            AdminDocumentOperation::RealmConfigComputeSet { compute } => {
                self.apply_config_setting(event, REALM_CONFIG_COMPUTE_PATH, compute_value(compute));
            }
            AdminDocumentOperation::RealmConfigPoliciesSet { policies } => {
                self.apply_config_setting(
                    event,
                    REALM_CONFIG_POLICIES_PATH,
                    policies_value(policies),
                );
            }
            AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash,
                expires_at,
                token_owner,
            } => {
                if !valid_token_hash(token_hash) {
                    return Err(AdminDocumentReducerError::InvalidTokenHash);
                }
                let status =
                    self.apply_revocation_full(event, token_hash, *expires_at, *token_owner);
                self.refresh_revocation_expiry();
                return Ok(status);
            }
            _ => return self.apply_placement(event, realm_id),
        }
        Ok(AdminDocumentApplyStatus::Applied)
    }
}
