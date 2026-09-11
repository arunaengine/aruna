use super::*;

impl ConfigValidationCache {
    pub(in crate::document_sync) fn invalidate(&mut self) {
        self.entry = None;
    }

    pub(in crate::document_sync) async fn load(
        &mut self,
        storage: &StorageHandle,
        realm_id: RealmId,
    ) -> Result<(
        Option<&RealmConfigDocument>,
        Option<&AdminDocumentReducerState>,
    )> {
        if self
            .entry
            .as_ref()
            .is_none_or(|(cached, ..)| *cached != realm_id)
        {
            let config = read_admin_realm_config(storage, realm_id).await?;
            let state =
                read_admin_reducer_state(storage, &AdminDocumentTarget::RealmConfig { realm_id })
                    .await?;
            self.entry = Some((realm_id, config, state));
        }
        match &self.entry {
            Some((_, config, state)) => Ok((config.as_ref(), state.as_ref())),
            None => Ok((None, None)),
        }
    }
}
