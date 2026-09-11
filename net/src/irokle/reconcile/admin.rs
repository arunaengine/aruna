use super::*;

pub(in crate::document_sync) async fn apply_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    match (&document_target, &event.target) {
        (DocumentSyncTarget::User { .. }, AdminDocumentTarget::User { .. }) => {
            apply_user_admin_document_operation_to_storage(storage, document_target, event).await
        }
        (DocumentSyncTarget::GroupAuthorization { .. }, AdminDocumentTarget::Group { .. }) => {
            apply_group_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        (DocumentSyncTarget::RealmAuthorization { .. }, AdminDocumentTarget::Realm { .. }) => {
            apply_realm_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        (DocumentSyncTarget::RealmConfig { .. }, AdminDocumentTarget::RealmConfig { .. }) => {
            apply_realm_config_admin_document_operation_to_storage(storage, document_target, event)
                .await
        }
        _ => Err(NetError::Bootstrap(
            "admin document operation target does not match document sync target".to_string(),
        )),
    }
}

pub(in crate::document_sync) async fn persist_stale_admin_document_event(
    storage: &StorageHandle,
    apply_status: AdminDocumentApplyStatus,
    reducer_state: &AdminDocumentReducerState,
) -> Result<bool> {
    match apply_status {
        AdminDocumentApplyStatus::Applied => Ok(false),
        AdminDocumentApplyStatus::Duplicate => Ok(true),
        AdminDocumentApplyStatus::Redundant | AdminDocumentApplyStatus::StaleOriginSequence => {
            storage_batch_write_to(
                storage,
                vec![
                    admin_document_reducer_state_write_entry(reducer_state)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                ],
            )
            .await?;
            Ok(true)
        }
    }
}
