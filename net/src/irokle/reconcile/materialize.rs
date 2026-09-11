use super::*;

pub(in crate::document_sync) fn target_write_entry(
    target: DocumentSyncTarget,
    value: Value,
) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

pub(in crate::document_sync) async fn apply_create_event(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
    target: DocumentSyncTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match create_fence_txn(storage, event, txn_id).await {
            Ok(true) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        let writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            ByteView::from(bytes.clone()),
        )];
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
            .await
        {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "metadata create admission conflicted twice".to_string(),
    ))
}
