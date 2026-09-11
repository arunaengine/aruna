use super::*;

pub(in crate::document_sync) async fn apply_metadata_registry_upsert_to_storage(
    storage: &StorageHandle,
    record: MetadataRegistryRecord,
    primary_bytes: Vec<u8>,
) -> Result<MetadataPlacementOutcome<()>> {
    if !registry_identity_valid(&record) {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let mut base_entries = metadata_registry_write_entries(&record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if let Some((_, _, value)) = base_entries.first_mut() {
        *value = primary_bytes.into();
    }
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id: record.group_id,
        document_id: record.document_id,
    };
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete_present = match delete_record_txn(storage, record.document_id, txn_id).await {
            Ok(delete) => delete.is_some(),
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        match record_fenced_txn(storage, &record, txn_id).await {
            Ok(true) => {
                // The stale local row carries the timestamp half of its index
                // key; the fenced incoming record may be stamped differently.
                let stale = match storage_read_from_transaction(
                    storage,
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                    Some(txn_id),
                )
                .await
                {
                    Ok(value) => value.and_then(|value| {
                        postcard::from_bytes::<MetadataRegistryRecord>(&value).ok()
                    }),
                    Err(error) => {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                };
                let deletes = metadata_registry_delete_entries(stale.as_ref().unwrap_or(&record));
                match storage_batch_delete_and_write_in_transaction(
                    storage,
                    txn_id,
                    deletes,
                    Vec::new(),
                )
                .await
                {
                    Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
                    Err(NetError::Storage(StorageError::TransactionConflict)) => {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        continue;
                    }
                    Err(error) => {
                        let _ = storage
                            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                            .await;
                        return Err(error);
                    }
                }
            }
            Ok(false) => {}
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        match metadata_placement_fence_in_transaction(storage, &record, txn_id).await {
            Ok(MetadataPlacementOutcome::Accepted(MetadataPlacementFence)) => {}
            Ok(MetadataPlacementOutcome::Deferred(dependency)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Deferred(dependency));
            }
            Ok(MetadataPlacementOutcome::Rejected) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Rejected);
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let existing_value = match storage_read_from_transaction(
            storage,
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let existing = match existing_value
            .map(|bytes| postcard::from_bytes::<MetadataRegistryRecord>(&bytes))
            .transpose()
        {
            Ok(existing) => existing,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Bootstrap(error.to_string()));
            }
        };
        if existing
            .as_ref()
            .is_some_and(|existing| !registry_identity_matches(existing, &record))
        {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(MetadataPlacementOutcome::Rejected);
        }
        if let Some(existing) = existing.as_ref().filter(|existing| {
            delete_present || incoming_metadata_registry_stale_or_equal(existing, &record)
        }) {
            let repairs = match registry_sidecar_repairs(storage, existing, txn_id).await {
                Ok(repairs) => repairs,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if repairs.is_empty() {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Accepted(()));
            }
            match storage_batch_delete_and_write_in_transaction(
                storage,
                txn_id,
                Vec::new(),
                repairs,
            )
            .await
            {
                Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
                Err(NetError::Storage(StorageError::TransactionConflict)) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    continue;
                }
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            }
        }

        let entries = base_entries.clone();
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), entries)
            .await
        {
            Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(())),
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
        "metadata registry admission conflicted twice".to_string(),
    ))
}
