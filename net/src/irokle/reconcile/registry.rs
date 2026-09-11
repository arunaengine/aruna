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

pub(in crate::document_sync) async fn apply_metadata_graph_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataGraphLifecycleRecord,
    primary_bytes: Vec<u8>,
) -> Result<bool> {
    if !record.is_deleted() {
        return Ok(false);
    }
    let (key_space, key, _) = metadata_graph_lifecycle_write_entry(record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete = match delete_record_txn(storage, record.document_id, txn_id).await {
            Ok(delete) => delete,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let Some(delete) = delete else {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        };
        let (registry_live, registry_row) = match registry_live_txn(
            storage,
            record.group_id,
            record.document_id,
            &delete,
            txn_id,
        )
        .await
        {
            Ok(live) => live,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        if !metadata_document_delete_matches_graph_lifecycle(&delete, record) || registry_live {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }
        let deletes = registry_row
            .as_ref()
            .map(metadata_registry_delete_entries)
            .unwrap_or_default();
        let writes = vec![(key_space.clone(), key.clone(), primary_bytes.clone().into())];
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, deletes, writes).await
        {
            Ok(()) => return Ok(true),
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
        "metadata graph lifecycle conflicted twice".to_string(),
    ))
}

pub(in crate::document_sync) async fn apply_metadata_document_lifecycle_to_storage(
    storage: &StorageHandle,
    record: &MetadataDocumentLifecycleRecord,
    change: DocumentSyncChange,
) -> Result<bool> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let writes = match metadata_document_lifecycle_write_entries_if_current(
            storage, record, &change, txn_id,
        )
        .await
        {
            Ok(writes) => writes,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let cleanup_delete = if let MetadataDocumentLifecycleRecord::Delete { event } = record
            && event.tombstone.is_deleted()
        {
            let current = match delete_record_txn(storage, record.document_id(), txn_id).await {
                Ok(current) => current,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
            if writes.is_some() {
                Some(event.clone())
            } else {
                current
            }
        } else {
            None
        };
        let deletes = if let Some(delete) = cleanup_delete.as_ref() {
            match registry_cleanup_txn(
                storage,
                delete.tombstone.group_id,
                delete.tombstone.document_id,
                delete,
                txn_id,
            )
            .await
            {
                Ok(deletes) => deletes,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            }
        } else {
            Vec::new()
        };
        if writes.is_none() && deletes.is_empty() {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            deletes,
            writes.unwrap_or_default(),
        )
        .await
        {
            Ok(()) => return Ok(true),
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
        "metadata document lifecycle conflicted twice".to_string(),
    ))
}
