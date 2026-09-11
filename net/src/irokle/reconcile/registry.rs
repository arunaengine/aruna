use super::*;

pub(in crate::irokle) async fn apply_metadata_registry_upsert_to_storage(
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

pub(in crate::irokle) async fn apply_metadata_graph_lifecycle_to_storage(
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

pub(in crate::irokle) async fn apply_metadata_document_lifecycle_to_storage(
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

/// Fold a replicated PID mapping into the local row inside one transaction, with
/// its sync sidecar and shard-manifest entry. The merge is monotone and derived
/// entirely from the two rows, so replay, reordering, and a frozen holder catching
/// up all converge on the same state and the same manifest revision — and an
/// Active row can never overwrite a local Withdrawn tombstone.
///
/// The same transaction fences the stamped placement against the one the
/// document id decodes to, so a publisher authorized for one shard can neither
/// stamp a document belonging to another shard nor write that shard's manifest.
pub(in crate::irokle) async fn store_pid_mapping(
    storage: &StorageHandle,
    realm_id: RealmId,
    incoming: &PersistentIdMapping,
    placement: PlacementRef,
) -> Result<MetadataPlacementOutcome<bool>> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match derive_placement_txn(storage, realm_id, None, incoming.target, placement, txn_id)
            .await
        {
            Ok(MetadataPlacementOutcome::Accepted(_)) => {}
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
        }
        let merged = match pid_merge_txn(storage, incoming, txn_id).await {
            Ok(Some(merged)) => merged,
            Ok(None) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Accepted(false));
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        };
        let target = persistent_id_target(merged.target);
        let change = persistent_id_change(&merged, placement);
        let mut writes = vec![(
            PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
            ByteView::from(persistent_id_key(merged.target)),
            Value::from(
                merged
                    .to_bytes()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            ),
        )];
        writes.push(
            document_sync_revision_write_entry(&target, &change)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        if let Some(entry) = shard_manifest_write_entry(&target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        {
            writes.push(entry);
        }
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
            .await
        {
            Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(true)),
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
        "persistent id mapping conflicted twice".to_string(),
    ))
}

/// Stores one replicated policy document. The bucket is re-derived from the
/// policy id inside the transaction, and a known id whose definition differs is
/// rejected rather than merged: one id resolves to exactly one rule.
pub(in crate::irokle) async fn store_policy_document(
    storage: &StorageHandle,
    realm_id: RealmId,
    incoming: &PlacementPolicyDocument,
    placement: PlacementRef,
) -> Result<MetadataPlacementOutcome<bool>> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match derive_policy_bucket(storage, realm_id, incoming.policy_id(), placement, txn_id).await
        {
            Ok(MetadataPlacementOutcome::Accepted(_)) => {}
            Ok(outcome) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(match outcome {
                    MetadataPlacementOutcome::Deferred(dependency) => {
                        MetadataPlacementOutcome::Deferred(dependency)
                    }
                    _ => MetadataPlacementOutcome::Rejected,
                });
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        let merged = match policy_merge_txn(storage, incoming, txn_id).await {
            Ok(Ok(Some(merged))) => merged,
            Ok(Ok(None)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(MetadataPlacementOutcome::Accepted(false));
            }
            Ok(Err(())) => {
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
        let target = placement_policy_target(merged.policy_id());
        let change = placement_policy_change(&merged, placement);
        let mut writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            Value::from(
                merged
                    .to_bytes()
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            ),
        )];
        writes.push(
            document_sync_revision_write_entry(&target, &change)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        if let Some(entry) = shard_manifest_write_entry(&target, &change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
        {
            writes.push(entry);
        }
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
            .await
        {
            Ok(()) => return Ok(MetadataPlacementOutcome::Accepted(true)),
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
        "placement policy document conflicted twice".to_string(),
    ))
}

/// The bucket a policy id must ride, derived from the realm config inside the
/// caller's transaction and compared against the stamped placement.
pub(in crate::irokle) async fn derive_policy_bucket(
    storage: &StorageHandle,
    realm_id: RealmId,
    policy_id: Ulid,
    placement: PlacementRef,
    txn_id: TxnId,
) -> Result<MetadataPlacementOutcome<PlacementRef>> {
    if placement == PlacementRef::NIL || placement.strategy_id.is_nil() {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let value = storage_read_from_transaction(
        storage,
        REALM_CONFIG_KEYSPACE.to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(MetadataPlacementOutcome::Deferred(
            DocumentSyncDependency::RealmConfig(realm_id),
        ));
    };
    let config = RealmConfigDocument::from_bytes(&value)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if config.realm_id != realm_id {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    match config.policy_placement(policy_id) {
        Some(derived) if derived == placement => Ok(MetadataPlacementOutcome::Accepted(derived)),
        Some(_) => Ok(MetadataPlacementOutcome::Rejected),
        None => Ok(MetadataPlacementOutcome::Deferred(
            DocumentSyncDependency::PlacementStrategy {
                realm_id,
                strategy_id: placement.strategy_id,
            },
        )),
    }
}

/// `Ok(Ok(None))` when the local row already absorbs the incoming one and
/// `Ok(Err(()))` when the id is reused with a different definition.
pub(in crate::irokle) async fn policy_merge_txn(
    storage: &StorageHandle,
    incoming: &PlacementPolicyDocument,
    txn_id: TxnId,
) -> Result<std::result::Result<Option<PlacementPolicyDocument>, ()>> {
    let target = placement_policy_target(incoming.policy_id());
    let local = storage_read_from_transaction(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?;
    let Some(local) = local else {
        return Ok(Ok(Some(incoming.clone())));
    };
    let mut local = PlacementPolicyDocument::from_bytes(&local)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    match local.merge(incoming) {
        Ok(true) => Ok(Ok(Some(local))),
        Ok(false) => Ok(Ok(None)),
        Err(_) => Ok(Err(())),
    }
}

/// `Ok(None)` when the local row already absorbs the incoming one.
pub(in crate::irokle) async fn pid_merge_txn(
    storage: &StorageHandle,
    incoming: &PersistentIdMapping,
    txn_id: TxnId,
) -> Result<Option<PersistentIdMapping>> {
    let local = storage_read_from_transaction(
        storage,
        PERSISTENT_ID_MAPPING_KEYSPACE.to_string(),
        ByteView::from(persistent_id_key(incoming.target)),
        Some(txn_id),
    )
    .await?;
    let Some(local) = local else {
        return Ok(Some(incoming.clone()));
    };
    let mut local = PersistentIdMapping::from_bytes(&local)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if !local.merge(incoming) {
        return Ok(None);
    }
    Ok(Some(local))
}

pub(in crate::irokle) async fn delete_registry_record(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        let delete = match delete_record_txn(storage, document_id, txn_id).await {
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
            return Ok(());
        };
        let deletes =
            match registry_cleanup_txn(storage, group_id, document_id, &delete, txn_id).await {
                Ok(deletes) => deletes,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(error);
                }
            };
        if deletes.is_empty() {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(());
        }
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, deletes, Vec::new())
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
        "metadata registry cleanup conflicted twice".to_string(),
    ))
}

pub(in crate::irokle) fn metadata_document_delete_matches_graph_lifecycle(
    delete: &MetadataDocumentDeleteRecord,
    record: &MetadataGraphLifecycleRecord,
) -> bool {
    metadata_document_delete_matches_registry(delete, record.group_id, record.document_id)
        && delete.tombstone.graph_iri == record.graph_iri
        && delete.tombstone.updated_at_ms >= record.updated_at_ms
}

pub(in crate::irokle) fn metadata_document_delete_matches_registry(
    delete: &MetadataDocumentDeleteRecord,
    group_id: Ulid,
    document_id: Ulid,
) -> bool {
    delete.tombstone.is_deleted()
        && delete.tombstone.group_id == group_id
        && delete.tombstone.document_id == document_id
}

pub(in crate::irokle) async fn metadata_document_lifecycle_write_entries_if_current(
    storage: &StorageHandle,
    record: &MetadataDocumentLifecycleRecord,
    change: &DocumentSyncChange,
    txn_id: TxnId,
) -> Result<Option<Vec<(String, ByteView, Value)>>> {
    let target = DocumentSyncTarget::MetadataDocumentLifecycle {
        document_id: record.document_id(),
    };
    if lifecycle_stale_txn(storage, &target, change, txn_id).await? {
        return Ok(None);
    }
    let mut acceptance_to_write = None;
    if let MetadataDocumentLifecycleRecord::Upsert { event } = record {
        validate_metadata_event(event)?;
        if create_fence_txn(storage, event, txn_id).await? {
            return Ok(None);
        }

        let accepted = storage_read_from_transaction(
            storage,
            METADATA_CREATE_ACCEPTANCE_KEYSPACE.to_string(),
            metadata_create_acceptance_key(event.record.document_id),
            Some(txn_id),
        )
        .await?
        .map(|value| {
            postcard::from_bytes::<MetadataCreateEventRecord>(&value)
                .map_err(|error| NetError::Bootstrap(error.to_string()))
        })
        .transpose()?;
        if let Some(accepted) = accepted.as_ref() {
            validate_metadata_event(accepted)?;
        }
        if event_is_create(event) {
            if accepted
                .as_ref()
                .is_some_and(|accepted| !same_create_event(accepted, event))
            {
                return Ok(None);
            }
            if accepted.is_none() {
                acceptance_to_write = Some(event.as_ref());
            }
        } else if accepted.as_ref().is_none_or(|accepted| {
            !event_is_create(accepted)
                || !registry_identity_matches(&accepted.record, &event.record)
        }) {
            return Ok(None);
        }
    }

    let mut entries = match record {
        MetadataDocumentLifecycleRecord::Upsert { event } => {
            metadata_create_event_and_pending_projection_write_entries(event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
        }
        MetadataDocumentLifecycleRecord::Delete { event } => {
            metadata_document_delete_write_entries(event)?
        }
    };
    if let Some(event) = acceptance_to_write {
        entries.push(
            metadata_create_acceptance_write_entry(event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
    }
    entries.push(
        document_sync_revision_write_entry(&target, change)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );
    if let Some(manifest) = shard_manifest_write_entry(&target, change)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?
    {
        entries.push(manifest);
    }
    Ok(Some(entries))
}

pub(in crate::irokle) async fn lifecycle_stale_txn(
    storage: &StorageHandle,
    target: &DocumentSyncTarget,
    incoming: &DocumentSyncChange,
    txn_id: TxnId,
) -> Result<bool> {
    let value = storage_read_from_transaction(
        storage,
        DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
        document_sync_revision_key(target),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(false);
    };
    let local: DocumentSyncChange =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(incoming.current <= local.current)
}

pub(in crate::irokle) fn incoming_metadata_registry_stale_or_equal(
    existing: &MetadataRegistryRecord,
    incoming: &MetadataRegistryRecord,
) -> bool {
    metadata_registry_freshness(incoming) <= metadata_registry_freshness(existing)
}

pub(in crate::irokle) fn registry_identity_matches(
    existing: &MetadataRegistryRecord,
    incoming: &MetadataRegistryRecord,
) -> bool {
    existing.realm_id == incoming.realm_id
        && existing.group_id == incoming.group_id
        && existing.document_id == incoming.document_id
        && existing.document_path == incoming.document_path
        && existing.graph_iri == incoming.graph_iri
        && existing.permission_path == incoming.permission_path
        && existing.placement == incoming.placement
        && existing.created_at_ms == incoming.created_at_ms
        && existing.establishing_event_id == incoming.establishing_event_id
}

pub(in crate::irokle) fn registry_identity_valid(record: &MetadataRegistryRecord) -> bool {
    let normalized_path = MetadataRegistryRecord::normalize_document_path(&record.document_path);
    record.establishing_event_id != Ulid::nil()
        && record.document_path == normalized_path
        && record.graph_iri == MetadataRegistryRecord::graph_iri_for(record.document_id)
        && record.permission_path
            == MetadataRegistryRecord::permission_path_for(
                &record.realm_id,
                record.group_id,
                &normalized_path,
                record.document_id,
            )
}

pub(in crate::irokle) fn validate_metadata_event(event: &MetadataCreateEventRecord) -> Result<()> {
    if !registry_identity_valid(&event.record)
        || event.record.last_event_id != event.event_id
        || event_is_create(event) && event.record.establishing_event_id != event.event_id
    {
        return Err(NetError::Bootstrap(
            "replicated metadata event has inconsistent event identity".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::irokle) fn event_is_create(event: &MetadataCreateEventRecord) -> bool {
    matches!(
        &event.payload,
        aruna_core::metadata::MetadataCreateEventPayload::Scaffold { .. }
            | aruna_core::metadata::MetadataCreateEventPayload::RoCrate { .. }
    )
}

pub(in crate::irokle) fn same_create_event(
    accepted: &MetadataCreateEventRecord,
    incoming: &MetadataCreateEventRecord,
) -> bool {
    accepted.event_id == incoming.event_id
        && registry_identity_matches(&accepted.record, &incoming.record)
        && accepted.record.public == incoming.record.public
        && accepted.record.updated_at_ms == incoming.record.updated_at_ms
        && accepted.record.last_event_id == incoming.record.last_event_id
        && accepted.user_id == incoming.user_id
        && accepted.node_id == incoming.node_id
        && accepted.payload == incoming.payload
        && accepted.occurred_at_ms == incoming.occurred_at_ms
}

pub(in crate::irokle) fn metadata_registry_freshness(
    record: &MetadataRegistryRecord,
) -> (u64, Ulid) {
    (record.updated_at_ms, record.last_event_id)
}

pub(in crate::irokle) async fn registry_sidecar_repairs(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<Vec<(String, ByteView, Value)>> {
    let entries = metadata_registry_write_entries(record)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut repairs = Vec::new();
    for (key_space, key, value) in entries.into_iter().skip(1) {
        let current =
            storage_read_from_transaction(storage, key_space.clone(), key.clone(), Some(txn_id))
                .await?;
        if current.as_ref() != Some(&value) {
            repairs.push((key_space, key, value));
        }
    }
    Ok(repairs)
}

pub(in crate::irokle) async fn graph_record_txn(
    storage: &StorageHandle,
    graph_iri: &str,
    txn_id: TxnId,
) -> Result<Option<MetadataGraphLifecycleRecord>> {
    let value = storage_read_from_transaction(
        storage,
        METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        metadata_graph_lifecycle_key(graph_iri),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(None);
    };
    let record: MetadataGraphLifecycleRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    Ok(Some(record))
}

pub(in crate::irokle) async fn delete_record_txn(
    storage: &StorageHandle,
    document_id: Ulid,
    txn_id: TxnId,
) -> Result<Option<MetadataDocumentDeleteRecord>> {
    let value = storage_read_from_transaction(
        storage,
        METADATA_DOCUMENT_LIFECYCLE_KEYSPACE.to_string(),
        metadata_document_lifecycle_key(document_id),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(None);
    };
    let record: MetadataDocumentLifecycleRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    match record {
        MetadataDocumentLifecycleRecord::Delete { event } => Ok(Some(event)),
        MetadataDocumentLifecycleRecord::Upsert { .. } => Ok(None),
    }
}

pub(in crate::irokle) async fn create_fence_txn(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
    txn_id: TxnId,
) -> Result<bool> {
    if let Some(delete) = delete_record_txn(storage, event.record.document_id, txn_id).await? {
        return Ok(event.event_id <= delete.deleted_after_event_id);
    }
    Ok(graph_record_txn(storage, &event.record.graph_iri, txn_id)
        .await?
        .is_some_and(|record| record.is_deleted()))
}

pub(in crate::irokle) async fn record_fenced_txn(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<bool> {
    if let Some(delete) = delete_record_txn(storage, record.document_id, txn_id).await? {
        return Ok(!registry_live_txn(
            storage,
            record.group_id,
            record.document_id,
            &delete,
            txn_id,
        )
        .await?
        .0);
    }
    Ok(graph_record_txn(storage, &record.graph_iri, txn_id)
        .await?
        .is_some_and(|record| record.is_deleted()))
}

pub(in crate::irokle) async fn registry_cleanup_txn(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDocumentDeleteRecord,
    txn_id: TxnId,
) -> Result<Vec<(String, ByteView)>> {
    if !metadata_document_delete_matches_registry(delete, group_id, document_id) {
        return Ok(Vec::new());
    }
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    // A missing registry row means cleanup already ran; an equal delete
    // replay must stay a no-op.
    let Some(value) = storage_read_from_transaction(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    let record: MetadataRegistryRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_event_id
    {
        return Ok(Vec::new());
    }
    Ok(metadata_registry_delete_entries(&record))
}

pub(in crate::irokle) async fn metadata_placement_fence_in_transaction(
    storage: &StorageHandle,
    record: &MetadataRegistryRecord,
    txn_id: TxnId,
) -> Result<MetadataPlacementOutcome<MetadataPlacementFence>> {
    Ok(
        match derive_placement_txn(
            storage,
            record.realm_id,
            Some(record.group_id),
            record.document_id,
            record.placement,
            txn_id,
        )
        .await?
        {
            MetadataPlacementOutcome::Accepted(_) => {
                MetadataPlacementOutcome::Accepted(MetadataPlacementFence)
            }
            MetadataPlacementOutcome::Deferred(dependency) => {
                MetadataPlacementOutcome::Deferred(dependency)
            }
            MetadataPlacementOutcome::Rejected => MetadataPlacementOutcome::Rejected,
        },
    )
}

/// The placement a structured metadata id must ride, derived from the realm
/// config inside the caller's transaction and compared against the `placement`
/// the publisher stamped. The transactional config read is the whole fence: a
/// concurrent config mutation conflicts the commit. `group_id` is compared only
/// when the caller knows it; a PID mapping target carries no group.
pub(in crate::irokle) async fn derive_placement_txn(
    storage: &StorageHandle,
    realm_id: RealmId,
    group_id: Option<Ulid>,
    document_id: Ulid,
    placement: PlacementRef,
    txn_id: TxnId,
) -> Result<MetadataPlacementOutcome<PlacementRef>> {
    let dependency = DocumentSyncDependency::PlacementStrategy {
        realm_id,
        strategy_id: placement.strategy_id,
    };
    if placement == PlacementRef::NIL || placement.strategy_id.is_nil() {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    let value = storage_read_from_transaction(
        storage,
        REALM_CONFIG_KEYSPACE.to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?;
    let Some(value) = value else {
        return Ok(MetadataPlacementOutcome::Deferred(
            DocumentSyncDependency::RealmConfig(realm_id),
        ));
    };
    let config = RealmConfigDocument::from_bytes(&value)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if config.realm_id != realm_id {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    let id = match MetaResourceId::from_bytes(document_id.to_bytes()) {
        Ok(id) => id,
        Err(_) => return Ok(MetadataPlacementOutcome::Rejected),
    };
    let resolved = match config.binding_directory().resolve_id(&id, |strategy_id| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    }) {
        Ok(resolved) => resolved,
        Err(BindingError::UnknownStrategy(_)) => {
            return Ok(MetadataPlacementOutcome::Deferred(dependency));
        }
        Err(BindingError::Unknown(_)) => {
            return Ok(MetadataPlacementOutcome::Deferred(
                DocumentSyncDependency::RealmConfig(realm_id),
            ));
        }
        Err(BindingError::Conflicted(_) | BindingError::BucketOutOfRange(_)) => {
            return Ok(MetadataPlacementOutcome::Rejected);
        }
    };
    let scope_matches = match resolved.scope {
        PlacementScope::Realm(scope_realm) => scope_realm == realm_id,
        PlacementScope::Group(scope_group) => group_id.is_none_or(|group| scope_group == group),
    };
    let derived = PlacementRef {
        strategy_id: resolved.strategy_id,
        shard: u32::from(resolved.bucket.get()),
    };
    if resolved.document_class != DocumentClass::Metadata || !scope_matches || derived != placement
    {
        return Ok(MetadataPlacementOutcome::Rejected);
    }
    Ok(MetadataPlacementOutcome::Accepted(derived))
}

pub(in crate::irokle) fn metadata_document_delete_write_entries(
    record: &MetadataDocumentDeleteRecord,
) -> Result<Vec<(String, ByteView, Value)>> {
    let lifecycle = MetadataDocumentLifecycleRecord::Delete {
        event: record.clone(),
    };
    let mut entries = vec![
        metadata_document_lifecycle_write_entry(&lifecycle)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        metadata_graph_lifecycle_write_entry(&record.tombstone)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    if record.tombstone.is_deleted() {
        let job = MetadataGraphPruneJobRecord::new(
            record.tombstone.graph_iri.clone(),
            unix_timestamp_millis(),
        );
        entries.push(
            metadata_graph_prune_job_write_entry(&job)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
    }
    Ok(entries)
}

pub(in crate::irokle) async fn registry_live_txn(
    storage: &StorageHandle,
    group_id: Ulid,
    document_id: Ulid,
    delete: &MetadataDocumentDeleteRecord,
    txn_id: TxnId,
) -> Result<(bool, Option<MetadataRegistryRecord>)> {
    let target = DocumentSyncTarget::MetadataRegistry {
        group_id,
        document_id,
    };
    let Some(value) = storage_read_from_transaction(
        storage,
        target.storage_keyspace().to_string(),
        target.storage_key(),
        Some(txn_id),
    )
    .await?
    else {
        return Ok((false, None));
    };
    let record: MetadataRegistryRecord =
        postcard::from_bytes(&value).map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let live = record.updated_at_ms > delete.tombstone.updated_at_ms
        || record.last_event_id > delete.deleted_after_event_id;
    Ok((live, Some(record)))
}
