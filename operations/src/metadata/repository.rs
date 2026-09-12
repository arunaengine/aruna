use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    METADATA_AUDIT_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_GRAPH_LIFECYCLE_KEYSPACE,
    METADATA_HOLDERS_KEYSPACE, METADATA_INDEX_KEYSPACE, METADATA_MATERIALIZATION_STATUS_KEYSPACE,
};
use aruna_core::metadata::MetadataDocumentLifecycleRecord;
use aruna_core::metadata::{
    MetadataCreateEventRecord, MetadataGraphLifecycleRecord, MetadataMaterializationJobRecord,
    MetadataMaterializationStatusRecord,
};
pub use aruna_core::storage_entries::{
    create_event_entry, create_projection_entries, document_job_entry, document_lifecycle_entry,
    graph_lifecycle_entry, graph_lifecycle_key, lifecycle_manifest_entry, lifecycle_revision_entry,
    materialization_job_entry, materialization_job_key, materialization_status_entry,
    materialization_status_key, metadata_document_key, metadata_registry_key,
    metadata_registry_prefix, registry_write_entries, shard_manifest_entry,
};
use aruna_core::structs::{MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::storage_read::{parse_storage_iter, parse_storage_read};

pub use crate::storage_read::StorageReadError;

pub const LIST_METADATA_PAGE_SIZE: usize = 128;
// Cache fills sweep whole keyspaces; large pages keep the number of storage
// actor round trips low (the data volume is small, the trips dominate).
pub const REGISTRY_FILL_PAGE_SIZE: usize = 8192;

pub fn metadata_audit_key(group_id: GroupId, document_id: Ulid, audit_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(48);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&document_id.to_bytes());
    bytes.extend_from_slice(&audit_id.to_bytes());
    ByteView::from(bytes)
}

pub fn read_registry_effect(group_id: GroupId, document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_INDEX_KEYSPACE.to_string(),
        key: metadata_registry_key(group_id, document_id),
        txn_id,
    })
}

pub fn read_document_registry(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
        key: metadata_document_key(document_id),
        txn_id,
    })
}

pub fn read_status_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
        key: materialization_status_key(document_id),
        txn_id,
    })
}

pub fn read_lifecycle_effect(graph_iri: &str, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        key: graph_lifecycle_key(graph_iri),
        txn_id,
    })
}

pub fn write_registry_effect(
    record: &MetadataRegistryRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: METADATA_INDEX_KEYSPACE.to_string(),
        key: metadata_registry_key(record.group_id, record.document_id),
        value: postcard::to_allocvec(record)?.into(),
        txn_id,
    }))
}

pub fn delete_registry_effect(
    group_id: GroupId,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: METADATA_INDEX_KEYSPACE.to_string(),
        key: metadata_registry_key(group_id, document_id),
        txn_id,
    })
}

pub fn delete_index_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
        key: metadata_document_key(document_id),
        txn_id,
    })
}

pub fn iter_registry_effect(
    group_id: GroupId,
    start_after: Option<Key>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_INDEX_KEYSPACE.to_string(),
        prefix: Some(metadata_registry_prefix(group_id)),
        start: start_after.map(IterStart::After),
        limit: LIST_METADATA_PAGE_SIZE,
        txn_id,
    })
}

pub fn write_graph_lifecycle(
    record: &MetadataGraphLifecycleRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = graph_lifecycle_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_document_lifecycle(
    record: &MetadataDocumentLifecycleRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = document_lifecycle_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_lifecycle_revision(
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
    placement: aruna_core::structs::PlacementRef,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let mut writes = vec![
        document_lifecycle_entry(record)?,
        lifecycle_revision_entry(record, delete_actor, placement)?,
    ];
    if let Some(manifest) = lifecycle_manifest_entry(record, delete_actor, placement)? {
        writes.push(manifest);
    }
    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id,
    }))
}

pub fn delete_holders_effect(
    group_id: GroupId,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: METADATA_HOLDERS_KEYSPACE.to_string(),
        key: metadata_registry_key(group_id, document_id),
        txn_id,
    })
}

// DEFERRED (#280 audit trail): these audit records back the generic audit read endpoint;
// the causal-event contract is the deferred remainder.
pub fn write_audit_effect(
    record: &MetadataAuditRecord,
    audit_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: METADATA_AUDIT_KEYSPACE.to_string(),
        key: metadata_audit_key(record.group_id, record.document_id, audit_id),
        value: postcard::to_allocvec(record)?.into(),
        txn_id,
    }))
}

pub fn write_create_effect(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let writes = create_outbox_entries(record, audit, audit_id, outbox)?;

    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id,
    }))
}

pub fn create_outbox_entries(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = registry_write_entries(record)?;
    writes.push((
        METADATA_AUDIT_KEYSPACE.to_string(),
        metadata_audit_key(record.group_id, record.document_id, audit_id),
        postcard::to_allocvec(audit)?.into(),
    ));
    if let Some(outbox) = outbox {
        writes.push(crate::sync::document_outbox::outbox_write_entry(outbox)?);
        if let Some((lifecycle, change)) = outbox_lifecycle_upsert(outbox)? {
            writes.push(lifecycle_revision_entry(
                &lifecycle,
                outbox.node_id,
                change.placement,
            )?);
            if let Some(manifest) = shard_manifest_entry(&outbox.target, &change)? {
                writes.push(manifest);
            }
        }
    }

    Ok(writes)
}

fn outbox_lifecycle_upsert(
    outbox: &DocumentSyncOutboxRecord,
) -> Result<
    Option<(
        MetadataDocumentLifecycleRecord,
        aruna_core::document::DocumentSyncChange,
    )>,
    ConversionError,
> {
    if !matches!(
        &outbox.target,
        DocumentSyncTarget::MetadataDocumentLifecycle { .. }
    ) {
        return Ok(None);
    }
    let DocumentSyncOutboxEvent::Upsert { bytes, change } = &outbox.event else {
        return Ok(None);
    };
    let lifecycle: MetadataDocumentLifecycleRecord = postcard::from_bytes(bytes)?;
    Ok(Some((lifecycle, *change)))
}

pub fn create_materialization_entries(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
    materialization_status: &MetadataMaterializationStatusRecord,
    materialization_job: &MetadataMaterializationJobRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = create_outbox_entries(record, audit, audit_id, outbox)?;
    writes.push(materialization_status_entry(materialization_status)?);
    writes.push(materialization_job_entry(materialization_job)?);
    writes.push(document_job_entry(materialization_job)?);
    Ok(writes)
}

pub fn event_projection_entries(
    event: &MetadataCreateEventRecord,
    audit: &MetadataAuditRecord,
    outbox: Option<&DocumentSyncOutboxRecord>,
    materialization_status: &MetadataMaterializationStatusRecord,
    materialization_job: &MetadataMaterializationJobRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = vec![create_event_entry(event)?];
    writes.extend(create_materialization_entries(
        &event.record,
        audit,
        event.event_id,
        outbox,
        materialization_status,
        materialization_job,
    )?);
    Ok(writes)
}

pub fn parse_registry_read(
    event: Event,
) -> Result<Option<MetadataRegistryRecord>, StorageReadError> {
    parse_storage_read(event, |bytes| {
        postcard::from_bytes(bytes).map_err(ConversionError::from)
    })
}

pub fn parse_status_read(
    event: Event,
) -> Result<Option<MetadataMaterializationStatusRecord>, StorageReadError> {
    parse_storage_read(event, |bytes| {
        postcard::from_bytes(bytes).map_err(ConversionError::from)
    })
}

pub fn parse_lifecycle_read(
    event: Event,
) -> Result<Option<MetadataGraphLifecycleRecord>, StorageReadError> {
    parse_storage_read(event, |bytes| {
        postcard::from_bytes(bytes).map_err(ConversionError::from)
    })
}

pub fn parse_registry_iter(
    event: Event,
) -> Result<(Vec<MetadataRegistryRecord>, Option<Key>), StorageReadError> {
    parse_storage_iter(event, |bytes| {
        postcard::from_bytes(bytes).map_err(ConversionError::from)
    })
}

pub fn empty_effects() -> Effects {
    smallvec![]
}

/// Untransacted batch delete of index keys in one keyspace, returning how many
/// were dropped.
pub async fn delete_index_keys(
    context: &DriverContext,
    keyspace: &str,
    keys: Vec<Key>,
) -> Result<usize, StorageReadError> {
    if keys.is_empty() {
        return Ok(0);
    }
    let count = keys.len();
    let deletes = keys
        .into_iter()
        .map(|key| (keyspace.to_string(), key))
        .collect();
    let event = context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        }))
        .await;
    match event {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(count),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::WriteError("unexpected event".to_string()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::projector::create_outbox_record;
    use crate::sync::document_outbox::outbox_key;
    use aruna_core::document::{DocumentSyncChange, DocumentSyncChangeKind};
    use aruna_core::keyspaces::{
        DOCUMENT_SYNC_OUTBOX_KEYSPACE, DOCUMENT_SYNC_REVISION_KEYSPACE,
        METADATA_UPDATED_INDEX_KEYSPACE, SHARD_MANIFEST_KEYSPACE,
    };
    use aruna_core::metadata::MetadataCreateEventPayload;
    use aruna_core::storage_entries::{shard_manifest_key, sync_revision_key, updated_index_key};
    use aruna_core::structs::{MetadataAuditOperation, PlacementRef, RealmId};

    fn node(seed: u8) -> NodeId {
        NodeId::from_bytes(&[seed; 32]).expect("node id")
    }

    fn outbox_fixture() -> (
        MetadataRegistryRecord,
        MetadataAuditRecord,
        DocumentSyncOutboxRecord,
    ) {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::from_bytes([4; 16]);
        let document_id = Ulid::from_bytes([5; 16]);
        let event_id = Ulid::from_bytes([6; 16]);
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            shard: 3,
        };
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/outbox-batch".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/outbox-batch",
                document_id,
            ),
            placement,
            holder_node_ids: vec![node(1)],
            created_at_ms: 1,
            updated_at_ms: 2,
            establishing_event_id: event_id,
            last_event_id: event_id,
        };
        let audit = MetadataAuditRecord {
            realm_id,
            group_id,
            document_id,
            graph_iri: record.graph_iri.clone(),
            user_id: Default::default(),
            node_id: node(1),
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: 2,
            details: None,
        };
        let create_event = MetadataCreateEventRecord {
            event_id,
            record: record.clone(),
            user_id: Default::default(),
            node_id: node(1),
            payload: MetadataCreateEventPayload::Scaffold {
                name: "Outbox Batch".to_string(),
                description: "Repository batch regression".to_string(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
            occurred_at_ms: 2,
        };
        (
            record,
            audit,
            create_outbox_record(&create_event, None, false),
        )
    }

    fn batch_entries(
        record: &MetadataRegistryRecord,
        audit: &MetadataAuditRecord,
        audit_id: Ulid,
        outbox: &DocumentSyncOutboxRecord,
    ) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
        create_outbox_entries(record, audit, audit_id, Some(outbox))
    }

    fn batch_effect(
        record: &MetadataRegistryRecord,
        audit: &MetadataAuditRecord,
        audit_id: Ulid,
        outbox: &DocumentSyncOutboxRecord,
        txn_id: Option<TxnId>,
    ) -> Result<Effect, ConversionError> {
        write_create_effect(record, audit, audit_id, Some(outbox), txn_id)
    }

    fn batch_parts(effect: Effect) -> (Vec<(String, ByteView, ByteView)>, Option<TxnId>) {
        let Effect::Storage(StorageEffect::BatchWrite { writes, txn_id }) = effect else {
            panic!("expected one batch write");
        };
        (writes, txn_id)
    }

    fn keys(writes: &[(String, ByteView, ByteView)]) -> Vec<(String, ByteView)> {
        writes
            .iter()
            .map(|(key_space, key, _)| (key_space.clone(), key.clone()))
            .collect()
    }

    #[test]
    fn upsert_outbox_batch() -> Result<(), ConversionError> {
        let (record, audit, outbox) = outbox_fixture();
        let audit_id = Ulid::from_bytes([9; 16]);
        let txn_id = TxnId::from_bytes([10; 16]);
        let writes = batch_entries(&record, &audit, audit_id, &outbox)?;
        let registry_key = metadata_registry_key(record.group_id, record.document_id);
        let document_key = metadata_document_key(record.document_id);
        let updated_key = updated_index_key(record.updated_at_ms, record.document_id);
        let audit_key = metadata_audit_key(record.group_id, record.document_id, audit_id);
        let outbox_row_key = outbox_key(&outbox);
        let revision_key = sync_revision_key(&outbox.target);
        let manifest_key = shard_manifest_key(&outbox.placement, &outbox.target);
        let expected = vec![
            (METADATA_INDEX_KEYSPACE.to_string(), registry_key.clone()),
            (METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(), document_key),
            (METADATA_HOLDERS_KEYSPACE.to_string(), registry_key),
            (METADATA_UPDATED_INDEX_KEYSPACE.to_string(), updated_key),
            (METADATA_AUDIT_KEYSPACE.to_string(), audit_key),
            (DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(), outbox_row_key),
            (DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(), revision_key),
            (SHARD_MANIFEST_KEYSPACE.to_string(), manifest_key),
        ];

        assert_eq!(keys(&writes), expected);
        assert_eq!(writes[0].2, writes[1].2);
        let record_bytes = ByteView::from(postcard::to_allocvec(&record)?);
        assert_eq!(writes[0].2, record_bytes);

        let effect = batch_effect(&record, &audit, audit_id, &outbox, Some(txn_id))?;
        let (effect_writes, effect_txn) = batch_parts(effect);

        assert_eq!(effect_txn, Some(txn_id));
        assert_eq!(effect_writes, writes);
        Ok(())
    }

    #[test]
    fn delete_outbox_batch() -> Result<(), ConversionError> {
        let (record, audit, outbox) = outbox_fixture();
        let audit_id = Ulid::from_bytes([11; 16]);
        let DocumentSyncOutboxEvent::Upsert { change, .. } = &outbox.event else {
            panic!("fixture outbox is an upsert");
        };
        let change = *change;
        let delete = DocumentSyncOutboxRecord {
            event: DocumentSyncOutboxEvent::Delete {
                change: DocumentSyncChange {
                    kind: DocumentSyncChangeKind::Delete,
                    ..change
                },
            },
            ..outbox
        };
        let writes = batch_entries(&record, &audit, audit_id, &delete)?;
        let registry_key = metadata_registry_key(record.group_id, record.document_id);
        let document_key = metadata_document_key(record.document_id);
        let updated_key = updated_index_key(record.updated_at_ms, record.document_id);
        let audit_key = metadata_audit_key(record.group_id, record.document_id, audit_id);
        let expected = vec![
            (METADATA_INDEX_KEYSPACE.to_string(), registry_key.clone()),
            (METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(), document_key),
            (METADATA_HOLDERS_KEYSPACE.to_string(), registry_key),
            (METADATA_UPDATED_INDEX_KEYSPACE.to_string(), updated_key),
            (METADATA_AUDIT_KEYSPACE.to_string(), audit_key),
            (
                DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
                outbox_key(&delete),
            ),
        ];

        assert_eq!(writes.len(), 6);
        assert_eq!(keys(&writes), expected);
        for (key_space, _, _) in &writes {
            assert_ne!(key_space.as_str(), DOCUMENT_SYNC_REVISION_KEYSPACE);
            assert_ne!(key_space.as_str(), SHARD_MANIFEST_KEYSPACE);
        }
        Ok(())
    }
}
