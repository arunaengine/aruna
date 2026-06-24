use aruna_core::NodeId;
use aruna_core::document::{DocumentSyncOutboxEvent, DocumentSyncOutboxRecord, DocumentSyncTarget};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
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
    metadata_create_event_write_entry, metadata_document_key,
    metadata_document_lifecycle_revision_write_entry, metadata_document_lifecycle_write_entry,
    metadata_graph_lifecycle_key, metadata_graph_lifecycle_write_entry,
    metadata_materialization_document_job_write_entry, metadata_materialization_job_key,
    metadata_materialization_job_write_entry, metadata_materialization_status_key,
    metadata_materialization_status_write_entry, metadata_registry_key, metadata_registry_prefix,
};
use aruna_core::structs::{MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

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

pub fn read_registry_by_document_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
        key: metadata_document_key(document_id),
        txn_id,
    })
}

pub fn read_materialization_status_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_MATERIALIZATION_STATUS_KEYSPACE.to_string(),
        key: metadata_materialization_status_key(document_id),
        txn_id,
    })
}

pub fn read_graph_lifecycle_effect(graph_iri: &str, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: METADATA_GRAPH_LIFECYCLE_KEYSPACE.to_string(),
        key: metadata_graph_lifecycle_key(graph_iri),
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

pub fn write_document_index_effect(
    record: &MetadataRegistryRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
        key: metadata_document_key(record.document_id),
        value: postcard::to_allocvec(record)?.into(),
        txn_id,
    }))
}

pub fn delete_document_index_effect(document_id: Ulid, txn_id: Option<TxnId>) -> Effect {
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

pub fn iter_all_registry_effect(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: REGISTRY_FILL_PAGE_SIZE,
        txn_id,
    })
}

pub fn write_holders_effect(
    record: &MetadataRegistryRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: METADATA_HOLDERS_KEYSPACE.to_string(),
        key: metadata_registry_key(record.group_id, record.document_id),
        value: postcard::to_allocvec(&record.holder_node_ids)?.into(),
        txn_id,
    }))
}

pub fn write_graph_lifecycle_effect(
    record: &MetadataGraphLifecycleRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = metadata_graph_lifecycle_write_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_document_lifecycle_effect(
    record: &MetadataDocumentLifecycleRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = metadata_document_lifecycle_write_entry(record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_document_lifecycle_with_revision_effect(
    record: &MetadataDocumentLifecycleRecord,
    delete_actor: NodeId,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes: vec![
            metadata_document_lifecycle_write_entry(record)?,
            metadata_document_lifecycle_revision_write_entry(record, delete_actor)?,
        ],
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

pub fn write_metadata_event_effect(
    event: &MetadataCreateEventRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = metadata_create_event_write_entry(event)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_create_event_effect(
    event: &MetadataCreateEventRecord,
) -> Result<Effect, ConversionError> {
    write_metadata_event_effect(event, None)
}

pub fn write_create_records_effect(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    write_create_records_and_outbox_effect(record, audit, audit_id, None, txn_id)
}

pub fn write_create_records_and_outbox_effect(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let writes = create_records_and_outbox_write_entries(record, audit, audit_id, outbox)?;

    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes,
        txn_id,
    }))
}

pub fn create_records_and_outbox_write_entries(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = vec![
        (
            METADATA_INDEX_KEYSPACE.to_string(),
            metadata_registry_key(record.group_id, record.document_id),
            postcard::to_allocvec(record)?.into(),
        ),
        (
            METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
            metadata_document_key(record.document_id),
            postcard::to_allocvec(record)?.into(),
        ),
        (
            METADATA_HOLDERS_KEYSPACE.to_string(),
            metadata_registry_key(record.group_id, record.document_id),
            postcard::to_allocvec(&record.holder_node_ids)?.into(),
        ),
        (
            METADATA_AUDIT_KEYSPACE.to_string(),
            metadata_audit_key(record.group_id, record.document_id, audit_id),
            postcard::to_allocvec(audit)?.into(),
        ),
    ];
    if let Some(outbox) = outbox {
        writes.push(crate::document_sync_outbox::outbox_write_entry(outbox)?);
        if let Some(revision) = document_lifecycle_revision_from_outbox(outbox)? {
            writes.push(revision);
        }
    }

    Ok(writes)
}

fn document_lifecycle_revision_from_outbox(
    outbox: &DocumentSyncOutboxRecord,
) -> Result<Option<(String, ByteView, ByteView)>, ConversionError> {
    if !matches!(
        &outbox.target,
        DocumentSyncTarget::MetadataDocumentLifecycle { .. }
    ) {
        return Ok(None);
    }
    let DocumentSyncOutboxEvent::Upsert { bytes } = &outbox.event else {
        return Ok(None);
    };
    let lifecycle: MetadataDocumentLifecycleRecord = postcard::from_bytes(bytes)?;
    Ok(Some(metadata_document_lifecycle_revision_write_entry(
        &lifecycle,
        outbox.node_id,
    )?))
}

pub fn write_create_records_outbox_and_materialization_effect(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
    materialization_status: &MetadataMaterializationStatusRecord,
    materialization_job: &MetadataMaterializationJobRecord,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let base_writes = create_records_outbox_and_materialization_write_entries(
        record,
        audit,
        audit_id,
        outbox,
        materialization_status,
        materialization_job,
    )?;

    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes: base_writes,
        txn_id,
    }))
}

pub fn create_records_outbox_and_materialization_write_entries(
    record: &MetadataRegistryRecord,
    audit: &MetadataAuditRecord,
    audit_id: Ulid,
    outbox: Option<&DocumentSyncOutboxRecord>,
    materialization_status: &MetadataMaterializationStatusRecord,
    materialization_job: &MetadataMaterializationJobRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = create_records_and_outbox_write_entries(record, audit, audit_id, outbox)?;
    writes.push(metadata_materialization_status_write_entry(
        materialization_status,
    )?);
    writes.push(metadata_materialization_job_write_entry(
        materialization_job,
    )?);
    writes.push(metadata_materialization_document_job_write_entry(
        materialization_job,
    )?);
    Ok(writes)
}

pub fn metadata_event_projection_write_entries(
    event: &MetadataCreateEventRecord,
    audit: &MetadataAuditRecord,
    outbox: Option<&DocumentSyncOutboxRecord>,
    materialization_status: &MetadataMaterializationStatusRecord,
    materialization_job: &MetadataMaterializationJobRecord,
) -> Result<Vec<(String, ByteView, ByteView)>, ConversionError> {
    let mut writes = vec![metadata_create_event_write_entry(event)?];
    writes.extend(create_records_outbox_and_materialization_write_entries(
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
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes)
                    .map_err(|error| StorageReadError::Conversion(error.into()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::ReadError,
        )),
    }
}

pub fn parse_materialization_status_read(
    event: Event,
) -> Result<Option<MetadataMaterializationStatusRecord>, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes)
                    .map_err(|error| StorageReadError::Conversion(error.into()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::ReadError,
        )),
    }
}

pub fn parse_graph_lifecycle_read(
    event: Event,
) -> Result<Option<MetadataGraphLifecycleRecord>, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| {
                postcard::from_bytes(&bytes)
                    .map_err(|error| StorageReadError::Conversion(error.into()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::ReadError,
        )),
    }
}

pub fn parse_registry_iter(
    event: Event,
) -> Result<(Vec<MetadataRegistryRecord>, Option<Key>), StorageReadError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let records = values
                .into_iter()
                .map(|(_, value)| {
                    postcard::from_bytes(&value)
                        .map_err(|error| StorageReadError::Conversion(error.into()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((records, next_start_after))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(
            aruna_core::errors::StorageError::ReadError,
        )),
    }
}

pub fn empty_effects() -> Effects {
    smallvec![]
}

#[derive(Debug)]
pub enum StorageReadError {
    Storage(aruna_core::errors::StorageError),
    Conversion(ConversionError),
}
