use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    METADATA_AUDIT_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE, METADATA_HOLDERS_KEYSPACE,
    METADATA_INDEX_KEYSPACE,
};
use aruna_core::structs::{MetadataAuditRecord, MetadataRegistryRecord};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

pub const LIST_METADATA_PAGE_SIZE: usize = 128;

pub fn metadata_registry_key(group_id: GroupId, document_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&document_id.to_bytes());
    ByteView::from(bytes)
}

pub fn metadata_registry_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn metadata_document_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

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
        start_after,
        limit: LIST_METADATA_PAGE_SIZE,
        txn_id,
    })
}

pub fn iter_all_registry_effect(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_DOCUMENT_INDEX_KEYSPACE.to_string(),
        prefix: None,
        start_after,
        limit: LIST_METADATA_PAGE_SIZE,
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
