use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    HARVEST_PROVENANCE_KEYSPACE, HARVEST_SOURCE_KEYSPACE, REPOSITORY_CONNECTOR_INDEX_KEYSPACE,
    REPOSITORY_CONNECTOR_SECRET_KEYSPACE,
};
use aruna_core::structs::{
    HarvestProvenance, HarvestSource, RepositoryConnector, RepositoryConnectorSecret,
    harvest_provenance_key, harvest_provenance_prefix,
};
use aruna_core::types::{GroupId, Key, TxnId};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

pub const HARVEST_SCAN_PAGE_SIZE: usize = 128;

pub fn connector_key(group_id: GroupId, connector_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&connector_id.to_bytes());
    ByteView::from(bytes)
}

pub fn connector_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn connector_secret_key(connector_id: Ulid) -> Key {
    ByteView::from(connector_id.to_bytes().to_vec())
}

pub fn source_key(group_id: GroupId, source_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&source_id.to_bytes());
    ByteView::from(bytes)
}

pub fn source_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn provenance_key(namespace: &str, source_record_id: &str) -> Key {
    ByteView::from(harvest_provenance_key(namespace, source_record_id))
}

pub fn provenance_prefix(namespace: &str) -> Key {
    ByteView::from(harvest_provenance_prefix(namespace))
}

pub fn read_connector_effect(
    group_id: GroupId,
    connector_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: REPOSITORY_CONNECTOR_INDEX_KEYSPACE.to_string(),
        key: connector_key(group_id, connector_id),
        txn_id,
    })
}

pub fn read_source_effect(group_id: GroupId, source_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: HARVEST_SOURCE_KEYSPACE.to_string(),
        key: source_key(group_id, source_id),
        txn_id,
    })
}

pub fn write_source_effect(
    record: &HarvestSource,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: HARVEST_SOURCE_KEYSPACE.to_string(),
        key: source_key(record.group_id, record.source_id),
        value: record.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn read_provenance_effect(
    namespace: &str,
    source_record_id: &str,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: HARVEST_PROVENANCE_KEYSPACE.to_string(),
        key: provenance_key(namespace, source_record_id),
        txn_id,
    })
}

pub fn write_provenance_effect(
    record: &HarvestProvenance,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: HARVEST_PROVENANCE_KEYSPACE.to_string(),
        key: provenance_key(&record.namespace, &record.source_record_id),
        value: record.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn iter_provenance_effect(
    namespace: &str,
    start_after: Option<Key>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: HARVEST_PROVENANCE_KEYSPACE.to_string(),
        prefix: Some(provenance_prefix(namespace)),
        start: start_after.map(IterStart::After),
        limit: HARVEST_SCAN_PAGE_SIZE,
        txn_id,
    })
}

pub fn parse_connector_read(
    event: Event,
) -> Result<Option<RepositoryConnector>, StorageReadError> {
    parse_storage_read(event, RepositoryConnector::from_bytes)
}

pub fn parse_source_read(event: Event) -> Result<Option<HarvestSource>, StorageReadError> {
    parse_storage_read(event, HarvestSource::from_bytes)
}

pub fn parse_provenance_read(
    event: Event,
) -> Result<Option<HarvestProvenance>, StorageReadError> {
    parse_storage_read(event, HarvestProvenance::from_bytes)
}

fn parse_storage_read<T>(
    event: Event,
    parse: impl FnOnce(&[u8]) -> Result<T, ConversionError>,
) -> Result<Option<T>, StorageReadError> {
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|bytes| parse(bytes.as_ref()).map_err(StorageReadError::Conversion))
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(StorageReadError::Storage(error)),
        _ => Err(StorageReadError::Storage(StorageError::ReadError)),
    }
}

/// Value builders for the connector write batch. Public so the create operation
/// keeps the connector and its secret in one atomic batch.
pub fn connector_writes(
    connector: &RepositoryConnector,
    secret: Option<&RepositoryConnectorSecret>,
) -> Result<Vec<(String, Key, ByteView)>, ConversionError> {
    let mut writes = vec![(
        REPOSITORY_CONNECTOR_INDEX_KEYSPACE.to_string(),
        connector_key(connector.group_id, connector.connector_id),
        connector.to_bytes()?.into(),
    )];
    if let Some(secret) = secret {
        writes.push((
            REPOSITORY_CONNECTOR_SECRET_KEYSPACE.to_string(),
            connector_secret_key(secret.connector_id),
            secret.to_bytes()?.into(),
        ));
    }
    Ok(writes)
}

#[derive(Debug, Error, PartialEq)]
pub enum StorageReadError {
    #[error(transparent)]
    Storage(StorageError),
    #[error(transparent)]
    Conversion(ConversionError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_key_is_group_scoped() {
        let group = Ulid::from_bytes([1u8; 16]);
        let key = connector_key(group, Ulid::from_bytes([2u8; 16]));
        assert!(key.as_ref().starts_with(connector_prefix(group).as_ref()));
        assert_eq!(key.as_ref().len(), 32);
    }

    #[test]
    fn provenance_key_scans_within_namespace() {
        let key = provenance_key("ns", "rec-1");
        assert!(key.as_ref().starts_with(provenance_prefix("ns").as_ref()));
    }
}
