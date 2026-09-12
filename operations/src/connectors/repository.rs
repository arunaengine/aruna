use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, SOURCE_CONNECTOR_INDEX_KEYSPACE, SOURCE_CONNECTOR_SECRET_KEYSPACE,
};
use aruna_core::structs::{BlobVersion, BlobVersionState, SourceConnector, SourceConnectorSecret};
use aruna_core::types::{GroupId, Key, TxnId};
use byteview::ByteView;
use ulid::Ulid;

use crate::storage_read::{parse_storage_iter, parse_storage_read};

pub use crate::storage_read::StorageReadError;

pub const LIST_SOURCE_CONNECTOR_PAGE_SIZE: usize = 128;
pub const CONNECTOR_REFERENCE_SCAN_PAGE_SIZE: usize = 128;

pub fn source_connector_key(group_id: GroupId, connector_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&connector_id.to_bytes());
    ByteView::from(bytes)
}

pub fn source_connector_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn connector_secret_key(connector_id: Ulid) -> Key {
    ByteView::from(connector_id.to_bytes().to_vec())
}

pub fn read_connector_effect(
    group_id: GroupId,
    connector_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
        key: source_connector_key(group_id, connector_id),
        txn_id,
    })
}

pub fn read_secret_effect(connector_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
        key: connector_secret_key(connector_id),
        txn_id,
    })
}

pub fn delete_connector_effect(
    group_id: GroupId,
    connector_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
        key: source_connector_key(group_id, connector_id),
        txn_id,
    })
}

pub fn delete_secret_effect(connector_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Delete {
        key_space: SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
        key: connector_secret_key(connector_id),
        txn_id,
    })
}

pub fn iter_connectors_effect(
    group_id: GroupId,
    start_after: Option<Key>,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
        prefix: Some(source_connector_prefix(group_id)),
        start: start_after.map(IterStart::After),
        limit: LIST_SOURCE_CONNECTOR_PAGE_SIZE,
        txn_id,
    })
}

pub fn reference_scan_effect(start_after: Option<Key>, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: CONNECTOR_REFERENCE_SCAN_PAGE_SIZE,
        txn_id,
    })
}

pub fn parse_connector_read(event: Event) -> Result<Option<SourceConnector>, StorageReadError> {
    parse_storage_read(event, SourceConnector::from_bytes)
}

pub fn parse_secret_read(event: Event) -> Result<Option<SourceConnectorSecret>, StorageReadError> {
    parse_storage_read(event, SourceConnectorSecret::from_bytes)
}

pub fn parse_connector_iter(
    event: Event,
) -> Result<(Vec<SourceConnector>, Option<Key>), StorageReadError> {
    parse_storage_iter(event, SourceConnector::from_bytes)
}

pub fn parse_version_iter(
    event: Event,
) -> Result<(Vec<BlobVersion>, Option<Key>), StorageReadError> {
    parse_storage_iter(event, BlobVersion::from_bytes)
}

pub fn references_connector(version: &BlobVersion, connector_id: Ulid) -> bool {
    matches!(
        &version.state,
        BlobVersionState::Reference { source, .. } if source.connector_id == Some(connector_id)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::{SourceConnector, SourceConnectorKind};
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn sample_record() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([4u8; 16]),
            Ulid::from_bytes([3u8; 16]),
            "source-a".to_string(),
            SourceConnectorKind::S3,
            HashMap::from([
                ("bucket".to_string(), "reads".to_string()),
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    #[test]
    fn prefix_matches_group() {
        let record = sample_record();
        let prefix = source_connector_prefix(record.group_id);
        let key = source_connector_key(record.group_id, record.connector_id);

        assert!(key.as_ref().starts_with(prefix.as_ref()));
        assert_eq!(key.as_ref().len(), 32);
    }

    #[test]
    fn secret_key_encoding() {
        let record = sample_record();
        let key = connector_secret_key(record.connector_id);

        assert_eq!(key.as_ref(), record.connector_id.to_bytes().as_slice());
    }

    #[test]
    fn decodes_connector_page() {
        let record = sample_record();
        let next_key = source_connector_key(record.group_id, Ulid::from_bytes([6u8; 16]));
        let event = Event::Storage(StorageEvent::IterResult {
            values: vec![(
                source_connector_key(record.group_id, record.connector_id),
                record.to_bytes().unwrap().into(),
            )],
            next_start_after: Some(next_key.clone()),
        });

        let (records, next_start_after) = parse_connector_iter(event).unwrap();

        assert_eq!(records, vec![record]);
        assert_eq!(next_start_after.unwrap().as_ref(), next_key.as_ref());
    }
}
