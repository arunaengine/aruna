//! Builds the keys, storage effects and read parsers of the repository connector and secret rows
//! that harvest connectors store and the source resolver reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageReadError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{CONNECTOR_INDEX_KEYSPACE, CONNECTOR_SECRET_KEYSPACE};
use aruna_core::structs::execution::harvest::RepositoryConnector;
use aruna_core::structs::execution::source_connector::SourceConnectorSecret;
use aruna_core::types::{GroupId, Key, TxnId};
use byteview::ByteView;
use ulid::Ulid;

use crate::storage_read::parse_storage_read;

/// Secret config key of the optional read token an Invenio connector keeps.
pub const INVENIO_TOKEN: &str = "token";

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

pub fn read_connector_effect(
    group_id: GroupId,
    connector_id: Ulid,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: CONNECTOR_INDEX_KEYSPACE.to_string(),
        key: connector_key(group_id, connector_id),
        txn_id,
    })
}

pub fn read_secret_effect(connector_id: Ulid, txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: CONNECTOR_SECRET_KEYSPACE.to_string(),
        key: connector_secret_key(connector_id),
        txn_id,
    })
}

pub fn parse_connector_read(event: Event) -> Result<Option<RepositoryConnector>, StorageReadError> {
    parse_storage_read(event, RepositoryConnector::from_bytes)
}

pub fn parse_secret_read(event: Event) -> Result<Option<SourceConnectorSecret>, StorageReadError> {
    parse_storage_read(event, SourceConnectorSecret::from_bytes)
}

/// Value builders for the connector write batch. Public so the create operation
/// keeps the connector and its secret in one atomic batch.
pub fn connector_writes(
    connector: &RepositoryConnector,
    secret: Option<&SourceConnectorSecret>,
) -> Result<Vec<(String, Key, ByteView)>, ConversionError> {
    let mut writes = vec![(
        CONNECTOR_INDEX_KEYSPACE.to_string(),
        connector_key(connector.group_id, connector.connector_id),
        connector.to_bytes()?.into(),
    )];
    if let Some(secret) = secret {
        writes.push((
            CONNECTOR_SECRET_KEYSPACE.to_string(),
            connector_secret_key(secret.connector_id),
            secret.to_bytes()?.into(),
        ));
    }
    Ok(writes)
}

#[cfg(test)]
mod tests {
    use super::*;

    // the connector key is group-scoped
    #[test]
    fn connector_key_scoped() {
        let group = Ulid::from_bytes([1u8; 16]);
        let key = connector_key(group, Ulid::from_bytes([2u8; 16]));
        assert!(key.as_ref().starts_with(connector_prefix(group).as_ref()));
        assert_eq!(key.as_ref().len(), 32);
    }
}
