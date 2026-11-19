//! Builds storage keys and effects for harvest sources and provenance records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageReadError};
use aruna_core::events::Event;
use aruna_core::keyspaces::{HARVEST_PROVENANCE_KEYSPACE, HARVEST_SOURCE_KEYSPACE};
use aruna_core::structs::execution::harvest::{
    HarvestProvenance, HarvestSource, harvest_provenance_key, harvest_provenance_prefix,
};
use aruna_core::types::{GroupId, Key, TxnId};
use byteview::ByteView;
use ulid::Ulid;

use crate::storage_read::parse_storage_read;

pub const HARVEST_PAGE_SIZE: usize = 128;

pub fn source_key(group_id: GroupId, source_id: Ulid) -> Key {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&group_id.to_bytes());
    bytes.extend_from_slice(&source_id.to_bytes());
    ByteView::from(bytes)
}

pub fn source_prefix(group_id: GroupId) -> Key {
    ByteView::from(group_id.to_bytes().to_vec())
}

pub fn provenance_key(group_id: GroupId, namespace: &str, source_record_id: &str) -> Key {
    ByteView::from(harvest_provenance_key(
        group_id,
        namespace,
        source_record_id,
    ))
}

pub fn provenance_prefix(group_id: GroupId, namespace: &str) -> Key {
    ByteView::from(harvest_provenance_prefix(group_id, namespace))
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
    group_id: GroupId,
    namespace: &str,
    source_record_id: &str,
    txn_id: Option<TxnId>,
) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: HARVEST_PROVENANCE_KEYSPACE.to_string(),
        key: provenance_key(group_id, namespace, source_record_id),
        txn_id,
    })
}

pub fn write_provenance_effect(
    record: &HarvestProvenance,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: HARVEST_PROVENANCE_KEYSPACE.to_string(),
        key: provenance_key(record.group_id, &record.namespace, &record.source_record_id),
        value: record.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn parse_source_read(event: Event) -> Result<Option<HarvestSource>, StorageReadError> {
    parse_storage_read(event, HarvestSource::from_bytes)
}

pub fn parse_provenance_read(event: Event) -> Result<Option<HarvestProvenance>, StorageReadError> {
    parse_storage_read(event, HarvestProvenance::from_bytes)
}

#[cfg(test)]
mod pure_tests {
    use super::*;

    // the provenance key scans within one group
    #[test]
    fn provenance_scans_group() {
        let group = Ulid::from_bytes([3u8; 16]);
        let key = provenance_key(group, "ns", "rec-1");
        assert!(
            key.as_ref()
                .starts_with(provenance_prefix(group, "ns").as_ref())
        );
        assert!(
            !key.as_ref()
                .starts_with(provenance_prefix(Ulid::from_bytes([4u8; 16]), "ns").as_ref())
        );
    }
}
