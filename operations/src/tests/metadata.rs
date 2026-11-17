//! Checks key presence and builds audit records for the metadata tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_storage::StorageHandle;
use byteview::ByteView;

pub(crate) async fn storage_key_exists(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> bool {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}

/// The create audit record a node writes for `record`.
pub(crate) fn create_audit(record: &MetadataRegistryRecord) -> MetadataAuditRecord {
    MetadataAuditRecord {
        realm_id: record.realm_id,
        group_id: record.group_id,
        document_id: record.document_id,
        graph_iri: record.graph_iri.clone(),
        user_id: Default::default(),
        node_id: NodeId::from_bytes(&[1u8; 32]).expect("node id"),
        operation: MetadataAuditOperation::Create,
        occurred_at_ms: record.updated_at_ms,
        details: None,
    }
}
