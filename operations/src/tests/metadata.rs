//! Checks key presence, refuses writes to one keyspace and builds audit records for the
//! metadata tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::structs::storage::metadata_registry::{
    MetadataAuditOperation, MetadataAuditRecord, MetadataRegistryRecord,
};
use aruna_storage::storage::handle::StorageHandle;
use byteview::ByteView;

/// A storage that forwards every effect to `real`, except writes and deletes that touch
/// `key_space`, which it refuses as a failed write.
pub(crate) fn refusing(real: StorageHandle, key_space: &'static str) -> StorageHandle {
    let (handle, receivers) = StorageHandle::new();
    for receiver in [receivers.foreground, receivers.bulk] {
        let real = real.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("proxy runtime builds");
            while let Ok((effect, response, ..)) = receiver.recv() {
                let touched = match &effect {
                    StorageEffect::Write { key_space: space, .. }
                    | StorageEffect::Delete { key_space: space, .. } => space == key_space,
                    StorageEffect::BatchWrite { writes, .. } => {
                        writes.iter().any(|(space, ..)| space == key_space)
                    }
                    StorageEffect::BatchDelete { deletes, .. } => {
                        deletes.iter().any(|(space, _)| space == key_space)
                    }
                    _ => false,
                };
                if touched {
                    let error = StorageError::WriteError("refused by the test".to_string());
                    response.send(StorageEvent::Error { error });
                    continue;
                }
                let Event::Storage(event) = runtime.block_on(real.send_storage_effect(effect))
                else {
                    panic!("storage answers with storage events");
                };
                response.send(event);
            }
        });
    }
    handle
}

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
