//! Builds a driver context backed by temporary storage and a sample document for device tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_storage::storage::FjallStorage;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

use crate::device::sync::folders::FolderError;
use crate::device::sync::repository::folder_entry;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::SyncedFolder;
use aruna_core::types::{Key, TxnId, Value};

pub(crate) async fn context() -> (TempDir, DriverContext) {
    let directory = tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    (
        directory,
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        },
    )
}

pub(crate) fn record() -> MetadataRegistryRecord {
    let document_id = Ulid::from_bytes([2u8; 16]);
    MetadataRegistryRecord {
        realm_id: RealmId::from_bytes([3u8; 32]),
        group_id: Ulid::from_bytes([1u8; 16]),
        document_id,
        document_path: "notes".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: "/notes".to_string(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 2,
        establishing_event_id: Ulid::from_bytes([4u8; 16]),
        last_event_id: Ulid::from_bytes([5u8; 16]),
    }
}

/// Stores rows outside any transaction; `true` once storage took them.
pub(crate) async fn write_rows(
    context: &DriverContext,
    writes: Vec<(String, Key, Value)>,
    txn_id: Option<TxnId>,
) -> bool {
    let write = StorageEffect::BatchWrite { writes, txn_id };
    matches!(
        context.storage_handle.send_storage_effect(write).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    )
}

/// Deletes rows outside any transaction; `true` once storage took the deletes.
pub(crate) async fn delete_rows(
    context: &DriverContext,
    deletes: Vec<(String, Key)>,
    txn_id: Option<TxnId>,
) -> bool {
    let delete = StorageEffect::BatchDelete { deletes, txn_id };
    matches!(
        context.storage_handle.send_storage_effect(delete).await,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    )
}

/// One row, or `None` when it is absent or the read failed.
pub(crate) async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    txn_id: Option<TxnId>,
) -> Option<Value> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        _ => None,
    }
}

/// Stores a folder row as a test's starting state.
pub(crate) async fn store_folder(
    context: &DriverContext,
    folder: &SyncedFolder,
) -> Result<(), FolderError> {
    let row = folder_entry(folder).map_err(|_| FolderError::Unavailable)?;
    match write_rows(context, vec![row], None).await {
        true => Ok(()),
        false => Err(FolderError::Unavailable),
    }
}
