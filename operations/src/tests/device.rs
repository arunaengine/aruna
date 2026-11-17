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
