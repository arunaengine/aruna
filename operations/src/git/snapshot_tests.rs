//! Tests which graph a refresh may snapshot while a materialization is pending.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::current;
use crate::driver::DriverContext;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEventPayload, MetadataEventRecord,
};
use aruna_core::storage_entries::{create_event_entry, materialization_status_entry};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_storage::FjallStorage;
use ulid::Ulid;

/// The stored crate event "older" and the registry row of the newer event `pending`.
fn events(pending: Ulid) -> (MetadataEventRecord, MetadataRegistryRecord) {
    let realm_id = RealmId::from_bytes([1; 32]);
    let group_id = Ulid::from_parts(1, 1);
    let document_id = Ulid::from_parts(1, 2);
    let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
    let older = Ulid::from_parts(1, 10);
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: "datasets/raw".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/raw",
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node_id],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: older,
        last_event_id: older,
    };
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [{"@id": "./", "@type": "Dataset", "name": "older"}]
    });
    let event = MetadataEventRecord {
        event_id: older,
        record: record.clone(),
        user_id: aruna_core::UserId::local(Ulid::from_parts(1, 3), realm_id),
        node_id,
        payload: MetadataEventPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
        occurred_at_ms: 1,
    };
    let document = MetadataRegistryRecord {
        last_event_id: pending,
        ..record
    };
    (event, document)
}

async fn stored(
    event: &MetadataEventRecord,
    status: &MaterializationStatusRecord,
) -> (tempfile::TempDir, DriverContext) {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let writes = vec![
        create_event_entry(event).unwrap(),
        materialization_status_entry(status).unwrap(),
    ];
    let written = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        written,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (directory, context)
}

// The newer event's job already published its snapshot; the older raw view must not replace it.
#[tokio::test]
async fn waits_for_materialization() {
    let pending = Ulid::from_parts(2, 10);
    let (event, document) = events(pending);
    let newer = MetadataEventRecord {
        event_id: pending,
        record: document.clone(),
        ..event.clone()
    };
    let status = MaterializationStatusRecord::pending(&newer, 2);
    let (_directory, context) = stored(&event, &status).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    assert_eq!(refreshed, None, "a refresh snapshots the older graph");
    let captured = current(&context, &document, None, true).await.unwrap();
    assert_eq!(captured.map(|(id, _)| id), Some(event.event_id));
}

#[tokio::test]
async fn snapshots_materialized_graph() {
    let (event, document) = events(Ulid::from_parts(1, 10));
    let mut status = MaterializationStatusRecord::pending(&event, 2);
    status.state = MaterializationState::Materialized;
    let (_directory, context) = stored(&event, &status).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    let (id, jsonld) = refreshed.expect("a materialized graph is ready");
    assert_eq!(id, event.event_id);
    assert!(jsonld.contains("older"));
}
