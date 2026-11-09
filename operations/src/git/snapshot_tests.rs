//! Tests which graph a refresh may snapshot while a materialization job waits or captures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::current;
use crate::driver::DriverContext;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::MATERIALIZATION_CAPTURE_KEYSPACE;
use aruna_core::metadata::{
    MaterializationState, MaterializationStatusRecord, MetadataEventPayload, MetadataEventRecord,
    MetadataMaterializationRecord, MetadataRawRevision,
};
use aruna_core::storage_entries::{
    create_event_entry, document_job_entry, document_job_key, materialization_status_entry,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_storage::FjallStorage;
use ulid::Ulid;

const OLDER: Ulid = Ulid::from_parts(1, 10);

/// The stored crate event "older" and the registry row of the newer event `last`.
fn events(last: Ulid) -> (MetadataEventRecord, MetadataRegistryRecord) {
    let realm_id = RealmId::from_bytes([1; 32]);
    let group_id = Ulid::from_parts(1, 1);
    let document_id = Ulid::from_parts(1, 2);
    let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
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
        establishing_event_id: OLDER,
        last_event_id: OLDER,
    };
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [{"@id": "./", "@type": "Dataset", "name": "older"}]
    });
    let event = MetadataEventRecord {
        event_id: OLDER,
        record: record.clone(),
        user_id: aruna_core::UserId::local(Ulid::from_parts(1, 3), realm_id),
        node_id,
        payload: MetadataEventPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
        occurred_at_ms: 1,
    };
    let document = MetadataRegistryRecord {
        last_event_id: last,
        ..record
    };
    (event, document)
}

fn status(
    event: &MetadataEventRecord,
    id: Ulid,
    state: MaterializationState,
) -> MaterializationStatusRecord {
    let mut status = MaterializationStatusRecord::pending(event, 2);
    status.event_id = id;
    status.state = state;
    status
}

/// Stores the older event, the status and the job row of `queued`, if any, which `captured`
/// marks as a job that published its graph.
async fn stored(
    event: &MetadataEventRecord,
    status: &MaterializationStatusRecord,
    queued: Option<Ulid>,
    captured: bool,
) -> (tempfile::TempDir, DriverContext) {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let mut writes = vec![
        create_event_entry(event).unwrap(),
        materialization_status_entry(status).unwrap(),
    ];
    if let Some(event_id) = queued {
        let job = MetadataMaterializationRecord {
            document_id: event.record.document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(document_job_entry(&job).unwrap());
        if captured {
            let key = document_job_key(job.document_id, event_id);
            let mark = (
                MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
                key,
                Vec::new().into(),
            );
            writes.push(mark);
        }
    }
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
    let status = status(&event, pending, MaterializationState::Pending);
    let (_directory, context) = stored(&event, &status, Some(pending), true).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    assert_eq!(refreshed, None, "a refresh snapshots the older graph");
    let stored = current(&context, &document, None, true).await.unwrap();
    assert_eq!(stored.map(|(id, _)| id), Some(OLDER));
    let supplied = MetadataRawRevision {
        jsonld: "{\"name\":\"newer\"}".to_string(),
        winning_event_id: pending,
        context_digest: [0; 32],
        dataset_digest: None,
        merged: None,
    };
    let captured = current(&context, &document, Some(&supplied), true).await;
    assert_eq!(
        captured.unwrap(),
        Some((pending, supplied.jsonld.clone())),
        "a capture snapshots the graph its job made"
    );
    let refreshed = current(&context, &document, Some(&supplied), false).await;
    assert_eq!(
        refreshed.unwrap(),
        None,
        "a refresh passes the captured job"
    );
}

// A batch waiting for a predecessor on an offline node holds back no completed revision, which
// this holder must still publish, for example after holder failover.
#[tokio::test]
async fn waiting_job_publishes() {
    let waiting = Ulid::from_parts(2, 10);
    let (event, document) = events(OLDER);
    let status = status(&event, OLDER, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, Some(waiting), false).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    assert_eq!(refreshed.map(|(id, _)| id), Some(OLDER));
}

// A late merge leaves the newer status materialized; its job still holds refreshes back.
#[tokio::test]
async fn waits_for_merge() {
    let newer = Ulid::from_parts(3, 10);
    let late = Ulid::from_parts(2, 10);
    let (event, document) = events(newer);
    let status = status(&event, newer, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, Some(late), true).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    assert_eq!(
        refreshed, None,
        "a refresh snapshots the graph before the merge"
    );

    // Completion removes the mark in the batch that stores the merged graph.
    let removed = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            key: document_job_key(document.document_id, late),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        removed,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));
    let finished = current(&context, &document, None, false).await.unwrap();
    assert_eq!(finished.map(|(id, _)| id), Some(OLDER));
}

#[tokio::test]
async fn snapshots_materialized_graph() {
    let (event, document) = events(OLDER);
    let status = status(&event, OLDER, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, None, false).await;

    let refreshed = current(&context, &document, None, false).await.unwrap();
    let (id, jsonld) = refreshed.expect("a materialized graph is ready");
    assert_eq!(id, OLDER);
    assert!(jsonld.contains("older"));
}
