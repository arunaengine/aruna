//! Tests graph lifecycle: tombstones block applies and deletes wait for the graph fence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::{auth_storage, node_id_seed};
use super::*;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::metadata::MetadataCrateRequest;
use aruna_core::metadata::MetadataGraphPolicy;
use aruna_core::metadata::MetadataRequestDurability;
use byteview::ByteView;
pub(super) fn registry_record(document_path: &str) -> MetadataRegistryRecord {
    let document_id = Ulid::generate();
    MetadataRegistryRecord {
        realm_id: RealmId([7u8; 32]),
        group_id: Ulid::generate(),
        document_id,
        document_path: document_path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: format!("/metadata/{document_path}"),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 0,
        updated_at_ms: 0,
        establishing_event_id: Ulid::nil(),
        last_event_id: Ulid::nil(),
    }
}

#[tokio::test]
async fn tombstone_blocks_apply() {
    let (_storage_dir, storage) = auth_storage();
    let metadata_dir = tempdir().expect("metadata dir");
    let graph_handle = aruna_graph::GraphHandle::open(
        metadata_dir.path(),
        node_id_seed(3),
        None,
        None,
        aruna_graph::GraphOptions::default()
            .with_search_storage(aruna_graph::MetadataSearchStorage::Memory),
    )
    .expect("metadata handle opens");
    let metadata_handle = MetadataHandle::new(
        metadata_dir.path(),
        storage.clone(),
        None,
        Default::default(),
    );
    let metadata_handle = graph_context(storage.clone(), metadata_handle, graph_handle);
    let record = registry_record("datasets/fenced");
    let held = metadata_graph_fence(&record.graph_iri)
        .acquire()
        .await
        .expect("graph fence remains open");
    let graph_iri = record.graph_iri.clone();
    let task_handle = metadata_handle.clone();
    let task = tokio::spawn(async move {
        send_metadata_effect(
            &task_handle,
            MetadataEffect::CreateCrate {
                request: MetadataCrateRequest {
                    graph_iri,
                    name: "fenced".to_string(),
                    description: "fenced".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: None,
                    policy: MetadataGraphPolicy {
                        public: true,
                        permission_paths: Vec::new(),
                    },
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor: None,
                },
            },
        )
        .await
    });
    tokio::task::yield_now().await;

    let tombstone = GraphLifecycleRecord::deleted(
        record.graph_iri.clone(),
        record.realm_id,
        record.group_id,
        record.document_id,
        2,
    );
    let bytes = postcard::to_allocvec(&tombstone).expect("tombstone serializes");
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: GRAPH_LIFECYCLE_KEYSPACE.to_string(),
            key: graph_lifecycle_key(&record.graph_iri),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected lifecycle write result: {other:?}"),
    }
    drop(held);

    let event = task.await.expect("materialization task joins");
    assert!(matches!(
        event,
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::InvalidInput(message),
            ..
        }) if message.contains("deleted")
    ));
    assert!(
        !metadata_handle
            .graph_handle
            .as_ref()
            .expect("graph store")
            .contains_graph(record.graph_iri.clone())
            .await
            .expect("graph probe succeeds")
    );
}

#[tokio::test]
async fn delete_waits_fence() {
    let graph_iri = "urn:test:graph-fence";
    let held = metadata_graph_fence(graph_iri)
        .acquire()
        .await
        .expect("graph fence remains open");
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = ready_tx.send(());
        let _permit = metadata_graph_fence(graph_iri)
            .acquire()
            .await
            .expect("graph fence remains open");
        let _ = done_tx.send(());
    });

    ready_rx.await.expect("delete task started");
    assert!(matches!(
        done_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    drop(held);
    done_rx.await.expect("delete task completed");
    task.await.expect("delete task joined");
}
