//! Tests graph lifecycle: live group records, scan caps and tombstone blocking.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::{auth_storage, node_id_seed};
use super::effect::memory_handle;
use super::visibility::{group_record, registry_record};
use super::*;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::metadata::MetadataCrateRequest;
use aruna_core::metadata::MetadataGraphPolicy;
use aruna_core::metadata::MetadataRequestDurability;
use byteview::ByteView;
async fn store_entries(storage: &StorageHandle, writes: Vec<(String, ByteView, ByteView)>) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected batch write result: {other:?}"),
    }
}
fn registry_entries(record: &MetadataRegistryRecord) -> Vec<(String, ByteView, ByteView)> {
    aruna_core::storage_entries::registry_write_entries(record).expect("registry entries encode")
}
#[tokio::test]
async fn group_records_live() {
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let live = group_record(group_id, "datasets/live");
    let gone = group_record(group_id, "datasets/gone");
    let tombstone = GraphLifecycleRecord::deleted(
        gone.graph_iri.clone(),
        gone.realm_id,
        gone.group_id,
        gone.document_id,
        2,
    );
    let mut writes = registry_entries(&live);
    writes.extend(registry_entries(&gone));
    writes.push(
        aruna_core::storage_entries::graph_lifecycle_entry(&tombstone).expect("tombstone encodes"),
    );
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let records = list_group_records(&handle, group_id, 16)
        .await
        .expect("group listing succeeds");

    assert_eq!(
        records
            .iter()
            .map(|record| record.document_id)
            .collect::<Vec<_>>(),
        vec![live.document_id]
    );
}
#[tokio::test]
async fn group_records_capped() {
    // The scan must refuse to answer past its candidate budget instead of
    // returning a silently truncated group listing.
    let (_storage_dir, storage) = auth_storage();
    let group_id = Ulid::generate();
    let mut writes = registry_entries(&group_record(group_id, "datasets/one"));
    writes.extend(registry_entries(&group_record(group_id, "datasets/two")));
    store_entries(&storage, writes).await;
    let (_metadata_dir, handle) = memory_handle(storage);

    let within = list_group_records(&handle, group_id, 2)
        .await
        .expect("group listing succeeds");
    let over = list_group_records(&handle, group_id, 1).await;

    assert_eq!(within.len(), 2);
    assert!(matches!(
        over,
        Err(MetadataError::Backend(message)) if message.contains("candidate limit exceeded")
    ));
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
