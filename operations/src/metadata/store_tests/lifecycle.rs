//! Tests that a tombstoned graph refuses changes through the deletion guard.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::{auth_storage, node_id_seed};
use super::*;
use crate::metadata::deletion_guard::DeletionGuardOperation;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::GRAPH_LIFECYCLE_KEYSPACE;
use aruna_core::metadata::GraphLifecycleRecord;
use aruna_core::metadata::MetadataCrateRequest;
use aruna_core::metadata::MetadataGraphPolicy;
use aruna_core::metadata::MetadataRequestDurability;
use aruna_drivers::driver::drive;
use byteview::ByteView;
pub(super) fn registry_record(document_path: &str) -> MetadataRegistryRecord {
    aruna_core::testing::registry_record(document_path)
}

#[tokio::test]
async fn tombstone_blocks_apply() {
    // A graph-addressed change on a graph whose lifecycle row is a tombstone is refused.
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
    storage.install_metadata_indexes(metadata_dir.path());
    let context = graph_context(storage.clone(), graph_handle);
    let record = registry_record("datasets/fenced");
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

    let create = MetadataEffect::CreateCrate {
        request: MetadataCrateRequest {
            graph_iri: record.graph_iri.clone(),
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
    };
    let guard = DeletionGuardOperation::new(record.graph_iri.clone(), create);
    let event = drive(guard, &context).await.expect("guard answers");
    assert!(matches!(
        event,
        MetadataEvent::Error {
            error: MetadataError::InvalidInput(message),
            ..
        } if message.contains("deleted")
    ));
    assert!(
        !context
            .graph_handle
            .as_ref()
            .expect("graph store")
            .contains_graph(record.graph_iri.clone())
            .await
            .expect("graph probe succeeds")
    );
}
