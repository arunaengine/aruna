//! Tests metadata effects: entity plans, merge convergence and read error mapping.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::{auth_storage, node_id_seed};
use super::*;
use crate::metadata::protocol::{MetadataReadError, metadata_read_error};
use aruna_core::metadata::MetadataBatch;
use aruna_core::metadata::MetadataBatchSource;
use aruna_core::metadata::MetadataCrateRequest;
use aruna_core::metadata::MetadataGraphPolicy;
use aruna_core::metadata::MetadataRequestDurability;
pub(super) fn memory_handle(storage: StorageHandle) -> (TempDir, DriverContext) {
    let metadata_dir = tempdir().expect("metadata dir");
    let graph_handle = aruna_graph::GraphHandle::open(
        metadata_dir.path(),
        node_id_seed(9),
        None,
        None,
        aruna_graph::GraphOptions::default()
            .with_search_storage(aruna_graph::MetadataSearchStorage::Memory),
    )
    .expect("metadata handle opens");
    storage.install_metadata_indexes(metadata_dir.path());
    (metadata_dir, graph_context(storage, graph_handle))
}

async fn plan_entity(
    handle: &DriverContext,
    graph_iri: &str,
    actor: u8,
    name: &str,
) -> MetadataBatch {
    let event = send_metadata_effect(
        &handle,
        MetadataEffect::PlanBatch {
            graph_iri: graph_iri.to_string(),
            actor: [actor; 32],
            counter: 1,
            source: MetadataBatchSource::UpsertContextualEntity {
                jsonld: format!(r##"{{"@id":"#{name}","@type":"Person","name":"{name}"}}"##),
            },
        },
    )
    .await;
    let Event::Metadata(MetadataEvent::BatchPlanned { batch, .. }) = event else {
        panic!("expected a planned batch, got {event:?}");
    };
    batch
}

async fn merge_into(handle: &DriverContext, graph_iri: &str, batch: &MetadataBatch) -> bool {
    let event = send_metadata_effect(
        &handle,
        MetadataEffect::MergeBatch {
            graph_iri: graph_iri.to_string(),
            batch: batch.clone(),
        },
    )
    .await;
    let Event::Metadata(MetadataEvent::BatchMerged { applied, .. }) = event else {
        panic!("expected a merged batch, got {event:?}");
    };
    applied
}

async fn graph_state(handle: &DriverContext, graph_iri: &str) -> Vec<(String, String, String)> {
    let event = send_metadata_effect(
        &handle,
        MetadataEffect::GraphSnapshot {
            graph_iri: graph_iri.to_string(),
        },
    )
    .await;
    let Event::Metadata(MetadataEvent::GraphSnapshotResult { snapshot, .. }) = event else {
        panic!("expected a graph snapshot, got {event:?}");
    };
    let mut quads = snapshot
        .quads
        .into_iter()
        .map(|quad| (quad.subject.0, quad.predicate.0, quad.object.0))
        .collect::<Vec<_>>();
    quads.sort();
    quads
}

#[tokio::test]
async fn plan_needs_graph() {
    // A graph this node has not materialized yet is a lagging replica, so
    // planning defers instead of rejecting the change set for good.
    let (_storage_dir, storage) = auth_storage();
    let (_metadata_dir, handle) = memory_handle(storage);

    let event = send_metadata_effect(
        &handle,
        MetadataEffect::PlanBatch {
            graph_iri: "urn:test:orset:absent".to_string(),
            actor: [7u8; 32],
            counter: 1,
            source: MetadataBatchSource::UpsertDataEntity {
                jsonld: r#"{"@id":"./latest.txt","@type":"File","name":"latest.txt"}"#.to_string(),
            },
        },
    )
    .await;

    assert!(matches!(
        event,
        Event::Metadata(MetadataEvent::Error {
            error: MetadataError::GraphNotFound,
            ..
        })
    ));
}

#[test]
fn read_error_mapping() {
    assert_eq!(
        metadata_read_error(MetadataError::GraphNotFound),
        MetadataReadError::NotFound
    );
    assert_eq!(
        metadata_read_error(MetadataError::Backend("storage".to_string())),
        MetadataReadError::Unavailable
    );
}
#[tokio::test]
async fn merges_converge() {
    // Two holders plan against the same base and merge in opposite orders.
    let graph_iri = "urn:test:orset:converge";
    let (_left_storage_dir, left_storage) = auth_storage();
    let (_left_dir, left) = memory_handle(left_storage);
    let (_right_storage_dir, right_storage) = auth_storage();
    let (_right_dir, right) = memory_handle(right_storage);
    let request = MetadataCrateRequest {
        graph_iri: graph_iri.to_string(),
        name: "Converge".to_string(),
        description: "OR-Set convergence".to_string(),
        date_published: "2026-08-26".to_string(),
        license: None,
        policy: MetadataGraphPolicy {
            public: true,
            permission_paths: Vec::new(),
        },
        durability: MetadataRequestDurability::Durable,
        deterministic_actor: Some([1u8; 32]),
    };
    for handle in [&left, &right] {
        assert!(matches!(
            send_metadata_effect(
                &handle,
                MetadataEffect::CreateCrate {
                    request: request.clone(),
                }
            )
            .await,
            Event::Metadata(MetadataEvent::CreateCrateResult { .. })
        ));
    }

    let first = plan_entity(&left, graph_iri, 2, "ada").await;
    let second = plan_entity(&right, graph_iri, 3, "grace").await;
    assert!(merge_into(&left, graph_iri, &first).await);
    assert!(merge_into(&left, graph_iri, &second).await);
    assert!(merge_into(&right, graph_iri, &second).await);
    assert!(merge_into(&right, graph_iri, &first).await);
    assert!(
        !merge_into(&left, graph_iri, &first).await,
        "a batch dot the clock already covers is a no-op"
    );

    let left_render = export_rocrate_jsonld(&left, graph_iri.to_string())
        .await
        .expect("left render");
    let right_render = export_rocrate_jsonld(&right, graph_iri.to_string())
        .await
        .expect("right render");
    assert_eq!(left_render, right_render);
    // The export renders only entities the crate references, so both
    // concurrent adds are checked against the authoritative graph state.
    let left_state = graph_state(&left, graph_iri).await;
    assert_eq!(left_state, graph_state(&right, graph_iri).await);
    for subject in ["<#ada>", "<#grace>"] {
        assert!(left_state.iter().any(|(held, ..)| held == subject));
    }
}
