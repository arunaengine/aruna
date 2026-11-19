//! Tests RO-Crate versions stored side by side and graph policies that mint no entities.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::auth::{auth_storage, node_id_seed};
use super::effect::memory_handle;
use super::*;
use aruna_core::metadata::MetadataCrateRequest;
use aruna_core::metadata::MetadataGraphPolicy;
use aruna_core::metadata::MetadataRequestDurability;
use serde_json::Value;

#[tokio::test]
async fn versions_roundtrip() {
    let (_storage_dir, storage) = auth_storage();
    let (_metadata_dir, handle) = memory_handle(storage);
    let cases = [
        ("1.2", "urn:fixture:aruna:rocrate:1.2", ROCRATE_12),
        ("1.3", "urn:fixture:aruna:rocrate:1.3", ROCRATE_13),
    ];

    // One handle stores both versions, covering a mixed-version realm.
    for (version, graph_iri, jsonld) in cases {
        let request = ApplyRoCrateRequest {
            graph_iri: graph_iri.to_string(),
            jsonld: jsonld.to_string(),
            policy: MetadataGraphPolicy {
                public: true,
                permission_paths: Vec::new(),
            },
            durability: MetadataRequestDurability::Durable,
            deterministic_actor: None,
        };
        assert!(matches!(
            send_metadata_effect(&handle, MetadataEffect::ValidateRoCrate {
                    request: request.clone(),
                })
                .await,
            Event::Metadata(MetadataEvent::ValidationResult { graph_iri: validated })
                if validated == graph_iri
        ));
        assert!(matches!(
            send_metadata_effect(&handle, MetadataEffect::ApplyRoCrate { request })
                .await,
            Event::Metadata(MetadataEvent::ApplyRoCrateResult { graph_iri: applied, .. })
                if applied == graph_iri
        ));

        let exported = export_rocrate_jsonld(&handle, graph_iri.to_string())
            .await
            .expect("RO-Crate exports");
        let output = serde_json::from_str::<Value>(&exported).expect("export is JSON");
        let input = serde_json::from_str::<Value>(jsonld).expect("fixture is JSON");
        assert_eq!(output["@context"], input["@context"]);
        let output_descriptor = output["@graph"]
            .as_array()
            .expect("export graph")
            .iter()
            .find(|entity| entity["@id"] == "ro-crate-metadata.json")
            .expect("export descriptor");
        let input_descriptor = input["@graph"]
            .as_array()
            .expect("fixture graph")
            .iter()
            .find(|entity| entity["@id"] == "ro-crate-metadata.json")
            .expect("fixture descriptor");
        assert_eq!(
            output_descriptor["conformsTo"],
            input_descriptor["conformsTo"]
        );
        if version == "1.2" {
            assert_eq!(
                output, input,
                "RO-Crate 1.2 JSON-LD behavior changed during import/export"
            );
        }
        assert_eq!(
            craqle::validate_rocrate_jsonld(&exported)
                .expect("export validates")
                .nquads,
            craqle::validate_rocrate_jsonld(jsonld)
                .expect("fixture validates")
                .nquads,
            "RO-Crate {version} RDF changed during import/export"
        );
    }

    let graph_iri = "urn:fixture:aruna:rocrate:created";
    assert!(matches!(
        send_metadata_effect(&handle, MetadataEffect::CreateCrate {
                request: MetadataCrateRequest {
                    graph_iri: graph_iri.to_string(),
                    name: "RO-Crate 1.3 scaffold".to_string(),
                    description: "Aruna scaffold version contract".to_string(),
                    date_published: "2026-08-19".to_string(),
                    license: None,
                    policy: MetadataGraphPolicy {
                        public: true,
                        permission_paths: Vec::new(),
                    },
                    durability: MetadataRequestDurability::Durable,
                    deterministic_actor: None,
                },
            })
            .await,
        Event::Metadata(MetadataEvent::CreateCrateResult { graph_iri: created, .. })
            if created == graph_iri
    ));
    let scaffold: Value = serde_json::from_str(
        &export_rocrate_jsonld(&handle, graph_iri.to_string())
            .await
            .expect("scaffold exports"),
    )
    .expect("scaffold is JSON");
    assert_eq!(
        scaffold["@context"],
        serde_json::json!("https://w3id.org/ro/crate/1.3/context")
    );
    let descriptor = scaffold["@graph"]
        .as_array()
        .expect("scaffold graph")
        .iter()
        .find(|entity| entity["@id"] == "ro-crate-metadata.json")
        .expect("scaffold descriptor");
    assert_eq!(
        descriptor["conformsTo"]["@id"],
        serde_json::json!("https://w3id.org/ro/crate/1.3")
    );
}

/// A holder that materializes a visibility change before it joins the graph topic must not
/// mint its own genesis: it names no peers, so it can never merge with the holders' topic.
#[tokio::test]
async fn policy_mints_nothing() {
    let (_storage_dir, storage) = auth_storage();
    let dir = tempdir().expect("metadata dir");
    let sync_node = irokle::Irokle::builder()
        .with_fjall_path(dir.path().join("sync"))
        .and_then(|builder| builder.build())
        .expect("sync node builds");
    let graph = aruna_graph::GraphHandle::open(
        dir.path().join("metadata"),
        node_id_seed(9),
        Some(sync_node),
        None,
        aruna_graph::GraphOptions::default()
            .with_search_storage(aruna_graph::MetadataSearchStorage::Memory),
    )
    .expect("metadata handle opens");
    install_indexes(&storage, dir.path().join("metadata"));
    let handle = graph_context(storage, graph);
    let graph_iri = "urn:fixture:aruna:policy:restored";
    let policy = |public| MetadataGraphPolicy {
        public,
        permission_paths: vec!["/groups/demo/datasets/restored".to_string()],
    };
    assert!(matches!(
        send_metadata_effect(
            &handle,
            MetadataEffect::CreateCrate {
                request: MetadataCrateRequest {
                    graph_iri: graph_iri.to_string(),
                    name: "Restored".to_string(),
                    description: "Created private, then made public".to_string(),
                    date_published: "2026-11-01".to_string(),
                    license: None,
                    policy: policy(false),
                    durability: MetadataRequestDurability::WalAlreadyDurable,
                    deterministic_actor: Some([7; 32]),
                },
            }
        )
        .await,
        Event::Metadata(MetadataEvent::CreateCrateResult { .. })
    ));

    let event = send_metadata_effect(
        &handle,
        MetadataEffect::SetGraphPolicy {
            graph_iri: graph_iri.to_string(),
            policy: policy(true),
        },
    )
    .await;

    assert!(
        matches!(event, Event::Metadata(MetadataEvent::GraphPolicySet { .. })),
        "{event:?}"
    );
    let event = send_metadata_effect(
        &handle,
        MetadataEffect::GetGraphPolicy {
            graph_iri: graph_iri.to_string(),
        },
    )
    .await;
    assert!(
        matches!(&event, Event::Metadata(MetadataEvent::GraphPolicyResult { policy: set, .. })
            if *set == policy(true)),
        "{event:?}"
    );
    let topic = handle
        .graph_handle
        .as_ref()
        .expect("graph store")
        .bind_topic(graph_iri.to_string())
        .await
        .expect("topic lookup");
    assert_eq!(topic, None);
}
