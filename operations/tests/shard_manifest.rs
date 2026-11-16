//! Tests that a created document leaves matching manifest rows on origin and receiver.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::StructuredId;
use aruna_core::UserId;
use aruna_core::document::{DocumentTarget, ShardManifest};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_operations::driver::drive;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, mint_local_document,
};
use aruna_operations::placement::resolve_shard_holders;
use aruna_operations::shard::assemble_shard_manifest;
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::store_realm_config;
use crate::topology::{TestNode, announce_nodes, spawn_replicas};

// A metadata document created on node A must leave a manifest row on the origin AND on the
// receiver that syncs it.
#[tokio::test]
async fn manifest_rows_match() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([120u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::generate();
    let actor = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    let document_id =
        mint_local_document(&config, &actor, group_id, "datasets/manifest-canary")?.as_ulid();

    let created = drive(
        CreateDocumentOperation::new(CreateDocumentConfig {
            actor: actor.clone(),
            group_id,
            document_id,
            document_path: "datasets/manifest-canary".to_string(),
            public: true,
            payload: CreateDocumentPayload::Scaffold {
                name: "Manifest Canary".to_string(),
                description: "manifest maintenance test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let document_id = created.record.document_id;

    let target = DocumentTarget::MetadataDocumentLifecycle { document_id };
    let placement = created.record.placement;
    assert_ne!(
        placement,
        PlacementRef::NIL,
        "document must resolve a shard"
    );
    assert!(resolve_shard_holders(&config, &placement).contains(&nodes[0].net.node_id()));

    // The receiver only writes its manifest row once the lifecycle syncs and
    // applies, so wait until node B observes the document.
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "receiver manifest entry did not arrive",
        || async {
            let manifest =
                assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
            Ok(usize::from(!manifest_contains(&manifest, &target)))
        },
    )
    .await?;
    let receiver = assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
    let origin = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;

    assert!(
        manifest_contains(&origin, &target),
        "origin manifest missing the document"
    );
    assert!(
        manifest_contains(&receiver, &target),
        "receiver manifest missing the document"
    );
    // Same shard, converged topic ⇒ identical digest across holders.
    assert_eq!(
        origin.digest, receiver.digest,
        "co-holder shard digests must agree"
    );
    // The lifecycle revision the origin recorded is the one the receiver applied.
    let origin_revision = manifest_revision(&origin, &target);
    let receiver_revision = manifest_revision(&receiver, &target);
    assert_eq!(origin_revision, receiver_revision);

    shutdown_nodes(nodes).await;
    Ok(())
}

fn manifest_contains(manifest: &ShardManifest, target: &DocumentTarget) -> bool {
    manifest.entries.iter().any(|entry| &entry.target == target)
}

fn manifest_revision(
    manifest: &ShardManifest,
    target: &DocumentTarget,
) -> aruna_core::document::DocumentSyncRevision {
    manifest
        .entries
        .iter()
        .find(|entry| &entry.target == target)
        .map(|entry| entry.revision)
        .expect("manifest entry present")
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
    let nodes = spawn_replicas(*realm_id, count, true, false).await?;
    announce_nodes(&nodes, realm_id).await?;
    let config = install_realm_config(&nodes, realm_id).await?;
    Ok((nodes, config))
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for (band, node) in nodes.iter().enumerate() {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
        config.seed_job_control(node.net.node_id(), band as u32);
    }
    for node in nodes {
        store_realm_config(&node.net, &node.context, *realm_id, &config).await?;
    }
    for node in nodes {
        aruna_operations::placement::process_placements::process_shard_placements(
            &node.context,
            *realm_id,
            node.net.node_id(),
        )
        .await;
    }
    Ok(config)
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
