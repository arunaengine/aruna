//! Tests that the shard manifest request refuses foreign or non-holder peers before serving.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::StructuredId;
use aruna_core::document::{DocumentTarget, ShardManifest};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::{PlacementRef, PlacementStrategy};
use aruna_core::{NodeId, UserId};
use aruna_operations::driver::drive;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, mint_local_document,
};
use aruna_operations::shard::assemble_shard_manifest;
use aruna_operations::shard::client::fetch_shard_manifest;
use aruna_operations::shard::verify::{is_shard_verified, verify_held_shards};
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::store_realm_config;
use crate::topology::{
    TestNode, announce_nodes, install_default_config, spawn_replica, spawn_replicas,
};

#[tokio::test]
async fn rejects_nonrealm_peer() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([121u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    // An outsider node that is not in the realm config at all.
    let outsider = spawn_replica(realm_id, true, true).await?;
    outsider
        .net
        .add_peer_addr(nodes[0].net.endpoint_addr())
        .await;
    nodes[0]
        .net
        .add_peer_addr(outsider.net.endpoint_addr())
        .await;

    let placement = any_held_placement(&config, nodes[0].net.node_id());
    let error = fetch_shard_manifest(&outsider.net, nodes[0].net.node_id(), realm_id, placement)
        .await
        .expect_err("non-realm peer must be rejected");
    // Inbound admission drops unregistered peers before the manifest protocol
    // can answer, so the client usually observes a closed connection.
    assert!(
        error.contains("connection lost") || error.contains("not a sync-eligible node"),
        "unexpected reject: {error}"
    );

    outsider.net.shutdown().await;
    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn rejects_unheld_shard() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([123u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 2).await?;

    // A placement whose strategy the config does not know resolves to no holders,
    // so the responder never holds it.
    let bogus = PlacementRef {
        strategy_id: Ulid::from_bytes([0xAB; 16]),
        shard: 1,
    };
    let error = fetch_shard_manifest(&nodes[1].net, nodes[0].net.node_id(), realm_id, bogus)
        .await
        .expect_err("non-held shard must be rejected");
    assert!(
        error.contains("does not hold shard"),
        "unexpected reject: {error}"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn rejects_nonholder_peer() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([127u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 4).await?;
    let responder = nodes[0].net.node_id();
    let (placement, non_holder, holder) = nonholder_placement(&config, &nodes);

    let non_holder_node = nodes
        .iter()
        .find(|node| node.net.node_id() == non_holder)
        .expect("non-holder node exists");
    let error = fetch_shard_manifest(&non_holder_node.net, responder, realm_id, placement)
        .await
        .expect_err("sync-eligible non-holder peer must be rejected");
    assert!(
        error.contains("is not a holder"),
        "unexpected reject: {error}"
    );

    let holder_node = nodes
        .iter()
        .find(|node| node.net.node_id() == holder)
        .expect("holder node exists");
    let manifest = fetch_shard_manifest(&holder_node.net, responder, realm_id, placement).await?;
    assert_eq!(manifest.holder, responder);
    assert_eq!(manifest.placement, placement);

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn new_holder_verifies() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([124u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::generate();
    let actor = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    let document_id =
        mint_local_document(&config, &actor, group_id, "datasets/verify-canary")?.as_ulid();

    let created = drive(
        CreateDocumentOperation::new(CreateDocumentConfig {
            actor: actor.clone(),
            group_id,
            document_id,
            document_path: "datasets/verify-canary".to_string(),
            public: true,
            payload: CreateDocumentPayload::Scaffold {
                name: "Verify Canary".to_string(),
                description: "verification test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: created.record.document_id,
    };
    let placement = created.record.placement;

    // Wait until node B (the co-holder) has synced the document into its shard.
    wait_manifest_entry(&nodes[1], realm_id, placement, &target).await?;

    // Before verification, node B has no marker for that shard.
    assert!(!is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await);

    // Node B fetches node A's manifest directly and sees the same document.
    let remote =
        fetch_shard_manifest(&nodes[1].net, nodes[0].net.node_id(), realm_id, placement).await?;
    assert!(remote.entries.iter().any(|entry| entry.target == target));

    let summary = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert!(
        summary.newly_verified > 0,
        "expected at least one shard verified, got {summary:?}"
    );
    assert!(
        is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await,
        "the document's shard must be verified"
    );

    // A second pass finds every held shard already verified (idempotent, cheap).
    let second = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert_eq!(second.newly_verified, 0);
    assert_eq!(second.already_verified, second.held);

    shutdown_nodes(nodes).await;
    Ok(())
}

// Two genesis-less holders compute the SAME (non-zero) empty fingerprint, so their digests
// match: exactly the condition that would falsely certify convergence.
#[tokio::test]
async fn genesisless_never_verify() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([126u8; 32]);
    let nodes = build_meshed_nodes(realm_id, 2).await?;
    let config = install_unplaced_config(&nodes, realm_id, 2).await?;

    let placement = any_held_placement(&config, nodes[1].net.node_id());
    let left = assemble_shard_manifest(nodes[0].context.as_ref(), realm_id, placement).await?;
    let right = assemble_shard_manifest(nodes[1].context.as_ref(), realm_id, placement).await?;
    assert_eq!(
        left.digest, right.digest,
        "genesis-less holders share the empty fingerprint"
    );
    assert!(
        !nodes[1]
            .net
            .sync_topic_exists(aruna_core::document::shard_topic_id(realm_id, &placement))
            .unwrap_or(false),
        "no local genesis exists for the shard"
    );

    let summary = verify_held_shards(&nodes[1].context, nodes[1].net.node_id(), realm_id).await;
    assert_eq!(
        summary.newly_verified, 0,
        "a matching empty digest must not verify a genesis-less shard: {summary:?}"
    );
    assert!(
        !is_shard_verified(nodes[1].context.as_ref(), realm_id, &placement).await,
        "a genesis-less shard must stay unverified"
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn build_meshed_nodes(
    realm_id: RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let nodes = spawn_replicas(realm_id, count, true, true).await?;
    Ok(nodes)
}

// Installs a small-shard realm config (every node holds every shard) but does
// not run the placement reconciler, so no shard topic genesis exists.
async fn install_unplaced_config(
    nodes: &[TestNode],
    realm_id: RealmId,
    shard_count: u32,
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    let strategy = PlacementStrategy {
        strategy_id: Ulid::generate(),
        name: "default".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count,
    };
    config.default_strategy_id = Some(strategy.strategy_id);
    config.strategies = vec![strategy];
    for (band, node) in nodes.iter().enumerate() {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
        config.seed_job_control(node.net.node_id(), band as u32);
    }
    for node in nodes {
        store_realm_config(&node.net, &node.context, realm_id, &config).await?;
    }
    Ok(config)
}

fn any_held_placement(config: &RealmConfigDocument, node_id: NodeId) -> PlacementRef {
    let strategy = config.strategies.first().expect("a strategy");
    for shard in 0..strategy.shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard,
        };
        if aruna_core::placement::resolve_shard_holders(config, &placement).contains(&node_id)
        {
            return placement;
        }
    }
    panic!("node holds no shard");
}

fn nonholder_placement(
    config: &RealmConfigDocument,
    nodes: &[TestNode],
) -> (PlacementRef, NodeId, NodeId) {
    let responder = nodes[0].net.node_id();
    let strategy = config.strategies.first().expect("a strategy");
    for shard in 0..strategy.shard_count {
        let placement = PlacementRef {
            strategy_id: strategy.strategy_id,
            shard,
        };
        let holders = aruna_core::placement::resolve_shard_holders(config, &placement);
        if !holders.contains(&responder) {
            continue;
        }
        let Some(non_holder) = nodes
            .iter()
            .map(|node| node.net.node_id())
            .find(|node_id| *node_id != responder && !holders.contains(node_id))
        else {
            continue;
        };
        let holder = holders
            .iter()
            .copied()
            .find(|node_id| *node_id != responder)
            .expect("responder has a co-holder");
        return (placement, non_holder, holder);
    }
    panic!("no shard found with a holder responder and sync-eligible non-holder requester");
}

async fn wait_manifest_entry(
    node: &TestNode,
    realm_id: RealmId,
    placement: PlacementRef,
    target: &DocumentTarget,
) -> Result<ShardManifest, Box<dyn std::error::Error>> {
    wait_for_convergence("timed out waiting for manifest entry", || async {
        let manifest = assemble_shard_manifest(node.context.as_ref(), realm_id, placement).await?;
        Ok::<_, Box<dyn std::error::Error>>(usize::from(
            !manifest.entries.iter().any(|entry| &entry.target == target),
        ))
    })
    .await?;
    Ok(assemble_shard_manifest(node.context.as_ref(), realm_id, placement).await?)
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
    let nodes = spawn_replicas(*realm_id, count, true, true).await?;
    announce_nodes(&nodes, realm_id).await?;
    let config = install_default_config(&nodes, realm_id).await?;
    Ok((nodes, config))
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
