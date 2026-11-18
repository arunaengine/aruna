//! Tests that interleaved writes on two holders converge to the same shard manifest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentTarget, ShardManifest, ShardManifestEntry};
use aruna_core::placement::resolve_shard_holders;
use aruna_core::placement::{
    PlacementResolutionContext, choose_origin_bucket, meta_bucket_subject, strategy_for_target,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::{MetaResourceId, StructuredId};
use aruna_core::{NodeId, UserId};
use aruna_operations::driver::drive;
use aruna_operations::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, mint_local_document,
};
use aruna_operations::metadata::delete_document::DeleteDocumentOperation;
use aruna_operations::metadata::persistent_id::read_mapping;
use ulid::Ulid;

use crate::convergence;
use crate::topology::store_realm_config;
use crate::topology::{TestNode, announce_nodes, spawn_replicas};
use convergence::wait_for_convergence;
use aruna_operations::shard::AssembleManifestOperation;

/// A fixed structured id (handle 1, bucket 0) for the strategy-resolution sample,
/// which does not exercise the create-mint flow.
fn doc_id(seed: u64) -> Ulid {
    MetaResourceId::try_from((1u128 << 60) | u128::from(seed))
        .unwrap()
        .as_ulid()
}

// Two holders write documents of one shard, interleaved from both sides.
#[tokio::test]
async fn interleaved_writes_converge() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([125u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 2).await?;
    let group_id = Ulid::generate();
    let holders = [nodes[0].net.node_id(), nodes[1].net.node_id()];

    // This fixture uses one shard so distinct metadata paths can exercise
    // interleaved writes without violating path uniqueness.
    let placement = shared_path_bucket(&config, &holders, realm_id, group_id);
    // Only the shard leader accepts local creates (it is the persistent-id
    // authority); the other holder learns every entry through replication.
    let leader = resolve_shard_holders(&config, &placement)[0];
    let leader_node = nodes
        .iter()
        .find(|node| node.net.node_id() == leader)
        .expect("the shard leader is one of the two holders");
    let document_ids: Vec<Ulid> = (0..6)
        .map(|_| {
            let actor = Actor {
                node_id: leader,
                user_id: UserId::local(Ulid::generate(), realm_id),
                realm_id,
            };
            mint_local_document(&config, &actor, group_id, CONVERGE_PATH).map(|id| id.as_ulid())
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (index, document_id) in document_ids.iter().enumerate() {
        create_document(leader_node, realm_id, group_id, *document_id, index).await?;
    }

    // The mint job rewrites each mapping once it activates the id, so the
    // manifests can only settle after every mapping is active on both holders.
    wait_active_ids(&nodes, &document_ids).await?;
    // Every create lands two rows in the shard: the document and its
    // persistent-id mapping. Both holders must converge on set and digest.
    let manifest_rows = document_ids.len() * 2;
    wait_manifest_agreement(&nodes[0], &nodes[1], realm_id, placement, manifest_rows).await?;

    let left = drive(AssembleManifestOperation::new(realm_id, placement), nodes[0].context.as_ref()).await?;
    let right = drive(AssembleManifestOperation::new(realm_id, placement), nodes[1].context.as_ref()).await?;
    assert_eq!(left.entries.len(), manifest_rows);
    assert_eq!(sorted_entries(&left), sorted_entries(&right));
    assert_eq!(left.digest, right.digest);

    // Receivers must stamp the origin actor into identical tombstone manifests.
    let deleted_id = document_ids[0];
    let deleted_target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: deleted_id,
    };
    drive(
        DeleteDocumentOperation::new(
            Actor {
                node_id: leader,
                user_id: UserId::local(Ulid::generate(), realm_id),
                realm_id,
            },
            group_id,
            deleted_id,
        ),
        leader_node.context.as_ref(),
    )
    .await?;

    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "delete tombstone did not converge across holders",
        || async {
            let left =
                drive(AssembleManifestOperation::new(realm_id, placement), nodes[0].context.as_ref()).await?;
            let right =
                drive(AssembleManifestOperation::new(realm_id, placement), nodes[1].context.as_ref()).await?;
            let left_tomb = left
                .entries
                .iter()
                .find(|entry| entry.target == deleted_target)
                .map(|entry| entry.revision);
            let right_tomb = right
                .entries
                .iter()
                .find(|entry| entry.target == deleted_target)
                .map(|entry| entry.revision);
            // A matching tombstone alone is not convergence: an earlier write may
            // still be in flight, so wait for the whole entry set before judging.
            let agreed = left_tomb.is_some()
                && left_tomb == right_tomb
                && left.entries.len() == manifest_rows
                && sorted_entries(&left) == sorted_entries(&right)
                && left.digest == right.digest;
            Ok(usize::from(!agreed))
        },
    )
    .await?;

    let left = drive(AssembleManifestOperation::new(realm_id, placement), nodes[0].context.as_ref()).await?;
    let right = drive(AssembleManifestOperation::new(realm_id, placement), nodes[1].context.as_ref()).await?;
    assert_eq!(
        left.entries.len(),
        manifest_rows,
        "the delete keeps a tombstone row"
    );
    assert_eq!(sorted_entries(&left), sorted_entries(&right));
    assert_eq!(left.digest, right.digest);

    shutdown_nodes(nodes).await;
    Ok(())
}

const CONVERGE_PATH: &str = "datasets/converge";

/// The bucket every document on [`CONVERGE_PATH`] lands in. The subject is the
/// canonical `(realm, group, path)` (6.3.6), and both holders hold every bucket
/// (RF over two nodes), so their origin-bucket choice is identical.
fn shared_path_bucket(
    config: &RealmConfigDocument,
    holders: &[NodeId],
    realm_id: RealmId,
    group_id: Ulid,
) -> PlacementRef {
    let target = DocumentTarget::MetadataDocumentLifecycle {
        document_id: doc_id(1),
    };
    let path = MetadataRegistryRecord::normalize_document_path(CONVERGE_PATH);
    let (strategy, _) = strategy_for_target(
        config,
        &target,
        PlacementResolutionContext {
            group_id: Some(group_id),
            metadata_path: Some(&path),
        },
    )
    .expect("strategy resolves");
    let subject = meta_bucket_subject(realm_id, group_id, &path);
    let placement = choose_origin_bucket(config, strategy, holders[0], &subject)
        .expect("origin holds a bucket");
    for holder in holders {
        assert_eq!(
            choose_origin_bucket(config, strategy, *holder, &subject).map(|bucket| bucket.shard),
            Some(placement.shard),
            "both holders choose the same bucket for the shared path"
        );
    }
    placement
}

fn sorted_entries(manifest: &ShardManifest) -> Vec<ShardManifestEntry> {
    let mut entries = manifest.entries.clone();
    entries.sort_by_key(|entry| postcard::to_allocvec(&entry.target).unwrap());
    entries
}

async fn create_document(
    node: &TestNode,
    realm_id: RealmId,
    group_id: Ulid,
    document_id: Ulid,
    index: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    drive(
        CreateDocumentOperation::new(CreateDocumentConfig {
            actor: Actor {
                node_id: node.net.node_id(),
                user_id: UserId::local(Ulid::generate(), realm_id),
                realm_id,
            },
            group_id,
            document_id,
            document_path: format!("{CONVERGE_PATH}/{index}"),
            public: true,
            payload: CreateDocumentPayload::Scaffold {
                name: format!("Converge {index}"),
                description: "convergence test".to_string(),
                date_published: "2026-07-07".to_string(),
                license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            },
        }),
        node.context.as_ref(),
    )
    .await?;
    Ok(())
}

async fn wait_active_ids(
    nodes: &[TestNode],
    document_ids: &[Ulid],
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_convergence("persistent ids did not activate", || async {
        let mut pending = 0usize;
        for node in nodes {
            for document_id in document_ids {
                let mapping = read_mapping(node.context.as_ref(), *document_id).await?;
                if !mapping.is_some_and(|mapping| mapping.is_active()) {
                    pending += 1;
                }
            }
        }
        Ok(pending)
    })
    .await
}

async fn wait_manifest_agreement(
    left: &TestNode,
    right: &TestNode,
    realm_id: RealmId,
    placement: PlacementRef,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_convergence("manifests did not converge", || async {
        let left_manifest =
            drive(AssembleManifestOperation::new(realm_id, placement), left.context.as_ref()).await?;
        let right_manifest =
            drive(AssembleManifestOperation::new(realm_id, placement), right.context.as_ref()).await?;
        Ok(usize::from(
            !(left_manifest.entries.len() == expected
                && right_manifest.entries.len() == expected
                && left_manifest.digest == right_manifest.digest),
        ))
    })
    .await
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
    for strategy in &mut config.strategies {
        strategy.shard_count = 1;
    }
    for (band, node) in nodes.iter().enumerate() {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
        config.seed_job_control(node.net.node_id(), band as u32);
    }
    for node in nodes {
        store_realm_config(&node.net, &node.context, *realm_id, &config).await?;
    }
    for node in nodes {
        aruna_operations::driver::drive(aruna_operations::placement::process_placements::ReconcilePlacementsOperation::new(*realm_id, node.net.node_id(), true), &node.context).await.unwrap_or_else(|_| aruna_operations::placement::process_placements::PlacementReconcileOutcome::storage_failure());
    }
    Ok(config)
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
