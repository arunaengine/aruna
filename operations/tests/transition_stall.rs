// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! A hand-off that cannot finish, and the only way out of it (#400).
//!
//! One target of the bucket is a node that exists in the map and nowhere else,
//! so its proof never arrives. Everything else works: the old holders fence,
//! the reachable targets pull the real history and sign it. The bucket still
//! must not cut over - authority stays with the old holders, nobody mints a
//! rival genesis to make progress, and the bucket simply stays incomplete and
//! visible in the health counts.
//!
//! The unreachable target is expressed as a node in the candidate map with no
//! process behind it rather than a killed one: the outcome the executor sees is
//! the same, and it needs no restart to recover. Recovery is the operator's
//! force-finalize, which the reducer accepts only because a reachable target
//! did prove - the last verified copy is never the one cut away.

mod topology;

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::structs::{NodePlacementEntry, PlacementRef, RealmNodeKind, TransitionLimits};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::mutate_realm_placement::RealmPlacementMutation;
use aruna_operations::placement::transition::{preview_transition, transition_health};
use aruna_operations::placement::{holds_placement, resolve_shard_holders};
use ulid::Ulid;

use topology::{LOCATIONS, NODE_WEIGHT, TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;
const SHARD_COUNT: u32 = 4;

#[tokio::test]
async fn unreachable_target_stalls() -> TestResult<()> {
    let mut realm = Topology::spawn_sharded(
        MANAGEMENT_NODES,
        USER_NODES,
        REPLICATION_FACTOR,
        SHARD_COUNT,
    )
    .await?;
    let group_id = realm.seed_group().await?;

    let path = "datasets/stalled";
    let document_id =
        mint_local_document(&realm.config, &realm.actor(realm.node(0)), group_id, path)?.as_ulid();
    let placement = create_document(&realm, realm.node(0), group_id, document_id, path).await?;
    let before = realm.assert_holder(realm.node(0).node_id(), &placement);
    for holder in &before {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    // A node the map knows and the realm cannot reach, plus every current
    // holder marked full: the bucket's target set is the two remaining nodes
    // and the phantom.
    let phantom = register_phantom(&mut realm).await?;
    for holder in &before {
        let mut entry = realm
            .config
            .placement_entry(*holder)
            .cloned()
            .expect("every management node is mapped");
        entry.full = true;
        realm
            .mutate(0, RealmPlacementMutation::UpsertNode(entry))
            .await?;
    }
    let epoch = realm.publish_map(0).await?;
    let preview = preview_transition(
        &realm.config,
        placement.strategy_id,
        &[placement.shard],
        epoch,
    )?
    .pop()
    .expect("the preview covers the requested bucket");
    assert!(
        preview.new_holders.contains(&phantom),
        "the fixture must route the bucket through the unreachable node"
    );
    let reachable: Vec<NodeId> = preview
        .new_holders
        .iter()
        .copied()
        .filter(|node_id| *node_id != phantom)
        .collect();
    assert!(!reachable.is_empty());

    let transition = realm
        .start_transition(
            0,
            placement.strategy_id,
            vec![placement.shard],
            epoch,
            TransitionLimits {
                max_incomplete_buckets: 1,
                grace_ms: 0,
            },
        )
        .await?;
    let bucket = placement.shard;
    let expected = reachable.len();
    realm
        .await_config("every reachable target proves", move |config| {
            config
                .transition(&transition)
                .is_some_and(|record| record.proofs_for(bucket).count() == expected)
        })
        .await?;

    // Every old holder fenced and every reachable target proved, and the bucket
    // still has not moved: one missing proof is enough to hold it.
    let record = realm
        .config
        .transition(&transition)
        .expect("the stalled record stays readable");
    assert!(record.barrier_established(bucket, &preview.old_holders));
    assert!(record.completion(bucket).is_none());
    assert_eq!(realm.holders(&placement), before);
    for view in realm.holder_views(&placement).await? {
        assert_eq!(view, before, "a node moved the bucket on its own");
    }
    for target in &reachable {
        assert!(!holds_placement(&realm.config, &placement, *target));
    }
    assert_eq!(
        transition_health(&realm.config, unix_timestamp_millis()).incomplete_buckets,
        1
    );

    // Force-finalize is the way out, and only because a verified copy exists.
    realm
        .mutate(
            0,
            RealmPlacementMutation::ForceFinalizeBucket {
                transition_id: transition,
                bucket,
                at_risk_report: "one target never proved".to_string(),
            },
        )
        .await?;
    realm
        .await_config("the forced bucket cuts over", move |config| {
            resolve_shard_holders(
                config,
                &PlacementRef {
                    strategy_id: placement.strategy_id,
                    shard: bucket,
                },
            ) != before
        })
        .await?;
    assert_eq!(realm.holders(&placement), preview.new_holders);

    // No rival genesis: what the reachable targets serve afterwards is the
    // history they pulled from the old holders, not one they minted to get
    // unstuck - the same document, on a set that shares no node with its author.
    for target in &reachable {
        let node = realm.find(*target);
        wait_until("target serves the pulled history", *target, || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

/// Adds a node to the realm config that no process backs, so a hand-off routed
/// through it can never complete.
async fn register_phantom(realm: &mut Topology) -> TestResult<NodeId> {
    let phantom = iroh::SecretKey::from_bytes(&[199; 32]).public();
    let mut config = realm.config.clone();
    config.ensure_node(phantom, RealmNodeKind::Management);
    config.placement_map.push(NodePlacementEntry {
        node_id: phantom,
        location: LOCATIONS[0].to_string(),
        weight: NODE_WEIGHT,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    });
    realm.apply_config(config).await?;
    Ok(phantom)
}

async fn create_document(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> TestResult<PlacementRef> {
    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: realm.actor(node),
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: false,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: document_path.to_string(),
                description: "stall fixture".to_string(),
                date_published: "2026-01-01".to_string(),
                license: None,
            },
        }),
        node.context.as_ref(),
    )
    .await?;
    replay_metadata_event_log(node.context.as_ref()).await?;
    Ok(created.record.placement)
}

async fn document_present(node: &TestNode, group_id: Ulid, document_id: Ulid) -> bool {
    drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        node.context.as_ref(),
    )
    .await
    .is_ok()
}
