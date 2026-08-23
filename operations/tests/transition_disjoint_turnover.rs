// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! Zero-overlap turnover, twice, in opposite directions (#400).
//!
//! A disjoint hand-off is the case that would break a design keyed on holders:
//! not one node of the old set survives into the new one. Topic identity is
//! `(realm, strategy, bucket)` and nothing else, so the bucket keeps its single
//! topic and its whole history across both moves - which is what these
//! assertions are for: the document written before the first turnover is still
//! served by a set that shares no node with the one that accepted it.

mod topology;

use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::document::shard_topic_id;
use aruna_core::structs::{PlacementRef, TransitionLimits};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::mutate_realm_placement::RealmPlacementMutation;
use aruna_operations::placement::transition::preview_transition;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 6;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;
const SHARD_COUNT: u32 = 4;

#[tokio::test]
async fn turnover_keeps_history() -> TestResult<()> {
    let mut realm = Topology::spawn_sharded(
        MANAGEMENT_NODES,
        USER_NODES,
        REPLICATION_FACTOR,
        SHARD_COUNT,
    )
    .await?;
    let group_id = realm.seed_group().await?;

    let path = "datasets/disjoint";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = create_document(&realm, origin, group_id, document_id, path).await?;
    let first = realm.assert_holder(origin.node_id(), &placement);
    let topic = shard_topic_id(realm.realm_id, &placement);
    for holder in &first {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let second = turn_over(&mut realm, &placement, &first, &[]).await?;
    assert!(
        second.iter().all(|holder| !first.contains(holder)),
        "the first turnover kept a holder: {second:?}"
    );
    let back = turn_over(&mut realm, &placement, &second, &first).await?;
    assert_eq!(back, first, "the bucket did not return to its first set");

    // One topic, one history: a node of the first set serves the document again
    // after it has been handed away and back.
    assert_eq!(shard_topic_id(realm.realm_id, &placement), topic);
    for holder in &back {
        let node = realm.find(*holder);
        wait_until("document survives two turnovers", *holder, || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

/// Fills `fill` and empties `drain`, then hands `placement` over to whatever
/// the resulting map resolves. Returns the new holder set.
async fn turn_over(
    realm: &mut Topology,
    placement: &PlacementRef,
    fill: &[NodeId],
    drain: &[NodeId],
) -> TestResult<Vec<NodeId>> {
    for (node_id, full) in fill
        .iter()
        .map(|node_id| (node_id, true))
        .chain(drain.iter().map(|node_id| (node_id, false)))
    {
        let mut entry = realm
            .config
            .placement_entry(*node_id)
            .cloned()
            .expect("every management node is mapped");
        entry.full = full;
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
    assert!(preview.disjoint, "the fixture must swap every holder");

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
    realm.await_transition(transition).await?;
    let holders = realm.holders(placement);
    assert_eq!(
        holders, preview.new_holders,
        "preview did not equal outcome"
    );
    Ok(holders)
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
                description: "disjoint fixture".to_string(),
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
