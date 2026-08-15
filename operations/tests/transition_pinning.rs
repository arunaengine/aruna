// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! The anti-reshuffle regression: once a bucket is activated, no config edit
//! moves its holder set.
//!
//! Before activation pinning, holder resolution rebuilt its view from the live
//! `placement_map` on every call, so a weight change, a label, or a joining node
//! silently re-ranked every bucket and stranded documents on nodes that were no
//! longer holders. These tests edit exactly those inputs and require the holder
//! sets - on every node's own replicated view - to stay byte-identical, and the
//! documents already written to stay readable.

mod topology;

use std::collections::BTreeMap;

use aruna_core::StructuredId;
use aruna_core::structs::{NodePlacementEntry, PlacementRef};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::placement::resolve_shard_holders;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

#[tokio::test]
async fn config_edits_never_move_holders() -> TestResult<()> {
    let mut realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let origin_id = realm.node(0).node_id();
    let path = "datasets/pinned";
    let document_id =
        mint_local_document(&realm.config, &realm.actor(realm.node(0)), group_id, path)?.as_ulid();
    let placement = realm
        .origin_placement(realm.node(0), group_id, document_id, path)
        .expect("a Management node holds buckets");
    let before = realm.holder_map();
    assert!(
        before
            .values()
            .any(|holders| holders.len() == REPLICATION_FACTOR as usize),
        "the fixture must cap at least one bucket below the node count"
    );

    create_document(&realm, realm.node(0), group_id, document_id, path).await?;
    for holder in realm.assert_holder(origin_id, &placement) {
        let node = realm.find(holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    // Every selection input the live view used to read: weights, labels, a
    // location move, and a freshly published map that nothing activates.
    let mut edited = realm.config.clone();
    for (index, entry) in edited.placement_map.iter_mut().enumerate() {
        entry.weight = 1 + (index as u32 % 3) * 900;
        entry.labels.insert("tier".to_string(), format!("t{index}"));
        entry.location = format!("moved-{index}");
    }
    let joiner = realm.user_node().node_id();
    edited.placement_map.push(NodePlacementEntry {
        node_id: joiner,
        location: "moved-late".to_string(),
        weight: 10_000,
        full: false,
        draining: false,
        labels: BTreeMap::new(),
    });
    let published = edited.snapshot_candidate_map();
    assert_eq!(published, 2, "the fixture activated epoch 1");
    realm.apply_config(edited).await?;

    assert_eq!(
        realm.holder_map(),
        before,
        "a config edit moved an activated holder set"
    );
    for view in realm.holder_views(&placement).await? {
        assert_eq!(
            view,
            before[&(placement.strategy_id, placement.shard)],
            "holder set diverged across nodes after the edit"
        );
    }

    // The write that landed before the edit is still served by its holders, and
    // a new create still stamps a bucket its origin holds.
    let holders = realm.assert_holder(origin_id, &placement);
    for holder in &holders {
        assert!(document_present(realm.find(*holder), group_id, document_id).await);
    }
    let second_path = "datasets/pinned-after";
    let second_id = mint_local_document(
        &realm.config,
        &realm.actor(realm.node(0)),
        group_id,
        second_path,
    )?
    .as_ulid();
    let stamped = create_document(&realm, realm.node(0), group_id, second_id, second_path).await?;
    assert!(
        resolve_shard_holders(&realm.config, &stamped).contains(&origin_id),
        "create stamped a bucket its origin does not hold"
    );

    realm.shutdown().await;
    Ok(())
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
                description: "pinning fixture".to_string(),
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
