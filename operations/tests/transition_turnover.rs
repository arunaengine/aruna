// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! A partial-overlap rebalance: one holder leaves a bucket, two stay, one joins.

mod topology;

use aruna_core::StructuredId;
use aruna_core::structs::TransitionLimits;
use aruna_operations::driver::drive;
use aruna_operations::metadata::create_document::mint_local_document;
use aruna_operations::metadata::forward::route_metadata_update;
use aruna_operations::metadata::get_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::update_document::UpdateMetadataDocumentMutation;
use aruna_operations::placement::holds_placement;
use aruna_operations::placement::transition::preview_transition;
use aruna_operations::realm::mutate_placement::RealmPlacementMutation;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

#[tokio::test]
async fn turnover_moves_holder() -> TestResult<()> {
    let mut realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let path = "datasets/turned-over";
    let origin = realm.leading_node(group_id, path);
    let leaver = origin.node_id();
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = realm
        .create_document(origin, group_id, document_id, path, "turnover fixture")
        .await?;
    let before = realm.assert_holder(leaver, &placement);
    for holder in &before {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    // Marking the origin full takes it out of the next map without draining it,
    // so the bucket keeps two of its three holders and gains one.
    let mut entry = realm
        .config
        .placement_entry(leaver)
        .cloned()
        .expect("every management node is mapped");
    entry.full = true;
    realm
        .mutate(0, RealmPlacementMutation::UpsertNode(entry))
        .await?;
    let epoch = realm.publish_map(0).await?;
    assert_eq!(
        realm.holders(&placement),
        before,
        "publishing is not activating"
    );

    let preview = preview_transition(
        &realm.config,
        placement.strategy_id,
        &[placement.shard],
        epoch,
    )?
    .pop()
    .expect("the preview covers the requested bucket");
    assert_eq!(preview.old_holders, before);
    assert!(!preview.disjoint, "the fixture must keep a shared holder");
    assert!(!preview.new_holders.contains(&leaver));
    let joiner = *preview
        .new_holders
        .iter()
        .find(|node_id| !before.contains(node_id))
        .expect("turnover replaces the leaver");

    let transition = realm
        .start_transition(
            1,
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

    // Preview equals outcome, on every node's own replicated view.
    let after = realm.holders(&placement);
    assert_eq!(after, preview.new_holders);
    for view in realm.holder_views(&placement).await? {
        assert_eq!(view, after, "holder set diverged across nodes");
    }
    // A complete local copy is not holdership: only the activation is.
    assert!(!holds_placement(&realm.config, &placement, leaver));
    assert!(document_present(realm.find(leaver), group_id, document_id).await);
    // Zero grace releases only once the leaver's drain report reduces, then
    // membership collapses from `|old U new|` back to the holders.
    realm.await_release(transition).await?;
    assert_eq!(realm.members(&placement), after);

    let node = realm.find(joiner);
    wait_until("document reaches the new holder", joiner, || {
        document_present(node, group_id, document_id)
    })
    .await?;

    // D10: the write arrives at the leaver and reaches the new holders anyway.
    let leaver_node = realm.find(leaver);
    route_metadata_update(
        &leaver_node.context,
        realm.actor(leaver_node),
        None,
        document_id,
        None,
        UpdateMetadataDocumentMutation::UpsertDataEntity {
            jsonld: r#"{"@id":"./turned-over.txt","@type":"File","name":"turned-over.txt"}"#
                .to_string(),
        },
        Some(realm.bearer_token()),
    )
    .await?;
    for holder in &after {
        let node = realm.find(*holder);
        wait_until("forwarded write reaches holder", *holder, || async {
            drive(
                GetMetadataDocumentOperation::new(group_id, document_id),
                node.context.as_ref(),
            )
            .await
            .is_ok_and(|view| view.jsonld.contains("turned-over.txt"))
        })
        .await?;
    }

    realm.shutdown().await;
    Ok(())
}

async fn document_present(node: &TestNode, group_id: Ulid, document_id: Ulid) -> bool {
    drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        node.context.as_ref(),
    )
    .await
    .is_ok()
}
