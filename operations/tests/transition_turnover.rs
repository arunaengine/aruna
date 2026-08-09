// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! A partial-overlap rebalance: one holder leaves a bucket, two stay, one joins.
//!
//! Turnover is the case expansion cannot reach: the bucket's holder set both
//! grows and shrinks, so the leaving node ends up with a complete local copy it
//! is no longer authority for. The scenario pins that the preview an operator
//! sees is the outcome they get, that the leaver's stale copy never counts as
//! holdership, and that a write arriving there is forwarded to the new holders
//! rather than silently applied (DECISIONS D10).

mod topology;

use aruna_core::StructuredId;
use aruna_core::structs::{PlacementRef, TransitionLimits};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::forward::update_metadata_document_routed;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::mutate_realm_placement::RealmPlacementMutation;
use aruna_operations::placement::holds_placement;
use aruna_operations::placement::transition::preview_transition;
use aruna_operations::update_metadata_document::UpdateMetadataDocumentMutation;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

#[tokio::test]
async fn turnover_moves_holder() -> TestResult<()> {
    let mut realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let leaver = realm.node(0).node_id();

    let path = "datasets/turned-over";
    let document_id =
        mint_local_document(&realm.config, &realm.actor(realm.node(0)), group_id, path)?.as_ulid();
    let placement = create_document(&realm, realm.node(0), group_id, document_id, path).await?;
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
    // The record carried a zero grace, so the leaver was released with the
    // cutover and membership collapsed from `|old U new|` back to the holders.
    assert_eq!(realm.members(&placement), after);

    let node = realm.find(joiner);
    wait_until("document reaches the new holder", joiner, || {
        document_present(node, group_id, document_id)
    })
    .await?;

    // D10: the write arrives at the leaver and reaches the new holders anyway.
    let leaver_node = realm.find(leaver);
    update_metadata_document_routed(
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
                description: "turnover fixture".to_string(),
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
