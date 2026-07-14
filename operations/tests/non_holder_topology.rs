//! Holder and non-holder coverage on a realm sized above the replication factor.
//!
//! Every other multi-node fixture in this workspace runs at `node_count <= RF`,
//! where every node holds every bucket and non-holder behaviour is unobservable.
//! These tests run five nodes at RF three and prove holdership against the bucket
//! a create actually stamps before exercising the path, so a regression to
//! universal holdership fails the fixture instead of quietly voiding the test.

mod topology;

use aruna_core::UserId;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{PlacementRef, RealmId};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::{
    GetMetadataDocumentError, GetMetadataDocumentOperation, load_metadata_record_by_document,
};
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::sync_placement::sort_node_ids;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const NODE_COUNT: usize = 5;
const REPLICATION_FACTOR: u32 = 3;

// The fixture itself is the deliverable: without this proof every test below
// degrades silently into the all-nodes-hold-everything case.
#[tokio::test]
async fn fixture_proves_nonholders() -> TestResult<()> {
    let realm = Topology::spawn(RealmId([90u8; 32]), NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([11; 16]);
    let document_id = Ulid::from_bytes([12; 16]);
    let path = "datasets/proof";
    let origin = realm.node(0);

    let placement = realm
        .origin_placement(origin, group_id, document_id, path)
        .expect("a Management node holds buckets of the default strategy");
    let holders = realm.assert_holder(origin.node_id(), &placement);
    assert_eq!(holders.len(), REPLICATION_FACTOR as usize);

    let non_holders = realm.non_holder_ids(&placement);
    assert_eq!(non_holders.len(), NODE_COUNT - REPLICATION_FACTOR as usize);
    for node_id in &non_holders {
        realm.assert_not_holder(*node_id, &placement);
    }

    // Holders are a pure function of the replicated config and the stamped bucket,
    // so the proof is exact rather than probabilistic: every node derives the same
    // set, in the same rank order.
    for view in realm.holder_views(&placement).await? {
        assert_eq!(view, holders, "holder set diverged across nodes");
    }

    realm.shutdown().await;
    Ok(())
}

// D3: a create stamps the best-ranked bucket its origin already holds, so the
// origin is always a holder of what it creates and can always publish it. A blind
// document hash would place it anywhere, including on buckets the origin does not
// hold - the state that makes an offline node's writes undeliverable.
#[tokio::test]
async fn create_stamps_origin() -> TestResult<()> {
    let realm = Topology::spawn(RealmId([91u8; 32]), NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([21; 16]);
    let document_id = Ulid::from_bytes([22; 16]);
    let path = "datasets/stamped-by-origin";
    let origin = realm.node(0);
    let expected = realm
        .origin_placement(origin, group_id, document_id, path)
        .expect("a Management node holds buckets");
    let holders = realm.assert_holder(origin.node_id(), &expected);

    let created = create_document(&realm, origin, group_id, document_id, path).await?;
    assert_eq!(
        created, expected,
        "create stamped a bucket it does not hold"
    );

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let view = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        origin.context.as_ref(),
    )
    .await?;
    assert_eq!(view.record.placement, expected);
    let mut recorded = view.record.holder_node_ids.clone();
    let mut planned = holders.clone();
    sort_node_ids(&mut recorded);
    sort_node_ids(&mut planned);
    assert_eq!(
        recorded, planned,
        "origin recorded a holder set that is not the stamped bucket's"
    );

    realm.shutdown().await;
    Ok(())
}

// A non-holder carries the document's registry row - the row rides the
// everywhere-bound registry class, which is what lets it route reads and writes -
// but it holds no bucket of the document, so it has no graph. The miss is
// therefore the graph's, not the record's.
#[tokio::test]
async fn read_misses_nonholder() -> TestResult<()> {
    let realm = Topology::spawn(RealmId([92u8; 32]), NODE_COUNT, REPLICATION_FACTOR).await?;

    let group_id = Ulid::from_bytes([31; 16]);
    let document_id = Ulid::from_bytes([32; 16]);
    let path = "datasets/read-off-holders";
    let origin = realm.node(0);
    let placement = create_document(&realm, origin, group_id, document_id, path).await?;
    let holders = realm.assert_holder(origin.node_id(), &placement);

    for holder in &holders {
        let node = realm.find(*holder);
        wait_until("document reaches holder", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }

    let bystander = realm.non_holder(&placement);
    wait_until(
        "registry row reaches non-holder",
        bystander.node_id(),
        || registry_row_present(bystander, document_id),
    )
    .await?;

    let result = drive(
        GetMetadataDocumentOperation::new(group_id, document_id),
        bystander.context.as_ref(),
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        GetMetadataDocumentError::MetadataError(MetadataError::GraphNotFound),
        "non-holder served a graph it never received"
    );

    realm.shutdown().await;
    Ok(())
}

fn document_config(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> CreateMetadataDocumentConfig {
    CreateMetadataDocumentConfig {
        actor: realm.actor(
            node,
            UserId::local(Ulid::from_bytes([9; 16]), realm.realm_id),
        ),
        group_id,
        document_id,
        document_path: document_path.to_string(),
        public: true,
        payload: CreateMetadataDocumentPayload::Scaffold {
            name: "Topology Dataset".to_string(),
            description: "Written on a realm above the replication factor".to_string(),
            date_published: "2026-01-01".to_string(),
            license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        },
    }
}

/// Creates locally on a bucket-holding node and returns the bucket it stamped.
async fn create_document(
    realm: &Topology,
    node: &TestNode,
    group_id: Ulid,
    document_id: Ulid,
    document_path: &str,
) -> TestResult<PlacementRef> {
    let created = drive(
        CreateMetadataDocumentOperation::new(document_config(
            realm,
            node,
            group_id,
            document_id,
            document_path,
        )),
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

async fn registry_row_present(node: &TestNode, document_id: Ulid) -> bool {
    load_metadata_record_by_document(node.context.as_ref(), document_id)
        .await
        .is_ok_and(|record| record.is_some())
}
