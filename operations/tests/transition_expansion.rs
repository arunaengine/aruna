// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! A late node joins and an explicit expansion transition hands it the buckets.
//!
//! Expansion is the superset case: every bucket's target set contains its old
//! set, so nothing moves off any node and the old holders stay write authority
//! throughout. It still runs the full machinery - barrier from every old
//! holder, a pulled and verified copy on every target, a signed proof each, and
//! only then the reduced cutover - which is what makes a multi-node bootstrap
//! exercise the same path a rebalance does.

mod topology;

use aruna_core::StructuredId;
use aruna_core::structs::{PlacementRef, RealmNodeKind, TransitionLimits};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_local_document,
};
use aruna_operations::driver::drive;
use aruna_operations::get_metadata_document::GetMetadataDocumentOperation;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use ulid::Ulid;

use topology::{TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 4;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;

#[tokio::test]
async fn expansion_hands_buckets_to_the_joiner() -> TestResult<()> {
    let mut realm = Topology::spawn(MANAGEMENT_NODES, USER_NODES, REPLICATION_FACTOR).await?;
    let group_id = realm.seed_group().await?;
    let everywhere = realm
        .config
        .strategies
        .iter()
        .find(|strategy| strategy.replica_count.is_none())
        .map(|strategy| strategy.strategy_id)
        .expect("the seeded realm binds an everywhere strategy");
    // Two named buckets: every step of the machinery runs, without paying for
    // the strategy's whole fan-out in a fixture.
    const BUCKETS: [u32; 2] = [0, 1];
    let probe = PlacementRef {
        strategy_id: everywhere,
        shard: BUCKETS[0],
    };
    let before = realm.holders(&probe);

    // A document written before the transition must still be readable after it.
    let path = "datasets/expanded";
    let document_id =
        mint_local_document(&realm.config, &realm.actor(realm.node(0)), group_id, path)?.as_ulid();
    let placement = create_document(&realm, realm.node(0), group_id, document_id, path).await?;

    let joiner = realm.spawn_late_node(RealmNodeKind::Management).await?;
    // A registered node holds nothing until a map naming it is activated.
    assert_eq!(realm.holders(&probe), before);
    assert!(!realm.is_holder(joiner, &probe));

    let epoch = realm.publish_map(0).await?;
    assert_eq!(
        realm.holders(&probe),
        before,
        "publishing is not activating"
    );

    // Every bucket may run at once: the one-at-a-time bound is exercised by the
    // bounds scenario, not here.
    let transition = realm
        .start_transition(
            0,
            everywhere,
            BUCKETS.to_vec(),
            epoch,
            TransitionLimits {
                max_incomplete_buckets: u32::MAX,
                ..TransitionLimits::default()
            },
        )
        .await?;
    // Every bucket of an everywhere strategy grows by exactly the joiner.
    let plan = realm
        .config
        .transition(&transition)
        .expect("the transition replicated")
        .plan
        .clone();
    for bucket in &plan.buckets {
        assert!(
            bucket
                .old_holders
                .iter()
                .all(|holder| bucket.target_holders.contains(holder)),
            "expansion must not move a bucket off any node"
        );
        assert!(bucket.target_holders.contains(&joiner));
    }
    // The joiner is a member from the moment the record exists, which is what
    // lets it pull. (Whether it is already a holder is a race: the reconciler
    // may have completed the whole handoff by now, so authority during the
    // transition is asserted by the reducer tests instead.)
    assert!(realm.members(&probe).contains(&joiner));

    realm.await_transition(transition).await?;

    let after = realm.holders(&probe);
    assert!(after.contains(&joiner), "the joiner never became a holder");
    for holder in &before {
        assert!(after.contains(holder), "expansion dropped an old holder");
    }
    for view in realm.holder_views(&probe).await? {
        assert_eq!(view, after, "holder set diverged across nodes");
    }
    let record = realm
        .config
        .transition(&transition)
        .expect("the completed record is still readable");
    for bucket in &plan.buckets {
        assert!(
            record.completion(bucket.bucket).is_some(),
            "bucket {} never cut over",
            bucket.bucket
        );
        for target in &bucket.target_holders {
            assert!(
                record
                    .proofs_for(bucket.bucket)
                    .any(|proof| proof.holder == *target),
                "target {target} never proved bucket {}",
                bucket.bucket
            );
        }
        for old in &bucket.old_holders {
            assert!(
                record
                    .barriers
                    .iter()
                    .any(|barrier| barrier.bucket == bucket.bucket && barrier.reported_by == *old),
                "old holder {old} never fenced bucket {}",
                bucket.bucket
            );
        }
    }

    // The pre-transition document survives on its holders, and a write made
    // after the cutover is readable too.
    for holder in realm.holders(&placement) {
        let node = realm.find(holder);
        wait_until("document survives cutover", node.node_id(), || {
            document_present(node, group_id, document_id)
        })
        .await?;
    }
    let late_path = "datasets/expanded-after";
    let late_id = mint_local_document(
        &realm.config,
        &realm.actor(realm.node(0)),
        group_id,
        late_path,
    )?
    .as_ulid();
    create_document(&realm, realm.node(0), group_id, late_id, late_path).await?;
    let origin = realm.node(0);
    wait_until("post-cutover write is readable", origin.node_id(), || {
        document_present(origin, group_id, late_id)
    })
    .await?;

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
                description: "expansion fixture".to_string(),
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
