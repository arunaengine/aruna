// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]
//! Membership bounds around a transition (#399).
//!
//! Two properties, neither of which any other scenario pins: an abort leaves
//! the realm with holder-only membership and no half-moved bucket, and a
//! transition limited to one bucket in flight really does hand them over one at
//! a time - observable in the order the cut-overs are stamped, which is carried
//! data rather than a sampled race.

mod topology;

use aruna_core::structs::{PlacementRef, TransitionLimits};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::mutate_realm_placement::RealmPlacementMutation;
use aruna_operations::placement::transition::{preview_transition, transition_health};

use topology::{TestResult, Topology};

const MANAGEMENT_NODES: usize = 5;
const USER_NODES: usize = 1;
const REPLICATION_FACTOR: u32 = 3;
const SHARD_COUNT: u32 = 4;
const BUCKETS: [u32; 2] = [0, 1];

#[tokio::test]
async fn abort_and_bound() -> TestResult<()> {
    let mut realm = Topology::spawn_sharded(
        MANAGEMENT_NODES,
        USER_NODES,
        REPLICATION_FACTOR,
        SHARD_COUNT,
    )
    .await?;
    let strategy_id = realm
        .config
        .default_strategy_id
        .expect("the seeded realm has a default strategy");
    let placements: Vec<PlacementRef> = BUCKETS
        .iter()
        .map(|shard| PlacementRef {
            strategy_id,
            shard: *shard,
        })
        .collect();

    let epoch = fill_a_holder(&mut realm, &placements[0]).await?;
    let preview = preview_transition(&realm.config, strategy_id, &BUCKETS, epoch)?;
    let aborted = realm
        .start_transition(
            1,
            strategy_id,
            BUCKETS.to_vec(),
            epoch,
            TransitionLimits {
                max_incomplete_buckets: 1,
                grace_ms: 0,
            },
        )
        .await?;
    realm
        .mutate(1, RealmPlacementMutation::AbortTransition(aborted))
        .await?;
    realm
        .await_config("the aborted record is released", move |config| {
            config.transition(&aborted).is_none()
        })
        .await?;

    // Whatever the reconciler managed before the abort, no bucket is left half
    // moved and nobody outside the holders stays a member.
    for (placement, bucket) in placements.iter().zip(&preview) {
        let holders = realm.holders(placement);
        assert!(
            holders == bucket.old_holders || holders == bucket.new_holders,
            "bucket {} holds a set the transition never named: {holders:?}",
            bucket.bucket
        );
        assert_eq!(realm.members(placement), holders);
    }
    assert_eq!(
        transition_health(&realm.config, unix_timestamp_millis()).active,
        0
    );

    // One bucket in flight: the second cut-over cannot be stamped before the
    // first, because no node steps a bucket while an earlier one is open.
    let epoch = fill_a_holder(&mut realm, &placements[1]).await?;
    let bounded = realm
        .start_transition(
            1,
            strategy_id,
            BUCKETS.to_vec(),
            epoch,
            TransitionLimits {
                max_incomplete_buckets: 1,
                ..TransitionLimits::default()
            },
        )
        .await?;
    realm.await_transition(bounded).await?;

    let record = realm
        .config
        .transition(&bounded)
        .expect("the default grace keeps the record readable");
    let stamps: Vec<u64> = record
        .plan
        .buckets
        .iter()
        .map(|bucket| {
            record
                .completion(bucket.bucket)
                .expect("every bucket cut over")
                .completed_at_ms
        })
        .collect();
    assert!(
        stamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "buckets cut over out of order under a one-bucket bound: {stamps:?}"
    );
    // The record is still inside its grace, so membership sits at its peak -
    // and the peak is exactly the two sets the transition named.
    for bucket in &record.plan.buckets {
        let placement = PlacementRef {
            strategy_id,
            shard: bucket.bucket,
        };
        for member in realm.members(&placement) {
            assert!(
                bucket.old_holders.contains(&member) || bucket.target_holders.contains(&member),
                "member {member} is outside the transition's holder sets"
            );
        }
    }

    realm.shutdown().await;
    Ok(())
}

/// Marks the bucket's rank-0 holder full and publishes the resulting map, so
/// the next transition has somewhere to move the bucket to.
async fn fill_a_holder(realm: &mut Topology, placement: &PlacementRef) -> TestResult<u64> {
    let holder = realm.holders(placement)[0];
    let mut entry = realm
        .config
        .placement_entry(holder)
        .cloned()
        .expect("every management node is mapped");
    entry.full = true;
    realm
        .mutate(0, RealmPlacementMutation::UpsertNode(entry))
        .await?;
    realm.publish_map(0).await
}
