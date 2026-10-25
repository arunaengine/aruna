// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_record::{member, replicated, secret};

/// Member 1 coordinates both shards and member 2 keeps a copy of each.
fn over_full() -> DistributionSnapshot {
    let (mut snapshot, _) = replicated(true, 1);
    snapshot.config.ranges[1].coordinator = member(1);
    snapshot.config.ranges[1].replicas = vec![member(2)];
    snapshot
}

fn fill(first: u64, second: u64) -> BTreeMap<NodeId, u64> {
    BTreeMap::from([(secret(1).public(), first), (secret(2).public(), second)])
}

#[test]
fn moves_one_range() {
    let snapshot = over_full();
    let proposal = propose(&snapshot, secret(1).public(), &fill(900, 100)).unwrap();
    assert_eq!(proposal.to, member(2));
    assert!(
        snapshot
            .config
            .ranges
            .iter()
            .any(|r| r.range_id == proposal.range_id)
    );
}

#[test]
fn balanced_sets_stay() {
    let snapshot = over_full();
    // Within the margin of the average nothing moves.
    assert_eq!(
        propose(&snapshot, secret(1).public(), &fill(105, 100)),
        None
    );
    // A node that hears nothing of itself does not decide.
    let unknown = BTreeMap::from([(secret(2).public(), 1)]);
    assert_eq!(propose(&snapshot, secret(1).public(), &unknown), None);
    // A member with a single range would leave the set instead of freeing room.
    let (single, _) = replicated(true, 1);
    assert_eq!(propose(&single, secret(1).public(), &fill(900, 100)), None);
}

#[test]
fn fill_is_relative() {
    // A node with more bytes but more room is the less filled one.
    let large = permille(900, Some(10_000)).unwrap();
    let small = permille(300, Some(1_000)).unwrap();
    assert!(large < small);
    let snapshot = over_full();
    assert_eq!(
        propose(&snapshot, secret(1).public(), &fill(large, small)),
        None
    );
    assert_eq!(permille(5, None), None, "an unknown capacity takes no part");
    assert_eq!(permille(5, Some(0)), None);
}
