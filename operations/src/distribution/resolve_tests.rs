// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{probe_targets, range_holders};
use crate::distribution::test_record::{member, replicated, secret};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSnapshot, MemberState, MetadataCoverage,
};

/// The two shards split at "m", each replicated on the other member, every member ready.
fn ready_set() -> DistributionSnapshot {
    let (mut snapshot, _) = replicated(true, 1);
    for member in &mut snapshot.config.members {
        member.state = MemberState::Ready;
    }
    snapshot
}

#[test]
fn holders_are_copies() {
    let mut snapshot = ready_set();
    // Member 1 coordinates "a", member 2 keeps a copy of it.
    assert_eq!(
        range_holders(&snapshot, "a"),
        vec![secret(1).public(), secret(2).public()]
    );
    // A replica that keeps copies on use may serve one, after the coordinator.
    snapshot.config.members[1].strategy = DataStrategy::CacheOnUse;
    assert_eq!(
        range_holders(&snapshot, "a"),
        vec![secret(1).public(), secret(2).public()]
    );
    // A streaming replica keeps no bytes to serve from.
    snapshot.config.members[1].strategy = DataStrategy::Stream;
    assert_eq!(range_holders(&snapshot, "a"), vec![secret(1).public()]);
    // A member that is not ready serves nothing.
    snapshot.config.members[0].state = MemberState::CatchingUp;
    assert!(range_holders(&snapshot, "a").is_empty());
}

#[test]
fn probes_one_full_member() {
    let mut snapshot = ready_set();
    // Assigned coverage: the coordinator of each range is asked, never this node.
    assert_eq!(
        probe_targets(&snapshot, secret(1).public()),
        vec![secret(2).public()]
    );
    assert_eq!(probe_targets(&snapshot, secret(3).public()).len(), 2);
    // One ready member with all coverage covers the whole set.
    let full = snapshot
        .config
        .members
        .iter_mut()
        .find(|candidate| candidate.member_id == member(2))
        .unwrap();
    full.coverage = MetadataCoverage::All;
    assert_eq!(
        probe_targets(&snapshot, secret(3).public()),
        vec![secret(2).public()]
    );
}

#[tokio::test]
async fn content_page_is_bounded() {
    use super::content_versions;
    use crate::distribution::test_store::{apply, context, install, open, range, write};
    use ulid::Ulid;

    let (_dir, handle) = open();
    let set = Ulid::from_bytes([9; 16]);
    install(&handle, set, &[range(1, "", None)]).await;
    assert!(apply(&handle, write(set, "a", 3, 10)).await.unwrap());
    let context = context(&handle);
    let found = content_versions(&context, set, &[3; 32], 4).await.unwrap();
    assert_eq!(
        found,
        (vec![(Ulid::from_bytes([3; 16]), "a".to_string())], false)
    );
    let cut = content_versions(&context, set, &[3; 32], 0).await.unwrap();
    assert_eq!(cut, (Vec::new(), true), "a cut page says more exist");
    let other = content_versions(&context, set, &[4; 32], 4).await.unwrap();
    assert_eq!(other, (Vec::new(), false));
}
