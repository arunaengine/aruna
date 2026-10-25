// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use ulid::Ulid;

use super::*;
use crate::structs::identity::realm::RealmId;
use crate::structs::storage::distribution::presets::shard_layout;
use crate::structs::storage::distribution::{
    DistributionConfig, DistributionRequest, DistributionSettings, SetRoot, next_snapshot,
};

fn pending() -> DistributionSnapshot {
    let secret = |seed: u8| iroh::SecretKey::from_bytes(&[seed; 32]);
    let root = SetRoot {
        publisher: secret(1).public(),
        owner_group: Ulid::from_bytes([90; 16]),
        nonce: [7; 16],
    };
    let nodes = [
        (Ulid::from_bytes([1; 16]), secret(1).public()),
        (Ulid::from_bytes([2; 16]), secret(2).public()),
    ];
    let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 0).unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: "spire".to_string(),
            owner_group: root.owner_group,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    next_snapshot(RealmId::from_bytes([9; 32]), None, &create).unwrap()
}

fn fence(range_id: Ulid, epoch: u64) -> RangeFence {
    RangeFence {
        range_id,
        transfer_id: Ulid::from_bytes([4; 16]),
        epoch,
        since_ms: 1,
        uploads_open: false,
    }
}

#[test]
fn refuses_old_epochs() {
    // A former coordinator never writes the fenced range at the fenced epoch again.
    let snapshot = pending();
    let (lower, upper) = (&snapshot.config.ranges[0], &snapshot.config.ranges[1]);
    let mut local = LocalFence::default();
    assert!(local.fence_range(fence(lower.range_id, 1)));
    assert_eq!(
        local.check_write(&snapshot, lower, 1),
        Err(RouteError::Moving(lower.range_id))
    );
    assert_eq!(local.check_write(&snapshot, lower, 2), Ok(()));
    assert_eq!(local.check_write(&snapshot, upper, 1), Ok(()));
}

#[test]
fn grace_passes_completions() {
    // During the upload grace period only completions commit; closing it refuses them too.
    let snapshot = pending();
    let range = &snapshot.config.ranges[0];
    let mut local = LocalFence::default();
    local.fence_range(RangeFence {
        uploads_open: true,
        ..fence(range.range_id, 1)
    });
    let moving = Err(RouteError::Moving(range.range_id));
    assert_eq!(local.check_write(&snapshot, range, 1), moving);
    assert_eq!(local.check_completion(&snapshot, range, 1), Ok(()));
    assert!(local.close_uploads(range.range_id));
    assert!(
        !local.close_uploads(range.range_id),
        "closing twice changes nothing"
    );
    assert_eq!(local.check_completion(&snapshot, range, 1), moving);
}

#[test]
fn fences_never_lower() {
    // Fencing again below a known fence changes nothing, so a replay cannot reopen writes.
    let range_id = Ulid::from_bytes([5; 16]);
    let mut local = LocalFence::default();
    assert!(local.fence_range(fence(range_id, 3)));
    assert!(!local.fence_range(fence(range_id, 2)));
    assert_eq!(local.range(range_id).map(|fence| fence.epoch), Some(3));
    assert!(local.fence_range(fence(range_id, 4)));
    assert_eq!(local.ranges.len(), 1);
}

#[test]
fn deletion_closes_set() {
    let snapshot = pending();
    let range = &snapshot.config.ranges[1];
    let local = LocalFence {
        deletion: Some([3; 32]),
        ..LocalFence::default()
    };
    assert_eq!(
        local.check_write(&snapshot, range, 9),
        Err(RouteError::Deleting(snapshot.set_id()))
    );
    assert!(!local.receiving());
    let encoded = local.to_bytes().unwrap();
    assert_eq!(LocalFence::decode(Some(&encoded)).unwrap(), local);
    assert_eq!(LocalFence::decode(None).unwrap(), LocalFence::default());
}

#[test]
fn clear_sorts_first() {
    // The clear cursor of a set sorts before its range purge cursors, so a walk after it skips it.
    let set_id = Ulid::from_bytes([3; 16]);
    let clear = ClearCursor::key(set_id);
    let purge = PurgeCursor::key(set_id, Ulid::from_bytes([0; 16]));
    assert!(clear < purge && purge.starts_with(&clear));
    let cursor = ClearCursor {
        step: 2,
        after: Some(vec![1]),
    };
    let encoded = cursor.to_bytes().unwrap();
    assert_eq!(ClearCursor::decode(Some(&encoded)).unwrap(), cursor);
    assert_eq!(ClearCursor::decode(None).unwrap(), ClearCursor::default());
}
