//! Tests split accounting against real storage: the charged total before the handoff, child
//! rebuilds, the handoff itself, the ownership guard and refused splits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::inventory::InventoryUpdate;
use crate::distribution::rebuild::{RebuildConfig, RebuildOperation, RebuildOutcome};
use crate::distribution::test_store::{
    account, apply, begin, commit, context, counters, exact, get, install, open, purge, range,
    stage, write,
};
use crate::driver::drive;
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([5; 16])
}

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn split_at(key: &str) -> SplitOperation {
    SplitOperation::new(SplitConfig {
        set_id: set(),
        range_id: id(1),
        split_key: key.to_string(),
        left: id(2),
        right: id(3),
    })
}

async fn rebuild(handle: &StorageHandle, range_id: Ulid) -> RebuildOutcome {
    let config = RebuildConfig {
        set_id: set(),
        range_id,
        page_rows: 2,
        page_bytes: 1 << 20,
        max_pages: 100,
    };
    drive(RebuildOperation::new(config), &context(handle))
        .await
        .expect("rebuild runs")
}

/// The sum of every row a range's charge names.
async fn charged(handle: &StorageHandle, range_id: Ulid) -> RangeCounters {
    let account = account(handle, set(), range_id)
        .await
        .expect("account exists");
    let mut total = RangeCounters::default();
    for row in charged_rows(set(), range_id, &account) {
        total = total.merge(counters(handle, row).await).unwrap();
    }
    total
}

#[tokio::test]
async fn charge_awaits_handoff() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed, size) in [("a", 1, 1), ("c", 2, 2), ("x", 3, 4)] {
        assert!(apply(&handle, write(set(), key, seed, size)).await.unwrap());
    }
    drive(split_at("m"), &context(&handle)).await.unwrap();

    assert!(apply(&handle, write(set(), "b", 4, 8)).await.unwrap());
    assert!(apply(&handle, write(set(), "y", 5, 16)).await.unwrap());
    assert!(apply(&handle, purge(set(), "a", &[1])).await.unwrap());
    assert_eq!(
        charged(&handle, id(1)).await,
        exact(4, 30),
        "snapshot plus pending rows"
    );
    let left = account(&handle, set(), id(2)).await.unwrap();
    assert!(
        !left.is_complete(),
        "child counters stay incomplete until the handoff"
    );

    assert_eq!(rebuild(&handle, id(2)).await, RebuildOutcome::Waiting);
    assert!(
        apply(&handle, write(set(), "d", 6, 32)).await.unwrap(),
        "after the left walk ended"
    );
    assert_eq!(charged(&handle, id(1)).await, exact(5, 62));
    assert_eq!(rebuild(&handle, id(3)).await, RebuildOutcome::Done);

    assert!(
        account(&handle, set(), id(1)).await.is_none(),
        "the parent is retired"
    );
    for (child, total) in [(id(2), exact(3, 42)), (id(3), exact(2, 20))] {
        let account = account(&handle, set(), child).await.unwrap();
        assert!(account.is_complete());
        assert_eq!(charged(&handle, child).await, total);
        assert!(counters(&handle, pending_key(set(), child)).await.is_zero());
    }
    assert!(
        counters(&handle, generation_key(set(), id(1), 1))
            .await
            .is_zero()
    );
}

#[tokio::test]
async fn divides_sample() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    assert!(apply(&handle, write(set(), "c", 1, 1)).await.unwrap());
    assert!(apply(&handle, write(set(), "x", 2, 1)).await.unwrap());
    drive(split_at("m"), &context(&handle)).await.unwrap();
    for (key, owner) in [("c", id(2)), ("x", id(3))] {
        let entry = SampleEntry::of_key(key);
        let moved = get(&handle, KEY_SAMPLE_KEYSPACE, entry.row_key(set(), owner)).await;
        assert!(moved.is_some());
        let old = get(&handle, KEY_SAMPLE_KEYSPACE, entry.row_key(set(), id(1))).await;
        assert!(old.is_none());
        let count = counters(&handle, sample_count_key(set(), owner)).await;
        assert_eq!(count.sampled, 1, "each child counts its share");
    }
}

/// A write that started before the split conflicts, and its retry reaches the child.
#[tokio::test]
async fn split_conflicts_writer() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    let writer = begin(&handle).await;
    stage(&handle, writer, &mut write(set(), "b", 1, 3))
        .await
        .unwrap();
    drive(split_at("m"), &context(&handle)).await.unwrap();
    assert!(!commit(&handle, writer).await);
    assert!(
        counters(&handle, generation_key(set(), id(1), 1))
            .await
            .is_zero()
    );
    assert!(apply(&handle, write(set(), "b", 1, 3)).await.unwrap());
    assert_eq!(
        counters(&handle, pending_key(set(), id(2))).await,
        exact(1, 3)
    );
}

#[tokio::test]
async fn refuses_bad_splits() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "b", Some("q"))]).await;
    let result = drive(split_at("b"), &context(&handle)).await;
    assert!(
        matches!(result, Err(SplitError::Refused(..))),
        "an empty left child"
    );
    let result = drive(split_at("r"), &context(&handle)).await;
    assert!(
        matches!(result, Err(SplitError::Refused(..))),
        "a key outside the range"
    );

    let config = RebuildConfig {
        set_id: set(),
        range_id: id(1),
        page_rows: 2,
        page_bytes: 1 << 20,
        max_pages: 0,
    };
    let paused = drive(RebuildOperation::new(config), &context(&handle)).await;
    assert_eq!(paused, Ok(RebuildOutcome::Paused));
    let result = drive(split_at("m"), &context(&handle)).await;
    assert!(
        matches!(result, Err(SplitError::Refused(..))),
        "a rebuild is running"
    );
}

/// Both children keep the parent's epoch maximum through the handoff.
#[tokio::test]
async fn children_keep_epoch() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    let mut change = write(set(), "a", 1, 1).change().clone();
    change.added[0].epoch = OriginalEpoch(7);
    assert!(apply(&handle, InventoryUpdate::new(change)).await.unwrap());
    assert!(apply(&handle, purge(set(), "a", &[1])).await.unwrap());
    drive(split_at("m"), &context(&handle)).await.unwrap();
    assert_eq!(rebuild(&handle, id(2)).await, RebuildOutcome::Waiting);
    assert_eq!(rebuild(&handle, id(3)).await, RebuildOutcome::Done);
    for child in [id(2), id(3)] {
        assert_eq!(charged(&handle, child).await.max_epoch, OriginalEpoch(7));
    }
}

/// A sample larger than one read is refused, so no entry stays behind under the parent.
#[tokio::test]
async fn refuses_crowded_sample() {
    use crate::distribution::test_store::crowd_sample;

    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    crowd_sample(&handle, set(), 1, u64::from(SAMPLE_SIZE) * 2 + 1).await;
    let result = drive(split_at("m"), &context(&handle)).await;
    assert!(matches!(result, Err(SplitError::Refused(..))));
}
