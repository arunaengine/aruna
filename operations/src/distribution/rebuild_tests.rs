//! Tests the counter rebuild against real storage: drift repair, resume from the durable
//! cursor, conflicts with writers that read an old cursor, and empty ranges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::inventory::{InventoryChange, InventoryUpdate};
use crate::distribution::test_store::{
    account, apply, begin, commit, context, counters, exact, install, open, purge, range, stage,
    write,
};
use crate::driver::drive;
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([4; 16])
}

fn range_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

fn config(page_rows: usize, max_pages: usize) -> RebuildConfig {
    RebuildConfig {
        set_id: set(),
        range_id: range_id(),
        page_rows,
        page_bytes: 1 << 20,
        max_pages,
    }
}

async fn run(handle: &StorageHandle, page_rows: usize, max_pages: usize) -> RebuildOutcome {
    let operation = RebuildOperation::new(config(page_rows, max_pages));
    drive(operation, &context(handle))
        .await
        .expect("rebuild runs")
}

async fn generation(handle: &StorageHandle, generation: u64) -> RangeCounters {
    counters(handle, generation_key(set(), range_id(), generation)).await
}

#[tokio::test]
async fn repairs_drift() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed, size) in [("a", 1, 10), ("b", 2, 5), ("c", 3, 3)] {
        assert!(apply(&handle, write(set(), key, seed, size)).await.unwrap());
    }
    let txn_id = begin(&handle).await;
    let drift = StorageEffect::AddCounters {
        key_space: RANGE_COUNTER_KEYSPACE.to_string(),
        deltas: vec![(generation_key(set(), range_id(), 1).into(), exact(5, 99))],
        txn_id,
    };
    handle.send_storage_effect(drift).await;
    assert!(commit(&handle, txn_id).await);

    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    let account = account(&handle, set(), range_id()).await.unwrap();
    assert_eq!((account.charge, account.rebuild), (Charge::Active(2), None));
    assert_eq!(generation(&handle, 2).await, exact(3, 18));
    assert!(
        generation(&handle, 1).await.is_zero(),
        "the old generation row is gone"
    );
}

/// A run that stops after one page resumes from the durable cursor, and writes on both
/// sides of the cursor in between are counted exactly once.
#[tokio::test]
async fn resumes_from_cursor() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for seed in 0..10u8 {
        let key = format!("k{seed}");
        assert!(
            apply(&handle, write(set(), &key, seed + 10, 1))
                .await
                .unwrap()
        );
    }
    assert_eq!(run(&handle, 3, 1).await, RebuildOutcome::Paused);
    let paused = account(&handle, set(), range_id()).await.unwrap();
    let rebuild = paused.rebuild.clone().unwrap();
    assert!(matches!(rebuild.versions, Cursor::At(_)) && rebuild.generation == 2);

    assert!(
        apply(&handle, write(set(), "a", 1, 100)).await.unwrap(),
        "behind the cursor"
    );
    assert!(
        apply(&handle, write(set(), "z", 2, 1000)).await.unwrap(),
        "ahead of the cursor"
    );
    assert!(
        apply(&handle, purge(set(), "k0", &[10])).await.unwrap(),
        "behind the cursor"
    );

    assert_eq!(run(&handle, 3, 1).await, RebuildOutcome::Paused);
    assert_eq!(run(&handle, 3, 100).await, RebuildOutcome::Done);
    let account = account(&handle, set(), range_id()).await.unwrap();
    assert_eq!(
        account.charge,
        Charge::Active(2),
        "a resumed run begins no new generation"
    );
    assert_eq!(generation(&handle, 2).await, exact(11, 1109));
}

/// A writer that read the cursor before a page commit conflicts and retries against the
/// new cursor, so its delta lands exactly once.
#[tokio::test]
async fn page_conflicts_writer() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed) in [("b", 1), ("c", 2), ("d", 3), ("e", 4)] {
        assert!(apply(&handle, write(set(), key, seed, 1)).await.unwrap());
    }
    assert_eq!(run(&handle, 2, 1).await, RebuildOutcome::Paused);
    let writer = begin(&handle).await;
    stage(&handle, writer, &mut write(set(), "a", 5, 10))
        .await
        .unwrap();
    assert_eq!(run(&handle, 2, 1).await, RebuildOutcome::Paused);
    assert!(
        !commit(&handle, writer).await,
        "the page commit changed the cursor"
    );
    assert!(apply(&handle, write(set(), "a", 5, 10)).await.unwrap());

    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 2).await, exact(5, 14));
}

/// An empty range reaches the end with its first page; open uploads are walked too.
#[tokio::test]
async fn walks_empty_uploads() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert!(generation(&handle, 2).await.is_zero());

    for upload in 1..=3u8 {
        let mut change = InventoryChange::new(set(), "k");
        change.upload = Some((Ulid::from_bytes([upload; 16]), true));
        assert!(apply(&handle, InventoryUpdate::new(change)).await.unwrap());
    }
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 3).await.open_uploads, 3);
}

/// A rebuild keeps the epoch maximum of versions that are gone, so the durable maximum never
/// falls below an epoch the range has seen.
#[tokio::test]
async fn keeps_epoch_maximum() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    let mut change = write(set(), "a", 1, 1).change().clone();
    change.added[0].epoch = OriginalEpoch(7);
    assert!(apply(&handle, InventoryUpdate::new(change)).await.unwrap());
    assert!(apply(&handle, purge(set(), "a", &[1])).await.unwrap());
    assert_eq!(run(&handle, 2, 100).await, RebuildOutcome::Done);
    assert_eq!(generation(&handle, 2).await.max_epoch, OriginalEpoch(7));
}
