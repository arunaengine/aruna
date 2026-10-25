//! Tests range counter deltas: merged at commit, never conflicting, dropped with their txn.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{commit_transaction, start_write_transaction};
use super::{FjallStorage, StorageHandle};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use byteview::ByteView;
use tempfile::tempdir;
use ulid::Ulid;

const SPACE: &str = "range_counters";
const ROW: &[u8] = b"row";

fn delta(objects: i128, epoch: u64) -> RangeCounters {
    RangeCounters {
        objects,
        max_epoch: OriginalEpoch(epoch),
        ..RangeCounters::default()
    }
}

async fn add(handle: &StorageHandle, txn_id: Ulid, counters: RangeCounters) {
    let event = handle
        .send_storage_effect(StorageEffect::AddCounters {
            key_space: SPACE.to_string(),
            deltas: vec![(ByteView::from(ROW), counters)],
            txn_id,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

async fn stored(handle: &StorageHandle) -> RangeCounters {
    let event = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: SPACE.to_string(),
            key: ByteView::from(ROW),
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event: {event:?}");
    };
    value.map_or_else(RangeCounters::default, |value| {
        RangeCounters::from_bytes(&value).unwrap()
    })
}

/// Two writers of one counter row both commit, in either order, and the row holds both.
#[tokio::test]
async fn concurrent_deltas_merge() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let first = start_write_transaction(&handle).await;
    let second = start_write_transaction(&handle).await;
    add(&handle, first, delta(3, 1)).await;
    add(&handle, first, delta(1, 0)).await;
    add(&handle, second, delta(-5, 4)).await;
    commit_transaction(&handle, second).await;
    commit_transaction(&handle, first).await;
    assert_eq!(
        stored(&handle).await,
        delta(-1, 4),
        "signed sum and epoch maximum"
    );
}

/// A transaction that conflicts or aborts adds none of its deltas.
#[tokio::test]
async fn failure_adds_nothing() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let loser = start_write_transaction(&handle).await;
    let read = StorageEffect::Read {
        key_space: SPACE.to_string(),
        key: ByteView::from(&b"guard"[..]),
        txn_id: Some(loser),
    };
    handle.send_storage_effect(read).await;
    add(&handle, loser, delta(7, 0)).await;
    let winner = start_write_transaction(&handle).await;
    let guard = StorageEffect::Write {
        key_space: SPACE.to_string(),
        key: ByteView::from(&b"guard"[..]),
        value: ByteView::from(&b"1"[..]),
        txn_id: Some(winner),
    };
    handle.send_storage_effect(guard).await;
    add(&handle, winner, delta(2, 0)).await;
    commit_transaction(&handle, winner).await;
    let event = handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id: loser })
        .await;
    assert_eq!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict
        })
    );
    let aborted = start_write_transaction(&handle).await;
    add(&handle, aborted, delta(9, 0)).await;
    handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id: aborted })
        .await;
    assert_eq!(stored(&handle).await, delta(2, 0));
}
