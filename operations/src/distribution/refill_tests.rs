//! Tests the sample refill: a lost sample is rebuilt exactly as incremental writes left it,
//! across interrupted runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{apply, context, counters, install, open, range, write};
use crate::driver::drive;
use aruna_core::structs::storage::distribution::accounting::sample_prefix;
use aruna_storage::storage::StorageHandle;

fn set() -> Ulid {
    Ulid::from_bytes([6; 16])
}

fn range_id() -> Ulid {
    Ulid::from_bytes([1; 16])
}

async fn sample(handle: &StorageHandle) -> Vec<(Key, Value)> {
    let event = handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: KEY_SAMPLE_KEYSPACE.to_string(),
            prefix: Some(sample_prefix(set(), range_id()).into()),
            start: None,
            limit: usize::MAX,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
        panic!("unexpected storage event: {event:?}");
    };
    values
}

async fn refill(handle: &StorageHandle, max_pages: usize) -> RefillOutcome {
    let config = RefillConfig {
        set_id: set(),
        range_id: range_id(),
        page_rows: 7,
        page_bytes: 1 << 20,
        max_pages,
    };
    drive(RefillOperation::new(config), &context(handle))
        .await
        .expect("refill runs")
}

#[tokio::test]
async fn restores_lost_sample() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for seed in 0..60u8 {
        let key = format!("k/{seed:03}");
        assert!(apply(&handle, write(set(), &key, seed, 1)).await.unwrap());
    }
    let written = sample(&handle).await;
    assert!(!written.is_empty());
    let deletes = (written.iter())
        .map(|(key, _)| (KEY_SAMPLE_KEYSPACE.to_string(), key.clone()))
        .collect();
    let event = handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));

    assert_eq!(refill(&handle, 2).await, RefillOutcome::Paused);
    assert_eq!(refill(&handle, 100).await, RefillOutcome::Done);
    assert_eq!(
        sample(&handle).await,
        written,
        "a refill places keys as writes did"
    );
    let count = counters(&handle, sample_count_key(set(), range_id())).await;
    assert_eq!(count.sampled, 60, "the refill corrects the count row");
}

/// A sample larger than one read is trimmed over repeated pages, and only a complete read
/// corrects the count.
#[tokio::test]
async fn trims_crowded_sample() {
    use crate::distribution::test_store::crowd_sample;

    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    crowd_sample(&handle, set(), 1, 1200).await;
    assert_eq!(refill(&handle, 100).await, RefillOutcome::Done);
    assert_eq!(sample(&handle).await.len(), usize::from(SAMPLE_SIZE));
    let count = counters(&handle, sample_count_key(set(), range_id())).await;
    assert_eq!(count.sampled, i128::from(SAMPLE_SIZE));
}
