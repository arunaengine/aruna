//! Tests kept copies against real storage: fill claims, attachments, cache admission with
//! eviction of the oldest entries, duplicates and entries larger than the cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{context, open};
use crate::driver::drive;
use aruna_core::structs::storage::distribution::on_use::FILL_CLAIM_TTL_MS;
use aruna_storage::storage::StorageHandle;
use ulid::Ulid;

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

async fn claim(handle: &StorageHandle, claim_id: u8, now_ms: u64) -> Claimed {
    let claim = FillClaim {
        claim_id: id(claim_id),
        claimed_at_ms: now_ms,
    };
    drive(
        ClaimFillOperation::new(b"fill".to_vec(), claim),
        &context(handle),
    )
    .await
    .unwrap()
}

// A second fill of the same copy waits for the first, until the first claim goes stale.
#[tokio::test]
async fn claims_fill_once() {
    let (_dir, handle) = open();
    assert_eq!(claim(&handle, 1, 1_000).await, Claimed::Claimed);
    assert_eq!(
        claim(&handle, 1, 1_001).await,
        Claimed::Claimed,
        "the holder renews"
    );
    assert_eq!(claim(&handle, 2, 2_000).await, Claimed::Busy);
    assert_eq!(
        claim(&handle, 2, 1_001 + FILL_CLAIM_TTL_MS).await,
        Claimed::Claimed
    );
}
