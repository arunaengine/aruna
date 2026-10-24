//! Tests kept copies against real storage: fill claims, attachments, cache admission with
//! eviction of the oldest entries, duplicates and entries larger than the cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{context, get, open};
use crate::driver::drive;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::distribution::on_use::{
    CacheTotals, FILL_CLAIM_TTL_MS, attachment_key, claim_key,
};
use aruna_core::structs::storage::format::StoredFormat;
use aruna_storage::storage::StorageHandle;
use std::collections::HashMap;
use std::time::SystemTime;

fn id(seed: u8) -> Ulid {
    Ulid::from_bytes([seed; 16])
}

fn location(seed: u8, size: u64) -> BackendLocation {
    BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/data".to_string(),
        storage_bucket: "hidden".to_string(),
        backend_path: format!("_jobs/{}/kept_{}", id(seed), id(seed)),
        ulid: id(seed),
        format: StoredFormat::default(),
        created_by: Default::default(),
        created_at: SystemTime::UNIX_EPOCH,
        staging: false,
        partial: false,
        blob_size: size,
        hashes: HashMap::new(),
    }
}

fn cached(seed: u8, size: u64, capacity: u64) -> KeptCopy {
    KeptCopy {
        kind: KeptKind::Cache,
        set_id: id(1),
        version_id: id(seed),
        blake3: [seed; 32],
        size,
        location: location(seed, size),
        claim_key: claim_key(KeptKind::Cache, id(1), id(seed), [seed; 32]),
        capacity,
        order: id(seed),
    }
}

async fn publish(handle: &StorageHandle, copy: KeptCopy) -> Published {
    drive(PublishKeptOperation::new(copy), &context(handle))
        .await
        .unwrap()
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

async fn totals(handle: &StorageHandle) -> CacheTotals {
    get(handle, CACHE_ENTRY_KEYSPACE, CACHE_TOTALS_KEY.to_vec())
        .await
        .map(|value| CacheTotals::from_bytes(&value).unwrap())
        .unwrap_or_default()
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

// An attachment row names the verified copy and publishing releases the claim.
#[tokio::test]
async fn publishes_attachment() {
    let (_dir, handle) = open();
    let copy = KeptCopy {
        kind: KeptKind::Attachment,
        ..cached(3, 10, 0)
    };
    let claim = copy.claim_key.clone();
    let claimed = FillClaim {
        claim_id: id(9),
        claimed_at_ms: 1,
    };
    drive(
        ClaimFillOperation::new(claim.clone(), claimed),
        &context(&handle),
    )
    .await
    .unwrap();

    let published = publish(&handle, copy.clone()).await;

    assert_eq!(published, Published::Kept { evicted: vec![] });
    let row = get(&handle, ATTACHMENT_KEYSPACE, attachment_key(id(1), id(3)))
        .await
        .unwrap();
    assert_eq!(
        Attachment::from_bytes(&row).unwrap().location,
        copy.location
    );
    assert_eq!(get(&handle, FILL_CLAIM_KEYSPACE, claim).await, None);
}

// A full cache evicts its oldest entries until the new one fits, and keeps its totals exact.
#[tokio::test]
async fn evicts_oldest_entries() {
    let (_dir, handle) = open();
    for seed in [1, 2, 3] {
        let published = publish(&handle, cached(seed, 4, 12)).await;
        assert_eq!(published, Published::Kept { evicted: vec![] });
    }
    assert_eq!(
        totals(&handle).await,
        CacheTotals {
            bytes: 12,
            entries: 3
        }
    );

    let published = publish(&handle, cached(4, 6, 12)).await;

    assert_eq!(
        published,
        Published::Kept {
            evicted: vec![location(1, 4), location(2, 4)]
        }
    );
    assert_eq!(
        totals(&handle).await,
        CacheTotals {
            bytes: 10,
            entries: 2
        }
    );
    assert_eq!(get(&handle, CACHE_ENTRY_KEYSPACE, vec![1; 32]).await, None);
    assert_eq!(
        get(&handle, CACHE_ORDER_KEYSPACE, id(2).to_bytes().to_vec()).await,
        None
    );
    assert!(
        get(&handle, CACHE_ENTRY_KEYSPACE, vec![3; 32])
            .await
            .is_some()
    );
}

// Bytes the cache keeps already, or more bytes than it can ever hold, are not kept again.
#[tokio::test]
async fn refuses_unkeepable() {
    let (_dir, handle) = open();
    publish(&handle, cached(1, 4, 12)).await;
    let again = KeptCopy {
        order: id(7),
        location: location(7, 4),
        ..cached(1, 4, 12)
    };
    assert_eq!(publish(&handle, again).await, Published::Unkept);
    assert_eq!(publish(&handle, cached(2, 13, 12)).await, Published::Unkept);
    assert_eq!(
        totals(&handle).await,
        CacheTotals {
            bytes: 4,
            entries: 1
        }
    );
}
