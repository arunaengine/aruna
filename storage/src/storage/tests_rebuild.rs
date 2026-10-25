//! Tests usage rebuilds: the snapshot and rebuild rows split commits exactly, finish replaces rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{commit_transaction, start_write_transaction};
use super::{FjallStorage, StorageHandle};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::structs::storage::usage::{
    BucketCounters, BucketDelta, ShadowCounters, USAGE_REBUILD_KEY, UsageCounters, UsageDelta,
    usage_shadow_key,
};
use aruna_core::types::TxnId;
use byteview::ByteView;
use tempfile::tempdir;

const SPACE: &str = "usage_stats";
const GROUP: &[u8] = b"group/a";
const BUCKET: &[u8] = b"bucket/a";

fn objects(objects: i128) -> UsageDelta {
    UsageDelta {
        objects,
        ..UsageDelta::default()
    }
}

async fn commit_delta(handle: &StorageHandle, key: &[u8], delta: UsageDelta) {
    let txn_id = start_write_transaction(handle).await;
    let event = handle
        .send_storage_effect(StorageEffect::AddUsage {
            key_space: SPACE.to_string(),
            deltas: vec![(ByteView::from(key), delta)],
            buckets: vec![(ByteView::from(BUCKET), BucketDelta::default())],
            txn_id,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    commit_transaction(handle, txn_id).await;
}

async fn read(handle: &StorageHandle, key: &[u8], txn_id: Option<TxnId>) -> Option<ByteView> {
    let event = handle
        .send_storage_effect(StorageEffect::Read {
            key_space: SPACE.to_string(),
            key: ByteView::from(key),
            txn_id,
        })
        .await;
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        panic!("unexpected storage event: {event:?}");
    };
    value
}

async fn begin(handle: &StorageHandle) -> TxnId {
    let effect = StorageEffect::BeginUsageRebuild {
        key_space: SPACE.to_string(),
    };
    match handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("rebuild did not begin: {other:?}"),
    }
}

async fn finish(handle: &StorageHandle) -> Event {
    let effect = StorageEffect::FinishUsageRebuild {
        key_space: SPACE.to_string(),
    };
    handle.send_storage_effect(effect).await
}

fn shadow(value: Option<ByteView>) -> Option<ShadowCounters> {
    value.map(|value| ShadowCounters::from_bytes(&value).unwrap())
}

fn counters(value: Option<ByteView>) -> Option<UsageCounters> {
    value.map(|value| UsageCounters::from_bytes(&value).unwrap())
}

/// A delta committed before the begin is only in the snapshot; one committed after it lands in
/// the live row and the rebuild row, and a removal walked later never clamps there.
#[tokio::test]
async fn begin_splits_commits() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    commit_delta(&handle, GROUP, objects(4)).await;
    let snapshot = begin(&handle).await;
    commit_delta(&handle, GROUP, objects(-1)).await;

    assert!(read(&handle, USAGE_REBUILD_KEY, None).await.is_some());
    assert_eq!(
        counters(read(&handle, GROUP, Some(snapshot)).await),
        Some(UsageCounters {
            objects: 4,
            ..UsageCounters::default()
        }),
        "the snapshot holds only earlier commits"
    );
    assert_eq!(
        shadow(read(&handle, &usage_shadow_key(GROUP), None).await),
        Some(ShadowCounters::Usage(objects(-1))),
        "only later commits reach the rebuild row, signed"
    );
    assert_eq!(
        shadow(read(&handle, &usage_shadow_key(BUCKET), None).await),
        Some(ShadowCounters::Bucket(BucketDelta::default()))
    );
    let marker = shadow(read(&handle, &usage_shadow_key(USAGE_REBUILD_KEY), None).await);
    assert_eq!(marker, None, "the marker is never copied");
}

/// Finish turns rebuild rows into live rows, deletes live rows without one and the marker,
/// and later deltas reach only the live rows.
#[tokio::test]
async fn finish_replaces_rows() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    commit_delta(&handle, b"group/stale", objects(9)).await;
    commit_delta(&handle, GROUP, objects(2)).await;
    let snapshot = begin(&handle).await;
    commit_delta(&handle, GROUP, objects(-1)).await;
    // The walk adds what it counted in the snapshot as a page delta onto the rebuild row.
    let page = start_write_transaction(&handle).await;
    handle
        .send_storage_effect(StorageEffect::AddUsage {
            key_space: SPACE.to_string(),
            deltas: vec![(ByteView::from(usage_shadow_key(GROUP)), objects(2))],
            buckets: vec![(
                ByteView::from(usage_shadow_key(BUCKET)),
                BucketDelta {
                    versions: 3,
                    ..BucketDelta::default()
                },
            )],
            txn_id: page,
        })
        .await;
    commit_transaction(&handle, page).await;
    handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id: snapshot })
        .await;

    assert!(matches!(
        finish(&handle).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let one = UsageCounters {
        objects: 1,
        ..UsageCounters::default()
    };
    assert_eq!(counters(read(&handle, GROUP, None).await), Some(one));
    let bucket = read(&handle, BUCKET, None).await.unwrap();
    assert_eq!(BucketCounters::from_bytes(&bucket).unwrap().versions, 3);
    assert!(read(&handle, b"group/stale", None).await.is_none());
    assert!(read(&handle, USAGE_REBUILD_KEY, None).await.is_none());
    assert!(
        read(&handle, &usage_shadow_key(GROUP), None)
            .await
            .is_none()
    );

    commit_delta(&handle, GROUP, objects(5)).await;
    assert!(
        read(&handle, &usage_shadow_key(GROUP), None)
            .await
            .is_none()
    );
    let six = UsageCounters {
        objects: 6,
        ..UsageCounters::default()
    };
    assert_eq!(counters(read(&handle, GROUP, None).await), Some(six));
}

/// A begin clears rebuild rows a crashed rebuild left; a finish without a begin in this
/// running storage, as after a restart, is refused and leaves the live rows alone.
#[tokio::test]
async fn begin_clears_leftovers() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    commit_delta(&handle, GROUP, objects(3)).await;
    let leftover = ShadowCounters::Usage(objects(1)).to_bytes().unwrap();
    for (key, value) in [
        (usage_shadow_key(GROUP), leftover),
        (USAGE_REBUILD_KEY.to_vec(), Vec::new()),
    ] {
        let write = StorageEffect::Write {
            key_space: SPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        };
        handle.send_storage_effect(write).await;
    }
    assert!(matches!(
        finish(&handle).await,
        Event::Storage(StorageEvent::Error { .. })
    ));
    assert!(read(&handle, GROUP, None).await.is_some());

    begin(&handle).await;
    assert!(
        read(&handle, &usage_shadow_key(GROUP), None)
            .await
            .is_none()
    );
    assert!(read(&handle, USAGE_REBUILD_KEY, None).await.is_some());
}
