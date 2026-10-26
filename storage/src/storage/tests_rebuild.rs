//! Tests usage rebuilds: the snapshot and rebuild rows split commits exactly, finish replaces
//! rows, and a rebuild resumes after a restart with the rows it began with.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{commit_transaction, start_write_transaction};
use super::{FjallStorage, StorageHandle};
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::structs::storage::usage::{
    BucketCounters, BucketDelta, RebuildProgress, ShadowCounters, USAGE_COMPLETE_KEY,
    USAGE_PROGRESS_KEY, USAGE_REBUILD_KEY, UsageCounters, UsageDelta, usage_shadow_key,
};
use aruna_core::types::TxnId;
use byteview::ByteView;
use tempfile::tempdir;

const SPACE: &str = "usage_stats";
const GROUP: &[u8] = b"group/a";
const BUCKET: &[u8] = b"bucket/a";
/// A keyspace a usage rebuild reads.
const SOURCE: &str = S3_BUCKET_KEYSPACE;

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
    // The finished rows count as complete, also while a later rebuild runs.
    assert!(read(&handle, USAGE_COMPLETE_KEY, None).await.is_some());
    let again = begin(&handle).await;
    handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id: again })
        .await;
    assert!(read(&handle, USAGE_COMPLETE_KEY, None).await.is_some());
    assert!(matches!(
        finish(&handle).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    assert!(read(&handle, USAGE_COMPLETE_KEY, None).await.is_some());
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

async fn write_row(handle: &StorageHandle, key: &[u8], value: &[u8], txn_id: Option<TxnId>) {
    let write = StorageEffect::Write {
        key_space: SOURCE.to_string(),
        key: ByteView::from(key),
        value: ByteView::from(value),
        txn_id,
    };
    let event = handle.send_storage_effect(write).await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

async fn source_page(handle: &StorageHandle, view: TxnId) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut start = None;
    loop {
        // One row per page, so pages end inside the rows the view lays over the snapshot.
        let iter = StorageEffect::Iter {
            key_space: SOURCE.to_string(),
            prefix: None,
            start: start.take(),
            limit: 1,
            txn_id: Some(view),
        };
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = handle.send_storage_effect(iter).await
        else {
            panic!("the view did not iterate");
        };
        rows.extend(values.iter().map(|(k, v)| (k.to_vec(), v.to_vec())));
        match next_start_after {
            Some(after) => start = Some(IterStart::After(after)),
            None => return rows,
        }
    }
}

fn progress(value: Option<ByteView>) -> RebuildProgress {
    RebuildProgress::from_bytes(&value.expect("progress row")).unwrap()
}

/// Rows changed after the begin keep their earlier value; after a restart the rebuild resumes
/// with its rows and its view still reads the rows as they were when it began.
#[tokio::test]
async fn resume_reads_start() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let handle = FjallStorage::open(&path).unwrap();
    write_row(&handle, b"a", b"a1", None).await;
    write_row(&handle, b"b", b"b1", None).await;
    let view = begin(&handle).await;
    let generation = progress(read(&handle, USAGE_PROGRESS_KEY, None).await).generation;
    commit_delta(&handle, GROUP, objects(2)).await;
    write_row(&handle, b"a", b"a2", None).await;
    write_row(&handle, b"a", b"a3", None).await;
    let txn_id = start_write_transaction(&handle).await;
    write_row(&handle, b"c", b"c1", Some(txn_id)).await;
    let delete = StorageEffect::Delete {
        key_space: SOURCE.to_string(),
        key: ByteView::from(&b"b"[..]),
        txn_id: Some(txn_id),
    };
    handle.send_storage_effect(delete).await;
    commit_transaction(&handle, txn_id).await;
    let start = vec![
        (b"a".to_vec(), b"a1".to_vec()),
        (b"b".to_vec(), b"b1".to_vec()),
    ];
    assert_eq!(source_page(&handle, view).await, start);
    commit_transaction(&handle, view).await;
    handle.close().await;

    let handle = FjallStorage::open(&path).unwrap();
    // Storage shadows deltas again before any begin, as the rebuild still runs.
    commit_delta(&handle, GROUP, objects(1)).await;
    write_row(&handle, b"d", b"d1", None).await;
    let view = begin(&handle).await;
    let resumed = progress(read(&handle, USAGE_PROGRESS_KEY, None).await);
    assert_eq!(resumed.generation, generation, "the same rebuild resumes");
    assert_eq!(
        shadow(read(&handle, &usage_shadow_key(GROUP), None).await),
        Some(ShadowCounters::Usage(objects(3))),
        "rebuild rows survive the restart and keep collecting deltas"
    );
    assert_eq!(source_page(&handle, view).await, start);
    let reads = vec![
        (SOURCE.to_string(), ByteView::from(&b"b"[..])),
        (SOURCE.to_string(), ByteView::from(&b"c"[..])),
    ];
    let event = handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: Some(view),
        })
        .await;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        panic!("the view did not read: {event:?}");
    };
    let values: Vec<_> = values
        .into_iter()
        .map(|(_, v)| v.map(|v| v.to_vec()))
        .collect();
    assert_eq!(values, vec![Some(b"b1".to_vec()), None]);
    commit_transaction(&handle, view).await;
    assert!(matches!(
        finish(&handle).await,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    assert!(read(&handle, USAGE_PROGRESS_KEY, None).await.is_none());

    // The next rebuild starts over and reads the rows as they are now.
    let view = begin(&handle).await;
    let fresh = progress(read(&handle, USAGE_PROGRESS_KEY, None).await);
    assert_ne!(fresh.generation, generation);
    let now = vec![
        (b"a".to_vec(), b"a3".to_vec()),
        (b"c".to_vec(), b"c1".to_vec()),
        (b"d".to_vec(), b"d1".to_vec()),
    ];
    assert_eq!(source_page(&handle, view).await, now);
}
