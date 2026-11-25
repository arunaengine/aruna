//! Tests the hot cache: hits, transaction bypass, drops before write replies and its bound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use super::handle::StorageHandle;
use super::hot_cache::{CAPACITY, CacheWriter, HotCache, Row, cached_slot};
use super::tests::{commit_transaction, poll_queued, start_write_transaction, worker};
use super::worker::{FjallStorage, SlowQueueAggregator};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, GROUP_KEYSPACE, NODE_INFO_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::storage::distribution::accounting::RangeCounters;
use aruna_core::structs::storage::usage::{BucketDelta, UsageDelta};
use byteview::ByteView;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

/// A keyspace the cache never holds.
const OTHER: &str = "uncached";
const TOKEN: &[u8] = b"token";

fn open() -> (TempDir, StorageHandle) {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open_test(dir.path().to_str().unwrap()).unwrap();
    (dir, handle)
}

fn cache(handle: &StorageHandle) -> &HotCache {
    handle.cache.as_deref().expect("the handle keeps a cache")
}

fn row(key_space: &str, key: &[u8]) -> Row {
    (cached_slot(key_space).unwrap(), ByteView::from(key))
}

fn bytes(value: &[u8]) -> Option<ByteView> {
    Some(ByteView::from(value))
}

/// Puts a value the database does not hold, so a read that returns it came from the cache.
fn plant(handle: &StorageHandle, key_space: &str, key: &[u8], value: &[u8]) {
    let cache = cache(handle);
    cache.fill(cache.generation(), [(row(key_space, key), bytes(value))]);
}

fn write_effect(key_space: &str, key: &[u8], value: &[u8], txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: key_space.to_string(),
        key: ByteView::from(key),
        value: ByteView::from(value),
        txn_id,
    }
}

async fn write(handle: &StorageHandle, key_space: &str, value: &[u8], txn_id: Option<Ulid>) {
    let event = handle
        .send_storage_effect(write_effect(key_space, TOKEN, value, txn_id))
        .await;
    assert!(
        matches!(event, Event::Storage(StorageEvent::WriteResult { .. })),
        "unexpected storage event: {event:?}"
    );
}

async fn read(handle: &StorageHandle, key_space: &str, txn_id: Option<Ulid>) -> Option<ByteView> {
    let effect = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: ByteView::from(TOKEN),
        txn_id,
    };
    match handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected storage event: {other:?}"),
    }
}

async fn batch(
    handle: &StorageHandle,
    reads: &[(&str, &[u8])],
    txn_id: Option<Ulid>,
) -> Vec<Option<ByteView>> {
    let reads = reads
        .iter()
        .map(|(key_space, key)| (key_space.to_string(), ByteView::from(*key)))
        .collect();
    match handle
        .send_storage_effect(StorageEffect::BatchRead { reads, txn_id })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            values.into_iter().map(|(_, value)| value).collect()
        }
        other => panic!("unexpected storage event: {other:?}"),
    }
}

#[tokio::test]
async fn reads_hit_cache() {
    let (_dir, handle) = open();
    write(&handle, AUTH_KEYSPACE, b"stored", None).await;
    write(&handle, OTHER, b"other", None).await;
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"stored"));
    let cached = cache(&handle).get(&row(AUTH_KEYSPACE, TOKEN));
    assert_eq!(cached, Some(bytes(b"stored")), "a miss fills the cache");

    plant(&handle, AUTH_KEYSPACE, TOKEN, b"planted");
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"planted"));

    plant(&handle, S3_BUCKET_KEYSPACE, b"bucket", b"bucket");
    let reads: [(&str, &[u8]); 4] = [
        (OTHER, TOKEN),
        (AUTH_KEYSPACE, TOKEN),
        (S3_BUCKET_KEYSPACE, b"missing"),
        (S3_BUCKET_KEYSPACE, b"bucket"),
    ];
    let expected = vec![bytes(b"other"), bytes(b"planted"), None, bytes(b"bucket")];
    assert_eq!(batch(&handle, &reads, None).await, expected);
    let absent = cache(&handle).get(&row(S3_BUCKET_KEYSPACE, b"missing"));
    assert_eq!(absent, Some(None), "an absent row is cached as absent");
    assert_eq!(batch(&handle, &reads, None).await, expected);
}

#[tokio::test]
async fn transactions_bypass_cache() {
    let (_dir, handle) = open();
    write(&handle, AUTH_KEYSPACE, b"stored", None).await;
    plant(&handle, AUTH_KEYSPACE, TOKEN, b"planted");
    let txn_id = start_write_transaction(&handle).await;
    assert_eq!(
        read(&handle, AUTH_KEYSPACE, Some(txn_id)).await,
        bytes(b"stored")
    );

    write(&handle, AUTH_KEYSPACE, b"pending", Some(txn_id)).await;
    assert_eq!(
        read(&handle, AUTH_KEYSPACE, Some(txn_id)).await,
        bytes(b"pending")
    );
    let reads: [(&str, &[u8]); 2] = [(AUTH_KEYSPACE, TOKEN), (OTHER, TOKEN)];
    let values = batch(&handle, &reads, Some(txn_id)).await;
    assert_eq!(values, vec![bytes(b"pending"), None]);
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"planted"));
    commit_transaction(&handle, txn_id).await;
}

#[tokio::test]
async fn writes_invalidate_cache() {
    let (_dir, handle) = open();
    let delete = |key_space: &str| (key_space.to_string(), ByteView::from(TOKEN));
    let effects = [
        (
            write_effect(AUTH_KEYSPACE, TOKEN, b"one", None),
            bytes(b"one"),
        ),
        (
            StorageEffect::BatchWrite {
                writes: vec![
                    (
                        OTHER.to_string(),
                        ByteView::from(TOKEN),
                        ByteView::from("x"),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        ByteView::from(TOKEN),
                        "two".into(),
                    ),
                ],
                txn_id: None,
            },
            bytes(b"two"),
        ),
        (
            StorageEffect::Delete {
                key_space: AUTH_KEYSPACE.to_string(),
                key: ByteView::from(TOKEN),
                txn_id: None,
            },
            None,
        ),
        (
            write_effect(AUTH_KEYSPACE, TOKEN, b"three", None),
            bytes(b"three"),
        ),
        (
            StorageEffect::BatchDelete {
                deletes: vec![delete(OTHER), delete(AUTH_KEYSPACE)],
                txn_id: None,
            },
            None,
        ),
    ];
    for (effect, expected) in effects {
        plant(&handle, AUTH_KEYSPACE, TOKEN, b"planted");
        let event = handle.send_storage_effect(effect).await;
        assert!(
            !matches!(event, Event::Storage(StorageEvent::Error { .. })),
            "unexpected storage event: {event:?}"
        );
        let cached = cache(&handle).get(&row(AUTH_KEYSPACE, TOKEN));
        assert_eq!(
            cached, None,
            "the write replies after its rows left the cache"
        );
        assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, expected);
    }
}

#[tokio::test]
async fn group_commit_invalidates() {
    let dir = tempdir().unwrap();
    let (mut handle, receivers) = StorageHandle::new();
    handle.cache = Some(Arc::new(HotCache::default()));
    let mut storage = worker(&dir, &handle);
    plant(&handle, AUTH_KEYSPACE, TOKEN, b"planted");
    plant(&handle, S3_BUCKET_KEYSPACE, TOKEN, b"planted");
    let generation = cache(&handle).generation();

    let effect = write_effect(AUTH_KEYSPACE, TOKEN, b"grouped", None);
    let mut written = Box::pin(handle.send_storage_effect(effect));
    poll_queued(&mut written).await;
    let effect = StorageEffect::BatchDelete {
        deletes: vec![
            (S3_BUCKET_KEYSPACE.to_string(), ByteView::from(TOKEN)),
            (OTHER.to_string(), ByteView::from(TOKEN)),
        ],
        txn_id: None,
    };
    let mut deleted = Box::pin(handle.send_storage_effect(effect));
    poll_queued(&mut deleted).await;
    let mut group = vec![
        receivers.foreground.recv().unwrap(),
        receivers.foreground.recv().unwrap(),
    ];

    storage.flush_write_group(&mut group, &mut SlowQueueAggregator::default());

    assert_eq!(cache(&handle).len(), 0, "both rows left before the replies");
    assert!(cache(&handle).generation() > generation);
    assert!(matches!(
        written.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    assert!(matches!(
        deleted.await,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));
}

#[tokio::test]
async fn abort_keeps_cache() {
    let (_dir, handle) = open();
    write(&handle, AUTH_KEYSPACE, b"stored", None).await;
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"stored"));
    let txn_id = start_write_transaction(&handle).await;
    write(&handle, AUTH_KEYSPACE, b"aborted", Some(txn_id)).await;

    let event = handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::TransactionAborted { .. })
    ));
    let cached = cache(&handle).get(&row(AUTH_KEYSPACE, TOKEN));
    assert_eq!(cached, Some(bytes(b"stored")));
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"stored"));
}

#[tokio::test]
async fn commit_invalidates_rows() {
    let (_dir, handle) = open();
    write(&handle, AUTH_KEYSPACE, b"stored", None).await;
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"stored"));
    let txn_id = start_write_transaction(&handle).await;
    write(&handle, AUTH_KEYSPACE, b"committed", Some(txn_id)).await;
    assert_eq!(read(&handle, AUTH_KEYSPACE, None).await, bytes(b"stored"));

    commit_transaction(&handle, txn_id).await;

    assert_eq!(cache(&handle).get(&row(AUTH_KEYSPACE, TOKEN)), None);
    assert_eq!(
        read(&handle, AUTH_KEYSPACE, None).await,
        bytes(b"committed")
    );
}

#[test]
fn capacity_bounds_entries() {
    let cache = HotCache::default();
    let key = |index: usize| (0, ByteView::from(index.to_be_bytes().as_slice()));
    for index in 0..=CAPACITY * 2 {
        cache.fill(cache.generation(), [(key(index), None)]);
        assert!(cache.len() <= CAPACITY);
    }
    assert_eq!(cache.get(&key(CAPACITY * 2)), Some(None));
}

#[test]
fn stale_fill_skipped() {
    let cache = HotCache::default();
    let rows = [(0, ByteView::from(TOKEN))];
    let before = cache.generation();
    cache.begin(&rows);
    cache.fill(cache.generation(), [(rows[0].clone(), None)]);
    assert_eq!(cache.get(&rows[0]), None, "no fill while a commit runs");
    cache.finish(&rows);
    cache.fill(before, [(rows[0].clone(), bytes(b"old"))]);
    assert_eq!(
        cache.get(&rows[0]),
        None,
        "a read from before the commit is dropped"
    );

    cache.fill(cache.generation(), [(rows[0].clone(), None)]);
    assert_eq!(cache.get(&rows[0]), Some(None));
    cache.begin(&rows);
    assert_eq!(cache.lookup(&[Some(rows[0].clone())]), vec![None]);
    cache.finish(&rows);
    assert_eq!(cache.get(&rows[0]), None);
}

#[tokio::test]
async fn closed_bypasses_cache() {
    // Once the worker is gone, cached reads report its failure like cold reads do.
    let (mut handle, receivers) = StorageHandle::new();
    handle.cache = Some(Arc::new(HotCache::default()));
    plant(&handle, AUTH_KEYSPACE, TOKEN, b"planted");
    drop(receivers);
    let closed = |event: Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::ChannelClosed
            })
        )
    };
    let cold = StorageEffect::Read {
        key_space: OTHER.to_string(),
        key: ByteView::from(TOKEN),
        txn_id: None,
    };
    assert!(closed(handle.send_storage_effect(cold).await));

    let point = StorageEffect::Read {
        key_space: AUTH_KEYSPACE.to_string(),
        key: ByteView::from(TOKEN),
        txn_id: None,
    };
    assert!(closed(handle.send_storage_effect(point).await));
    let reads = vec![(AUTH_KEYSPACE.to_string(), ByteView::from(TOKEN))];
    let batch = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    assert!(closed(handle.send_storage_effect(batch).await));
}

#[test]
fn counters_drop_rows() {
    // Counter deltas on a cached keyspace drop its rows, as every other write does.
    let writer = CacheWriter::new(Some(Arc::new(HotCache::default())));
    let usage = StorageEffect::AddUsage {
        key_space: AUTH_KEYSPACE.to_string(),
        deltas: vec![(ByteView::from(TOKEN), UsageDelta::default())],
        buckets: vec![(ByteView::from("bucket"), BucketDelta::default())],
        txn_id: Ulid::default(),
    };
    let expected = vec![row(AUTH_KEYSPACE, TOKEN), row(AUTH_KEYSPACE, b"bucket")];
    assert_eq!(writer.rows(&usage), expected);
    let counters = |key_space: &str| StorageEffect::AddCounters {
        key_space: key_space.to_string(),
        deltas: vec![(ByteView::from(TOKEN), RangeCounters::default())],
        txn_id: Ulid::default(),
    };
    assert_eq!(
        writer.rows(&counters(S3_BUCKET_KEYSPACE)),
        vec![row(S3_BUCKET_KEYSPACE, TOKEN)]
    );
    assert!(writer.rows(&counters(OTHER)).is_empty());
}

#[tokio::test]
async fn group_keyspace_cached() {
    let (_dir, handle) = open();
    for space in [GROUP_KEYSPACE, NODE_INFO_KEYSPACE] {
        write(&handle, space, b"stored", None).await;
        assert_eq!(read(&handle, space, None).await, bytes(b"stored"));
        assert_eq!(
            cache(&handle).get(&row(space, TOKEN)),
            Some(bytes(b"stored"))
        );

        plant(&handle, space, TOKEN, b"planted");
        assert_eq!(read(&handle, space, None).await, bytes(b"planted"));

        write(&handle, space, b"direct", None).await;
        assert_eq!(cache(&handle).get(&row(space, TOKEN)), None);
        assert_eq!(read(&handle, space, None).await, bytes(b"direct"));

        let txn_id = start_write_transaction(&handle).await;
        write(&handle, space, b"txn", Some(txn_id)).await;
        assert_eq!(read(&handle, space, None).await, bytes(b"direct"));
        commit_transaction(&handle, txn_id).await;
        assert_eq!(cache(&handle).get(&row(space, TOKEN)), None);
        assert_eq!(read(&handle, space, None).await, bytes(b"txn"));

        let delete = StorageEffect::Delete {
            key_space: space.to_string(),
            key: ByteView::from(TOKEN),
            txn_id: None,
        };
        handle.send_storage_effect(delete).await;
        assert_eq!(cache(&handle).get(&row(space, TOKEN)), None);
        assert_eq!(read(&handle, space, None).await, None);
    }
}
