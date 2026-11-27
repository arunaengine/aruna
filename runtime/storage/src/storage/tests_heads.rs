//! Tests that every change of a real head removes its watch key in the same commit and tells
//! the head observer, in Fjall storage and in memory storage alike.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
use aruna_core::structs::storage::blob::BlobHeadKey;
use aruna_core::structs::storage::vfs::{HeadChangeObserver, VfsWatchKey};
use byteview::ByteView;
use tempfile::tempdir;
use ulid::Ulid;

use super::handle::StorageHandle;
use super::memory::MemoryStorage;
use super::tests::{poll_queued, worker};
use super::worker::{FjallStorage, SlowQueueAggregator};

const BUCKET: &str = "bkt";
const OTHER: &str = "other";

/// Records each head change it is told about as `bucket/key`.
#[derive(Default)]
struct Seen(Mutex<Vec<String>>);

impl HeadChangeObserver for Seen {
    fn on_head_changed(&self, bucket: &str, key: &str) {
        self.0.lock().unwrap().push(format!("{bucket}/{key}"));
    }
}

impl Seen {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// Runs `scenario` on Fjall storage and on memory storage, each with a fresh observer.
async fn both(scenario: impl AsyncFn(&StorageHandle, &Seen)) {
    let dir = tempdir().unwrap();
    let fjall = FjallStorage::open_test(dir.path().to_str().unwrap()).unwrap();
    for handle in [fjall, MemoryStorage::open()] {
        let seen = Arc::new(Seen::default());
        assert!(handle.install_head_observer(seen.clone()));
        assert!(!handle.install_head_observer(Arc::new(Seen::default())));
        scenario(&handle, &*seen).await;
    }
}

fn head(key: &str) -> ByteView {
    ByteView::from(BlobHeadKey::new(BUCKET, key).to_bytes().unwrap())
}

fn watch(key: &str) -> ByteView {
    ByteView::from(VfsWatchKey::new(BUCKET, key).to_bytes().unwrap())
}

fn write(key: ByteView, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key,
        value: ByteView::from(&b"value"[..]),
        txn_id,
    }
}

fn delete(key: ByteView, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Delete {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key,
        txn_id,
    }
}

fn read(key: ByteView, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key,
        txn_id,
    }
}

/// Reads or changes one row outside the head keyspace.
fn other(txn_id: Option<Ulid>, change: bool) -> StorageEffect {
    let key = ByteView::from(&b"x"[..]);
    let key_space = OTHER.to_string();
    match change {
        true => StorageEffect::Write {
            key_space,
            key,
            value: ByteView::from(&b"changed"[..]),
            txn_id,
        },
        false => StorageEffect::Read {
            key_space,
            key,
            txn_id,
        },
    }
}

async fn send(handle: &StorageHandle, effect: StorageEffect) -> StorageEvent {
    let Event::Storage(event) = handle.send_storage_effect(effect).await else {
        panic!("storage event expected");
    };
    event
}

async fn stored(handle: &StorageHandle, key: ByteView) -> Option<ByteView> {
    match send(handle, read(key, None)).await {
        StorageEvent::ReadResult { value, .. } => value,
        other => panic!("read failed: {other:?}"),
    }
}

async fn begin(handle: &StorageHandle) -> Ulid {
    match send(handle, StorageEffect::StartTransaction { read: false }).await {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => panic!("transaction did not start: {other:?}"),
    }
}

async fn commit(handle: &StorageHandle, txn_id: Ulid) -> StorageEvent {
    send(handle, StorageEffect::CommitTransaction { txn_id }).await
}

fn committed(event: &StorageEvent) -> bool {
    matches!(event, StorageEvent::TransactionCommitted { .. })
}

fn conflicted(event: &StorageEvent) -> bool {
    matches!(
        event,
        StorageEvent::Error {
            error: StorageError::TransactionConflict
        }
    )
}

async fn immediate(handle: &StorageHandle, seen: &Seen) {
    for key in ["a", "b", "c", "d"] {
        send(handle, write(watch(key), None)).await;
    }
    assert!(seen.take().is_empty(), "a watch key is no head");
    assert!(
        stored(handle, watch("a")).await.is_some(),
        "no self-removal"
    );

    send(handle, write(head("a"), None)).await;
    let rows = vec![
        (
            BLOB_HEAD_KEYSPACE.to_string(),
            head("b"),
            ByteView::from(&b"v"[..]),
        ),
        (OTHER.to_string(), head("c"), ByteView::from(&b"v"[..])),
    ];
    send(
        handle,
        StorageEffect::BatchWrite {
            writes: rows,
            txn_id: None,
        },
    )
    .await;
    send(handle, delete(head("c"), None)).await;
    let deletes = vec![(BLOB_HEAD_KEYSPACE.to_string(), head("d"))];
    send(
        handle,
        StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        },
    )
    .await;

    assert_eq!(seen.take(), ["bkt/a", "bkt/b", "bkt/c", "bkt/d"]);
    for key in ["a", "b", "c", "d"] {
        assert_eq!(
            stored(handle, watch(key)).await,
            None,
            "watch of {key} removed"
        );
    }
    send(handle, write(watch("e"), None)).await;
    send(handle, delete(watch("e"), None)).await;
    assert!(seen.take().is_empty());
}

#[tokio::test]
async fn immediate_changes_unwatch() {
    both(immediate).await;
}

#[tokio::test]
async fn grouped_writes_unwatch() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let seen = Arc::new(Seen::default());
    assert!(handle.install_head_observer(seen.clone()));
    for key in ["a", "b"] {
        storage.process_effect(write(watch(key), None));
    }
    let mut first = Box::pin(handle.send_storage_effect(write(head("a"), None)));
    poll_queued(&mut first).await;
    let mut second = Box::pin(handle.send_storage_effect(delete(head("b"), None)));
    poll_queued(&mut second).await;
    let mut group = vec![
        receivers.foreground.recv().unwrap(),
        receivers.foreground.recv().unwrap(),
    ];

    storage.flush_write_group(&mut group, &mut SlowQueueAggregator::default());

    assert!(matches!(
        first.await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    assert!(matches!(
        second.await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));
    assert_eq!(seen.take(), ["bkt/a", "bkt/b"]);
    for key in ["a", "b"] {
        let event = storage.process_effect(read(watch(key), None));
        assert!(matches!(
            event,
            StorageEvent::ReadResult { value: None, .. }
        ));
    }
}

async fn transactional(handle: &StorageHandle, seen: &Seen) {
    for key in ["a", "b", "c"] {
        send(handle, write(watch(key), None)).await;
    }
    let txn_id = begin(handle).await;
    send(handle, write(head("a"), Some(txn_id))).await;
    send(handle, delete(head("z"), Some(txn_id))).await;
    assert!(
        stored(handle, watch("a")).await.is_some(),
        "kept until the commit"
    );
    assert!(seen.take().is_empty());
    assert!(committed(&commit(handle, txn_id).await));
    assert_eq!(seen.take(), ["bkt/a", "bkt/z"]);
    assert_eq!(stored(handle, watch("a")).await, None);

    // A transaction that writes the watch key of its head itself keeps its own row.
    let txn_id = begin(handle).await;
    send(handle, write(head("b"), Some(txn_id))).await;
    send(handle, write(watch("b"), Some(txn_id))).await;
    send(handle, delete(watch("c"), Some(txn_id))).await;
    assert!(committed(&commit(handle, txn_id).await));
    assert_eq!(seen.take(), ["bkt/b"]);
    assert!(stored(handle, watch("b")).await.is_some());
    assert_eq!(stored(handle, watch("c")).await, None);
}

#[tokio::test]
async fn transactions_unwatch_heads() {
    both(transactional).await;
}

async fn unchanged(handle: &StorageHandle, seen: &Seen) {
    for key in ["a", "b"] {
        send(handle, write(watch(key), None)).await;
    }
    let aborted = begin(handle).await;
    send(handle, write(head("a"), Some(aborted))).await;
    let event = send(handle, StorageEffect::AbortTransaction { txn_id: aborted }).await;
    assert!(matches!(event, StorageEvent::TransactionAborted { .. }));

    let stale = begin(handle).await;
    send(handle, other(Some(stale), false)).await;
    send(handle, write(head("b"), Some(stale))).await;
    send(handle, other(None, true)).await;
    assert!(conflicted(&commit(handle, stale).await));

    assert!(seen.take().is_empty());
    for key in ["a", "b"] {
        assert!(
            stored(handle, watch(key)).await.is_some(),
            "watch of {key} kept"
        );
        assert_eq!(stored(handle, head(key)).await, None);
    }
}

#[tokio::test]
async fn rollback_keeps_watch() {
    both(unchanged).await;
}

/// A file server transaction that finds `key` without a head and installs its watch key.
async fn install(handle: &StorageHandle, key: &str) -> Ulid {
    let txn_id = begin(handle).await;
    let event = send(handle, read(head(key), Some(txn_id))).await;
    assert!(matches!(
        event,
        StorageEvent::ReadResult { value: None, .. }
    ));
    send(handle, write(watch(key), Some(txn_id))).await;
    txn_id
}

async fn racing(handle: &StorageHandle, seen: &Seen) {
    // The head commits first: the watch key installation conflicts.
    let vfs = install(handle, "a").await;
    let s3 = begin(handle).await;
    send(handle, write(head("a"), Some(s3))).await;
    assert!(committed(&commit(handle, s3).await));
    assert!(conflicted(&commit(handle, vfs).await));
    assert!(stored(handle, head("a")).await.is_some());
    assert_eq!(stored(handle, watch("a")).await, None);

    // The watch key commits first: the head write conflicts.
    let vfs = install(handle, "b").await;
    let s3 = begin(handle).await;
    send(handle, write(head("b"), Some(s3))).await;
    assert!(committed(&commit(handle, vfs).await));
    assert!(conflicted(&commit(handle, s3).await));
    assert_eq!(stored(handle, head("b")).await, None);
    assert!(stored(handle, watch("b")).await.is_some());

    // An immediate head write lands while the installation is open.
    let vfs = install(handle, "c").await;
    send(handle, write(head("c"), None)).await;
    assert!(conflicted(&commit(handle, vfs).await));
    assert!(stored(handle, head("c")).await.is_some());
    assert_eq!(stored(handle, watch("c")).await, None);

    assert_eq!(seen.take(), ["bkt/a", "bkt/c"]);
}

#[tokio::test]
async fn installs_race_heads() {
    both(racing).await;
}
