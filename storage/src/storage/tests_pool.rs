//! Tests that a read in a transaction runs on a read pool while the write thread goes on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::thread;

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use tempfile::tempdir;
use ulid::Ulid;

use super::worker::PoolItem;
use super::{FjallPersistPolicy, FjallStorage, StorageHandle, Store, Txn};

const SPACE: &str = "pooled_reads";

fn write(key: &[u8], txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: SPACE.to_string(),
        key: key.to_vec().into(),
        value: b"v".to_vec().into(),
        txn_id,
    }
}

async fn start(handle: &StorageHandle) -> Ulid {
    let start = StorageEffect::StartTransaction { read: false };
    match handle.send_storage_effect(start).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => panic!("no transaction: {other:?}"),
    }
}

fn is_error(event: &Event, expected: StorageError) -> bool {
    matches!(event, Event::Storage(StorageEvent::Error { error }) if *error == expected)
}

type Held = crossfire::MRx<crossfire::mpmc::Array<PoolItem>>;

/// A worker whose foreground and bulk read pools are queues the test takes reads from.
fn held_pools(path: &Path) -> (Store, StorageHandle, [Held; 2], thread::JoinHandle<()>) {
    let db = fjall::OptimisticTxDatabase::builder(path)
        .manual_journal_persist(true)
        .open()
        .unwrap();
    let store = Store::new(db);
    let (handle, receivers) = StorageHandle::new();
    let policy = FjallPersistPolicy::default();
    let mut storage = FjallStorage::new(store.clone(), policy, &handle, 0, 0);
    let (pool, held) = crossfire::mpmc::bounded_blocking(8);
    let (bulk_pool, bulk) = crossfire::mpmc::bounded_blocking(8);
    (storage.read_pool, storage.bulk_read_pool) = (pool, bulk_pool);
    let worker = thread::spawn(move || storage.receive_loop(receivers));
    (store, handle, [held, bulk], worker)
}

fn batch(keys: impl Iterator<Item = u32>, txn_id: Option<Ulid>) -> StorageEffect {
    let reads = keys.map(|key| (SPACE.to_string(), key.to_be_bytes().to_vec().into()));
    StorageEffect::BatchRead {
        reads: reads.collect(),
        txn_id,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_leave_writer() {
    // A read held on the pool leaves the write thread free. Its transaction takes no write and
    // no commit until the read ends, and the read sees the transaction's own write.
    let dir = tempdir().unwrap();
    let (store, handle, [held, _], worker) = held_pools(dir.path());

    let txn_id = start(&handle).await;
    let own = handle
        .send_storage_effect(write(b"own", Some(txn_id)))
        .await;
    assert!(matches!(
        own,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let read = StorageEffect::BatchRead {
        reads: vec![(SPACE.to_string(), b"own".to_vec().into())],
        txn_id: Some(txn_id),
    };
    let reader = handle.clone();
    let answer = tokio::spawn(async move { reader.send_storage_effect(read).await });
    let ((effect, response, ..), txn) = held.recv().unwrap();
    assert!(
        matches!(txn, Some(Txn::Write(_))),
        "the read carries its transaction"
    );

    let busy = handle
        .send_storage_effect(write(b"later", Some(txn_id)))
        .await;
    assert!(is_error(&busy, StorageError::TransactionConflict));
    let other = handle.send_storage_effect(write(b"other", None)).await;
    assert!(matches!(
        other,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    let event = super::records::run_read(&store, txn.as_ref(), effect);
    drop(txn);
    assert!(response.send(event));
    match answer.await.unwrap() {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            assert_eq!(values[0].1.as_deref(), Some(&b"v"[..]));
        }
        other => panic!("unexpected read {other:?}"),
    }
    let later = handle
        .send_storage_effect(write(b"later", Some(txn_id)))
        .await;
    assert!(matches!(
        later,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));

    // A commit that meets a running read refuses: nothing of the transaction lands.
    let read = StorageEffect::Read {
        key_space: SPACE.to_string(),
        key: b"own".to_vec().into(),
        txn_id: Some(txn_id),
    };
    let reader = handle.clone();
    let pending = tokio::spawn(async move { reader.send_storage_effect(read).await });
    let held_read = held.recv().unwrap();
    let commit = StorageEffect::CommitTransaction { txn_id };
    let commit = handle.send_storage_effect(commit).await;
    assert!(is_error(&commit, StorageError::TransactionConflict));
    drop(held_read);
    assert!(is_error(
        &pending.await.unwrap(),
        StorageError::ChannelClosed
    ));
    let check = StorageEffect::Read {
        key_space: SPACE.to_string(),
        key: b"own".to_vec().into(),
        txn_id: None,
    };
    let event = super::records::run_read(&store, None, check);
    assert!(matches!(
        event,
        StorageEvent::ReadResult { value: None, .. }
    ));

    drop(handle);
    worker.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_reads_bulk() {
    // A large batch read goes to the bulk pool, so small foreground reads never queue behind it.
    let dir = tempdir().unwrap();
    let (_store, handle, [foreground, bulk], worker) = held_pools(dir.path());
    let reader = handle.clone();
    let large = tokio::spawn(async move { reader.send_storage_effect(batch(0..300, None)).await });
    let ((effect, ..), _) = bulk.recv().unwrap();
    assert!(matches!(effect, StorageEffect::BatchRead { reads, .. } if reads.len() == 300));
    let reader = handle.clone();
    let small = tokio::spawn(async move { reader.send_storage_effect(batch(0..3, None)).await });
    let ((effect, ..), _) = foreground.recv().unwrap();
    assert!(matches!(effect, StorageEffect::BatchRead { reads, .. } if reads.len() == 3));
    for answer in [large, small] {
        assert!(is_error(
            &answer.await.unwrap(),
            StorageError::ChannelClosed
        ));
    }
    drop(handle);
    worker.join().unwrap();
}

#[tokio::test]
async fn large_batch_ordered() {
    // A batch read split across threads answers every key in request order; in a transaction it
    // also sees the transaction's own writes.
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let row = |key: u32| {
        (
            SPACE.to_string(),
            key.to_be_bytes().to_vec().into(),
            b"v".to_vec().into(),
        )
    };
    let stored = StorageEffect::BatchWrite {
        writes: (0..2000).step_by(2).map(row).collect(),
        txn_id: None,
    };
    let stored = handle.send_storage_effect(stored).await;
    assert!(matches!(
        stored,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let txn_id = start(&handle).await;
    let own = StorageEffect::BatchWrite {
        writes: (1..200).step_by(2).map(row).collect(),
        txn_id: Some(txn_id),
    };
    let own = handle.send_storage_effect(own).await;
    assert!(matches!(
        own,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    for txn in [None, Some(txn_id)] {
        let read = handle
            .send_storage_effect(batch((0..2000).rev(), txn))
            .await;
        let Event::Storage(StorageEvent::BatchReadResult { values }) = read else {
            panic!("unexpected read {read:?}");
        };
        let expected = (0..2000u32).rev().map(|key| {
            let found = key % 2 == 0 || (txn.is_some() && key < 200);
            (key.to_be_bytes().to_vec(), found)
        });
        let got = (values.iter()).map(|(key, value)| (key.to_vec(), value.is_some()));
        assert!(got.eq(expected));
    }
}
